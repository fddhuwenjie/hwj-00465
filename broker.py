import socket
import threading
import time
import json
import select
import os
import signal
import sys
import fnmatch
import random
import uuid
import calendar
from datetime import datetime, timedelta

from database import init_db, get_cursor
from protocol import recv_message, send_message, encode_message
from queue_manager import QueueManager
from exchange import ExchangeManager

BROKER_HOST = 'localhost'
BROKER_PORT = 9465
DLQ_QUEUE_NAME = 'dead-letter'
MAX_DELIVERY_COUNT = 3
ACK_TIMEOUT = 30
DEFAULT_SLOW_LOG_THRESHOLD_MS = 1000


class CronParser:
    @staticmethod
    def parse(cron_expr):
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 fields: minute hour day month weekday")
        minute, hour, day, month, weekday = parts
        return {
            'minute': CronParser._parse_field(minute, 0, 59),
            'hour': CronParser._parse_field(hour, 0, 23),
            'day': CronParser._parse_field(day, 1, 31),
            'month': CronParser._parse_field(month, 1, 12),
            'weekday': CronParser._parse_field(weekday, 0, 6),
        }

    @staticmethod
    def _parse_field(field, min_val, max_val):
        values = set()
        for part in field.split(','):
            if '/' in part:
                range_part, step = part.split('/')
                step = int(step)
                if range_part == '*':
                    start, end = min_val, max_val
                elif '-' in range_part:
                    s, e = range_part.split('-')
                    start, end = int(s), int(e)
                else:
                    start, end = int(range_part), max_val
                for v in range(start, end + 1, step):
                    if min_val <= v <= max_val:
                        values.add(v)
            elif part == '*':
                values.update(range(min_val, max_val + 1))
            elif '-' in part:
                s, e = part.split('-')
                for v in range(int(s), int(e) + 1):
                    if min_val <= v <= max_val:
                        values.add(v)
            else:
                v = int(part)
                if min_val <= v <= max_val:
                    values.add(v)
        return values

    @staticmethod
    def get_next_run(cron_expr, from_time=None):
        if from_time is None:
            from_time = time.time()
        parsed = CronParser.parse(cron_expr)
        dt = datetime.fromtimestamp(from_time)
        dt = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)

        for _ in range(525600):
            if (dt.minute in parsed['minute']
                    and dt.hour in parsed['hour']
                    and dt.day in parsed['day']
                    and dt.month in parsed['month']
                    and dt.weekday() in parsed['weekday']):
                return dt.timestamp()
            dt += timedelta(minutes=1)
        raise ValueError("Cannot compute next run time")


class Consumer:
    def __init__(self, conn, addr, queue_name, prefetch_count=0, ack_mode='auto',
                 group=None, strategy='round-robin'):
        self.conn = conn
        self.addr = addr
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.ack_mode = ack_mode
        self.group = group
        self.strategy = strategy
        self.consumer_id = str(uuid.uuid4())[:8]
        self.unacked = set()
        self.lock = threading.Lock()
        self.delivered_count = 0

    def can_deliver(self):
        if self.prefetch_count <= 0:
            return True
        with self.lock:
            return len(self.unacked) < self.prefetch_count

    def add_unacked(self, msg_id):
        with self.lock:
            self.unacked.add(msg_id)
        self.delivered_count += 1

    def remove_unacked(self, msg_id):
        with self.lock:
            self.unacked.discard(msg_id)

    def unacked_count(self):
        with self.lock:
            return len(self.unacked)


class Broker:
    def __init__(self):
        self.queue_mgr = QueueManager()
        self.exchange_mgr = ExchangeManager()
        self.consumers = {}
        self.consumers_lock = threading.Lock()
        self.new_msg_event = threading.Event()
        self.running = False
        self.server_sock = None
        self.clients = []
        self.clients_lock = threading.Lock()
        self.start_time = None
        self.group_round_robin = {}
        self.group_rr_lock = threading.Lock()
        self.slow_log_threshold_ms = DEFAULT_SLOW_LOG_THRESHOLD_MS

    def start(self):
        init_db()
        self._ensure_dlq()
        self.start_time = time.time()
        self.running = True

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((BROKER_HOST, BROKER_PORT))
        self.server_sock.listen(50)
        self.server_sock.settimeout(1.0)

        print(f"Broker started on {BROKER_HOST}:{BROKER_PORT}")
        print(f"Data file: {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mq_data.db')}")

        t = threading.Thread(target=self._delivery_loop, daemon=True)
        t.start()

        t2 = threading.Thread(target=self._timeout_loop, daemon=True)
        t2.start()

        t3 = threading.Thread(target=self._scheduler_loop, daemon=True)
        t3.start()

        try:
            while self.running:
                try:
                    conn, addr = self.server_sock.accept()
                except socket.timeout:
                    continue
                t = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                t.start()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self):
        self.running = False
        if self.server_sock:
            self.server_sock.close()
        with self.clients_lock:
            for c in self.clients:
                try:
                    c.close()
                except Exception:
                    pass
        print("\nBroker stopped")

    def _ensure_dlq(self):
        self.queue_mgr.create_queue(DLQ_QUEUE_NAME, durable=True, max_length=0)

    def _handle_client(self, conn, addr):
        with self.clients_lock:
            self.clients.append(conn)
        try:
            while self.running:
                msg = recv_message(conn)
                if msg is None:
                    break
                response = self._handle_command(msg, conn, addr)
                if response is not None:
                    send_message(conn, response)
        except Exception as e:
            pass
        finally:
            self._remove_consumer(conn)
            with self.clients_lock:
                if conn in self.clients:
                    self.clients.remove(conn)
            try:
                conn.close()
            except Exception:
                pass

    def _handle_command(self, msg, conn, addr):
        cmd = msg.get('command')
        if not cmd:
            return {'status': 'error', 'message': 'No command specified'}

        handlers = {
            'create-queue': self._cmd_create_queue,
            'delete-queue': self._cmd_delete_queue,
            'list-queues': self._cmd_list_queues,
            'purge-queue': self._cmd_purge_queue,
            'create-exchange': self._cmd_create_exchange,
            'delete-exchange': self._cmd_delete_exchange,
            'list-exchanges': self._cmd_list_exchanges,
            'bind': self._cmd_bind,
            'unbind': self._cmd_unbind,
            'publish': self._cmd_publish,
            'subscribe': self._cmd_subscribe,
            'ack': self._cmd_ack,
            'nack': self._cmd_nack,
            'status': self._cmd_status,
            'replay': self._cmd_replay,
            'schedule-add': self._cmd_schedule_add,
            'schedule-remove': self._cmd_schedule_remove,
            'schedule-list': self._cmd_schedule_list,
            'schedule-enable': self._cmd_schedule_enable,
            'schedule-disable': self._cmd_schedule_disable,
            'trace': self._cmd_trace,
            'slow-log': self._cmd_slow_log,
        }

        handler = handlers.get(cmd)
        if handler:
            try:
                return handler(msg, conn, addr)
            except Exception as e:
                import traceback
                traceback.print_exc()
                return {'status': 'error', 'message': str(e)}
        return {'status': 'error', 'message': f'Unknown command: {cmd}'}

    def _cmd_create_queue(self, msg, conn, addr):
        name = msg.get('name', '')
        durable = msg.get('durable', True)
        max_length = msg.get('max_length', 0)
        if not name:
            return {'status': 'error', 'message': 'Queue name required'}
        ok, message = self.queue_mgr.create_queue(name, durable, max_length)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_delete_queue(self, msg, conn, addr):
        name = msg.get('name', '')
        if not name:
            return {'status': 'error', 'message': 'Queue name required'}
        ok, message = self.queue_mgr.delete_queue(name)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_list_queues(self, msg, conn, addr):
        queues = self.queue_mgr.list_queues()
        for q in queues:
            q['consumers'] = self._count_consumers(q['name'])
        return {'status': 'ok', 'queues': queues}

    def _cmd_purge_queue(self, msg, conn, addr):
        name = msg.get('name', '')
        if not name:
            return {'status': 'error', 'message': 'Queue name required'}
        ok, message = self.queue_mgr.purge_queue(name)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_create_exchange(self, msg, conn, addr):
        name = msg.get('name', '')
        type = msg.get('type', 'direct')
        ok, message = self.exchange_mgr.create_exchange(name, type)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_delete_exchange(self, msg, conn, addr):
        name = msg.get('name', '')
        ok, message = self.exchange_mgr.delete_exchange(name)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_list_exchanges(self, msg, conn, addr):
        exchanges = self.exchange_mgr.list_exchanges()
        return {'status': 'ok', 'exchanges': exchanges}

    def _cmd_bind(self, msg, conn, addr):
        exchange = msg.get('exchange', '')
        queue = msg.get('queue', '')
        routing_key = msg.get('routing_key', '')
        if not queue:
            return {'status': 'error', 'message': 'Queue name required'}
        ok, message = self.exchange_mgr.bind_queue(exchange, queue, routing_key)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_unbind(self, msg, conn, addr):
        exchange = msg.get('exchange', '')
        queue = msg.get('queue', '')
        routing_key = msg.get('routing_key', '')
        if not queue:
            return {'status': 'error', 'message': 'Queue name required'}
        ok, message = self.exchange_mgr.unbind_queue(exchange, queue, routing_key)
        return {'status': 'ok' if ok else 'error', 'message': message}

    def _cmd_publish(self, msg, conn, addr):
        exchange = msg.get('exchange', '')
        routing_key = msg.get('routing_key', '')
        body = msg.get('body', '')
        priority = msg.get('priority', 0)
        ttl = msg.get('ttl', 0)
        correlation_id = msg.get('correlation_id', '')
        reply_to = msg.get('reply_to', '')
        delay = msg.get('delay', 0)

        if not exchange and routing_key:
            queue_names = [routing_key]
        else:
            queue_names = self.exchange_mgr.get_bound_queues(exchange, routing_key)

        if not queue_names:
            return {'status': 'error', 'message': 'No matching queue'}

        count = 0
        now = time.time()
        for queue_name in queue_names:
            queue_id, max_length = self.queue_mgr.get_queue_id(queue_name)
            if queue_id is None:
                continue

            available_at = now + delay
            expires_at = now + ttl if ttl > 0 else 0

            if max_length > 0:
                with get_cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) as cnt FROM messages WHERE queue_id = ? AND status = 'ready'",
                        (queue_id,)
                    )
                    current = cur.fetchone()['cnt']
                    if current >= max_length:
                        continue

            with get_cursor() as cur:
                cur.execute("""
                    INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                         delay, available_at, expires_at, status, delivery_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', 0, ?)
                """, (queue_id, body, priority, ttl, correlation_id, reply_to,
                      delay, available_at, expires_at, now))
                msg_id = cur.lastrowid

                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, timestamp)
                    VALUES (?, ?, ?, ?, 'publish', 'publish', ?)
                """, (msg_id, queue_id, correlation_id, body, now))

                cur.execute("UPDATE stats SET total_published = total_published + 1")

            count += 1

        self.new_msg_event.set()
        self.new_msg_event.clear()

        return {'status': 'ok', 'message': f'Published to {count} queue(s)'}

    def _cmd_subscribe(self, msg, conn, addr):
        queue_name = msg.get('queue', '')
        prefetch_count = msg.get('prefetch_count', 0)
        ack_mode = msg.get('ack_mode', 'auto')
        group = msg.get('group')
        strategy = msg.get('strategy', 'round-robin')

        if strategy not in ('round-robin', 'least-unacked', 'random'):
            strategy = 'round-robin'

        if not queue_name:
            return {'status': 'error', 'message': 'Queue name required'}

        queue_id, _ = self.queue_mgr.get_queue_id(queue_name)
        if queue_id is None:
            return {'status': 'error', 'message': 'Queue not found'}

        consumer = Consumer(conn, addr, queue_name, prefetch_count, ack_mode, group, strategy)
        with self.consumers_lock:
            if conn not in self.consumers:
                self.consumers[conn] = []
            self.consumers[conn].append(consumer)

        self.new_msg_event.set()
        self.new_msg_event.clear()

        return None

    def _cmd_ack(self, msg, conn, addr):
        message_id = msg.get('message_id')
        if message_id is None:
            return {'status': 'error', 'message': 'message_id required'}

        consumer = self._find_consumer_by_conn(conn)
        if consumer:
            consumer.remove_unacked(message_id)

        now = time.time()
        duration_ms = None

        with get_cursor() as cur:
            cur.execute("SELECT queue_id, body, correlation_id, delivered_at FROM messages WHERE id = ?", (message_id,))
            row = cur.fetchone()
            if row:
                if row['delivered_at']:
                    duration_ms = (now - row['delivered_at']) * 1000
                cur.execute("DELETE FROM messages WHERE id = ?", (message_id,))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, duration_ms, timestamp)
                    VALUES (?, ?, ?, ?, 'ack', 'consume', ?, ?, ?, ?)
                """, (message_id, row['queue_id'], row['correlation_id'], row['body'],
                      consumer.consumer_id if consumer else None,
                      consumer.group if consumer else None,
                      duration_ms, now))
                cur.execute("UPDATE stats SET total_acked = total_acked + 1, total_consumed = total_consumed + 1")

        self.new_msg_event.set()
        self.new_msg_event.clear()

        return {'status': 'ok', 'message': 'Message acked'}

    def _cmd_nack(self, msg, conn, addr):
        message_id = msg.get('message_id')
        requeue = msg.get('requeue', True)

        if message_id is None:
            return {'status': 'error', 'message': 'message_id required'}

        consumer = self._find_consumer_by_conn(conn)
        if consumer:
            consumer.remove_unacked(message_id)

        now = time.time()
        duration_ms = None

        with get_cursor() as cur:
            cur.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            row = cur.fetchone()
            if not row:
                return {'status': 'error', 'message': 'Message not found'}

            if row['delivered_at']:
                duration_ms = (now - row['delivered_at']) * 1000

            new_delivery_count = row['delivery_count'] + 1

            if not requeue or new_delivery_count >= MAX_DELIVERY_COUNT:
                dlq_id, _ = self.queue_mgr.get_queue_id(DLQ_QUEUE_NAME)
                if dlq_id:
                    cur.execute("""
                        INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                             delay, available_at, expires_at, status, delivery_count, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ready', 0, ?)
                    """, (dlq_id, row['body'], row['priority'], row['ttl'],
                          row['correlation_id'], row['reply_to'], row['delay'],
                          time.time(), time.time()))

                cur.execute("DELETE FROM messages WHERE id = ?", (message_id,))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, duration_ms, timestamp)
                    VALUES (?, ?, ?, ?, 'dead_letter', 'dlq', ?, ?, ?, ?)
                """, (message_id, row['queue_id'], row['correlation_id'], row['body'],
                      consumer.consumer_id if consumer else None,
                      consumer.group if consumer else None,
                      duration_ms, now))
                cur.execute("UPDATE stats SET total_nacked = total_nacked + 1")
            else:
                cur.execute("""
                    UPDATE messages SET status = 'ready', delivery_count = ?, available_at = ?
                    WHERE id = ?
                """, (new_delivery_count, time.time(), message_id))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, duration_ms, timestamp)
                    VALUES (?, ?, ?, ?, 'nack_requeue', 'requeue', ?, ?, ?, ?)
                """, (message_id, row['queue_id'], row['correlation_id'], row['body'],
                      consumer.consumer_id if consumer else None,
                      consumer.group if consumer else None,
                      duration_ms, now))

        self.new_msg_event.set()
        self.new_msg_event.clear()

        return {'status': 'ok', 'message': 'Message nacked'}

    def _cmd_status(self, msg, conn, addr):
        with get_cursor() as cur:
            cur.execute("SELECT * FROM stats ORDER BY id DESC LIMIT 1")
            stats_row = cur.fetchone()

            cur.execute("SELECT COUNT(*) as cnt FROM messages")
            total_messages = cur.fetchone()['cnt']

            cur.execute("SELECT COUNT(*) as cnt FROM queues")
            total_queues = cur.fetchone()['cnt']

            cur.execute("SELECT COUNT(*) as cnt FROM messages WHERE status = 'ready'")
            ready_messages = cur.fetchone()['cnt']

            cur.execute("SELECT COUNT(*) as cnt FROM messages WHERE status = 'unacked'")
            unacked_messages = cur.fetchone()['cnt']

            cur.execute("SELECT COUNT(*) as cnt FROM schedules WHERE enabled = 1")
            active_schedules = cur.fetchone()['cnt']

        uptime = time.time() - self.start_time if self.start_time else 0
        total_consumed = stats_row['total_consumed'] if stats_row else 0
        consume_rate = total_consumed / uptime if uptime > 0 else 0

        return {
            'status': 'ok',
            'broker': {
                'uptime_seconds': int(uptime),
                'uptime_formatted': self._format_uptime(uptime),
                'total_queues': total_queues,
                'total_messages': total_messages,
                'ready_messages': ready_messages,
                'unacked_messages': unacked_messages,
                'total_published': stats_row['total_published'] if stats_row else 0,
                'total_consumed': total_consumed,
                'total_acked': stats_row['total_acked'] if stats_row else 0,
                'total_nacked': stats_row['total_nacked'] if stats_row else 0,
                'consume_rate': round(consume_rate, 2),
                'active_consumers': self._count_total_consumers(),
                'active_schedules': active_schedules,
            }
        }

    def _cmd_replay(self, msg, conn, addr):
        queue_name = msg.get('queue', '')
        count = msg.get('count', 10)
        offset = msg.get('offset', 0)

        if not queue_name:
            return {'status': 'error', 'message': 'Queue name required'}

        queue_id, _ = self.queue_mgr.get_queue_id(queue_name)
        if queue_id is None:
            return {'status': 'error', 'message': 'Queue not found'}

        with get_cursor() as cur:
            cur.execute("""
                SELECT DISTINCT message_id, body, correlation_id FROM message_history
                WHERE queue_id = ? AND action = 'publish'
                ORDER BY id DESC LIMIT ? OFFSET ?
            """, (queue_id, count, offset))
            rows = cur.fetchall()

            replayed = 0
            for row in reversed(rows):
                now = time.time()
                cur.execute("""
                    INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                         delay, available_at, expires_at, status, delivery_count, created_at)
                    VALUES (?, ?, 0, 0, ?, '', 0, ?, 0, 'ready', 0, ?)
                """, (queue_id, row['body'], row['correlation_id'] or '', now, now))
                new_msg_id = cur.lastrowid
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, timestamp)
                    VALUES (?, ?, ?, ?, 'publish', 'replay', ?)
                """, (new_msg_id, queue_id, row['correlation_id'], row['body'], now))
                replayed += 1

            if replayed > 0:
                cur.execute("UPDATE stats SET total_published = total_published + ?", (replayed,))

        if replayed > 0:
            self.new_msg_event.set()
            self.new_msg_event.clear()

        return {'status': 'ok', 'message': f'Replayed {replayed} messages'}

    def _cmd_schedule_add(self, msg, conn, addr):
        name = msg.get('name', '')
        cron_expr = msg.get('cron', '')
        exchange = msg.get('exchange', '')
        routing_key = msg.get('routing_key', '')
        queue_name = msg.get('queue', '')
        body = msg.get('body', '')
        priority = msg.get('priority', 0)
        ttl = msg.get('ttl', 0)
        correlation_id = msg.get('correlation_id', '')
        reply_to = msg.get('reply_to', '')
        description = msg.get('description', '')
        enabled = 1 if msg.get('enabled', True) else 0

        if not name:
            return {'status': 'error', 'message': 'Schedule name required'}
        if not cron_expr:
            return {'status': 'error', 'message': 'Cron expression required'}
        if not body:
            return {'status': 'error', 'message': 'Body required'}
        if not exchange and not routing_key and not queue_name:
            return {'status': 'error', 'message': 'Either exchange+routing_key or queue is required'}

        try:
            CronParser.parse(cron_expr)
        except Exception as e:
            return {'status': 'error', 'message': f'Invalid cron expression: {e}'}

        if queue_name and not routing_key:
            routing_key = queue_name

        now = time.time()
        try:
            next_run = CronParser.get_next_run(cron_expr, now) if enabled else None
        except Exception:
            next_run = None

        with get_cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO schedules (name, cron_expression, exchange, routing_key, queue_name, body,
                                          priority, ttl, correlation_id, reply_to, description,
                                          enabled, last_run_at, next_run_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, cron_expr, exchange, routing_key, queue_name, body,
                      priority, ttl, correlation_id, reply_to, description,
                      enabled, None, next_run, now, now))
            except Exception as e:
                return {'status': 'error', 'message': f'Schedule already exists: {e}'}

        return {'status': 'ok', 'message': f'Schedule "{name}" added', 'next_run': next_run}

    def _cmd_schedule_remove(self, msg, conn, addr):
        name = msg.get('name', '')
        if not name:
            return {'status': 'error', 'message': 'Schedule name required'}

        with get_cursor() as cur:
            cur.execute("DELETE FROM schedules WHERE name = ?", (name,))
            if cur.rowcount == 0:
                return {'status': 'error', 'message': 'Schedule not found'}

        return {'status': 'ok', 'message': f'Schedule "{name}" removed'}

    def _cmd_schedule_list(self, msg, conn, addr):
        with get_cursor() as cur:
            cur.execute("""
                SELECT id, name, cron_expression, exchange, routing_key, queue_name, body,
                       priority, ttl, correlation_id, reply_to, description,
                       enabled, last_run_at, next_run_at, created_at, updated_at
                FROM schedules ORDER BY name
            """)
            rows = cur.fetchall()
            schedules = []
            for r in rows:
                schedules.append({
                    'id': r['id'],
                    'name': r['name'],
                    'cron': r['cron_expression'],
                    'exchange': r['exchange'],
                    'routing_key': r['routing_key'],
                    'queue': r['queue_name'],
                    'body': r['body'],
                    'priority': r['priority'],
                    'ttl': r['ttl'],
                    'correlation_id': r['correlation_id'],
                    'reply_to': r['reply_to'],
                    'description': r['description'],
                    'enabled': bool(r['enabled']),
                    'last_run_at': r['last_run_at'],
                    'next_run_at': r['next_run_at'],
                    'created_at': r['created_at'],
                    'updated_at': r['updated_at'],
                })
        return {'status': 'ok', 'schedules': schedules}

    def _cmd_schedule_enable(self, msg, conn, addr):
        name = msg.get('name', '')
        if not name:
            return {'status': 'error', 'message': 'Schedule name required'}

        now = time.time()
        with get_cursor() as cur:
            cur.execute("SELECT cron_expression FROM schedules WHERE name = ?", (name,))
            row = cur.fetchone()
            if not row:
                return {'status': 'error', 'message': 'Schedule not found'}
            try:
                next_run = CronParser.get_next_run(row['cron_expression'], now)
            except Exception:
                next_run = None
            cur.execute("""
                UPDATE schedules SET enabled = 1, next_run_at = ?, updated_at = ? WHERE name = ?
            """, (next_run, now, name))

        return {'status': 'ok', 'message': f'Schedule "{name}" enabled'}

    def _cmd_schedule_disable(self, msg, conn, addr):
        name = msg.get('name', '')
        if not name:
            return {'status': 'error', 'message': 'Schedule name required'}

        now = time.time()
        with get_cursor() as cur:
            cur.execute("SELECT name FROM schedules WHERE name = ?", (name,))
            row = cur.fetchone()
            if not row:
                return {'status': 'error', 'message': 'Schedule not found'}
            cur.execute("""
                UPDATE schedules SET enabled = 0, next_run_at = NULL, updated_at = ? WHERE name = ?
            """, (now, name))

        return {'status': 'ok', 'message': f'Schedule "{name}" disabled'}

    def _cmd_trace(self, msg, conn, addr):
        message_id = msg.get('message_id')
        correlation_id = msg.get('correlation_id')

        if not message_id and not correlation_id:
            return {'status': 'error', 'message': 'Either message_id or correlation_id is required'}

        with get_cursor() as cur:
            params = []
            conditions = []
            if message_id:
                conditions.append("message_id = ?")
                params.append(message_id)
            if correlation_id:
                conditions.append("correlation_id = ?")
                params.append(correlation_id)

            where = " OR ".join(conditions)

            cur.execute(f"""
                SELECT h.id, h.message_id, h.queue_id, h.correlation_id, h.body,
                       h.action, h.stage, h.detail, h.consumer_id, h.consumer_group,
                       h.duration_ms, h.timestamp, q.name as queue_name
                FROM message_history h
                LEFT JOIN queues q ON h.queue_id = q.id
                WHERE {where}
                ORDER BY h.timestamp ASC, h.id ASC
            """, params)
            rows = cur.fetchall()

            events = []
            for r in rows:
                events.append({
                    'id': r['id'],
                    'message_id': r['message_id'],
                    'queue_id': r['queue_id'],
                    'queue_name': r['queue_name'],
                    'correlation_id': r['correlation_id'],
                    'body': r['body'],
                    'action': r['action'],
                    'stage': r['stage'],
                    'detail': r['detail'],
                    'consumer_id': r['consumer_id'],
                    'consumer_group': r['consumer_group'],
                    'duration_ms': round(r['duration_ms'], 2) if r['duration_ms'] is not None else None,
                    'timestamp': r['timestamp'],
                    'timestamp_formatted': datetime.fromtimestamp(r['timestamp']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                })

            summary = {}
            if events:
                first_ts = events[0]['timestamp']
                last_ts = events[-1]['timestamp']
                summary = {
                    'total_events': len(events),
                    'total_duration_ms': round((last_ts - first_ts) * 1000, 2),
                    'first_event': events[0]['action'],
                    'last_event': events[-1]['action'],
                    'correlation_id': events[0]['correlation_id'],
                }

        return {'status': 'ok', 'events': events, 'summary': summary}

    def _cmd_slow_log(self, msg, conn, addr):
        threshold_ms = msg.get('threshold_ms', self.slow_log_threshold_ms)
        limit = msg.get('limit', 50)

        with get_cursor() as cur:
            cur.execute("""
                SELECT h.id, h.message_id, h.queue_id, h.correlation_id, h.body,
                       h.action, h.stage, h.duration_ms, h.timestamp, q.name as queue_name,
                       h.consumer_id, h.consumer_group
                FROM message_history h
                LEFT JOIN queues q ON h.queue_id = q.id
                WHERE h.duration_ms IS NOT NULL AND h.duration_ms >= ?
                ORDER BY h.duration_ms DESC
                LIMIT ?
            """, (threshold_ms, limit))
            rows = cur.fetchall()

            slow_msgs = []
            for r in rows:
                slow_msgs.append({
                    'id': r['id'],
                    'message_id': r['message_id'],
                    'queue_id': r['queue_id'],
                    'queue_name': r['queue_name'],
                    'correlation_id': r['correlation_id'],
                    'action': r['action'],
                    'stage': r['stage'],
                    'duration_ms': round(r['duration_ms'], 2),
                    'timestamp': r['timestamp'],
                    'timestamp_formatted': datetime.fromtimestamp(r['timestamp']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'consumer_id': r['consumer_id'],
                    'consumer_group': r['consumer_group'],
                })

        return {'status': 'ok', 'slow_messages': slow_msgs, 'threshold_ms': threshold_ms}

    def _find_consumer_by_conn(self, conn):
        with self.consumers_lock:
            if conn in self.consumers and self.consumers[conn]:
                return self.consumers[conn][0]
        return None

    def _count_consumers(self, queue_name):
        count = 0
        with self.consumers_lock:
            for conn, consumers in self.consumers.items():
                for c in consumers:
                    if c.queue_name == queue_name:
                        count += 1
        return count

    def _count_total_consumers(self):
        count = 0
        with self.consumers_lock:
            for consumers in self.consumers.values():
                count += len(consumers)
        return count

    def _remove_consumer(self, conn):
        with self.consumers_lock:
            if conn in self.consumers:
                consumers = self.consumers.pop(conn)
                for c in consumers:
                    for msg_id in list(c.unacked):
                        self._requeue_unacked(msg_id, c)

    def _requeue_unacked(self, msg_id, consumer=None):
        with get_cursor() as cur:
            cur.execute("SELECT delivery_count, delivered_at, correlation_id, body, queue_id FROM messages WHERE id = ?", (msg_id,))
            row = cur.fetchone()
            if row:
                now = time.time()
                duration_ms = None
                if row['delivered_at']:
                    duration_ms = (now - row['delivered_at']) * 1000
                new_count = row['delivery_count'] + 1
                if new_count >= MAX_DELIVERY_COUNT:
                    cur.execute("SELECT queue_id, body FROM messages WHERE id = ?", (msg_id,))
                    msg_row = cur.fetchone()
                    dlq_id, _ = self.queue_mgr.get_queue_id(DLQ_QUEUE_NAME)
                    if dlq_id and msg_row:
                        cur.execute("""
                            INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                                 delay, available_at, expires_at, status, delivery_count, created_at)
                            VALUES (?, ?, 0, 0, '', '', 0, ?, 0, 'ready', 0, ?)
                        """, (dlq_id, msg_row['body'], time.time(), time.time()))
                    cur.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                    cur.execute("""
                        INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, duration_ms, timestamp)
                        VALUES (?, ?, ?, ?, 'timeout_dlq', 'disconnect_dlq', ?, ?, ?, ?)
                    """, (msg_id, row['queue_id'], row['correlation_id'], row['body'],
                          consumer.consumer_id if consumer else None,
                          consumer.group if consumer else None,
                          duration_ms, now))
                    cur.execute("UPDATE stats SET total_nacked = total_nacked + 1")
                else:
                    cur.execute("""
                        UPDATE messages SET status = 'ready', delivery_count = ?, available_at = ?
                        WHERE id = ?
                    """, (new_count, time.time(), msg_id))
                    cur.execute("""
                        INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, duration_ms, timestamp)
                        VALUES (?, ?, ?, ?, 'disconnect_requeue', 'requeue', ?, ?, ?, ?)
                    """, (msg_id, row['queue_id'], row['correlation_id'], row['body'],
                          consumer.consumer_id if consumer else None,
                          consumer.group if consumer else None,
                          duration_ms, now))

    def _scheduler_loop(self):
        while self.running:
            try:
                self._process_schedules()
            except Exception as e:
                pass
            time.sleep(1)

    def _process_schedules(self):
        now = time.time()
        with get_cursor() as cur:
            cur.execute("""
                SELECT * FROM schedules
                WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?
            """, (now,))
            due = cur.fetchall()

            for sched in due:
                try:
                    self._publish_scheduled_message(sched)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                next_run = None
                try:
                    next_run = CronParser.get_next_run(sched['cron_expression'], now)
                except Exception:
                    pass
                cur.execute("""
                    UPDATE schedules SET last_run_at = ?, next_run_at = ?, updated_at = ? WHERE id = ?
                """, (now, next_run, now, sched['id']))

    def _publish_scheduled_message(self, sched):
        exchange = sched['exchange']
        routing_key = sched['routing_key']
        body = sched['body']
        priority = sched['priority']
        ttl = sched['ttl']
        correlation_id = sched['correlation_id'] or ''
        reply_to = sched['reply_to'] or ''

        if not exchange and routing_key:
            queue_names = [routing_key]
        else:
            queue_names = self.exchange_mgr.get_bound_queues(exchange, routing_key)

        if not queue_names:
            return

        now = time.time()
        for queue_name in queue_names:
            queue_id, max_length = self.queue_mgr.get_queue_id(queue_name)
            if queue_id is None:
                continue

            available_at = now
            expires_at = now + ttl if ttl > 0 else 0

            if max_length > 0:
                with get_cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) as cnt FROM messages WHERE queue_id = ? AND status = 'ready'",
                        (queue_id,)
                    )
                    current = cur.fetchone()['cnt']
                    if current >= max_length:
                        continue

            with get_cursor() as cur:
                cur.execute("""
                    INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                         delay, available_at, expires_at, status, delivery_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 'ready', 0, ?)
                """, (queue_id, body, priority, ttl, correlation_id, reply_to,
                      available_at, expires_at, now))
                msg_id = cur.lastrowid

                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, detail, timestamp)
                    VALUES (?, ?, ?, ?, 'publish', 'scheduled', ?, ?)
                """, (msg_id, queue_id, correlation_id, body, f'schedule:{sched["name"]}', now))

                cur.execute("UPDATE stats SET total_published = total_published + 1")

        self.new_msg_event.set()
        self.new_msg_event.clear()

    def _delivery_loop(self):
        while self.running:
            try:
                delivered = self._try_deliver()
                if not delivered:
                    self.new_msg_event.wait(timeout=0.5)
            except Exception as e:
                import traceback
                traceback.print_exc()
                time.sleep(0.5)

    def _try_deliver(self):
        delivered = False
        now = time.time()

        with self.consumers_lock:
            all_consumers = []
            for conn, consumers in self.consumers.items():
                for c in consumers:
                    all_consumers.append((conn, c))

        queue_to_consumers = {}
        for conn, c in all_consumers:
            if c.queue_name not in queue_to_consumers:
                queue_to_consumers[c.queue_name] = []
            queue_to_consumers[c.queue_name].append((conn, c))

        for queue_name, consumer_list in queue_to_consumers.items():
            queue_id, _ = self.queue_mgr.get_queue_id(queue_name)
            if queue_id is None:
                continue

            grouped = {}
            standalone = []
            for conn, c in consumer_list:
                if c.group:
                    key = c.group
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append((conn, c))
                else:
                    standalone.append((conn, c))

            for conn, c in standalone:
                if not c.can_deliver():
                    continue
                d = self._deliver_to_consumer(conn, c, queue_id, now)
                if d:
                    delivered = True

            for group, group_consumers in grouped.items():
                picked_msgs_ids = set()
                max_rounds = max(1, len(group_consumers) * 10)
                for _ in range(max_rounds):
                    eligible = [(conn, c) for conn, c in group_consumers if c.can_deliver()]
                    if not eligible:
                        break
                    conn, c = self._select_consumer_for_group(eligible, group, queue_name)
                    if conn is None:
                        break
                    msg_id = self._deliver_one_to_consumer(conn, c, queue_id, now, exclude_msg_ids=picked_msgs_ids)
                    if msg_id:
                        picked_msgs_ids.add(msg_id)
                        delivered = True
                    else:
                        break

        return delivered

    def _select_consumer_for_group(self, eligible, group, queue_name):
        if not eligible:
            return None, None

        strategy = eligible[0][1].strategy

        if strategy == 'least-unacked':
            eligible.sort(key=lambda x: x[1].unacked_count())
            return eligible[0]
        elif strategy == 'random':
            return random.choice(eligible)
        else:
            rr_key = (queue_name, group)
            with self.group_rr_lock:
                if rr_key not in self.group_round_robin:
                    self.group_round_robin[rr_key] = 0
                idx = self.group_round_robin[rr_key] % len(eligible)
                self.group_round_robin[rr_key] += 1
            return eligible[idx]

    def _deliver_to_consumer(self, conn, consumer, queue_id, now):
        return self._deliver_one_to_consumer(conn, consumer, queue_id, now) is not None

    def _deliver_one_to_consumer(self, conn, consumer, queue_id, now, exclude_msg_ids=None):
        with get_cursor() as cur:
            query = """
                SELECT * FROM messages
                WHERE queue_id = ? AND status = 'ready' AND available_at <= ?
                AND (expires_at = 0 OR expires_at > ?)
            """
            params = [queue_id, now, now]

            if exclude_msg_ids:
                placeholders = ','.join(['?'] * len(exclude_msg_ids))
                query += f" AND id NOT IN ({placeholders})"
                params.extend(list(exclude_msg_ids))

            query += " ORDER BY priority DESC, id ASC LIMIT 1"

            cur.execute(query, params)
            row = cur.fetchone()

            if not row:
                return None

            msg_id = row['id']
            cur.execute("UPDATE messages SET status = 'unacked', delivered_at = ?, delivery_count = delivery_count + 1 WHERE id = ?",
                        (now, msg_id))

            cur.execute("""
                INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, timestamp)
                VALUES (?, ?, ?, ?, 'deliver', 'routing_deliver', ?, ?, ?)
            """, (msg_id, queue_id, row['correlation_id'], row['body'],
                  consumer.consumer_id, consumer.group, now))

            msg_data = {
                'type': 'message',
                'message_id': msg_id,
                'body': row['body'],
                'priority': row['priority'],
                'correlation_id': row['correlation_id'],
                'reply_to': row['reply_to'],
                'delivery_count': row['delivery_count'] + 1,
                'consumer_id': consumer.consumer_id,
                'consumer_group': consumer.group,
            }

            try:
                conn.sendall(encode_message(msg_data))
                if consumer.ack_mode == 'manual':
                    consumer.add_unacked(msg_id)
                else:
                    duration_ms = (now - row['created_at']) * 1000
                    cur.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                    cur.execute("""
                        INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, consumer_id, consumer_group, duration_ms, timestamp)
                        VALUES (?, ?, ?, ?, 'consume', 'consume', ?, ?, ?, ?)
                    """, (msg_id, queue_id, row['correlation_id'], row['body'],
                          consumer.consumer_id, consumer.group, duration_ms, now))
                    cur.execute("UPDATE stats SET total_consumed = total_consumed + 1, total_acked = total_acked + 1")
                return msg_id
            except Exception:
                cur.execute("UPDATE messages SET status = 'ready', delivered_at = NULL WHERE id = ?", (msg_id,))
                return None

    def _timeout_loop(self):
        while self.running:
            try:
                self._check_expired()
                self._check_ack_timeout()
            except Exception:
                pass
            time.sleep(1)

    def _check_expired(self):
        now = time.time()
        with get_cursor() as cur:
            cur.execute("""
                SELECT id, queue_id, body, correlation_id FROM messages
                WHERE status = 'ready' AND expires_at > 0 AND expires_at <= ?
            """, (now,))
            rows = cur.fetchall()
            for row in rows:
                cur.execute("DELETE FROM messages WHERE id = ?", (row['id'],))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, timestamp)
                    VALUES (?, ?, ?, ?, 'expired', 'ttl_expire', ?)
                """, (row['id'], row['queue_id'], row['correlation_id'], row['body'], now))

    def _check_ack_timeout(self):
        now = time.time()
        with get_cursor() as cur:
            cur.execute("""
                SELECT id, queue_id, body, delivery_count, delivered_at, correlation_id FROM messages
                WHERE status = 'unacked' AND delivered_at IS NOT NULL AND (delivered_at + ?) <= ?
            """, (ACK_TIMEOUT, now))
            rows = cur.fetchall()
            for row in rows:
                msg_id = row['id']
                duration_ms = None
                if row['delivered_at']:
                    duration_ms = (now - row['delivered_at']) * 1000
                new_count = row['delivery_count'] + 1
                if new_count >= MAX_DELIVERY_COUNT:
                    dlq_id, _ = self.queue_mgr.get_queue_id(DLQ_QUEUE_NAME)
                    if dlq_id:
                        cur.execute("""
                            INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                                 delay, available_at, expires_at, status, delivery_count, created_at)
                            VALUES (?, ?, 0, 0, ?, '', 0, ?, 0, 'ready', 0, ?)
                        """, (dlq_id, row['body'], row['correlation_id'] or '', now, now))
                    cur.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                    cur.execute("""
                        INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, duration_ms, timestamp)
                        VALUES (?, ?, ?, ?, 'timeout_dlq', 'ack_timeout_dlq', ?, ?)
                    """, (msg_id, row['queue_id'], row['correlation_id'], row['body'], duration_ms, now))
                    cur.execute("UPDATE stats SET total_nacked = total_nacked + 1")
                else:
                    cur.execute("""
                        UPDATE messages SET status = 'ready', delivery_count = ?, available_at = ?, delivered_at = NULL
                        WHERE id = ?
                    """, (new_count, now, msg_id))
                    cur.execute("""
                        INSERT INTO message_history (message_id, queue_id, correlation_id, body, action, stage, duration_ms, timestamp)
                        VALUES (?, ?, ?, ?, 'timeout_requeue', 'ack_timeout_requeue', ?, ?)
                    """, (msg_id, row['queue_id'], row['correlation_id'], row['body'], duration_ms, now))

                self._remove_consumer_unacked_by_msg_id(msg_id)

    def _remove_consumer_unacked_by_msg_id(self, msg_id):
        with self.consumers_lock:
            for conn, consumers in self.consumers.items():
                for c in consumers:
                    c.remove_unacked(msg_id)

    def _format_uptime(self, seconds):
        seconds = int(seconds)
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        parts.append(f"{seconds}s")
        return ' '.join(parts)


def main():
    broker = Broker()
    broker.start()


if __name__ == '__main__':
    main()
