import socket
import threading
import time
import json
import select
import os
import signal
import sys
import fnmatch

from database import init_db, get_cursor
from protocol import recv_message, send_message, encode_message
from queue_manager import QueueManager
from exchange import ExchangeManager

BROKER_HOST = 'localhost'
BROKER_PORT = 9465
DLQ_QUEUE_NAME = 'dead-letter'
MAX_DELIVERY_COUNT = 3
ACK_TIMEOUT = 30


class Consumer:
    def __init__(self, conn, addr, queue_name, prefetch_count=0, ack_mode='auto'):
        self.conn = conn
        self.addr = addr
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.ack_mode = ack_mode
        self.unacked = set()
        self.lock = threading.Lock()

    def can_deliver(self):
        if self.prefetch_count <= 0:
            return True
        with self.lock:
            return len(self.unacked) < self.prefetch_count

    def add_unacked(self, msg_id):
        with self.lock:
            self.unacked.add(msg_id)

    def remove_unacked(self, msg_id):
        with self.lock:
            self.unacked.discard(msg_id)


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
        }

        handler = handlers.get(cmd)
        if handler:
            try:
                return handler(msg, conn, addr)
            except Exception as e:
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
        for queue_name in queue_names:
            queue_id, max_length = self.queue_mgr.get_queue_id(queue_name)
            if queue_id is None:
                continue

            now = time.time()
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
                    INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                    VALUES (?, ?, ?, 'publish', ?)
                """, (msg_id, queue_id, body, now))

                cur.execute("UPDATE stats SET total_published = total_published + 1")

            count += 1

        self.new_msg_event.set()
        self.new_msg_event.clear()

        return {'status': 'ok', 'message': f'Published to {count} queue(s)'}

    def _cmd_subscribe(self, msg, conn, addr):
        queue_name = msg.get('queue', '')
        prefetch_count = msg.get('prefetch_count', 0)
        ack_mode = msg.get('ack_mode', 'auto')

        if not queue_name:
            return {'status': 'error', 'message': 'Queue name required'}

        queue_id, _ = self.queue_mgr.get_queue_id(queue_name)
        if queue_id is None:
            return {'status': 'error', 'message': 'Queue not found'}

        consumer = Consumer(conn, addr, queue_name, prefetch_count, ack_mode)
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

        with get_cursor() as cur:
            cur.execute("SELECT queue_id, body FROM messages WHERE id = ?", (message_id,))
            row = cur.fetchone()
            if row:
                cur.execute("DELETE FROM messages WHERE id = ?", (message_id,))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                    VALUES (?, ?, ?, 'ack', ?)
                """, (message_id, row['queue_id'], row['body'], time.time()))
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

        with get_cursor() as cur:
            cur.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            row = cur.fetchone()
            if not row:
                return {'status': 'error', 'message': 'Message not found'}

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
                    INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                    VALUES (?, ?, ?, 'dead_letter', ?)
                """, (message_id, row['queue_id'], row['body'], time.time()))
                cur.execute("UPDATE stats SET total_nacked = total_nacked + 1")
            else:
                cur.execute("""
                    UPDATE messages SET status = 'ready', delivery_count = ?, available_at = ?
                    WHERE id = ?
                """, (new_delivery_count, time.time(), message_id))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                    VALUES (?, ?, ?, 'nack_requeue', ?)
                """, (message_id, row['queue_id'], row['body'], time.time()))

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
                SELECT DISTINCT message_id, body FROM message_history
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
                    VALUES (?, ?, 0, 0, '', '', 0, ?, 0, 'ready', 0, ?)
                """, (queue_id, row['body'], now, now))
                replayed += 1

            if replayed > 0:
                cur.execute("UPDATE stats SET total_published = total_published + ?", (replayed,))

        if replayed > 0:
            self.new_msg_event.set()
            self.new_msg_event.clear()

        return {'status': 'ok', 'message': f'Replayed {replayed} messages'}

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
                        self._requeue_unacked(msg_id)

    def _requeue_unacked(self, msg_id):
        with get_cursor() as cur:
            cur.execute("SELECT delivery_count FROM messages WHERE id = ?", (msg_id,))
            row = cur.fetchone()
            if row:
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
                else:
                    cur.execute("""
                        UPDATE messages SET status = 'ready', delivery_count = ?, available_at = ?
                        WHERE id = ?
                    """, (new_count, time.time(), msg_id))

    def _delivery_loop(self):
        while self.running:
            try:
                delivered = self._try_deliver()
                if not delivered:
                    self.new_msg_event.wait(timeout=0.5)
            except Exception as e:
                time.sleep(0.5)

    def _try_deliver(self):
        delivered = False
        now = time.time()

        with self.consumers_lock:
            consumer_list = []
            for conn, consumers in self.consumers.items():
                for c in consumers:
                    consumer_list.append((conn, c))

        for conn, consumer in consumer_list:
            if not consumer.can_deliver():
                continue

            queue_id, _ = self.queue_mgr.get_queue_id(consumer.queue_name)
            if queue_id is None:
                continue

            with get_cursor() as cur:
                cur.execute("""
                    SELECT * FROM messages
                    WHERE queue_id = ? AND status = 'ready' AND available_at <= ?
                    AND (expires_at = 0 OR expires_at > ?)
                    ORDER BY priority DESC, id ASC
                    LIMIT 1
                """, (queue_id, now, now))
                row = cur.fetchone()

                if row:
                    msg_id = row['id']
                    cur.execute("UPDATE messages SET status = 'unacked' WHERE id = ?", (msg_id,))

                    msg_data = {
                        'type': 'message',
                        'message_id': msg_id,
                        'body': row['body'],
                        'priority': row['priority'],
                        'correlation_id': row['correlation_id'],
                        'reply_to': row['reply_to'],
                        'delivery_count': row['delivery_count'] + 1,
                    }

                    try:
                        conn.sendall(encode_message(msg_data))
                        if consumer.ack_mode == 'manual':
                            consumer.add_unacked(msg_id)
                        else:
                            cur.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                            cur.execute("""
                                INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                                VALUES (?, ?, ?, 'consume', ?)
                            """, (msg_id, queue_id, row['body'], now))
                            cur.execute("UPDATE stats SET total_consumed = total_consumed + 1, total_acked = total_acked + 1")
                        delivered = True
                    except Exception:
                        cur.execute("UPDATE messages SET status = 'ready' WHERE id = ?", (msg_id,))

        return delivered

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
                SELECT id, queue_id, body FROM messages
                WHERE status = 'ready' AND expires_at > 0 AND expires_at <= ?
            """, (now,))
            rows = cur.fetchall()
            for row in rows:
                cur.execute("DELETE FROM messages WHERE id = ?", (row['id'],))
                cur.execute("""
                    INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                    VALUES (?, ?, ?, 'expired', ?)
                """, (row['id'], row['queue_id'], row['body'], now))

    def _check_ack_timeout(self):
        now = time.time()
        with get_cursor() as cur:
            cur.execute("""
                SELECT id, queue_id, body, delivery_count FROM messages
                WHERE status = 'unacked' AND (available_at + ?) <= ?
            """, (ACK_TIMEOUT, now))
            rows = cur.fetchall()
            for row in rows:
                msg_id = row['id']
                new_count = row['delivery_count'] + 1
                if new_count >= MAX_DELIVERY_COUNT:
                    dlq_id, _ = self.queue_mgr.get_queue_id(DLQ_QUEUE_NAME)
                    if dlq_id:
                        cur.execute("""
                            INSERT INTO messages (queue_id, body, priority, ttl, correlation_id, reply_to,
                                                 delay, available_at, expires_at, status, delivery_count, created_at)
                            VALUES (?, ?, 0, 0, '', '', 0, ?, 0, 'ready', 0, ?)
                        """, (dlq_id, row['body'], now, now))
                    cur.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                    cur.execute("""
                        INSERT INTO message_history (message_id, queue_id, body, action, timestamp)
                        VALUES (?, ?, ?, 'timeout_dlq', ?)
                    """, (msg_id, row['queue_id'], row['body'], now))
                    cur.execute("UPDATE stats SET total_nacked = total_nacked + 1")
                else:
                    cur.execute("""
                        UPDATE messages SET status = 'ready', delivery_count = ?, available_at = ?
                        WHERE id = ?
                    """, (new_count, now, msg_id))

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
