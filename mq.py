#!/usr/bin/env python3
import sys
import os
import socket
import time
import json
import argparse
import subprocess
import threading

from protocol import send_message, recv_message

BROKER_HOST = 'localhost'
BROKER_PORT = 9465


def connect_broker():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((BROKER_HOST, BROKER_PORT))
        return sock
    except ConnectionRefusedError:
        print(f"Error: Cannot connect to broker at {BROKER_HOST}:{BROKER_PORT}")
        print("Make sure the broker is running: python mq.py broker start")
        sys.exit(1)


def send_command(cmd_data):
    sock = connect_broker()
    try:
        send_message(sock, cmd_data)
        response = recv_message(sock)
        return response
    finally:
        sock.close()


def cmd_broker_start(args):
    broker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'broker.py')
    if args.daemon:
        proc = subprocess.Popen(
            [sys.executable, broker_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )
        time.sleep(0.5)
        if proc.poll() is None:
            print(f"Broker started in background (PID: {proc.pid})")
        else:
            stdout, stderr = proc.communicate()
            print(f"Broker failed to start: {stderr.decode()}")
    else:
        os.execvp(sys.executable, [sys.executable, broker_path])


def cmd_create_queue(args):
    data = {
        'command': 'create-queue',
        'name': args.name,
        'durable': not args.non_durable,
        'max_length': args.max_length or 0,
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_delete_queue(args):
    data = {'command': 'delete-queue', 'name': args.name}
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_list_queues(args):
    data = {'command': 'list-queues'}
    resp = send_command(data)
    if resp.get('status') != 'ok':
        print(f"Error: {resp.get('message', '')}")
        sys.exit(1)

    queues = resp.get('queues', [])
    if not queues:
        print("No queues")
        return

    print(f"{'Queue Name':<20} {'Ready':>7} {'Unacked':>8} {'Total':>7} {'Consumers':>9} {'Durable':>7}")
    print("-" * 65)
    for q in queues:
        durable = 'yes' if q['durable'] else 'no'
        print(f"{q['name']:<20} {q['ready_messages']:>7} {q['unacked_messages']:>8} {q['total_messages']:>7} {q['consumers']:>9} {durable:>7}")


def cmd_purge_queue(args):
    data = {'command': 'purge-queue', 'name': args.name}
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_create_exchange(args):
    data = {
        'command': 'create-exchange',
        'name': args.name,
        'type': args.type,
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_delete_exchange(args):
    data = {'command': 'delete-exchange', 'name': args.name}
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_list_exchanges(args):
    data = {'command': 'list-exchanges'}
    resp = send_command(data)
    if resp.get('status') != 'ok':
        print(f"Error: {resp.get('message', '')}")
        sys.exit(1)

    exchanges = resp.get('exchanges', [])
    print(f"{'Exchange Name':<20} {'Type':<10}")
    print("-" * 32)
    for ex in exchanges:
        print(f"{ex['name']:<20} {ex['type']:<10}")


def cmd_bind(args):
    data = {
        'command': 'bind',
        'exchange': args.exchange,
        'queue': args.queue,
        'routing_key': args.routing_key or '',
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_unbind(args):
    data = {
        'command': 'unbind',
        'exchange': args.exchange,
        'queue': args.queue,
        'routing_key': args.routing_key or '',
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_publish(args):
    data = {
        'command': 'publish',
        'exchange': args.exchange or '',
        'routing_key': args.routing_key or args.queue or '',
        'body': args.body,
        'priority': args.priority or 0,
        'ttl': args.ttl or 0,
        'correlation_id': args.correlation_id or '',
        'reply_to': args.reply_to or '',
        'delay': args.delay or 0,
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_subscribe(args):
    sock = connect_broker()

    sub_data = {
        'command': 'subscribe',
        'queue': args.queue,
        'prefetch_count': args.prefetch_count or 0,
        'ack_mode': args.ack_mode or 'auto',
    }
    if args.group:
        sub_data['group'] = args.group
    if args.strategy:
        sub_data['strategy'] = args.strategy
    send_message(sock, sub_data)

    ack_mode = args.ack_mode or 'auto'
    group_info = f", group={args.group}" if args.group else ""
    strategy_info = f", strategy={args.strategy}" if args.group else ""
    print(f"Subscribed to queue '{args.queue}' (ack_mode={ack_mode}{group_info}{strategy_info}). Press Ctrl+C to exit.")
    print("-" * 50)

    try:
        while True:
            msg = recv_message(sock)
            if msg is None:
                print("\nConnection closed")
                break

            msg_type = msg.get('type')
            if msg_type == 'message':
                msg_id = msg.get('message_id')
                body = msg.get('body', '')
                delivery_count = msg.get('delivery_count', 1)

                try:
                    parsed = json.loads(body)
                    body_display = json.dumps(parsed, indent=2, ensure_ascii=False)
                except (json.JSONDecodeError, TypeError):
                    body_display = body

                print(f"\n[Message #{msg_id}] (delivery: {delivery_count})")
                print(body_display)

                if ack_mode == 'manual':
                    print("\nEnter 'ack' to acknowledge, 'nack' to reject, 'nack-requeue' to requeue:")
                    try:
                        choice = input("> ").strip().lower()
                    except EOFError:
                        choice = 'ack'

                    if choice == 'ack':
                        ack_data = {'command': 'ack', 'message_id': msg_id}
                        send_message(sock, ack_data)
                        recv_message(sock)
                        print("Acknowledged.")
                    elif choice == 'nack':
                        nack_data = {'command': 'nack', 'message_id': msg_id, 'requeue': False}
                        send_message(sock, nack_data)
                        recv_message(sock)
                        print("Rejected (sent to DLQ if delivery count exceeded).")
                    elif choice == 'nack-requeue':
                        nack_data = {'command': 'nack', 'message_id': msg_id, 'requeue': True}
                        send_message(sock, nack_data)
                        recv_message(sock)
                        print("Rejected and requeued.")
                    else:
                        ack_data = {'command': 'ack', 'message_id': msg_id}
                        send_message(sock, ack_data)
                        recv_message(sock)
                        print("Acknowledged.")

    except KeyboardInterrupt:
        print("\n\nUnsubscribing...")
    finally:
        sock.close()


def cmd_status(args):
    data = {'command': 'status'}
    resp = send_command(data)
    if resp.get('status') != 'ok':
        print(f"Error: {resp.get('message', '')}")
        sys.exit(1)

    broker = resp.get('broker', {})
    print("=" * 50)
    print("  Broker Status")
    print("=" * 50)
    print(f"  Uptime:          {broker.get('uptime_formatted', 'N/A')}")
    print(f"  Total Queues:    {broker.get('total_queues', 0)}")
    print(f"  Active Consumers:{broker.get('active_consumers', 0):>4}")
    print()
    print(f"  Total Messages:  {broker.get('total_messages', 0):>4}")
    print(f"    Ready:         {broker.get('ready_messages', 0):>4}")
    print(f"    Unacked:       {broker.get('unacked_messages', 0):>4}")
    print()
    print(f"  Published:       {broker.get('total_published', 0):>4}")
    print(f"  Consumed:        {broker.get('total_consumed', 0):>4}")
    print(f"  Acked:           {broker.get('total_acked', 0):>4}")
    print(f"  Nacked:          {broker.get('total_nacked', 0):>4}")
    print(f"  Consume Rate:    {broker.get('consume_rate', 0):>4} msg/s")
    print("=" * 50)


def cmd_monitor(args):
    print("Monitoring queues. Press Ctrl+C to exit.")
    print()

    prev_counts = {}
    try:
        while True:
            data = {'command': 'list-queues'}
            resp = send_command(data)
            if resp.get('status') != 'ok':
                print(f"Error: {resp.get('message', '')}")
                time.sleep(1)
                continue

            queues = resp.get('queues', [])
            current_counts = {q['name']: q['total_messages'] for q in queues}

            os.system('clear' if os.name != 'nt' else 'cls')
            print("Queue Monitor (press Ctrl+C to exit)")
            print("=" * 60)
            print(f"{'Queue Name':<20} {'Ready':>7} {'Unacked':>8} {'Total':>7} {'Delta':>7} {'Consumers':>9}")
            print("-" * 60)

            for q in queues:
                prev = prev_counts.get(q['name'], q['total_messages'])
                delta = q['total_messages'] - prev
                delta_str = f"+{delta}" if delta > 0 else str(delta) if delta < 0 else "0"
                print(f"{q['name']:<20} {q['ready_messages']:>7} {q['unacked_messages']:>8} {q['total_messages']:>7} {delta_str:>7} {q['consumers']:>9}")

            prev_counts = current_counts
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nMonitor stopped.")


def cmd_replay(args):
    data = {
        'command': 'replay',
        'queue': args.queue,
        'count': args.count or 10,
        'offset': args.offset or 0,
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_schedule_add(args):
    data = {
        'command': 'schedule-add',
        'name': args.name,
        'cron': args.cron,
        'exchange': args.exchange or '',
        'routing_key': args.routing_key or '',
        'queue': args.queue or '',
        'body': args.body,
        'priority': args.priority or 0,
        'ttl': args.ttl or 0,
        'correlation_id': args.correlation_id or '',
        'reply_to': args.reply_to or '',
        'description': args.description or '',
        'enabled': not args.disabled,
    }
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('next_run'):
        from datetime import datetime
        print(f"  Next run: {datetime.fromtimestamp(resp['next_run']).strftime('%Y-%m-%d %H:%M:%S')}")
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_schedule_remove(args):
    data = {'command': 'schedule-remove', 'name': args.name}
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_schedule_list(args):
    data = {'command': 'schedule-list'}
    resp = send_command(data)
    if resp.get('status') != 'ok':
        print(f"Error: {resp.get('message', '')}")
        sys.exit(1)

    schedules = resp.get('schedules', [])
    if not schedules:
        print("No schedules")
        return

    from datetime import datetime
    print(f"{'Name':<20} {'Enabled':>7} {'Cron':<18} {'Target':<20} {'Next Run':<20}")
    print("-" * 90)
    for s in schedules:
        enabled = 'yes' if s['enabled'] else 'no'
        target = s.get('queue') or (s['exchange'] + '/' + s['routing_key'])
        next_run = ''
        if s.get('next_run_at'):
            next_run = datetime.fromtimestamp(s['next_run_at']).strftime('%Y-%m-%d %H:%M:%S')
        print(f"{s['name']:<20} {enabled:>7} {s['cron']:<18} {target[:20]:<20} {next_run:<20}")
        if s.get('description'):
            print(f"  Description: {s['description']}")


def cmd_schedule_enable(args):
    data = {'command': 'schedule-enable', 'name': args.name}
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_schedule_disable(args):
    data = {'command': 'schedule-disable', 'name': args.name}
    resp = send_command(data)
    print(resp.get('message', ''))
    if resp.get('status') != 'ok':
        sys.exit(1)


def cmd_trace(args):
    data = {'command': 'trace'}
    if args.message_id:
        data['message_id'] = args.message_id
    if args.correlation_id:
        data['correlation_id'] = args.correlation_id

    resp = send_command(data)
    if resp.get('status') != 'ok':
        print(f"Error: {resp.get('message', '')}")
        sys.exit(1)

    events = resp.get('events', [])
    summary = resp.get('summary', {})

    if not events:
        print("No trace events found")
        return

    print("=" * 100)
    print("  Message Trace")
    print("=" * 100)
    if summary:
        print(f"  Total events:    {summary.get('total_events', 0)}")
        print(f"  Total duration:  {summary.get('total_duration_ms', 0)} ms")
        print(f"  First event:     {summary.get('first_event', '')}")
        print(f"  Last event:      {summary.get('last_event', '')}")
        if summary.get('correlation_id'):
            print(f"  Correlation ID:  {summary['correlation_id']}")
    print("=" * 100)
    print(f"{'#':>3} {'Time':<24} {'Action':<18} {'Stage':<20} {'Queue':<15} {'Duration(ms)':>12} {'Consumer'}")
    print("-" * 100)
    for i, e in enumerate(events, 1):
        ts = e.get('timestamp_formatted', '')
        action = e.get('action', '')
        stage = e.get('stage', '') or ''
        queue = e.get('queue_name', '') or ''
        dur = f"{e['duration_ms']:.2f}" if e.get('duration_ms') is not None else '-'
        consumer = ''
        if e.get('consumer_group'):
            consumer = f"{e['consumer_group']}/"
        if e.get('consumer_id'):
            consumer += e['consumer_id']
        print(f"{i:>3} {ts:<24} {action:<18} {stage:<20} {queue[:15]:<15} {dur:>12} {consumer}")
        if e.get('detail'):
            print(f"     Detail: {e['detail']}")
    print("=" * 100)


def cmd_slow_log(args):
    data = {
        'command': 'slow-log',
        'threshold_ms': args.threshold or 1000,
        'limit': args.limit or 50,
    }
    resp = send_command(data)
    if resp.get('status') != 'ok':
        print(f"Error: {resp.get('message', '')}")
        sys.exit(1)

    msgs = resp.get('slow_messages', [])
    threshold = resp.get('threshold_ms', 1000)

    print("=" * 110)
    print(f"  Slow Message Log (threshold: {threshold} ms)")
    print("=" * 110)
    if not msgs:
        print("  No slow messages found")
        print("=" * 110)
        return

    print(f"{'#':>3} {'Message ID':>10} {'Duration(ms)':>13} {'Action':<18} {'Stage':<20} {'Queue':<15} {'Time':<24} {'Consumer'}")
    print("-" * 110)
    for i, m in enumerate(msgs, 1):
        mid = m.get('message_id', '')
        dur = f"{m['duration_ms']:.2f}"
        action = m.get('action', '')
        stage = m.get('stage', '') or ''
        queue = m.get('queue_name', '') or ''
        ts = m.get('timestamp_formatted', '')
        consumer = ''
        if m.get('consumer_group'):
            consumer = f"{m['consumer_group']}/"
        if m.get('consumer_id'):
            consumer += m['consumer_id']
        print(f"{i:>3} {mid:>10} {dur:>13} {action:<18} {stage:<20} {queue[:15]:<15} {ts:<24} {consumer}")
    print("=" * 110)


def main():
    parser = argparse.ArgumentParser(
        description='Local Message Queue System - A RabbitMQ-like message broker',
        prog='mq.py',
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    broker_parser = subparsers.add_parser('broker', help='Broker management')
    broker_sub = broker_parser.add_subparsers(dest='broker_command')
    broker_start = broker_sub.add_parser('start', help='Start the broker')
    broker_start.add_argument('--daemon', '-d', action='store_true', help='Run in background')
    broker_start.set_defaults(func=cmd_broker_start)

    create_q_parser = subparsers.add_parser('create-queue', help='Create a queue')
    create_q_parser.add_argument('name', help='Queue name')
    create_q_parser.add_argument('--non-durable', action='store_true', help='Non-durable queue')
    create_q_parser.add_argument('--max-length', type=int, help='Maximum queue length')
    create_q_parser.set_defaults(func=cmd_create_queue)

    delete_q_parser = subparsers.add_parser('delete-queue', help='Delete a queue')
    delete_q_parser.add_argument('name', help='Queue name')
    delete_q_parser.set_defaults(func=cmd_delete_queue)

    list_q_parser = subparsers.add_parser('list-queues', help='List all queues')
    list_q_parser.set_defaults(func=cmd_list_queues)

    purge_q_parser = subparsers.add_parser('purge-queue', help='Purge all messages from a queue')
    purge_q_parser.add_argument('name', help='Queue name')
    purge_q_parser.set_defaults(func=cmd_purge_queue)

    create_ex_parser = subparsers.add_parser('create-exchange', help='Create an exchange')
    create_ex_parser.add_argument('name', help='Exchange name')
    create_ex_parser.add_argument('--type', '-t', default='direct',
                                  choices=['direct', 'fanout', 'topic'],
                                  help='Exchange type (default: direct)')
    create_ex_parser.set_defaults(func=cmd_create_exchange)

    delete_ex_parser = subparsers.add_parser('delete-exchange', help='Delete an exchange')
    delete_ex_parser.add_argument('name', help='Exchange name')
    delete_ex_parser.set_defaults(func=cmd_delete_exchange)

    list_ex_parser = subparsers.add_parser('list-exchanges', help='List all exchanges')
    list_ex_parser.set_defaults(func=cmd_list_exchanges)

    bind_parser = subparsers.add_parser('bind', help='Bind a queue to an exchange')
    bind_parser.add_argument('exchange', help='Exchange name')
    bind_parser.add_argument('queue', help='Queue name')
    bind_parser.add_argument('--routing-key', '-r', help='Routing key')
    bind_parser.set_defaults(func=cmd_bind)

    unbind_parser = subparsers.add_parser('unbind', help='Unbind a queue from an exchange')
    unbind_parser.add_argument('exchange', help='Exchange name')
    unbind_parser.add_argument('queue', help='Queue name')
    unbind_parser.add_argument('--routing-key', '-r', help='Routing key')
    unbind_parser.set_defaults(func=cmd_unbind)

    pub_parser = subparsers.add_parser('publish', help='Publish a message')
    pub_parser.add_argument('queue', nargs='?', help='Queue name (shortcut for default exchange)')
    pub_parser.add_argument('body', help='Message body (JSON string)')
    pub_parser.add_argument('--exchange', '-e', help='Exchange name')
    pub_parser.add_argument('--routing-key', '-r', help='Routing key')
    pub_parser.add_argument('--priority', '-p', type=int, help='Priority (0-9)')
    pub_parser.add_argument('--ttl', type=int, help='Message TTL in seconds')
    pub_parser.add_argument('--correlation-id', help='Correlation ID')
    pub_parser.add_argument('--reply-to', help='Reply-to queue')
    pub_parser.add_argument('--delay', type=int, help='Delay in seconds before consumable')
    pub_parser.set_defaults(func=cmd_publish)

    sub_parser = subparsers.add_parser('subscribe', help='Subscribe to a queue')
    sub_parser.add_argument('queue', help='Queue name')
    sub_parser.add_argument('--ack-mode', choices=['auto', 'manual'], default='auto',
                            help='Acknowledgment mode (default: auto)')
    sub_parser.add_argument('--prefetch-count', type=int, help='Max unacknowledged messages')
    sub_parser.add_argument('--group', '-g', help='Consumer group name (enables load balancing)')
    sub_parser.add_argument('--strategy', '-s', choices=['round-robin', 'least-unacked', 'random'],
                            default='round-robin', help='Load balancing strategy for consumer groups')
    sub_parser.set_defaults(func=cmd_subscribe)

    status_parser = subparsers.add_parser('status', help='Show broker status')
    status_parser.set_defaults(func=cmd_status)

    monitor_parser = subparsers.add_parser('monitor', help='Monitor queues in real-time')
    monitor_parser.set_defaults(func=cmd_monitor)

    replay_parser = subparsers.add_parser('replay', help='Replay messages from history')
    replay_parser.add_argument('queue', help='Queue name')
    replay_parser.add_argument('--count', '-n', type=int, default=10, help='Number of messages to replay')
    replay_parser.add_argument('--offset', type=int, default=0, help='Offset from latest')
    replay_parser.set_defaults(func=cmd_replay)

    schedule_parser = subparsers.add_parser('schedule', help='Scheduled message management')
    schedule_sub = schedule_parser.add_subparsers(dest='schedule_command')

    sched_add = schedule_sub.add_parser('add', help='Add a scheduled message')
    sched_add.add_argument('name', help='Schedule name')
    sched_add.add_argument('cron', help='Cron expression (5 fields: min hour day month weekday)')
    sched_add.add_argument('body', help='Message body (JSON string)')
    sched_add.add_argument('--queue', help='Target queue name')
    sched_add.add_argument('--exchange', '-e', help='Target exchange name')
    sched_add.add_argument('--routing-key', '-r', help='Routing key')
    sched_add.add_argument('--priority', '-p', type=int, help='Priority (0-9)')
    sched_add.add_argument('--ttl', type=int, help='Message TTL in seconds')
    sched_add.add_argument('--correlation-id', help='Correlation ID')
    sched_add.add_argument('--reply-to', help='Reply-to queue')
    sched_add.add_argument('--description', '-d', help='Schedule description')
    sched_add.add_argument('--disabled', action='store_true', help='Create disabled')
    sched_add.set_defaults(func=cmd_schedule_add)

    sched_rm = schedule_sub.add_parser('remove', aliases=['rm'], help='Remove a scheduled message')
    sched_rm.add_argument('name', help='Schedule name')
    sched_rm.set_defaults(func=cmd_schedule_remove)

    sched_list = schedule_sub.add_parser('list', aliases=['ls'], help='List all scheduled messages')
    sched_list.set_defaults(func=cmd_schedule_list)

    sched_enable = schedule_sub.add_parser('enable', help='Enable a scheduled message')
    sched_enable.add_argument('name', help='Schedule name')
    sched_enable.set_defaults(func=cmd_schedule_enable)

    sched_disable = schedule_sub.add_parser('disable', help='Disable a scheduled message')
    sched_disable.add_argument('name', help='Schedule name')
    sched_disable.set_defaults(func=cmd_schedule_disable)

    trace_parser = subparsers.add_parser('trace', help='Trace message lifecycle')
    trace_group = trace_parser.add_mutually_exclusive_group(required=True)
    trace_group.add_argument('--message-id', '-m', type=int, help='Trace by message ID')
    trace_group.add_argument('--correlation-id', '-c', help='Trace by correlation ID')
    trace_parser.set_defaults(func=cmd_trace)

    slowlog_parser = subparsers.add_parser('slow-log', help='Show slow message log')
    slowlog_parser.add_argument('--threshold', '-t', type=int, default=1000,
                                help='Duration threshold in ms (default: 1000)')
    slowlog_parser.add_argument('--limit', '-n', type=int, default=50,
                                help='Max number of entries (default: 50)')
    slowlog_parser.set_defaults(func=cmd_slow_log)

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == '__main__':
    main()
