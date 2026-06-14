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
    send_message(sock, sub_data)

    ack_mode = args.ack_mode or 'auto'
    print(f"Subscribed to queue '{args.queue}' (ack_mode={ack_mode}). Press Ctrl+C to exit.")
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

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == '__main__':
    main()
