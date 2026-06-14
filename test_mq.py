#!/usr/bin/env python3
import sys
import os
import time
import json
import threading
import socket

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import send_message, recv_message

BROKER_HOST = 'localhost'
BROKER_PORT = 9465


def test_connection():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((BROKER_HOST, BROKER_PORT))
        sock.close()
        return True
    except Exception:
        return False


def send_cmd(cmd_data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((BROKER_HOST, BROKER_PORT))
    try:
        send_message(sock, cmd_data)
        resp = recv_message(sock)
        return resp
    finally:
        sock.close()


def run_tests():
    print("=" * 60)
    print("  Running MQ System Tests")
    print("=" * 60)

    if not test_connection():
        print("ERROR: Broker is not running!")
        print("Start broker first: python3 broker.py")
        sys.exit(1)
    print("[✓] Broker connection OK")

    print("\n--- Test 1: Queue Management ---")

    resp = send_cmd({'command': 'create-queue', 'name': 'test-queue-1', 'durable': True, 'max_length': 0})
    print(f"  Create queue test-queue-1: {resp.get('message')}")

    resp = send_cmd({'command': 'create-queue', 'name': 'test-queue-2', 'durable': True, 'max_length': 100})
    print(f"  Create queue test-queue-2: {resp.get('message')}")

    resp = send_cmd({'command': 'list-queues'})
    queues = resp.get('queues', [])
    print(f"  List queues: {len(queues)} queues found")
    for q in queues:
        print(f"    - {q['name']}: ready={q['ready_messages']}, durable={q['durable']}")

    print("\n--- Test 2: Publish & Subscribe (auto ack) ---")

    for i in range(5):
        body = json.dumps({'id': i, 'message': f'Hello {i}'})
        resp = send_cmd({
            'command': 'publish',
            'exchange': '',
            'routing_key': 'test-queue-1',
            'body': body,
            'priority': 0,
            'ttl': 0,
            'delay': 0,
        })

    resp = send_cmd({'command': 'list-queues'})
    queues = {q['name']: q for q in resp.get('queues', [])}
    print(f"  Published 5 messages to test-queue-1, ready count: {queues['test-queue-1']['ready_messages']}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((BROKER_HOST, BROKER_PORT))
    send_message(sock, {
        'command': 'subscribe',
        'queue': 'test-queue-1',
        'prefetch_count': 0,
        'ack_mode': 'auto',
    })

    received = 0
    sock.settimeout(2)
    try:
        for _ in range(5):
            msg = recv_message(sock)
            if msg and msg.get('type') == 'message':
                received += 1
    except socket.timeout:
        pass
    sock.close()

    print(f"  Consumed {received} messages with auto-ack")

    resp = send_cmd({'command': 'list-queues'})
    queues = {q['name']: q for q in resp.get('queues', [])}
    print(f"  After consume, test-queue-1 ready count: {queues['test-queue-1']['ready_messages']}")

    print("\n--- Test 3: Message Properties ---")

    body = json.dumps({'test': 'priority'})
    resp = send_cmd({
        'command': 'publish',
        'routing_key': 'test-queue-2',
        'body': body,
        'priority': 5,
        'correlation_id': 'corr-123',
        'reply_to': 'reply-queue',
        'ttl': 3600,
    })
    print(f"  Publish with properties: {resp.get('message')}")

    print("\n--- Test 4: Exchange & Binding ---")

    resp = send_cmd({'command': 'create-exchange', 'name': 'logs', 'type': 'fanout'})
    print(f"  Create fanout exchange 'logs': {resp.get('message')}")

    resp = send_cmd({'command': 'create-exchange', 'name': 'direct-ex', 'type': 'direct'})
    print(f"  Create direct exchange 'direct-ex': {resp.get('message')}")

    resp = send_cmd({'command': 'create-exchange', 'name': 'topic-ex', 'type': 'topic'})
    print(f"  Create topic exchange 'topic-ex': {resp.get('message')}")

    resp = send_cmd({'command': 'bind', 'exchange': 'logs', 'queue': 'test-queue-1', 'routing_key': ''})
    print(f"  Bind test-queue-1 to logs: {resp.get('message')}")

    resp = send_cmd({'command': 'bind', 'exchange': 'logs', 'queue': 'test-queue-2', 'routing_key': ''})
    print(f"  Bind test-queue-2 to logs: {resp.get('message')}")

    resp = send_cmd({
        'command': 'publish',
        'exchange': 'logs',
        'routing_key': 'any',
        'body': json.dumps({'log': 'test fanout'}),
    })
    print(f"  Publish to fanout exchange: {resp.get('message')}")

    resp = send_cmd({'command': 'list-queues'})
    queues = {q['name']: q for q in resp.get('queues', [])}
    print(f"  After fanout publish: test-queue-1 ready={queues['test-queue-1']['ready_messages']}, test-queue-2 ready={queues['test-queue-2']['ready_messages']}")

    resp = send_cmd({'command': 'bind', 'exchange': 'topic-ex', 'queue': 'test-queue-1', 'routing_key': '*.error'})
    print(f"  Bind test-queue-1 to topic-ex with *.error: {resp.get('message')}")

    resp = send_cmd({
        'command': 'publish',
        'exchange': 'topic-ex',
        'routing_key': 'app.error',
        'body': json.dumps({'topic': 'test'}),
    })
    print(f"  Publish to topic exchange: {resp.get('message')}")

    print("\n--- Test 5: Manual ACK / NACK ---")

    resp = send_cmd({'command': 'purge-queue', 'name': 'test-queue-2'})
    print(f"  Purge test-queue-2: {resp.get('message')}")

    for i in range(3):
        send_cmd({
            'command': 'publish',
            'routing_key': 'test-queue-2',
            'body': json.dumps({'item': i}),
        })

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((BROKER_HOST, BROKER_PORT))
    send_message(sock, {
        'command': 'subscribe',
        'queue': 'test-queue-2',
        'prefetch_count': 2,
        'ack_mode': 'manual',
    })

    sock.settimeout(2)
    msg1 = recv_message(sock)
    msg2 = recv_message(sock)
    print(f"  Got {bool(msg1)} and {bool(msg2)} messages with prefetch=2")

    if msg1:
        msg_id = msg1.get('message_id')
        send_message(sock, {'command': 'ack', 'message_id': msg_id})
        ack_resp = recv_message(sock)
        print(f"  Ack message {msg_id}: {ack_resp.get('message')}")

    if msg2:
        msg_id = msg2.get('message_id')
        send_message(sock, {'command': 'nack', 'message_id': msg_id, 'requeue': True})
        nack_resp = recv_message(sock)
        print(f"  Nack+requeue message {msg_id}: {nack_resp.get('message')}")

    sock.close()

    print("\n--- Test 6: Dead Letter Queue ---")

    resp = send_cmd({'command': 'list-queues'})
    queues = {q['name']: q for q in resp.get('queues', [])}
    dlq_count = queues.get('dead-letter', {}).get('ready_messages', 0)
    print(f"  Dead-letter queue ready messages: {dlq_count}")
    print(f"  (DLQ auto-created: {'dead-letter' in queues})")

    print("\n--- Test 7: Broker Status ---")

    resp = send_cmd({'command': 'status'})
    broker = resp.get('broker', {})
    print(f"  Uptime: {broker.get('uptime_formatted')}")
    print(f"  Total queues: {broker.get('total_queues')}")
    print(f"  Total published: {broker.get('total_published')}")
    print(f"  Total consumed: {broker.get('total_consumed')}")
    print(f"  Total acked: {broker.get('total_acked')}")
    print(f"  Total nacked: {broker.get('total_nacked')}")
    print(f"  Consume rate: {broker.get('consume_rate')} msg/s")

    print("\n--- Test 8: Message Replay ---")

    resp = send_cmd({'command': 'replay', 'queue': 'test-queue-1', 'count': 2})
    print(f"  Replay 2 messages to test-queue-1: {resp.get('message')}")

    print("\n--- Test 9: Purge & Delete ---")

    resp = send_cmd({'command': 'purge-queue', 'name': 'test-queue-1'})
    print(f"  Purge test-queue-1: {resp.get('message')}")

    resp = send_cmd({'command': 'delete-queue', 'name': 'test-queue-1'})
    print(f"  Delete test-queue-1: {resp.get('message')}")

    resp = send_cmd({'command': 'list-queues'})
    queues = resp.get('queues', [])
    print(f"  Remaining queues: {len(queues)}")

    print("\n" + "=" * 60)
    print("  All tests completed!")
    print("=" * 60)


if __name__ == '__main__':
    run_tests()
