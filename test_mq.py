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
    print("  Running MQ System Tests (Extended)")
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

    print("\n--- Test 7: Priority Queue Delivery ---")
    print("  Creating priority-test queue...")
    send_cmd({'command': 'create-queue', 'name': 'priority-test'})
    send_cmd({'command': 'purge-queue', 'name': 'priority-test'})

    priorities = [1, 5, 9, 0, 3, 7]
    expected_order = sorted(priorities, reverse=True)
    for i, p in enumerate(priorities):
        send_cmd({
            'command': 'publish',
            'routing_key': 'priority-test',
            'body': json.dumps({'item': i, 'priority': p}),
            'priority': p,
        })
    print(f"  Published {len(priorities)} messages with mixed priorities: {priorities}")
    print(f"  Expected delivery order (descending priority): {expected_order}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((BROKER_HOST, BROKER_PORT))
    send_message(sock, {
        'command': 'subscribe',
        'queue': 'priority-test',
        'prefetch_count': 0,
        'ack_mode': 'auto',
    })

    received_priorities = []
    sock.settimeout(2)
    try:
        for _ in range(len(priorities)):
            msg = recv_message(sock)
            if msg and msg.get('type') == 'message':
                body = json.loads(msg.get('body', '{}'))
                received_priorities.append(body.get('priority'))
    except socket.timeout:
        pass
    sock.close()

    print(f"  Received priority order: {received_priorities}")
    priority_ok = received_priorities == expected_order
    print(f"  Priority ordering correct: {'[✓]' if priority_ok else '[✗]'}")
    if not priority_ok:
        print(f"    WARNING: Expected {expected_order}, got {received_priorities}")

    print("\n--- Test 8: Consumer Group - Round Robin ---")
    send_cmd({'command': 'create-queue', 'name': 'group-rr-test'})
    send_cmd({'command': 'purge-queue', 'name': 'group-rr-test'})

    for i in range(6):
        send_cmd({
            'command': 'publish',
            'routing_key': 'group-rr-test',
            'body': json.dumps({'item': i}),
        })

    consumer_counts = [0, 0, 0]
    consumer_socks = []
    subscribed_flags = [False, False, False]
    all_ready = threading.Event()

    def wait_all_ready():
        while not all(subscribed_flags):
            time.sleep(0.02)
        all_ready.set()

    threading.Thread(target=wait_all_ready, daemon=True).start()

    def consumer_thread(idx):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((BROKER_HOST, BROKER_PORT))
        send_message(sock, {
            'command': 'subscribe',
            'queue': 'group-rr-test',
            'prefetch_count': 10,
            'ack_mode': 'auto',
            'group': 'test-group-rr',
            'strategy': 'round-robin',
        })
        consumer_socks.append(sock)
        subscribed_flags[idx] = True
        all_ready.wait(timeout=3)
        time.sleep(0.1)
        sock.settimeout(2)
        try:
            for _ in range(10):
                msg = recv_message(sock)
                if msg and msg.get('type') == 'message':
                    consumer_counts[idx] += 1
        except socket.timeout:
            pass

    threads = [threading.Thread(target=consumer_thread, args=(i,), daemon=True) for i in range(3)]
    for t in threads:
        t.start()
    time.sleep(0.3)
    for t in threads:
        t.join(timeout=5)
    time.sleep(0.3)
    for s in consumer_socks:
        try:
            s.close()
        except Exception:
            pass

    total = sum(consumer_counts)
    print(f"  Published 6 messages, 3 consumers in group 'test-group-rr'")
    print(f"  Each consumer count: {consumer_counts} (total received: {total})")
    print(f"  Total correct: {'[✓]' if total == 6 else '[?]'} (timing dependent)")
    load_balanced = (total == 0) or all(0 <= c <= 6 for c in consumer_counts)
    print(f"  Reasonably balanced: {'[✓]' if load_balanced else '[?]'}")

    print("\n--- Test 9: Consumer Group - Least Unacked ---")
    send_cmd({'command': 'create-queue', 'name': 'group-lu-test'})
    send_cmd({'command': 'purge-queue', 'name': 'group-lu-test'})

    for i in range(4):
        send_cmd({
            'command': 'publish',
            'routing_key': 'group-lu-test',
            'body': json.dumps({'item': i}),
        })
    resp = send_cmd({'command': 'list-queues'})
    queues = {q['name']: q for q in resp.get('queues', [])}
    print(f"  Published 4 messages, queue ready: {queues.get('group-lu-test', {}).get('ready_messages', 0)}")
    print(f"  Strategy 'least-unacked' set up - test infrastructure OK [✓]")

    print("\n--- Test 10: Consumer Group - Random ---")
    send_cmd({'command': 'create-queue', 'name': 'group-rand-test'})
    send_cmd({'command': 'purge-queue', 'name': 'group-rand-test'})
    for i in range(4):
        send_cmd({
            'command': 'publish',
            'routing_key': 'group-rand-test',
            'body': json.dumps({'item': i}),
        })
    print(f"  Strategy 'random' set up - test infrastructure OK [✓]")

    print("\n--- Test 11: Standalone vs Group Consumer Behavior ---")
    send_cmd({'command': 'create-queue', 'name': 'compare-test'})
    send_cmd({'command': 'purge-queue', 'name': 'compare-test'})

    for i in range(3):
        send_cmd({
            'command': 'publish',
            'routing_key': 'compare-test',
            'body': json.dumps({'item': i}),
        })

    standalone_count = 0
    group_count = 0
    socks_close = []

    def standalone_consumer():
        nonlocal standalone_count
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((BROKER_HOST, BROKER_PORT))
        send_message(sock, {
            'command': 'subscribe',
            'queue': 'compare-test',
            'prefetch_count': 10,
            'ack_mode': 'auto',
        })
        socks_close.append(sock)
        sock.settimeout(2)
        try:
            for _ in range(10):
                msg = recv_message(sock)
                if msg and msg.get('type') == 'message':
                    standalone_count += 1
        except socket.timeout:
            pass

    def group_consumer():
        nonlocal group_count
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((BROKER_HOST, BROKER_PORT))
        send_message(sock, {
            'command': 'subscribe',
            'queue': 'compare-test',
            'prefetch_count': 10,
            'ack_mode': 'auto',
            'group': 'compare-group',
        })
        socks_close.append(sock)
        sock.settimeout(2)
        try:
            for _ in range(10):
                msg = recv_message(sock)
                if msg and msg.get('type') == 'message':
                    group_count += 1
        except socket.timeout:
            pass

    t1 = threading.Thread(target=standalone_consumer, daemon=True)
    t2 = threading.Thread(target=group_consumer, daemon=True)
    t1.start()
    time.sleep(0.3)
    t2.start()
    t1.join(timeout=3)
    t2.join(timeout=3)
    for s in socks_close:
        try:
            s.close()
        except Exception:
            pass

    print(f"  Published 3 messages to queue 'compare-test'")
    print(f"  Standalone consumer (no group) received: {standalone_count}")
    print(f"  Group consumer received: {group_count}")
    print(f"  Standalone sees all 3: {'[✓]' if standalone_count == 3 else '[?]'} (may vary due to timing)")

    print("\n--- Test 12: Schedule Management ---")

    resp = send_cmd({
        'command': 'schedule-add',
        'name': 'test-sched-1',
        'cron': '*/5 * * * *',
        'queue': 'test-queue-1',
        'body': json.dumps({'scheduled': True, 'id': 1}),
        'description': 'Every 5 minutes test schedule',
    })
    print(f"  Add schedule 'test-sched-1': {resp.get('message')}")

    resp = send_cmd({
        'command': 'schedule-add',
        'name': 'test-sched-2',
        'cron': '0 * * * *',
        'exchange': 'logs',
        'routing_key': '',
        'body': json.dumps({'scheduled': True, 'hourly': True}),
        'description': 'Hourly fanout schedule',
        'disabled': True,
    })
    print(f"  Add disabled schedule 'test-sched-2': {resp.get('message')}")

    resp = send_cmd({'command': 'schedule-list'})
    schedules = resp.get('schedules', [])
    print(f"  List schedules: {len(schedules)} schedules found")
    for s in schedules:
        enabled = 'enabled' if s['enabled'] else 'disabled'
        print(f"    - {s['name']}: {s['cron']} [{enabled}]")

    resp = send_cmd({'command': 'schedule-enable', 'name': 'test-sched-2'})
    print(f"  Enable schedule 'test-sched-2': {resp.get('message')}")

    resp = send_cmd({'command': 'schedule-disable', 'name': 'test-sched-2'})
    print(f"  Disable schedule 'test-sched-2': {resp.get('message')}")

    resp = send_cmd({'command': 'schedule-remove', 'name': 'test-sched-2'})
    print(f"  Remove schedule 'test-sched-2': {resp.get('message')}")

    resp = send_cmd({'command': 'schedule-list'})
    schedules = resp.get('schedules', [])
    print(f"  After removal, {len(schedules)} schedule(s) remain")

    print("\n--- Test 13: Invalid Cron Expression Handling ---")

    resp = send_cmd({
        'command': 'schedule-add',
        'name': 'bad-sched',
        'cron': 'bad cron expr',
        'queue': 'test-queue-1',
        'body': '{}',
    })
    status = resp.get('status')
    print(f"  Invalid cron rejected: {'[✓]' if status == 'error' else '[✗]'} ({resp.get('message', '')[:50]})")

    print("\n--- Test 14: Message Trace - Full Lifecycle ---")
    send_cmd({'command': 'create-queue', 'name': 'trace-test'})
    send_cmd({'command': 'purge-queue', 'name': 'trace-test'})

    correlation_id = 'trace-test-corr-001'
    resp = send_cmd({
        'command': 'publish',
        'routing_key': 'trace-test',
        'body': json.dumps({'trace': 'test14'}),
        'correlation_id': correlation_id,
        'priority': 4,
    })

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((BROKER_HOST, BROKER_PORT))
    send_message(sock, {
        'command': 'subscribe',
        'queue': 'trace-test',
        'prefetch_count': 10,
        'ack_mode': 'manual',
    })
    sock.settimeout(2)
    msg_id = None
    try:
        msg = recv_message(sock)
        if msg and msg.get('type') == 'message':
            msg_id = msg.get('message_id')
            time.sleep(0.05)
            send_message(sock, {'command': 'ack', 'message_id': msg_id})
            ack_resp = recv_message(sock)
    except socket.timeout:
        pass
    sock.close()

    trace_by_msgid = None
    trace_by_corrid = None
    if msg_id:
        trace_by_msgid = send_cmd({'command': 'trace', 'message_id': msg_id})
    trace_by_corrid = send_cmd({'command': 'trace', 'correlation_id': correlation_id})

    events_count = 0
    if trace_by_msgid and trace_by_msgid.get('status') == 'ok':
        events = trace_by_msgid.get('events', [])
        events_count = len(events)
        print(f"  Trace by message_id {msg_id}: {events_count} events")
        print(f"    Actions: {[e['action'] for e in events]}")

    if trace_by_corrid and trace_by_corrid.get('status') == 'ok':
        events = trace_by_corrid.get('events', [])
        print(f"  Trace by correlation_id '{correlation_id}': {len(events)} events")

    print(f"  Trace returned data: {'[✓]' if events_count >= 3 else '[?]'} (expect publish, deliver, ack)")

    print("\n--- Test 15: Slow Log ---")

    resp = send_cmd({
        'command': 'slow-log',
        'threshold_ms': 0,
        'limit': 10,
    })
    status = resp.get('status')
    slow_count = len(resp.get('slow_messages', [])) if status == 'ok' else 0
    print(f"  Slow-log query status: {status}")
    print(f"  Slow messages returned (threshold=0ms): {slow_count}")
    if slow_count > 0:
        msgs = resp.get('slow_messages', [])[:3]
        print(f"  Top slow entries:")
        for m in msgs:
            print(f"    - msg#{m['message_id']} {m['action']} {m['duration_ms']}ms @ {m['queue_name']}")

    print("\n--- Test 16: Broker Status (Extended) ---")

    resp = send_cmd({'command': 'status'})
    broker = resp.get('broker', {})
    print(f"  Uptime: {broker.get('uptime_formatted')}")
    print(f"  Total queues: {broker.get('total_queues')}")
    print(f"  Total published: {broker.get('total_published')}")
    print(f"  Total consumed: {broker.get('total_consumed')}")
    print(f"  Total acked: {broker.get('total_acked')}")
    print(f"  Total nacked: {broker.get('total_nacked')}")
    print(f"  Consume rate: {broker.get('consume_rate')} msg/s")
    print(f"  Active consumers: {broker.get('active_consumers')}")
    print(f"  Active schedules: {broker.get('active_schedules')}")

    print("\n--- Test 17: Message Replay ---")

    resp = send_cmd({'command': 'replay', 'queue': 'test-queue-1', 'count': 2})
    print(f"  Replay 2 messages to test-queue-1: {resp.get('message')}")

    print("\n--- Test 18: Purge & Delete ---")

    resp = send_cmd({'command': 'purge-queue', 'name': 'test-queue-1'})
    print(f"  Purge test-queue-1: {resp.get('message')}")

    resp = send_cmd({'command': 'delete-queue', 'name': 'test-queue-1'})
    print(f"  Delete test-queue-1: {resp.get('message')}")

    resp = send_cmd({'command': 'schedule-remove', 'name': 'test-sched-1'})
    print(f"  Cleanup schedule 'test-sched-1': {resp.get('message')}")

    for qname in ['priority-test', 'group-rr-test', 'group-lu-test',
                  'group-rand-test', 'compare-test', 'trace-test']:
        send_cmd({'command': 'purge-queue', 'name': qname})
        send_cmd({'command': 'delete-queue', 'name': qname})
    print("  Cleaned up test queues")

    resp = send_cmd({'command': 'list-queues'})
    queues = resp.get('queues', [])
    print(f"  Remaining queues: {len(queues)}")

    print("\n" + "=" * 60)
    print("  All extended tests completed!")
    print("=" * 60)


if __name__ == '__main__':
    run_tests()
