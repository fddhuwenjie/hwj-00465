import time
import threading
from database import get_cursor


class ExchangeManager:
    def __init__(self):
        self._lock = threading.Lock()

    def create_exchange(self, name, type='direct'):
        if type not in ('direct', 'fanout', 'topic'):
            return False, "Invalid exchange type. Must be direct, fanout, or topic"
        with self._lock:
            with get_cursor() as cur:
                cur.execute("SELECT id FROM exchanges WHERE name = ?", (name,))
                if cur.fetchone():
                    return False, "Exchange already exists"
                cur.execute(
                    "INSERT INTO exchanges (name, type, created_at) VALUES (?, ?, ?)",
                    (name, type, time.time())
                )
                return True, "Exchange created"

    def delete_exchange(self, name):
        with self._lock:
            with get_cursor() as cur:
                cur.execute("DELETE FROM exchanges WHERE name = ?", (name,))
                if cur.rowcount == 0:
                    return False, "Exchange not found"
                return True, "Exchange deleted"

    def list_exchanges(self):
        with get_cursor() as cur:
            cur.execute("SELECT name, type, created_at FROM exchanges ORDER BY name")
            rows = cur.fetchall()
            return [{'name': row['name'] if row['name'] else '(default)',
                     'type': row['type'],
                     'created_at': row['created_at']} for row in rows]

    def bind_queue(self, exchange_name, queue_name, routing_key):
        with self._lock:
            with get_cursor() as cur:
                cur.execute("SELECT id, type FROM exchanges WHERE name = ?", (exchange_name,))
                row = cur.fetchone()
                if not row:
                    return False, "Exchange not found"
                try:
                    cur.execute(
                        "INSERT INTO bindings (exchange_id, queue_name, routing_key, created_at) VALUES (?, ?, ?, ?)",
                        (row['id'], queue_name, routing_key, time.time())
                    )
                except Exception:
                    return False, "Binding already exists"
                return True, "Binding created"

    def unbind_queue(self, exchange_name, queue_name, routing_key):
        with self._lock:
            with get_cursor() as cur:
                cur.execute("SELECT id FROM exchanges WHERE name = ?", (exchange_name,))
                row = cur.fetchone()
                if not row:
                    return False, "Exchange not found"
                cur.execute(
                    "DELETE FROM bindings WHERE exchange_id = ? AND queue_name = ? AND routing_key = ?",
                    (row['id'], queue_name, routing_key)
                )
                if cur.rowcount == 0:
                    return False, "Binding not found"
                return True, "Binding removed"

    def get_bound_queues(self, exchange_name, routing_key):
        with get_cursor() as cur:
            cur.execute("SELECT id, type FROM exchanges WHERE name = ?", (exchange_name,))
            row = cur.fetchone()
            if not row:
                return []

            exchange_id = row['id']
            exchange_type = row['type']

            if exchange_type == 'fanout':
                cur.execute(
                    "SELECT DISTINCT queue_name FROM bindings WHERE exchange_id = ?",
                    (exchange_id,)
                )
                return [r['queue_name'] for r in cur.fetchall()]

            elif exchange_type == 'direct':
                cur.execute(
                    "SELECT DISTINCT queue_name FROM bindings WHERE exchange_id = ? AND routing_key = ?",
                    (exchange_id, routing_key)
                )
                return [r['queue_name'] for r in cur.fetchall()]

            elif exchange_type == 'topic':
                cur.execute(
                    "SELECT queue_name, routing_key FROM bindings WHERE exchange_id = ?",
                    (exchange_id,)
                )
                rows = cur.fetchall()
                matched = set()
                for r in rows:
                    if self._topic_match(r['routing_key'], routing_key):
                        matched.add(r['queue_name'])
                return list(matched)

        return []

    def _topic_match(self, pattern, key):
        pattern_parts = pattern.split('.')
        key_parts = key.split('.')

        i = j = 0
        while i < len(pattern_parts) and j < len(key_parts):
            if pattern_parts[i] == '#':
                if i == len(pattern_parts) - 1:
                    return True
                for k in range(j, len(key_parts)):
                    if self._topic_match('.'.join(pattern_parts[i+1:]), '.'.join(key_parts[k:])):
                        return True
                return False
            elif pattern_parts[i] == '*':
                i += 1
                j += 1
            elif pattern_parts[i] == key_parts[j]:
                i += 1
                j += 1
            else:
                return False

        while i < len(pattern_parts) and pattern_parts[i] == '#':
            i += 1

        return i == len(pattern_parts) and j == len(key_parts)
