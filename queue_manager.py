import time
import threading
from database import get_cursor


class QueueManager:
    def __init__(self):
        self._lock = threading.Lock()

    def create_queue(self, name, durable=True, max_length=0):
        with self._lock:
            with get_cursor() as cur:
                cur.execute("SELECT id FROM queues WHERE name = ?", (name,))
                if cur.fetchone():
                    return False, "Queue already exists"
                cur.execute(
                    "INSERT INTO queues (name, durable, max_length, created_at) VALUES (?, ?, ?, ?)",
                    (name, 1 if durable else 0, max_length, time.time())
                )
                return True, "Queue created"

    def delete_queue(self, name):
        with self._lock:
            with get_cursor() as cur:
                cur.execute("DELETE FROM queues WHERE name = ?", (name,))
                if cur.rowcount == 0:
                    return False, "Queue not found"
                cur.execute("DELETE FROM bindings WHERE queue_name = ?", (name,))
                return True, "Queue deleted"

    def list_queues(self):
        with get_cursor() as cur:
            cur.execute("""
                SELECT q.id, q.name, q.durable, q.max_length, q.created_at,
                       (SELECT COUNT(*) FROM messages m WHERE m.queue_id = q.id AND m.status = 'ready') as ready_count,
                       (SELECT COUNT(*) FROM messages m WHERE m.queue_id = q.id AND m.status = 'unacked') as unacked_count
                FROM queues q
                ORDER BY q.name
            """)
            rows = cur.fetchall()
            queues = []
            for row in rows:
                queues.append({
                    'name': row['name'],
                    'durable': bool(row['durable']),
                    'max_length': row['max_length'],
                    'ready_messages': row['ready_count'],
                    'unacked_messages': row['unacked_count'],
                    'total_messages': row['ready_count'] + row['unacked_count'],
                    'consumers': 0,
                    'created_at': row['created_at']
                })
            return queues

    def purge_queue(self, name):
        with self._lock:
            with get_cursor() as cur:
                cur.execute("SELECT id FROM queues WHERE name = ?", (name,))
                row = cur.fetchone()
                if not row:
                    return False, "Queue not found"
                queue_id = row['id']
                cur.execute("DELETE FROM messages WHERE queue_id = ?", (queue_id,))
                return True, f"Purged {cur.rowcount} messages"

    def get_queue_id(self, name):
        with get_cursor() as cur:
            cur.execute("SELECT id, max_length FROM queues WHERE name = ?", (name,))
            row = cur.fetchone()
            if row:
                return row['id'], row['max_length']
            return None, 0

    def get_queue_name(self, queue_id):
        with get_cursor() as cur:
            cur.execute("SELECT name FROM queues WHERE id = ?", (queue_id,))
            row = cur.fetchone()
            if row:
                return row['name']
            return None
