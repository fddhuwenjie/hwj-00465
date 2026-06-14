import sqlite3
import os
import threading
import time
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mq_data.db')

_db_lock = threading.Lock()
_conn = None


def get_connection():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
    return _conn


@contextmanager
def get_cursor():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def init_db():
    with get_cursor() as cur:
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                durable INTEGER DEFAULT 1,
                max_length INTEGER DEFAULT 0,
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                body TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                ttl INTEGER DEFAULT 0,
                correlation_id TEXT,
                reply_to TEXT,
                delay INTEGER DEFAULT 0,
                available_at REAL NOT NULL,
                expires_at REAL DEFAULT 0,
                status TEXT DEFAULT 'ready',
                delivery_count INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_messages_queue_status ON messages(queue_id, status);
            CREATE INDEX IF NOT EXISTS idx_messages_available ON messages(available_at);

            CREATE TABLE IF NOT EXISTS exchanges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL DEFAULT 'direct',
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange_id INTEGER NOT NULL,
                queue_name TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY (exchange_id) REFERENCES exchanges(id) ON DELETE CASCADE,
                UNIQUE(exchange_id, queue_name, routing_key)
            );

            CREATE TABLE IF NOT EXISTS message_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                queue_id INTEGER NOT NULL,
                body TEXT NOT NULL,
                action TEXT NOT NULL,
                timestamp REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_history_queue ON message_history(queue_id);

            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_published INTEGER DEFAULT 0,
                total_consumed INTEGER DEFAULT 0,
                total_acked INTEGER DEFAULT 0,
                total_nacked INTEGER DEFAULT 0,
                start_time REAL NOT NULL
            );
        """)

        cur.execute("SELECT COUNT(*) as cnt FROM stats")
        row = cur.fetchone()
        if row['cnt'] == 0:
            cur.execute("INSERT INTO stats (start_time) VALUES (?)", (time.time(),))

        cur.execute("SELECT COUNT(*) as cnt FROM exchanges WHERE name = ''")
        row = cur.fetchone()
        if row['cnt'] == 0:
            cur.execute(
                "INSERT INTO exchanges (name, type, created_at) VALUES (?, ?, ?)",
                ('', 'direct', time.time())
            )
