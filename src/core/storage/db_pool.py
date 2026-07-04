"""
PostgreSQL connection pool.

psycopg2 connections are not thread-safe, but gunicorn's gthread worker runs
concurrent requests on shared Python objects. This module gives each request
thread its own connection, checked out on first DB access and returned on
Flask teardown. All heavy DB objects (PostgreSQLDatabase, PostgreSQLGraphExporter)
resolve `self.conn` through `checkout()` so they don't need per-instance
connection state.

Statement timeout is set at the pool level so a slow query cannot hold a
worker thread past 15 s.
"""
import threading
from typing import Dict, Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

# Pool sizing: 32 max is well below Aurora / RDS default max_connections (100)
# and leaves headroom for batch jobs and admin tools. Min 2 keeps a warm
# connection per worker.
MIN_CONN = 2
MAX_CONN = 32

# Kill any query that runs longer than 15 s. Prevents a slow query from
# wedging a worker thread indefinitely.
STATEMENT_TIMEOUT_MS = 15000

# Register the psycopg2 UUID adapter once, globally. Safe to call multiple
# times but this ensures it's registered before any pool connection is used.
psycopg2.extras.register_uuid()


class _PoolRegistry:
    """One ThreadedConnectionPool per unique connection string, lazy-init'd
    on first access. Lazy init is important so pools are created in gunicorn
    workers (post-fork), not in the master process."""

    _pools: Dict[str, ThreadedConnectionPool] = {}
    _lock = threading.Lock()

    @classmethod
    def get_pool(cls, conn_string: str) -> ThreadedConnectionPool:
        pool = cls._pools.get(conn_string)
        if pool is not None:
            return pool
        with cls._lock:
            pool = cls._pools.get(conn_string)
            if pool is None:
                pool = ThreadedConnectionPool(
                    minconn=MIN_CONN,
                    maxconn=MAX_CONN,
                    dsn=conn_string,
                    keepalives=1,
                    keepalives_idle=60,
                    keepalives_interval=15,
                    keepalives_count=3,
                    options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
                )
                cls._pools[conn_string] = pool
            return pool


_local = threading.local()


def _thread_conns() -> Dict[str, psycopg2.extensions.connection]:
    conns = getattr(_local, "conns", None)
    if conns is None:
        conns = {}
        _local.conns = conns
    return conns


def checkout(conn_string: str) -> psycopg2.extensions.connection:
    """Return the current thread's connection for conn_string, checking one
    out of the pool on first access within this thread."""
    conns = _thread_conns()
    conn = conns.get(conn_string)
    if conn is not None and not conn.closed:
        return conn

    if conn is not None:
        # Prior conn was closed by server side — hand it back so the pool
        # can drop and refill.
        try:
            _PoolRegistry.get_pool(conn_string).putconn(conn, close=True)
        except Exception:
            pass

    pool = _PoolRegistry.get_pool(conn_string)
    conn = pool.getconn()
    conn.autocommit = False
    conns[conn_string] = conn
    return conn


def discard(conn_string: str) -> None:
    """Forcibly close this thread's connection for conn_string. Use after an
    OperationalError so the next checkout gets a fresh one."""
    conns = _thread_conns()
    conn = conns.pop(conn_string, None)
    if conn is None:
        return
    try:
        _PoolRegistry.get_pool(conn_string).putconn(conn, close=True)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def release_all() -> None:
    """Return every thread-local connection to its pool. Call this at Flask
    request teardown so connections don't leak across requests handled by
    the same worker thread."""
    conns = _thread_conns()
    if not conns:
        return
    for conn_string, conn in list(conns.items()):
        try:
            pool = _PoolRegistry.get_pool(conn_string)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            continue

        if conn.closed:
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
            continue

        # Roll back any transaction still open — a lingering open tx would
        # otherwise poison the next thread that reuses this connection.
        try:
            if conn.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                conn.rollback()
        except Exception:
            pass

        try:
            pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    _local.conns = {}
