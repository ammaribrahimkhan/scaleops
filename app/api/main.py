import json
import math
import os
import socket
import threading
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, HTTPException, Query
from prometheus_fastapi_instrumentator import Instrumentator
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel, Field

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "scaleops")
DB_USER = os.getenv("DB_USER", "scaleops")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
QUEUE_NAME = "jobs"
HOSTNAME = socket.gethostname()

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS items (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);
"""

app = FastAPI(title="ScaleOps API")
Instrumentator().instrument(app).expose(app, include_in_schema=False)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=2,
)

_pool = None
_pool_lock = threading.Lock()


def get_pool():
    """Create the DB pool lazily so the API can start before Postgres is ready."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                pool = ThreadedConnectionPool(
                    1,
                    10,
                    host=DB_HOST,
                    port=DB_PORT,
                    dbname=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    connect_timeout=3,
                )
                conn = pool.getconn()
                try:
                    with conn:
                        with conn.cursor() as cur:
                            cur.execute(SCHEMA_SQL)
                finally:
                    pool.putconn(conn)
                _pool = pool
    return _pool


@contextmanager
def db_cursor():
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn:  # commits on success, rolls back on error
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
    finally:
        pool.putconn(conn, close=bool(conn.closed))


class ItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1")
        redis_client.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"not ready: {exc}")
    return {"status": "ready"}


@app.get("/api/info")
def info():
    return {"pod": HOSTNAME, "version": APP_VERSION}


@app.get("/api/items")
def list_items():
    with db_cursor() as cur:
        cur.execute(
            "SELECT id, name, status, created_at, processed_at "
            "FROM items ORDER BY id DESC LIMIT 20"
        )
        rows = cur.fetchall()
    return {"items": rows, "pod": HOSTNAME}


@app.post("/api/items", status_code=201)
def create_item(item: ItemIn):
    with db_cursor() as cur:
        cur.execute(
            "INSERT INTO items (name) VALUES (%s) "
            "RETURNING id, name, status, created_at",
            (item.name,),
        )
        row = cur.fetchone()
    redis_client.rpush(QUEUE_NAME, json.dumps({"id": row["id"]}))
    return row


@app.get("/api/queue")
def queue_length():
    return {"length": redis_client.llen(QUEUE_NAME)}


@app.get("/api/work")
def work(ms: int = Query(50, ge=1, le=2000)):
    """Burn CPU for `ms` milliseconds. Used to trigger autoscaling."""
    end = time.perf_counter() + ms / 1000
    x = 0.0
    while time.perf_counter() < end:
        x += math.sqrt(x + 1)
    return {"burned_ms": ms, "pod": HOSTNAME}