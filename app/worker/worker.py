import json
import os
import signal
import time

import psycopg2
import redis

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "scaleops")
DB_USER = os.getenv("DB_USER", "scaleops")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
WORK_SECONDS = float(os.getenv("WORK_SECONDS", "2"))
QUEUE_NAME = "jobs"

running = True


def stop(signum, frame):
    global running
    running = False
    print("shutdown signal received, finishing current job", flush=True)


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def connect_db():
    while running:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=3,
            )
            conn.autocommit = True
            return conn
        except Exception as exc:
            print(f"db not ready: {exc}", flush=True)
            time.sleep(2)
    return None


def main():
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=10,
    )
    conn = connect_db()
    print("worker started", flush=True)

    while running and conn is not None:
        try:
            item = r.blpop(QUEUE_NAME, timeout=5)
            if item is None:
                continue

            job = json.loads(item[1])
            print(f"processing job {job['id']}", flush=True)
            time.sleep(WORK_SECONDS)

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE items SET status = 'done', processed_at = now() "
                        "WHERE id = %s",
                        (job["id"],),
                    )
                print(f"finished job {job['id']}", flush=True)
            except psycopg2.Error as exc:
                print(f"db error, requeueing job: {exc}", flush=True)
                r.rpush(QUEUE_NAME, item[1])
                conn = connect_db()
        except redis.RedisError as exc:
            print(f"redis error: {exc}", flush=True)
            time.sleep(2)

    print("worker stopped", flush=True)


if __name__ == "__main__":
    main()