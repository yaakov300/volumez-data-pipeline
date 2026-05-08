import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import psycopg2
from psycopg2.extras import Json, execute_values
import requests
from sseclient import SSEClient


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
RECONNECT_SECONDS = int(os.getenv("RECONNECT_SECONDS", "5"))
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "pipeline")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "kestra"),
        "user": os.getenv("POSTGRES_USER", "kestra"),
        "password": os.getenv("POSTGRES_PASSWORD", "kestra"),
    }


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_event_time(event):
    meta_dt = event.get("meta", {}).get("dt")
    if meta_dt:
        return datetime.fromisoformat(meta_dt.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)

    timestamp = event.get("timestamp")
    if timestamp is not None:
        return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).replace(tzinfo=None)

    return utc_now()


def event_id(event):
    meta_id = event.get("meta", {}).get("id")
    if meta_id:
        return str(meta_id)

    wiki = event.get("wiki", "unknown")
    change_id = event.get("id") or event.get("rcid") or event.get("revision", {}).get("new")
    timestamp = event.get("timestamp") or event.get("meta", {}).get("dt") or utc_now().isoformat()
    return f"{wiki}:{change_id}:{timestamp}"


def extract_row(event):
    event_time = parse_event_time(event)
    return (
        event_id(event),
        event_time,
        event.get("type"),
        event.get("wiki"),
        event.get("user"),
        event.get("bot"),
        Json(event),
    )


def connect_db():
    conn = psycopg2.connect(**db_config())
    conn.autocommit = False
    return conn


def table_name(table):
    if not IDENTIFIER_RE.match(POSTGRES_SCHEMA) or not IDENTIFIER_RE.match(table):
        raise ValueError(f"Invalid PostgreSQL identifier: {POSTGRES_SCHEMA}.{table}")
    return f'"{POSTGRES_SCHEMA}"."{table}"'


def get_last_event_time(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT last_event_time FROM {table_name('consumer_state')} WHERE id = 1")
        row = cur.fetchone()
        if row is None:
            cur.execute(f"INSERT INTO {table_name('consumer_state')} (id, last_event_time) VALUES (1, NULL)")
            conn.commit()
            return None
        return row[0]


def stream_url(last_event_time):
    if not last_event_time:
        return STREAM_URL


    resume_from = last_event_time - timedelta(seconds=1)
    since = resume_from.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return f"{STREAM_URL}?{urlencode({'since': since})}"


def insert_batch(conn, batch):
    if not batch:
        return

    max_event_time = max(row[1] for row in batch if row[1] is not None)

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name('raw_wikimedia_events')} (
                event_id,
                event_time,
                event_type,
                wiki,
                user_name,
                is_bot,
                raw
            ) VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            """,
            batch,
        )
        cur.execute(
            f"""
            UPDATE {table_name('consumer_state')}
            SET last_event_time = GREATEST(COALESCE(last_event_time, %s), %s)
            WHERE id = 1
            """,
            (max_event_time, max_event_time),
        )

    conn.commit()
    print(f"Inserted/checkpointed batch of {len(batch)} events through {max_event_time.isoformat()}", flush=True)


def consume_once(conn):
    last_event_time = get_last_event_time(conn)
    url = stream_url(last_event_time)
    print(f"Connecting to Wikimedia stream: {url}", flush=True)

    response = requests.get(
        url,
        stream=True,
        timeout=(10, 90),
        headers={"Accept": "text/event-stream", "User-Agent": "kestra-postgres-local-pipeline/1.0"},
    )
    response.raise_for_status()

    batch = []
    for message in SSEClient(response).events():
        if not message.data:
            continue

        try:
            event = json.loads(message.data)
        except json.JSONDecodeError:
            print("Skipping invalid JSON event", flush=True)
            continue

        batch.append(extract_row(event))

        if len(batch) >= BATCH_SIZE:
            insert_batch(conn, batch)
            batch = []


def main():
    while True:
        conn = None
        try:
            conn = connect_db()
            consume_once(conn)
        except Exception as exc:
            if conn:
                conn.rollback()
            print(f"Consumer error: {exc}. Reconnecting in {RECONNECT_SECONDS}s", flush=True)
            time.sleep(RECONNECT_SECONDS)
        finally:
            if conn:
                conn.close()


if __name__ == "__main__":
    main()
