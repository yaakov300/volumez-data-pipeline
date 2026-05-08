CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.raw_wikimedia_events (
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMP,
    event_type TEXT,
    wiki TEXT,
    user_name TEXT,
    is_bot BOOLEAN,
    raw JSONB,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline.consumer_state (
    id SERIAL PRIMARY KEY,
    last_event_time TIMESTAMP
);

INSERT INTO pipeline.consumer_state (id, last_event_time)
VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;
