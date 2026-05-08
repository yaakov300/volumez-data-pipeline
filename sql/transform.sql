CREATE TABLE IF NOT EXISTS pipeline.stg_wikimedia_events (
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMP,
    page_title TEXT,
    wiki TEXT,
    user_name TEXT,
    is_bot BOOLEAN,
    is_anonymous BOOLEAN,
    edit_delta INT,
    edit_size_bucket TEXT,
    namespace INT,
    hour TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline.transform_state (
    id TEXT PRIMARY KEY,
    last_run_time TIMESTAMP
);

INSERT INTO pipeline.transform_state (id, last_run_time)
VALUES ('wikimedia_events', TIMESTAMP '1970-01-01')
ON CONFLICT (id) DO NOTHING;

WITH raw_increment AS (
    SELECT r.*
    FROM pipeline.raw_wikimedia_events r
    WHERE r.ingested_at > (
        SELECT last_run_time
        FROM pipeline.transform_state
        WHERE id = 'wikimedia_events'
    )
), deduped AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.event_id
            ORDER BY r.ingested_at DESC
        ) AS row_num
    FROM raw_increment r
), transformed AS (
    SELECT
        event_id,
        event_time,
        raw ->> 'title' AS page_title,
        wiki,
        user_name,
        is_bot,
        COALESCE(user_name ~ '^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$', false)
            OR COALESCE(user_name ~* '^[0-9a-f:]+:[0-9a-f:]+$', false) AS is_anonymous,
        CASE
            WHEN (raw #>> '{length,new}') ~ '^-?[0-9]+$'
             AND (raw #>> '{length,old}') ~ '^-?[0-9]+$'
            THEN (raw #>> '{length,new}')::INT - (raw #>> '{length,old}')::INT
            ELSE NULL
        END AS edit_delta,
        CASE
            WHEN (raw #>> '{length,new}') ~ '^-?[0-9]+$'
             AND (raw #>> '{length,old}') ~ '^-?[0-9]+$'
             AND ABS((raw #>> '{length,new}')::INT - (raw #>> '{length,old}')::INT) < 50
            THEN 'small'
            WHEN (raw #>> '{length,new}') ~ '^-?[0-9]+$'
             AND (raw #>> '{length,old}') ~ '^-?[0-9]+$'
             AND ABS((raw #>> '{length,new}')::INT - (raw #>> '{length,old}')::INT) <= 500
            THEN 'medium'
            WHEN (raw #>> '{length,new}') ~ '^-?[0-9]+$'
             AND (raw #>> '{length,old}') ~ '^-?[0-9]+$'
            THEN 'large'
            ELSE NULL
        END AS edit_size_bucket,
        CASE
            WHEN (raw ->> 'namespace') ~ '^-?[0-9]+$'
            THEN (raw ->> 'namespace')::INT
            ELSE NULL
        END AS namespace,
        DATE_TRUNC('hour', event_time) AS hour
    FROM deduped
    WHERE row_num = 1
), loaded AS (
    INSERT INTO pipeline.stg_wikimedia_events (
        event_id,
        event_time,
        page_title,
        wiki,
        user_name,
        is_bot,
        is_anonymous,
        edit_delta,
        edit_size_bucket,
        namespace,
        hour
    )
    SELECT
        event_id,
        event_time,
        page_title,
        wiki,
        user_name,
        is_bot,
        is_anonymous,
        edit_delta,
        edit_size_bucket,
        namespace,
        hour
    FROM transformed
    ON CONFLICT (event_id) DO UPDATE SET
        event_time = EXCLUDED.event_time,
        page_title = EXCLUDED.page_title,
        wiki = EXCLUDED.wiki,
        user_name = EXCLUDED.user_name,
        is_bot = EXCLUDED.is_bot,
        is_anonymous = EXCLUDED.is_anonymous,
        edit_delta = EXCLUDED.edit_delta,
        edit_size_bucket = EXCLUDED.edit_size_bucket,
        namespace = EXCLUDED.namespace,
        hour = EXCLUDED.hour
    RETURNING event_id
), checkpoint AS (
    SELECT MAX(ingested_at) AS max_ingested_at
    FROM raw_increment
)
UPDATE pipeline.transform_state s
SET last_run_time = checkpoint.max_ingested_at
FROM checkpoint
WHERE s.id = 'wikimedia_events'
  AND checkpoint.max_ingested_at IS NOT NULL;
