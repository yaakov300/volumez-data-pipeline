CREATE TABLE IF NOT EXISTS pipeline.edit_wars (
    page_title TEXT,
    window_start TIMESTAMP,
    edit_count INT
);

TRUNCATE TABLE pipeline.edit_wars;

INSERT INTO pipeline.edit_wars (
    page_title,
    window_start,
    edit_count
)
WITH windowed_edits AS (
    SELECT
        page_title,
        DATE_TRUNC('hour', event_time)
            + FLOOR(EXTRACT(MINUTE FROM event_time) / 5) * INTERVAL '5 minutes' AS window_start,
        COUNT(*)::INT AS edit_count
    FROM pipeline.stg_wikimedia_events
    WHERE event_time IS NOT NULL
      AND page_title IS NOT NULL
    GROUP BY page_title, window_start
)
SELECT
    page_title,
    window_start,
    edit_count
FROM windowed_edits
WHERE edit_count > 20;
