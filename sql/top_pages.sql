CREATE TABLE IF NOT EXISTS pipeline.top_pages_per_hour (
    hour TIMESTAMP,
    page_title TEXT,
    edit_count INT,
    rank INT
);

TRUNCATE TABLE pipeline.top_pages_per_hour;

INSERT INTO pipeline.top_pages_per_hour (
    hour,
    page_title,
    edit_count,
    rank
)

WITH page_counts AS (
    SELECT
        hour,
        page_title,
        COUNT(*)::INT AS edit_count
    FROM pipeline.stg_wikimedia_events
    WHERE hour IS NOT NULL
      AND page_title IS NOT NULL
    GROUP BY hour, page_title
),

ranked_pages AS (
    SELECT
        hour,
        page_title,
        edit_count,
        RANK() OVER (
            PARTITION BY hour
            ORDER BY edit_count DESC
        )::INT AS rank
    FROM page_counts
)
SELECT
    hour,
    page_title,
    edit_count,
    rank
FROM ranked_pages
WHERE rank <= 10;
