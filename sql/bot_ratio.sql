CREATE TABLE IF NOT EXISTS pipeline.bot_vs_human_ratio (
    hour TIMESTAMP,
    bot_edits INT,
    human_edits INT,
    bot_ratio NUMERIC
);

TRUNCATE TABLE pipeline.bot_vs_human_ratio;

INSERT INTO pipeline.bot_vs_human_ratio (
    hour,
    bot_edits,
    human_edits,
    bot_ratio
)

WITH hourly_counts AS (
    SELECT
        hour,
        COUNT(*) FILTER (WHERE is_bot IS TRUE)::INT AS bot_edits,
        COUNT(*) FILTER (WHERE is_bot IS NOT TRUE)::INT AS human_edits
    FROM pipeline.stg_wikimedia_events
    WHERE hour IS NOT NULL
    GROUP BY hour
)
SELECT
    hour,
    bot_edits,
    human_edits,
    ROUND(bot_edits::NUMERIC / NULLIF(bot_edits + human_edits, 0), 4) AS bot_ratio
FROM hourly_counts;
