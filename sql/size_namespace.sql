CREATE TABLE IF NOT EXISTS pipeline.edit_size_by_namespace (
    namespace INT,
    edit_size_bucket TEXT,
    edit_count INT
);

TRUNCATE TABLE pipeline.edit_size_by_namespace;

INSERT INTO pipeline.edit_size_by_namespace (
    namespace,
    edit_size_bucket,
    edit_count
)
SELECT
    namespace,
    edit_size_bucket,
    COUNT(*)::INT AS edit_count
FROM pipeline.stg_wikimedia_events
WHERE namespace IS NOT NULL
  AND edit_size_bucket IS NOT NULL
GROUP BY namespace, edit_size_bucket;
