# Wikimedia Data Pipeline

Minimal local pipeline that ingests Wikimedia recent-change events, transforms them in PostgreSQL, and refreshes insight tables with Kestra orchestration.

```text
Wikimedia EventStreams -> raw events -> staging events -> insight tables
```

## Flows

### Ingest

Kestra flow: `data_pipeline.ingest_flow`

The ingest flow runs `scripts/consume_stream.py` in a transient `python:3.11-slim` container. It reads Wikimedia `recentchange` events and writes them to `pipeline.raw_wikimedia_events`.

Key behavior:

- Stores the full source event in `raw JSONB`.
- Extracts basic fields like `event_id`, `event_time`, `event_type`, `wiki`, `user_name`, and `is_bot`.
- Inserts with `ON CONFLICT (event_id) DO NOTHING`, so duplicate delivery is safe.
- Tracks resume state in `pipeline.consumer_state`.
- Commits each batch and checkpoint in the same transaction.

### Transformation

Kestra flow: `data_pipeline.transform_flow`

The transformation flow runs every five minutes and executes `sql/transform.sql`. It reads raw events and writes cleaned rows to `pipeline.stg_wikimedia_events`.

Key behavior:

- Processes only raw rows newer than `pipeline.transform_state.last_run_time`.
- Deduplicates by `event_id`, keeping the latest ingested copy.
- Upserts into staging, so reruns are safe.
- Derives analysis fields like `page_title`, `namespace`, `is_anonymous`, `edit_delta`, `edit_size_bucket`, and hourly buckets.

### Insights

Kestra flow: `data_pipeline.insights_flow`

The insights flow runs every five minutes and refreshes analytical tables from `pipeline.stg_wikimedia_events`.

Insight tables:

- `pipeline.top_pages_per_hour`: top edited pages per hour.
- `pipeline.bot_vs_human_ratio`: hourly bot versus human edit counts.
- `pipeline.edit_wars`: pages with more than 20 edits in a five-minute window.
- `pipeline.edit_size_by_namespace`: edit-size bucket counts by namespace.

Each insight is refreshed with `TRUNCATE` plus `INSERT`, so the output reflects the current staging data.

## Run Locally

Start services:

```sh
make up
```

Open Kestra:

```text
http://localhost:8080
```

Follow logs:

```sh
make logs
```

Stop services:

```sh
make down
```

## Tables

- `pipeline.raw_wikimedia_events`: raw ingested events.
- `pipeline.stg_wikimedia_events`: cleaned, deduplicated events.
- `pipeline.top_pages_per_hour`: page activity by hour.
- `pipeline.bot_vs_human_ratio`: bot and human edit ratios.
- `pipeline.edit_wars`: concentrated edit spikes.
- `pipeline.edit_size_by_namespace`: edit sizes by namespace.
- `pipeline.consumer_state`: ingest resume checkpoint.
- `pipeline.transform_state`: transform checkpoint.
