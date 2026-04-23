# jobs/

Python batch jobs run as Cloud Run Jobs, triggered by Cloud Scheduler.

Each job is a self-contained subdirectory with its own `pyproject.toml`, `Dockerfile`,
and `main.py`. No shared library yet — when two jobs need the same code, factor it into
`packages/` and convert this directory into a uv workspace.

## Current jobs

- **`ingest_kolkostruva/`** — daily fetch of the `kolkostruva.bg` open-data zip;
  writes raw archive + extracted CSVs to GCS landed zone.
- **`load_to_bigquery/`** — transforms extracted CSVs to NDJSON and loads into
  `landed_raw.kolkostruva_daily` partitioned by `ingestion_date`.

## Planned jobs

- **`transform/`** — landed → staged → prod SQL transformations.
- **`match_products/`** — Claude-driven cross-retailer product matching loop.
- **`sync_to_postgres/`** — nightly sync of `prod.canonical_products` +
  `prod.current_prices` → operational Postgres.
