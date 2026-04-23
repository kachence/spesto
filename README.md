# spesto.bg

Bulgarian supermarket food-price comparison. Daily ingestion of retailer price feeds,
cross-retailer product matching, consumer-facing comparison UI, and B2B price/inflation
analytics.

## Status

Early development. First working piece: daily ingestion of `kolkostruva.bg` open-data
price feeds into a GCS landed zone.

## Repo layout

```
apps/
  web/                     Next.js frontend (Vercel) — not yet scaffolded
  api/                     FastAPI backend (Cloud Run Service) — not yet scaffolded
jobs/                      Python batch jobs for Cloud Run Jobs
  ingest_kolkostruva/      Daily kolkostruva.bg → GCS landed zone
sql/                       BigQuery SQL transformations (landed → staged → prod)
infra/
  terraform/               GCP infrastructure as code
```

## Deployment targets

- **Python jobs** → Cloud Run Jobs, scheduled via Cloud Scheduler.
- **API** → Cloud Run Service.
- **Frontend** → Vercel.
- **Data warehouse** → BigQuery (`landed_raw`, `staged`, `prod` datasets).
- **Operational DB** → Cloud SQL Postgres (small, TTL-bounded, frontend-facing).

## Local development

Each subproject is self-contained with its own dependency manager.

### Python jobs (uv + Python 3.12)

```bash
cd jobs/ingest_kolkostruva
uv sync
GCS_BUCKET=your-bucket uv run python main.py --date 2026-04-22
```

### Frontend / API

Not yet scaffolded.
