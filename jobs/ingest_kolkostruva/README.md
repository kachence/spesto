# ingest_kolkostruva

Daily fetch of the `kolkostruva.bg` open-data zip; writes the raw archive and the
extracted retailer CSVs into the GCS landed zone.

## GCS layout produced

```
gs://{GCS_BUCKET}/
  raw/{YYYY-MM-DD}.zip                       ← untouched archive as fetched
  extracted/{YYYY-MM-DD}/
    Билла ... _130007884.csv                 ← one CSV per retailer
    Кауфланд ... _131129282.csv
    ...
    _MANIFEST.json                           ← file list, sizes, sha256, source URL, timestamps
    _SUCCESS                                 ← empty sentinel; presence means job completed
```

The `_SUCCESS` marker is the idempotency signal — if it exists, the job skips (unless
`--force`). This makes re-runs and backfills safe.

## Configuration

| Env var                | Default                                              | Notes                                     |
| ---------------------- | ---------------------------------------------------- | ----------------------------------------- |
| `GCS_BUCKET`           | *required*                                           | Landed zone bucket name.                  |
| `INGEST_DATE`          | today in `Europe/Sofia`                              | `YYYY-MM-DD`. CLI `--date` overrides.     |
| `SOURCE_URL_TEMPLATE`  | `https://kolkostruva.bg/opendata_files/{date}.zip`   | `{date}` placeholder is required.         |

CLI flags override env vars:

```
python main.py [--date YYYY-MM-DD] [--bucket NAME] [--force]
```

## Local development

```bash
uv sync
GCS_BUCKET=your-dev-bucket uv run python main.py --date 2026-04-22
```

Authentication uses [Application Default Credentials]. For local runs:

```bash
gcloud auth application-default login
```

On Cloud Run Jobs, the attached service account is used automatically.

## Container build

```bash
docker build -t ingest-kolkostruva .
docker run --rm \
  -e GCS_BUCKET=your-dev-bucket \
  -v ~/.config/gcloud:/home/app/.config/gcloud:ro \
  ingest-kolkostruva
```

## Deploy (Cloud Run Jobs)

TBD via `infra/terraform/`. Manual deployment sketch:

```bash
gcloud run jobs deploy ingest-kolkostruva \
  --source . \
  --region europe-west3 \
  --set-env-vars GCS_BUCKET=spesto-landed \
  --service-account ingest-kolkostruva@PROJECT.iam.gserviceaccount.com

gcloud scheduler jobs create http ingest-kolkostruva-daily \
  --schedule "0 16 * * *" \
  --time-zone "Europe/Sofia" \
  --uri "https://europe-west3-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT/jobs/ingest-kolkostruva:run" \
  --http-method POST \
  --oauth-service-account-email scheduler@PROJECT.iam.gserviceaccount.com
```

Schedule will be revised once the publish time is confirmed.

## Failure modes

- **HTTP 404 on source URL** — file not yet published for the requested date. Script
  exits non-zero; Cloud Scheduler raises an alert. Safe to re-run manually once
  published.
- **Transient network errors** — `httpx` retries the transport 3× automatically.
- **GCS write errors** — `google-cloud-storage` applies its built-in retry policy.

[Application Default Credentials]: https://cloud.google.com/docs/authentication/application-default-credentials
