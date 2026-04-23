# load_to_bigquery

Transforms the per-date extracted kolkostruva CSVs into gzipped NDJSON and loads them
into `landed_raw.kolkostruva_daily`. Runs after `ingest_kolkostruva` for the same date.

## Pipeline

```
gs://{bucket}/extracted/{date}/*.csv             (produced by ingest_kolkostruva)
          │
          ▼  parse + enrich with ingestion metadata
gs://{bucket}/processed_ndjson/{date}/{eik}.ndjson.gz    (one file per source CSV)
          │
          ▼  BQ load (WRITE_TRUNCATE on partition)
{project}.landed_raw.kolkostruva_daily${YYYYMMDD}
```

Idempotency markers:

- `extracted/{date}/_SUCCESS` — prerequisite; if missing, this job fails with a clear
  message telling you to run the ingest job first.
- `processed_ndjson/{date}/_SUCCESS` — this job's own marker. Re-runs for the same
  date skip unless `--force`.
- BQ `WRITE_TRUNCATE` on the date partition — re-runs overwrite that one partition,
  leaving other partitions untouched.

## Schema: `landed_raw.kolkostruva_daily`

Partitioned by `ingestion_date` (DAY); clustered by `retailer_eik, category_code`.

| Field                | Type      | Required | Notes                                             |
| -------------------- | --------- | -------- | ------------------------------------------------- |
| `ingestion_date`     | DATE      | yes      | Partition key.                                    |
| `loaded_at`          | TIMESTAMP | yes      | Wall-clock time of this load job.                 |
| `source_file`        | STRING    | yes      | CSV filename within the daily zip.                |
| `retailer_eik`       | STRING    | yes      | Bulgarian EIK; one retailer brand may have many.  |
| `retailer_raw_name`  | STRING    | yes      | As appears in the filename prefix.                |
| `settlement_code`    | STRING    |          | Empirically an EKATTE code, e.g. 68134 = Sofia.   |
| `store_name`         | STRING    |          | `Търговски обект`; readable name + address.       |
| `product_name`       | STRING    |          | `Наименование на продукта`.                       |
| `product_code`       | STRING    |          | Retailer-proprietary SKU — do NOT join across.    |
| `category_code`      | STRING    |          | Shared numeric taxonomy across retailers.         |
| `retail_price_raw`   | STRING    |          | `Цена на дребно`. Cast to NUMERIC in staged.      |
| `promo_price_raw`    | STRING    |          | `Цена в промоция`. Empty string when no promo.    |
| `raw_row_hash`       | STRING    | yes      | sha256 of retailer_eik + joined source row.       |

All data columns are STRING in landed — no type coercion. Casting happens in `staged`.

## Configuration

| Env var       | Default                          | Notes                                       |
| ------------- | -------------------------------- | ------------------------------------------- |
| `GCS_BUCKET`  | *required*                       | Landed-zone bucket.                         |
| `INGEST_DATE` | today in `Europe/Sofia`          | `YYYY-MM-DD`. `--date` overrides.           |
| `BQ_DATASET`  | `landed_raw`                     | `--dataset` overrides.                      |
| `BQ_TABLE`    | `kolkostruva_daily`              | `--table` overrides.                        |
| `BQ_LOCATION` | `EU`                             | BigQuery multi-region. Must match dataset.  |

CLI:

```
python main.py [--date YYYY-MM-DD] [--bucket NAME] [--dataset NAME] [--table NAME] [--force]
```

## Local development

Requires the BigQuery API enabled on the project (one-time):

```bash
gcloud services enable bigquery.googleapis.com --project YOUR-PROJECT
```

Then:

```bash
uv sync
GCS_BUCKET=spesto-landed uv run python main.py --date 2026-04-22
```

ADC authentication uses your `gcloud auth application-default login`. On Cloud Run
Jobs, the attached service account is used.

## Container build

```bash
docker build -t load-to-bigquery .
docker run --rm \
  -e GCS_BUCKET=spesto-landed \
  -v ~/.config/gcloud:/home/app/.config/gcloud:ro \
  load-to-bigquery --date 2026-04-22
```

## Deploy (sketch)

```bash
gcloud run jobs deploy load-to-bigquery \
  --source . \
  --region europe-west3 \
  --set-env-vars GCS_BUCKET=spesto-landed \
  --service-account load-to-bigquery@PROJECT.iam.gserviceaccount.com
```

Service account needs: `roles/storage.objectUser` on the bucket,
`roles/bigquery.dataEditor` on the dataset, `roles/bigquery.jobUser` on the project.

## Inspecting results

Note: `rows` is a reserved keyword in BigQuery GoogleSQL — use `row_count` or
similar as a column alias.

```bash
# One-off row count for a date partition
bq query --use_legacy_sql=false --location=EU \
  "SELECT COUNT(*) AS row_count FROM \`PROJECT.landed_raw.kolkostruva_daily\`
   WHERE ingestion_date = '2026-04-22'"

# Distinct retailers for a date
bq query --use_legacy_sql=false --location=EU \
  "SELECT retailer_eik, ANY_VALUE(retailer_raw_name) AS name, COUNT(*) AS row_count
   FROM \`PROJECT.landed_raw.kolkostruva_daily\`
   WHERE ingestion_date = '2026-04-22'
   GROUP BY retailer_eik ORDER BY row_count DESC"
```

## Failure modes

- **Prerequisite missing** — exits non-zero if `extracted/{date}/_SUCCESS` isn't present.
- **Unexpected CSV header** — strict check against the known 7-column header; any
  mismatch aborts the job. This is a canary for feed format drift.
- **Delimiter** — kolkostruva retailers use a mix of `,` and `;` as CSV delimiters
  (confirmed: `Бакалия 2014 ООД` uses `;` while Billa / Kaufland use `,`). The job
  picks the delimiter by counting occurrences on the header line and logs it when
  it's not the default comma. Unsupported delimiters will surface as a
  header-mismatch error.
- **Whitespace after delimiter** — some retailers (confirmed: `Виа Тракия`) emit
  `"field",   "next field"` with whitespace between the delimiter and the opening
  quote. The reader is configured with `skipinitialspace=True` so this is handled
  transparently.
- **Malformed row** (wrong column count) — aborts the job. Better to notice bad data
  than to silently ingest 208 of 209 files.
- **BQ load errors** — `job.result()` raises; the NDJSON files are already written, so
  you can re-run with `--force` after investigating without re-parsing CSVs (they'll
  be re-uploaded, but the cost is negligible).
