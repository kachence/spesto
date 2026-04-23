# transform

Builds `staged.price_observations` from `landed_raw.kolkostruva_daily` for a given date.
Also (re)loads the three dim tables from GCS so the fact table always joins against
the latest dim state.

## Pipeline

```
landed_raw.kolkostruva_daily${date}
           │  (filter garbage rows,
           │   normalize settlement codes + prices,
           │   hash retailer_sku_id + store_id,
           │   join 3 dims,
           │   dedup by raw_row_hash)
           ▼
staged.price_observations${date}         ← WRITE_TRUNCATE on the target partition
```

Dim tables are refreshed from `gs://{bucket}/dims/*.csv`:
- `staged.retailer_dim`
- `staged.category_dim`
- `staged.ekatte_dim`

## Normalization rules applied here

- **Garbage rows dropped** — rows where `settlement_code = 'Населено място'` or
  `category_code = 'Категория'` (CSV-header-as-data leaks).
- **Settlement codes**
  - Sofia district suffixes stripped: `68134-01 → 68134`.
  - Short codes left-padded with zeros: `7079 → 07079`, `151 → 00151`.
  - Extra leading zero trimmed: `068134 → 68134`.
- **Prices** — comma-decimal locale normalised: `2,56 → 2.56`. `SAFE_CAST` to
  NUMERIC, so any un-parseable value becomes NULL rather than failing the job.
- **`retailer_sku_id`** — `sha256(retailer_eik || \x1f || product_code)`, hex.
  Stable across days.
- **`store_id`** — `sha256(retailer_eik || \x1f || normalized settlement_code
  || \x1f || store_name)`, hex. Stable as long as the raw store_name doesn't
  drift between feeds; not a guarantee — `jobs/resolve_stores/` will eventually
  assign canonical store IDs via Google Places.
- **`has_promo`** — true only when `promo_price > 0 AND promo_price < retail_price`.
  Some retailers emit `0.00` as "no promo"; this filter treats that correctly.
- **Dedup** — `QUALIFY ROW_NUMBER() OVER (PARTITION BY raw_row_hash)` picks one row
  per input hash. Shouldn't trigger in practice (landed already 1:1 with source) but
  acts as a safety net.

## Schema: `staged.price_observations`

Partitioned by `observation_date` (DAY), clustered by `retailer_eik, category_code`.

All data columns come from either (a) the landed row, (b) a dim lookup, or
(c) derived in SQL. The full list is in [`main.py`](main.py).

## Configuration

| Env var              | Default              | Notes                                     |
| -------------------- | -------------------- | ----------------------------------------- |
| `GCS_BUCKET`         | *required*           | Bucket where dim CSVs live at `dims/`.    |
| `INGEST_DATE`        | today in Europe/Sofia| `YYYY-MM-DD`. `--date` overrides.         |
| `BQ_DATASET_STAGED`  | `staged`             | Target dataset for this job's outputs.    |
| `BQ_DATASET_LANDED`  | `landed_raw`         | Read-source dataset.                      |
| `BQ_LOCATION`        | `EU`                 | BigQuery multi-region.                    |

CLI:

```
python main.py [--date YYYY-MM-DD] [--bucket NAME]
               [--staged-dataset NAME] [--landed-dataset NAME]
               [--upload-dims] [--skip-dims]
```

- `--upload-dims` — copies local `sql/staged/dims/*.csv` → `gs://{bucket}/dims/`
  before loading into BQ. Handy during development when you've just edited a dim
  CSV. Default off.
- `--skip-dims` — leaves dim tables as-is and only rebuilds the fact partition.
  Useful for quick re-runs of the same date when only the SQL logic changed.

## Local development

One-time dim upload (or pass `--upload-dims` on your first run):

```bash
gcloud storage cp ../../sql/staged/dims/*.csv gs://spesto-landed/dims/
```

Then run:

```bash
uv sync
GCS_BUCKET=spesto-landed uv run python main.py --date 2026-04-22 --upload-dims
```

On re-runs with unchanged dims:

```bash
GCS_BUCKET=spesto-landed uv run python main.py --date 2026-04-22 --skip-dims
```

## Sanity queries after a run

```bash
# Row count + coverage of the partition
bq query --use_legacy_sql=false --location=EU \
  "SELECT observation_date, COUNT(*) rows,
          COUNT(DISTINCT retailer_eik) retailers,
          COUNT(DISTINCT store_id) stores,
          COUNTIF(is_grocery) grocery_rows
   FROM \`spesto.staged.price_observations\`
   WHERE observation_date = '2026-04-22'
   GROUP BY 1"

# Rows that failed to dim-join (flag potential dim gaps)
bq query --use_legacy_sql=false --location=EU \
  "SELECT 'no retailer_brand' issue, COUNT(*) n FROM \`spesto.staged.price_observations\`
     WHERE observation_date='2026-04-22' AND retailer_brand IS NULL
   UNION ALL
   SELECT 'no category_name_bg', COUNT(*) FROM \`spesto.staged.price_observations\`
     WHERE observation_date='2026-04-22' AND category_name_bg IS NULL
   UNION ALL
   SELECT 'no settlement_name_bg', COUNT(*) FROM \`spesto.staged.price_observations\`
     WHERE observation_date='2026-04-22' AND settlement_name_bg IS NULL"

# Price parsing failures — retail_price_raw that didn't cast
bq query --use_legacy_sql=false --location=EU \
  "SELECT COUNT(*) n FROM \`spesto.landed_raw.kolkostruva_daily\` l
   WHERE ingestion_date='2026-04-22'
     AND l.retail_price_raw IS NOT NULL AND l.retail_price_raw != ''
     AND SAFE_CAST(REPLACE(l.retail_price_raw, ',', '.') AS NUMERIC) IS NULL"
```

## Failure modes

- **Landed partition empty** — job fails fast with a clear message telling you to run
  `load_to_bigquery` first.
- **Dim CSV missing in GCS** — BQ load error; re-upload with `--upload-dims` or
  copy via `gcloud storage cp`.
- **Schema drift on a dim** — BQ load fails with a column-mismatch error; update the
  schema in `main.py` to match.

## Deploy sketch (Cloud Run Jobs)

```bash
gcloud run jobs deploy transform \
  --source . \
  --region europe-west3 \
  --set-env-vars GCS_BUCKET=spesto-landed \
  --service-account transform@spesto.iam.gserviceaccount.com
```

Service account needs:
- `roles/storage.objectViewer` on the bucket (to read dim CSVs)
- `roles/bigquery.dataEditor` on both `landed_raw` and `staged` datasets
- `roles/bigquery.jobUser` on the project
