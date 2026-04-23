# scripts/

Operational scripts that don't belong in any specific job — backfills, ad-hoc
data fixups, one-off migrations.

Not deployed anywhere. Run locally against the live BigQuery project.

## `backfill.py`

Walks a date range and runs `ingest → load → transform` for each day. Idempotent
— safe to re-run, safe to interrupt and resume.

### Before first use

Seed the dim tables once (the backfill script skips dim reloads by default):

```bash
cd jobs/transform
GCS_BUCKET=spesto-landed uv run python main.py --date 2026-04-22 --upload-dims
```

### Running

```bash
# Typical: backfill from the first available date to yesterday
GCS_BUCKET=spesto-landed python scripts/backfill.py --start 2025-10-16 2>&1 | tee backfill.log

# Run in background so your terminal is free (macOS)
GCS_BUCKET=spesto-landed nohup python scripts/backfill.py --start 2025-10-16 \
  > backfill.log 2>&1 &

# Watch progress
tail -f backfill.log
```

Expected wall time: ~60s per date × ~190 days = **2–3 hours** on a normal
connection, mostly waiting on the kolkostruva zip downloads.

### On failure

By default the script continues past a per-date failure (one bad feed day
shouldn't kill the whole backfill). All failures are summarized at the end. To
stop on the first failure instead, pass `--fail-fast`.

Re-run the script with the same `--start`/`--end` to pick up any failed dates
(successful ones are fast-skipped by the per-job idempotency markers).

### Verifying after a run

```bash
# Number of daily partitions landed
bq query --use_legacy_sql=false --location=EU \
  "SELECT observation_date, COUNT(*) AS rows
   FROM \`spesto.staged.price_observations\`
   GROUP BY observation_date ORDER BY observation_date"
```
