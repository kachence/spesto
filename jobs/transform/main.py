"""Build `staged.price_observations` from landed data for a given date.

Pipeline per run:
  1. (optional) Upload local `sql/staged/dims/*.csv` to `gs://{bucket}/dims/`.
  2. Load the three dim CSVs from GCS into `staged.retailer_dim`, `staged.category_dim`,
     `staged.ekatte_dim` with WRITE_TRUNCATE.
  3. Ensure `staged.price_observations` exists with the expected schema, partitioning,
     and clustering.
  4. Run the price-observations build SQL, writing the target date partition with
     WRITE_TRUNCATE. Idempotent per date.

Run after `load_to_bigquery` for the same date.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField

SOFIA = ZoneInfo("Europe/Sofia")
log = logging.getLogger("transform")

JOB_DIR = Path(__file__).parent
REPO_DIMS_DIR = JOB_DIR.parent.parent / "sql" / "staged" / "dims"
SQL_DIR = JOB_DIR / "sql"

RETAILER_DIM_SCHEMA = [
    SchemaField("retailer_eik", "STRING", mode="REQUIRED"),
    SchemaField("retailer_brand", "STRING", mode="REQUIRED"),
    SchemaField("retailer_type", "STRING", mode="REQUIRED"),
    SchemaField(
        "is_primary",
        "BOOL",
        mode="REQUIRED",
        description="True for the top consumer supermarket chains in v1 scope. "
        "Matching/consumer UI filters on this; other retailers are ingested and "
        "typed but not shown to consumers.",
    ),
    SchemaField("notes", "STRING"),
]

CATEGORY_DIM_SCHEMA = [
    SchemaField("category_code", "STRING", mode="REQUIRED"),
    SchemaField("category_name_bg", "STRING", mode="REQUIRED"),
    SchemaField("category_name_en", "STRING", mode="REQUIRED"),
    SchemaField("is_grocery", "BOOL", mode="REQUIRED"),
    SchemaField("notes", "STRING"),
]

EKATTE_DIM_SCHEMA = [
    SchemaField("ekatte_code", "STRING", mode="REQUIRED"),
    SchemaField("settlement_name_bg", "STRING", mode="REQUIRED"),
    SchemaField("settlement_name_en", "STRING", mode="REQUIRED"),
    SchemaField("settlement_type", "STRING", mode="REQUIRED"),
    SchemaField("oblast_code", "STRING", mode="REQUIRED"),
    SchemaField("oblast_name_bg", "STRING", mode="REQUIRED"),
    SchemaField("obshtina_name_bg", "STRING", mode="REQUIRED"),
    SchemaField("nuts3_code", "STRING", mode="REQUIRED"),
]

SETTLEMENT_OVERRIDES_SCHEMA = [
    SchemaField("retailer_eik", "STRING", mode="REQUIRED"),
    SchemaField("raw_settlement_code", "STRING", mode="REQUIRED"),
    SchemaField("corrected_ekatte_code", "STRING", mode="REQUIRED"),
    SchemaField("reason", "STRING"),
]

DIM_SPECS = {
    "retailer_dim":          ("retailer_dim.csv",          RETAILER_DIM_SCHEMA),
    "category_dim":          ("category_dim.csv",          CATEGORY_DIM_SCHEMA),
    "ekatte_dim":            ("ekatte_dim.csv",            EKATTE_DIM_SCHEMA),
    "settlement_overrides":  ("settlement_overrides.csv",  SETTLEMENT_OVERRIDES_SCHEMA),
}

PRICE_OBS_SCHEMA = [
    SchemaField("observation_date", "DATE", mode="REQUIRED"),
    SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("retailer_eik", "STRING", mode="REQUIRED"),
    SchemaField("retailer_brand", "STRING"),
    SchemaField("retailer_type", "STRING"),
    SchemaField(
        "is_primary",
        "BOOL",
        description="True when this retailer is in the top-7 consumer chains "
        "(Billa/Kaufland/Lidl/Fantastiko/T Market/Minimart/Metro). Downstream "
        "matching and consumer UI filter on this.",
    ),
    SchemaField(
        "retailer_sku_id",
        "STRING",
        mode="REQUIRED",
        description="sha256 hex of retailer_eik + product_code. Stable across days.",
    ),
    SchemaField(
        "store_id",
        "STRING",
        mode="REQUIRED",
        description="sha256 hex of retailer_eik + normalized settlement_code + store_name.",
    ),
    SchemaField("store_name", "STRING"),
    SchemaField(
        "settlement_code",
        "STRING",
        description="Normalized 5-digit EKATTE code (suffix stripped, leading zeros fixed).",
    ),
    SchemaField("settlement_name_bg", "STRING"),
    SchemaField("oblast_code", "STRING"),
    SchemaField("oblast_name_bg", "STRING"),
    SchemaField("category_code", "STRING"),
    SchemaField("category_name_bg", "STRING"),
    SchemaField("is_grocery", "BOOL"),
    SchemaField("product_name", "STRING"),
    SchemaField("product_code", "STRING"),
    SchemaField("retail_price", "NUMERIC"),
    SchemaField("promo_price", "NUMERIC"),
    SchemaField("has_promo", "BOOL"),
    SchemaField("discount_pct", "NUMERIC"),
    SchemaField("source_file", "STRING"),
    SchemaField("raw_row_hash", "STRING"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date", help="YYYY-MM-DD; default today in Europe/Sofia")
    p.add_argument("--bucket", help="Override the GCS_BUCKET env var")
    p.add_argument(
        "--staged-dataset",
        default=os.environ.get("BQ_DATASET_STAGED", "staged"),
    )
    p.add_argument(
        "--landed-dataset",
        default=os.environ.get("BQ_DATASET_LANDED", "landed_raw"),
    )
    p.add_argument(
        "--skip-dims",
        action="store_true",
        help="Skip the dim load step (only rebuild the fact partition).",
    )
    p.add_argument(
        "--upload-dims",
        action="store_true",
        help="Upload local sql/staged/dims/*.csv to gs://{bucket}/dims/ before loading.",
    )
    return p.parse_args()


def resolve_config(args: argparse.Namespace) -> tuple[dt.date, str, str, str, str]:
    bucket = args.bucket or os.environ.get("GCS_BUCKET")
    if not bucket:
        sys.exit("GCS_BUCKET env var (or --bucket) is required")
    date_str = (
        args.date
        or os.environ.get("INGEST_DATE")
        or dt.datetime.now(SOFIA).date().isoformat()
    )
    try:
        date = dt.date.fromisoformat(date_str)
    except ValueError as e:
        sys.exit(f"Invalid date {date_str!r}: {e}")
    location = os.environ.get("BQ_LOCATION", "EU")
    return date, bucket, args.staged_dataset, args.landed_dataset, location


def upload_dims_to_gcs(storage_client: storage.Client, bucket_name: str) -> None:
    if not REPO_DIMS_DIR.is_dir():
        sys.exit(f"local dims directory not found: {REPO_DIMS_DIR}")
    bucket = storage_client.bucket(bucket_name)
    for table, (filename, _) in DIM_SPECS.items():
        local = REPO_DIMS_DIR / filename
        if not local.is_file():
            sys.exit(f"missing local dim file: {local}")
        blob = bucket.blob(f"dims/{filename}")
        blob.upload_from_filename(str(local), content_type="text/csv")
        log.info("uploaded gs://%s/dims/%s (%d bytes)", bucket_name, filename, local.stat().st_size)


def ensure_dataset(bq: bigquery.Client, project: str, dataset_id: str, location: str) -> None:
    ds = bigquery.Dataset(f"{project}.{dataset_id}")
    ds.location = location
    ds.description = (
        "Typed, normalized, dim-joined data derived from landed_raw. "
        "One row per retailer-store-product-day observation."
    )
    bq.create_dataset(ds, exists_ok=True)


def load_dims(
    bq: bigquery.Client,
    project: str,
    dataset: str,
    bucket: str,
    location: str,
) -> None:
    for table, (filename, schema) in DIM_SPECS.items():
        uri = f"gs://{bucket}/dims/{filename}"
        dest = f"{project}.{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            allow_quoted_newlines=True,
            # BQ's default treats empty fields as NULL, which clashes with REQUIRED
            # STRING columns that legitimately hold "" (e.g. the empty-category dim
            # row). Using `\N` as the null marker — matches PostgreSQL COPY convention
            # and doesn't appear in any of our CSVs.
            null_marker="\\N",
        )
        job = bq.load_table_from_uri(uri, dest, job_config=job_config, location=location)
        job.result()
        log.info("loaded %s <- %s (%d rows)", dest, uri, job.output_rows or 0)


def ensure_price_obs_table(
    bq: bigquery.Client, project: str, dataset: str
) -> None:
    fq = f"{project}.{dataset}.price_observations"
    table = bigquery.Table(fq, schema=PRICE_OBS_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="observation_date"
    )
    table.clustering_fields = ["retailer_eik", "category_code"]
    table.description = (
        "Typed, normalized, dim-joined price observations. One row per "
        "retailer-store-product per ingestion_date (copied to observation_date)."
    )
    bq.create_table(table, exists_ok=True)


def landed_partition_exists(
    bq: bigquery.Client, project: str, landed_dataset: str, date: dt.date, location: str
) -> int:
    sql = (
        f"SELECT COUNT(*) AS row_count "
        f"FROM `{project}.{landed_dataset}.kolkostruva_daily` "
        f"WHERE ingestion_date = @ingestion_date"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ingestion_date", "DATE", date.isoformat())
        ]
    )
    row = next(iter(bq.query(sql, job_config=job_config, location=location).result()))
    return int(row["row_count"])


def run_price_obs_build(
    bq: bigquery.Client,
    project: str,
    staged_dataset: str,
    landed_dataset: str,
    date: dt.date,
    location: str,
) -> None:
    sql_template = (SQL_DIR / "price_observations.sql").read_text()
    sql = sql_template.format(
        project=project,
        staged=staged_dataset,
        landed=landed_dataset,
    )
    partition = date.strftime("%Y%m%d")
    dest = f"{project}.{staged_dataset}.price_observations${partition}"
    job_config = bigquery.QueryJobConfig(
        destination=dest,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # ALLOW_FIELD_ADDITION lets partition-scoped writes introduce new
        # nullable columns (e.g. when we add a dim field). Without it, adding
        # is_primary / future schema tweaks would need ALTER TABLE first.
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        query_parameters=[
            bigquery.ScalarQueryParameter("ingestion_date", "DATE", date.isoformat())
        ],
    )
    log.info("building %s", dest)
    job = bq.query(sql, job_config=job_config, location=location)
    job.result()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    args = parse_args()
    date, bucket, staged_dataset, landed_dataset, location = resolve_config(args)

    storage_client = storage.Client()
    bq = bigquery.Client()
    project = bq.project

    log.info(
        "date=%s project=%s bucket=%s staged=%s landed=%s location=%s",
        date.isoformat(),
        project,
        bucket,
        staged_dataset,
        landed_dataset,
        location,
    )

    rows_in_landed = landed_partition_exists(bq, project, landed_dataset, date, location)
    if rows_in_landed == 0:
        sys.exit(
            f"no rows in {project}.{landed_dataset}.kolkostruva_daily for "
            f"{date.isoformat()} — run load_to_bigquery first"
        )
    log.info("landed partition has %d rows", rows_in_landed)

    ensure_dataset(bq, project, staged_dataset, location)

    if args.upload_dims:
        upload_dims_to_gcs(storage_client, bucket)

    if not args.skip_dims:
        load_dims(bq, project, staged_dataset, bucket, location)
    else:
        log.info("--skip-dims: leaving dim tables as-is")

    ensure_price_obs_table(bq, project, staged_dataset)

    # Count how many rows ended up in the target partition.
    count_sql = (
        f"SELECT COUNT(*) AS row_count "
        f"FROM `{project}.{staged_dataset}.price_observations` "
        f"WHERE observation_date = @observation_date"
    )

    run_price_obs_build(bq, project, staged_dataset, landed_dataset, date, location)

    row = next(iter(
        bq.query(
            count_sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("observation_date", "DATE", date.isoformat())
                ]
            ),
            location=location,
        ).result()
    ))
    log.info(
        "done: %s partition=%s rows=%d",
        f"{project}.{staged_dataset}.price_observations",
        date.strftime("%Y%m%d"),
        int(row["row_count"]),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
