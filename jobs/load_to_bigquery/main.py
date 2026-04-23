"""Transform extracted kolkostruva CSVs into gzipped NDJSON and load into BigQuery.

Reads `gs://{bucket}/extracted/{date}/*.csv`, enriches each row with ingestion metadata
(source file, retailer EIK, timestamps, row hash), writes `gs://{bucket}/processed_ndjson/
{date}/{eik}.ndjson.gz`, and loads the lot into `landed_raw.kolkostruva_daily` scoped to
the `ingestion_date` partition (WRITE_TRUNCATE — idempotent per date).

Run after `ingest_kolkostruva` for the same date.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import logging
import os
import re
import sys
from zoneinfo import ZoneInfo

from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField

SOFIA = ZoneInfo("Europe/Sofia")
log = logging.getLogger("load_to_bigquery")

CSV_HEADER: list[str] = [
    "Населено място",
    "Търговски обект",
    "Наименование на продукта",
    "Код на продукта",
    "Категория",
    "Цена на дребно",
    "Цена в промоция",
]

FIELD_MAP: dict[str, str] = {
    "Населено място": "settlement_code",
    "Търговски обект": "store_name",
    "Наименование на продукта": "product_name",
    "Код на продукта": "product_code",
    "Категория": "category_code",
    "Цена на дребно": "retail_price_raw",
    "Цена в промоция": "promo_price_raw",
}

BQ_SCHEMA: list[SchemaField] = [
    SchemaField("ingestion_date", "DATE", mode="REQUIRED"),
    SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField(
        "source_file",
        "STRING",
        mode="REQUIRED",
        description="CSV filename within the daily kolkostruva zip.",
    ),
    SchemaField(
        "retailer_eik",
        "STRING",
        mode="REQUIRED",
        description="Bulgarian EIK extracted from the filename suffix. A single "
        "retailer brand may have multiple EIKs (one per holding company).",
    ),
    SchemaField(
        "retailer_raw_name",
        "STRING",
        mode="REQUIRED",
        description="Retailer display name as it appears in the filename prefix. "
        "Not yet normalized; use retailer_dim in staged for brand grouping.",
    ),
    SchemaField(
        "settlement_code",
        "STRING",
        description="Raw value of `Населено място`; empirically an EKATTE code "
        "(e.g. 68134 = Sofia), not a city name.",
    ),
    SchemaField(
        "store_name",
        "STRING",
        description="Raw value of `Търговски обект`; human-readable store name/address.",
    ),
    SchemaField("product_name", "STRING", description="Raw value of `Наименование на продукта`."),
    SchemaField(
        "product_code",
        "STRING",
        description="Raw value of `Код на продукта`; retailer-proprietary SKU — "
        "do NOT join on this across retailers.",
    ),
    SchemaField(
        "category_code",
        "STRING",
        description="Raw value of `Категория`; shared numeric taxonomy across retailers.",
    ),
    SchemaField(
        "retail_price_raw",
        "STRING",
        description="Raw value of `Цена на дребно`; cast to NUMERIC in staged.",
    ),
    SchemaField(
        "promo_price_raw",
        "STRING",
        description="Raw value of `Цена в промоция`; empty string when no promo.",
    ),
    SchemaField(
        "raw_row_hash",
        "STRING",
        mode="REQUIRED",
        description="sha256 of retailer_eik + joined source-row fields; stable across "
        "re-runs for the same input.",
    ),
]

FILENAME_RE = re.compile(r"^(?P<brand>.+)_(?P<eik>\d{9,13})\.csv$")


def detect_delimiter(text: str) -> str:
    """Pick delimiter by counting occurrences on the header line. Supports `,` and `;`.

    Simpler than `csv.Sniffer`, and more predictable: Sniffer occasionally returns
    non-standard dialects (e.g., unusual quotechar) that break downstream parsing.
    """
    first_line = text.split("\n", 1)[0]
    return ";" if first_line.count(";") > first_line.count(",") else ","


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date", help="YYYY-MM-DD; default: today in Europe/Sofia")
    p.add_argument("--bucket", help="Override the GCS_BUCKET env var")
    p.add_argument("--dataset", help="BQ dataset (default: env BQ_DATASET or 'landed_raw')")
    p.add_argument("--table", help="BQ table (default: env BQ_TABLE or 'kolkostruva_daily')")
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if processed_ndjson/{date}/_SUCCESS already exists",
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

    dataset = args.dataset or os.environ.get("BQ_DATASET", "landed_raw")
    table = args.table or os.environ.get("BQ_TABLE", "kolkostruva_daily")
    location = os.environ.get("BQ_LOCATION", "EU")
    return date, bucket, dataset, table, location


def parse_filename(name: str) -> tuple[str, str]:
    m = FILENAME_RE.match(name)
    if not m:
        raise ValueError(f"filename does not match expected pattern: {name!r}")
    return m.group("brand"), m.group("eik")


def extracted_success_exists(client: storage.Client, bucket: str, date: dt.date) -> bool:
    marker = client.bucket(bucket).blob(f"extracted/{date.isoformat()}/_SUCCESS")
    return marker.exists()


def loaded_success_exists(client: storage.Client, bucket: str, date: dt.date) -> bool:
    marker = client.bucket(bucket).blob(f"processed_ndjson/{date.isoformat()}/_SUCCESS")
    return marker.exists()


def row_hash(retailer_eik: str, raw_values: list[str]) -> str:
    h = hashlib.sha256()
    h.update(retailer_eik.encode("utf-8"))
    h.update(b"\x1f")
    h.update("\x1f".join(raw_values).encode("utf-8"))
    return h.hexdigest()


def transform_csv_to_ndjson_gz(
    csv_bytes: bytes,
    date: dt.date,
    source_file: str,
    retailer_eik: str,
    retailer_raw_name: str,
    loaded_at_iso: str,
) -> tuple[bytes, int]:
    """Parse one CSV into gzipped NDJSON. Returns (gz_bytes, row_count)."""
    out = io.BytesIO()
    rows = 0
    text = csv_bytes.decode("utf-8-sig")
    delimiter = detect_delimiter(text)
    if delimiter != ",":
        log.info("%s: detected delimiter=%r", source_file, delimiter)
    with gzip.GzipFile(fileobj=out, mode="wb", compresslevel=6, mtime=0) as gz:
        # skipinitialspace=True handles feeds like Виа Тракия that emit `,   "value"`
        # (whitespace between delimiter and opening quote).
        reader = csv.reader(io.StringIO(text), delimiter=delimiter, skipinitialspace=True)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"{source_file}: empty file (no header)")
        header = [h.strip() for h in header]
        if header != CSV_HEADER:
            raise ValueError(
                f"{source_file}: unexpected header\n  got:      {header!r}\n  expected: {CSV_HEADER!r}"
            )
        for raw in reader:
            if len(raw) != len(CSV_HEADER):
                raise ValueError(
                    f"{source_file}: row has {len(raw)} fields, expected {len(CSV_HEADER)}: {raw!r}"
                )
            obj = {
                "ingestion_date": date.isoformat(),
                "loaded_at": loaded_at_iso,
                "source_file": source_file,
                "retailer_eik": retailer_eik,
                "retailer_raw_name": retailer_raw_name,
                "raw_row_hash": row_hash(retailer_eik, raw),
            }
            for col, val in zip(CSV_HEADER, raw, strict=True):
                obj[FIELD_MAP[col]] = val
            gz.write(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
            gz.write(b"\n")
            rows += 1
    return out.getvalue(), rows


def ensure_dataset(
    bq: bigquery.Client, project: str, dataset_id: str, location: str
) -> None:
    ds = bigquery.Dataset(f"{project}.{dataset_id}")
    ds.location = location
    ds.description = "Raw landed data: one table per source, 1:1 with source files."
    bq.create_dataset(ds, exists_ok=True)


def ensure_table(
    bq: bigquery.Client, project: str, dataset_id: str, table_id: str
) -> None:
    fq = f"{project}.{dataset_id}.{table_id}"
    table = bigquery.Table(fq, schema=BQ_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_date",
    )
    table.clustering_fields = ["retailer_eik", "category_code"]
    table.description = (
        "Daily kolkostruva.bg open-data price feed, one row per source CSV row. "
        "All data columns are STRING at this layer — typing happens in staged."
    )
    bq.create_table(table, exists_ok=True)


def run_bq_load(
    bq: bigquery.Client,
    project: str,
    dataset_id: str,
    table_id: str,
    date: dt.date,
    source_uri: str,
    location: str,
) -> int:
    partition = date.strftime("%Y%m%d")
    dest = f"{project}.{dataset_id}.{table_id}${partition}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=BQ_SCHEMA,
    )
    log.info("BQ load: %s <- %s", dest, source_uri)
    job = bq.load_table_from_uri(
        source_uri, dest, job_config=job_config, location=location
    )
    job.result()
    return int(job.output_rows or 0)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    args = parse_args()
    date, bucket, dataset, table, location = resolve_config(args)

    storage_client = storage.Client()
    bq = bigquery.Client()
    project = bq.project

    log.info(
        "load_date=%s bucket=%s bq=%s.%s.%s location=%s",
        date.isoformat(),
        bucket,
        project,
        dataset,
        table,
        location,
    )

    if not extracted_success_exists(storage_client, bucket, date):
        sys.exit(
            f"extracted/{date.isoformat()}/_SUCCESS missing in gs://{bucket} — "
            "run ingest_kolkostruva for this date first"
        )

    if not args.force and loaded_success_exists(storage_client, bucket, date):
        log.info(
            "skip: processed_ndjson/%s/_SUCCESS already present (use --force to re-run)",
            date.isoformat(),
        )
        return 0

    ensure_dataset(bq, project, dataset, location)
    ensure_table(bq, project, dataset, table)

    loaded_at_iso = dt.datetime.now(dt.UTC).isoformat()
    bucket_obj = storage_client.bucket(bucket)
    ndjson_prefix = f"processed_ndjson/{date.isoformat()}/"

    total_files = 0
    total_rows = 0
    total_bytes = 0

    blobs = storage_client.list_blobs(bucket, prefix=f"extracted/{date.isoformat()}/")
    for blob in blobs:
        basename = blob.name.rsplit("/", 1)[-1]
        if not basename.endswith(".csv") or basename.startswith("_"):
            continue
        brand, eik = parse_filename(basename)

        csv_bytes = blob.download_as_bytes()
        ndjson_gz, rows = transform_csv_to_ndjson_gz(
            csv_bytes, date, basename, eik, brand, loaded_at_iso
        )

        out_path = f"{ndjson_prefix}{eik}.ndjson.gz"
        bucket_obj.blob(out_path).upload_from_string(
            ndjson_gz, content_type="application/x-ndjson"
        )
        log.info(
            "wrote gs://%s/%s (%d rows, %d bytes gz)",
            bucket,
            out_path,
            rows,
            len(ndjson_gz),
        )
        total_files += 1
        total_rows += rows
        total_bytes += len(ndjson_gz)

    if total_files == 0:
        sys.exit(f"no CSV files found in gs://{bucket}/extracted/{date.isoformat()}/")

    log.info(
        "transformed %d files, %d rows, %d bytes NDJSON total",
        total_files,
        total_rows,
        total_bytes,
    )

    source_uri = f"gs://{bucket}/{ndjson_prefix}*.ndjson.gz"
    rows_loaded = run_bq_load(bq, project, dataset, table, date, source_uri, location)
    log.info(
        "BQ load done: %d rows in %s.%s.%s partition=%s",
        rows_loaded,
        project,
        dataset,
        table,
        date.strftime("%Y%m%d"),
    )

    bucket_obj.blob(f"{ndjson_prefix}_SUCCESS").upload_from_string(
        b"", content_type="text/plain"
    )
    log.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
