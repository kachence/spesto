"""Fetch the daily kolkostruva.bg open-data zip and store raw + extracted CSVs in GCS."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import sys
import zipfile
from zoneinfo import ZoneInfo

import httpx
from google.cloud import storage

SOFIA = ZoneInfo("Europe/Sofia")
DEFAULT_URL_TEMPLATE = "https://kolkostruva.bg/opendata_files/{date}.zip"

log = logging.getLogger("ingest_kolkostruva")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="YYYY-MM-DD; default: today in Europe/Sofia")
    parser.add_argument("--bucket", help="Override the GCS_BUCKET env var")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest even if the _SUCCESS marker already exists",
    )
    return parser.parse_args()


def resolve_config(args: argparse.Namespace) -> tuple[dt.date, str, str]:
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

    url_template = os.environ.get("SOURCE_URL_TEMPLATE", DEFAULT_URL_TEMPLATE)
    source_url = url_template.format(date=date.isoformat())
    return date, bucket, source_url


def already_ingested(client: storage.Client, bucket: str, date: dt.date) -> bool:
    marker = client.bucket(bucket).blob(f"extracted/{date.isoformat()}/_SUCCESS")
    return marker.exists()


def fetch_zip(url: str) -> bytes:
    log.info("fetching %s", url)
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport, timeout=60.0, follow_redirects=True) as client:
        r = client.get(url)
        if r.status_code == 404:
            sys.exit(f"source file not found at {url} (HTTP 404) — may not be published yet")
        r.raise_for_status()
        return r.content


def upload_bytes(
    client: storage.Client,
    bucket: str,
    path: str,
    data: bytes,
    content_type: str,
) -> None:
    blob = client.bucket(bucket).blob(path)
    blob.upload_from_string(data, content_type=content_type)
    log.info("wrote gs://%s/%s (%d bytes)", bucket, path, len(data))


def extract_and_upload(
    client: storage.Client,
    bucket: str,
    date: dt.date,
    zip_bytes: bytes,
) -> list[dict]:
    files: list[dict] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            data = zf.read(info)
            gcs_path = f"extracted/{date.isoformat()}/{info.filename}"
            upload_bytes(client, bucket, gcs_path, data, "text/csv; charset=utf-8")
            files.append(
                {
                    "name": info.filename,
                    "gcs_path": f"gs://{bucket}/{gcs_path}",
                    "size_bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
    return files


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    args = parse_args()
    date, bucket, source_url = resolve_config(args)
    log.info("ingest_date=%s bucket=%s source=%s", date.isoformat(), bucket, source_url)

    client = storage.Client()

    if not args.force and already_ingested(client, bucket, date):
        log.info(
            "skip: _SUCCESS already present for %s (use --force to re-ingest)",
            date.isoformat(),
        )
        return 0

    zip_bytes = fetch_zip(source_url)
    zip_sha = hashlib.sha256(zip_bytes).hexdigest()

    raw_path = f"raw/{date.isoformat()}.zip"
    upload_bytes(client, bucket, raw_path, zip_bytes, "application/zip")

    files = extract_and_upload(client, bucket, date, zip_bytes)

    manifest = {
        "ingestion_date": date.isoformat(),
        "source_url": source_url,
        "fetched_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "raw": {
            "gcs_path": f"gs://{bucket}/{raw_path}",
            "size_bytes": len(zip_bytes),
            "sha256": zip_sha,
        },
        "files": files,
    }
    manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    upload_bytes(
        client,
        bucket,
        f"extracted/{date.isoformat()}/_MANIFEST.json",
        manifest_bytes,
        "application/json",
    )

    upload_bytes(
        client,
        bucket,
        f"extracted/{date.isoformat()}/_SUCCESS",
        b"",
        "text/plain",
    )

    log.info("done: %d files extracted for %s", len(files), date.isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
