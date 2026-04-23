#!/usr/bin/env python3
"""Backfill the spesto pipeline across a range of dates.

For each date in [--start, --end], runs in sequence:
  1. jobs/ingest_kolkostruva  — fetch the zip + extracted CSVs into GCS
  2. jobs/load_to_bigquery    — NDJSON + BQ partition load
  3. jobs/transform           — staged.price_observations for that date

Dims are NOT loaded by this script. Before running, do a one-off upload manually:
    cd jobs/transform && GCS_BUCKET=spesto-landed uv run python main.py \\
        --date <any-valid-date> --upload-dims

All three jobs are idempotent: re-running a completed date is safe and fast
(ingest skips via _SUCCESS marker, load skips via its own _SUCCESS, transform
WRITE_TRUNCATEs the partition).

The script continues past per-date failures by default so a single bad day (feed
outage, one retailer's corrupted CSV) doesn't abort the whole run. Use --fail-fast
to flip that.

Expected wall time: ~60s per date × N dates, bottlenecked on kolkostruva download.
189 days ≈ 2–3 hours. Run in a screen/tmux/nohup session so it survives your shell.

Usage:
    GCS_BUCKET=spesto-landed python scripts/backfill.py --start 2025-10-16

    # With explicit end and log to file:
    GCS_BUCKET=spesto-landed python scripts/backfill.py \\
        --start 2025-10-16 --end 2026-04-21 2>&1 | tee backfill.log
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

SOFIA = ZoneInfo("Europe/Sofia")
REPO_ROOT = Path(__file__).parent.parent
JOBS_DIR = REPO_ROOT / "jobs"

JOB_SEQUENCE: list[tuple[str, list[str]]] = [
    ("ingest_kolkostruva", []),
    ("load_to_bigquery",   []),
    ("transform",          ["--skip-dims"]),
]

log = logging.getLogger("backfill")


def daterange(start: dt.date, end: dt.date):
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)


def run_job(job: str, date: dt.date, extra_args: list[str]) -> int:
    cmd = ["uv", "run", "python", "main.py", "--date", date.isoformat(), *extra_args]
    cwd = JOBS_DIR / job
    started = time.monotonic()
    log.info("  [%s] start", job)
    result = subprocess.run(cmd, cwd=cwd)
    elapsed = time.monotonic() - started
    if result.returncode == 0:
        log.info("  [%s] ok (%.1fs)", job, elapsed)
    else:
        log.error("  [%s] FAILED rc=%d (%.1fs)", job, result.returncode, elapsed)
    return result.returncode


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", required=True, help="first date to process, YYYY-MM-DD")
    p.add_argument(
        "--end",
        help="last date (inclusive), YYYY-MM-DD; default: yesterday in Europe/Sofia",
    )
    p.add_argument(
        "--fail-fast",
        action="store_true",
        help="stop on first failure rather than continuing to the next date",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    if os.environ.get("GCS_BUCKET") is None:
        sys.exit("GCS_BUCKET must be set in the environment")

    try:
        start = dt.date.fromisoformat(args.start)
    except ValueError as e:
        sys.exit(f"invalid --start: {e}")

    if args.end:
        try:
            end = dt.date.fromisoformat(args.end)
        except ValueError as e:
            sys.exit(f"invalid --end: {e}")
    else:
        end = dt.datetime.now(SOFIA).date() - dt.timedelta(days=1)

    if end < start:
        sys.exit(f"--end ({end}) is before --start ({start})")

    dates = list(daterange(start, end))
    log.info(
        "backfilling %d date(s) from %s to %s (fail-fast=%s)",
        len(dates),
        start,
        end,
        args.fail_fast,
    )

    overall_start = time.monotonic()
    failures: list[tuple[dt.date, str]] = []

    for i, date in enumerate(dates, start=1):
        log.info("[%d/%d] %s", i, len(dates), date.isoformat())
        stopped = False
        for job, extra in JOB_SEQUENCE:
            rc = run_job(job, date, extra)
            if rc != 0:
                failures.append((date, job))
                stopped = True
                break
        if stopped and args.fail_fast:
            log.error("stopping after failure at %s (use without --fail-fast to continue)", date)
            break

    total_elapsed = time.monotonic() - overall_start
    ok = len(dates) - len({d for d, _ in failures})
    log.info("-" * 60)
    log.info("done: %d ok, %d with failures, %.1f minutes total", ok, len(failures), total_elapsed / 60)
    if failures:
        log.info("failed dates:")
        for date, job in failures:
            log.info("  %s at %s", date.isoformat(), job)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
