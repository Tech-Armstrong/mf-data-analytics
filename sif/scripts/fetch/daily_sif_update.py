"""
sif/scripts/fetch/daily_sif_update.py

Incremental daily SIF NAV update using the SIF_NAVAll.txt feed.

What it does:
1. Fetches https://portal.amfiindia.com/spages/SIF_NAVAll.txt  (single request)
2. Parses every SIF row for today's date (no universe filter — we keep all SIFs)
3. Checks Blob nav_history (current year) — skips if today's data already loaded
4. Uploads today's rows as a new raw parquet  (raw/nav/year=YYYY/)
5. Merges today's rows into the current year's nav_history partition on Blob
6. Rebuilds scheme_master from today's labels, so any NEW SIF scheme that just
   appeared in the feed is captured and labelled automatically — no curated
   universe, no manual step.

There is no .duckdb file: the views are computed live at query time by
sif/config/duckdb_session.py reading these parquet files from Blob.

Usage:
    python -m sif.scripts.fetch.daily_sif_update
    python -m sif.scripts.fetch.daily_sif_update --force   # re-load even if today already exists
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import requests
import polars as pl

from sif.config.constants import (
    BLOB_RAW_PREFIX,
    BLOB_NAV_HISTORY_DIR,
    SIF_DAILY_URL,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF,
)
from sif.config.blob_io import upload_bytes, download_bytes, to_parquet_bytes
from sif.config.logging_utils import get_logger
from sif.scripts.sif_parse import parse_sif_lines
from sif.scripts.processing.build_sif_scheme_master import build_from_labelled_rows

log = get_logger("daily_sif_update")

# Column layout of the SIF daily feed (6 cols):
# Scheme Code; ISIN Growth; ISIN Reinvest; Scheme Name; NAV; Date
_DAILY_NAME_IDX, _DAILY_NAV_IDX, _DAILY_DATE_IDX = 3, 4, 5


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_nav_text() -> str:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(SIF_DAILY_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            log.info("Fetched SIF_NAVAll.txt: %d bytes", len(r.content))
            return r.text
        except Exception as exc:
            wait = RETRY_BACKOFF ** attempt
            log.warning("Attempt %d failed: %s — retrying in %.1fs", attempt, exc, wait)
            time.sleep(wait)
    log.error("All retries exhausted for %s", SIF_DAILY_URL)
    sys.exit(1)


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_nav_text(text: str) -> pl.DataFrame:
    """Parse the SIF daily feed into labelled rows (all SIFs, no filter)."""
    rows = parse_sif_lines(
        text,
        name_idx=_DAILY_NAME_IDX, nav_idx=_DAILY_NAV_IDX, date_idx=_DAILY_DATE_IDX,
    )
    if not rows:
        return pl.DataFrame(schema={
            "scheme_code": pl.Utf8,
            "nav_date":    pl.Date,
            "nav":         pl.Float64,
            "scheme_name": pl.Utf8,
            "fund_house":  pl.Utf8,
            "category":    pl.Utf8,
        })
    return pl.DataFrame(rows).with_columns(
        pl.col("nav_date").cast(pl.Date),
        pl.col("nav").cast(pl.Float64),
    )


# ── Blob partition helpers ────────────────────────────────────────────────────

def _nav_history_blob_path(year: int) -> str:
    return f"{BLOB_NAV_HISTORY_DIR}/year={year}/data.parquet"


def _read_year_partition(year: int) -> pl.DataFrame | None:
    """Download the current year's nav_history partition from Blob, or None."""
    data = download_bytes(_nav_history_blob_path(year))
    if data is None:
        return None
    return pl.read_parquet(data)


# ── Already loaded check ──────────────────────────────────────────────────────

def already_loaded(nav_date: date) -> bool:
    """Returns True if this date already exists in the year's Blob partition."""
    part = _read_year_partition(nav_date.year)
    if part is None or part.is_empty():
        return False
    return part.filter(pl.col("nav_date") == nav_date).height > 0


# ── Upload today's rows as a raw parquet ──────────────────────────────────────

def write_raw(df: pl.DataFrame) -> None:
    today_str = date.today().strftime("%Y_%m_%d")
    df_with_year = df.with_columns(
        pl.col("nav_date").dt.year().cast(pl.Utf8).alias("year")
    )
    for (year_val,), group in df_with_year.group_by("year"):
        blob_path = f"{BLOB_RAW_PREFIX}/year={year_val}/daily_{today_str}.parquet"
        upload_bytes(to_parquet_bytes(group.drop("year")), blob_path)
        log.info("Raw parquet uploaded: %s (%d rows)", blob_path, len(group))


# ── Merge today's rows into the current year's nav_history partition ───────────

def update_nav_history(new_df: pl.DataFrame) -> None:
    """
    Rewrite only the partition(s) for the year(s) present in new_df.
    Reads that year's parquet from Blob, merges + dedupes, uploads it back.
    Old years are never touched. Only scheme_code/nav_date/nav are stored.
    """
    new_df = new_df.select(["scheme_code", "nav_date", "nav"])
    new_with_year = new_df.with_columns(pl.col("nav_date").dt.year().alias("year"))

    for (year_val,), group in new_with_year.group_by("year"):
        group = group.drop("year")
        existing = _read_year_partition(year_val)

        combined = group if existing is None else pl.concat([existing, group])

        deduped = (
            combined
            .sort("nav_date", descending=True)
            .unique(subset=["scheme_code", "nav_date"], keep="first")
            .sort(["scheme_code", "nav_date"])
        )

        upload_bytes(to_parquet_bytes(deduped), _nav_history_blob_path(year_val))
        log.info("nav_history year=%s updated: %d rows, %d schemes",
                 year_val, len(deduped), deduped["scheme_code"].n_unique())


# ── Main ──────────────────────────────────────────────────────────────────────

def main(force: bool = False) -> None:
    today = date.today()
    log.info("-------------------------------------------")
    log.info("Daily SIF NAV update — %s", today)
    log.info("-------------------------------------------")

    # Fetch + parse first (we need the feed's NAV date, which may lag 'today').
    text = fetch_nav_text()
    df = parse_nav_text(text)

    if df.is_empty():
        log.warning("No SIF data parsed. AMFI may not have published yet.")
        return

    nav_date = df["nav_date"].max()
    found    = df["scheme_code"].n_unique()
    log.info("Parsed: %d rows | latest NAV date: %s | %d schemes",
             len(df), nav_date, found)

    # Idempotence: skip if this NAV date is already on Blob.
    if not force and already_loaded(nav_date):
        log.info("NAV date %s already on Blob. Nothing to do. Use --force to reload.", nav_date)
        return

    # Upload today's rows as a raw parquet (audit trail)
    write_raw(df)

    # Merge into the current year's nav_history partition on Blob
    update_nav_history(df)

    # Refresh scheme_master so new SIFs / new strategy headers are labelled.
    build_from_labelled_rows(df)

    log.info("-------------------------------------------")
    log.info("Daily SIF update complete.")
    log.info("-------------------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily SIF NAV update from SIF_NAVAll.txt")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-load even if this NAV date is already on Blob",
    )
    args = parser.parse_args()
    main(force=args.force)
