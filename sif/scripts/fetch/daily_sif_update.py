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

def select_new_rows(df: pl.DataFrame) -> pl.DataFrame:
    """
    Return only the (scheme_code, nav_date) rows not already on Blob.

    Replaces an earlier `already_loaded(nav_date)` guard that collapsed the feed
    to df["nav_date"].max() and returned early if that date was present. The AMFI
    feed is not single-dated -- late-striking schemes carry a later stamp than the
    main business day -- so once that later date was stored the guard discarded the
    whole file, including rows for an earlier day never written. See the same fix
    in scripts/fetch/daily_nav_update.py, where it cost 55 schemes on 2026-09-18.
    """
    parts = [
        part
        for year in df["nav_date"].dt.year().unique().to_list()
        if (part := _read_year_partition(year)) is not None and not part.is_empty()
    ]
    if not parts:
        return df

    existing = pl.concat(parts, how="vertical_relaxed").select("scheme_code", "nav_date")
    return df.join(existing, on=["scheme_code", "nav_date"], how="anti")


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

    log.info("Parsed: %d rows | %d schemes", len(df), df["scheme_code"].n_unique())
    for row in df.group_by("nav_date").len().sort("nav_date").iter_rows():
        log.info("  feed date %s: %d scheme(s)", row[0], row[1])

    # Idempotence per (scheme_code, nav_date), not per feed date -- so a scheme
    # that reported late is merged by a later run instead of being dropped.
    if not force:
        new_df = select_new_rows(df)
        if new_df.is_empty():
            log.info("All %d parsed row(s) already on Blob. Nothing to do. "
                     "Use --force to reload.", len(df))
            return
        log.info("New rows to write: %d of %d parsed (%d already on Blob)",
                 len(new_df), len(df), len(df) - len(new_df))
        df = new_df

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
