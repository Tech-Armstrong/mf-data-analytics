"""
sif/scripts/fetch/fetch_sif_history.py

Fetches historical SIF NAV data from the AMFI SIF history endpoint:

    https://portal.amfiindia.com/SIF_DownloadNAVHistoryReport.aspx
        ?frmdt=DD-Mon-YYYY&todt=DD-Mon-YYYY

Unlike the MF history endpoint there is NO mf= parameter — a single request per
date window returns every SIF — so this just iterates date chunks (no AMC loop).

Strategy
--------
- Date range: today-minus-5-years -> today, split into ~180-day chunks (the
  portal rejects very large windows). For a brand-new asset class most of that
  range will be empty, which is fine.
- One HTTP request per chunk, with a pause between requests.
- Parses the 8-column SIF history response via sif_parse.parse_sif_lines and
  writes year-partitioned parquet exactly like the MF pipeline.
- nav_history parquet keeps only (scheme_code, nav_date, nav); the labels go to
  scheme_master via build_from_labelled_rows.

Usage
-----
    python -m sif.scripts.fetch.fetch_sif_history
    python -m sif.scripts.fetch.fetch_sif_history --local        # write to disk, no Blob
    python -m sif.scripts.fetch.fetch_sif_history --pause 3
    python -m sif.scripts.fetch.fetch_sif_history --chunk-days 90
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import requests
import polars as pl

from sif.config.constants import (
    RAW_NAV_DIR,
    PROCESSED_DIR,
    NAV_HISTORY_PARQUET,
    BLOB_RAW_PREFIX,
    BLOB_NAV_HISTORY_DIR,
    SIF_HISTORY_URL,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF,
)
from sif.config.blob_io import upload_bytes, to_parquet_bytes
from sif.config.logging_utils import get_logger
from sif.scripts.sif_parse import parse_sif_lines
from sif.scripts.processing.build_sif_scheme_master import build_from_labelled_rows

log = get_logger("fetch_sif_history")

# AMFI date format for the URL parameters
_AMFI_DATE_FMT = "%d-%b-%Y"   # e.g. 01-Feb-2026

# Column layout of the SIF history feed (8 cols):
# Scheme Code; Scheme Name; ISIN Growth; ISIN Reinvest; NAV; Repurchase; Sale; Date
_HIST_NAME_IDX, _HIST_NAV_IDX, _HIST_DATE_IDX = 1, 4, 7


# ── Date chunking ─────────────────────────────────────────────────────────────

def _date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive chunks of at most chunk_days days."""
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


# ── HTTP fetch with retry ─────────────────────────────────────────────────────

def _fetch_text(url: str) -> str | None:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as exc:
            wait = RETRY_BACKOFF ** attempt
            log.warning(
                "Attempt %d failed for %s: %s — retrying in %.1fs",
                attempt, url, exc, wait,
            )
            time.sleep(wait)
    log.error("All retries exhausted for %s", url)
    return None


# ── Write helpers (Blob or local, year-partitioned) ──────────────────────────

def _write_raw_partition(df: pl.DataFrame, label: str, local: bool) -> None:
    if df.is_empty():
        return
    today_str = date.today().strftime("%Y_%m_%d")
    df = df.with_columns(pl.col("nav_date").dt.year().cast(pl.Utf8).alias("year"))
    for (year_val,), group in df.group_by("year"):
        out = group.drop("year")
        fname = f"sif_{label}_{today_str}.parquet"
        if local:
            year_dir = RAW_NAV_DIR / f"year={year_val}"
            year_dir.mkdir(parents=True, exist_ok=True)
            out.write_parquet(year_dir / fname)
            log.info("  raw -> %s  (%d rows)", year_dir / fname, len(out))
        else:
            blob_path = f"{BLOB_RAW_PREFIX}/year={year_val}/{fname}"
            upload_bytes(to_parquet_bytes(out), blob_path)
            log.info("  raw -> az://.../%s  (%d rows)", blob_path, len(out))


def _write_nav_history(new_df: pl.DataFrame, local: bool) -> None:
    """
    Full-history fetch: write each year as its own partition (fresh, deduped).
    Only scheme_code/nav_date/nav are stored (labels live in scheme_master).
    """
    if new_df.is_empty():
        log.info("Nothing fetched -- nav_history unchanged")
        return

    deduped = (
        new_df
        .select(["scheme_code", "nav_date", "nav"])
        .sort("nav_date", descending=True)
        .unique(subset=["scheme_code", "nav_date"], keep="first")
        .sort(["scheme_code", "nav_date"])
    )

    if local:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        deduped.write_parquet(NAV_HISTORY_PARQUET)
        log.info("nav_history written locally: %s (%d rows, %d schemes)",
                 NAV_HISTORY_PARQUET, len(deduped), deduped["scheme_code"].n_unique())
        return

    by_year = deduped.with_columns(pl.col("nav_date").dt.year().alias("year"))
    for (year_val,), group in by_year.group_by("year"):
        group = group.drop("year")
        blob_path = f"{BLOB_NAV_HISTORY_DIR}/year={year_val}/data.parquet"
        upload_bytes(to_parquet_bytes(group), blob_path)
        log.info("  nav_history year=%s -> %d rows", year_val, len(group))

    log.info("nav_history written to Blob: %d total rows across %d schemes",
             len(deduped), deduped["scheme_code"].n_unique())


# ── Main ──────────────────────────────────────────────────────────────────────

def main(pause_seconds: float = 2.0, chunk_days: int = 180, local: bool = False) -> None:
    today      = date.today()
    start_date = today.replace(year=today.year - 5)
    chunks     = _date_chunks(start_date, today, chunk_days)

    log.info("===================================================")
    log.info("SIF historical fetch  --  %s -> %s  (%s)",
             start_date, today, "LOCAL" if local else "BLOB")
    log.info("chunks: %d  |  pause: %.1fs", len(chunks), pause_seconds)
    log.info("===================================================")

    all_rows: list[dict] = []
    for i, (frmdt, todt) in enumerate(chunks, start=1):
        url = SIF_HISTORY_URL.format(
            frmdt=frmdt.strftime(_AMFI_DATE_FMT),
            todt=todt.strftime(_AMFI_DATE_FMT),
        )
        log.info("  [%d/%d] %s -> %s",
                 i, len(chunks),
                 frmdt.strftime(_AMFI_DATE_FMT), todt.strftime(_AMFI_DATE_FMT))

        text = _fetch_text(url)
        if text:
            rows = parse_sif_lines(
                text,
                name_idx=_HIST_NAME_IDX, nav_idx=_HIST_NAV_IDX, date_idx=_HIST_DATE_IDX,
            )
            all_rows.extend(rows)
            log.info("    parsed %d rows", len(rows))
        else:
            log.warning("    no data returned")

        if i < len(chunks):
            time.sleep(pause_seconds)

    if not all_rows:
        log.error("No data fetched. Check connectivity and the SIF endpoint.")
        sys.exit(1)

    log.info("Total rows fetched: %d", len(all_rows))

    df = pl.DataFrame(all_rows).with_columns(
        pl.col("nav_date").cast(pl.Date),
        pl.col("nav").cast(pl.Float64),
    )

    _write_raw_partition(df, "history", local=local)
    _write_nav_history(df, local=local)

    # Publish labels (scheme_name/fund_house/category) so scheme_master reflects
    # every SIF just fetched — fully automatic, no curated universe.
    build_from_labelled_rows(df, local=local)

    log.info("===================================================")
    log.info("SIF historical fetch complete.")
    log.info("===================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch 5 years of SIF NAV history")
    parser.add_argument(
        "--pause", type=float, default=2.0,
        help="Seconds to pause between HTTP requests (default: 2.0)",
    )
    parser.add_argument(
        "--chunk-days", type=int, default=180,
        help="Date window per request in days (default: 180)",
    )
    parser.add_argument(
        "--local", action="store_true",
        help="Write to local parquet instead of Azure Blob (for inspection)",
    )
    args = parser.parse_args()
    main(pause_seconds=args.pause, chunk_days=args.chunk_days, local=args.local)
