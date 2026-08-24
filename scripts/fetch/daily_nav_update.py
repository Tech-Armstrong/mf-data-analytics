"""
scripts/fetch/daily_nav_update.py

Incremental daily NAV update using the AMFI NAVAll.txt feed.

What it does:
1. Fetches https://portal.amfiindia.com/spages/NAVAll.txt  (single request, ~1MB)
2. Parses all ~17,000 scheme NAVs for today's date
3. Filters to only the 262 schemes in our fund universe
4. Checks Blob nav_history (current year) — skips if today's data already loaded
5. Uploads today's rows as a new raw parquet  (raw/nav/year=YYYY/)
6. Merges today's rows into the current year's nav_history partition on Blob
   (processed/nav_history/year=YYYY/data.parquet) — only that one year is rewritten.

There is no .duckdb file: the views are computed live at query time by
config/duckdb_session.py reading these parquet files from Blob.

Run daily after AMFI publishes NAVs (~8 PM IST on weekdays).

Usage:
    python -m scripts.fetch.daily_nav_update
    python -m scripts.fetch.daily_nav_update --force   # re-load even if today already exists
"""

import sys
import argparse
from pathlib import Path
from datetime import date, datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests
import polars as pl

from config.constants import (
    BLOB_RAW_PREFIX,
    BLOB_NAV_HISTORY_DIR,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF,
)
from config.blob_io import upload_bytes, download_bytes, to_parquet_bytes
from config.logging_utils import get_logger
from scripts.processing.fund_universe import FUND_UNIVERSE

log = get_logger("daily_nav_update")

AMFI_DAILY_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"

# Flat set of all scheme codes we care about
UNIVERSE_CODES: set[str] = {
    code
    for funds in FUND_UNIVERSE.values()
    for code, _, _ in funds
}


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_nav_text() -> str:
    import time
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(AMFI_DAILY_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            log.info("Fetched NAVAll.txt: %d bytes", len(r.content))
            return r.text
        except Exception as exc:
            wait = RETRY_BACKOFF ** attempt
            log.warning("Attempt %d failed: %s — retrying in %.1fs", attempt, exc, wait)
            time.sleep(wait)
    log.error("All retries exhausted for %s", AMFI_DAILY_URL)
    sys.exit(1)


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_nav_text(text: str) -> pl.DataFrame:
    """
    NAVAll.txt is semicolon delimited with a header line naming the columns:
        Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;
        Scheme Name;Plan;Option;Net Asset Value;Date

    Returns DataFrame filtered to UNIVERSE_CODES only.

    NOTE: Plan and Option are their own columns, which puts NAV at index 6 and
    Date at index 7. An earlier layout had them at 4 and 5; reading those now
    yields "Regular Plan"/"Growth", float() raises, and the except below drops
    every row -- the job then logs "no data parsed" and exits 0, looking like a
    successful no-op. That silently lost 21-Aug-2026. Both indices are resolved
    by name off the header so a further reshuffle fails loudly instead.
    """
    rows = []
    # Defaults match AMFI's current 8-column layout; the header line overrides.
    nav_idx, date_idx = 6, 7
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(";")
        if len(parts) < 6:
            continue
        code = parts[0].strip()

        if code == "Scheme Code":
            labels = [c.strip().lower() for c in parts]
            try:
                nav_idx  = labels.index("net asset value")
                date_idx = labels.index("date")
            except ValueError:
                raise RuntimeError(
                    "NAVAll.txt header lacks 'Net Asset Value'/'Date' columns; "
                    f"got {parts!r}. The response format changed."
                )
            continue

        if not code.isdigit():
            continue
        if code not in UNIVERSE_CODES:
            continue
        try:
            nav_val  = float(parts[nav_idx].strip())
            nav_date = datetime.strptime(parts[date_idx].strip(), "%d-%b-%Y").date()
        except (ValueError, IndexError):
            continue
        rows.append({
            "scheme_code": code,
            "nav_date":    nav_date,
            "nav":         nav_val,
        })

    if not rows:
        return pl.DataFrame(schema={
            "scheme_code": pl.Utf8,
            "nav_date":    pl.Date,
            "nav":         pl.Float64,
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
    Old years are never touched.
    """
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
    log.info("Daily NAV update — %s", today)
    log.info("Universe: %d schemes across %d categories",
             len(UNIVERSE_CODES), len(FUND_UNIVERSE))
    log.info("-------------------------------------------")

    # Fetch first, then decide: the guard must test the date AMFI actually
    # published, not today's. AMFI publishes in the evening and a run can fire
    # before that, or on a day after a holiday -- guarding on `today` would skip
    # a still-unloaded prior session, and that day is then unrecoverable here
    # because NAVAll.txt only ever carries the latest date.
    text = fetch_nav_text()

    # Parse + filter to universe
    df = parse_nav_text(text)

    if df.is_empty():
        log.error(
            "No rows parsed for our universe from a %d-byte response. Either "
            "AMFI has not published, or the file layout changed -- check the "
            "header handling in parse_nav_text before assuming the former.",
            len(text),
        )
        return

    # The file's own latest date, which is what we are about to load.
    feed_date = df["nav_date"].max()
    if not force and already_loaded(feed_date):
        log.info("NAV for %s already on Blob. Nothing to do. Use --force to reload.",
                 feed_date)
        return

    nav_date   = feed_date
    found      = df["scheme_code"].n_unique()
    missing    = len(UNIVERSE_CODES) - found

    log.info("Parsed: %d rows | NAV date: %s | Universe matched: %d/%d schemes",
             len(df), nav_date, found, len(UNIVERSE_CODES))

    if missing > 0:
        present = set(df["scheme_code"].to_list())
        absent  = sorted(UNIVERSE_CODES - present)
        log.warning("%d scheme(s) not in today's feed: %s", missing, absent)

    # Upload today's rows as a raw parquet (audit trail)
    write_raw(df)

    # Merge into the current year's nav_history partition on Blob
    update_nav_history(df)

    log.info("-------------------------------------------")
    log.info("Daily update complete.")
    log.info("-------------------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily NAV update from AMFI NAVAll.txt")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-load even if today's data is already in DuckDB",
    )
    args = parser.parse_args()
    main(force=args.force)
