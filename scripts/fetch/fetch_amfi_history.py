"""
scripts/fetch/fetch_amfi_history.py

Fetches 5 years of historical NAV data from the AMFI portal for a fixed set
of AMCs, using the bulk-download endpoint:

    https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx
        ?mf=<amc_code>&frmdt=DD-Mon-YYYY&todt=DD-Mon-YYYY

Strategy
--------
- Date range: today-minus-5-years  →  today, split into ~90-day chunks
  (AMFI portal rejects very large windows, so we page in quarterly slices).
- One HTTP request per AMC per chunk, with a configurable pause between
  requests to avoid rate-limiting.
- Parses the pipe-delimited text response into (scheme_code, nav_date, nav)
  rows and writes raw parquet files partitioned by year, then rebuilds
  nav_history.parquet exactly like the existing pipeline.

Usage
-----
    python -m scripts.fetch.fetch_amfi_history
    python -m scripts.fetch.fetch_amfi_history --pause 3   # 3-second pause between requests
    python -m scripts.fetch.fetch_amfi_history --chunk-days 90
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import date, timedelta, datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests
import polars as pl

from config.constants import (
    RAW_NAV_DIR,
    PROCESSED_DIR,
    NAV_HISTORY_PARQUET,
    SCHEME_MASTER_PARQUET,
    BLOB_RAW_PREFIX,
    BLOB_NAV_HISTORY_DIR,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF,
)
from config.blob_io import upload_bytes, to_parquet_bytes
from config.logging_utils import get_logger


def _load_universe() -> set[str]:
    """Labelled scheme_codes — fetched rows are filtered to these."""
    if not SCHEME_MASTER_PARQUET.exists():
        log.error("scheme_master.parquet not found at %s. Run build_scheme_master first.",
                  SCHEME_MASTER_PARQUET)
        sys.exit(1)
    return set(
        pl.read_parquet(SCHEME_MASTER_PARQUET)
        .with_columns(pl.col("scheme_code").cast(pl.Utf8))["scheme_code"]
        .unique()
        .to_list()
    )

log = get_logger("fetch_amfi_history")

# ── AMC catalogue ─────────────────────────────────────────────────────────────

AMC_LIST = [
    {"code":  3, "name": "Aditya Birla Sun Life Mutual Fund"},
    {"code":  4, "name": "Baroda BNP Paribas Mutual Fund"},
    {"code":  6, "name": "DSP Mutual Fund"},
    {"code":  9, "name": "HDFC Mutual Fund"},
    {"code": 20, "name": "ICICI Prudential Mutual Fund"},
    {"code": 22, "name": "SBI Mutual Fund"},
    {"code": 25, "name": "Tata Mutual Fund"},
    {"code": 28, "name": "UTI Mutual Fund"},
    {"code": 32, "name": "Canara Robeco Mutual Fund"},
    {"code": 33, "name": "Sundaram Mutual Fund"},
    {"code": 37, "name": "HSBC Mutual Fund"},
    {"code": 42, "name": "Invesco Mutual Fund"},
    {"code": 45, "name": "Mirae Asset Mutual Fund"},
    {"code": 47, "name": "Edelweiss Mutual Fund"},
    {"code": 53, "name": "Axis Mutual Fund"},
    {"code": 61, "name": "Union Mutual Fund"},
    {"code": 17, "name": "Kotak Mahindra Mutual Fund"},
    {"code": 16, "name": "JM Financial Mutual Fund"},
]

AMFI_URL = (
    "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
    "?mf={mf}&frmdt={frmdt}&todt={todt}"
)

# AMFI date format for the URL parameters
_AMFI_DATE_FMT = "%d-%b-%Y"   # e.g. 01-Feb-2019


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


# ── Response parser ───────────────────────────────────────────────────────────

def _parse_amfi_text(text: str) -> list[dict]:
    """
    AMFI bulk NAV response is a plain-text, semicolon-delimited file.

    Header block (repeated per scheme):
        Scheme Code;ISIN Div Payout/IDCW;ISIN Div Reinvestment;Scheme Name;
        Net Asset Value;Repurchase Price;Sale Price;Date

    We only care about: Scheme Code, Net Asset Value, Date.
    Lines with non-numeric scheme codes (e.g. fund-house headers) are skipped.
    """
    rows: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(";")
        if len(parts) < 8:
            continue
        scheme_code_raw = parts[0].strip()
        nav_raw         = parts[4].strip()
        date_raw        = parts[7].strip()

        # Skip header/fund-house label lines
        if not scheme_code_raw.isdigit():
            continue

        try:
            nav_val  = float(nav_raw)
            nav_date = datetime.strptime(date_raw, "%d-%b-%Y").date()
        except (ValueError, TypeError):
            continue

        rows.append({
            "scheme_code": scheme_code_raw,
            "nav_date":    nav_date,
            "nav":         nav_val,
        })
    return rows


# ── Write helpers (Blob or local, year-partitioned) ──────────────────────────

def _write_raw_partition(df: pl.DataFrame, label: str, local: bool) -> None:
    if df.is_empty():
        return
    today_str = date.today().strftime("%Y_%m_%d")
    df = df.with_columns(pl.col("nav_date").dt.year().cast(pl.Utf8).alias("year"))
    for (year_val,), group in df.group_by("year"):
        out = group.drop("year")
        fname = f"amfi_{label}_{today_str}.parquet"
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
    local=True  -> processed/nav_history.parquet (flat, for inspection/migrate)
    local=False -> Blob processed/nav_history/year=YYYY/data.parquet
    """
    if new_df.is_empty():
        log.info("Nothing fetched -- nav_history unchanged")
        return

    deduped = (
        new_df
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
    universe   = _load_universe()

    log.info("===================================================")
    log.info("AMFI historical fetch  --  %s -> %s  (%s)",
             start_date, today, "LOCAL" if local else "BLOB")
    log.info("AMCs: %d  |  chunks: %d  |  pause: %.1fs  |  universe: %d funds",
             len(AMC_LIST), len(chunks), pause_seconds, len(universe))
    log.info("===================================================")

    all_rows: list[dict] = []
    total_requests = len(AMC_LIST) * len(chunks)
    req_num = 0

    for amc in AMC_LIST:
        amc_rows: list[dict] = []
        log.info("-- %s (mf=%d) --", amc["name"], amc["code"])

        for frmdt, todt in chunks:
            req_num += 1
            url = AMFI_URL.format(
                mf=amc["code"],
                frmdt=frmdt.strftime(_AMFI_DATE_FMT),
                todt=todt.strftime(_AMFI_DATE_FMT),
            )
            log.info(
                "  [%d/%d] %s -> %s",
                req_num, total_requests,
                frmdt.strftime(_AMFI_DATE_FMT),
                todt.strftime(_AMFI_DATE_FMT),
            )

            text = _fetch_text(url)
            if text:
                rows = _parse_amfi_text(text)
                # keep only labelled-universe schemes
                rows = [r for r in rows if r["scheme_code"] in universe]
                amc_rows.extend(rows)
                log.info("    parsed %d rows (universe-filtered)", len(rows))
            else:
                log.warning("    no data returned")

            # Pause between every request to be polite to the server
            if req_num < total_requests:
                time.sleep(pause_seconds)

        log.info("  %s total rows: %d", amc["name"], len(amc_rows))
        all_rows.extend(amc_rows)

    if not all_rows:
        log.error("No data fetched. Check connectivity and AMC codes.")
        sys.exit(1)

    log.info("Total rows fetched: %d", len(all_rows))

    df = pl.DataFrame(all_rows).with_columns(
        pl.col("nav_date").cast(pl.Date),
        pl.col("nav").cast(pl.Float64),
    )

    _write_raw_partition(df, "amfi_bulk", local=local)
    _write_nav_history(df, local=local)

    log.info("===================================================")
    log.info("AMFI historical fetch complete.")
    log.info("===================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch 5 years of AMFI NAV history")
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
        help="Write to local parquet instead of Azure Blob (for inspection before migrate)",
    )
    args = parser.parse_args()
    main(pause_seconds=args.pause, chunk_days=args.chunk_days, local=args.local)
