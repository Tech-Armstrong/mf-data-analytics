"""
sif/scripts/check_blob_freshness.py

Quick operational check: is the SIF nav_history current-year partition fresh?
Prints the latest nav_date + row counts. Run after a daily Action to confirm it
wrote new data.

    python -m sif.scripts.check_blob_freshness            # read from Blob (default)
    python -m sif.scripts.check_blob_freshness --local    # read the staged local parquet
"""

import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from datetime import date
import polars as pl

from sif.config.blob_io import download_bytes
from sif.config.constants import BLOB_NAV_HISTORY_DIR, NAV_HISTORY_PARQUET


def _load(local: bool) -> tuple[str, pl.DataFrame | None]:
    """Return (source_label, df_or_None) for the current-year partition."""
    if local:
        if not NAV_HISTORY_PARQUET.exists():
            return str(NAV_HISTORY_PARQUET), None
        return str(NAV_HISTORY_PARQUET), pl.read_parquet(NAV_HISTORY_PARQUET)

    year = date.today().year
    path = f"{BLOB_NAV_HISTORY_DIR}/year={year}/data.parquet"
    data = download_bytes(path)
    if data is None:
        return f"az://.../{path}", None
    return f"az://.../{path}", pl.read_parquet(data)


def main(local: bool = False) -> int:
    src, df = _load(local)
    if df is None:
        print(f"NO data at {src} — nothing staged/published yet.")
        return 0

    latest = df["nav_date"].max()
    print(f"Source: {src}")
    print(f"  rows           = {len(df)}")
    print(f"  schemes        = {df['scheme_code'].n_unique()}")
    print(f"  nav_date range = {df['nav_date'].min()} -> {latest}")
    print(f"  rows on {latest} = {df.filter(pl.col('nav_date') == latest).height}")
    print(f"  today          = {date.today()}  "
          f"({'MATCH' if latest == date.today() else 'latest is older than today'})")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check SIF nav_history freshness")
    parser.add_argument("--local", action="store_true",
                        help="Read the staged local parquet instead of Blob")
    args = parser.parse_args()
    sys.exit(main(local=args.local))
