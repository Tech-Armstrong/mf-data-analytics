"""
sif/scripts/sif_nav.py

Show the latest NAV values for one SIF scheme, read via DuckDB.

Usage:
    python -m sif.scripts.sif_nav SIF-112             # last 10 NAVs from Blob
    python -m sif.scripts.sif_nav SIF-112 30          # last 30 NAVs
    python -m sif.scripts.sif_nav SIF-112 30 --local  # read staged local parquet
"""

import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

sys.stdout.reconfigure(encoding="utf-8")  # Windows cp1252 -> UTF-8 for table chars

import polars as pl

from sif.config.duckdb_session import get_connection, get_local_connection


def main(code: str, n: int, local: bool) -> int:
    connect = get_local_connection if local else get_connection
    with connect() as con:
        rows = con.execute(
            """
            SELECT scheme_code, scheme_name, category, fund_house, nav_date, nav
            FROM nav_history
            WHERE scheme_code = ?
            ORDER BY nav_date DESC
            LIMIT ?
            """,
            [code, n],
        ).pl()

    if rows.is_empty():
        print(f"No NAV rows found for scheme_code {code} ({'local' if local else 'Blob'}).")
        return 0

    name = rows["scheme_name"][0]
    print(f"Scheme {code} — {name}")
    print(f"Latest {len(rows)} NAVs (most recent first):\n")
    with pl.Config(tbl_rows=50, fmt_str_lengths=70):
        print(rows.select("nav_date", "nav"))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Show latest NAVs for a SIF scheme")
    parser.add_argument("scheme_code", help="e.g. SIF-112")
    parser.add_argument("n_rows", nargs="?", type=int, default=10,
                        help="Number of recent NAVs (default: 10)")
    parser.add_argument("--local", action="store_true",
                        help="Read staged local parquet instead of Blob")
    args = parser.parse_args()
    sys.exit(main(args.scheme_code, args.n_rows, args.local))
