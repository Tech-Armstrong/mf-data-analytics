"""
sif/scripts/check_sif_funds.py

Ad-hoc inspector for the SIF dataset.

No args   -> per-category coverage: scheme counts and how many have NAV rows.
With codes -> NAV summary + recent rows for each scheme_code.

SIF has no curated universe, so the "universe" is whatever is in scheme_master
(derived from the feed). This flags any scheme in scheme_master that has no
nav_history rows.

    python -m sif.scripts.check_sif_funds                 # coverage report (Blob)
    python -m sif.scripts.check_sif_funds --local         # coverage from staged local parquet
    python -m sif.scripts.check_sif_funds SIF-112         # one scheme detail
    python -m sif.scripts.check_sif_funds SIF-112 SIF-33
"""

import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

sys.stdout.reconfigure(encoding="utf-8")

from sif.config.duckdb_session import get_connection, get_local_connection


def coverage(con) -> None:
    master = con.execute(
        "SELECT scheme_code, category FROM scheme_master"
    ).fetchall()
    present = {
        r[0] for r in con.execute(
            "SELECT DISTINCT scheme_code FROM nav_history"
        ).fetchall()
    }
    total = len(master)
    with_data = sum(1 for c, _ in master if c in present)
    missing = [(c, cat) for c, cat in master if c not in present]

    print(f"Schemes in scheme_master: {total}  |  with NAV data: {with_data}  "
          f"|  MISSING: {len(missing)}")
    print("-" * 70)

    # per-category breakdown
    by_cat: dict[str, list[str]] = {}
    for c, cat in master:
        by_cat.setdefault(cat or "(uncategorised)", []).append(c)
    for cat in sorted(by_cat):
        codes = by_cat[cat]
        have = sum(1 for c in codes if c in present)
        print(f"  {cat:<45} {have}/{len(codes)} with data")

    if missing:
        print("-" * 70)
        print("Schemes with NO NAV data:")
        for c, cat in missing:
            print(f"  [{cat}] {c}")


def detail(con, code: str) -> None:
    meta = con.execute(
        "SELECT scheme_name, fund_house, category FROM scheme_master WHERE scheme_code = ?",
        [code],
    ).fetchone()
    summ = con.execute(
        "SELECT COUNT(*), MIN(nav_date), MAX(nav_date) FROM nav_history WHERE scheme_code = ?",
        [code],
    ).fetchone()

    print("=" * 70)
    print(f"scheme_code: {code}")
    if meta:
        print(f"  name    : {meta[0]}")
        print(f"  house   : {meta[1]}   category: {meta[2]}")
    else:
        print("  (NOT in scheme_master — unmapped)")
    if summ and summ[0]:
        print(f"  rows    : {summ[0]}    range: {summ[1]} -> {summ[2]}")
        recent = con.execute(
            "SELECT nav_date, nav FROM nav_history WHERE scheme_code = ? "
            "ORDER BY nav_date DESC LIMIT 5",
            [code],
        ).fetchall()
        print("  latest 5 NAVs:")
        for d, n in recent:
            print(f"    {d}   {n}")
    else:
        print("  rows    : 0  (NO NAV data)")


def main(codes: list[str], local: bool) -> int:
    connect = get_local_connection if local else get_connection
    with connect() as con:
        if not codes:
            coverage(con)
        else:
            for c in codes:
                detail(con, c)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect the SIF dataset")
    parser.add_argument("codes", nargs="*", help="scheme_code(s) to detail; omit for coverage report")
    parser.add_argument("--local", action="store_true",
                        help="Read staged local parquet instead of Blob")
    args = parser.parse_args()
    sys.exit(main(args.codes, args.local))
