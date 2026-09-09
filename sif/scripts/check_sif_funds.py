"""
sif/scripts/check_sif_funds.py

Ad-hoc inspector for the SIF dataset.

No args   -> per-category coverage: scheme counts and how many have NAV rows.
With codes -> NAV summary + recent rows for each scheme_code.
--amc-check -> flags any fund_house currently in scheme_master that is NOT the
    canonical AMC name AMC_NAME_MAP would resolve it to (i.e. a brand that's
    unmapped, or a stale value published before its mapping existed/changed).

SIF has no curated universe, so the "universe" is whatever is in scheme_master
(derived from the feed). This flags any scheme in scheme_master that has no
nav_history rows.

    python -m sif.scripts.check_sif_funds                 # coverage report (Blob)
    python -m sif.scripts.check_sif_funds --local         # coverage from staged local parquet
    python -m sif.scripts.check_sif_funds SIF-112         # one scheme detail
    python -m sif.scripts.check_sif_funds SIF-112 SIF-33
    python -m sif.scripts.check_sif_funds --amc-check     # audit fund_house mapping
"""

import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

sys.stdout.reconfigure(encoding="utf-8")

from sif.config.duckdb_session import get_connection, get_local_connection
from sif.config.amc_map import resolve_amc_name


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


def amc_check(con) -> None:
    """
    Report fund_house values in scheme_master that resolve_amc_name() would
    NOT leave unchanged today — either because they were never mapped
    (published before this brand's entry existed), or because AMC_NAME_MAP
    has since been corrected and the change hasn't been re-run through
    build_from_labelled_rows() yet (that happens automatically on the next
    daily fetch, since it re-applies the map to the full merged master).
    """
    rows = con.execute(
        "SELECT DISTINCT fund_house, COUNT(*) OVER (PARTITION BY fund_house) "
        "FROM scheme_master ORDER BY fund_house"
    ).fetchall()

    stale = []
    for fund_house, count in rows:
        canonical, was_mapped = resolve_amc_name(fund_house)
        if not was_mapped:
            stale.append((fund_house, count, "UNMAPPED — no AMC_NAME_MAP entry"))
        elif canonical != fund_house:
            stale.append((fund_house, count, f"STALE — map now resolves to {canonical!r}"))

    print(f"fund_house values in scheme_master: {len(rows)}")
    if not stale:
        print("All fund_house values match AMC_NAME_MAP. Nothing to fix.")
        return

    print(f"Found {len(stale)} value(s) needing attention:")
    print("-" * 70)
    for fund_house, count, reason in stale:
        print(f"  {fund_house!r:<35} ({count} schemes)  {reason}")
    print("-" * 70)
    print("Add/fix entries in sif/config/amc_map.py, then re-run the daily "
          "fetcher (or python -m sif.scripts.processing.build_sif_scheme_master) "
          "to self-heal scheme_master.")


def main(codes: list[str], local: bool, do_amc_check: bool) -> int:
    connect = get_local_connection if local else get_connection
    with connect() as con:
        if do_amc_check:
            amc_check(con)
        elif not codes:
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
    parser.add_argument("--amc-check", action="store_true",
                        help="Audit fund_house values against AMC_NAME_MAP")
    args = parser.parse_args()
    sys.exit(main(args.codes, args.local, args.amc_check))
