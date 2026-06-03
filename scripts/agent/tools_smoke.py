"""
scripts/agent/tools_smoke.py

Smoke test for the agent tools against the serverless DuckDB-over-Blob layer.
Exercises every tool plus the tricky paths (multiple periods, SI, errors).

Run:
    python -m scripts.agent.tools_smoke
Exits non-zero if any check fails.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.agent.tools import (
    list_categories,
    list_funds_in_category,
    get_fund_returns,
    get_category_returns,
)

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))


def main() -> int:
    # 1. list_categories
    cats = list_categories()["categories"]
    check("list_categories returns 8 equity categories", len(cats) == 8, str(cats))

    # 2. list_funds_in_category
    first_cat = cats[0]
    funds = list_funds_in_category(first_cat)
    has_funds = funds["total"] > 0 and all(
        f.get("scheme_code") and f.get("scheme_name") for f in funds["funds"]
    )
    check(f"list_funds_in_category('{first_cat}') returns funds", has_funds,
          f"{funds['total']} funds")

    # 3. get_fund_returns for one fund across several periods
    sample_code = funds["funds"][0]["scheme_code"]
    sample_name = funds["funds"][0]["scheme_name"]
    for period in ["1M", "1Y", "3Y", "SI", "YTD"]:
        r = get_fund_returns(sample_code, period)
        res = r["results"][0]
        ok = res["error"] is None and res["return_pct"] is not None
        check(f"get_fund_returns({period}) for sample fund", ok,
              f"{res.get('return_pct')}% [{res.get('error')}]")

    # 4. get_fund_returns with a bad code -> graceful error, no crash
    bad = get_fund_returns("000000", "1Y")["results"][0]
    check("get_fund_returns(bad code) returns error not crash",
          bad["error"] is not None, bad["error"])

    # 5. get_category_returns for every category (1Y)
    for cat in cats:
        cr = get_category_returns(cat, "1Y")
        ok = cr["total_funds"] > 0 and cr["computed"] > 0 and cr["avg_return_pct"] is not None
        check(f"get_category_returns('{cat}', 1Y)", ok,
              f"{cr['computed']}/{cr['total_funds']} funds, avg {cr['avg_return_pct']}%")

    # 6. get_category_returns for a bogus category -> graceful
    bogus = get_category_returns("NOT A CATEGORY", "1Y")
    check("get_category_returns(bogus) handled", bogus["total_funds"] == 0,
          bogus.get("error", ""))

    # ── report ────────────────────────────────────────────────────────────────
    print(f"\nAgent tools smoke test  —  sample fund: {sample_name} ({sample_code})\n")
    width = max(len(n) for _, n, _ in results)
    for status, name, detail in results:
        print(f"  [{status}] {name:<{width}}  {detail}")

    failed = [r for r in results if r[0] == FAIL]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
