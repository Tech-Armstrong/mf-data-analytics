"""
scripts/agent/tools_returns_demo.py

Quick display of returns for two specific funds via the agent tools.
Calls get_fund_returns for scheme_codes 103166 and 101592 across a range of
periods and prints them in a readable table.

Run:
    python -m scripts.agent.tools_returns_demo
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.agent.tools import get_fund_returns

SCHEME_CODES = ["103166", "101592"]
PERIODS = ["1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y", "SI"]


def main() -> int:
    # Header line per fund is taken from the first successful result.
    print(f"\nFund returns  —  scheme codes: {', '.join(SCHEME_CODES)}\n")

    for code in SCHEME_CODES:
        rows = []
        name = fund_house = category = None

        for period in PERIODS:
            payload = get_fund_returns(code, period)
            res = payload["results"][0]
            name       = name or res.get("scheme_name")
            fund_house = fund_house or res.get("fund_house")
            category   = category or res.get("category")
            rows.append((period, res, payload))

        title = name or f"scheme_code {code}"
        print(f"── {title}  ({code})")
        if fund_house or category:
            print(f"   {fund_house or '?'}  |  {category or '?'}")
        print()
        print(f"   {'Period':<8}{'Start NAV':>12}{'End NAV':>12}{'Return %':>12}   Window")
        print(f"   {'-'*8}{'-'*12:>12}{'-'*12:>12}{'-'*12:>12}   {'-'*24}")

        for period, res, payload in rows:
            if res["error"]:
                print(f"   {period:<8}{'—':>12}{'—':>12}{'—':>12}   {res['error']}")
                continue
            window = f"{res['start_nav_date']} → {res['end_nav_date']}"
            ret = res["return_pct"]
            ret_str = f"{ret:>11.2f}" if ret is not None else f"{'—':>12}"
            print(
                f"   {period:<8}"
                f"{res['start_nav']:>12.4f}"
                f"{res['end_nav']:>12.4f}"
                f"{ret_str}   {window}"
            )
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
