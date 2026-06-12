"""
sif/scripts/agent/tools.py

Tool functions for the SIF analytics agent.

Identical to scripts/agent/tools.py (the returns math, period resolution, and
the last-NAV anchor convention are asset-agnostic) except it reads from the SIF
serverless DB via sif.config.duckdb_session.

Each function is self-contained: it opens a read-only DuckDB connection,
runs the query, and returns a structured dict the agent can reason over.

Available tools
---------------
get_fund_returns(scheme_codes, period)
    Point-to-point return for one or many funds over a named period.

get_category_returns(category, period)
    Point-to-point returns for every fund in a category.

list_categories()
    Enumerate valid category names stored in the DB.

list_funds_in_category(category)
    List all schemes in a given category with their scheme_code.

Supported period strings
------------------------
    1W, 2W                        — weeks
    1M, 3M, 6M, 9M               — months
    1Y, 2Y, 3Y, 5Y               — years
    YTD                          — year-to-date (Jan 1 → today)
    MTD                          — month-to-date (1st → today)
    SI  (since inception)        — earliest available NAV → today
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Union

import duckdb

from sif.config.duckdb_session import get_connection

# ── helpers ───────────────────────────────────────────────────────────────────

def _connect() -> duckdb.DuckDBPyConnection:
    """
    In-memory DuckDB wired to the parquet in Azure Blob (the serverless DB).

    The connection exposes read-only VIEWS over Blob parquet — the agent tools
    only ever run SELECTs, so there is nothing to mutate.
    """
    return get_connection()


def _point_to_point_return(nav_start: float, nav_end: float) -> float:
    """Absolute return (%) from start NAV to end NAV."""
    if nav_start == 0:
        return None
    return round((nav_end - nav_start) / nav_start * 100, 4)


def _nav_on_or_before(con: duckdb.DuckDBPyConnection, scheme_code: str, target_date: date) -> tuple:
    """
    Return (nav_date, nav) for the trading day on or immediately before target_date,
    walking backward over weekends/holidays. Used for the END of a window.
    """
    return con.execute("""
        SELECT nav_date, nav
        FROM   nav_history
        WHERE  scheme_code = ?
          AND  nav_date    <= ?
        ORDER BY nav_date DESC
        LIMIT 1
    """, [scheme_code, target_date]).fetchone()  # (nav_date, nav) or None


def _nav_on_or_after(con: duckdb.DuckDBPyConnection, scheme_code: str, target_date: date) -> tuple:
    """
    Return (nav_date, nav) for the trading day on or immediately after target_date,
    walking forward over weekends/holidays. Used for the START of a window.
    """
    return con.execute("""
        SELECT nav_date, nav
        FROM   nav_history
        WHERE  scheme_code = ?
          AND  nav_date    >= ?
        ORDER BY nav_date ASC
        LIMIT 1
    """, [scheme_code, target_date]).fetchone()  # (nav_date, nav) or None


_VALID_PERIODS = {
    "1W", "2W",
    "1M", "3M", "6M", "9M",
    "1Y", "2Y", "3Y", "5Y",
    "YTD", "MTD", "SI",
}

def _resolve_period(period: str, anchor: date) -> tuple[date, date]:
    """
    Convert a period string to (start_target, end_target) calendar dates,
    anchored to `anchor` (the fund's LAST available NAV date — NOT today).

    For trailing periods the start_target is exclusive: it is (anchor - period),
    and the caller snaps FORWARD to the first NAV strictly after it. This matches
    the industry convention (ET Money / Value Research / Google Finance): a "1M"
    return runs from the NAV just after one month ago to the latest NAV.

    Raises ValueError for unrecognised period strings.
    """
    p = period.upper().strip()

    if p == "YTD":
        return date(anchor.year, 1, 1), anchor
    if p == "MTD":
        return date(anchor.year, anchor.month, 1), anchor
    if p == "SI":
        # Caller handles SI at the per-fund level (earliest NAV date).
        return date.min, anchor

    if p.endswith("W") and p[:-1].isdigit():
        return anchor - timedelta(weeks=int(p[:-1])), anchor
    if p.endswith("M") and p[:-1].isdigit():
        return anchor - relativedelta(months=int(p[:-1])), anchor
    if p.endswith("Y") and p[:-1].isdigit():
        return anchor - relativedelta(years=int(p[:-1])), anchor

    raise ValueError(
        f"Unrecognised period '{period}'. "
        f"Valid values: {', '.join(sorted(_VALID_PERIODS))}"
    )


# ── public tools ─────────────────────────────────────────────────────────────

def get_fund_returns(
    scheme_codes: Union[str, list[str]],
    period: str,
) -> dict:
    """
    Calculate point-to-point returns for one or many funds.

    Parameters
    ----------
    scheme_codes : str or list[str]
        One scheme_code or a list of them.
    period : str
        Named time window. Supported values:
            1W, 2W, 1M, 3M, 6M, 9M, 1Y, 2Y, 3Y, 5Y, YTD, MTD, SI
        SI (since inception) uses each fund's earliest available NAV date.

    Returns
    -------
    {
        "period":       str,
        "period_start": str,   # resolved calendar date
        "period_end":   str,   # resolved calendar date (today)
        "results": [
            {
                "scheme_code":    str,
                "scheme_name":    str,
                "fund_house":     str,
                "category":       str,
                "start_nav_date": date,
                "start_nav":      float,
                "end_nav_date":   date,
                "end_nav":        float,
                "return_pct":     float | None,
                "error":          str | None,
            },
            ...
        ]
    }
    """
    if isinstance(scheme_codes, str):
        scheme_codes = [scheme_codes]

    period = period.upper().strip()
    si_mode  = (period == "SI")
    ytd_mtd  = period in ("YTD", "MTD")

    results = []
    with _connect() as con:
        for code in scheme_codes:
            meta = con.execute("""
                SELECT scheme_name, fund_house, category
                FROM   scheme_master
                WHERE  scheme_code = ?
            """, [code]).fetchone()

            if not meta:
                results.append({
                    "scheme_code": code,
                    "scheme_name": None,
                    "fund_house":  None,
                    "category":    None,
                    "start_nav_date": None,
                    "start_nav":     None,
                    "end_nav_date":  None,
                    "end_nav":       None,
                    "return_pct":    None,
                    "error":         f"scheme_code '{code}' not found in scheme_master",
                })
                continue

            scheme_name, fund_house, category = meta

            # Anchor the window to the fund's LAST available NAV date, not today.
            # The end of every window is that latest NAV; published returns
            # (ET Money / Value Research / Google) all anchor here, not on the
            # calendar 'today' which may be a non-trading or not-yet-published day.
            last = con.execute("""
                SELECT MAX(nav_date) FROM nav_history WHERE scheme_code = ?
            """, [code]).fetchone()
            anchor = last[0] if last else None
            if anchor is None:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "fund_house": fund_house, "category": category,
                    "start_nav_date": None, "start_nav": None,
                    "end_nav_date": None, "end_nav": None,
                    "return_pct": None,
                    "error": "No NAV data for this scheme",
                })
                continue

            start_target, end_target = _resolve_period(period, anchor)

            # SI: start at the fund's earliest NAV (inclusive).
            if si_mode:
                row = con.execute("""
                    SELECT MIN(nav_date) FROM nav_history WHERE scheme_code = ?
                """, [code]).fetchone()
                start_target = row[0] if row and row[0] else start_target

            # END = latest NAV on/before the anchor (== anchor itself).
            end_row = _nav_on_or_before(con, code, end_target)

            # START snap:
            #  - trailing periods (1M/3M/.../1Y): the boundary day is EXCLUSIVE,
            #    so snap FORWARD to the first NAV strictly after (anchor - period).
            #  - YTD/MTD/SI: the boundary is INCLUSIVE (Jan 1 / month 1st /
            #    inception), so snap forward to the first NAV on/after it.
            if si_mode or ytd_mtd:
                start_row = _nav_on_or_after(con, code, start_target)
            else:
                start_row = _nav_on_or_after(con, code, start_target + timedelta(days=1))

            if not start_row:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "fund_house": fund_house, "category": category,
                    "start_nav_date": None, "start_nav": None,
                    "end_nav_date": end_row[0] if end_row else None,
                    "end_nav": end_row[1] if end_row else None,
                    "return_pct": None,
                    "error": f"No NAV data on or after {start_target}",
                })
                continue

            if not end_row:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "fund_house": fund_house, "category": category,
                    "start_nav_date": start_row[0], "start_nav": start_row[1],
                    "end_nav_date": None, "end_nav": None,
                    "return_pct": None,
                    "error": f"No NAV data on or before {end_target}",
                })
                continue

            ret = _point_to_point_return(start_row[1], end_row[1])
            results.append({
                "scheme_code":    code,
                "scheme_name":    scheme_name,
                "fund_house":     fund_house,
                "category":       category,
                "start_nav_date": start_row[0],
                "start_nav":      start_row[1],
                "end_nav_date":   end_row[0],
                "end_nav":        end_row[1],
                "return_pct":     ret,
                "error":          None,
            })

    return {
        "period":       period,
        "period_start": "fund-inception" if si_mode else "per-fund-last-nav-minus-period",
        "period_end":   "per-fund-last-nav",
        "results":      results,
    }


def get_category_returns(
    category: str,
    period: str,
    sort_by: str = "return_pct",
    ascending: bool = False,
) -> dict:
    """
    Calculate point-to-point returns for every fund in a category.

    Parameters
    ----------
    category  : str
        Category name (case-insensitive).
    period    : str
        Named time window — same values as get_fund_returns.
    sort_by   : str
        One of: 'return_pct', 'scheme_name', 'fund_house', 'start_nav',
        'end_nav'. Default: 'return_pct'.
    ascending : bool
        Sort direction. Default: False (highest return first).

    Returns
    -------
    {
        "category":       str,
        "period":         str,
        "period_start":   str,
        "period_end":     str,
        "total_funds":    int,
        "computed":       int,
        "avg_return_pct": float | None,
        "results":        [ ...same shape as get_fund_returns results... ]
    }
    """
    with _connect() as con:
        rows = con.execute("""
            SELECT scheme_code
            FROM   scheme_master
            WHERE  UPPER(category) = UPPER(?)
            ORDER BY scheme_name
        """, [category]).fetchall()

    if not rows:
        return {
            "category":       category.upper(),
            "period":         period.upper().strip(),
            "period_start":   "per-fund-last-nav-minus-period",
            "period_end":     "per-fund-last-nav",
            "total_funds":    0,
            "computed":       0,
            "avg_return_pct": None,
            "results":        [],
            "error":          f"No funds found for category '{category}'. "
                              "Use list_categories() to see valid names.",
        }

    scheme_codes = [r[0] for r in rows]
    payload = get_fund_returns(scheme_codes, period)
    results = payload["results"]

    valid_sort_cols = {"return_pct", "scheme_name", "fund_house", "start_nav", "end_nav"}
    if sort_by not in valid_sort_cols:
        sort_by = "return_pct"

    # None values always go last; sort valid entries in the requested direction
    none_rows  = [r for r in results if r[sort_by] is None]
    valid_rows = [r for r in results if r[sort_by] is not None]
    valid_rows.sort(key=lambda r: r[sort_by], reverse=not ascending)
    results = valid_rows + none_rows

    computed = [r for r in results if r["return_pct"] is not None]
    avg = round(sum(r["return_pct"] for r in computed) / len(computed), 4) if computed else None

    return {
        "category":       category.upper(),
        "period":         payload["period"],
        "period_start":   payload["period_start"],
        "period_end":     payload["period_end"],
        "total_funds":    len(results),
        "computed":       len(computed),
        "avg_return_pct": avg,
        "results":        results,
    }


def list_categories() -> dict:
    """
    Return all distinct category names present in scheme_master.

    Returns
    -------
    {
        "categories": [str, ...]   # sorted alphabetically
    }
    """
    with _connect() as con:
        rows = con.execute("""
            SELECT DISTINCT category
            FROM   scheme_master
            WHERE  category IS NOT NULL
            ORDER BY category
        """).fetchall()
    return {"categories": [r[0] for r in rows]}


def list_funds_in_category(category: str) -> dict:
    """
    List every scheme in the given category.

    Parameters
    ----------
    category : str
        Category name (case-insensitive).

    Returns
    -------
    {
        "category": str,
        "total":    int,
        "funds": [
            {
                "scheme_code": str,
                "scheme_name": str,
                "fund_house":  str,
            },
            ...
        ]
    }
    """
    with _connect() as con:
        rows = con.execute("""
            SELECT scheme_code, scheme_name, fund_house
            FROM   scheme_master
            WHERE  UPPER(category) = UPPER(?)
            ORDER BY fund_house, scheme_name
        """, [category]).fetchall()

    funds = [
        {"scheme_code": r[0], "scheme_name": r[1], "fund_house": r[2]}
        for r in rows
    ]
    return {
        "category": category.upper(),
        "total":    len(funds),
        "funds":    funds,
    }
