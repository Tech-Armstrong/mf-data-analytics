"""
scripts/agent/tools.py

Tool functions for the MF analytics agent.

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
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Union

import duckdb

from config.duckdb_session import get_connection

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


def _nearest_nav(con: duckdb.DuckDBPyConnection, scheme_code: str, target_date: date) -> tuple:
    """
    Return (nav_date, nav) for the trading day on or immediately before target_date.
    Handles weekends / holidays by walking backward up to 10 calendar days.
    """
    row = con.execute("""
        SELECT nav_date, nav
        FROM   nav_history
        WHERE  scheme_code = ?
          AND  nav_date    <= ?
        ORDER BY nav_date DESC
        LIMIT 1
    """, [scheme_code, target_date]).fetchone()
    return row  # (nav_date, nav) or None


_VALID_PERIODS = {
    "1W", "2W",
    "1M", "3M", "6M", "9M",
    "1Y", "2Y", "3Y", "5Y",
    "YTD", "MTD", "SI",
}

def _resolve_period(period: str) -> tuple[date, date]:
    """
    Convert a period string to (start_date, end_date) anchored to today.

    Raises ValueError for unrecognised period strings.
    """
    p = period.upper().strip()
    today = date.today()

    if p == "YTD":
        return date(today.year, 1, 1), today
    if p == "MTD":
        return date(today.year, today.month, 1), today
    if p == "SI":
        # Caller must handle SI at the per-fund level (earliest NAV date).
        # Returning date.min signals that to get_fund_returns.
        return date.min, today

    if p.endswith("W") and p[:-1].isdigit():
        weeks = int(p[:-1])
        return today - timedelta(weeks=weeks), today
    if p.endswith("M") and p[:-1].isdigit():
        months = int(p[:-1])
        return today - relativedelta(months=months), today
    if p.endswith("Y") and p[:-1].isdigit():
        years = int(p[:-1])
        return today - relativedelta(years=years), today

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

    start_date, end_date = _resolve_period(period)
    si_mode = (period.upper().strip() == "SI")

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

            # SI: use the fund's own earliest NAV date as start
            effective_start = start_date
            if si_mode:
                row = con.execute("""
                    SELECT MIN(nav_date) FROM nav_history WHERE scheme_code = ?
                """, [code]).fetchone()
                effective_start = row[0] if row and row[0] else start_date

            start_row = _nearest_nav(con, code, effective_start)
            end_row   = _nearest_nav(con, code, end_date)

            if not start_row:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "fund_house": fund_house, "category": category,
                    "start_nav_date": None, "start_nav": None,
                    "end_nav_date": None, "end_nav": None,
                    "return_pct": None,
                    "error": f"No NAV data on or before {effective_start}",
                })
                continue

            if not end_row:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "fund_house": fund_house, "category": category,
                    "start_nav_date": start_row[0], "start_nav": start_row[1],
                    "end_nav_date": None, "end_nav": None,
                    "return_pct": None,
                    "error": f"No NAV data on or before {end_date}",
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
        "period":       period.upper().strip(),
        "period_start": str(start_date) if not si_mode else "fund-inception",
        "period_end":   str(end_date),
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

    start_date, end_date = _resolve_period(period)

    if not rows:
        return {
            "category":       category.upper(),
            "period":         period.upper().strip(),
            "period_start":   str(start_date),
            "period_end":     str(end_date),
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
