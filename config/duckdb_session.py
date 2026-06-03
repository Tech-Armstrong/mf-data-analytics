"""
config/duckdb_session.py

The "serverless DB" bootstrap.

There is no materialised .duckdb file. Instead, every consumer opens an
in-memory DuckDB connection, loads the `azure` extension, authenticates with
the Blob connection string, and creates VIEWS over the parquet files that live
in Azure Blob Storage. DuckDB reads those parquet files directly over the
network (HTTP range requests — only the row-groups/columns a query needs).

Schema exposed (all VIEWS over Blob parquet):
    scheme_master       -- the fund universe (scheme_code, fund_house, category, scheme_name)
    nav_history         -- every (scheme_code, nav_date, nav) joined to scheme_master labels
    latest_nav          -- most recent NAV per scheme, fully labelled
    category_summary    -- latest NAV aggregated by category
    fund_house_summary  -- latest NAV aggregated by fund house

Usage:
    from config.duckdb_session import get_connection
    with get_connection() as con:
        con.execute("SELECT * FROM latest_nav").fetchall()
"""

import duckdb

from config.constants import (
    AZURE_CONNECTION_STRING,
    AZ_NAV_HISTORY_GLOB,
    AZ_SCHEME_MASTER,
)
from config.logging_utils import get_logger

log = get_logger("duckdb_session")


# ── View DDL (single source of truth — was previously in build_duckdb.py) ──────
# These run against an in-memory DB and reference the Blob parquet directly.
# nav_history is read with hive_partitioning so the year=YYYY dirs are pruned
# when a query filters on nav_date's year.

_VIEW_SQL = f"""
CREATE OR REPLACE VIEW scheme_master AS
SELECT
    scheme_code,
    fund_house,
    category,
    scheme_name
FROM read_parquet('{AZ_SCHEME_MASTER}');

CREATE OR REPLACE VIEW nav_history AS
SELECT
    h.scheme_code,
    h.nav_date::DATE  AS nav_date,
    h.nav::DOUBLE     AS nav,
    s.fund_house,
    s.category,
    s.scheme_name
FROM read_parquet('{AZ_NAV_HISTORY_GLOB}', hive_partitioning = true) h
LEFT JOIN scheme_master s USING (scheme_code);

CREATE OR REPLACE VIEW latest_nav AS
SELECT
    h.scheme_code,
    h.fund_house,
    h.category,
    h.scheme_name,
    h.nav_date,
    h.nav
FROM nav_history h
INNER JOIN (
    SELECT scheme_code, MAX(nav_date) AS max_date
    FROM nav_history
    GROUP BY scheme_code
) latest
    ON h.scheme_code = latest.scheme_code
   AND h.nav_date    = latest.max_date;

CREATE OR REPLACE VIEW category_summary AS
SELECT
    category,
    COUNT(DISTINCT scheme_code)     AS total_schemes,
    ROUND(AVG(nav), 4)              AS avg_latest_nav,
    ROUND(MIN(nav), 4)              AS min_latest_nav,
    ROUND(MAX(nav), 4)              AS max_latest_nav,
    MAX(nav_date)                   AS data_as_of
FROM latest_nav
WHERE category IS NOT NULL
GROUP BY category
ORDER BY category;

CREATE OR REPLACE VIEW fund_house_summary AS
SELECT
    fund_house,
    COUNT(DISTINCT category)        AS categories_present,
    COUNT(DISTINCT scheme_code)     AS total_schemes,
    STRING_AGG(DISTINCT category, ', ' ORDER BY category) AS categories,
    MAX(nav_date)                   AS data_as_of
FROM latest_nav
WHERE fund_house IS NOT NULL
GROUP BY fund_house
ORDER BY fund_house;
"""


def _connect_azure() -> duckdb.DuckDBPyConnection:
    """
    In-memory DuckDB with the azure extension loaded and the connection-string
    secret registered — but WITHOUT the views. Use this for ad-hoc reads of
    arbitrary Blob parquet (e.g. rebuilding from the raw layer).

    Raises RuntimeError if AZURE_STORAGE_CONNECTION_STRING is not set.
    """
    if not AZURE_CONNECTION_STRING:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING is not set. "
            "Add it to your local .env file (gitignored) or, in CI, as a "
            "GitHub Actions secret. The serverless query layer reads parquet "
            "from Azure Blob and cannot run without it."
        )

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL azure;")
    con.execute("LOAD azure;")
    con.execute(
        "CREATE SECRET az (TYPE azure, CONNECTION_STRING ?);",
        [AZURE_CONNECTION_STRING],
    )
    return con


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Open a fresh in-memory DuckDB wired to the Blob parquet, with all views
    created (scheme_master, nav_history, latest_nav, category_summary,
    fund_house_summary). The caller owns the connection and should use it as a
    context manager.
    """
    con = _connect_azure()
    con.execute(_VIEW_SQL)
    return con
