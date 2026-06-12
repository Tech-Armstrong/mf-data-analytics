"""
sif/config/duckdb_session.py

The "serverless DB" bootstrap for SIF — mirror of config/duckdb_session.py.

There is no materialised .duckdb file. Every consumer opens an in-memory
DuckDB connection, loads the `azure` extension, authenticates with the Blob
connection string, and creates VIEWS over the parquet files in the SIF Blob
container. DuckDB reads those parquet files directly over the network.

The SIF parquet has the identical schema to MF (nav_history is
scheme_code/nav_date/nav; scheme_master is scheme_code/fund_house/category/
scheme_name), so the view DDL is the same — only the az:// globs differ
(they come from sif.config.constants).

Schema exposed (all VIEWS over Blob parquet):
    scheme_master       -- (scheme_code, fund_house, category, scheme_name)
    nav_history         -- every (scheme_code, nav_date, nav) joined to labels
    latest_nav          -- most recent NAV per scheme, fully labelled
    category_summary    -- latest NAV aggregated by category
    fund_house_summary  -- latest NAV aggregated by fund house

Usage:
    from sif.config.duckdb_session import get_connection
    with get_connection() as con:
        con.execute("SELECT * FROM latest_nav").fetchall()
"""

import duckdb

from sif.config.constants import (
    AZURE_CONNECTION_STRING,
    AZ_NAV_HISTORY_GLOB,
    AZ_SCHEME_MASTER,
    NAV_HISTORY_PARQUET,
    SCHEME_MASTER_PARQUET,
)
from sif.config.logging_utils import get_logger

log = get_logger("duckdb_session")


def _view_sql(nav_source: str, master_source: str) -> str:
    """Build the view DDL against the given nav_history / scheme_master sources.

    `nav_source` and `master_source` are already-quoted read_parquet() arguments
    (an az:// glob for Blob, or a local file path for --local inspection).
    """
    return f"""
CREATE OR REPLACE VIEW scheme_master AS
SELECT
    scheme_code,
    fund_house,
    category,
    scheme_name
FROM read_parquet({master_source});

CREATE OR REPLACE VIEW nav_history AS
SELECT
    h.scheme_code,
    h.nav_date::DATE  AS nav_date,
    h.nav::DOUBLE     AS nav,
    s.fund_house,
    s.category,
    s.scheme_name
FROM read_parquet({nav_source}) h
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
    arbitrary Blob parquet.

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
    Open a fresh in-memory DuckDB wired to the SIF Blob parquet, with all views
    created. The caller owns the connection and should use it as a context
    manager.
    """
    con = _connect_azure()
    nav_source = f"'{AZ_NAV_HISTORY_GLOB}', hive_partitioning = true"
    master_source = f"'{AZ_SCHEME_MASTER}'"
    con.execute(_view_sql(nav_source, master_source))
    return con


def get_local_connection() -> duckdb.DuckDBPyConnection:
    """
    Open a fresh in-memory DuckDB wired to the STAGED LOCAL parquet
    (sif/data/processed/...), with the same views. For inspecting a `--local`
    fetch before publishing to Blob. No Azure credentials required.

    Raises FileNotFoundError if the local files are not present.
    """
    if not NAV_HISTORY_PARQUET.exists() or not SCHEME_MASTER_PARQUET.exists():
        raise FileNotFoundError(
            "Local SIF parquet not found. Run a fetch with --local first, e.g.\n"
            "  python -m sif.scripts.fetch.fetch_sif_history --local"
        )
    con = duckdb.connect(database=":memory:")
    nav_source = f"'{NAV_HISTORY_PARQUET.as_posix()}'"
    master_source = f"'{SCHEME_MASTER_PARQUET.as_posix()}'"
    con.execute(_view_sql(nav_source, master_source))
    return con
