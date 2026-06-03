"""
scripts/processing/rebuild_processed.py

Rebuilds the processed nav_history layer on Blob FROM the raw layer on Blob.

Reads all raw/nav/year=*/*.parquet straight from Azure Blob with DuckDB,
deduplicates by (scheme_code, nav_date), and writes one partition per year
back to processed/nav_history/year=YYYY/data.parquet.

Use this if the processed layer is ever lost or needs a full re-derive from raw.
Normal daily operation does NOT need this — daily_nav_update maintains the
current year's partition incrementally.

Usage:
    python -m scripts.processing.rebuild_processed
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import polars as pl

from config.constants import (
    BLOB_CONTAINER,
    BLOB_RAW_PREFIX,
    BLOB_NAV_HISTORY_DIR,
)
from config.blob_io import upload_bytes, to_parquet_bytes
from config.duckdb_session import _connect_azure
from config.logging_utils import get_logger

log = get_logger("rebuild_processed")

RAW_GLOB = f"az://{BLOB_CONTAINER}/{BLOB_RAW_PREFIX}/year=*/*.parquet"


def load_raw_from_blob() -> pl.DataFrame:
    """Read every raw partition from Blob via DuckDB, return as a Polars frame."""
    con = _connect_azure()
    try:
        # raw files hold only (scheme_code, nav_date, nav)
        arrow_tbl = con.execute(
            f"SELECT scheme_code, nav_date, nav "
            f"FROM read_parquet('{RAW_GLOB}', hive_partitioning = true, union_by_name = true)"
        ).arrow()
    finally:
        con.close()
    df = pl.from_arrow(arrow_tbl)
    log.info("Loaded %d raw rows from Blob", len(df))
    return df


def build_nav_history(df: pl.DataFrame) -> pl.DataFrame:
    before = len(df)
    deduped = (
        df
        .with_columns(
            pl.col("scheme_code").cast(pl.Utf8),
            pl.col("nav_date").cast(pl.Date),
            pl.col("nav").cast(pl.Float64),
        )
        .sort("nav_date", descending=True)
        .unique(subset=["scheme_code", "nav_date"], keep="first")
        .sort(["scheme_code", "nav_date"])
    )
    dropped = before - len(deduped)
    if dropped:
        log.info("Deduplicated: %d → %d rows (%d duplicates dropped)", before, len(deduped), dropped)
    return deduped


def write_partitions(df: pl.DataFrame) -> None:
    df = df.with_columns(pl.col("nav_date").dt.year().alias("year"))
    for (year_val,), group in df.group_by("year"):
        group = group.drop("year")
        blob_path = f"{BLOB_NAV_HISTORY_DIR}/year={year_val}/data.parquet"
        upload_bytes(to_parquet_bytes(group), blob_path)
        log.info("  nav_history year=%s → %d rows", year_val, len(group))


def main() -> None:
    log.info("-------------------------------------------")
    log.info("Rebuild nav_history (Blob) from raw layer (Blob)")
    log.info("-------------------------------------------")

    raw_df = load_raw_from_blob()
    if raw_df.is_empty():
        log.error("No raw rows found at %s", RAW_GLOB)
        sys.exit(1)

    nav_df = build_nav_history(raw_df)
    write_partitions(nav_df)

    log.info(
        "Done: %d rows | %d schemes | %s to %s",
        len(nav_df),
        nav_df["scheme_code"].n_unique(),
        str(nav_df["nav_date"].min()),
        str(nav_df["nav_date"].max()),
    )
    log.info("-------------------------------------------")


if __name__ == "__main__":
    main()
