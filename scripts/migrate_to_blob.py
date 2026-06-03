"""
scripts/migrate_to_blob.py

ONE-TIME migration: push the existing local parquet up to Azure Blob in the
layout the serverless query layer expects.

What it does
------------
1. scheme_master.parquet            → az://<container>/processed/scheme_master.parquet
2. nav_history.parquet (flat)       → split by year → az://.../processed/nav_history/year=YYYY/data.parquet
3. raw/nav/year=YYYY/*.parquet      → az://.../raw/nav/year=YYYY/<same filename>   (optional audit trail)

Run once after creating the Blob container and setting AZURE_STORAGE_CONNECTION_STRING.

Usage:
    python -m scripts.migrate_to_blob
    python -m scripts.migrate_to_blob --skip-raw      # processed layer only
"""

import sys
import argparse
from datetime import date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from config.constants import (
    RAW_NAV_DIR,
    NAV_HISTORY_PARQUET,
    SCHEME_MASTER_PARQUET,
    BLOB_RAW_PREFIX,
    BLOB_NAV_HISTORY_DIR,
    BLOB_SCHEME_MASTER,
)
from config.blob_io import upload_bytes, to_parquet_bytes
from config.logging_utils import get_logger

log = get_logger("migrate_to_blob")

# A fund counts as "deep history" if its earliest NAV is on/before this date.
# The dataset splits cleanly: deep funds start 2021-05; thin (un-backfilled)
# funds start 2026-05. Anything older than this cutoff is real history.
DEEP_HISTORY_CUTOFF = date(2025, 1, 1)


def _load_universe() -> list[str]:
    """
    The migration scope: only funds that BOTH
      (a) are in the labelled universe (scheme_master), AND
      (b) have deep history (earliest NAV on/before DEEP_HISTORY_CUTOFF).

    This drops the ~28 AMCs that only have a few days of data — every fund we
    migrate is fully usable for any return period.
    """
    if not SCHEME_MASTER_PARQUET.exists():
        log.error("scheme_master.parquet not found at %s", SCHEME_MASTER_PARQUET)
        sys.exit(1)
    if not NAV_HISTORY_PARQUET.exists():
        log.error("nav_history.parquet not found at %s", NAV_HISTORY_PARQUET)
        sys.exit(1)

    labelled = set(
        pl.read_parquet(SCHEME_MASTER_PARQUET)
        .with_columns(pl.col("scheme_code").cast(pl.Utf8))["scheme_code"]
        .unique()
        .to_list()
    )

    starts = (
        pl.read_parquet(NAV_HISTORY_PARQUET)
        .with_columns(pl.col("scheme_code").cast(pl.Utf8))
        .group_by("scheme_code")
        .agg(pl.col("nav_date").min().alias("first"))
    )
    deep = set(
        starts.filter(pl.col("first") <= DEEP_HISTORY_CUTOFF)["scheme_code"].to_list()
    )

    universe = sorted(labelled & deep)
    log.info(
        "Universe: %d labelled funds -> %d with deep history (cutoff %s)",
        len(labelled), len(universe), DEEP_HISTORY_CUTOFF,
    )
    return universe


def migrate_scheme_master(universe: list[str]) -> None:
    """Upload scheme_master, restricted to the migrated (deep-history) funds."""
    if not SCHEME_MASTER_PARQUET.exists():
        log.error("scheme_master.parquet not found at %s", SCHEME_MASTER_PARQUET)
        sys.exit(1)
    sm = pl.read_parquet(SCHEME_MASTER_PARQUET).with_columns(
        pl.col("scheme_code").cast(pl.Utf8)
    )
    sm = sm.filter(pl.col("scheme_code").is_in(universe))
    upload_bytes(to_parquet_bytes(sm), BLOB_SCHEME_MASTER)
    log.info("scheme_master uploaded: %d funds", sm.height)


def migrate_nav_history(universe: list[str]) -> None:
    if not NAV_HISTORY_PARQUET.exists():
        log.error("nav_history.parquet not found at %s", NAV_HISTORY_PARQUET)
        sys.exit(1)

    df = pl.read_parquet(NAV_HISTORY_PARQUET).with_columns(
        pl.col("scheme_code").cast(pl.Utf8)
    )

    # Scope: only the deep-history labelled funds (see _load_universe). Excludes
    # both the ~8,600 unlabelled schemes and the ~28 AMCs with only a few days
    # of data — every migrated fund is fully usable for any return period.
    before = df.height
    df = df.filter(pl.col("scheme_code").is_in(universe))
    log.info(
        "Filtered to labelled universe: %d → %d rows (%d schemes)",
        before, df.height, df["scheme_code"].n_unique(),
    )

    df = df.with_columns(pl.col("nav_date").dt.year().alias("year"))
    years = sorted(df["year"].unique().to_list())
    log.info("Splitting nav_history into %d year partitions: %s", len(years), years)

    for year_val in years:
        part = df.filter(pl.col("year") == year_val).drop("year")
        blob_path = f"{BLOB_NAV_HISTORY_DIR}/year={year_val}/data.parquet"
        upload_bytes(to_parquet_bytes(part), blob_path)
        log.info("  year=%s -> %d rows", year_val, len(part))


def migrate_raw(universe: list[str]) -> None:
    files = sorted(RAW_NAV_DIR.glob("year=*/*.parquet"))
    if not files:
        log.warning("No raw parquet files found under %s — skipping raw.", RAW_NAV_DIR)
        return
    log.info("Uploading %d raw files (filtered to labelled universe)...", len(files))
    for f in files:
        part = pl.read_parquet(f).with_columns(pl.col("scheme_code").cast(pl.Utf8))
        part = part.filter(pl.col("scheme_code").is_in(universe))
        if part.is_empty():
            continue  # this raw file held only unlabelled schemes
        # preserve year=YYYY/<filename> structure under the raw prefix
        blob_path = f"{BLOB_RAW_PREFIX}/{f.parent.name}/{f.name}"
        upload_bytes(to_parquet_bytes(part), blob_path)


def main(skip_raw: bool = False) -> None:
    log.info("-------------------------------------------")
    log.info("Migrating local parquet -> Azure Blob (deep-history labelled funds)")
    log.info("-------------------------------------------")

    universe = _load_universe()

    migrate_scheme_master(universe)
    migrate_nav_history(universe)
    if not skip_raw:
        migrate_raw(universe)

    log.info("-------------------------------------------")
    log.info("Migration complete. Verify with: python -m scripts.agent.tools_smoke")
    log.info("-------------------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="One-time upload of local parquet to Azure Blob")
    parser.add_argument("--skip-raw", action="store_true", help="Upload processed layer only")
    args = parser.parse_args()
    main(skip_raw=args.skip_raw)
