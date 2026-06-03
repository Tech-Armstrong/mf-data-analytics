"""
scripts/processing/build_scheme_master.py

Builds data/processed/scheme_master.parquet directly from the fund universe
defined in TEST_PROCESSING.py (FUND-CATEGORY-MAPPING.md).

No API calls needed. Run once, or re-run whenever the fund universe changes.

Usage:
    python -m scripts.processing.build_scheme_master
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import polars as pl

from config.constants import BLOB_SCHEME_MASTER
from config.blob_io import upload_bytes, to_parquet_bytes
from config.logging_utils import get_logger
from scripts.processing.fund_universe import FUND_UNIVERSE

log = get_logger("build_scheme_master")


def build_scheme_master() -> pl.DataFrame:
    rows = []
    for category, funds in FUND_UNIVERSE.items():
        for scheme_code, fund_house, scheme_name in funds:
            rows.append({
                "scheme_code": scheme_code,
                "fund_house":  fund_house,
                "category":    category,
                "scheme_name": scheme_name,
            })

    df = pl.DataFrame(rows).with_columns(
        pl.col("scheme_code").cast(pl.Utf8),
        pl.col("fund_house").cast(pl.Utf8),
        pl.col("category").cast(pl.Utf8),
        pl.col("scheme_name").cast(pl.Utf8),
    )
    return df


def main() -> None:
    log.info("Building scheme_master from fund universe...")

    df = build_scheme_master()

    upload_bytes(to_parquet_bytes(df), BLOB_SCHEME_MASTER)

    log.info("scheme_master uploaded: %d schemes across %d categories",
             len(df), df["category"].n_unique())

    log.info("Breakdown:")
    for row in df.group_by("category").len().sort("category").to_dicts():
        log.info("  %-20s  %d schemes", row["category"], row["len"])


if __name__ == "__main__":
    main()
