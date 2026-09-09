"""
sif/scripts/processing/build_sif_scheme_master.py

Builds / refreshes processed/scheme_master.parquet for SIF.

Unlike MF (which has a hand-curated FUND_UNIVERSE), the SIF universe is derived
entirely from the feed: whatever schemes the parser saw, with the category it
read from the AMFI section header. The fetchers call build_from_labelled_rows()
on every run, so scheme_master always reflects the current feed and a brand-new
SIF scheme is labelled the same day with no manual step.

fund_house is different: AMFI's sub-header is a SIF *brand* name (e.g. "Apex
SIF"), not the registered AMC name (e.g. "Aditya Birla Sun Life Mutual Fund").
build_from_labelled_rows() runs every parsed fund_house through
sif.config.amc_map.resolve_amc_name() before it lands in scheme_master, so the
published fund_house is always the canonical AMC name. A brand AMFI adds that
isn't in AMC_NAME_MAP yet still gets labelled (with AMFI's raw string, so
nothing goes missing) but is logged as UNMAPPED so it can be added to the map
— see sif/config/amc_map.py.

scheme_master columns: scheme_code, fund_house, category, scheme_name

Usage (standalone, re-label from existing Blob nav data — rarely needed):
    python -m sif.scripts.processing.build_sif_scheme_master
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import polars as pl

from sif.config.constants import BLOB_SCHEME_MASTER
from sif.config.blob_io import upload_bytes, download_bytes, to_parquet_bytes
from sif.config.logging_utils import get_logger
from sif.config.amc_map import resolve_amc_name

log = get_logger("build_sif_scheme_master")

_MASTER_COLS = ["scheme_code", "fund_house", "category", "scheme_name"]


def _apply_amc_map(df: pl.DataFrame) -> pl.DataFrame:
    """
    Overwrite fund_house with the canonical AMC name via resolve_amc_name().
    Logs one warning per distinct unmapped raw string so a new SIF brand (or
    an AMC renaming its brand) is surfaced instead of silently persisting
    AMFI's raw sub-header text.
    """
    if "fund_house" not in df.columns or df.is_empty():
        return df

    raw_values = df["fund_house"].unique().to_list()
    resolved: dict[str, str | None] = {}
    unmapped: list[str] = []
    for raw in raw_values:
        name, was_mapped = resolve_amc_name(raw)
        resolved[raw] = name
        if raw is not None and not was_mapped:
            unmapped.append(raw)

    if unmapped:
        for raw in sorted(unmapped):
            log.warning(
                "fund_house UNMAPPED: %r has no entry in AMC_NAME_MAP "
                "(sif/config/amc_map.py) — persisted as-is; add a mapping.",
                raw,
            )

    return df.with_columns(
        pl.col("fund_house").map_elements(
            lambda v: resolved.get(v), return_dtype=pl.Utf8
        )
    )


def _read_existing_master() -> pl.DataFrame | None:
    data = download_bytes(BLOB_SCHEME_MASTER)
    if data is None:
        return None
    return pl.read_parquet(data)


def _latest_label_per_code(df: pl.DataFrame) -> pl.DataFrame:
    """
    Reduce labelled rows to one row per scheme_code. Rows are assumed to arrive
    in feed order (newest fetch last); we keep the LAST occurrence so a renamed
    scheme or recategorised strategy updates cleanly.
    """
    return (
        df.select(_MASTER_COLS)
        .with_columns(pl.col("scheme_code").cast(pl.Utf8))
        .unique(subset=["scheme_code"], keep="last", maintain_order=True)
        .sort("scheme_code")
    )


def build_from_labelled_rows(labelled: pl.DataFrame, *, local: bool = False) -> pl.DataFrame:
    """
    Merge freshly-parsed labelled rows into scheme_master and publish.

    Existing scheme_master rows are preserved (so schemes absent from today's
    feed are not dropped); rows present in `labelled` overwrite their existing
    label (last-write-wins on scheme_code).

    Returns the merged scheme_master DataFrame.
    """
    if labelled.is_empty():
        log.warning("No labelled rows passed — scheme_master unchanged.")
        existing = _read_existing_master()
        return existing if existing is not None else pl.DataFrame(schema={c: pl.Utf8 for c in _MASTER_COLS})

    fresh = _apply_amc_map(
        labelled.select([c for c in _MASTER_COLS if c in labelled.columns])
        .with_columns(pl.col("scheme_code").cast(pl.Utf8))
    )

    existing = _read_existing_master()
    if existing is not None and not existing.is_empty():
        # existing first, fresh last -> fresh wins in keep="last"
        combined = pl.concat([existing.select(_MASTER_COLS), fresh.select(_MASTER_COLS)], how="vertical")
    else:
        combined = fresh.select(_MASTER_COLS)

    master = _latest_label_per_code(combined).with_columns(
        pl.col("scheme_code").cast(pl.Utf8),
        pl.col("fund_house").cast(pl.Utf8),
        pl.col("category").cast(pl.Utf8),
        pl.col("scheme_name").cast(pl.Utf8),
    )
    # Re-apply the map to the merged result too, so AMC_NAME_MAP additions/
    # fixes self-heal previously-published rows (e.g. Blob still holding an
    # old raw brand string from before a mapping existed) on the very next
    # run, with no separate backfill/migration step required.
    master = _apply_amc_map(master)

    if local:
        from sif.config.constants import SCHEME_MASTER_PARQUET, PROCESSED_DIR
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        master.write_parquet(SCHEME_MASTER_PARQUET)
        log.info("scheme_master written locally: %s (%d schemes)",
                 SCHEME_MASTER_PARQUET, len(master))
    else:
        upload_bytes(to_parquet_bytes(master), BLOB_SCHEME_MASTER)
        log.info("scheme_master uploaded: %d schemes across %d categories",
                 len(master), master["category"].n_unique())

    for row in master.group_by("category").len().sort("category").to_dicts():
        log.info("  %-45s  %d schemes", row["category"], row["len"])

    return master


def main() -> None:
    """
    Standalone rebuild: fetch the live SIF daily feed and (re)label scheme_master
    from it. The labels (scheme_name/fund_house/category) live only in the feed
    text — nav_history parquet stores just scheme_code/nav_date/nav — so the feed
    is the authoritative source for a rebuild. Normally unnecessary because the
    fetchers keep scheme_master fresh on every run.
    """
    import requests

    from sif.config.constants import SIF_DAILY_URL, REQUEST_TIMEOUT
    from sif.scripts.sif_parse import parse_sif_lines

    log.info("Rebuilding SIF scheme_master from the live daily feed...")
    resp = requests.get(SIF_DAILY_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    rows = parse_sif_lines(resp.text, name_idx=3, nav_idx=4, date_idx=5)

    if not rows:
        log.error("No SIF rows parsed from the daily feed — nothing to build.")
        sys.exit(1)

    build_from_labelled_rows(pl.DataFrame(rows))


if __name__ == "__main__":
    main()
