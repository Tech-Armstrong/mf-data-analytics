"""
Central config for all scripts.
Edit these values if MFAPI endpoints or paths change.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# ── Project root ──────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent

# Load .env (gitignored) so AZURE_STORAGE_CONNECTION_STRING is available
# locally. In CI the var comes from the environment directly; load_dotenv is
# a no-op there.
load_dotenv(ROOT_DIR / ".env")

# ── Local data paths (staging before upload + one-time migration source) ──
RAW_NAV_DIR           = ROOT_DIR / "data" / "raw" / "nav"
PROCESSED_DIR         = ROOT_DIR / "data" / "processed"

NAV_HISTORY_PARQUET   = PROCESSED_DIR / "nav_history.parquet"
SCHEME_MASTER_PARQUET = PROCESSED_DIR / "scheme_master.parquet"

# ── Azure Blob (the source of truth — serverless lakehouse) ──────────────
# Connection string is read from the environment, never hard-coded.
# Locally: put it in a .env file (gitignored). In CI: a GitHub Actions secret.
AZURE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

BLOB_CONTAINER        = "mfnavdata"

# Logical paths inside the container. nav_history is partitioned by year so the
# daily job only rewrites the current year; old years stay frozen.
BLOB_RAW_PREFIX       = "raw/nav"                         # raw/nav/year=YYYY/*.parquet
BLOB_NAV_HISTORY_DIR  = "processed/nav_history"           # processed/nav_history/year=YYYY/data.parquet
BLOB_SCHEME_MASTER    = "processed/scheme_master.parquet"

# az:// URIs DuckDB reads from (hive_partitioning picks up the year=YYYY dirs)
AZ_NAV_HISTORY_GLOB   = f"az://{BLOB_CONTAINER}/{BLOB_NAV_HISTORY_DIR}/year=*/*.parquet"
AZ_SCHEME_MASTER      = f"az://{BLOB_CONTAINER}/{BLOB_SCHEME_MASTER}"

# ── AMFI endpoints ────────────────────────────────────────────
AMFI_DAILY_URL        = "https://portal.amfiindia.com/spages/NAVAll.txt"
AMFI_HISTORY_URL      = (
    "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
    "?mf={mf}&frmdt={frmdt}&todt={todt}"
)

# ── AMFI AMC ids (the `mf=` parameter in AMFI_HISTORY_URL) ────────────────
# AMFI's internal AMC id, NOT a scheme code. Needed by
# scripts/fetch/backfill_added_funds.py (--mf) and fetch_amfi_history.py.
#
# Verified 2026-07-31 by probing the history endpoint for ids 1..95 and reading
# back the AMC name each returned. Ids with no live schemes are absent (gaps in
# the sequence are wound-up or merged AMCs, not mistakes).
#
# Keys are the AMC name exactly as AMFI returns it, which is NOT always the
# `fund_house` label used in FUND_UNIVERSE (e.g. "HDFC Mutual Fund" here vs
# "HDFC" there). Use amc_code() for tolerant lookup.
#
# Gotcha: merged/renamed AMCs keep the SURVIVING entity's id --
# Baroda BNP Paribas = 4 (not 59), Nippon India = 21 (ex-Reliance).
AMFI_AMC_CODES: dict[str, int] = {
    "360 ONE Mutual Fund": 62,
    "Aditya Birla Sun Life Mutual Fund": 3,
    "Baroda BNP Paribas Mutual Fund": 4,
    "DSP Mutual Fund": 6,
    "HDFC Mutual Fund": 9,
    "quant Mutual Fund": 13,
    "JM Financial Mutual Fund": 16,
    "Kotak Mahindra Mutual Fund": 17,
    "LIC Mutual Fund": 18,
    "ICICI Prudential Mutual Fund": 20,
    "Nippon India Mutual Fund": 21,
    "SBI Mutual Fund": 22,
    "Tata Mutual Fund": 25,
    "Taurus Mutual Fund": 26,
    "Franklin Templeton Mutual Fund": 27,
    "UTI Mutual Fund": 28,
    "Canara Robeco Mutual Fund": 32,
    "Sundaram Mutual Fund": 33,
    "HSBC Mutual Fund": 37,
    "Quantum Mutual Fund": 41,
    "Invesco Mutual Fund": 42,
    "Mirae Asset Mutual Fund": 45,
    "Bank of India Mutual Fund": 46,
    "Edelweiss Mutual Fund": 47,
    "Bandhan Mutual Fund": 48,
    "Axis Mutual Fund": 53,
    "Navi Mutual Fund": 54,
    "Motilal Oswal Mutual Fund": 55,
    "PGIM India Mutual Fund": 58,
    "Union Mutual Fund": 61,
    "Groww Mutual Fund": 63,
    "PPFAS Mutual Fund": 64,
    "Shriram Mutual Fund": 67,
    "Mahindra Manulife Mutual Fund": 69,
    "ITI Mutual Fund": 70,
    "WhiteOak Capital Mutual Fund": 71,
    "Trust Mutual Fund": 72,
    "NJ Mutual Fund": 73,
    "Samco Mutual Fund": 74,
    "Bajaj Finserv Mutual Fund": 75,
    "Helios Mutual Fund": 76,
    "Zerodha Mutual Fund": 77,
    "Old Bridge Mutual Fund": 78,
    "Unifi Mutual Fund": 79,
    "Angel One Mutual Fund": 80,
    "Capitalmind Mutual Fund": 81,
    "Jio BlackRock Mutual Fund": 82,
    "The Wealth Company Mutual Fund": 83,
    "Choice Mutual Fund": 84,
    "Abakkus Mutual Fund": 85,
    "AlphaGrep Mutual Fund": 86,
}


# FUND_UNIVERSE fund_house spellings that are neither the full AMFI name nor it
# minus " Mutual Fund". Keep in sync when a new fund_house label is introduced;
# `python -m scripts.processing.fund_universe --check-amc-codes` fails loudly if
# one is missed. (The check lives there, not here: config must not import from
# scripts, or the dependency becomes circular.)
AMC_NAME_ALIASES: dict[str, str] = {
    "The Wealth Co.": "The Wealth Company Mutual Fund",
}


def amc_code(name: str) -> int:
    """
    Resolve an AMC name to its AMFI `mf=` id, tolerating the short `fund_house`
    spellings used in FUND_UNIVERSE ("HDFC" -> "HDFC Mutual Fund" -> 9).

    Case-insensitive; matches exactly first, then by prefix. Raises KeyError
    listing the candidates if the name is ambiguous or unknown, so a typo fails
    loudly instead of silently backfilling the wrong AMC.
    """
    key = name.strip()
    key = AMC_NAME_ALIASES.get(key, key).lower()

    # Exact match, then exact match against the name minus its trailing
    # " Mutual Fund" -- that shortened form is what FUND_UNIVERSE's fund_house
    # column holds. Both run before prefix matching, because some AMC names are
    # prefixes of others ("quant" vs "Quantum"): without this, an exact request
    # for "quant" would be reported as ambiguous.
    for amc, code in AMFI_AMC_CODES.items():
        low = amc.lower()
        if low == key or low.removesuffix(" mutual fund") == key:
            return code

    hits = {a: c for a, c in AMFI_AMC_CODES.items() if a.lower().startswith(key)}
    if len(hits) == 1:
        return next(iter(hits.values()))
    if len(hits) > 1:
        raise KeyError(f"AMC name {name!r} is ambiguous: {sorted(hits)}")
    raise KeyError(
        f"Unknown AMC {name!r}. If it is a new AMC, probe "
        f"AMFI_HISTORY_URL with candidate mf= ids and add the verified id here."
    )


# ── Fetch settings ────────────────────────────────────────────
REQUEST_TIMEOUT       = 30          # seconds per request
RETRY_ATTEMPTS        = 3
RETRY_BACKOFF         = 2.0         # exponential backoff multiplier
CONCURRENT_WORKERS    = 10          # threads for parallel fetching

# ── Retention ─────────────────────────────────────────────────
RETENTION_YEARS       = 5           # keep only last N years of raw files


# ── Logging ───────────────────────────────────────────────────
LOG_DIR               = ROOT_DIR / "logs"
LOG_FILE              = LOG_DIR / "pipeline.log"
LOG_LEVEL             = "INFO"