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