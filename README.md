# mf-analytics-lake

Mutual fund NAV data lake. Fetches from [AMFI](https://portal.amfiindia.com/) bulk download,
stores in partitioned Parquet, and powers a DuckDB analytical database — refreshed daily via
GitHub Actions.

---

## Architecture

```
AMFI bulk download
    └──► scripts/fetch/fetch_amfi_history.py
              └──► data/raw/nav/year=YYYY/          (immutable raw, partitioned by year)
                        └──► scripts/processing/rebuild_processed.py
                                  └──► data/processed/nav_history.parquet
                                            └──► scripts/processing/build_duckdb.py
                                                      └──► data/duckdb/mf_analytics.duckdb
```

---

## First-time Setup

```bash
# 1. Clone and install
pip install -r requirements.txt

# 2. Fetch full NAV history from AMFI (5 years, all configured AMCs)
python -m scripts.fetch.fetch_amfi_history

# 3. Build processed layer
python -m scripts.processing.rebuild_processed

# 4. Build DuckDB
python -m scripts.processing.build_duckdb
```

---

## Daily Manual Run

```bash
python -m scripts.fetch.fetch_amfi_history
python -m scripts.processing.rebuild_processed
python -m scripts.processing.retention_cleanup
python -m scripts.processing.build_duckdb
```

---

## Coverage Check

To verify which schemes from the fund universe are present in raw data:

```bash
python -m scripts.processing.TEST_PROCESSING
```

---

## GitHub Actions

The workflow `daily_nav_update.yml` runs automatically
**Mon–Fri at 11:30 PM IST** (after AMFI publishes NAVs).

To trigger manually: go to **Actions → Daily NAV Update → Run workflow**.

---

## Data Layout

| Path | Description |
|------|-------------|
| `data/raw/nav/year=YYYY/amfi_amfi_bulk_YYYY_MM_DD.parquet` | Daily raw snapshot, partitioned by year |
| `data/processed/nav_history.parquet` | Deduplicated full history, all schemes |
| `data/processed/scheme_master.parquet` | Scheme codes + names (optional metadata) |
| `data/duckdb/mf_analytics.duckdb` | Analytical DB, rebuilt daily |

---

## DuckDB Tables

| Table / View | Description |
|---|---|
| `nav_history` | All NAV rows: `scheme_code, nav_date, nav` |
| `scheme_master` | Scheme list: `scheme_code, scheme_name` |
| `latest_nav` | View — most recent NAV per scheme, joined with name |

---

## Scripts

| Script | Purpose |
|---|---|
| `scripts/fetch/fetch_amfi_history.py` | Fetch NAV history from AMFI bulk download |
| `scripts/fetch/fetch_scheme_master.py` | Bootstrap scheme metadata (run once) |
| `scripts/processing/rebuild_processed.py` | Raw → deduplicated nav_history.parquet |
| `scripts/processing/retention_cleanup.py` | Delete raw files older than 5 years |
| `scripts/processing/build_duckdb.py` | Build DuckDB from processed parquet |
| `scripts/processing/TEST_PROCESSING.py` | Coverage report across fund universe |

---

## Config

All paths, URLs, and tuning knobs live in `config/constants.py`.
No `.env` file needed — everything is open/public data.
