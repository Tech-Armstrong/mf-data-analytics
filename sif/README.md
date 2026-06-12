# SIF pipeline

The SIF (Specialized Investment Fund) data pipeline — a mirror of the MF
lakehouse for a separate AMFI asset class, in its own Blob container.

Same serverless-lakehouse design as the root project:
AMFI text feed → year-partitioned parquet on Azure Blob → DuckDB views read live
over `az://`. No materialised database file.

## What's different from MF

- **Separate Blob container:** `sifnavdata` (same storage account / same
  `AZURE_STORAGE_CONNECTION_STRING` secret). MF's `mfnavdata` is never touched.
- **Different feeds:**
  - Daily:   `https://portal.amfiindia.com/spages/SIF_NAVAll.txt` (6 cols)
  - History: `https://portal.amfiindia.com/SIF_DownloadNAVHistoryReport.aspx?frmdt=…&todt=…`
    (8 cols, no `mf=` parameter — one request returns all SIFs)
- **Non-numeric scheme codes** (`SIF-120`), so the parser gates on a `SIF-`
  prefix, not `isdigit()`.
- **Self-extending, no curated universe:** category comes from the AMFI section
  header, fund_house from the AMC sub-header, both parsed live
  ([sif/scripts/sif_parse.py](scripts/sif_parse.py)). `scheme_master` is rebuilt
  from the feed on every fetch, so a new SIF scheme is ingested and labelled the
  same day with no code or manual step.

## Layout

```
sif/
  config/      constants.py, blob_io.py, duckdb_session.py, logging_utils.py
  scripts/
    sif_parse.py                       shared header-aware parser
    fetch/fetch_sif_history.py         5-year backfill
    fetch/daily_sif_update.py          daily incremental (used by CI)
    processing/build_sif_scheme_master.py
    agent/tools.py                     returns/query tools (SIF DB)
```

GitHub Actions: [.github/workflows/daily_sif_update.yml](../.github/workflows/daily_sif_update.yml).

## Bootstrap (one-time)

1. Create the `sifnavdata` container in the existing storage account.
2. `python -m sif.scripts.fetch.fetch_sif_history --local`  — inspect the parse on disk.
3. `python -m sif.scripts.fetch.fetch_sif_history`          — publish history + scheme_master.
4. `python -m sif.scripts.fetch.daily_sif_update`           — confirm the daily path.
5. Enable the workflow. New SIFs then onboard automatically.

## Query

```python
from sif.config.duckdb_session import get_connection
with get_connection() as con:
    con.execute("SELECT * FROM latest_nav LIMIT 5").fetchall()

from sif.scripts.agent.tools import get_fund_returns
get_fund_returns("SIF-112", "1M")
```
