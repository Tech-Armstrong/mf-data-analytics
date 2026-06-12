# Cheat Sheet — MF & SIF Data Pipelines

Two parallel pipelines, identical architecture, separate Blob containers.
Run everything from the repo root with `python -m <module.path>` (PowerShell).

| | Mutual Funds (MF) | SIF |
|---|---|---|
| Code root | `scripts/` + `config/` | `sif/scripts/` + `sif/config/` |
| Blob container | `mfnavdata` | `sifnavdata` |
| Storage account | `mfhistoricalnav` (shared — same `AZURE_STORAGE_CONNECTION_STRING`) | same |
| Daily feed | `…/spages/NAVAll.txt` | `…/spages/SIF_NAVAll.txt` |
| History feed | `DownloadNAVHistoryReport_Po.aspx?mf=&frmdt=&todt=` | `SIF_DownloadNAVHistoryReport.aspx?frmdt=&todt=` (no `mf=`) |
| Scheme codes | numeric (`103174`) | `SIF-120` |
| Universe | curated `FUND_UNIVERSE` (~262 schemes) | none — derived from feed (self-extending) |
| Category source | hand-mapped in `fund_universe.py` | AMFI section header (parsed live) |
| Daily workflow | `.github/workflows/daily_nav_update.yml` | `.github/workflows/daily_sif_update.yml` |

> **Architecture in one line:** AMFI text feed → year-partitioned parquet on Azure Blob →
> DuckDB views read live over `az://`. No materialised `.duckdb` file. Labels live in
> `scheme_master`; `nav_history` parquet holds only `scheme_code, nav_date, nav`.

---

## Daily operations

```powershell
# --- MF ---
python -m scripts.fetch.daily_nav_update              # fetch + merge today's NAVs to Blob
python -m scripts.fetch.daily_nav_update --force      # re-load even if today already present

# --- SIF ---
python -m sif.scripts.fetch.daily_sif_update          # same, into sifnavdata
python -m sif.scripts.fetch.daily_sif_update --force
```
Both are idempotent: they skip if the feed's NAV date is already on Blob. The SIF job also
refreshes `scheme_master` each run, so a brand-new `SIF-xxx` onboards automatically.

---

## History backfill (one-time / recovery)

```powershell
# --- MF (loops 18 AMCs × date chunks) ---
python -m scripts.fetch.fetch_amfi_history --local            # write to disk, inspect first
python -m scripts.fetch.fetch_amfi_history                    # publish 5y history to Blob
python -m scripts.fetch.fetch_amfi_history --pause 3 --chunk-days 90

# --- SIF (no AMC loop, one request per date chunk) ---
python -m sif.scripts.fetch.fetch_sif_history --local
python -m sif.scripts.fetch.fetch_sif_history                 # publishes history + scheme_master
python -m sif.scripts.fetch.fetch_sif_history --pause 3 --chunk-days 90
```

---

## scheme_master (the label table)

```powershell
# --- MF: built from the curated FUND_UNIVERSE dict ---
python -m scripts.processing.build_scheme_master

# --- SIF: rebuilt from the live feed (no curated list) ---
python -m sif.scripts.processing.build_sif_scheme_master
```
SIF fetchers call this automatically; run standalone only to force a re-label.

---

## Inspect / debug

```powershell
# Is the current-year partition fresh?
python -m scripts.check_blob_freshness
python -m sif.scripts.check_blob_freshness            #   --local reads staged parquet

# Latest NAVs for one scheme
python -m scripts.fund_nav 103174 30                  # MF: <code> [n_rows]
python -m sif.scripts.sif_nav SIF-112 30             # SIF: --local supported

# Coverage / per-scheme detail
python -m scripts._check_blob_funds                   # MF: universe gap check
python -m scripts._check_blob_funds 103174            # MF: one fund detail
python -m sif.scripts.check_sif_funds                 # SIF: per-category coverage (--local)
python -m sif.scripts.check_sif_funds SIF-112         # SIF: one scheme detail

# Returns sanity dump (MF helper)
python -m scripts.debug_returns 129647
```

> **`--local` flag** (SIF helpers + both fetchers): reads/writes the staged
> `sif/data/processed/` parquet instead of Blob — credential-free inspection before publishing.
> MF history fetch also supports `--local`.

---

## Query from Python (returns & views)

```python
# Swap the import to switch asset class — everything else is identical.
from config.duckdb_session import get_connection            # MF
# from sif.config.duckdb_session import get_connection      # SIF
# from sif.config.duckdb_session import get_local_connection  # SIF, staged local parquet

with get_connection() as con:
    con.execute("SELECT * FROM latest_nav LIMIT 5").fetchall()
    con.execute("SELECT * FROM category_summary").fetchall()
```

```python
from scripts.agent.tools import get_fund_returns, get_category_returns, list_categories
# from sif.scripts.agent.tools import get_fund_returns      # SIF equivalent

get_fund_returns("103174", "1Y")        # MF
get_fund_returns("SIF-112", "SI")       # SIF (short history → use SI/1M for now)
get_category_returns("LARGE CAP", "1Y", sort_by="return_pct", ascending=False)
list_categories()
```

**Views available** (both pipelines): `scheme_master`, `nav_history`, `latest_nav`,
`category_summary`, `fund_house_summary`.

**Return periods:** `1W 2W 1M 3M 6M 9M 1Y 2Y 3Y 5Y YTD MTD SI`.
Returns anchor to each fund's **last available NAV** (not calendar today); trailing-period start
is exclusive/forward-snapped — matches ET Money / Value Research / Google.

---

## Blob layout (per container)

```
<container>/
  processed/
    scheme_master.parquet                       # labels: code, fund_house, category, scheme_name
    nav_history/year=YYYY/data.parquet          # code, nav_date, nav  (year-partitioned)
    nav_history/year=YYYY/added_funds.parquet   # MF only: sidecar for back-filled funds
  raw/nav/year=YYYY/*.parquet                   # audit trail of every fetch
```

MF-only: `backfill_added_funds.py` (add late-listed funds via a sidecar without touching
`data.parquet`), `rebuild_processed.py` (rebuild processed from raw), `migrate_to_blob.py`.

---

## CI schedule

Both workflows: `cron: "30 9 * * 1-5"` (3 PM IST, **testing**). TODO → `0 18 * * 1-5`
(11:30 PM IST) once verified, since AMFI publishes ~8–11 PM IST. Manual run: GitHub Actions UI →
*Run workflow* (optional `force` checkbox). Secret used: `AZURE_STORAGE_CONNECTION_STRING`.

---

## Gotchas

- **SIF history is short** (asset class launched ~Oct 2025) → `1Y/3Y/5Y` returns will error
  with "no NAV on/after" until data accrues. Use `SI` / `1M`.
- **SIF parser gates on `SIF-` prefix**, not `isdigit()` — the two SIF feeds differ in column
  count/order (daily = 6 cols, history = 8 cols); see `sif/scripts/sif_parse.py`.
- AMFI occasionally publishes a literal `0.0` NAV for a scheme/day; the zero-start-NAV guard in
  `_point_to_point_return` keeps it from crashing returns.
- Run from the **repo root**; modules use `python -m …` package paths.
