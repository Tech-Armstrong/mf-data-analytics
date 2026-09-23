# Whitelist Monitor

Daily anomaly alert for a whitelist of mutual funds: flags any fund whose
trailing return has fallen too far behind its **category peer average**, and
emails a summary report.

Migrated from the `armstrong dashboard` (Peer Bench) project's whitelist
anomaly-email system. The detection/render/email logic is an unchanged port;
only the data source changed — this version reads live from this repo's
Azure Blob lakehouse (`nav_history` + `scheme_master`, via
`config.duckdb_session.get_connection()`) instead of a static
`mfapi.in`-derived `index.json`.

## What counts as an anomaly

For a whitelisted fund, at a given horizon (6M / 1Y / 3Y / 5Y):

```
(fund_return − category_peer_avg_return) < −threshold_pp
```

Thresholds (in percentage points) are set per horizon in `config.json`. A
horizon with no configured threshold is not evaluated. A fund can breach on
0, 1, or multiple horizons at once — the email reports every breaching
horizon per fund.

## Files

```
whitelist_monitor/
  anomaly_email.py       core logic: load whitelist/config -> query Blob ->
                          detect breaches -> render -> send
  whitelist.txt           one fund NAME per line (must match
                          scheme_master.scheme_name exactly)
  config.json              thresholds_pp + email settings (recipients,
                          subject prefix, from name)
  templates/
    anomaly_email.html    HTML email body (Jinja) — edit wording/layout here
    anomaly_email.txt     plain-text email body (Jinja)
  requirements.txt        extra deps beyond the repo root's requirements.txt
  .env                    Brevo credentials (git-ignored, not committed)
  anomaly_report.html     generated on each run (git-ignored, not committed)
```

## Run it

```bash
# from the mf-data-analytics repo root
pip install -r whitelist_monitor/requirements.txt   # jinja2, mcp, httpx
                                                      # (duckdb/polars/etc. already
                                                      #  in the repo's own requirements.txt)

# validate whitelist.txt names resolve, without sending anything
python -m whitelist_monitor.anomaly_email --check

# full run: detect anomalies, write anomaly_report.html, send email
python -m whitelist_monitor.anomaly_email
```

Load `.env` first so the Brevo credentials are in the environment:

```bash
# bash
set -a; source whitelist_monitor/.env; set +a
python -m whitelist_monitor.anomaly_email

# PowerShell
Get-Content whitelist_monitor\.env | ForEach-Object {
    if ($_ -match '^\s*([^#=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
}
python -m whitelist_monitor.anomaly_email
```

If the Brevo env vars are not set, the script still queries Blob, detects
anomalies, and writes `anomaly_report.html` to disk — it just skips sending
and says so. Safe to run anywhere, anytime, without credentials.

## Editing the whitelist

`whitelist.txt` holds one fund name per line. The name must match
`scheme_master.scheme_name` **exactly** (surrounding whitespace is trimmed,
nothing else is normalized). Find the canonical string:

```python
from config.duckdb_session import get_connection
con = get_connection()
con.execute("SELECT scheme_name FROM scheme_master WHERE scheme_name ILIKE '%some fund%'").fetchall()
```

After editing, always run:

```bash
python -m whitelist_monitor.anomaly_email --check
```

It reports every line's match status and, for anything unmatched, a
best-guess "did you mean" (normalized token match across the *entire*
`scheme_master`, not just categories the current whitelist already touches).
Exits non-zero if anything is unresolved — safe to wire into a pre-commit
check or CI gate on this file.

**Note:** a fund can only be whitelisted if it's in this repo's
`FUND_UNIVERSE` (see `scripts/processing/fund_universe.py`) — the curated set
that actually gets backfilled to Blob. `FUND_UNIVERSE` is currently
equity/hybrid/thematic only; there is no debt/Gilt/duration category, so
debt funds can't be whitelisted yet (see the commented-out lines in
`whitelist.txt` for a live example).

## Editing thresholds / recipients

`config.json`:

```jsonc
{
  "thresholds_pp": { "m6": 2.0, "y1": 3.0, "y3": 4.0, "y5": 5.0 },
  "email": {
    "recipients": ["someone@armstrong-cap.com"],
    "subject_prefix": "Peer Bench",
    "from_name": "Whitelist Monitor Alert",
    "send_when_no_anomalies": true
  }
}
```

`send_when_no_anomalies: false` suppresses the all-clear email on days with
no breaches. `config.json` itself holds recipient emails — keep it
git-ignored-equivalent in spirit (it's tracked here since it has no secrets,
only recipient addresses; don't add tokens to it).

## Email transport — Brevo via MCP

Sent through **Brevo's MCP server** (`https://mcp.brevo.com/v1/brevo/mcp`),
not SMTP. Credentials come from environment variables only, never committed:

| Variable | Where to get it |
|---|---|
| `BREVO_MCP_TOKEN` | Brevo → Account → SMTP & API → API Keys → generate a key with **MCP** checked |
| `BREVO_SENDER_EMAIL` | Must be an address verified under Brevo → Senders |
| `BREVO_MCP_URL` *(optional)* | Only set to use a scoped module endpoint instead of the main MCP server |

Brevo's MCP server proxies to the same underlying Brevo API, so it still
enforces the account's **Authorized IPs** allow-list
(https://app.brevo.com/security/authorised_ips) even over MCP — an unlisted
IP gets HTTP 401 `brevo_api_rejected_credentials` from the MCP server
itself. Either disable that restriction, or give wherever this runs (CI /
cloud) a static outbound IP.

## Automating the daily run

Not yet wired into a scheduled job in this repo (the dashboard project ran
it via its own GitHub Actions `refresh.yml`). To automate here, add a step
to a workflow that already has `AZURE_STORAGE_CONNECTION_STRING` available
(e.g. alongside `daily_nav_update.yml`), install
`whitelist_monitor/requirements.txt`, set `BREVO_MCP_TOKEN` /
`BREVO_SENDER_EMAIL` as repo secrets, and run
`python -m whitelist_monitor.anomaly_email`.

## Relationship to the `armstrong dashboard` project

`armstrong dashboard/` is a separate, independently deployed static site
with its own data pipeline (mfapi.in → `public/index.json` → browser). It
has its **own** copy of this same anomaly-email logic
(`armstrong dashboard/scripts/anomaly_email.py`) and its own `.env` —
untouched by this folder. The two are not linked at runtime; this folder
exists so the alerting logic can run against this repo's own Blob data
without depending on the dashboard project at all.
