"""
whitelist_monitor/anomaly_email.py

Whitelist anomaly detection + email notification — migrated from the
"armstrong dashboard" (Peer Bench) project's scripts/anomaly_email.py.

Run standalone:
    python -m whitelist_monitor.anomaly_email            # detect, write report, send email
    python -m whitelist_monitor.anomaly_email --check     # validate whitelist.txt only

An ANOMALY for a horizon (6m / 1y / 3y / 5y) is a whitelisted fund whose
trailing return is BELOW its category peer average by more than the configured
threshold (in percentage points):

        (fund_return - peer_avg) < -threshold_pp

Name matching is EXACT (both sides stripped of surrounding whitespace).
whitelist.txt must hold names exactly as they appear in scheme_master.scheme_name
(query via config.duckdb_session.get_connection() to find the canonical string).
`--check` reports any line that does not resolve (with a best-guess "did you
mean") and exits non-zero, so it can gate a commit.

Data source (THE key difference from the dashboard version)
-------------------------------------------------------------
The dashboard read a precomputed public/index.json built from mfapi.in. This
repo already has the same fund universe live on Azure Blob (nav_history +
scheme_master, read serverlessly via DuckDB) — see config/duckdb_session.py.
`_build_index()` below queries Blob directly and reshapes the result into the
same {trailing, horizon_peer, funds} in-memory shape the original
`find_anomalies()` detector expects, so that function (and everything after
it — rendering, email transport) is a straight port, unchanged.

Trailing returns and category peer averages are computed for every fund in
every category that has >=1 whitelisted fund (not the whole universe) — one
bulk DuckDB read per relevant category, then vectorised in Polars. This is
the same "read a category's data once, compute all funds' returns in Polars"
pattern used for the quartile-ranking logic in scripts/agent/tools.py, and
keeps this fast even though whitelist_monitor now touches Blob instead of a
static JSON file.

The email body is rendered from Jinja templates in whitelist_monitor/templates/
(anomaly_email.html and anomaly_email.txt) — change wording/layout/styling
there without touching this file. Needs the `jinja2` package.

Email transport: Brevo's MCP server (https://mcp.brevo.com/v1/brevo/mcp),
not SMTP. Credentials read from ENVIRONMENT VARIABLES only (never committed):
  BREVO_MCP_TOKEN    Bearer token (Brevo > Account > SMTP & API > API Keys >
                     generate a key with "MCP" checked)
  BREVO_SENDER_EMAIL Must be an address verified under Brevo > Senders
  BREVO_MCP_URL      Optional override for a scoped module endpoint

If those vars are absent the script still writes the HTML report to
whitelist_monitor/anomaly_report.html, prints a summary, and returns without
sending.

Note: Brevo's MCP server proxies to the same underlying Brevo API, so it
still enforces the account's Authorized IPs allow-list
(https://app.brevo.com/security/authorised_ips) even over MCP — this must be
disabled (or the caller given a static outbound IP) for CI/cloud runs.
"""

import os
import re
import sys
import json
import asyncio
from datetime import date, datetime, timezone, timedelta

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from dateutil.relativedelta import relativedelta

from config.duckdb_session import get_connection

MCP_URL = os.environ.get("BREVO_MCP_URL", "https://mcp.brevo.com/v1/brevo/mcp")

HORIZONS = [("m6", "6M"), ("y1", "1Y"),
            ("y3", "3Y"), ("y5", "5Y")]

# Horizon key -> the relativedelta kwargs used to compute its trailing
# start date from a fund's own latest NAV date, same convention as
# scripts/agent/tools.py::_resolve_period for M/Y period strings.
_HORIZON_DELTA = {
    "m6": relativedelta(months=6),
    "y1": relativedelta(years=1),
    "y3": relativedelta(years=3),
    "y5": relativedelta(years=5),
}

ROOT = os.path.dirname(os.path.abspath(__file__))
WHITELIST_PATH = os.path.join(ROOT, "whitelist.txt")
CONFIG_PATH = os.path.join(ROOT, "config.json")
REPORT_OUT = os.path.join(ROOT, "anomaly_report.html")
TEMPLATE_DIR = os.path.join(ROOT, "templates")


# --------------------------------------------------------------------------- #
# inputs
# --------------------------------------------------------------------------- #
def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_whitelist():
    names = []
    with open(WHITELIST_PATH, encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                names.append(ln)
    return names


# --------------------------------------------------------------------------- #
# Blob/DuckDB data layer — replaces the dashboard's load_index() (public/index.json)
# --------------------------------------------------------------------------- #
def _whitelisted_categories(con, whitelist_names):
    """Which categories the whitelisted funds actually belong to — used to
    scope the bulk NAV read (whole universe is unnecessary and slower)."""
    if not whitelist_names:
        return []
    placeholders = ",".join("?" for _ in whitelist_names)
    rows = con.execute(f"""
        SELECT DISTINCT category
        FROM   scheme_master
        WHERE  scheme_name IN ({placeholders})
          AND  category IS NOT NULL
    """, whitelist_names).fetchall()
    return [r[0] for r in rows]


def _build_index(whitelist_names):
    """
    Query Blob directly and reshape into the same {trailing, horizon_peer,
    funds} dict shape scripts/anomaly_email.py (the dashboard version) built
    from public/index.json, so find_anomalies() is unchanged.

    trailing[code]        = {m6, y1, y3, y5, latest_date}
    horizon_peer[category] = {m6, y1, y3, y5}   # category average return
    funds                  = [{code, name, category}, ...]
    """
    import polars as pl

    with get_connection() as con:
        categories = _whitelisted_categories(con, whitelist_names)
        if not categories:
            return {"trailing": {}, "horizon_peer": {}, "funds": [],
                     "generated_at": datetime.now(timezone.utc).isoformat()}

        placeholders = ",".join("?" for _ in categories)
        nav_df = con.execute(f"""
            SELECT h.scheme_code, h.nav_date, h.nav,
                   s.scheme_name, s.category
            FROM   nav_history h
            JOIN   scheme_master s USING (scheme_code)
            WHERE  s.category IN ({placeholders})
        """, categories).pl()

    if nav_df.is_empty():
        return {"trailing": {}, "horizon_peer": {}, "funds": [],
                 "generated_at": datetime.now(timezone.utc).isoformat()}

    nav_df = nav_df.sort(["scheme_code", "nav_date"])

    # Each fund's own anchor = its own latest NAV date (funds can trade on
    # different last dates — same convention as get_fund_returns).
    anchors = (
        nav_df.group_by("scheme_code")
        .agg(pl.col("nav_date").max().alias("anchor"),
             pl.col("scheme_name").first(),
             pl.col("category").first())
    )

    funds = [
        {"code": r["scheme_code"], "name": r["scheme_name"], "category": r["category"]}
        for r in anchors.iter_rows(named=True)
    ]

    trailing: dict[str, dict] = {r["scheme_code"]: {"latest_date": str(r["anchor"])}
                                  for r in anchors.iter_rows(named=True)}

    # Per (fund, horizon) return, computed per-fund since each fund's anchor
    # (and therefore its trailing start date) differs.
    per_fund_returns: dict[str, dict[str, float]] = {code: {} for code in trailing}

    for hk, delta in _HORIZON_DELTA.items():
        for row in anchors.iter_rows(named=True):
            code, anchor = row["scheme_code"], row["anchor"]
            start_target = anchor - delta + timedelta(days=1)  # exclusive-start, same as get_fund_returns

            fund_navs = nav_df.filter(pl.col("scheme_code") == code)
            end_rows = fund_navs.filter(pl.col("nav_date") <= anchor)
            start_rows = fund_navs.filter(pl.col("nav_date") >= start_target)
            if end_rows.is_empty() or start_rows.is_empty():
                continue

            end_nav = end_rows.sort("nav_date")["nav"][-1]
            start_nav = start_rows.sort("nav_date")["nav"][0]
            if start_nav == 0:
                continue

            ret = round((end_nav - start_nav) / start_nav * 100, 4)
            per_fund_returns[code][hk] = ret
            trailing[code][hk] = ret

    # Category peer averages per horizon (simple mean of funds that HAVE a
    # value for that horizon — funds too new for a 5Y window are excluded
    # from the 5Y average rather than dragging it toward None).
    horizon_peer: dict[str, dict[str, float]] = {}
    fund_category = {r["scheme_code"]: r["category"] for r in anchors.iter_rows(named=True)}
    for hk, _label in HORIZONS:
        by_cat: dict[str, list[float]] = {}
        for code, rets in per_fund_returns.items():
            if hk in rets:
                by_cat.setdefault(fund_category[code], []).append(rets[hk])
        for cat, vals in by_cat.items():
            horizon_peer.setdefault(cat, {})[hk] = round(sum(vals) / len(vals), 4)

    return {
        "trailing":      trailing,
        "horizon_peer":  horizon_peer,
        "funds":         funds,
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------------------------------- #
# name matching  (EXACT - whitespace-normalised on both sides)
# --------------------------------------------------------------------------- #
def _norm(s):
    """Loose key used ONLY for 'did you mean' suggestions, never to match."""
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def match_whitelist(funds, whitelist):
    """
    Resolve each whitelist NAME to a fund by EXACT name, comparing both sides
    stripped of surrounding whitespace. Matching is deliberately exact —
    whitelist.txt must hold scheme_master.scheme_name's canonical string.

    Returns (matched, unmatched):
      matched   : list of (whitelist_name, fund) in whitelist.txt order
      unmatched : list of whitelist names with no exact fund
    """
    by_exact = {f["name"].strip(): f for f in funds}
    matched, unmatched = [], []
    for name in whitelist:
        fund = by_exact.get(name.strip())
        if fund is None:
            unmatched.append(name)
        else:
            matched.append((name, fund))
    return matched, unmatched


def suggest_name(name, funds):
    """
    Best-guess canonical name for an unmatched whitelist entry — diagnostics
    only (email body + `--check`). Never used to substitute a match.
    Tries normalized-string equality, then normalized token-set equality.
    """
    target_norm = _norm(name)
    target_tokens = frozenset(target_norm.split())
    token_hit = None
    for f in funds:
        fn = _norm(f["name"])
        if fn == target_norm:
            return f["name"]
        if token_hit is None and frozenset(fn.split()) == target_tokens:
            token_hit = f["name"]
    return token_hit


def suggest_name_global(name):
    """
    Like suggest_name, but searches ALL of scheme_master, not just the
    categories the current whitelist happens to touch — used by `--check` so
    a name from a category with zero OTHER whitelisted funds still gets a
    useful "did you mean", since _build_index() only pulls categories that
    already have >=1 resolved whitelist entry.
    """
    with get_connection() as con:
        rows = con.execute("SELECT scheme_code, scheme_name, category FROM scheme_master").fetchall()
    funds = [{"code": r[0], "name": r[1], "category": r[2]} for r in rows]
    return suggest_name(name, funds), funds


# --------------------------------------------------------------------------- #
# detection  (UNCHANGED from the dashboard version — operates on the
# {trailing, horizon_peer, funds} shape regardless of where it came from)
# --------------------------------------------------------------------------- #
def find_anomalies(index, whitelist, thresholds):
    """
    Walk every whitelisted fund and test each horizon against its category
    peer average.

    Returns (anomalies, unmatched) where:
      anomalies : rows that breach on >= 1 horizon, each shaped
                  {name, category, latest_date,
                   cells: {hk: {fund, peer, diff, breach}}, breaches: [hk, ...]}
      unmatched : list of {name, suggestion} for whitelist names with no fund
    """
    trailing = index.get("trailing", {})
    horizon_peer = index.get("horizon_peer", {})
    funds = index.get("funds", [])

    matched, unmatched_names = match_whitelist(funds, whitelist)

    anomalies = []
    for name, fund in matched:
        tr = trailing.get(str(fund["code"]), {})
        peer = horizon_peer.get(fund["category"], {})

        cells, breaches = {}, []
        for hk, _label in HORIZONS:
            fv = tr.get(hk)
            pv = peer.get(hk)
            diff = None
            breach = False
            if fv is not None and pv is not None:
                diff = round(fv - pv, 2)
                if diff < -abs(thresholds.get(hk, 0)):
                    breach = True
                    breaches.append(hk)
            cells[hk] = {"fund": fv, "peer": pv, "diff": diff, "breach": breach}

        if breaches:
            anomalies.append({
                "name": name,
                "category": fund["category"],
                "latest_date": tr.get("latest_date"),
                "cells": cells,
                "breaches": breaches,
            })

    unmatched = [{"name": n, "suggestion": suggest_name(n, funds)}
                 for n in unmatched_names]
    return anomalies, unmatched


# --------------------------------------------------------------------------- #
# rendering  (layout / wording / styling live in templates/, not here)
# --------------------------------------------------------------------------- #
def _fmt_pct(v):
    return "—" if v is None else f"{v:+.2f}%"


def _fmt_pp(v):
    return "—" if v is None else f"{v:+.2f}pp"


def _thr_label(thresholds, hk):
    v = thresholds.get(hk)
    return "n/a" if v is None else f"{v:g}pp"


HORIZON_LABEL = dict(HORIZONS)


def _commentary(r):
    """
    One-line auto summary of a fund's breach, e.g.
    'Trailing peers on all 4 horizons, worst on 5-year (-46.90pp).'
    Picks the worst (most negative) diff among the breaching horizons.
    """
    breaches = r["breaches"]
    if not breaches:
        return ""
    worst_hk = min(breaches, key=lambda hk: r["cells"][hk]["diff"])
    worst = r["cells"][worst_hk]
    worst_label = HORIZON_LABEL[worst_hk]
    n = len(breaches)
    total = len(HORIZONS)
    scope = f"all {total} horizons" if n == total else f"{n} of {total} horizons"
    return (f"Trailing peers on {scope}, "
            f"worst on {worst_label} ({_fmt_pp(worst['diff'])}).")


def build_context(anomalies, unmatched, thresholds, generated_at):
    """
    Flatten the detector output into plain strings / lists / bools so the Jinja
    templates only have to arrange text. Numbers are pre-formatted here;
    wording, layout and colours are all editable in the template files.
    `cells` is emitted in HORIZONS order so it lines up with `horizons`.
    """
    return {
        "generated_at": str(generated_at),
        "has_anomalies": bool(anomalies),
        "anomaly_count": len(anomalies),
        "horizons": [
            {"key": hk, "label": lbl, "thr_label": _thr_label(thresholds, hk)}
            for hk, lbl in HORIZONS
        ],
        "anomalies": [
            {
                "name": r["name"],
                "category": r["category"],
                "latest_date": r["latest_date"],
                "breaches": r["breaches"],
                "commentary": _commentary(r),
                "cells": [
                    {
                        "key": hk,
                        "label": lbl,
                        "thr_label": _thr_label(thresholds, hk),
                        "fund": _fmt_pct(c["fund"]),
                        "peer": _fmt_pct(c["peer"]),
                        "diff": _fmt_pp(c["diff"]),
                        "has_diff": c["diff"] is not None,
                        "breach": c["breach"],
                    }
                    for hk, lbl in HORIZONS
                    for c in (r["cells"][hk],)
                ],
            }
            for r in anomalies
        ],
        "unmatched": [
            {"name": u["name"], "suggestion": u["suggestion"]} for u in unmatched
        ],
    }


_JINJA_ENV = None


def _env():
    global _JINJA_ENV
    if _JINJA_ENV is None:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        _JINJA_ENV = Environment(
            loader=FileSystemLoader(TEMPLATE_DIR),
            autoescape=select_autoescape(enabled_extensions=("html",)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
    return _JINJA_ENV


def render_email(anomalies, unmatched, thresholds, generated_at):
    """Return (text_body, html_body), rendered from templates/."""
    ctx = build_context(anomalies, unmatched, thresholds, generated_at)
    text_body = _env().get_template("anomaly_email.txt").render(**ctx)
    html_body = _env().get_template("anomaly_email.html").render(**ctx)
    return text_body, html_body


# --------------------------------------------------------------------------- #
# transport — Brevo MCP server (https://mcp.brevo.com), not SMTP.
#
# Auth is a Bearer token (BREVO_MCP_TOKEN env var), generated in Brevo under
# Account > SMTP & API > API Keys with the "MCP" option checked.
#
# NOTE: Brevo's MCP server proxies to the same underlying Brevo API, and that
# API still enforces the account's Authorized IPs allow-list
# (https://app.brevo.com/security/authorised_ips) even over MCP — an
# unlisted IP gets HTTP 401 "brevo_api_rejected_credentials" from the MCP
# server itself. Either disable that restriction or give wherever this runs
# a static outbound IP.
#
# The tool used is transac_templates_send_transac_email — a single
# transactional send, not a bulk campaign tool (wrong shape for a small ops
# alert list).
# --------------------------------------------------------------------------- #
SEND_TOOL_CANDIDATES = (
    "transac_templates_send_transac_email",
    "sendTransacEmail", "send_transac_email",
    "send_transactional_email", "sendTransactionalEmail",
)


def _pick_send_tool(tools):
    names = {t.name for t in tools}
    for cand in SEND_TOOL_CANDIDATES:
        if cand in names:
            return cand
    # fall back to a loose match so a renamed tool still resolves
    for t in tools:
        if "send" in t.name.lower() and (
            "transac" in t.name.lower() or "email" in t.name.lower()
        ):
            return t.name
    return None


async def _send_via_mcp(subject, text_body, html_body, sender, from_name,
                         recipients, token):
    import httpx
    from mcp import Client
    from mcp.client.streamable_http import streamable_http_client

    http_client = httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"})
    transport = streamable_http_client(MCP_URL, http_client=http_client)
    async with Client(transport) as client:
        tools = (await client.list_tools()).tools
        tool_name = _pick_send_tool(tools)
        if tool_name is None:
            available = ", ".join(t.name for t in tools) or "(none)"
            raise RuntimeError(
                "No transactional-send tool found on the Brevo MCP server. "
                f"Available tools: {available}"
            )
        result = await client.call_tool(tool_name, {
            "sender": {"name": from_name, "email": sender},
            "to": [{"email": r} for r in recipients],
            "subject": subject,
            "htmlContent": html_body,
            "textContent": text_body,
        })
        return result


def send_email(subject, text_body, html_body, cfg):
    token = os.environ.get("BREVO_MCP_TOKEN")
    if not token:
        print("BREVO_MCP_TOKEN not set — report written to disk, "
              "email NOT sent.")
        return False

    recipients = cfg.get("email", {}).get("recipients", [])
    if not recipients:
        print("config.json email.recipients is empty — email NOT sent.")
        return False

    sender = os.environ.get("BREVO_SENDER_EMAIL")
    if not sender:
        print("BREVO_SENDER_EMAIL not set — report written to disk, "
              "email NOT sent.")
        return False
    from_name = cfg.get("email", {}).get("from_name", "Whitelist Monitor")

    try:
        asyncio.run(_send_via_mcp(subject, text_body, html_body, sender,
                                   from_name, recipients, token))
    except Exception as e:
        print(f"Brevo MCP send failed: {e}")
        return False

    print(f"Anomaly email sent to {len(recipients)} recipient(s) via Brevo MCP.")
    return True


# --------------------------------------------------------------------------- #
# entry points
# --------------------------------------------------------------------------- #
def run():
    """
    Detect whitelist anomalies (querying Blob live), write
    whitelist_monitor/anomaly_report.html, and email the summary.

    Returns (anomalies, unmatched).
    """
    cfg = load_config()
    whitelist = load_whitelist()
    index = _build_index(whitelist)

    thresholds = cfg.get("thresholds_pp", {})
    generated_at = index.get("generated_at", "?")

    anomalies, unmatched = find_anomalies(index, whitelist, thresholds)

    text_body, html_body = render_email(anomalies, unmatched, thresholds,
                                        generated_at)

    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        f.write(html_body)

    print(f"Whitelist: {len(whitelist)} fund(s) | {len(anomalies)} anomalous | "
          f"{len(unmatched)} unmatched. Report -> {REPORT_OUT}")
    for r in anomalies:
        print(f"  ANOMALY {r['name']} :: " + ", ".join(r["breaches"]))
    for u in unmatched:
        hint = f"  (did you mean: {u['suggestion']})" if u["suggestion"] else ""
        print(f"  UNMATCHED {u['name']}{hint}")

    send_when_clear = cfg.get("email", {}).get("send_when_no_anomalies", True)
    if not anomalies and not send_when_clear:
        print("No anomalies and email.send_when_no_anomalies is false — "
              "email NOT sent.")
        return anomalies, unmatched

    now = datetime.now(timezone.utc)
    date_str = f"{now.day} {now.strftime('%b %Y')}"
    subject_prefix = cfg.get("email", {}).get("subject_prefix", "Whitelist Monitor")
    subject = f"{subject_prefix} — Whitelist Monitoring Alert - {date_str}"
    if unmatched:
        subject += f" — {len(unmatched)} unmatched"

    send_email(subject, text_body, html_body, cfg)
    return anomalies, unmatched


def check():
    """
    Validate whitelist.txt against scheme_master on Blob. Prints each name's
    status (with a 'did you mean' guess for misses, searched across the WHOLE
    universe, not just categories the whitelist already touches) and returns
    an exit code: 0 if every name resolves, 1 otherwise. Does not send email.
    """
    whitelist = load_whitelist()

    with get_connection() as con:
        rows = con.execute("SELECT scheme_code, scheme_name, category FROM scheme_master").fetchall()
    funds = [{"code": r[0], "name": r[1], "category": r[2]} for r in rows]

    matched, unmatched_names = match_whitelist(funds, whitelist)

    print(f"whitelist.txt: {len(whitelist)} name(s) — {len(matched)} matched, "
          f"{len(unmatched_names)} unmatched.\n")
    for name, _fund in matched:
        print(f"  OK        {name}")
    for name in unmatched_names:
        s = suggest_name(name, funds)
        hint = f"   (did you mean: {s})" if s else ""
        print(f"  UNMATCHED {name}{hint}")

    if unmatched_names:
        print("\nFix the unmatched names in whitelist.txt. Query scheme_master "
              "via config.duckdb_session.get_connection() to find the exact "
              "canonical scheme_name string.")
        return 1
    print("\nAll whitelist names resolve.")
    return 0


if __name__ == "__main__":
    if "--check" in sys.argv[1:]:
        raise SystemExit(check())
    run()
