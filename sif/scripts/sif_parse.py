"""
sif/scripts/sif_parse.py

Shared, header-aware parser for both SIF AMFI feeds. This is the only genuinely
new logic in the SIF pipeline — everything else mirrors the MF scripts.

Why a single parser, parameterised by column indices?
-----------------------------------------------------
The two SIF feeds are both semicolon-delimited with dates as %d-%b-%Y, but they
differ in column count AND order:

  Daily  SIF_NAVAll.txt              (6 cols):
      Scheme Code; ISIN Growth; ISIN Reinvest; Scheme Name; NAV; Date
      -> name_idx=3, nav_idx=4, date_idx=5

  History SIF_DownloadNAVHistoryReport.aspx (8 cols):
      Scheme Code; Scheme Name; ISIN Growth; ISIN Reinvest; NAV; Repurchase; Sale; Date
      -> name_idx=1, nav_idx=4, date_idx=7

Self-extending labelling
------------------------
Both feeds group rows under header lines. As we scan top-to-bottom we track two
"current" values and stamp them onto every following data row:

  * category   <- the SECTION header, e.g.
        "Open Ended Schemes(Equity Oriented Investment Strategies - Equity Long-Short Fund)"
    We take the text inside the parentheses (dropping the
    "Equity Oriented Investment Strategies - " prefix) as the category.

  * fund_house <- the AMC SUB-header, the non-section, non-data line just below
    the section header (e.g. "Altiva SIF", "The Wealth Company Mutual Fund").

There is NO hardcoded category/keyword map: a new strategy section or a new AMC
that AMFI adds is captured automatically with zero code change.

SIF scheme codes are non-numeric ("SIF-120"), so unlike the MF parsers we gate
data rows on a "SIF-" prefix rather than isdigit().
"""

from datetime import datetime


def _looks_like_data_row(first_field: str) -> bool:
    return first_field.upper().startswith("SIF-")


def _looks_like_section_header(line: str) -> bool:
    # Section headers describe the strategy and always carry parentheses, e.g.
    # "Open Ended Schemes(Equity Oriented Investment Strategies - ... Fund)".
    return line.startswith("Open Ended Schemes") and "(" in line and ")" in line


def _extract_category(section_header: str) -> str:
    inner = section_header[section_header.find("(") + 1 : section_header.rfind(")")].strip()
    # Drop the "...Investment Strategies - " prefix when present, keep the
    # actual strategy name as the category.
    if " - " in inner:
        inner = inner.split(" - ", 1)[1].strip()
    return inner


def parse_sif_lines(
    text: str,
    *,
    name_idx: int,
    nav_idx: int,
    date_idx: int,
) -> list[dict]:
    """
    Parse a SIF feed into labelled rows.

    Returns a list of dicts:
        {scheme_code, nav_date (date), nav (float),
         scheme_name, fund_house, category}

    Rows whose NAV/date cannot be parsed are skipped. category/fund_house may be
    None for rows that appear before any header (defensive — not expected).
    """
    rows: list[dict] = []
    category: str | None = None
    fund_house: str | None = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(";")]

        if _looks_like_data_row(parts[0]):
            try:
                nav_val = float(parts[nav_idx])
                nav_date = datetime.strptime(parts[date_idx], "%d-%b-%Y").date()
                scheme_name = parts[name_idx]
            except (ValueError, IndexError):
                continue
            rows.append({
                "scheme_code": parts[0],
                "nav_date":    nav_date,
                "nav":         nav_val,
                "scheme_name": scheme_name,
                "fund_house":  fund_house,
                "category":    category,
            })
            continue

        # Header line: a section header sets the category, anything else
        # (and not the column-title line) is the AMC sub-header.
        if _looks_like_section_header(line):
            category = _extract_category(line)
        elif line.startswith("Scheme Code"):
            # the column-title row at the top of the file — ignore
            continue
        else:
            fund_house = line

    return rows
