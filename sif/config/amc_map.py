"""
sif/config/amc_map.py

Curated map from the raw AMC sub-header string AMFI prints in the SIF feed
(e.g. "Altiva SIF") to the actual registered AMC / fund house name that
sponsors that SIF brand (e.g. "Edelweiss Mutual Fund").

Why this exists
----------------
sif_parse.py labels fund_house straight off AMFI's free-text sub-header line
with zero normalisation (see sif/scripts/sif_parse.py). That line is a SIF
*brand* name, not the legal AMC name — e.g. AMFI prints "Apex SIF" for a fund
actually run by Aditya Birla Sun Life Mutual Fund. A few AMCs (Franklin,
Mirae, The Wealth Company) reused their MF entity name for the SIF header too,
so those already come through correct; most don't.

This map is applied as a lookup AFTER parsing, in
build_sif_scheme_master.py::_apply_amc_map(), so nav_history/raw parquet keep
AMFI's original text untouched (audit trail) while scheme_master.fund_house
carries the corrected, canonical AMC name.

Keys are matched case-insensitively against the raw fund_house string as
parsed (whitespace-normalised). Keep keys as the exact brand string AMFI uses
today; if AMFI tweaks capitalisation/spacing that's handled by the normaliser,
but a genuinely new string (new brand, or an existing AMC renaming its SIF
brand) will NOT match and falls through unmapped -- see
build_sif_scheme_master.py for how unmapped values are surfaced.
"""

# raw AMFI SIF sub-header (brand name) -> canonical AMC / fund house name
AMC_NAME_MAP: dict[str, str] = {
    "Dyna SIF":       "360 ONE Asset",
    "DynaSIF SIF":    "360 ONE Asset",   # AMFI feed variant seen in practice ("DynaSIF SIF")
    "Apex SIF":       "Aditya Birla Sun Life Mutual Fund",
    "Arudha SIF":     "Bandhan Mutual Fund",
    "Endurance SIF":  "DSP Mutual Fund",
    "Altiva SIF":     "Edelweiss Mutual Fund",
    "Sapphire SIF":   "Franklin Templeton Mutual Fund",
    "RedHex SIF":     "HSBC Mutual Fund",
    "iSIF":           "ICICI Prudential Mutual Fund",
    "iSIF SIF":       "ICICI Prudential Mutual Fund",  # AMFI feed variant seen in practice ("iSIF SIF")
    "Summit SIF":     "Invesco Mutual Fund",
    "Diviniti SIF":   "ITI Mutual Fund",
    "Prism SIF":      "Jio BlackRock Mutual Fund",
    "Infinity SIF":   "Kotak Mahindra Mutual Fund",
    "Platinum SIF":   "Mirae Asset Mutual Fund",
    "qSIF":           "quant Mutual Fund",
    "qsif SIF":       "quant Mutual Fund",  # AMFI feed variant seen in practice ("qsif SIF")
    "Magnum SIF":     "SBI Mutual Fund",
    "Titanium SIF":   "Tata Mutual Fund",
    "WSIF":           "The Wealth Company Mutual Fund",
    "The Wealth Company Mutual Fund": "The Wealth Company Mutual Fund",  # AMFI already prints the real name
    "Arthaya SIF":    "Union Mutual Fund",
    # AMCs where AMFI's SIF sub-header already IS the correct legal name --
    # kept explicit (rather than left to fall through unmapped) so the
    # "unmapped" report stays a true signal of brands nobody has vetted yet.
    "Franklin Templeton Mutual Fund": "Franklin Templeton Mutual Fund",
    "Mirae Asset Mutual Fund":        "Mirae Asset Mutual Fund",
}


# Canonical AMC names that resolve_amc_name() may be called with directly --
# e.g. when build_from_labelled_rows() re-applies the map to the already-
# resolved scheme_master (see build_sif_scheme_master.py). Without this set,
# every canonical name that isn't ALSO a literal AMC_NAME_MAP key (i.e. every
# AMC whose SIF brand differs from its own legal name) would look "unmapped"
# on every subsequent run and log a false warning forever. Derived once from
# AMC_NAME_MAP's values so it can never drift out of sync with the map itself.
_KNOWN_CANONICAL_NAMES = {name.lower() for name in AMC_NAME_MAP.values()}


def _normalise(raw: str) -> str:
    return " ".join(raw.strip().split())


def resolve_amc_name(raw_fund_house: str | None) -> tuple[str | None, bool]:
    """
    Resolve a raw AMFI SIF sub-header string (or an already-canonical AMC
    name) to the canonical AMC name.

    Returns (name, was_mapped):
      - was_mapped=True  -> raw string matched an AMC_NAME_MAP key
        (case-insensitive), OR is already a known canonical AMC name (a value
        somewhere in AMC_NAME_MAP) -- either way `name` is the canonical AMC
        name and nothing needs fixing.
      - was_mapped=False -> no match either way; `name` is the raw string
        unchanged (never dropped/blanked -- an unmapped fund still gets
        labelled with whatever AMFI printed, it just needs a map entry added).

    None in -> (None, False).
    """
    if raw_fund_house is None:
        return None, False

    key = _normalise(raw_fund_house)

    for map_key, canonical in AMC_NAME_MAP.items():
        if _normalise(map_key).lower() == key.lower():
            return canonical, True

    if key.lower() in _KNOWN_CANONICAL_NAMES:
        return raw_fund_house, True

    return raw_fund_house, False
