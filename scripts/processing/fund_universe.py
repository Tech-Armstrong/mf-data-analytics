"""
scripts/processing/fund_universe.py

Defines FUND_UNIVERSE -- the full scheme catalogue from FUND-CATEGORY-MAPPING.md.
Also runnable as a coverage report against raw NAV data.

Usage:
    python -m scripts.processing.fund_universe
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import polars as pl

from config.constants import RAW_NAV_DIR, amc_code
from config.logging_utils import get_logger

log = get_logger("test_processing")


# ── Fund Universe (source: FUND-CATEGORY-MAPPING.md) ─────────────────────────
# Format: (scheme_code, fund_house, scheme_name)

FUND_UNIVERSE: dict[str, list[tuple[str, str, str]]] = {
    "LARGE CAP": [
        ("103174", "Aditya Birla Sun Life", "ABSL Large Cap Fund-Growth"),
        ("112277", "Axis",                  "Axis Large Cap Fund - Regular - Growth"),
        ("152780", "Bajaj Finserv",         "Bajaj Finserv Large Cap Fund - Regular - Growth"),
        ("108799", "Bandhan",               "Bandhan Large Cap Fund - Regular - Growth"),
        ("148982", "Bank of India",         "BOI Large Cap Fund Regular Growth"),
        ("150185", "Baroda BNP Paribas",    "Baroda BNP Paribas Large Cap Fund - Regular - Growth"),
        ("113221", "Canara Robeco",         "Canara Robeco Large Cap Fund - Regular - Growth"),
        ("101635", "DSP",                   "DSP Large Cap Fund - Regular - Growth"),
        ("111940", "Edelweiss",             "Edelweiss Large Cap Fund - Regular - Growth"),
        ("100471", "Franklin Templeton",    "Franklin India Large Cap Fund-Growth"),
        ("116547", "Groww",                 "Groww Largecap Fund - Regular - Growth"),
        ("102000", "HDFC",                  "HDFC Large Cap Fund - Growth - Regular"),
        ("101594", "HSBC",                  "HSBC Large Cap Fund - Regular Growth"),
        ("108466", "ICICI Prudential",      "ICICI Pru Large Cap Fund - Growth"),
        ("112098", "Invesco",               "Invesco India Largecap Fund - Regular - Growth"),
        ("148351", "ITI",                   "ITI Large Cap Fund - Regular - Growth"),
        ("100219", "JM Financial",          "JM Large Cap Fund (Regular) - Growth"),
        ("114458", "Kotak Mahindra",        "Kotak Large Cap Fund - Growth"),
        ("106871", "LIC",                   "LIC MF Large Cap Fund-Regular-Growth"),
        ("146551", "Mahindra Manulife",     "Mahindra Manulife Large Cap Fund - Regular - Growth"),
        ("107578", "Mirae Asset",           "Mirae Asset Large Cap Fund - Growth"),
        ("152352", "Motilal Oswal",         "Motilal Oswal Large Cap Regular Growth"),
        ("106235", "Nippon India",          "Nippon India Large Cap Fund - Growth"),
        ("138308", "PGIM India",            "PGIM India Large Cap Fund - Growth"),
        ("154154", "PPFAS",                 "Parag Parikh Large Cap Fund - Regular - Growth"),
        ("150441", "quant",                 "quant Large Cap Fund - Growth - Regular"),
        ("153238", "Samco",                 "Samco Large Cap Fund - Regular - Growth"),
        ("103504", "SBI",                   "SBI Large Cap Fund - Regular Growth"),
        ("148504", "Sundaram",              "Sundaram Large Cap Fund - Regular - Growth"),
        ("100475", "Tata",                  "Tata Large Cap Fund - Regular - Growth"),
        ("101209", "Taurus",                "Taurus Large Cap Fund - Regular - Growth"),
        ("141247", "Union",                 "Union Largecap Fund - Regular - Growth"),
        ("100651", "UTI",                   "UTI Large Cap Fund - Regular - Growth"),
        ("150799", "WhiteOak Capital",      "WhiteOak Capital Large Cap Fund Regular Growth"),
    ],
    "MID CAP": [
        ("101592", "Aditya Birla Sun Life", "ABSL Midcap Fund - Growth"),
        ("114564", "Axis",                  "Axis Midcap Fund - Regular - Growth"),
        ("150402", "Bandhan",               "Bandhan Midcap Fund - Growth - Regular"),
        ("153726", "Bank of India",         "BOI Mid Cap Fund - Regular Growth"),
        ("150209", "Baroda BNP Paribas",    "Baroda BNP Paribas Mid Cap Fund - Regular - Growth"),
        ("150816", "Canara Robeco",         "Canara Robeco Mid Cap Fund - Regular - Growth"),
        ("104481", "DSP",                   "DSP Midcap Fund - Regular - Growth"),
        ("140225", "Edelweiss",             "Edelweiss Mid Cap Fund - Regular - Growth"),
        ("100473", "Franklin Templeton",    "Franklin India Mid Cap Fund-Growth"),
        ("105758", "HDFC",                  "HDFC Mid Cap Fund - Growth"),
        ("153327", "Helios",                "Helios Mid Cap Fund - Regular - Growth"),
        ("151034", "HSBC",                  "HSBC Midcap Fund - Regular Growth"),
        ("102528", "ICICI Prudential",      "ICICI Pru MidCap Fund - Growth"),
        ("105503", "Invesco",               "Invesco India Midcap Fund - Regular - Growth"),
        ("148732", "ITI",                   "ITI Mid Cap Fund - Regular - Growth"),
        ("150812", "JM Financial",          "JM Midcap Fund (Regular) - Growth"),
        ("104908", "Kotak Mahindra",        "Kotak Midcap Fund - Regular - Growth"),
        ("152001", "LIC",                   "LIC MF Mid Cap Fund-Regular-Growth"),
        ("142109", "Mahindra Manulife",     "Mahindra Manulife Mid Cap Fund - Regular - Growth"),
        ("147479", "Mirae Asset",           "Mirae Asset Midcap Fund - Regular - Growth"),
        ("127039", "Motilal Oswal",         "Motilal Oswal Midcap Fund - Regular - Growth"),
        ("100377", "Nippon India",          "Nippon India Growth Mid Cap Fund-Growth"),
        ("125305", "PGIM India",            "PGIM India Midcap Fund - Regular - Growth"),
        ("101065", "quant",                 "quant Mid Cap Fund - Growth - Regular"),
        ("154115", "Samco",                 "Samco Mid Cap Fund - Regular"),
        ("102941", "SBI",                   "SBI Midcap Fund - Regular - Growth"),
        ("101539", "Sundaram",              "Sundaram Mid Cap Fund Regular - Growth"),
        ("102328", "Tata",                  "Tata Mid Cap Fund Regular - Growth"),
        ("100477", "Taurus",                "Taurus Mid Cap Fund - Regular - Growth"),
        ("154211", "Trust",                 "TrustMF Mid Cap Fund - Regular-Growth"),
        ("148071", "Union",                 "Union Midcap Fund - Regular - Growth"),
        ("102394", "UTI",                   "UTI Mid Cap Fund-Growth"),
        ("150583", "WhiteOak Capital",      "WhiteOak Capital Mid Cap Fund Regular Growth"),
    ],
    "SMALL CAP": [
        ("154214", "Abakkus",               "Abakkus Small Cap Fund - Regular - Growth"),
        ("105804", "Aditya Birla Sun Life", "ABSL Small Cap Fund - Growth"),
        ("125350", "Axis",                  "Axis Small Cap Fund - Regular - Growth"),
        ("153609", "Bajaj Finserv",         "Bajaj Finserv Small Cap Fund - Regular - Growth"),
        ("147944", "Bandhan",               "Bandhan Small Cap Fund - Regular Growth"),
        ("145677", "Bank of India",         "BOI Small Cap Fund Regular Growth"),
        ("152130", "Baroda BNP Paribas",    "Baroda BNP Paribas Small Cap Fund - Regular - Growth"),
        ("146127", "Canara Robeco",         "Canara Robeco Small Cap Fund - Regular - Growth"),
        ("105989", "DSP",                   "DSP Small Cap Fund - Regular - Growth"),
        ("146193", "Edelweiss",             "Edelweiss Small Cap Fund - Regular - Growth"),
        ("103360", "Franklin Templeton",    "Franklin India Small Cap Fund-Growth"),
        ("154102", "Groww",                 "Groww Small Cap Fund-Regular-Growth"),
        ("130502", "HDFC",                  "HDFC Small Cap Fund - Growth"),
        ("153909", "Helios",                "Helios Small Cap Fund - Regular - Growth"),
        ("151133", "HSBC",                  "HSBC Small Cap Fund - Regular Growth"),
        ("106823", "ICICI Prudential",      "ICICI Pru Smallcap Fund - Growth"),
        ("145139", "Invesco",               "Invesco India Smallcap Fund - Regular - Growth"),
        ("147920", "ITI",                   "ITI Small Cap Fund - Regular - Growth"),
        ("152612", "JM Financial",          "JM Small Cap Fund (Regular) - Growth"),
        ("102875", "Kotak Mahindra",        "Kotak Small Cap Fund - Growth"),
        ("152003", "LIC",                   "LIC MF Small Cap Fund-Regular-Growth"),
        ("150912", "Mahindra Manulife",     "Mahindra Manulife Small Cap Fund - Regular - Growth"),
        ("153198", "Mirae Asset",           "Mirae Asset Small Cap Fund - Regular - Growth"),
        ("152232", "Motilal Oswal",         "Motilal Oswal Small Cap Fund - Regular - Growth"),
        ("113177", "Nippon India",          "Nippon India Small Cap Fund - Growth"),
        ("149020", "PGIM India",            "PGIM India Small Cap Fund - Regular - Growth"),
        ("100177", "quant",                 "quant Small Cap Fund - Growth - Regular"),
        ("152108", "Quantum",               "Quantum Small Cap Fund - Regular Growth"),
        ("153869", "Samco",                 "Samco Small Cap Fund - Regular"),
        ("125494", "SBI",                   "SBI Small Cap Fund - Regular - Growth"),
        ("100795", "Sundaram",              "Sundaram Small Cap Fund Regular - Growth"),
        ("145208", "Tata",                  "Tata Small Cap Fund-Regular-Growth"),
        ("154268", "The Wealth Co.",        "The Wealth Co. Small Cap Fund - Regular-Growth"),
        ("152940", "Trust",                 "TrustMF Small Cap Fund - Regular-Growth"),
        ("129647", "Union",                 "Union Small Cap Fund - Regular - Growth"),
        ("148617", "UTI",                   "UTI Small Cap Fund - Regular - Growth"),
    ],
    "MULTI CAP": [
        ("148918", "Aditya Birla Sun Life", "ABSL Multi-Cap Fund - Regular Growth"),
        ("149382", "Axis",                  "Axis Multicap Fund - Regular - Growth"),
        ("153307", "Bajaj Finserv",         "Bajaj Finserv Multi Cap Fund - Regular - Growth"),
        ("149305", "Bandhan",               "Bandhan Multi Cap Fund - Growth - Regular"),
        ("151445", "Bank of India",         "BOI Multi Cap Fund Regular - Growth"),
        ("102020", "Baroda BNP Paribas",    "Baroda BNP Paribas Multi Cap Fund - Regular Growth"),
        ("151821", "Canara Robeco",         "Canara Robeco Multi Cap Fund - Regular - Growth"),
        ("152307", "DSP",                   "DSP Multicap Fund - Regular - Growth"),
        ("152095", "Edelweiss",             "Edelweiss Multi Cap Fund - Regular - Growth"),
        ("152738", "Franklin Templeton",    "Franklin India Multi Cap Fund - Growth"),
        ("153100", "Groww",                 "Groww Multicap Fund - Regular - Growth"),
        ("149366", "HDFC",                  "HDFC Multi Cap Fund - Growth"),
        ("151289", "HSBC",                  "HSBC Multi Cap Fund - Regular - Growth"),
        ("101228", "ICICI Prudential",      "ICICI Pru Multicap Fund - Growth"),
        ("107353", "Invesco",               "Invesco India Multicap Fund - Regular Growth"),
        ("147184", "ITI",                   "ITI Multi Cap Fund - Regular - Growth"),
        ("149182", "Kotak Mahindra",        "Kotak Multicap Fund-Regular-Growth"),
        ("150661", "LIC",                   "LIC MF Multi Cap Fund-Regular-Growth"),
        ("141224", "Mahindra Manulife",     "Mahindra Manulife Multi Cap Fund - Regular - Growth"),
        ("151812", "Mirae Asset",           "Mirae Asset Multicap Fund - Regular - Growth"),
        ("152650", "Motilal Oswal",         "Motilal Oswal Multi Cap Fund Regular Growth"),
        ("101161", "Nippon India",          "Nippon India Multi Cap Fund - Growth"),
        ("152816", "PGIM India",            "PGIM India Multi Cap Fund - Regular - Growth"),
        ("100631", "quant",                 "quant Multi Cap Fund - Growth - Regular"),
        ("152848", "Samco",                 "Samco Multi Cap Fund - Regular - Growth"),
        ("149886", "SBI",                   "SBI Multicap Fund - Regular - Growth"),
        ("149667", "Sundaram",              "Sundaram Multi Cap Fund - Growth"),
        ("151235", "Tata",                  "Tata Multicap Fund - Regular - Growth"),
        ("153645", "Trust",                 "TrustMF Multi Cap Fund - Regular-Growth"),
        ("150855", "Union",                 "Union Multicap Fund - Regular - Growth"),
        ("153516", "UTI",                   "UTI Multi Cap Fund - Regular - Growth"),
        ("152072", "WhiteOak Capital",      "WhiteOak Capital Multi Cap Fund Regular Growth"),
    ],
    "FLEXI CAP": [
        ("151799", "360 ONE",               "360 ONE Flexicap Fund - Regular - Growth"),
        ("154041", "Abakkus",               "Abakkus Flexi Cap Fund - Regular - Growth"),
        ("103166", "Aditya Birla Sun Life", "ABSL Flexi Cap Fund - Growth - Regular"),
        ("141927", "Axis",                  "Axis Flexi Cap Fund - Regular - Growth"),
        ("151898", "Bajaj Finserv",         "Bajaj Finserv Flexi Cap Fund - Regular-Growth"),
        ("108594", "Bandhan",               "Bandhan Flexi Cap Fund - Regular - Growth"),
        ("148405", "Bank of India",         "BOI Flexi Cap Fund Regular - Growth"),
        ("150385", "Baroda BNP Paribas",    "Baroda BNP Paribas Flexi Cap Fund - Regular Growth"),
        ("101922", "Canara Robeco",         "Canara Robeco Flexicap Fund - Regular Growth"),
        ("153739", "Capitalmind",           "Capitalmind Flexi Cap Fund Regular Growth"),
        ("105875", "DSP",                   "DSP Flexi Cap Fund - Regular - Growth"),
        ("140355", "Edelweiss",             "Edelweiss Flexi Cap Fund - Regular - Growth"),
        ("100520", "Franklin Templeton",    "Franklin India Flexi Cap Fund - Growth"),
        ("101762", "HDFC",                  "HDFC Flexi Cap Fund - Growth"),
        ("152136", "Helios",                "Helios Flexi Cap Fund - Regular - Growth"),
        ("102252", "HSBC",                  "HSBC Flexi Cap Fund - Regular Growth"),
        ("148989", "ICICI Prudential",      "ICICI Pru Flexicap Fund - Growth"),
        ("149766", "Invesco",               "Invesco India Flexi Cap Fund - Regular - Growth"),
        ("151377", "ITI",                   "ITI Flexi Cap Fund - Regular - Growth"),
        ("109522", "JM Financial",          "JM Flexicap Fund (Regular) - Growth"),
        ("112090", "Kotak Mahindra",        "Kotak Flexicap Fund - Growth"),
        ("100313", "LIC",                   "LIC MF Flexi Cap Fund-Regular-Growth"),
        ("149101", "Mahindra Manulife",     "Mahindra Manulife Flexi Cap Fund - Regular - Growth"),
        ("151414", "Mirae Asset",           "Mirae Asset Flexi Cap Fund - Regular - Growth"),
        ("129048", "Motilal Oswal",         "Motilal Oswal Flexi Cap Fund Regular-Growth"),
        ("143787", "Navi",                  "Navi Flexi Cap Fund - Regular - Growth"),
        ("149089", "Nippon India",          "Nippon India Flexi Cap Fund - Regular - Growth"),
        ("151920", "NJ",                    "NJ Flexi Cap Fund - Regular - Growth"),
        ("154226", "Old Bridge",            "Old Bridge Flexi Cap Fund Regular Growth"),
        ("133836", "PGIM India",            "PGIM India Flexi Cap Fund - Regular - Growth"),
        ("122640", "PPFAS",                 "Parag Parikh Flexi Cap Fund - Regular - Growth"),
        ("109830", "quant",                 "quant Flexi Cap Fund - Growth - Regular"),
        ("149449", "Samco",                 "Samco Flexi Cap Fund - Regular - Growth"),
        ("103215", "SBI",                   "SBI Flexicap Fund - Regular - Growth"),
        ("144902", "Shriram",               "Shriram Flexi Cap Fund - Regular Growth"),
        ("150568", "Sundaram",              "Sundaram Flexicap Fund Regular Growth"),
        ("144548", "Tata",                  "Tata Flexi Cap Fund - Regular-Growth"),
        ("100476", "Taurus",                "Taurus Flexi Cap Fund - Regular - Growth"),
        ("153870", "The Wealth Co.",        "The Wealth Co. Flexi Cap Fund - Regular Growth"),
        ("152582", "Trust",                 "TrustMF Flexi Cap Fund - Regular - Growth"),
        ("153542", "Unifi",                 "Unifi Flexi Cap Fund - Regular Growth"),
        ("115270", "Union",                 "Union Flexi Cap Fund - Growth"),
        ("100669", "UTI",                   "UTI Flexi Cap Fund-Growth"),
        ("150347", "WhiteOak Capital",      "WhiteOak Capital Flexi Cap Fund Regular-Growth"),
    ],
    "LARGE & MID CAP": [
        ("100033", "Aditya Birla Sun Life", "ABSL Large & Mid Cap Fund - Regular Growth"),
        ("145112", "Axis",                  "Axis Large & Mid Cap Fund - Regular - Growth"),
        ("152406", "Bajaj Finserv",         "Bajaj Finserv Large and Mid Cap Fund - Regular - Growth"),
        ("108596", "Bandhan",               "Bandhan Large & Mid Cap Fund - Regular - Growth"),
        ("110603", "Bank of India",         "BOI Large & Mid Cap Fund Regular - Growth"),
        ("148471", "Baroda BNP Paribas",    "Baroda BNP Paribas Large and Mid Cap Fund - Regular - Growth"),
        ("102920", "Canara Robeco",         "Canara Robeco Large and Mid Cap Fund - Regular Growth"),
        ("103819", "DSP",                   "DSP Large & Mid Cap Fund - Regular - Growth"),
        ("140172", "Edelweiss",             "Edelweiss Large & Mid Cap Fund - Regular - Growth"),
        ("102883", "Franklin Templeton",    "Franklin India Large & Mid Cap Fund - Growth"),
        ("130496", "HDFC",                  "HDFC Large and Mid Cap Fund - Growth"),
        ("152943", "Helios",                "Helios Large & Mid Cap Fund - Regular - Growth"),
        ("146771", "HSBC",                  "HSBC Large & Mid Cap Fund - Regular Growth"),
        ("100349", "ICICI Prudential",      "ICICI Pru Large & Mid Cap Fund - Growth"),
        ("106144", "Invesco",               "Invesco India Large & Mid Cap Fund - Regular - Growth"),
        ("152824", "ITI",                   "ITI Large & Midcap Fund - Regular - Growth"),
        ("153627", "JM Financial",          "JM Large & Mid Cap Fund (Regular) - Growth"),
        ("103234", "Kotak Mahindra",        "Kotak Large & Midcap Fund - Growth-Regular"),
        ("133711", "LIC",                   "LIC MF Large & Mid Cap Fund-Regular-Growth"),
        ("147843", "Mahindra Manulife",     "Mahindra Manulife Large & Mid Cap Fund - Regular - Growth"),
        ("112932", "Mirae Asset",           "Mirae Asset Large & Midcap Fund - Regular - Growth"),
        ("147701", "Motilal Oswal",         "Motilal Oswal Large and Midcap Fund - Regular Growth"),
        ("135678", "Navi",                  "Navi Large & Midcap Fund - Regular - Growth"),
        ("100380", "Nippon India",          "Nippon India Vision Large & Midcap Fund-Growth"),
        ("152383", "PGIM India",            "PGIM India Large and Midcap Fund - Regular - Growth"),
        ("104513", "quant",                 "quant Large & Mid Cap Fund - Growth"),
        ("153533", "Samco",                 "Samco Large & Mid Cap Fund - Regular - Growth"),
        ("103024", "SBI",                   "SBI Large & Midcap Fund - Regular - Growth"),
        ("105001", "Sundaram",              "Sundaram Large and Midcap Fund Regular - Growth"),
        ("101824", "Tata",                  "Tata Large & Mid Cap Fund - Regular - Growth"),
        ("147748", "Union",                 "Union Large & Midcap Fund - Regular - Growth"),
        ("100664", "UTI",                   "UTI Large & Mid Cap Fund - Regular - Growth"),
        ("152225", "WhiteOak Capital",      "Whiteoak Capital Large & Mid Cap Fund Regular Growth"),
    ],
    "VALUE": [
        ("108167", "Aditya Birla Sun Life", "ABSL Value Fund - Growth"),
        ("149167", "Axis",                  "Axis Value Fund - Regular - Growth"),
        ("108909", "Bandhan",               "Bandhan Value Fund - Regular - Growth"),
        ("151747", "Baroda BNP Paribas",    "Baroda BNP Paribas Value Fund - Regular - Growth"),
        ("149088", "Canara Robeco",         "Canara Robeco Value Fund - Regular - Growth"),
        ("148594", "DSP",                   "DSP Value Fund - Regular - Growth"),
        ("100496", "Franklin Templeton",    "Templeton India Value Fund - Growth"),
        ("135343", "Groww",                 "Groww Value Fund - Regular - Growth"),
        ("101764", "HDFC",                  "HDFC Value Fund - Growth"),
        ("151110", "HSBC",                  "HSBC Value Fund - Regular Growth"),
        ("102594", "ICICI Prudential",      "ICICI Pru Value Fund - Growth"),
        ("148973", "ITI",                   "ITI Value Fund - Regular - Growth"),
        ("100254", "JM Financial",          "JM Value Fund (Regular) - Growth"),
        ("152016", "LIC",                   "LIC MF Value Fund-Regular-Growth"),
        ("153304", "Mahindra Manulife",     "Mahindra Manulife Value Fund - Regular - Growth"),
        ("103085", "Nippon India",          "Nippon India Value Fund - Growth"),
        ("149337", "quant",                 "quant Value Fund - Growth - Regular"),
        ("141068", "Quantum",               "Quantum Value Fund - Regular Growth"),
        ("101853", "Sundaram",              "Sundaram Value Fund Regular - Growth"),
        ("101672", "Tata",                  "Tata Value Fund - Regular - Growth"),
        ("145471", "Union",                 "Union Value Fund - Regular - Growth"),
        ("103098", "UTI",                   "UTI Value Fund - Regular - Growth"),
    ],
    "FOCUSED": [
        ("131578", "360 ONE",               "360 ONE Focused Fund - Regular - Growth"),
        ("103309", "Aditya Birla Sun Life", "ABSL Focused Fund - Growth"),
        ("117560", "Axis",                  "Axis Focused Fund - Regular - Growth"),
        ("108592", "Bandhan",               "Bandhan Focused Fund - Regular - Growth"),
        ("150263", "Baroda BNP Paribas",    "Baroda BNP Paribas Focused Fund - Regular - Growth"),
        ("148884", "Canara Robeco",         "Canara Robeco Focused Fund - Regular - Growth"),
        ("112901", "DSP",                   "DSP Focused Fund - Regular - Growth"),
        ("150382", "Edelweiss",             "Edelweiss Focused Fund - Regular - Growth"),
        ("105817", "Franklin Templeton",    "Franklin India Focused Equity Fund - Growth"),
        ("102760", "HDFC",                  "HDFC Focused Fund - Growth"),
        ("148409", "HSBC",                  "HSBC Focused Fund - Regular Growth"),
        ("111957", "ICICI Prudential",      "ICICI Pru Focused Equity Fund - Growth"),
        ("148483", "Invesco",               "Invesco India Focused Fund - Regular - Growth"),
        ("151778", "ITI",                   "ITI Focused Fund - Regular - Growth"),
        ("107410", "JM Financial",          "JM Focused Fund (Regular) - Growth"),
        ("147477", "Kotak Mahindra",        "Kotak Focused Fund - Regular - Growth"),
        ("152009", "LIC",                   "LIC MF Focused Fund-Regular-Growth"),
        ("148571", "Mahindra Manulife",     "Mahindra Manulife Focused Fund - Regular - Growth"),
        ("147203", "Mirae Asset",           "Mirae Asset Focused Fund Regular Growth"),
        ("122387", "Motilal Oswal",         "Motilal Oswal Focused Fund - Regular Growth"),
        ("104637", "Nippon India",          "Nippon India Focused Fund - Growth"),
        ("152361", "Old Bridge",            "Old Bridge Focused Fund - Regular Growth"),
        ("109275", "quant",                 "quant Focused Fund - Growth - Regular"),
        ("102756", "SBI",                   "SBI Focused Fund - Regular - Growth"),
        ("149532", "Sundaram",              "Sundaram Focused Fund - Growth"),
        ("147760", "Tata",                  "Tata Focused Fund-Regular-Growth"),
        ("147490", "Union",                 "Union Focused Fund - Regular - Growth"),
        ("149090", "UTI",                   "UTI Focused Fund - Regular - Growth"),
    ],
    "CHILDREN FUND": [
        ("148489", "SBI",                   "SBI Children's Fund - Investment Plan - Regular Plan - Growth"),
        ("135766", "Axis",                  "Axis Children's Fund - No Lock in - Regular Plan - Growth"),
        ("135759", "Axis",                  "Axis Children's Fund - Lock in - Regular Growth"),
        ("100900", "HDFC",                  "HDFC Childrens Fund - Growth"),
        ("146409", "Aditya Birla Sun Life", "Aditya Birla Sun Life Bal Bhavishya Yojna - Regular - Growth"),
    ],
    "RETIREMENT FUND": [
        ("133565", "Nippon India",          "Nippon India Retirement Fund - Wealth Creation Scheme - Growth"),
    ],
    "FOF OVERSEAS": [
        ("106370", "Sundaram",              "Sundaram Global Brand Fund - Regular - Growth"),
        ("150750", "Axis",                  "Axis NASDAQ 100 US Specific Equity Passive FOF - Regular - Growth"),
        ("149100", "Bandhan",               "Bandhan US specific Equity Active FOF - Regular - Growth"),
        ("149817", "DSP",                   "DSP Global Innovation Overseas Equity Omni FoF - Regular - Growth"),
        ("117691", "DSP",                   "DSP US Specific Equity Omni FoF - Regular - Growth"),
        ("116633", "Franklin Templeton",    "Franklin U.S. Opportunities Equity Active FOF - Growth"),
        ("149290", "Aditya Birla Sun Life", "Aditya Birla Sun Life US Equity Passive FOF - Regular - Growth"),
        ("148661", "Kotak Mahindra",        "Kotak US Specific Equity Passive FOF - Regular - Growth"),
        ("149056", "Kotak Mahindra",        "Kotak Global Innovation Overseas Equity Omni FOF - Regular - Growth"),
        ("150594", "Mirae Asset",           "Mirae Asset Global Electric & Autonomous Vehicles Equity Passive FOF - Regular - Growth"),
    ],
    "FOF DOMESTIC HYBRID": [
        ("153963", "Axis",                  "Axis Multi-Asset Active FoF - Regular - Growth"),
        ("132174", "Aditya Birla Sun Life", "Aditya Birla Sun Life Aggressive Hybrid Omni FOF - Regular - Growth"),
        ("154165", "DSP",                   "DSP Multi Asset Omni Fund of Funds - Regular - Growth"),
        ("153779", "Edelweiss",             "Edelweiss Multi Asset Omni Fund of Fund - Regular - Growth"),
        ("132180", "Aditya Birla Sun Life", "Aditya Birla Sun Life Dynamic Asset Allocation Omni FOF - Regular - Growth"),
        ("102574", "Kotak Mahindra",        "Kotak Multi Asset Omni FOF - Regular - Growth"),
        ("148663", "Nippon India",          "Nippon India Multi-Asset Omni FoF - Regular - Growth"),
    ],
    "FOF DOMESTIC SILVER": [
        ("150617", "Axis",                  "Axis Silver Fund of Fund - Regular - Growth"),
        ("154135", "Bandhan",               "Bandhan Silver ETF FOF - Regular - Growth"),
        ("153486", "DSP",                   "DSP Silver ETF Fund of Fund - Regular - Growth"),
    ],
    "FOF INCOME PLUS ARBITRAGE": [
        ("147890", "Axis",                  "Axis Income Plus Arbitrage Active FOF - Regular - Growth"),
        ("108545", "Bandhan",               "Bandhan Income Plus Arbitrage Active FOF - Regular - Growth"),
        ("130533", "HDFC",                  "HDFC Income Plus Arbitrage Active FOF - Growth"),
    ],
    "BALANCED ADVANTAGE": [
        ("131666", "Aditya Birla Sun Life", "ABSL Balanced Advantage Fund - Regular - Growth"),
        ("141644", "Axis",                  "Axis Balanced Advantage Fund - Regular Plan - Growth"),
        ("152196", "Bajaj Finserv",         "Bajaj Finserv Balanced Advantage Fund - Regular - Growth"),
        ("131357", "Bandhan",               "Bandhan Balanced Advantage Fund - Regular Plan - Growth"),
        ("127849", "Bank of India",         "BOI Balanced Advantage Fund - Regular - Growth"),
        ("145387", "Baroda BNP Paribas",    "Baroda BNP Paribas Balanced Advantage Fund - Regular - Growth"),
        ("152690", "Canara Robeco",         "Canara Robeco Balanced Advantage Fund - Regular - Growth"),
        ("126394", "DSP",                   "DSP Dynamic Asset Allocation Fund - Regular Plan - Growth"),
        ("112117", "Edelweiss",             "Edelweiss Balanced Advantage Fund - Regular Plan - Growth Option"),
        ("150480", "Franklin Templeton",    "Franklin India Balanced Advantage Fund - Growth"),
        ("100119", "HDFC",                  "HDFC Balanced Advantage Fund - Growth Plan"),
        ("152512", "Helios",                "Helios Balanced Advantage Fund - Regular - Growth"),
        ("151127", "HSBC",                  "HSBC Balanced Advantage Fund - Regular Growth"),
        ("104685", "ICICI Prudential",      "ICICI Prudential Balanced Advantage Fund - Growth"),
        ("106317", "Invesco",               "Invesco India Balanced Advantage Fund - Regular Plan - Growth"),
        ("147787", "ITI",                   "ITI Balanced Advantage Fund - Regular Plan - Growth Option"),
        ("144333", "Kotak Mahindra",        "Kotak Balanced Advantage Fund - Regular Plan - Growth Option"),
        ("149259", "LIC",                   "LIC MF Balanced Advantage Fund - Regular - Growth"),
        ("149404", "Mahindra Manulife",     "Mahindra Manulife Balanced Advantage Fund - Regular - Growth"),
        ("150473", "Mirae Asset",           "Mirae Asset Balanced Advantage Fund - Regular - Growth"),
        ("139870", "Motilal Oswal",         "Motilal Oswal Balanced Advantage Fund - Regular - Growth"),
        ("149266", "NJ",                    "NJ Balanced Advantage Fund - Regular - Growth"),
        ("102846", "Nippon India",          "Nippon India Balanced Advantage Fund - Growth Plan - Growth Option"),
        ("152464", "PPFAS",                 "Parag Parikh Dynamic Asset Allocation Fund - Regular - Growth"),
        ("148657", "PGIM India",            "PGIM India Balanced Advantage Fund - Regular - Growth"),
        ("151714", "quant",                 "quant Dynamic Asset Allocation Fund - Growth - Regular"),
        ("152187", "Samco",                 "Samco Dynamic Asset Allocation Fund - Regular Plan - Growth Option"),
        ("149132", "SBI",                   "SBI Balanced Advantage Fund - Regular Plan - Growth"),
        ("147405", "Shriram",               "Shriram Balanced Advantage Fund - Regular Growth"),
        ("149715", "Sundaram",              "Sundaram Balanced Advantage Fund - Growth"),
        ("146007", "Tata",                  "Tata Balanced Advantage Fund - Regular Plan - Growth"),
        ("154183", "The Wealth Co.",        "The Wealth Company Balanced Advantage Fund - Regular Growth"),
        ("153377", "Unifi",                 "Unifi Dynamic Asset Allocation Fund - Regular Growth"),
        ("142035", "Union",                 "Union Balanced Advantage Fund - Regular Plan - Growth Option"),
        ("151882", "UTI",                   "UTI Balanced Advantage Fund - Regular - Growth"),
        ("151268", "WhiteOak Capital",      "WhiteOak Capital Balanced Advantage Fund Regular Plan Growth"),
    ],
    "CONSERVATIVE HYBRID": [
        ("101818", "Aditya Birla Sun Life", "ABSL Regular Savings Fund - Regular - Growth"),
        ("112924", "Axis",                  "Axis Conservative Hybrid Fund - Regular Plan - Growth"),
        ("112353", "Bandhan",               "Bandhan Conservative Hybrid Fund - Regular Plan - Growth"),
        ("100601", "Canara Robeco",         "Canara Robeco Conservative Hybrid Fund - Regular - Growth"),
        ("102448", "DSP",                   "DSP Regular Savings Fund - Regular Plan - Growth"),
        ("100948", "Franklin Templeton",    "Franklin India Conservative Hybrid Fund - Growth"),
        ("102147", "HDFC",                  "HDFC Hybrid Debt Fund - Growth"),
        ("102262", "HSBC",                  "HSBC Conservative Hybrid Fund - Regular Growth"),
        ("102330", "ICICI Prudential",      "ICICI Prudential Regular Savings Fund - Growth"),
        ("114859", "Kotak Mahindra",        "Kotak Debt Hybrid - Regular - Growth"),
        ("101869", "LIC",                   "LIC MF Conservative Hybrid Fund - Regular - Growth"),
        ("102172", "Nippon India",          "Nippon India Conservative Hybrid Fund - Growth"),
        ("148959", "PPFAS",                 "Parag Parikh Conservative Hybrid Fund - Regular - Growth"),
        ("100968", "SBI",                   "SBI Conservative Hybrid Fund - Regular Plan - Growth"),
        ("112868", "Sundaram",              "Sundaram Conservative Hybrid Fund - Regular Plan - Growth"),
        ("102535", "UTI",                   "UTI Conservative Hybrid Fund - Regular Plan - Growth"),
    ],
    "AGGRESSIVE HYBRID": [
        ("103155", "Aditya Birla Sun Life", "Aditya Birla Sun Life Equity Hybrid '95 Fund - Regular - Growth"),
        ("144393", "Axis",                  "Axis Aggressive Hybrid Fund - Regular - Growth"),
        ("140381", "Bandhan",               "Bandhan Aggressive Hybrid Fund - Regular - Growth"),
        ("139529", "Bank of India",         "BOI Mid & Small Cap Equity & Debt Fund - Regular - Growth"),
        ("106166", "Canara Robeco",         "Canara Robeco Equity Hybrid Fund - Regular - Growth"),
        ("100081", "DSP",                   "DSP Aggressive Hybrid Fund - Regular - Growth"),
        ("112108", "Edelweiss",             "Edelweiss Aggressive Hybrid Fund - Regular - Growth"),
        ("100550", "Franklin Templeton",    "Franklin India Aggressive Hybrid Fund - Growth"),
        ("145605", "Groww",                 "Groww Aggressive Hybrid Fund - Regular - Growth"),
        ("102948", "HDFC",                  "HDFC Hybrid Equity Fund - Growth"),
        ("151120", "HSBC",                  "HSBC Aggressive Hybrid Fund - Regular Growth"),
        ("100356", "ICICI Prudential",      "ICICI Prudential Equity & Debt Fund - Growth"),
        ("143536", "Invesco",               "Invesco India Aggressive Hybrid Fund - Regular - Growth"),
        ("100221", "JM Financial",          "JM Aggressive Hybrid Fund (Regular) - Growth"),
        ("133036", "Kotak Mahindra",        "Kotak Aggressive Hybrid Fund - Regular - Growth"),
        ("100323", "LIC",                   "LIC MF Aggressive Hybrid Fund - Regular - Growth"),
        ("147447", "Mahindra Manulife",     "Mahindra Manulife Aggressive Hybrid Fund - Regular - Growth"),
        ("134815", "Mirae Asset",           "Mirae Asset Aggressive Hybrid Fund - Regular - Growth"),
        ("143162", "Navi",                  "Navi Aggressive Hybrid Fund - Regular - Growth"),
        ("112936", "Nippon India",          "Nippon India Aggressive Hybrid Fund - Growth"),
        ("138382", "PGIM India",            "PGIM India Aggressive Hybrid Equity Fund - Growth"),
        ("101070", "quant",                 "quant Aggressive Hybrid Fund - Growth - Regular"),
        ("102885", "SBI",                   "SBI Equity Hybrid Fund - Regular - Growth"),
        ("125713", "Shriram",               "Shriram Aggressive Hybrid Fund - Regular Growth"),
        ("149599", "Sundaram",              "Sundaram Aggressive Hybrid Fund - Growth"),
        ("100414", "Tata",                  "Tata Aggressive Hybrid Fund - Regular - Growth"),
        ("148591", "Union",                 "Union Aggressive Hybrid Fund - Regular - Growth"),
        ("100684", "UTI",                   "UTI Aggressive Hybrid Fund - Regular - Growth"),
        ("154393", "WhiteOak Capital",      "WhiteOak Capital Aggressive Hybrid Fund Regular Growth"),
    ],
    "THEMATIC CONSUMPTION": [
        ("103111", "Aditya Birla Sun Life", "ABSL Consumption Fund - Regular - Growth"),
        ("152804", "Axis",                  "Axis Consumption Fund - Regular - Growth"),
        ("153072", "Bajaj Finserv",         "Bajaj Finserv Consumption Fund - Regular - Growth"),
        ("153140", "Bank of India",         "BOI Consumption Fund - Regular - Growth"),
        ("150268", "Baroda BNP Paribas",    "Baroda BNP Paribas India Consumption Fund - Regular - Growth"),
        ("112152", "Canara Robeco",         "Canara Robeco Consumption Fund - Regular - Growth"),
        ("153255", "Edelweiss",             "Edelweiss Consumption Fund - Regular - Growth"),
        ("151803", "HDFC",                  "HDFC Consumption Fund - Regular - Growth"),
        ("152027", "HSBC",                  "HSBC Consumption Fund - Regular - Growth"),
        ("146950", "ICICI Prudential",      "ICICI Prudential Bharat Consumption Fund - Growth"),
        ("153902", "Invesco",               "Invesco India Consumption Fund - Regular - Growth"),
        ("153263", "ITI",                   "ITI Bharat Consumption Fund - Regular - Growth"),
        ("152170", "Kotak Mahindra",        "Kotak Consumption Fund - Regular - Growth"),
        ("153950", "LIC",                   "LIC MF Consumption Fund - Regular - Growth"),
        ("145355", "Mahindra Manulife",     "Mahindra Manulife Consumption Fund - Regular - Growth"),
        ("114931", "Mirae Asset",           "Mirae Asset Great Consumer Fund - Regular - Growth"),
        ("153916", "Motilal Oswal",         "Motilal Oswal Consumption Fund - Regular - Growth"),
        ("102751", "Nippon India",          "Nippon India Consumption Fund - Growth"),
        ("152338", "quant",                 "quant Consumption Fund - Growth - Regular"),
        ("120576", "SBI",                   "SBI Consumption Opportunities Fund - Regular - Growth"),
        ("102142", "Sundaram",              "Sundaram Consumption Fund - Regular - Growth"),
        ("135804", "Tata",                  "Tata India Consumer Fund - Regular - Growth"),
        ("154022", "Union",                 "Union Consumption Fund - Regular - Growth"),
        ("106425", "UTI",                   "UTI India Consumer Fund - Regular - Growth"),
        ("154162", "WhiteOak Capital",      "WhiteOak Capital Consumption Opportunities Fund - Regular Growth"),
    ],
    "SECTORAL INFRASTRUCTURE": [
        ("103476", "Aditya Birla Sun Life", "ABSL Infrastructure Fund - Regular - Growth"),
        ("114476", "Bandhan",               "Bandhan Infrastructure Fund - Regular - Growth"),
        ("112359", "Bank of India",         "BOI Manufacturing & Infrastructure Fund - Regular - Growth"),
        ("103390", "Canara Robeco",         "Canara Robeco Infrastructure - Regular - Growth"),
        ("102434", "DSP",                   "DSP India T.I.G.E.R. Fund - Regular - Growth"),
        ("112092", "Franklin Templeton",    "Franklin Build India Fund - Growth"),
        ("107524", "HDFC",                  "HDFC Infrastructure Fund - Growth"),
        ("151037", "HSBC",                  "HSBC Infrastructure Fund - Regular Growth"),
        ("103149", "ICICI Prudential",      "ICICI Prudential Infrastructure Fund - Growth"),
        ("106654", "Invesco",               "Invesco India Infrastructure Fund - Regular - Growth"),
        ("133796", "Kotak Mahindra",        "Kotak Infrastructure and Economic Reform Fund - Regular - Growth"),
        ("107763", "LIC",                   "LIC MF Infrastructure Fund - Regular - Growth"),
        ("153985", "Mirae Asset",           "Mirae Asset Infrastructure Fund - Regular - Growth"),
        ("153482", "Motilal Oswal",         "Motilal Oswal Infrastructure Fund - Regular - Growth"),
        ("106170", "quant",                 "quant Infrastructure Fund - Growth - Regular"),
        ("106096", "SBI",                   "SBI Infrastructure Fund - Regular - Growth"),
        ("129213", "Sundaram",              "Sundaram Infrastructure Advantage Fund - Regular - Growth"),
        ("101766", "Tata",                  "Tata Infrastructure Fund - Regular - Growth"),
        ("105417", "Taurus",                "Taurus Infrastructure Fund - Regular - Growth"),
        ("102395", "UTI",                   "UTI Infrastructure Fund - Regular - Growth"),
    ],
    "ULTRA SHORT DURATION": [
        ("149535", "Sundaram",              "Sundaram Ultra Short Duration Fund - Growth"),
        ("144171", "Bandhan",               "Bandhan Ultra Short Duration Fund - Regular - Growth"),
    ],
    "LOW DURATION": [
        ("149519", "Sundaram",              "Sundaram Low Duration Fund - Growth"),
        ("153652", "Union",                 "Union Low Duration Fund - Regular - Growth"),
        ("153419", "Edelweiss",             "Edelweiss Low Duration Fund - Regular - Growth"),
    ],
    "SHORT DURATION": [
        ("149585", "Sundaram",              "Sundaram Short Duration Fund - Growth"),
        ("108768", "Bandhan",               "Bandhan Short Duration Fund - Regular - Growth"),
    ],
    "LIQUID": [
        ("149661", "Sundaram",              "Sundaram Liquid Fund - Growth"),
        ("102012", "UTI",                   "UTI Liquid Cash Plan - Regular - Growth"),
    ],
    "INDEX FUND": [
        ("101314", "Aditya Birla Sun Life", "ABSL Nifty 50 Index Fund - Regular - Growth"),
        ("152800", "Aditya Birla Sun Life", "ABSL Nifty India Defence Index Fund - Regular - Growth"),
        ("154170", "Axis",                  "Axis BSE India Sectors Leaders Index Fund - Regular - Growth"),
        ("151787", "Axis",                  "Axis Nifty IT Index Fund - Regular Plan - Growth"),
        ("152730", "Axis",                  "Axis Nifty 500 Index Fund - Regular Plan - Growth Option"),
        ("153245", "Axis",                  "Axis Nifty500 Momentum 50 Index Fund - Regular - Growth"),
        ("150633", "Bandhan",               "Bandhan Nifty100 Low Volatility 30 Index Fund - Regular - Growth"),
        ("150592", "Bandhan",               "Bandhan Nifty200 Momentum 30 Index Fund - Regular - Growth"),
        ("150427", "DSP",                   "DSP Nifty Midcap 150 Quality 50 Index Fund - Regular - Growth"),
        ("150900", "Edelweiss",             "Edelweiss Nifty Midcap150 Momentum 50 Index Fund - Regular Plan - Growth"),
        ("150894", "Edelweiss",             "Edelweiss Nifty Smallcap 250 Index Fund - Regular Plan - Growth"),
        ("152617", "Edelweiss",             "Edelweiss Nifty Alpha Low Volatility 30 Index Fund - Regular Plan - Growth"),
        ("152988", "Edelweiss",             "Edelweiss Nifty500 Multicap Momentum Quality 50 Index Fund - Regular - Growth"),
        ("101525", "HDFC",                  "HDFC Nifty 50 Index Fund - Growth Plan"),
        ("152779", "HDFC",                  "HDFC NIFTY500 Multicap 50:25:25 Index Fund - Regular - Growth"),
        ("149218", "ICICI Prudential",      "ICICI Prudential NASDAQ 100 Index Fund - Growth"),
        ("150454", "ICICI Prudential",      "ICICI Prudential Nifty 200 Momentum 30 Index Fund - Growth"),
        ("152934", "ICICI Prudential",      "ICICI Prudential Nifty200 Value 30 Index Fund - Growth"),
        ("148974", "Kotak Mahindra",        "Kotak Nifty 50 Index Fund - Regular Plan - Growth"),
        ("148382", "Motilal Oswal",         "Motilal Oswal S&P 500 Index Fund - Regular Plan - Growth"),
        ("150519", "Motilal Oswal",         "Motilal Oswal BSE Enhanced Value Index Fund - Regular - Growth"),
        ("152711", "Motilal Oswal",         "Motilal Oswal Nifty India Defence Index Fund - Regular - Growth"),
        ("151815", "Motilal Oswal",         "Motilal Oswal Nifty Microcap 250 Index Fund - Regular - Growth"),
        ("152876", "Motilal Oswal",         "Motilal Oswal Nifty 500 Momentum 50 Index Fund - Regular - Growth"),
        ("150490", "Nippon India",          "Nippon India Nifty Alpha Low Volatility 30 Index Fund - Regular - Growth"),
        ("152883", "Nippon India",          "Nippon India Nifty 500 Momentum 50 Index Fund - Regular - Growth"),
        ("102272", "SBI",                   "SBI Nifty Index Fund - Regular Plan - Growth"),
        ("150676", "SBI",                   "SBI Nifty Smallcap 250 Index Fund - Regular Plan - Growth"),
        ("153580", "Tata",                  "Tata Nifty Midcap 150 Index Fund - Regular Plan - Growth"),
        ("153425", "Tata",                  "Tata BSE Quality Index Fund - Regular Plan - Growth"),
        ("150741", "Tata",                  "Tata Nifty Midcap 150 Momentum 50 Index Fund - Regular Plan - Growth"),
        ("154064", "Tata",                  "Tata BSE Multicap Consumption 50:30:20 Index Fund - Regular Plan - Growth"),
        ("154198", "UTI",                   "UTI Nifty500 Shariah Index Fund - Regular - Growth"),
        ("153273", "UTI",                   "UTI Nifty Midsmallcap 400 Momentum Quality 100 Index Fund - Regular - Growth"),
        ("151738", "UTI",                   "UTI Nifty 500 Value 50 Index Fund - Regular - Growth"),
        ("100822", "UTI",                   "UTI Nifty 50 Index Fund - Regular - Growth"),
        ("152858", "UTI",                   "UTI Nifty 200 Quality 30 Index Fund - Regular - Growth"),
        ("148704", "UTI",                   "UTI Nifty 200 Momentum 30 Index Fund - Regular - Growth"),
        ("143340", "UTI",                   "UTI Nifty Next 50 Index Fund - Regular - Growth"),
    ],
    "ELSS": [
        ("101834", "UTI",                   "UTI Master Equity Plan Unit Scheme"),
    ],
    "THEMATIC OTHERS": [
        ("154151", "Kotak Mahindra",        "Kotak Services Fund - Regular - Growth"),
        ("153561", "Motilal Oswal",         "Motilal Oswal Services Fund - Regular - Growth"),
        ("153721", "Bandhan",               "Bandhan Multi-Factor Fund - Regular - Growth"),
        ("153459", "ICICI Prudential",      "ICICI Prudential Quality Fund - Growth"),
        ("153181", "ICICI Prudential",      "ICICI Prudential Rural Opportunities Fund - Growth"),
        ("153865", "ICICI Prudential",      "ICICI Prudential Conglomerate Fund - Growth"),
        ("153969", "Kotak Mahindra",        "Kotak Rural Opportunities Fund - Regular - Growth"),
    ],
    "MONEY MARKET": [
        ("108756", "Bandhan",               "Bandhan Money Market Fund - Regular - Growth"),
    ],
    "CORPORATE BOND": [
        ("135914", "Bandhan",               "Bandhan Corporate Bond Fund - Regular - Growth"),
    ],
    "THEMATIC ENERGY": [
        ("153226", "Baroda BNP Paribas",    "Baroda BNP Paribas Energy Opportunities Fund - Regular - Growth"),
        ("108202", "DSP",                   "DSP Natural Resources And New Energy Fund - Regular - Growth"),
        ("152726", "ICICI Prudential",      "ICICI Prudential Energy Opportunities Fund - Growth"),
        ("153449", "Kotak Mahindra",        "Kotak Energy Opportunities Fund - Regular - Growth"),
        ("101262", "Nippon India",          "Nippon India Power & Infra Fund - Growth"),
        ("152418", "SBI",                   "SBI Energy Opportunities Fund - Regular - Growth"),
        ("135815", "Tata",                  "Tata Resources & Energy Fund - Regular - Growth"),
    ],
    "SECTORAL PHARMA HEALTHCARE": [
        ("153602", "Baroda BNP Paribas",    "Baroda BNP Paribas Health and Wellness Fund - Regular - Growth"),
        ("143873", "ICICI Prudential",      "ICICI Prudential Pharma Healthcare and Diagnostics (P.H.D) Fund - Growth"),
        ("145456", "DSP",                   "DSP Healthcare Fund - Regular - Growth"),
        ("102431", "Nippon India",          "Nippon India Pharma Fund - Growth"),
        ("102823", "SBI",                   "SBI Healthcare Opportunities Fund - Regular - Growth"),
        ("100807", "UTI",                   "UTI Healthcare Fund - Regular - Growth"),
        ("147407", "Aditya Birla Sun Life", "Aditya Birla Sun Life Pharma and Healthcare Fund - Regular - Growth"),
        ("153155", "Bajaj Finserv",         "Bajaj Finserv Healthcare Fund - Regular - Growth"),
        ("153974", "Bandhan",               "Bandhan Healthcare Fund - Regular - Growth"),
        ("152084", "HDFC",                  "HDFC Pharma and Healthcare Fund - Growth"),
        ("149270", "ITI",                   "ITI Pharma and Healthcare Fund - Regular - Growth"),
        ("152216", "Kotak Mahindra",        "Kotak Healthcare Fund - Regular - Growth"),
        ("152025", "LIC",                   "LIC MF Healthcare Fund - Regular - Growth"),
        ("143785", "Mirae Asset",           "Mirae Asset Healthcare Fund - Regular - Growth"),
        ("153040", "PGIM India",            "PGIM India Healthcare Fund - Regular - Growth"),
        ("151855", "quant",                 "quant Healthcare Fund - Growth - Regular"),
        # Tata India Pharma & Healthcare (135812, mf=25) deferred 2026-07-31:
        # AMFI's history endpoint serves its data only through 2026-07-24 and
        # returns nothing for 25-30 Jul (verified at 365/180/60/6-day chunk
        # sizes). Add it once AMFI serves that window -- the daily job cannot
        # backfill the gap, it only ever appends the current day.
        ("152348", "WhiteOak Capital",      "WhiteOak Capital Pharma and Healthcare Fund Regular Growth"),
    ],
    "THEMATIC ESG": [
        ("154194", "Baroda BNP Paribas",    "Baroda BNP Paribas Best-in-Class Strategy Fund - Regular - Growth"),
    ],
    "MULTI ASSET ALLOCATION": [
        ("153772", "360 ONE",              "360 ONE Multi Asset Allocation Fund - Regular - Growth"),
        ("151309", "Aditya Birla Sun Life", "ABSL Multi Asset Allocation Fund - Regular - Growth"),
        ("154440", "AlphaGrep",            "AlphaGrep Multi Asset Allocation Fund - Regular - Growth"),
        ("113064", "Axis",                 "Axis Multi Asset Allocation Fund - Regular - Growth"),
        ("152642", "Bajaj Finserv",        "Bajaj Finserv Multi Asset Allocation Fund - Regular - Growth"),
        ("152326", "Bandhan",              "Bandhan Multi Asset Allocation Fund - Regular - Growth"),
        ("152398", "Bank of India",        "BOI Multi Asset Allocation Fund - Regular - Growth"),
        ("150863", "Baroda BNP Paribas",   "Baroda BNP Paribas Multi Asset Fund - Regular - Growth"),
        ("153466", "Canara Robeco",        "Canara Robeco Multi Asset Allocation Fund - Regular - Growth"),
        ("154231", "Capitalmind",          "Capitalmind Multi Asset Allocation Fund - Regular - Growth"),
        ("152053", "DSP",                  "DSP Multi Asset Allocation Fund - Regular - Growth"),
        ("151795", "Edelweiss",            "Edelweiss Multi Asset Allocation Fund - Regular - Growth"),
        ("153733", "Franklin Templeton",   "Franklin India Multi Asset Allocation Fund - Growth"),
        ("153851", "Groww",                "Groww Multi Asset Allocation Fund - Regular - Growth"),
        ("103131", "HDFC",                 "HDFC Multi-Asset Fund - Growth"),
        ("152378", "HSBC",                 "HSBC Multi Asset Allocation Fund - Regular - Growth"),
        ("101144", "ICICI Prudential",     "ICICI Prudential Multi-Asset Fund - Growth"),
        ("153046", "Invesco",              "Invesco India Multi Asset Allocation Fund - Regular - Growth"),
        ("154424", "JM Financial",         "JM Multi Asset Allocation Fund (Regular) - Growth"),
        ("152065", "Kotak Mahindra",       "Kotak Multi Asset Allocation Fund - Regular - Growth"),
        ("153248", "LIC",                  "LIC MF Multi Asset Allocation Fund - Regular - Growth"),
        ("152441", "Mahindra Manulife",    "Mahindra Manulife Multi Asset Allocation Fund - Regular - Growth"),
        ("152347", "Mirae Asset",          "Mirae Asset Multi Asset Allocation Fund - Regular - Growth"),
        ("148459", "Nippon India",         "Nippon India Multi Asset Allocation Fund - Regular - Growth"),
        ("153988", "PGIM India",           "PGIM India Multi Asset Allocation Fund - Regular - Growth"),
        ("101072", "quant",                "quant Multi Asset Allocation Fund - Growth - Regular"),
        ("152475", "Quantum",              "Quantum Multi Asset Allocation Fund - Regular - Growth"),
        ("153093", "Samco",                "Samco Multi Asset Allocation Fund - Regular - Growth"),
        ("103408", "SBI",                  "SBI Multi Asset Allocation Fund - Regular - Growth"),
        ("152052", "Shriram",              "Shriram Multi Asset Allocation Fund - Regular Growth"),
        ("152311", "Sundaram",             "Sundaram Multi Asset Allocation Fund - Regular Plan - Growth"),
        ("148050", "Tata",                 "Tata Multi Asset Allocation Fund - Regular - Growth"),
        ("154002", "The Wealth Co.",       "The Wealth Company Multi Asset Allocation Fund - Regular Growth"),
        ("152786", "Union",                "Union Multi Asset Allocation Fund - Regular - Growth"),
        ("111599", "UTI",                  "UTI Multi Asset Allocation Fund - Regular - Growth"),
        ("151746", "WhiteOak Capital",     "WhiteOak Capital Multi Asset Allocation Fund Regular Plan Growth"),
    ],
    "DIVIDEND YIELD": [
        ("103678", "Franklin Templeton",    "Franklin India Dividend Yield Fund - Growth"),
    ],
    "ARBITRAGE": [
        ("130771", "Axis",                  "Axis Arbitrage Fund - Regular Plan - Growth"),
        ("130205", "Edelweiss",             "Edelweiss Arbitrage Fund - Regular Plan - Growth Option"),
        ("141605", "Edelweiss",             "Edelweiss Arbitrage Fund - Monthly Regular Plan - IDCW Option"),
        ("106793", "HDFC",                  "HDFC Arbitrage Fund - Regular - Growth"),
        ("105603", "Invesco",               "Invesco India Arbitrage Fund - Regular Plan - Growth Option"),
        ("105968", "Kotak Mahindra",        "Kotak Arbitrage Fund - Regular - Growth"),
        ("148400", "Mirae Asset",           "Mirae Asset Arbitrage Fund Regular Growth"),
        ("104457", "SBI",                   "SBI Arbitrage Opportunities Fund - Regular Plan - Growth"),
        ("145723", "Tata",                  "Tata Arbitrage Fund - Regular Plan - Growth"),
        ("104075", "UTI",                   "UTI Arbitrage Fund - Regular Plan - Growth Option"),
    ],
    "FOF DOMESTIC EQUITY": [
        ("153861", "HDFC",                  "HDFC Diversified Equity All Cap Active FOF - Regular - Growth"),
        ("102135", "ICICI Prudential",      "ICICI Prudential Aggressive Hybrid Active FOF - Growth"),
        ("143904", "ICICI Prudential",      "ICICI Prudential Bharat 22 FOF - Growth"),
    ],
    "FOF DOMESTIC GOLD": [
        ("115833", "ICICI Prudential",      "ICICI Prudential Gold ETF FOF - Growth"),
    ],
    "FOF DOMESTIC DEBT": [
        ("102141", "ICICI Prudential",      "ICICI Prudential Diversified Debt Strategy Active FOF - Growth"),
    ],
    "FOF DOMESTIC GOLD AND SILVER": [
        ("153922", "Kotak Mahindra",        "Kotak Gold Silver Passive FOF - Regular - Growth"),
        ("153814", "Mirae Asset",           "Mirae Asset Gold Silver Passive FoF - Regular - Growth"),
        ("150641", "Motilal Oswal",         "Motilal Oswal Gold and Silver Passive Fund of Funds - Regular - Growth"),
    ],
    "CONTRA": [
        ("103040", "Kotak Mahindra",        "Kotak Contra Fund - Regular - Growth"),
    ],
    "MEDIUM DURATION": [
        ("130037", "Nippon India",          "Nippon India Medium Duration Fund - Growth"),
    ],
    "THEMATIC BUSINESS CYCLE": [
        ("149296", "Aditya Birla Sun Life", "ABSL Business Cycle Fund - Regular - Growth"),
        ("151366", "Axis",                  "Axis Business Cycles Fund - Regular - Growth"),
        ("152877", "Bandhan",               "Bandhan Business Cycle Fund - Regular - Growth"),
        ("152790", "Bank of India",         "BOI Business Cycle Fund - Regular - Growth"),
        ("149140", "Baroda BNP Paribas",    "Baroda BNP Paribas Business Cycle Fund - Regular - Growth"),
        ("153122", "DSP",                   "DSP Business Cycle Fund - Regular - Growth"),
        ("152724", "Edelweiss",             "Edelweiss Business Cycle Fund - Regular - Growth"),
        ("150803", "HDFC",                  "HDFC Business Cycle Fund - Growth"),
        ("151144", "HSBC",                  "HSBC Business Cycles Fund - Regular Growth"),
        ("148653", "ICICI Prudential",      "ICICI Prudential Business Cycle Fund - Growth"),
        ("153288", "Invesco",               "Invesco India Business Cycle Fund - Regular - Growth"),
        ("154159", "ITI",                   "ITI Business Cycle Fund - Regular - Growth"),
        ("150626", "Kotak Mahindra",        "Kotak Business Cycle Fund - Regular - Growth"),
        ("152046", "Mahindra Manulife",     "Mahindra Manulife Business Cycle Fund - Regular - Growth"),
        ("152794", "Motilal Oswal",         "Motilal Oswal Business Cycle Fund - Regular - Growth"),
        ("151758", "quant",                 "quant Business Cycle Fund - Growth - Regular"),
        ("152678", "Sundaram",              "Sundaram Business Cycle Fund - Regular Plan - Growth"),
        ("149068", "Tata",                  "Tata Business Cycle Fund - Regular - Growth"),
        ("152411", "Union",                 "Union Business Cycle Fund - Regular - Growth"),
    ],
    "SECTORAL BANKING FINANCIAL SERVICES": [
        ("125595", "Aditya Birla Sun Life", "ABSL Banking and Financial Services Fund - Regular - Growth"),
        ("153935", "Bajaj Finserv",         "Bajaj Finserv Banking and Financial Services Fund - Regular - Growth"),
        ("151818", "Bandhan",               "Bandhan Financial Services Fund - Regular - Growth"),
        ("154095", "Bank of India",         "BOI Banking & Financial Services Fund - Regular - Growth"),
        ("117549", "Baroda BNP Paribas",    "Baroda BNP Paribas Banking and Financial Services Fund - Regular - Growth"),
        ("154248", "Canara Robeco",         "Canara Robeco Banking and Financial Services Fund - Regular - Growth"),
        ("152208", "DSP",                   "DSP Banking & Financial Services Fund - Regular - Growth"),
        ("154107", "Edelweiss",             "Edelweiss Financial Services Fund - Regular - Growth"),
        ("152274", "Groww",                 "Groww Banking & Financial Services Fund - Regular - Growth"),
        ("148987", "HDFC",                  "HDFC Banking & Financial Services Fund - Growth"),
        ("152682", "Helios",                "Helios Financial Services Fund - Regular - Growth"),
        ("153267", "HSBC",                  "HSBC Financial Services Fund - Regular Growth"),
        ("109445", "ICICI Prudential",      "ICICI Prudential Banking and Financial Services Fund - Growth"),
        ("108378", "Invesco",               "Invesco India Financial Services Fund - Regular - Growth"),
        ("149324", "ITI",                   "ITI Banking and Financial Services Fund - Regular - Growth"),
        ("151381", "Kotak Mahindra",        "Kotak Banking & Financial Services Fund - Regular - Growth"),
        ("134016", "LIC",                   "LIC MF Banking and Financial Services Fund - Regular - Growth"),
        ("153675", "Mahindra Manulife",     "Mahindra Manulife Banking & Financial Services Fund - Regular - Growth"),
        ("148621", "Mirae Asset",           "Mirae Asset Banking and Financial Services Fund - Regular - Growth"),
        ("154137", "Motilal Oswal",         "Motilal Oswal Financial Services Fund - Regular - Growth"),
        ("101862", "Nippon India",          "Nippon India Banking & Financial Services Fund - Growth"),
        ("151788", "quant",                 "quant BFSI Fund - Growth - Regular"),
        ("133858", "SBI",                   "SBI Banking & Financial Services Fund - Regular - Growth"),
        ("109059", "Sundaram",              "Sundaram Financial Services Opportunities Fund - Regular - Growth"),
        ("135794", "Tata",                  "Tata Banking and Financial Services Fund - Regular - Growth"),
        ("117312", "Taurus",                "Taurus Banking & Financial Services Fund - Regular - Growth"),
        ("102401", "UTI",                   "UTI Banking and Financial Services Fund - Regular - Growth"),
        ("152322", "WhiteOak Capital",      "WhiteOak Capital Banking & Financial Services Fund - Regular Growth"),
    ],
    "SECTORAL TECHNOLOGY": [
        ("100522", "Franklin Templeton",    "Franklin India Technology Fund - Growth"),
        ("100363", "ICICI Prudential",      "ICICI Prudential Technology Fund - Growth"),
        ("135797", "Tata",                  "Tata Digital India Fund - Regular - Growth"),
        ("120577", "SBI",                   "SBI Technology Opportunities Fund - Regular - Growth"),
        ("152966", "Motilal Oswal",         "Motilal Oswal Digital India Fund - Regular - Growth"),
        ("152439", "Edelweiss",             "Edelweiss Technology Fund - Regular - Growth"),
        ("103168", "Aditya Birla Sun Life", "Aditya Birla Sun Life Digital India Fund - Regular - Growth"),
        ("152862", "Invesco",               "Invesco India Technology Fund - Regular - Growth"),
        ("152058", "HDFC",                  "HDFC Technology Fund - Regular - Growth"),
        ("152460", "Kotak Mahindra",        "Kotak Technology Fund - Regular - Growth"),
        ("152070", "quant",                 "quant Teck Fund - Regular - Growth"),
        ("152914", "WhiteOak Capital",      "WhiteOak Capital Digital Bharat Fund Regular Growth"),
    ],
}


# ── Data loading ──────────────────────────────────────────────────────────────

def load_all_raw_parquets() -> pl.DataFrame:
    files = sorted(RAW_NAV_DIR.glob("year=*/*.parquet"))
    if not files:
        log.error("No raw parquet files found under %s", RAW_NAV_DIR)
        sys.exit(1)

    log.info("Found %d raw parquet file(s) across %d year partition(s)",
             len(files), len({f.parent for f in files}))

    dfs = [
        pl.read_parquet(f).with_columns(pl.col("scheme_code").cast(pl.Utf8))
        for f in files
    ]
    combined = pl.concat(dfs, how="diagonal_relaxed")
    log.info("Total rows loaded: %d", len(combined))
    return combined


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(df: pl.DataFrame) -> None:
    available = set(df["scheme_code"].unique().to_list())

    W = 100
    total_schemes = sum(len(v) for v in FUND_UNIVERSE.values())
    total_present = 0
    total_missing = 0

    # Pre-compute per-code stats for present codes
    stats = (
        df.group_by("scheme_code")
          .agg([
              pl.col("nav_date").min().alias("date_from"),
              pl.col("nav_date").max().alias("date_to"),
              pl.len().alias("rows"),
          ])
    )
    stats_map = {
        row["scheme_code"]: row
        for row in stats.to_dicts()
    }

    print("\n" + "=" * W)
    print(f"  MUTUAL FUND DATA COVERAGE REPORT  |  Raw NAV Data Check")
    print(f"  Total unique scheme codes in raw data : {len(available)}")
    print(f"  Fund universe size                    : {total_schemes} schemes across {len(FUND_UNIVERSE)} categories")
    print("=" * W)

    category_summary: list[tuple[str, int, int]] = []

    for category, funds in FUND_UNIVERSE.items():
        present = [(c, h, n) for c, h, n in funds if c in available]
        missing = [(c, h, n) for c, h, n in funds if c not in available]
        total_present += len(present)
        total_missing += len(missing)
        category_summary.append((category, len(present), len(missing)))

        status_icon = "OK" if not missing else f"MISSING {len(missing)}"
        print(f"\n  [{status_icon}]  {category}  ({len(present)}/{len(funds)} present)")
        print("  " + "-" * (W - 2))

        if present:
            print(f"  {'CODE':<10} {'FUND HOUSE':<22} {'ROWS':>6}  {'DATE FROM':<12} {'DATE TO':<12}  SCHEME")
            for code, house, name in present:
                s = stats_map.get(code, {})
                rows     = s.get("rows", 0)
                d_from   = str(s.get("date_from", ""))
                d_to     = str(s.get("date_to", ""))
                print(f"  {code:<10} {house:<22} {rows:>6}  {d_from:<12} {d_to:<12}  {name}")

        if missing:
            print(f"\n  {'CODE':<10} {'FUND HOUSE':<22}  SCHEME  [NOT FOUND IN RAW DATA]")
            for code, house, name in missing:
                print(f"  {code:<10} {house:<22}  {name}")

    # ── Overall summary table ──────────────────────────────────────────────
    print("\n" + "=" * W)
    print(f"  CATEGORY SUMMARY")
    print("  " + "-" * (W - 2))
    print(f"  {'CATEGORY':<22} {'TOTAL':>6} {'PRESENT':>8} {'MISSING':>8}  COVERAGE")
    print("  " + "-" * (W - 2))
    for cat, pres, miss in category_summary:
        total = pres + miss
        pct   = pres / total * 100 if total else 0
        bar   = "#" * int(pct / 5) + "-" * (20 - int(pct / 5))
        flag  = "" if miss == 0 else f"  <-- {miss} missing"
        print(f"  {cat:<22} {total:>6} {pres:>8} {miss:>8}  [{bar}] {pct:5.1f}%{flag}")
    print("  " + "-" * (W - 2))
    overall_pct = total_present / total_schemes * 100 if total_schemes else 0
    print(f"  {'TOTAL':<22} {total_schemes:>6} {total_present:>8} {total_missing:>8}  {overall_pct:.1f}% coverage")
    print("=" * W + "\n")

    if total_missing == 0:
        log.info("All %d schemes across all categories are present in raw data.", total_schemes)
    else:
        log.warning("%d / %d scheme(s) are missing from raw data.", total_missing, total_schemes)


# ── Main ──────────────────────────────────────────────────────────────────────

def check_amc_codes() -> int:
    """
    Assert every fund_house label resolves to an AMFI `mf=` id.

    Run this after adding funds: an unresolved label means backfilling that AMC
    would need a hand-guessed mf= id, which is how the wrong AMC's history gets
    fetched. Returns the number of unresolved labels (0 = healthy).
    """
    houses = sorted({h for funds in FUND_UNIVERSE.values() for _, h, _ in funds})
    unresolved: list[tuple[str, str]] = []
    for house in houses:
        try:
            amc_code(house)
        except KeyError as exc:
            unresolved.append((house, str(exc)))

    log.info("fund_house labels: %d  |  resolved: %d  |  unresolved: %d",
             len(houses), len(houses) - len(unresolved), len(unresolved))
    for house, err in unresolved:
        log.error("  %s -> %s", house, err)
    if unresolved:
        log.error("Add the verified id to AMFI_AMC_CODES (or an entry to "
                  "AMC_NAME_ALIASES) in config/constants.py.")
    return len(unresolved)


def main() -> None:
    if "--check-amc-codes" in sys.argv:
        sys.exit(1 if check_amc_codes() else 0)
    log.info("Loading raw NAV data from: %s", RAW_NAV_DIR)
    df = load_all_raw_parquets()
    print_report(df)


if __name__ == "__main__":
    main()
