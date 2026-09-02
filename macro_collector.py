"""Macro data collection — pulls economic indicators from FRED into SQLite.

Two entry points:
- backfill_macro_all(years=10)  → fetch full history for every indicator
- collect_macro_latest()         → fetch only new observations since last stored

Computed series (spreads, real yields) are derived in the API layer from raw
inputs; they are NOT stored separately to avoid double-bookkeeping.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, timedelta
from typing import Optional

import requests
import yfinance as yf

import database as db
import fred_client

logger = logging.getLogger(__name__)

_STOOQ_URL = "https://stooq.com/q/d/l/"
_STOOQ_TIMEOUT = 15  # seconds


def _stooq_fetch(ticker: str, start: str) -> list[tuple[str, float]]:
    """Pull daily closes from Stooq's CSV endpoint.

    Stooq returns plain CSV: Date,Open,High,Low,Close,Volume. Used for series
    not on FRED and not on Yahoo Finance — e.g. China 10Y bond yield (10cny.b).
    No API key required.
    """
    try:
        r = requests.get(
            _STOOQ_URL,
            params={"s": ticker, "i": "d"},
            headers={"User-Agent": "vol-dashboard/1.0"},
            timeout=_STOOQ_TIMEOUT,
        )
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Stooq fetch failed for %s: %s", ticker, exc)
        return []
    text = r.text.strip()
    # Stooq returns "No data" (sometimes with surrounding whitespace) for bad tickers
    if not text or text.lower().startswith("no data"):
        logger.warning("Stooq returned no data for %s — verify ticker", ticker)
        return []
    out: list[tuple[str, float]] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        ds = row.get("Date") or ""
        if ds < start:
            continue
        try:
            close = float(row.get("Close") or "")
        except (TypeError, ValueError):
            continue
        if close != close:  # NaN
            continue
        out.append((ds, close))
    return out


def _yf_fetch(ticker: str, start: str) -> list[tuple[str, float]]:
    """Pull daily closes from Yahoo Finance for non-FRED macro instruments.

    Returns (YYYY-MM-DD, close) tuples. Used for ICE DXY, MOVE, anything that
    isn't free on FRED. Stays inside the existing yfinance integration so we
    don't add a new dependency.
    """
    try:
        df = yf.Ticker(ticker).history(start=start, auto_adjust=False)
    except Exception as exc:
        logger.warning("yfinance fetch failed for %s: %s", ticker, exc)
        return []
    if df is None or df.empty:
        return []
    out: list[tuple[str, float]] = []
    for ts, row in df.iterrows():
        try:
            close = float(row.get("Close"))
        except (TypeError, ValueError):
            continue
        if close != close:  # NaN check without importing math
            continue
        out.append((ts.strftime("%Y-%m-%d"), close))
    return out


_SPDR_ARCHIVE_URL = "https://api.spdrgoldshares.com/api/v1/historical-archive"
_IBIT_HOLDINGS_URL = ("https://www.ishares.com/us/products/333011/"
                      "ishares-bitcoin-trust-etf/latest-holdings.csv")
_ISSUER_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def _spdr_gld_tonnes(start: str) -> list[tuple[str, float]]:
    """Daily tonnes of gold held in the GLD trust, from SPDR's official
    historical-archive API (XLSX, full history since Nov 2004).

    Tonnes-in-trust changes only on creations/redemptions, so its delta is the
    western gold ETF flow, independent of price. The whole archive is fetched
    each run (~540KB); rows at or before `start` are dropped and the DB layer
    dedups the rest.
    """
    import datetime as _dt
    import openpyxl
    try:
        r = requests.get(_SPDR_ARCHIVE_URL,
                         params={"product": "gld", "exchange": "NYSE", "lang": "en"},
                         timeout=60, headers=_ISSUER_HEADERS)
        r.raise_for_status()
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True)
        ws = wb["US GLD Historical Archive"]
    except Exception as exc:
        logger.warning("SPDR GLD archive fetch failed: %s", exc)
        return []
    out: list[tuple[str, float]] = []
    in_data = False
    for row in ws.iter_rows(values_only=True):
        if not in_data:
            in_data = row and row[0] == "Date"
            continue
        try:
            ts = _dt.datetime.strptime(str(row[0]), "%d-%b-%Y").date().isoformat()
            tonnes = float(row[9])
        except (TypeError, ValueError, IndexError):
            continue
        if ts > start:
            out.append((ts, round(tonnes, 2)))
    return out


_NYFED_HLW_URL = ("https://www.newyorkfed.org/medialibrary/media/research/"
                  "economists/williams/data/Holston_Laubach_Williams_current_estimates.xlsx")


def _nyfed_hlw_rstar(start: str) -> list[tuple[str, float]]:
    """US natural rate of interest (r*) from the NY Fed's official
    Holston-Laubach-Williams estimates (quarterly XLSX, 1961→present).

    Sheet 'HLW Estimates': col A is the quarter start date, col K the US r*.
    Published quarterly with a lag; estimates carry ~±1pp standard errors —
    treat as a model anchor, not an observable.
    """
    import datetime as _dt
    import openpyxl
    try:
        r = requests.get(_NYFED_HLW_URL, timeout=60, headers=_ISSUER_HEADERS)
        r.raise_for_status()
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True)
        ws = wb["HLW Estimates"]
    except Exception as exc:
        logger.warning("NY Fed HLW fetch failed: %s", exc)
        return []
    out: list[tuple[str, float]] = []
    for row in ws.iter_rows(values_only=True):
        try:
            ts = row[0].date().isoformat() if isinstance(row[0], _dt.datetime) else None
            val = float(row[10])
        except (TypeError, ValueError, IndexError):
            continue
        if ts and ts > start:
            out.append((ts, round(val, 2)))
    return out


_DEBT_PENNY_URL = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
                   "/v2/accounting/od/debt_to_penny")


def _treasury_debt_penny(start: str) -> list[tuple[str, float]]:
    """Total public debt outstanding, daily, from Treasury's Debt to the Penny
    API (no key required). Stored in $B. FRED's GFDEBTN carries the same
    number but lags ~2 quarters; this is the live series.
    """
    try:
        r = requests.get(_DEBT_PENNY_URL, timeout=60, headers=_ISSUER_HEADERS, params={
            "fields": "record_date,tot_pub_debt_out_amt",
            "filter": f"record_date:gte:{start}",
            "sort": "record_date",
            "page[size]": "10000",
        })
        r.raise_for_status()
        data = r.json().get("data", [])
    except Exception as exc:
        logger.warning("Debt to the Penny fetch failed: %s", exc)
        return []
    out: list[tuple[str, float]] = []
    for row in data:
        try:
            out.append((row["record_date"],
                        round(float(row["tot_pub_debt_out_amt"]) / 1e9, 1)))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _ibit_btc_holdings() -> list[tuple[str, float]]:
    """BTC held in the IBIT trust, from iShares' latest-holdings CSV.

    The file carries its own as-of date ('Fund Holdings as of,"Aug 28, 2026"'),
    so weekend/holiday runs re-report the last trading day and dedup in the DB.
    No history endpoint exists — the series accumulates one point per trading
    day from first collection (2026-08-28).
    """
    import datetime as _dt
    try:
        r = requests.get(_IBIT_HOLDINGS_URL, timeout=30, headers=_ISSUER_HEADERS)
        r.raise_for_status()
        lines = r.text.splitlines()
        asof = None
        for row in csv.reader(lines[:8]):
            if row and row[0].strip().lower().startswith("fund holdings as of"):
                asof = _dt.datetime.strptime(row[1].strip(), "%b %d, %Y").date().isoformat()
                break
        for row in csv.reader(lines):
            if row and row[0].strip('"') == "BTC":
                qty = float(row[7].replace(",", ""))
                return [(asof, round(qty, 2))] if asof else []
    except Exception as exc:
        logger.warning("IBIT holdings fetch failed: %s", exc)
    return []


def _unpack_source(entry: tuple) -> tuple[str, str]:
    """Return (source_type, source_arg). source_type is 'fred', 'yfinance', 'spdrgold', 'ibitcsv', or 'stooq'."""
    if len(entry) <= 5:
        return ("fred", entry[0])
    src = entry[5] or "fred"
    if src.startswith("yf:"):
        return ("yfinance", src[3:])
    if src == "spdrgold":
        return ("spdrgold", entry[0])
    if src == "nyfedhlw":
        return ("nyfedhlw", entry[0])
    if src == "debtpenny":
        return ("debtpenny", entry[0])
    if src == "ibitcsv":
        return ("ibitcsv", entry[0])
    if src.startswith("stooq:"):
        return ("stooq", src[6:])
    return ("fred", entry[0])


# ── Indicator catalog ─────────────────────────────────────────────────────────
# (series_id, display_name, category, units, frequency)
# Series IDs are FRED unless flagged otherwise in source kwarg.

MACRO_SERIES: list[tuple[str, str, str, str, str]] = [
    # US Rates — Treasury yield curve + policy rates
    ("SOFR",     "SOFR",                    "US Rates",    "%",   "daily"),
    ("IORB",     "IORB",                    "US Rates",    "%",   "daily"),
    ("DFF",      "Fed Funds",               "US Rates",    "%",   "daily"),
    ("DGS3MO",   "3M T-Bill",               "US Rates",    "%",   "daily"),
    ("DGS2",     "2Y UST",                  "US Rates",    "%",   "daily"),
    ("DGS5",     "5Y UST",                  "US Rates",    "%",   "daily"),
    ("DGS10",    "10Y UST",                 "US Rates",    "%",   "daily"),
    ("DGS30",    "30Y UST",                 "US Rates",    "%",   "daily"),

    # Global & Spreads
    ("IRLTLT01JPM156N", "Japan 10Y",        "Global & Spreads","%",   "monthly"),
    ("IRLTLT01DEM156N", "Germany 10Y",      "Global & Spreads","%",   "monthly"),
    ("IRLTLT01GBM156N", "UK 10Y",           "Global & Spreads","%",   "monthly"),
    ("DFII10",   "Real Yield 10Y",          "Global & Spreads","%",   "daily"),

    # FX & Dollar — ICE DXY comes from yfinance (FRED only has trade-weighted index)
    ("DXY",      "DXY",                     "FX & Dollar", "Index","daily", "yf:DX-Y.NYB"),
    ("DEXJPUS",  "USDJPY",                  "FX & Dollar", "FX",  "daily"),
    ("DEXUSEU",  "EURUSD",                  "FX & Dollar", "FX",  "daily"),
    ("DEXUSUK",  "GBPUSD",                  "FX & Dollar", "FX",  "daily"),
    ("DEXCHUS",  "USDCNH",                  "FX & Dollar", "FX",  "daily"),
    ("DEXSZUS",  "USDCHF",                  "FX & Dollar", "FX",  "daily"),

    # Commodities & Vol
    ("DCOILBRENTEU", "Brent Oil",           "Commodities & Vol", "USD","daily"),
    ("DCOILWTICO", "WTI Oil",               "Commodities & Vol", "USD","daily"),
    ("DHHNGSP",  "Natural Gas",             "Commodities & Vol", "USD","daily"),
    ("PCOPPUSDM","Copper",                  "Commodities & Vol", "USD","monthly"),
    ("GOLD",     "Gold (XAU)",              "Commodities & Vol", "USD","daily", "yf:GC=F"),
    ("MOVE",     "MOVE Index",              "Commodities & Vol", "Index","daily","yf:^MOVE"),

    # Money & Credit
    ("M2SL",     "M2 Money Supply",         "Money & Credit","$B", "weekly"),
    ("WALCL",    "Fed Balance Sheet",       "Money & Credit","$M", "weekly"),
    ("WDTGAL",   "TGA",                     "Money & Credit","$M", "weekly"),
    ("WRESBAL",  "Bank Reserves",           "Money & Credit","$M", "weekly"),
    ("RRPONTSYD","RRP Usage",               "Money & Credit","$B", "daily"),
    ("RPONTSYD", "SRF Usage",               "Money & Credit","$B", "daily"),
    ("BAMLH0A0HYM2", "HY Credit Spread",    "Money & Credit","%",  "daily"),

    # Reserve adequacy denominators (used to compute Reserves/GDP and Ample Reserves)
    ("TLAACBW027SBOG", "Total Bank Assets", "Reserve Adequacy", "$B", "weekly"),
    ("GDP",      "Nominal GDP",             "Reserve Adequacy", "$B", "quarterly"),

    # Inflation
    # CPI indexes are NSA (CPIAUCNS/CPILFENS): BLS quotes headline 12-month
    # changes from the NSA series, so YoY here matches the reported prints.
    # Core PCE stays SA (PCEPILFE) — BEA reports PCE inflation from SA data.
    ("CPIAUCNS", "CPI",                     "Inflation",   "Index","monthly"),
    ("PCEPILFE", "Core PCE",                "Inflation",   "Index","monthly"),
    ("CPILFENS", "Core CPI",                "Inflation",   "Index","monthly"),  # raw index; YoY surfaced as core_cpi_yoy
    ("PCETRIM12M159SFRBDAL", "Trimmed Mean PCE (YoY)", "Inflation", "%", "monthly"),  # Dallas Fed — already YoY

    # Inflation Forwards — market-derived breakeven and forward inflation expectations
    ("T5YIFR",   "5y5y Forward Inflation",  "Inflation Forwards", "%", "daily"),
    ("T5YIE",    "5Y Breakeven Inflation",  "Inflation Forwards", "%", "daily"),
    ("T10YIE",   "10Y Breakeven Inflation", "Inflation Forwards", "%", "daily"),
    ("T30YIEM",  "30Y Breakeven Inflation", "Inflation Forwards", "%", "monthly"),

    # ── Liquidity Framework additions (Phase 1) ───────────────────────────────

    # Central Bank Liquidity — additional CB balance sheets (§4.1)
    ("ECBASSETSW",       "ECB Total Assets",          "CB Liquidity", "$M",  "weekly"),
    ("JPNASSETS",        "BoJ Total Assets",          "CB Liquidity", "$M",  "monthly"),
    # Note: BoE (BOENABASSETSW) and PBoC (CHCBBSNAV) do not exist on FRED —
    # deferred to Opus for replacement ticker research.

    # Bank Credit — US and foreign M3/credit (§4.2.1)
    ("BUSLOANS",         "C&I Loans",                 "Bank Credit",  "$B",  "monthly"),
    ("TOTLL",            "Total Bank Loans",          "Bank Credit",  "$B",  "weekly"),
    ("H8B1023NCBCMG",   "Commercial Bank Credit",    "Bank Credit",  "$B",  "weekly"),
    # OECD global M3 series — note that 2024-01-01 start returns empty;
    # must use start_date ~2015-01-01 to retrieve data (OECD lag applies).
    ("MABMM301EZQ189S",  "Euro Area M3 (OECD)",      "Bank Credit",  "Index","quarterly"),
    ("MABMM301JPM189S",  "Japan M3 (OECD)",          "Bank Credit",  "Index","monthly"),
    ("MABMM301CNM189S",  "China M3 (OECD)",          "Bank Credit",  "Index","monthly"),
    ("MABMM301GBQ189S",  "UK M3 (OECD)",             "Bank Credit",  "Index","quarterly"),

    # Shadow Banking US (§4.2.2)
    ("MMMFFAQ027S",      "MMF Total Assets",          "Shadow Banking","$B", "quarterly"),
    ("COMPOUT",          "Commercial Paper Outstandg","Shadow Banking","$B", "weekly"),
    ("MABMM301USA189S",  "US Monetary Base Ext (OECD)","Shadow Banking","Index","quarterly"),
    # FINRECEIVABLES does not exist on FRED — deferred to Opus.
    # GSE total liabilities — BOGZ1FL403190005Q exists but ends 2024-Q2 and
    # appears to be GSE misc liabilities (~$98B) rather than total credit-market
    # liabilities; ASTDSL (Agency+GSE-backed debt securities outstanding) is a
    # better proxy for GSE balance sheet exposure (~$66T notional, all sectors).
    ("ASTDSL",           "Agency/GSE-Backed Debt Secs","Shadow Banking","$M","quarterly"),

    # ── Liquidity Framework additions (Phase 2) ───────────────────────────────

    # Z.1 Flow of Funds via FRED (§4.2.3)
    # TCMDO: Total Credit Market Debt Outstanding, all sectors (Z.1 L.2 table).
    # Units: millions of USD, quarterly. Latest: 2026-Q1 ~$115.5T.
    ("TCMDO",            "Total Credit Market Debt",  "Z.1 Flow of Funds","$M","quarterly"),
    # BOGZ1FL892090005Q: all-sectors credit market instruments outstanding (Z.1).
    # A broader aggregate than TCMDO (~$167T latest), includes financial sectors.
    ("BOGZ1FL892090005Q","All-Sector Credit Instruments","Z.1 Flow of Funds","$M","quarterly"),
    # BOGZ1FL403065005Q: GSE total financial assets (Z.1 L.121 table).
    # Units: millions of USD, quarterly. ~$7.7T latest (Fannie/Freddie/FHLBs).
    ("BOGZ1FL403065005Q","GSE Total Financial Assets", "Z.1 Flow of Funds","$M","quarterly"),
    # BOGZ1FL664090005Q: securities broker-dealer total financial assets (Z.1 L.130).
    # Units: millions of USD, quarterly. ~$6.7T latest.
    ("BOGZ1FL664090005Q","Broker-Dealer Total Assets", "Z.1 Flow of Funds","$M","quarterly"),

    # Offshore $ rate spread inputs (§4.3 computed series, Phase 2)
    # SOFR is already in "US Rates". OBFR is the Overnight Bank Funding Rate
    # (published by NY Fed; includes Eurodollar/offshore segment).
    # Spread OBFR - SOFR = offshore premium over pure US secured rate.
    # Note: OBFR starts 2016-03-01 (SOFR starts 2018-04-03); overlap from SOFR start.
    ("OBFR",             "Overnight Bank Funding Rate","Cross-Border",  "%",  "daily"),
    # IR3TIB01EZM156N: Euro area 3M interbank rate (OECD). Monthly.
    # Used as the EUR-denominated funding rate in the cross-border spread.
    ("IR3TIB01EZM156N",  "Euro Area 3M Interbank",    "Cross-Border",  "%",  "monthly"),
    # IR3TIB01USM156N: US 3M interbank / money market rate (OECD). Monthly.
    # Pairing with EUR rate gives cross-currency funding spread.
    ("IR3TIB01USM156N",  "US 3M Interbank Rate",      "Cross-Border",  "%",  "monthly"),

    # Cross-Border Flows (§4.3)
    ("RBUSBIS",          "Real Broad Dollar Index",   "Cross-Border",  "Index","monthly"),

    # Quality Overlays (§4.4)
    ("THREEFYTP10",      "Term Premium 10Y (ACM)",    "Quality Overlays","%", "daily"),

    # Liquidity Overlays — price overlays for the composite chart (§4.5)
    ("BTC-USD",  "Bitcoin",                           "Liquidity Overlays","USD","daily","yf:BTC-USD"),
    ("SPX",      "S&P 500",                           "Liquidity Overlays","Index","daily","yf:^GSPC"),

    # Debasement Watch — western-bid detectors (ETF creations/redemptions),
    # measured in metal/coins held in trust (price-independent, issuer-official).
    # GLD: full daily history since 2004 from SPDR's archive API.
    # IBIT: iShares publishes only current holdings (with as-of date), so the
    # series accumulates one point per trading day from 2026-08-28; its flow
    # series (ibit_flow_4w) populates ~4 weeks after that.
    ("GLD_TONNES", "Gold in GLD Trust", "Debasement Watch", "tonnes", "daily", "spdrgold"),
    ("IBIT_BTC",   "BTC in IBIT Trust", "Debasement Watch", "BTC",    "daily", "ibitcsv"),

    # Neutral rate — NY Fed Holston-Laubach-Williams US r* (quarterly model
    # estimate, ±1pp standard errors). Market-implied and stance versions are
    # computed series (rstar_market, policy_gap).
    ("RSTAR_HLW",  "R-star (HLW, NY Fed)", "US Rates", "%", "quarterly", "nyfedhlw"),

    # Fiscal — Gromen "true interest expense" inputs (NIPA quarterly, SAAR $B).
    # True IE = interest + SS + Medicare + Medicaid + VA, vs tax receipts
    # (W006 excludes payroll contributions; W780 is the payroll leg for the
    # symmetric/net version). Medicaid here is the NIPA benefits series, which
    # runs close to the federal share of program cost.
    ("A091RC1Q027SBEA", "Interest Expense",         "Fiscal", "$B", "quarterly"),
    ("W006RC1Q027SBEA", "Federal Tax Receipts",     "Fiscal", "$B", "quarterly"),
    ("W823RC1Q027SBEA", "Social Security Benefits", "Fiscal", "$B", "quarterly"),
    ("W824RC1Q027SBEA", "Medicare Benefits",        "Fiscal", "$B", "quarterly"),
    ("W729RC1Q027SBEA", "Medicaid Benefits",        "Fiscal", "$B", "quarterly"),
    ("W826RC1Q027SBEA", "Veterans Benefits",        "Fiscal", "$B", "quarterly"),
    ("W780RC1Q027SBEA", "Payroll Tax Receipts",     "Fiscal", "$B", "quarterly"),
    # GFDEBTN (quarterly, ~2q lag) is kept only for avg_debt_rate, where it must
    # align with quarterly NIPA interest; DEBT_PENNY is the live daily number.
    ("GFDEBTN",         "Gross Federal Debt (qtly)","Fiscal", "$M", "quarterly"),
    ("DEBT_PENNY",      "Gross Federal Debt",       "Fiscal", "$B", "daily", "debtpenny"),
]


def _seed_metadata() -> None:
    """Ensure every catalog entry has a row in macro_metadata."""
    for entry in MACRO_SERIES:
        sid, name, cat, units, freq = entry[:5]
        src_type, _ = _unpack_source(entry)
        db.upsert_macro_metadata(sid, name, cat, units, freq, source=src_type)


def _fetch_series(entry: tuple, start: str, end: Optional[str] = None) -> list[tuple[str, float]]:
    """Dispatch fetch by source (FRED, yfinance, or Stooq)."""
    src_type, src_arg = _unpack_source(entry)
    if src_type == "yfinance":
        return _yf_fetch(src_arg, start)
    if src_type == "spdrgold":
        return _spdr_gld_tonnes(start)
    if src_type == "nyfedhlw":
        return _nyfed_hlw_rstar(start)
    if src_type == "debtpenny":
        return _treasury_debt_penny(start)
    if src_type == "ibitcsv":
        return _ibit_btc_holdings()
    if src_type == "stooq":
        return _stooq_fetch(src_arg, start)
    return fred_client.get_series(src_arg, start_date=start, end_date=end)


def backfill_macro_all(years: int = 10) -> dict[str, int]:
    """Fetch up to `years` of history for every indicator.

    Returns {series_id: rows_inserted}. Idempotent — existing rows are skipped
    via INSERT OR IGNORE in the DB layer.
    """
    _seed_metadata()
    start = (date.today() - timedelta(days=int(years * 365.25))).isoformat()
    results: dict[str, int] = {}
    for entry in MACRO_SERIES:
        sid, name = entry[0], entry[1]
        try:
            rows = _fetch_series(entry, start)
            n = db.insert_macro_observations(sid, rows)
            results[sid] = n
            logger.info("Macro backfill %s (%s): %d new rows", sid, name, n)
        except Exception as exc:
            logger.warning("Macro backfill %s failed: %s", sid, exc)
            results[sid] = 0
    return results


def collect_macro_latest() -> dict[str, int]:
    """Daily delta pull: fetch observations newer than what's in the DB."""
    _seed_metadata()
    today = date.today().isoformat()
    results: dict[str, int] = {}
    for entry in MACRO_SERIES:
        sid, name = entry[0], entry[1]
        latest = db.get_latest_macro_value(sid)
        start = latest["ts"] if latest else (date.today() - timedelta(days=365)).isoformat()
        try:
            rows = _fetch_series(entry, start, today)
            n = db.insert_macro_observations(sid, rows)
            results[sid] = n
            if n:
                logger.info("Macro update %s (%s): %d new rows", sid, name, n)
        except Exception as exc:
            logger.warning("Macro update %s failed: %s", sid, exc)
            results[sid] = 0
    return results


def has_minimum_history(min_per_series: int = 30) -> bool:
    """True if the daily benchmark series (DGS10) has enough rows to imply a
    prior full backfill ran. Monthly series in the catalog only accumulate ~120
    rows over 10 years, so per-series thresholds don't generalize; gating on the
    most-populated daily series is a good proxy.
    """
    return db.count_macro_observations("DGS10") >= min_per_series


# ── Computed series (derived at read time, not stored) ────────────────────────

def get_computed_series(name: str, days: Optional[int] = None) -> list[dict]:
    """Resolve derived indicators by combining stored series at read time."""
    name = name.lower()
    if name == "2s10s":
        return _spread("DGS10", "DGS2", days)
    if name == "5s30s":
        return _spread("DGS30", "DGS5", days)
    if name == "sofr_iorb":
        return _spread("SOFR", "IORB", days)
    if name == "reserves_gdp":
        return _reserves_over_denom("GDP", days)
    if name == "ample_reserves":
        return _ample_reserves(days)
    if name == "cpi_yoy":
        return _yoy_change("CPIAUCNS", days)
    if name == "pce_yoy":
        return _yoy_change("PCEPILFE", days)
    if name == "core_cpi_yoy":
        return _yoy_change("CPILFENS", days)
    if name == "walcl_13w":
        return _trailing_change("WALCL", days, target=91, lo=84, hi=98, annualize=True)
    if name == "gld_flow_4w":
        return _trailing_change("GLD_TONNES", days, target=28, lo=24, hi=35)
    if name == "ibit_flow_4w":
        return _trailing_change("IBIT_BTC", days, target=28, lo=24, hi=35)
    if name == "rstar_market":
        return _rstar_market(days)
    if name == "policy_gap":
        return _policy_gap(days)
    if name == "rv10y_20d":
        return _rv10y_20d(days)
    if name == "walcl_accel":
        return _walcl_accel(days)
    if name in ("true_ie_gross", "true_ie_net", "interest_pct_rev", "avg_debt_rate"):
        return _fiscal_series(name, days)
    raise ValueError(f"Unknown computed series: {name}")


# Gromen "true interest expense" components (interest + entitlement benefits)
_TRUE_IE_COMPONENTS = ("A091RC1Q027SBEA", "W823RC1Q027SBEA", "W824RC1Q027SBEA",
                       "W729RC1Q027SBEA", "W826RC1Q027SBEA")


def _fiscal_series(name: str, days: Optional[int]) -> list[dict]:
    """Fiscal ratios on the quarterly NIPA axis.

    - true_ie_gross:   (interest + SS + Medicare + Medicaid + VA) / tax receipts, %
                       — Gromen's headline construction: entitlement outlays in,
                       the payroll taxes that fund them excluded from receipts.
    - true_ie_net:     same numerator minus payroll contributions, / tax receipts
                       — the symmetric version that survives the accounting critique.
    - interest_pct_rev: bond interest alone / tax receipts, %
    - avg_debt_rate:   interest expense / gross federal debt, % — the implied
                       average coupon; its gap to the 10Y is the rollover ratchet.
    """
    tax = db.get_macro_series("W006RC1Q027SBEA")
    comps = {sid: {r["ts"]: r["value"] for r in db.get_macro_series(sid)}
             for sid in _TRUE_IE_COMPONENTS}
    payroll = {r["ts"]: r["value"] for r in db.get_macro_series("W780RC1Q027SBEA")}
    debt = {r["ts"]: r["value"] for r in db.get_macro_series("GFDEBTN")}
    out: list[dict] = []
    for r in tax:
        ts, rev = r["ts"], r["value"]
        if not rev:
            continue
        if name == "interest_pct_rev":
            v = comps["A091RC1Q027SBEA"].get(ts)
            val = v / rev * 100.0 if v is not None else None
        elif name == "avg_debt_rate":
            v, d = comps["A091RC1Q027SBEA"].get(ts), debt.get(ts)
            val = v / (d / 1000.0) * 100.0 if v is not None and d else None
        else:
            vals = [comps[sid].get(ts) for sid in _TRUE_IE_COMPONENTS]
            if any(v is None for v in vals):
                continue
            total = sum(vals)
            if name == "true_ie_net":
                p = payroll.get(ts)
                if p is None:
                    continue
                total -= p
            val = total / rev * 100.0
        if val is not None:
            out.append({"ts": ts, "value": round(val, 1)})
    if days is not None and out:
        from datetime import date as _date
        cutoff = (_date.today() - timedelta(days=int(days))).isoformat()
        out = [o for o in out if o["ts"] >= cutoff]
    return out


def _rv10y_20d(days: Optional[int]) -> list[dict]:
    """Realized volatility of the 10Y yield: rolling 20-obs stdev of daily
    yield changes in bp, annualized (×√252) to bp/yr so it plots on the same
    axis as MOVE. The MOVE − realized gap is the rates VRP: persistently
    negative (realized above implied) is the "MOVE is suppressed" narrative
    showing up as evidence.
    """
    import math
    import statistics
    rows = db.get_macro_series("DGS10")
    if len(rows) < 25:
        return []
    diffs = [(rows[i]["ts"], (rows[i]["value"] - rows[i - 1]["value"]) * 100.0)
             for i in range(1, len(rows))]
    out: list[dict] = []
    for i in range(20, len(diffs) + 1):
        window = [d[1] for d in diffs[i - 20:i]]
        out.append({"ts": diffs[i - 1][0],
                    "value": round(statistics.pstdev(window) * math.sqrt(252), 1)})
    if days is not None and out:
        from datetime import date as _date
        cutoff = (_date.today() - timedelta(days=int(days))).isoformat()
        out = [o for o in out if o["ts"] >= cutoff]
    return out


def _walcl_accel(days: Optional[int]) -> list[dict]:
    """Acceleration of the Fed balance sheet: 13-week annualized growth now
    minus the same measure ~13 weeks ago, in percentage points. Positive and
    rising = the throttle hand is actively opening, not just open.
    """
    base = _trailing_change("WALCL", None, target=91, lo=84, hi=98, annualize=True)
    from datetime import date as _date
    out: list[dict] = []
    for i, r in enumerate(base):
        cur_dt = _date.fromisoformat(r["ts"])
        prior = None
        best_err = None
        for j in range(i - 1, -1, -1):
            gap = (cur_dt - _date.fromisoformat(base[j]["ts"])).days
            if gap < 84:
                continue
            if gap > 98:
                break
            err = abs(gap - 91)
            if best_err is None or err < best_err:
                best_err, prior = err, base[j]
        if prior is None:
            continue
        out.append({"ts": r["ts"], "value": round(r["value"] - prior["value"], 2)})
    if days is not None and out:
        cutoff = (_date.today() - timedelta(days=int(days))).isoformat()
        out = [o for o in out if o["ts"] >= cutoff]
    return out


# ── Velocity board — how fast is each instrument moving vs its own history ────

# (series_id, display name, change units: 'bp' = absolute yield/spread change,
#  '%' = percent price change)
_VELOCITY_INSTRUMENTS: list[tuple[str, str, str]] = [
    ("DGS10",        "10Y UST",            "bp"),
    ("2s10s",        "2s10s Spread",       "bp"),
    ("DFII10",       "Real 10Y (TIPS)",    "bp"),
    ("T5YIFR",       "5y5y Fwd Inflation", "bp"),
    ("BAMLH0A0HYM2", "HY Credit Spread",   "bp"),
    ("sofr_iorb",    "SOFR − IORB",        "bp"),
    ("DXY",          "Dollar (DXY)",       "%"),
    ("GOLD",         "Gold",               "%"),
    ("BTC-USD",      "Bitcoin",            "%"),
]


def get_velocity_board() -> list[dict]:
    """1-week and 1-month changes for key daily instruments, z-scored against
    ~5 years of that instrument's own same-length changes.

    Windows are in trading-day observations (5 and 21). Scale is robust
    (median / 1.4826×MAD, not stdev) so one violent quarter doesn't stretch
    the yardstick exactly when it needs to stay rigid.
    """
    import statistics
    out: list[dict] = []
    for sid, name, unit in _VELOCITY_INSTRUMENTS:
        if sid in ("2s10s", "sofr_iorb"):
            rows = get_computed_series(sid, days=None)
        else:
            rows = db.get_macro_series(sid)
        rows = rows[-1300:]  # ~5y of trading days
        vals = [r["value"] for r in rows]
        if len(vals) < 200:
            continue
        entry: dict = {"sid": sid, "name": name, "unit": unit, "asof": rows[-1]["ts"]}
        for label, k in (("1w", 5), ("1m", 21)):
            if unit == "%":
                chgs = [(vals[i] / vals[i - k] - 1.0) * 100.0
                        for i in range(k, len(vals)) if vals[i - k]]
            else:
                # 2s10s / sofr_iorb are already stored in bp; yields are in %
                scale = 1.0 if sid in ("2s10s", "sofr_iorb") else 100.0
                chgs = [(vals[i] - vals[i - k]) * scale for i in range(k, len(vals))]
            if len(chgs) < 100:
                continue
            cur = chgs[-1]
            med = statistics.median(chgs)
            mad = statistics.median(abs(c - med) for c in chgs) * 1.4826
            entry[f"chg_{label}"] = round(cur, 2)
            entry[f"z_{label}"] = round((cur - med) / mad, 2) if mad > 1e-9 else 0.0
        out.append(entry)
    return out


def _rstar_market(days: Optional[int]) -> list[dict]:
    """Market-implied long-run real rate: 5y5y forward nominal minus 5y5y
    forward inflation, daily.

    5y5y nominal forward ≈ 2×10Y − 5Y (par-yield approximation); inflation leg
    is T5YIFR. Contains term premium, so it sits structurally above model r* —
    read its *changes* and its spread vs HLW, not its level as "the" r*.
    """
    y10 = {r["ts"]: r["value"] for r in db.get_macro_series("DGS10", days)}
    y5 = {r["ts"]: r["value"] for r in db.get_macro_series("DGS5", days)}
    out: list[dict] = []
    for r in db.get_macro_series("T5YIFR", days):
        ts = r["ts"]
        if ts in y10 and ts in y5:
            fwd = 2.0 * y10[ts] - y5[ts]
            out.append({"ts": ts, "value": round(fwd - r["value"], 2)})
    return out


def _policy_gap(days: Optional[int]) -> list[dict]:
    """Policy stance vs the neutral rate: (fed funds − core PCE YoY) − HLW r*.

    Positive = restrictive, negative = accommodative. Monthly core PCE YoY and
    quarterly r* are forward-filled onto the daily fed-funds axis, so the line
    steps at data releases; that is the honest cadence of the inputs.
    """
    from datetime import date as _date
    infl = _yoy_change("PCEPILFE", None)
    rstar = db.get_macro_series("RSTAR_HLW")
    if not infl or not rstar:
        return []
    out: list[dict] = []
    ii = ri = 0
    last_infl = last_rstar = None
    for r in db.get_macro_series("DFF", days):
        ts = r["ts"]
        while ii < len(infl) and infl[ii]["ts"] <= ts:
            last_infl = infl[ii]["value"]; ii += 1
        while ri < len(rstar) and rstar[ri]["ts"] <= ts:
            last_rstar = rstar[ri]["value"]; ri += 1
        if last_infl is None or last_rstar is None:
            continue
        out.append({"ts": ts, "value": round(r["value"] - last_infl - last_rstar, 2)})
    return out


def _trailing_change(series_id: str, days: Optional[int], target: int,
                     lo: int, hi: int, annualize: bool = False) -> list[dict]:
    """Trailing percent change over ~`target` days (prior obs closest to target
    within [lo, hi] days back), optionally annualized. Powers the Debasement
    Watch series: WALCL 13-week annualized growth and 4-week ETF share flows.
    """
    rows = db.get_macro_series(series_id)
    if len(rows) < 2:
        return []
    from datetime import date as _date
    out: list[dict] = []
    for i, r in enumerate(rows):
        cur_dt = _date.fromisoformat(r["ts"])
        prior = None
        best_err = None
        gap_used = None
        for j in range(i - 1, -1, -1):
            gap = (cur_dt - _date.fromisoformat(rows[j]["ts"])).days
            if gap < lo:
                continue
            if gap > hi:
                break
            err = abs(gap - target)
            if best_err is None or err < best_err:
                best_err, prior, gap_used = err, rows[j], gap
        if not prior or prior["value"] == 0:
            continue
        chg = (r["value"] / prior["value"] - 1.0) * 100.0
        if annualize:
            chg *= 365.0 / gap_used
        out.append({"ts": r["ts"], "value": round(chg, 2)})
    if days is not None and out:
        cutoff = (_date.today() - timedelta(days=int(days))).isoformat()
        out = [o for o in out if o["ts"] >= cutoff]
    return out


def _yoy_change(series_id: str, days: Optional[int]) -> list[dict]:
    """Year-over-year percent change for an index series (CPI, PCE, etc).

    Standard headline-inflation formulation: (level_t / level_{t-12mo} - 1) × 100.
    For each observation we pick the prior observation closest to 365 days back,
    accepting only gaps in [350, 380] days — the indexes are monthly, so this
    locks onto the same month 12 prints ago, and skips the point entirely if
    that month is missing rather than compute an 11- or 13-month change.
    """
    rows = db.get_macro_series(series_id)
    if len(rows) < 12:
        return []
    from datetime import date as _date
    out: list[dict] = []
    for i, r in enumerate(rows):
        cur_dt = _date.fromisoformat(r["ts"])
        prior = None
        best_err = None
        for j in range(i - 1, -1, -1):
            gap = (cur_dt - _date.fromisoformat(rows[j]["ts"])).days
            if gap < 350:
                continue
            if gap > 380:
                break
            err = abs(gap - 365)
            if best_err is None or err < best_err:
                best_err, prior = err, rows[j]
        if not prior or prior["value"] == 0:
            continue
        yoy = (r["value"] / prior["value"] - 1.0) * 100.0
        out.append({"ts": r["ts"], "value": round(yoy, 2)})
    # Apply days window after computing (so we have enough lookback)
    if days is not None and out:
        cutoff = (_date.today() - timedelta(days=int(days))).isoformat()
        out = [o for o in out if o["ts"] >= cutoff]
    return out


def _spread(long_id: str, short_id: str, days: Optional[int]) -> list[dict]:
    """Return the long − short spread aligned by date."""
    long_rows = {r["ts"]: r["value"] for r in db.get_macro_series(long_id, days)}
    short_rows = db.get_macro_series(short_id, days)
    out: list[dict] = []
    for r in short_rows:
        ts = r["ts"]
        if ts in long_rows:
            out.append({"ts": ts, "value": round((long_rows[ts] - r["value"]) * 100, 1)})  # bps
    return out


def _forward_filled(rows: list[dict]) -> dict[str, float]:
    """Return a date→value dict where each key is a string date in YYYY-MM-DD."""
    return {r["ts"]: r["value"] for r in rows}


def _reserves_over_denom(denom_id: str, days: Optional[int]) -> list[dict]:
    """Bank reserves divided by some denominator (GDP or bank assets), as %.

    Anchored on daily RRP date axis (so the chart has daily resolution like the
    Fed-watcher tools), with weekly WRESBAL and quarterly GDP forward-filled.
    WRESBAL=$M; GDP/bank assets=$B; we normalize before dividing.
    """
    daily_axis = db.get_macro_series("RRPONTSYD", days)     # daily $B (anchor)
    reserves   = sorted(db.get_macro_series("WRESBAL"), key=lambda r: r["ts"])
    denom_rows = sorted(db.get_macro_series(denom_id), key=lambda r: r["ts"])
    if not daily_axis or not reserves or not denom_rows:
        return []
    out: list[dict] = []
    ri = di = 0
    last_res = last_denom = None
    for r in daily_axis:
        ts = r["ts"]
        while ri < len(reserves) and reserves[ri]["ts"] <= ts:
            last_res = reserves[ri]["value"]
            ri += 1
        while di < len(denom_rows) and denom_rows[di]["ts"] <= ts:
            last_denom = denom_rows[di]["value"]
            di += 1
        if last_res is None or last_denom is None or last_denom == 0:
            continue
        ratio = (last_res / 1000.0) / last_denom * 100.0
        out.append({"ts": ts, "value": round(ratio, 2)})
    return out


def _ample_reserves(days: Optional[int]) -> list[dict]:
    """(Bank Reserves + ON RRP) / Total Commercial Bank Assets, as percent.

    Anchored on daily RRP so the line shows day-to-day variation (matching
    public Fed-watcher tools). WRESBAL (weekly Wed) and TLAACBW027SBOG
    (weekly) are forward-filled onto the daily RRP axis.

    Units: WRESBAL=$M, RRPONTSYD=$B, TLAACBW027SBOG=$B — normalized to $B.
    Sep 2019 repo spike happened around 8%; Fed considers >~12% "ample".
    """
    rrp        = db.get_macro_series("RRPONTSYD", days)     # daily $B (anchor)
    reserves   = sorted(db.get_macro_series("WRESBAL"), key=lambda r: r["ts"])  # $M weekly
    bank_rows  = sorted(db.get_macro_series("TLAACBW027SBOG"), key=lambda r: r["ts"])  # $B weekly
    if not rrp or not reserves or not bank_rows:
        return []
    out: list[dict] = []
    ri = bi = 0
    last_res = last_bank = None
    for r in rrp:
        ts = r["ts"]
        while ri < len(reserves) and reserves[ri]["ts"] <= ts:
            last_res = reserves[ri]["value"]
            ri += 1
        while bi < len(bank_rows) and bank_rows[bi]["ts"] <= ts:
            last_bank = bank_rows[bi]["value"]
            bi += 1
        if last_res is None or last_bank is None or last_bank == 0:
            continue
        reserves_b = last_res / 1000.0  # $M → $B
        ratio = (reserves_b + r["value"]) / last_bank * 100.0
        out.append({"ts": ts, "value": round(ratio, 2)})
    return out
