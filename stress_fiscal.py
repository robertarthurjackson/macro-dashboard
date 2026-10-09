"""Sovereign fiscal stress panel -> site/data/stress/fiscal.json

Per country (US, CA, DE, FR, IT, JP, UK):
  - debt/GDP, deficit/GDP          IMF WEO via DataMapper API (live)
  - net interest % GDP / % revenue  IMF Fiscal Monitor (primary - overall balance)
  - 10y yield latest + 1y daily     official daily sources (UST/FRED, BoC Valet,
                                    Bundesbank, MOF, BoE), CNBC fallback (FR/IT
                                    and any official failure), FRED monthly last
  - 10y spread to benchmark         Bund for DE/FR/IT, UST for US/CA/JP/UK
  - avg maturity of debt            manual table (debt-office publications)
  - gross issuance programme progress  manual table + time-elapsed pace
  - sovereign ratings               manual table

Every figure carries "as_of" and "source"; any failing source degrades to
seed / fallback / null and is reported in "sources" — never raises.
"""
import io
import logging
from datetime import date, datetime, timedelta

import pandas as pd

from stress_common import (dump, fred_series, http_get, load_seed, now_iso,
                           save_seed, source)

logger = logging.getLogger("stress.fiscal")

COUNTRIES = {
    "US": {"name": "United States", "imf": "USA", "bench": "US"},
    "CA": {"name": "Canada", "imf": "CAN", "bench": "US"},
    "DE": {"name": "Germany", "imf": "DEU", "bench": "DE"},
    "FR": {"name": "France", "imf": "FRA", "bench": "DE"},
    "IT": {"name": "Italy", "imf": "ITA", "bench": "DE"},
    "JP": {"name": "Japan", "imf": "JPN", "bench": "US"},
    "UK": {"name": "United Kingdom", "imf": "GBR", "bench": "US"},
}
BENCH_LABEL = {"US": "UST 10y", "DE": "Bund 10y"}

# IMF DataMapper's Akamai front blocks browser-like UAs; a plain client UA works.
IMF_HEADERS = {"User-Agent": "python-requests/2.32", "Accept": "application/json"}
IMF_BASE = "https://www.imf.org/external/datamapper/api/v1"
IMF_IND = {
    "debt": "GGXWDG_NGDP",            # WEO gross debt % GDP
    "balance": "GGXCNL_NGDP",         # WEO net lending/borrowing % GDP
    "fm_balance": "GGXCNL_G01_GDP_PT",  # Fiscal Monitor overall balance
    "fm_primary": "GGXONLB_G01_GDP_PT",  # Fiscal Monitor primary balance
    "revenue": "GGR_G01_GDP_PT",      # Fiscal Monitor revenue % GDP
}

# --------------------------------------------------------------------------
# Manual tables (no clean free API). Update the as_of when you touch a row.
# --------------------------------------------------------------------------
RATINGS_AS_OF = "2026-10-09"
RATINGS = {
    # country: {agency: (rating, outlook, date of last action)}
    "US": {"sp": ("AA+", "stable", "2023-08-01"), "moodys": ("Aa1", "stable", "2025-05-16"),
           "fitch": ("AA+", "stable", "2023-08-01")},
    "CA": {"sp": ("AAA", "stable", "2024-04-01"), "moodys": ("Aaa", "stable", "2024-04-01"),
           "fitch": ("AA+", "stable", "2020-06-24")},
    "DE": {"sp": ("AAA", "stable", "2024-04-01"), "moodys": ("Aaa", "stable", "2024-04-01"),
           "fitch": ("AAA", "stable", "2024-04-01")},
    "FR": {"sp": ("A+", "stable", "2025-10-17"), "moodys": ("Aa3", "negative", "2025-10-24"),
           "fitch": ("A+", "stable", "2025-09-12")},
    "IT": {"sp": ("BBB+", "stable", "2025-04-11"), "moodys": ("Baa2", "stable", "2025-11-21"),
           "fitch": ("BBB+", "stable", "2025-09-19")},
    "JP": {"sp": ("A+", "stable", "2015-09-16"), "moodys": ("A1", "stable", "2014-12-01"),
           "fitch": ("A", "stable", "2020-07-28")},
    "UK": {"sp": ("AA", "stable", "2023-04-21"), "moodys": ("Aa3", "stable", "2023-10-20"),
           "fitch": ("AA-", "stable", "2022-10-18")},
}

# Average maturity of marketable/negotiable central-government debt (years).
AVG_MATURITY = {
    "US": (5.9, "2026-06-30", "US Treasury quarterly refunding charts (~71 months, marketable debt)"),
    "CA": (6.6, "2026-03-31", "Dept of Finance Debt Management Report (avg term of market debt)"),
    "DE": (7.2, "2025-12-31", "Finanzagentur (avg residual maturity, federal securities)"),
    "FR": (8.5, "2025-12-31", "AFT monthly bulletin (avg maturity of negotiable debt)"),
    "IT": (7.0, "2025-12-31", "MEF Public Debt Management Guidelines (avg life of govt securities)"),
    "JP": (9.2, "2026-03-31", "MOF Debt Management Report (avg remaining maturity of JGBs)"),
    "UK": (13.8, "2026-03-31", "UK DMO quarterly review (avg maturity of conventional+IL gilts)"),
}

# Planned gross issuance programmes. completed/completed_as_of: latest known
# cumulative issuance from the debt office (None = not tracked yet).
ISSUANCE = {
    "US": {"program": None, "note": "No fixed annual programme; Treasury sizes auctions "
           "quarterly via refunding. See Auctions panel."},
    "CA": {"program": "Debt Management Strategy 2026-27 bond programme", "unit": "CAD bn",
           "planned": None, "start": "2026-04-01", "end": "2027-03-31"},
    "DE": {"program": "Finanzagentur Emissionsplanung 2026 (Bund securities, capital market)",
           "unit": "EUR bn", "planned": None, "start": "2026-01-01", "end": "2026-12-31"},
    "FR": {"program": "AFT 2026 medium/long-term financing programme", "unit": "EUR bn",
           "planned": None, "start": "2026-01-01", "end": "2026-12-31"},
    "IT": {"program": "MEF Linee Guida 2026 (medium/long-term gross issuance, indicative)",
           "unit": "EUR bn", "planned": None, "start": "2026-01-01", "end": "2026-12-31"},
    "JP": {"program": "MOF FY2026 JGB issuance plan (market issuance, calendar-based)",
           "unit": "JPY tn", "planned": None, "start": "2026-04-01", "end": "2027-03-31"},
    "UK": {"program": "DMO 2026-27 gilt remit", "unit": "GBP bn",
           "planned": None, "start": "2026-04-01", "end": "2027-03-31"},
}
ISSUANCE_AS_OF = "2026-10-09"


# --------------------------------------------------------------------------
# IMF
# --------------------------------------------------------------------------
def _imf_vintages() -> dict:
    r = http_get(f"{IMF_BASE}/indicators", headers=IMF_HEADERS)
    ind = r.json().get("indicators", {})
    return {k: (ind.get(v) or {}).get("source") for k, v in IMF_IND.items()}


def _vintage_month(label: str | None) -> str | None:
    """'World Economic Outlook (April 2026)' -> '2026-04'."""
    if not label or "(" not in label:
        return None
    try:
        return datetime.strptime(label.split("(")[-1].rstrip(")").strip(), "%B %Y").strftime("%Y-%m")
    except ValueError:
        return None


def fetch_imf() -> tuple[dict, list]:
    srcs, out = [], {}
    codes = "/".join(c["imf"] for c in COUNTRIES.values())
    try:
        vint = _imf_vintages()
    except Exception as e:  # noqa: BLE001
        logger.warning("IMF indicators metadata: %s", e)
        vint = {}
    for key, ind in IMF_IND.items():
        try:
            r = http_get(f"{IMF_BASE}/{ind}/{codes}", headers=IMF_HEADERS)
            vals = r.json()["values"][ind]
            out[key] = {"vintage": vint.get(key),
                        "data": {c: vals.get(m["imf"], {}) for c, m in COUNTRIES.items()}}
            srcs.append(source(f"IMF DataMapper {ind}", "ok", _vintage_month(vint.get(key)),
                               vint.get(key) or ""))
        except Exception as e:  # noqa: BLE001
            logger.warning("IMF %s failed: %s", ind, e)
            srcs.append(source(f"IMF DataMapper {ind}", "error", None, str(e)[:200]))
    seed = load_seed("fiscal_imf.json", {}) or {}
    if out:
        merged = {**seed.get("indicators", {}), **out}
        save_seed("fiscal_imf.json", {"fetched_at": now_iso(), "indicators": merged})
        out = merged
    elif seed.get("indicators"):
        out = seed["indicators"]
        srcs.append(source("IMF DataMapper (seed fallback)", "ok", seed.get("fetched_at", "")[:10],
                           "live fetch failed; using last good seed"))
    return out, srcs


def _imf_point(imf: dict, key: str, cc: str, year: int, transform=lambda v: v) -> dict:
    blk = imf.get(key)
    if not blk:
        return {"value": None, "as_of": None, "source": f"IMF {IMF_IND[key]} (unavailable)"}
    series = blk["data"].get(cc) or {}
    v = series.get(str(year))
    prev = series.get(str(year - 1))
    vint = blk.get("vintage") or "IMF"
    return {
        "value": None if v is None else round(transform(v), 2),
        "prior_year": None if prev is None else round(transform(prev), 2),
        "year": year,
        "estimate": True,  # current-year WEO/FM figures are projections
        "as_of": _vintage_month(vint),
        "source": f"IMF {IMF_IND[key]} — {vint}",
    }


def _net_interest(imf: dict, cc: str, year: int) -> tuple[dict, dict]:
    """Net interest = primary balance - overall balance (both Fiscal Monitor)."""
    try:
        p = imf["fm_primary"]["data"][cc]
        b = imf["fm_balance"]["data"][cc]
        rev = (imf.get("revenue") or {}).get("data", {}).get(cc, {})
        vint = imf["fm_primary"].get("vintage") or "IMF Fiscal Monitor"

        def at(y):
            if str(y) in p and str(y) in b:
                return p[str(y)] - b[str(y)]
            return None
        ni, ni_prev = at(year), at(year - 1)
        pct_gdp = {"value": None if ni is None else round(ni, 2),
                   "prior_year": None if ni_prev is None else round(ni_prev, 2),
                   "year": year, "estimate": True, "as_of": _vintage_month(vint),
                   "source": f"IMF Fiscal Monitor primary minus overall balance — {vint}",
                   "note": "Net interest (interest paid less interest received)"}
        r = rev.get(str(year))
        rp = rev.get(str(year - 1))
        pct_rev = {"value": round(100 * ni / r, 1) if ni is not None and r else None,
                   "prior_year": round(100 * ni_prev / rp, 1) if ni_prev is not None and rp else None,
                   "year": year, "estimate": True, "as_of": _vintage_month(vint),
                   "source": f"IMF Fiscal Monitor net interest / revenue (GGR) — {vint}"}
        return pct_gdp, pct_rev
    except Exception as e:  # noqa: BLE001
        na = {"value": None, "as_of": None, "source": f"IMF Fiscal Monitor (unavailable: {e})"}
        return na, dict(na)


# --------------------------------------------------------------------------
# OECD Economic Outlook: gross interest payments / GDP and / total receipts
# --------------------------------------------------------------------------
OECD_EO = ("https://sdmx.oecd.org/public/rest/data/OECD.ECO.MAD,DSD_EO@DF_EO,/"
           "USA+CAN+DEU+FRA+ITA+JPN+GBR.GGINTP+GDP+YRGT.A")


def fetch_oecd_interest() -> tuple[dict, list]:
    """{cc: {year: {"gdp": pct, "rev": pct}}, "_vintage": label}"""
    name = "OECD Economic Outlook (GGINTP, YRGT, GDP)"
    try:
        r = http_get(OECD_EO, timeout=60, params={"startPeriod": date.today().year - 3,
                                                  "format": "csvfilewithlabels"})
        df = pd.read_csv(io.StringIO(r.text))
        vintage = str(df["STRUCTURE_NAME"].iloc[0])
        piv = df.pivot_table(index=["REF_AREA", "TIME_PERIOD"], columns="MEASURE",
                             values="OBS_VALUE", aggfunc="last")
        imf2cc = {m["imf"]: cc for cc, m in COUNTRIES.items()}
        out = {"_vintage": vintage}
        for (area, yr), row in piv.iterrows():
            cc = imf2cc.get(area)
            if cc and pd.notna(row.get("GGINTP")):
                out.setdefault(cc, {})[str(yr)] = {
                    "gdp": round(100 * row["GGINTP"] / row["GDP"], 2) if pd.notna(row.get("GDP")) else None,
                    "rev": round(100 * row["GGINTP"] / row["YRGT"], 1) if pd.notna(row.get("YRGT")) else None}
        save_seed("fiscal_oecd.json", {"fetched_at": now_iso(), "data": out})
        return out, [source(name, "ok", None, vintage)]
    except Exception as e:  # noqa: BLE001
        logger.warning("%s: %s", name, e)
        srcs = [source(name, "error", None, str(e)[:200])]
        seed = load_seed("fiscal_oecd.json")
        if seed and seed.get("data"):
            srcs.append(source("OECD EO (seed fallback)", "ok", seed["fetched_at"][:10], "last good seed"))
            return seed["data"], srcs
        return {}, srcs


def _gross_interest(oecd: dict, cc: str, year: int) -> tuple[dict, dict]:
    rows = oecd.get(cc) or {}
    vint = oecd.get("_vintage", "OECD Economic Outlook")
    cur, prev = rows.get(str(year)) or {}, rows.get(str(year - 1)) or {}
    mk = lambda k, label: {
        "value": cur.get(k), "prior_year": prev.get(k), "year": year, "estimate": True,
        "as_of": vint, "source": f"OECD {vint}: gross GG interest payments / {label}"}
    if not cur:
        na = {"value": None, "as_of": None, "source": "OECD EO unavailable"}
        return na, dict(na)
    return mk("gdp", "nominal GDP"), mk("rev", "total GG receipts")


# --------------------------------------------------------------------------
# 10y yields (daily)
# --------------------------------------------------------------------------
def _y_us(start: str) -> pd.Series:
    return fred_series("DGS10", start)


def _y_ca(start: str) -> pd.Series:
    r = http_get("https://www.bankofcanada.ca/valet/observations/BD.CDN.10YR.DQ.YLD/json",
                 params={"start_date": start})
    obs = r.json()["observations"]
    s = pd.Series({o["d"]: (o.get("BD.CDN.10YR.DQ.YLD") or {}).get("v") for o in obs})
    return s


def _y_de(start: str) -> pd.Series:
    r = http_get("https://api.statistiken.bundesbank.de/rest/data/BBSIS/"
                 "D.I.ZST.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A",
                 params={"format": "csv", "startPeriod": start, "lang": "en"})
    rows = {}
    for line in r.text.splitlines():
        parts = line.split(",")
        if len(parts) >= 2 and len(parts[0]) == 10 and parts[0][4] == "-":
            rows[parts[0]] = parts[1]
    return pd.Series(rows)


def _y_jp(start: str) -> pd.Series:
    base = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/"
    frames = []
    for url in (base + "historical/jgbcme_all.csv", base + "jgbcme.csv"):
        try:
            txt = http_get(url, timeout=60).text
            df = pd.read_csv(io.StringIO(txt), skiprows=1)
            frames.append(pd.Series(df["10Y"].values, index=df["Date"].values))
        except Exception as e:  # noqa: BLE001
            logger.warning("MOF %s: %s", url, e)
    if not frames:
        raise RuntimeError("MOF JGB CSVs unavailable")
    s = pd.concat(frames)
    s.index = pd.to_datetime(s.index, format="%Y/%m/%d", errors="coerce")
    s = s[s.index.notna()]
    return s[s.index >= pd.Timestamp(start)]


def _y_uk(start: str) -> pd.Series:
    d0 = datetime.strptime(start, "%Y-%m-%d").strftime("%d/%b/%Y")
    r = http_get("https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp",
                 params={"csv.x": "yes", "Datefrom": d0, "Dateto": "now",
                         "SeriesCodes": "IUDMNPY", "CSVF": "TN", "UsingCodes": "Y",
                         "VPD": "Y", "VFD": "N"})
    df = pd.read_csv(io.StringIO(r.text))
    s = pd.Series(df.iloc[:, 1].values, index=pd.to_datetime(df.iloc[:, 0], format="%d %b %Y").values)
    return s


CNBC_SYM = {"US": "US10Y", "CA": "CA10Y-CA", "DE": "DE10Y-DE", "FR": "FR10Y-FR",
            "IT": "IT10Y-IT", "JP": "JP10Y-JP", "UK": "GB10Y-GB"}


def _y_cnbc(cc: str, start: str) -> pd.Series:
    r = http_get("https://ts-api.cnbc.com/harmony/app/charts/1Y.json",
                 params={"symbol": CNBC_SYM[cc]})
    bars = r.json()["barData"]["priceBars"]
    s = pd.Series({b["tradeTime"][:8]: b["close"] for b in bars})
    s.index = pd.to_datetime(s.index, format="%Y%m%d")
    return s[s.index >= pd.Timestamp(start)]


FRED_MONTHLY = {"US": "IRLTLT01USM156N", "CA": "IRLTLT01CAM156N", "DE": "IRLTLT01DEM156N",
                "FR": "IRLTLT01FRM156N", "IT": "IRLTLT01ITM156N", "JP": "IRLTLT01JPM156N",
                "UK": "IRLTLT01GBM156N"}
# Euro trio (DE/FR/IT) all use CNBC first so Bund spreads compare like with like
# (no free official daily OAT/BTP feed); Bundesbank is the DE fallback.
OFFICIAL = {
    "US": (_y_us, "FRED DGS10 (US Treasury constant maturity)"),
    "CA": (_y_ca, "Bank of Canada Valet BD.CDN.10YR.DQ.YLD (benchmark)"),
    "JP": (_y_jp, "Japan MOF JGB benchmark curve 10Y"),
    "UK": (_y_uk, "Bank of England IUDMNPY (10y nominal par yield)"),
}


def _clean(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(pd.Series(s).replace(".", None), errors="coerce").dropna()
    s.index = pd.to_datetime(s.index)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s[s.index.dayofweek < 5]


def fetch_yields() -> tuple[dict, list]:
    """Returns {cc: (series, source_label, daily: bool)}."""
    start = (date.today() - timedelta(days=380)).isoformat()
    out, srcs = {}, []
    for cc in COUNTRIES:
        attempts = []
        if cc in OFFICIAL:
            fn, label = OFFICIAL[cc]
            attempts.append((lambda fn=fn: fn(start), label, True))
        attempts.append((lambda cc=cc: _y_cnbc(cc, start), f"CNBC {CNBC_SYM[cc]} daily close", True))
        if cc == "DE":
            attempts.append((lambda: _y_de(start), "Deutsche Bundesbank BBSIS Svensson 10y zero yield", True))
        attempts.append((lambda cc=cc: fred_series(FRED_MONTHLY[cc], start),
                         f"FRED {FRED_MONTHLY[cc]} (OECD monthly avg)", False))
        for fn, label, daily in attempts:
            try:
                s = _clean(fn())
                if len(s) < 3:
                    raise ValueError(f"only {len(s)} obs")
                out[cc] = (s, label, daily)
                srcs.append(source(f"{cc} 10y: {label}", "ok", s.index[-1].date().isoformat()))
                break
            except Exception as e:  # noqa: BLE001
                logger.warning("%s 10y via %s failed: %s", cc, label, e)
                srcs.append(source(f"{cc} 10y: {label}", "error", None, str(e)[:200]))
    return out, srcs


def _hist(s: pd.Series, nd: int = 3) -> list:
    return [[d.date().isoformat(), round(float(v), nd)] for d, v in s.items()]


def _yield_block(cc: str, yields: dict) -> dict:
    if cc not in yields:
        return {"value": None, "as_of": None, "source": "unavailable", "history": []}
    s, label, daily = yields[cc]
    one_y = s[s.index >= s.index[-1] - pd.Timedelta(days=365)]
    chg = lambda days: (round(100 * (float(s.iloc[-1]) - float(s[s.index <= s.index[-1] - pd.Timedelta(days=days)].iloc[-1])), 1)
                        if (s.index <= s.index[-1] - pd.Timedelta(days=days)).any() else None)
    return {"value": round(float(s.iloc[-1]), 3), "as_of": s.index[-1].date().isoformat(),
            "source": label, "frequency": "daily" if daily else "monthly",
            "chg_1m_bp": chg(30), "chg_1y_bp": chg(365), "history": _hist(one_y)}


def _spread_block(cc: str, yields: dict) -> dict:
    bench = COUNTRIES[cc]["bench"]
    base = {"benchmark": BENCH_LABEL[bench]}
    if cc == bench:
        return {**base, "value_bp": 0.0, "as_of": yields[cc][0].index[-1].date().isoformat()
                if cc in yields else None, "source": "self (benchmark)", "history": []}
    if cc not in yields or bench not in yields:
        return {**base, "value_bp": None, "as_of": None, "source": "unavailable", "history": []}
    a, la, da = yields[cc]
    b, lb, db = yields[bench]
    if da != db:  # mixed daily/monthly: compare on month-end
        a, b = a.resample("ME").last(), b.resample("ME").last()
    sp = (a - b).dropna() * 100
    if sp.empty:
        return {**base, "value_bp": None, "as_of": None, "source": "no overlapping dates", "history": []}
    sp = sp[sp.index >= sp.index[-1] - pd.Timedelta(days=365)]
    return {**base, "value_bp": round(float(sp.iloc[-1]), 1), "as_of": sp.index[-1].date().isoformat(),
            "source": f"{la} minus {lb}", "history": _hist(sp, 1)}


# --------------------------------------------------------------------------
# US extras (live): average interest rate on marketable debt
# --------------------------------------------------------------------------
def fetch_us_avg_rate() -> tuple[dict | None, dict]:
    name = "US Treasury FiscalData avg_interest_rates"
    try:
        r = http_get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates",
                     params={"filter": "security_desc:eq:Total Marketable", "sort": "-record_date",
                             "page[size]": 13})
        rows = r.json()["data"]
        latest = rows[0]
        yago = rows[12] if len(rows) > 12 else None
        blk = {"value": float(latest["avg_interest_rate_amt"]), "as_of": latest["record_date"],
               "year_ago": float(yago["avg_interest_rate_amt"]) if yago else None,
               "source": "US Treasury FiscalData (avg interest rate, total marketable)"}
        return blk, source(name, "ok", latest["record_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning("%s: %s", name, e)
        return None, source(name, "error", None, str(e)[:200])


# --------------------------------------------------------------------------
# Manual blocks
# --------------------------------------------------------------------------
def _ratings_block(cc: str) -> dict:
    blk = {}
    for ag, (rating, outlook, last) in RATINGS[cc].items():
        blk[ag] = {"rating": rating, "outlook": outlook, "last_action": last,
                   "as_of": RATINGS_AS_OF, "source": "manual"}
    return blk


def _maturity_block(cc: str) -> dict:
    v, as_of, where = AVG_MATURITY[cc]
    return {"value_years": v, "as_of": as_of, "source": "manual", "reference": where,
            "approximate": True}


def _issuance_block(cc: str, overrides: dict) -> dict:
    row = {**ISSUANCE[cc], **(overrides.get(cc) or {})}
    if not row.get("program"):
        return {"program": None, "completed_pct": None, "status": "n/a",
                "note": row.get("note"), "as_of": ISSUANCE_AS_OF, "source": "manual"}
    today = date.today()
    s, e = date.fromisoformat(row["start"]), date.fromisoformat(row["end"])
    elapsed = max(0.0, min(1.0, (today - s).days / max(1, (e - s).days)))
    planned, done = row.get("planned"), row.get("completed")
    pct = round(100 * done / planned, 1) if planned and done is not None else row.get("completed_pct")
    return {
        "program": row["program"], "unit": row.get("unit"),
        "period_start": row["start"], "period_end": row["end"],
        "planned": planned, "completed": done, "completed_pct": pct,
        "completed_as_of": row.get("completed_as_of"),
        "time_elapsed_pct": round(100 * elapsed, 1),
        "ahead_of_pace_pp": round(pct - 100 * elapsed, 1) if pct is not None else None,
        "status": "manual" if pct is not None else ("partial" if planned else "missing"),
        "note": row.get("note", ""),
        "as_of": row.get("as_of", ISSUANCE_AS_OF),
        "source": row.get("source", "manual"),
    }


# --------------------------------------------------------------------------
def build_fiscal() -> dict:
    sources = []
    year = date.today().year
    try:
        imf, s = fetch_imf()
        sources += s
    except Exception as e:  # noqa: BLE001
        imf = {}
        sources.append(source("IMF DataMapper", "error", None, str(e)[:200]))
    try:
        yields, s = fetch_yields()
        sources += s
    except Exception as e:  # noqa: BLE001
        yields = {}
        sources.append(source("10y yields", "error", None, str(e)[:200]))
    oecd, s = fetch_oecd_interest()
    sources += s
    us_rate, s = fetch_us_avg_rate()
    sources.append(s)
    overrides = (load_seed("fiscal_issuance.json", {}) or {}).get("countries", {})
    if overrides:
        sources.append(source("seed/stress/fiscal_issuance.json", "ok", None, "manual issuance overrides"))
    sources += [source("Sovereign ratings", "ok", RATINGS_AS_OF, "manual table in stress_fiscal.py"),
                source("Average maturity", "ok", None, "manual table in stress_fiscal.py"),
                source("Issuance programmes", "ok", ISSUANCE_AS_OF, "manual table + seed overrides")]

    countries = {}
    for cc, meta in COUNTRIES.items():
        try:
            debt = _imf_point(imf, "debt", cc, year)
            deficit = _imf_point(imf, "balance", cc, year, transform=lambda v: -v)
            deficit["note"] = "Positive = deficit (negated IMF net lending/borrowing)"
            ni_gdp, ni_rev = _net_interest(imf, cc, year) if imf else (
                {"value": None, "as_of": None, "source": "IMF unavailable"},) * 2
            gi_gdp, gi_rev = _gross_interest(oecd, cc, year)
            if gi_gdp.get("value") is None:  # fall back to IMF net interest
                gi_gdp, gi_rev = ni_gdp, ni_rev
            rec = {
                "name": meta["name"],
                "benchmark": BENCH_LABEL[meta["bench"]],
                "debt_gdp": debt,
                "deficit_gdp": deficit,
                "interest_gdp": gi_gdp,
                "interest_revenue": gi_rev,
                "interest_net_gdp": ni_gdp,
                "interest_net_revenue": ni_rev,
                "yield_10y": _yield_block(cc, yields),
                "spread_10y": _spread_block(cc, yields),
                "avg_maturity": _maturity_block(cc),
                "issuance": _issuance_block(cc, overrides),
                "ratings": _ratings_block(cc),
            }
            if cc == "US" and us_rate:
                rec["avg_interest_rate"] = us_rate
            countries[cc] = rec
        except Exception as e:  # noqa: BLE001
            logger.exception("fiscal %s failed", cc)
            countries[cc] = {"name": meta["name"], "error": str(e)[:200]}

    return {"generated_at": now_iso(), "sources": sources,
            "order": list(COUNTRIES), "year": year, "countries": countries}


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    t0 = time.time()
    payload = build_fiscal()
    dump("fiscal.json", payload)
    for cc, r in payload["countries"].items():
        y, sp = r.get("yield_10y", {}), r.get("spread_10y", {})
        print(f"{cc}: debt {r.get('debt_gdp', {}).get('value')} def {r.get('deficit_gdp', {}).get('value')} "
              f"int {r.get('interest_gdp', {}).get('value')}/{r.get('interest_revenue', {}).get('value')}%rev "
              f"10y {y.get('value')} ({y.get('as_of')}, {y.get('frequency')}, n={len(y.get('history', []))}) "
              f"spr {sp.get('value_bp')}bp")
    bad = [s for s in payload["sources"] if s["status"] == "error"]
    print(f"{len(payload['sources'])} sources, {len(bad)} errors, {time.time() - t0:.1f}s")
    for s in bad:
        print("  ERR", s["name"], s["note"][:100])
