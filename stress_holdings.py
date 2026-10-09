"""Investor base of government debt (Stress & Auctions tab): who holds the
sovereign debt of US, CA, DE, FR, IT, JP, UK, and how fast central banks
are running it off (QT).

Cadence: weekly. Raw source series are cached in seed/stress/holdings.json
({"fetched_at", "raw": {key: series}}); build_holdings() refetches only when
the seed is older than 6 days (1 day if the last refetch was partial; or
STRESS_FORCE=1), otherwise it rebuilds the payload from the seed. A source that
fails on refetch keeps its previous raw series (flagged "stale") and never raises.

Output: site/data/stress/holdings.json

Schema (all dates ISO period-end "YYYY-MM-DD"; amounts in `unit`; shares in %)
{
  "generated_at": "...Z", "fetched_at": "...Z",
  "sources": [{"name","status":"ok|error|stub|stale","as_of","note"}],
  "order": ["US","CA","DE","FR","IT","JP","UK"],
  "sectors": {"foreign": "Non-residents", "central_bank": ..., "banks": ...,
              "other_financial": "Insurers, pensions, funds & other fin.",
              "households_other": "Households & other domestic"},
  "comparison": [   # latest snapshot per country, one row each
     {"country":"US","as_of":"2026-06-30","source":"...","freq":"Q",
      "foreign":31.9,"domestic":68.1,"central_bank":15.7,"banks":7.0,
      "other_financial":...,"households_other":...,
      "foreign_latest":31.9,"foreign_latest_as_of":"...","foreign_latest_source":"...",
      "cb_qt_ann_12m":369.7,"cb_qt_pct_12m":8.8,"cb_unit":"USD bn"}],
  "countries": {
    "US": {
      "name": "United States", "currency": "USD",
      "holders": {             # sector holdings time series
         "source","as_of","freq":"Q|A","unit":"USD bn","measure": "...",
         "dates":[...],
         "levels": {"total":[...],"foreign":[...],"central_bank":[...],
                    "banks":[...],"other_financial":[...],"households_other":[...]},
         "share_pct": {"foreign":[...],"domestic":[...],"central_bank":[...], ...}},
      "latest_split": {"as_of","source","total", "unit",
         "items":[{"key":"central_bank","label":"...","value":4567.8,"share":15.7}]},
      "central_bank": {        # CB holdings of govt debt, month-end sampled
         "name":"Federal Reserve (SOMA Treasuries)","source","as_of","freq":"W",
         "unit":"USD bn","dates":[...],"values":[...],
         "qt": {"as_of","level", "chg_3m","chg_6m","chg_12m",          # unit
                "pct_3m","pct_6m","pct_12m",                             # % change
                "ann_3m","ann_6m","ann_12m"}},                           # unit / yr
      "extra": {   # optional, per country
         "foreign_monthly": {"label","source","as_of","unit","dates","values"},   # US (TIC)
         "foreign_quarterly": {"label","source","as_of","unit","dates",           # IT (Eurostat)
                               "total","foreign","foreign_pct"}},
      "notes": ["..."]
    }, ...
  }
}
Null values mean "not available". Sector categories are mapped from each
national/ECB classification (see each country's notes), so cross-country
comparisons are approximate.
"""
from __future__ import annotations

import concurrent.futures as cf
import io
import logging
import os
import time

import pandas as pd

from stress_common import (dump, fred_series, http_get, load_seed, now_iso,
                           save_seed, seed_age_days, source)

logger = logging.getLogger("stress.holdings")

SEED = "holdings.json"
OUT = "holdings.json"
START = "2005-01-01"
MAX_AGE_DAYS = 6

ORDER = ["US", "CA", "DE", "FR", "IT", "JP", "UK"]
NAMES = {"US": "United States", "CA": "Canada", "DE": "Germany", "FR": "France",
         "IT": "Italy", "JP": "Japan", "UK": "United Kingdom"}
CCY = {"US": "USD", "CA": "CAD", "DE": "EUR", "FR": "EUR", "IT": "EUR",
       "JP": "JPY", "UK": "GBP"}
SECTORS = {
    "foreign": "Non-residents",
    "central_bank": "Central bank",
    "banks": "Banks / deposit-takers",
    "other_financial": "Insurers, pensions, funds & other fin.",
    "households_other": "Households & other domestic",
}
DOMESTIC = ["central_bank", "banks", "other_financial", "households_other"]

SRC_FRED_Z1 = "Federal Reserve Z.1 Financial Accounts via FRED"
SRC_FRED_SOMA = "Federal Reserve H.4.1 via FRED (TREAST)"
SRC_TIC = "US Treasury TIC (Major Foreign Holders: mfhhis01 + slt_table5)"
SRC_BOC = "Bank of Canada Valet (G4: GoC securities distribution of holdings)"
SRC_STATCAN = "Statistics Canada WDS"
SRC_ECB_GFS = "ECB Data Portal, GFS (Maastricht debt by holder)"
SRC_ECB_BSI = "ECB Data Portal, BSI (NCB holdings of euro-area govt securities)"
SRC_BOJ_FF = "Bank of Japan Flow of Funds (stat-search API, db FF)"
SRC_BOJ_BS = "Bank of Japan Accounts (stat-search API, db BS01)"
SRC_ONS = "ONS UK Economic Accounts (AF.32N1 central govt long-term debt securities)"
SRC_BOE = "Bank of England IADB (APF gilt holdings, YWWB9T9)"


# ----------------------------------------------------------------- helpers
def _period_end(d: pd.Timestamp, freq: str) -> str:
    if freq == "Q":
        return (d + pd.offsets.QuarterEnd(0)).strftime("%Y-%m-%d")
    if freq == "M":
        return (d + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")
    if freq == "A":
        return f"{d.year}-12-31"
    return d.strftime("%Y-%m-%d")


def _raw(s: pd.Series, freq: str, unit: str, src: str, scale: float = 1.0,
         period_end: bool = True) -> dict:
    """Serialize a pandas Series into the seed's raw form."""
    s = pd.to_numeric(s, errors="coerce").dropna().sort_index()
    s = s[s.index >= pd.Timestamp(START) - pd.Timedelta(days=400)]
    if s.empty:
        raise ValueError("empty series")
    dates = [(_period_end(d, freq) if period_end else d.strftime("%Y-%m-%d"))
             for d in s.index]
    return {"freq": freq, "unit": unit, "source": src,
            "dates": dates, "values": [round(float(v) * scale, 3) for v in s.values],
            "as_of": dates[-1], "status": "ok"}


def _ser(raw: dict | None) -> pd.Series | None:
    if not raw or not raw.get("dates"):
        return None
    return pd.Series(raw["values"], index=pd.to_datetime(raw["dates"]), dtype=float)


# ---------------------------------------------------------------- fetchers
# Each fetcher returns {raw_key: raw_series_dict}. Exceptions are caught by
# the orchestrator per fetcher.

US_Z1 = {  # Z.1 Treasury securities holdings, USD millions, quarterly
    "total": ["BOGZ1FL893061105Q"],                       # all sectors (asset side)
    "foreign": ["BOGZ1FL263061110Q", "BOGZ1FL263061120Q"],  # rest of world
    "central_bank": ["BOGZ1FL713061103Q", "BOGZ1FL713061113Q"],  # monetary authority
    "banks": ["BOGZ1FL703061105Q"],                       # private depository institutions
    "ins_pens": ["BOGZ1FL543061105Q", "BOGZ1FL513061105Q",  # life, P&C insurers
                 "BOGZ1FL573061105Q", "BOGZ1FL223061143Q",  # private, S&L DB pensions
                 "BOGZ1FL343061105Q"],                    # federal retirement funds
    "funds": ["BOGZ1FL653061105Q", "BOGZ1FL633061110Q"],  # mutual funds, MMFs
}


def fetch_us() -> dict:
    ids = sorted({i for v in US_Z1.values() for i in v})
    with cf.ThreadPoolExecutor(6) as ex:
        got = dict(zip(ids, ex.map(lambda i: fred_series(i, "2003-01-01"), ids)))
    out = {}
    for k, lst in US_Z1.items():
        s = sum(got[i] for i in lst).dropna()
        out[f"US.z1.{k}"] = _raw(s, "Q", "USD bn", SRC_FRED_Z1, 1e-3)
    return out


def fetch_us_soma() -> dict:
    s = fred_series("TREAST", "2003-01-01")
    return {"US.cb": _raw(s, "W", "USD bn", SRC_FRED_SOMA, 1e-3, period_end=False)}


def fetch_us_tic() -> dict:
    import tic_client
    rows = tic_client.get_foreign_ust_holdings(start_date=START)   # history (mfhhis01)
    pts = {pd.Timestamp(d): v for d, v in rows}
    try:  # latest 13 months (slt_table5) override/extend the history file
        txt = http_get("https://ticdata.treasury.gov/Publish/slt_table5.txt").text
        hdr = tot = None
        for line in txt.splitlines():
            parts = [x.strip() for x in line.split("\t")]
            if parts[0] == "Country":
                hdr = parts[1:]
            elif parts[0] == "Grand Total":
                tot = parts[1:]
        for d, v in zip(hdr or [], tot or []):
            if d and v:
                pts[pd.Timestamp(d + "-01")] = float(v.replace(",", ""))
    except Exception as e:  # noqa: BLE001
        logger.warning("TIC slt_table5 failed: %s", e)
    if not pts:
        raise RuntimeError("TIC returned no rows")
    s = pd.Series(pts)
    return {"US.tic": _raw(s, "M", "USD bn", SRC_TIC)}


def _valet(series: list[str]) -> pd.DataFrame:
    r = http_get(f"https://www.bankofcanada.ca/valet/observations/{','.join(series)}/json",
                 params={"start_date": "2003-01-01"}, timeout=60)
    obs = r.json()["observations"]
    rows = {o["d"]: {k: (float(o[k]["v"]) if k in o and o[k].get("v") not in (None, "") else None)
                     for k in series} for o in obs}
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def fetch_ca_boc() -> dict:
    # Weekly: BoC holdings (total, unadjusted par) and total GoC securities outstanding.
    w = _valet(["DTBOCTOTUAW", "DTTOTUAW"])
    m = _valet(["V37369", "V37340"])  # monthly versions (longer, month-end)
    return {
        "CA.cb": _raw(w["DTBOCTOTUAW"], "W", "CAD bn", SRC_BOC, 1e-3, period_end=False),
        "CA.boc_m": _raw(m["V37369"], "M", "CAD bn", SRC_BOC, 1e-3),
        "CA.total_m": _raw(m["V37340"], "M", "CAD bn", SRC_BOC, 1e-3),
    }


# StatCan table 36-10-0580-01 (National Balance Sheet Accounts), market value, CAD mn,
# quarterly; asset side = holder. Each pair = [GoC short-term paper, GoC bonds].
CA_STATCAN: dict[str, list[int]] = {
    "total": [62693624, 62693629],          # total all sectors
    "foreign": [62694844, 62694849],        # non-residents
    "central_bank": [62696139, 62696144],   # monetary authorities (BoC)
    "banks": [62696244, 62696249],          # chartered banks + quasi-banks
    "ins_pens": [62696559, 62696564],       # insurance & pension funds
    "funds": [62697189, 62697194],          # mutual funds
}


def _statcan_vectors(vecs: list[int], latest_n: int = 90) -> dict[int, pd.Series]:
    import requests
    r = requests.post("https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods",
                      json=[{"vectorId": v, "latestN": latest_n} for v in vecs],
                      headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    out = {}
    for item in r.json():
        if item.get("status") != "SUCCESS":
            continue
        obj = item["object"]
        pts = {pd.Timestamp(p["refPer"]): p["value"] for p in obj["vectorDataPoint"]
               if p.get("value") is not None}
        out[int(obj["vectorId"])] = pd.Series(pts, dtype=float).sort_index()
    return out


def fetch_ca_statcan() -> dict:
    if not CA_STATCAN:
        raise RuntimeError("no StatCan vectors configured")
    vecs = sorted({v for lst in CA_STATCAN.values() for v in lst})
    got = _statcan_vectors(vecs)
    out = {}
    for k, lst in CA_STATCAN.items():
        if not all(v in got for v in lst):
            continue
        s = sum(got[v] for v in lst).dropna()
        out[f"CA.sc.{k}"] = _raw(s, "Q", "CAD bn", SRC_STATCAN, 1e-3)
    if not out:
        raise RuntimeError("StatCan returned no usable vectors")
    return out


ECB = "https://data-api.ecb.europa.eu/service/data"


def _ecb_csv(flow: str, key: str, start: str) -> pd.DataFrame:
    r = http_get(f"{ECB}/{flow}/{key}", params={"startPeriod": start, "detail": "dataonly"},
                 headers={"Accept": "text/csv"}, timeout=90)
    return pd.read_csv(io.StringIO(r.text))


GFS_MAP = {("W0", "S1"): "total", ("W1", "S1"): "foreign", ("W2", "S1"): "domestic",
           ("W2", "S121"): "central_bank", ("W2", "S12T"): "banks",
           ("W2", "S12P"): "other_financial", ("W2", "S1U"): "households_other"}


def fetch_ea_gfs() -> dict:
    key = ("A.N.DE+FR+IT.W0+W1+W2.S13.S1+S121+S12T+S12P+S1U.C.L.LE.GD.T._Z.XDC._T.F.V.N._T")
    df = _ecb_csv("GFS", key, "2000")
    out = {}
    for (cty, area, sec), g in df.groupby(["REF_AREA", "COUNTERPART_AREA", "COUNTERPART_SECTOR"]):
        k = GFS_MAP.get((area, sec))
        if not k:
            continue
        s = pd.Series(g["OBS_VALUE"].values, index=pd.to_datetime(g["TIME_PERIOD"].astype(str)))
        out[f"{cty}.gfs.{k}"] = _raw(s, "A", "EUR bn", SRC_ECB_GFS, 1e-3)
    if not out:
        raise RuntimeError("GFS returned nothing")
    return out


def fetch_ea_bsi() -> dict:
    df = _ecb_csv("BSI", "M.DE+FR+IT.N.N.A30.A.1.U2.2100.Z01.E", "2003-01")
    out = {}
    for cty, g in df.groupby("REF_AREA"):
        s = pd.Series(g["OBS_VALUE"].values, index=pd.to_datetime(g["TIME_PERIOD"]))
        out[f"{cty}.cb"] = _raw(s, "M", "EUR bn", SRC_ECB_BSI, 1e-3)
    return out


SRC_ESTAT = "Eurostat gov_10q_ggdebt (quarterly debt, domestic vs non-domestic holders)"


def fetch_ea_estat() -> dict:
    r = http_get("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10q_ggdebt",
                 params=[("geo", "DE"), ("geo", "FR"), ("geo", "IT"), ("sector", "S13"),
                         ("unit", "MIO_EUR"), ("na_item", "GD"), ("na_item", "GD_S2"),
                         ("na_item", "GD_S1"), ("sinceTimePeriod", "2003-Q1")], timeout=90)
    j = r.json()
    ids, size = j["id"], j["size"]
    cat = {d: {v: k for k, v in j["dimension"][d]["category"]["index"].items()} for d in ids}
    out_pts: dict = {}
    for flat, val in j["value"].items():
        i, coords = int(flat), {}
        for d, n in zip(reversed(ids), reversed(size)):
            coords[d] = cat[d][i % n]
            i //= n
        y, q = coords["time"].split("-Q")
        out_pts.setdefault((coords["geo"], coords["na_item"]), {})[
            pd.Timestamp(year=int(y), month=3 * int(q), day=1)] = val
    name = {"GD": "total", "GD_S2": "foreign", "GD_S1": "domestic"}
    out = {f"{g}.estat.{name[n]}": _raw(pd.Series(pts), "Q", "EUR bn", SRC_ESTAT, 1e-3)
           for (g, n), pts in out_pts.items()}
    if not out:
        raise RuntimeError("Eurostat returned nothing")
    return out


BOJ = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"


def _boj(db: str, codes: list[str], start: str) -> dict[str, list]:
    url = f"{BOJ}?format=json&lang=en&db={db}&code={','.join(codes)}&startDate={start}"
    out: dict[str, list] = {}
    pos = None
    for _ in range(10):
        r = http_get(url + (f"&startPosition={pos}" if pos else ""), timeout=60)
        j = r.json()
        if j.get("STATUS") != 200:
            raise RuntimeError(f"BoJ API status {j.get('STATUS')}: {j.get('MESSAGE')}")
        for res in j["RESULTSET"]:
            v = res["VALUES"]
            out.setdefault(res["SERIES_CODE"], []).extend(zip(v["SURVEY_DATES"], v["VALUES"]))
        pos = j.get("NEXTPOSITION")
        if not pos:
            break
    return out


JP_FF = {"total": "700", "central_bank": "110", "banks": "120", "ins_pens": "130",
         "households": "430", "foreign": "500"}


def fetch_jp_ff() -> dict:
    codes = {k: f"FOF_FFAS{s}A311" for k, s in JP_FF.items()}
    got = _boj("FF", list(codes.values()), "200301")
    out = {}
    for k, c in codes.items():
        pts = {}
        for d, v in got.get(c, []):
            if v in (None, ""):
                continue
            d = str(d)
            y, q = int(d[:4]), int(d[4:])          # quarterly: YYYYQQ (quarter number)
            pts[pd.Timestamp(year=y, month=3 * q, day=1)] = float(v)
        # 100 million yen -> trillion yen
        out[f"JP.ff.{k}"] = _raw(pd.Series(pts), "Q", "JPY tn", SRC_BOJ_FF, 1e-4)
    return out


def fetch_jp_boj() -> dict:
    got = _boj("BS01", ["MABJMA5"], "200301")
    pts = {pd.Timestamp(year=int(str(d)[:4]), month=int(str(d)[4:6]), day=1): float(v)
           for d, v in got.get("MABJMA5", []) if v not in (None, "")}
    return {"JP.cb": _raw(pd.Series(pts), "M", "JPY tn", SRC_BOJ_BS, 1e-4)}


ONS_URL = "https://www.ons.gov.uk/economy/grossdomesticproductgdp/timeseries/{c}/ukea/data"
UK_ONS = {"total": "NYXQ", "foreign": "NLDT", "mfi": "NNTV", "ins_pens": "NIZB",
          "fin_all": "NLKB", "uk_all": "NYXP"}


def fetch_uk_ons() -> dict:
    out = {}
    for k, c in UK_ONS.items():
        j = http_get(ONS_URL.format(c=c.lower()), timeout=45).json()
        pts = {}
        for q in j.get("quarters", []):
            if q.get("value") in (None, ""):
                continue
            y, qq = q["date"].split(" Q")
            pts[pd.Timestamp(year=int(y), month=3 * int(qq), day=1)] = float(q["value"])
        out[f"UK.ons.{k}"] = _raw(pd.Series(pts), "Q", "GBP bn", SRC_ONS, 1e-3)
        time.sleep(0.3)
    return out


def fetch_uk_boe() -> dict:
    url = ("https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
           "?csv.x=yes&Datefrom=01/Jan/2009&Dateto=now&CSVF=TN&UsingCodes=Y&SeriesCodes=YWWB9T9")
    r = http_get(url, timeout=60)
    df = pd.read_csv(io.StringIO(r.text))
    if "YWWB9T9" not in df.columns:
        raise RuntimeError("BoE IADB did not return YWWB9T9")
    s = pd.Series(df["YWWB9T9"].values, index=pd.to_datetime(df.iloc[:, 0], format="%d %b %Y"))
    return {"UK.cb": _raw(s, "W", "GBP bn", SRC_BOE, 1e-3, period_end=False)}


FETCHERS = {
    "US Z.1": fetch_us, "US SOMA": fetch_us_soma, "US TIC": fetch_us_tic,
    "CA BoC": fetch_ca_boc, "CA StatCan": fetch_ca_statcan,
    "ECB GFS": fetch_ea_gfs, "ECB BSI": fetch_ea_bsi, "Eurostat": fetch_ea_estat,
    "JP FoF": fetch_jp_ff, "JP BoJ": fetch_jp_boj,
    "UK ONS": fetch_uk_ons, "UK BoE": fetch_uk_boe,
}
FETCH_SRC = {"US Z.1": SRC_FRED_Z1, "US SOMA": SRC_FRED_SOMA, "US TIC": SRC_TIC,
             "CA BoC": SRC_BOC, "CA StatCan": SRC_STATCAN, "ECB GFS": SRC_ECB_GFS,
             "ECB BSI": SRC_ECB_BSI, "Eurostat": SRC_ESTAT, "JP FoF": SRC_BOJ_FF, "JP BoJ": SRC_BOJ_BS,
             "UK ONS": SRC_ONS, "UK BoE": SRC_BOE}


def refetch(prev: dict | None) -> dict:
    """Run every fetcher; keep previous raw series for any fetcher that fails."""
    prev_raw = (prev or {}).get("raw", {})
    prev_status = {s["name"]: s for s in (prev or {}).get("fetch_status", [])}
    raw: dict = {}
    status = []

    def run(name):
        t0 = time.time()
        try:
            return name, FETCHERS[name](), None, time.time() - t0
        except Exception as e:  # noqa: BLE001 - one source must never fail the build
            return name, None, f"{type(e).__name__}: {str(e)[:200]}", time.time() - t0

    with cf.ThreadPoolExecutor(len(FETCHERS)) as ex:
        results = list(ex.map(run, FETCHERS))
    for name, got, err, dt in results:
        logger.info("%s: %s (%.1fs)", name, "ok" if got else err, dt)
        if got:
            raw.update(got)
            as_of = max(v["as_of"] for v in got.values())
            status.append({"name": name, "status": "ok", "as_of": as_of, "note": ""})
        else:
            # carry forward previous series from this fetcher, marked stale
            stale = {k: {**v, "status": "stale"} for k, v in prev_raw.items()
                     if v.get("fetcher") == name}
            raw.update(stale)
            st = "stale" if stale else "error"
            note = err + (" (serving cached data)" if stale else "")
            status.append({"name": name, "status": st,
                           "as_of": prev_status.get(name, {}).get("as_of"), "note": note})
        for k in (got or {}):
            raw[k]["fetcher"] = name
    partial = any(s["status"] != "ok" for s in status)
    return {"fetched_at": now_iso(), "partial": partial, "raw": raw, "fetch_status": status}


# ---------------------------------------------------------------- builders
def _qt(s: pd.Series | None) -> dict | None:
    if s is None or s.empty:
        return None
    s = s.dropna().sort_index()
    end = s.index[-1]
    lvl = float(s.iloc[-1])
    out = {"as_of": end.strftime("%Y-%m-%d"), "level": round(lvl, 1)}
    for n in (3, 6, 12):
        past = s[s.index <= end - pd.DateOffset(months=n) + pd.Timedelta(days=3)]
        if past.empty:
            out.update({f"chg_{n}m": None, f"pct_{n}m": None, f"ann_{n}m": None})
            continue
        p = float(past.iloc[-1])
        chg = lvl - p
        out[f"chg_{n}m"] = round(chg, 1)
        out[f"pct_{n}m"] = round(100 * chg / p, 2) if p else None
        out[f"ann_{n}m"] = round(chg * 12 / n, 1)
    return out


def _cb_block(raw: dict | None, name: str, note: str = "") -> dict | None:
    s = _ser(raw)
    if s is None:
        return None
    hist = s[s.index >= pd.Timestamp(START)]
    hist = hist.groupby(hist.index.to_period("M")).last()   # month-end sampling
    dates = [p.to_timestamp(how="end").strftime("%Y-%m-%d") for p in hist.index]
    dates[-1] = raw["as_of"]
    blk = {"name": name, "source": raw["source"], "as_of": raw["as_of"], "freq": raw["freq"],
           "unit": raw["unit"], "dates": dates, "values": [round(float(v), 1) for v in hist.values],
           "qt": _qt(s)}
    if raw.get("status") == "stale":
        blk["stale"] = True
    if note:
        blk["note"] = note
    return blk


def _holders(levels: dict[str, pd.Series], freq: str, unit: str, src: str,
             measure: str) -> dict | None:
    """levels: total + any of foreign/central_bank/banks/other_financial/households_other."""
    if "total" not in levels or levels["total"] is None:
        return None
    df = pd.DataFrame({k: v for k, v in levels.items() if v is not None})
    df = df[df.index >= pd.Timestamp(START)].dropna(subset=["total"])
    # keep only periods where at least one sector is present
    sect = [c for c in df.columns if c != "total"]
    df = df.dropna(how="all", subset=sect)
    if df.empty:
        return None
    r1 = lambda x: None if pd.isna(x) else round(float(x), 1)  # noqa: E731
    dates = [d.strftime("%Y-%m-%d") for d in df.index]
    lv = {c: [r1(x) for x in df[c]] for c in ["total"] + [k for k in SECTORS if k in df]}
    sh = {}
    for c in [k for k in SECTORS if k in df]:
        sh[c] = [r1(x) for x in 100 * df[c] / df["total"]]
    if "foreign" in df:
        sh["domestic"] = [r1(x) for x in 100 * (df["total"] - df["foreign"]) / df["total"]]
    return {"source": src, "as_of": dates[-1], "freq": freq, "unit": unit, "measure": measure,
            "dates": dates, "levels": lv, "share_pct": sh}


def _latest_split(h: dict | None) -> dict | None:
    if not h:
        return None
    i = len(h["dates"]) - 1
    items = []
    for k, label in SECTORS.items():
        if k in h["levels"]:
            v = h["levels"][k][i]
            items.append({"key": k, "label": label, "value": v,
                          "share": h["share_pct"][k][i]})
    return {"as_of": h["as_of"], "source": h["source"], "unit": h["unit"],
            "total": h["levels"]["total"][i], "items": items}


def _q_sample(s: pd.Series | None) -> pd.Series | None:
    """Sample a weekly/monthly series at quarter-end (last obs in quarter), quarter-end index."""
    if s is None:
        return None
    q = s.groupby(s.index.to_period("Q")).last()
    q.index = q.index.to_timestamp(how="end").normalize()
    return q


def _country_us(raw: dict) -> dict:
    g = lambda k: _ser(raw.get(f"US.z1.{k}"))  # noqa: E731
    notes = ["Z.1 Treasury securities (marketable + nonmarketable held outside federal govt), "
             "book value; quarterly.",
             "Other financial = insurers + private/state&local/federal pensions + mutual & money "
             "market funds; households & other = residual (households, nonprofits, "
             "state & local govts, nonfinancial business, brokers, GSEs, etc.)."]
    lv = {"total": g("total"), "foreign": g("foreign"), "central_bank": g("central_bank"),
          "banks": g("banks")}
    if g("ins_pens") is not None and g("funds") is not None:
        lv["other_financial"] = g("ins_pens") + g("funds")
    if all(lv.get(k) is not None for k in ["total", "foreign", "central_bank", "banks",
                                           "other_financial"]):
        lv["households_other"] = (lv["total"] - lv["foreign"] - lv["central_bank"]
                                  - lv["banks"] - lv["other_financial"])
    h = _holders(lv, "Q", "USD bn", SRC_FRED_Z1, "Treasury securities held by sector (Z.1)")
    extra = {}
    t = raw.get("US.tic")
    if t:
        extra["foreign_monthly"] = {k: t[k] for k in ("source", "as_of", "unit", "dates", "values")}
        extra["foreign_monthly"]["label"] = "Foreign holdings of Treasuries (TIC, monthly)"
    return {"holders": h, "central_bank": _cb_block(raw.get("US.cb"),
            "Federal Reserve SOMA Treasuries (TREAST, Wednesday level)"),
            "extra": extra, "notes": notes}


def _country_ca(raw: dict) -> dict:
    notes = []
    tot = _ser(raw.get("CA.total_m"))
    boc = _ser(raw.get("CA.boc_m"))
    sc = {k[len("CA.sc."):]: _ser(v) for k, v in raw.items() if k.startswith("CA.sc.")}
    h = None
    if sc.get("total") is not None:
        lv = {"total": sc["total"]}
        for k in ("foreign", "banks", "central_bank"):
            if sc.get(k) is not None:
                lv[k] = sc[k]
        if sc.get("ins_pens") is not None and sc.get("funds") is not None:
            lv["other_financial"] = sc["ins_pens"] + sc["funds"]
        if all(lv.get(k) is not None for k in ("foreign", "banks", "central_bank",
                                               "other_financial")):
            lv["households_other"] = (lv["total"] - lv["foreign"] - lv["banks"]
                                      - lv["central_bank"] - lv["other_financial"])
        if lv.get("central_bank") is None and boc is not None:
            lv["central_bank"] = _q_sample(boc)
            notes.append("Central bank from BoC Valet (par value) sampled at quarter-end.")
        h = _holders(lv, "Q", "CAD bn", SRC_STATCAN + " + " + SRC_BOC,
                     "Government of Canada debt securities by holder")
        notes.append("StatCan National Balance Sheet (36-10-0580), GoC short-term paper + bonds, "
                     "market value. Banks = chartered banks + quasi-banks; other financial = "
                     "insurers & pension funds + mutual funds; households & other = residual "
                     "(households, provincial govts, other financial intermediaries, etc.).")
    elif tot is not None and boc is not None:
        q_tot, q_boc = _q_sample(tot), _q_sample(boc)
        h = _holders({"total": q_tot, "central_bank": q_boc}, "Q", "CAD bn", SRC_BOC,
                     "GoC marketable + retail securities outstanding; BoC holdings")
        notes.append("Only central-bank share available (StatCan sector data not fetched); "
                     "foreign/bank/other splits missing.")
    return {"holders": h, "central_bank": _cb_block(raw.get("CA.cb"),
            "Bank of Canada holdings of GoC securities (weekly, par)"), "notes": notes}


def _country_ea(raw: dict, c: str) -> dict:
    lv = {k: _ser(raw.get(f"{c}.gfs.{k}")) for k in
          ["total", "foreign", "central_bank", "banks", "other_financial", "households_other"]}
    h = _holders(lv, "A", "EUR bn", SRC_ECB_GFS,
                 "General government consolidated gross (Maastricht) debt, nominal, by holder")
    notes = ["Holder split is annual (ECB GFS / Eurostat): W1 non-residents; resident S121 "
             "central bank, S122-123 banks (MFIs ex CB), S124-129 other financial, "
             "non-financial sectors.",
             "Central bank monthly = NCB holdings of euro-area general government debt "
             "securities (ECB BSI); mostly PSPP/PEPP domestic sovereign bonds but includes other "
             "euro-area govt paper and non-policy portfolios; excludes the ECB's own share."]
    extra = {}
    ft, ff = _ser(raw.get(f"{c}.estat.total")), _ser(raw.get(f"{c}.estat.foreign"))
    if ft is not None and ff is not None:
        df = pd.DataFrame({"t": ft, "f": ff}).dropna()
        df = df[df.index >= pd.Timestamp(START)]
        dates = [d.strftime("%Y-%m-%d") for d in df.index]
        extra["foreign_quarterly"] = {
            "label": "Non-resident share of general government debt (quarterly)",
            "source": SRC_ESTAT, "as_of": dates[-1], "unit": "EUR bn", "dates": dates,
            "total": [round(float(x), 1) for x in df["t"]],
            "foreign": [round(float(x), 1) for x in df["f"]],
            "foreign_pct": [round(float(x), 1) for x in 100 * df["f"] / df["t"]]}
    return {"holders": h, "extra": extra, "central_bank": _cb_block(raw.get(f"{c}.cb"),
            {"DE": "Deutsche Bundesbank", "FR": "Banque de France",
             "IT": "Banca d'Italia"}[c] + " holdings of euro-area govt securities"),
            "notes": notes}


def _country_jp(raw: dict) -> dict:
    g = lambda k: _ser(raw.get(f"JP.ff.{k}"))  # noqa: E731
    lv = {"total": g("total"), "foreign": g("foreign"), "central_bank": g("central_bank"),
          "banks": g("banks"), "other_financial": g("ins_pens")}
    if all(v is not None for v in lv.values()):
        lv["households_other"] = (lv["total"] - lv["foreign"] - lv["central_bank"]
                                  - lv["banks"] - lv["other_financial"])
    h = _holders(lv, "Q", "JPY tn", SRC_BOJ_FF,
                 "Central government securities & FILP bonds by holder (Flow of Funds)")
    notes = ["Flow of Funds is market value and includes FILP bonds; BoJ-accounts series below "
             "is book value of JGBs, so levels differ.",
             "Other financial = insurance & pension funds only; securities firms, investment "
             "trusts and other financial intermediaries fall into households & other (residual)."]
    return {"holders": h, "central_bank": _cb_block(raw.get("JP.cb"),
            "Bank of Japan JGB holdings (month-end, book value)"), "notes": notes}


def _country_uk(raw: dict) -> dict:
    g = lambda k: _ser(raw.get(f"UK.ons.{k}"))  # noqa: E731
    apf = _q_sample(_ser(raw.get("UK.cb")))
    tot, fx, mfi, fin, uk = g("total"), g("foreign"), g("mfi"), g("fin_all"), g("uk_all")
    lv = {"total": tot, "foreign": fx}
    notes = ["ONS AF.32N1 (long-term central govt debt securities = essentially gilts), "
             "market value, quarterly.",
             "Central bank = BoE APF gilt stock (IADB YWWB9T9) at quarter-end; banks = ONS MFIs "
             "(S.121-123) minus APF, floored at 0 (valuation bases differ, approximate).",
             "Other financial = all financial corps minus MFIs (insurers & pensions, funds, "
             "other); households & other = UK non-financial sectors."]
    if mfi is not None:
        if apf is not None:
            cb = apf.reindex(mfi.index)
            lv["central_bank"] = cb
            lv["banks"] = (mfi - cb.fillna(0)).clip(lower=0)
        else:
            lv["banks"] = mfi
            notes.append("APF series unavailable: banks include the Bank of England.")
    if fin is not None and mfi is not None:
        lv["other_financial"] = fin - mfi
    if uk is not None and fin is not None:
        lv["households_other"] = uk - fin
    h = _holders(lv, "Q", "GBP bn", SRC_ONS + " + " + SRC_BOE,
                 "Gilts (central govt long-term debt securities) by holder")
    return {"holders": h, "central_bank": _cb_block(raw.get("UK.cb"),
            "BoE Asset Purchase Facility gilt holdings (weekly)"), "notes": notes}


def build_payload(seed: dict) -> dict:
    raw = seed.get("raw", {})
    countries, comparison = {}, []
    builders = {"US": _country_us, "CA": _country_ca, "JP": _country_jp, "UK": _country_uk,
                "DE": lambda r: _country_ea(r, "DE"), "FR": lambda r: _country_ea(r, "FR"),
                "IT": lambda r: _country_ea(r, "IT")}
    for c in ORDER:
        try:
            blk = builders[c](raw)
        except Exception as e:  # noqa: BLE001
            logger.exception("holdings build %s failed", c)
            blk = {"holders": None, "central_bank": None, "notes": [f"build error: {e}"]}
        blk = {"name": NAMES[c], "currency": CCY[c], **blk}
        blk["latest_split"] = _latest_split(blk.get("holders"))
        countries[c] = blk
        h = blk.get("holders")
        row = {"country": c, "as_of": h["as_of"] if h else None,
               "source": h["source"] if h else None, "freq": h["freq"] if h else None}
        for k in ["foreign", "domestic"] + DOMESTIC:
            row[k] = h["share_pct"].get(k, [None])[-1] if h else None
        fq = (blk.get("extra") or {}).get("foreign_quarterly")
        if fq and (not h or fq["as_of"] > h["as_of"]):
            row["foreign_latest"], row["foreign_latest_as_of"] = fq["foreign_pct"][-1], fq["as_of"]
            row["foreign_latest_source"] = fq["source"]
        else:
            row["foreign_latest"] = row["foreign"]
            row["foreign_latest_as_of"] = row["as_of"]
            row["foreign_latest_source"] = row["source"]
        cb = blk.get("central_bank")
        row["cb_qt_ann_12m"] = (cb or {}).get("qt", {}) and cb["qt"].get("ann_12m")
        row["cb_qt_pct_12m"] = (cb or {}).get("qt", {}) and cb["qt"].get("pct_12m")
        row["cb_unit"] = cb["unit"] if cb else None
        comparison.append(row)

    sources = [source(s["name"] + " - " + FETCH_SRC.get(s["name"], ""), s["status"],
                      s.get("as_of"), s.get("note", "")) for s in seed.get("fetch_status", [])]
    sources.append(source("IMF Sovereign Debt Investor Base (Arslanalp-Tsuda)", "error", None,
                          "imf.org blocks automated downloads (HTTP 403) and the dataset is not "
                          "on api.imf.org; national/ECB sources used for every country instead."))
    return {"generated_at": now_iso(), "fetched_at": seed.get("fetched_at"),
            "sources": sources, "order": ORDER, "sectors": SECTORS,
            "comparison": comparison, "countries": countries}


def build_holdings() -> dict:
    try:
        seed = load_seed(SEED)
    except Exception:  # noqa: BLE001 - corrupt seed: refetch
        seed = None
    try:
        age = seed_age_days(SEED) if seed else None
    except Exception:  # noqa: BLE001
        age = None
    force = os.environ.get("STRESS_FORCE") == "1"
    stale_limit = 1 if (seed or {}).get("partial") else MAX_AGE_DAYS  # retry failures daily
    if force or age is None or age > stale_limit or not seed.get("raw"):
        try:
            new = refetch(seed)
            if new["raw"]:
                save_seed(SEED, new)
                seed = new
        except Exception:  # noqa: BLE001
            logger.exception("holdings refetch failed; using seed")
    if not seed:
        return {"generated_at": now_iso(), "fetched_at": None, "order": ORDER,
                "sectors": SECTORS, "comparison": [], "countries": {},
                "sources": [source("holdings", "error", None, "no data and no seed")]}
    try:
        return build_payload(seed)
    except Exception as e:  # noqa: BLE001
        logger.exception("holdings payload build failed")
        return {"generated_at": now_iso(), "fetched_at": seed.get("fetched_at"), "order": ORDER,
                "sectors": SECTORS, "comparison": [], "countries": {},
                "sources": [source("holdings", "error", None, f"build failed: {e}")]}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    t0 = time.time()
    payload = build_holdings()
    p = dump(OUT, payload)
    print(f"wrote {p} ({p.stat().st_size / 1024:.1f} KB) in {time.time() - t0:.1f}s")
    for s in payload["sources"]:
        print(f"  [{s['status']}] {s['name']} as_of={s['as_of']} {s['note']}")
