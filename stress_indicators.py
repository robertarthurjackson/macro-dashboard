"""Stress indicator panel for the Stress & Auctions tab.

build_indicators() -> dict, written to site/data/stress/indicators.json.

Schema
------
Top level:
  generated_at   ISO-8601 UTC stamp
  sources        [{name, status: ok|error|stub, as_of, note}]  one per indicator
                 (stress_common contract)
  summary        {ok, error, stub, total, in_stress_zone: [ids], first_use: [ids]}
  groups         {group_id: label}
  regimes        {regime_id: label}
  rules          plain-text definitions of every statistic (see STATS_RULES)
  indicators     [record, ...]

Indicator record:
  id, label, group, regimes[], direction (+1 higher = more stress,
  -1 lower = more stress), units, source, frequency (daily|weekly|monthly|
  quarterly), status (ok|error|stub), note, proxy_for (id of the indicator this
  proxies, or null), proxy (id of a free proxy for a stub, or null),
  binary (emergency-facility usage indicator), as_of (YYYY-MM-DD), stale (bool),
  latest (float), history [[YYYY-MM-DD, value], ...] (last ~3y; weekly
  Friday-close when the native series is daily), stats {...} (null unless ok),
  flags {first_use, active, sign_flip} (null when not applicable).

stats (computed on the last 10y of native-frequency data, or all available):
  z_3y             (latest - mean_3y) / std_3y
  pct_full         percentile rank of latest in the 10y history (0-100)
  dir_pct          pct_full if direction=+1 else 100 - pct_full
  dir_z            z_3y * direction
  stress_zone      dir_pct >= 90 or dir_z >= 2
  chg_1w/1m/3m     latest minus value as of 7/30/91 calendar days earlier
                   (native units; chg_1w null for monthly/quarterly series)
  accel_1m         chg_1m - (chg_1m measured one month earlier)
  var_1y, ac1_1y   rolling 1y variance and lag-1 autocorrelation of
                   native-frequency changes (early-warning / critical-slowing-down
                   statistics); null when the 1y window has < 20 changes
  var_1y_6m_ago, ac1_1y_6m_ago, var_trend, ac1_trend (rising|falling|flat:
                   variance ratio vs 6m ago > 1.25 / < 0.8; autocorr delta
                   >= +0.1 / <= -0.1), ew_signal (var_trend and ac1_trend both
                   rising)
  n_obs, history_start

Binary emergency facilities (swap lines, FIMA repo, SRF, discount window,
BTFP): flags.first_use = max usage over the last 30 days > threshold while the
preceding 90 days (days 31-120 back) never exceeded it; flags.active = latest
> threshold; srf_usage adds flags.turn_driven (all recent usage within 2
business days of a month end).  Stock-bond correlation: flags.sign_flip = sign of latest 63d
correlation differs from its sign 21 trading days earlier.

Trending prices are made stationary before stats: bank equities as 3m
relative return vs SPY, BKLN as drawdown from 1y high, dollar / USDJPY / EM FX
/ gold as 3m % change, deposits as 13w % change, MMF assets and margin debt as
YoY %, Fed balance sheet as 13w $ change.

Every fetch is wrapped: a failure marks that indicator "error" and never
raises.  Fetches run concurrently in a thread pool.
"""
from __future__ import annotations

import io
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from stress_common import dump, fred_series, http_get, now_iso, source

logger = logging.getLogger("stress.indicators")

START = "2014-01-01"            # fetch start (>10y so the 10y stats window is full)
STATS_YEARS = 10
Z_YEARS = 3
HISTORY_YEARS = 3
MAX_WORKERS = 16

GROUPS = {
    "dollar_funding": "Dollar funding",
    "money_repo": "Money markets & repo",
    "bank_funding": "Bank funding",
    "sovereign_functioning": "Sovereign market functioning",
    "sovereign_stress": "Sovereign stress",
    "credit": "Credit",
    "cross_asset": "Cross-asset",
    "macro": "Macro / curve",
    "leverage": "Leverage",
    "composite": "Composite stress indices",
}
REGIMES = {
    "liquidity_tightening": "Liquidity tightening",
    "funding_crisis": "Funding crisis",
    "market_functioning": "Market functioning",
    "sovereign_fiscal": "Sovereign / fiscal",
    "credit_stress": "Credit stress",
    "policy_intervention": "Policy intervention",
}
STATS_RULES = {
    "window": f"Stats use the last {STATS_YEARS}y of native-frequency observations (less where the source is shorter; ICE BofA OAS on FRED is limited to ~3y).",
    "z_3y": f"(latest - mean of trailing {Z_YEARS}y) / std of trailing {Z_YEARS}y; needs >= 12 obs.",
    "pct_full": "Midrank percentile of latest in the stats window: (share < latest + 0.5 * share == latest) x100.",
    "dir_pct": "pct_full if direction=+1 else 100-pct_full (higher = more stress).",
    "dir_z": "z_3y * direction (higher = more stress).",
    "stress_zone": "dir_pct >= 90 OR dir_z >= 2.",
    "changes": "chg_1w/chg_1m/chg_3m = latest minus last value on or before 7/30/91 calendar days earlier, in native units. accel_1m = chg_1m - chg_1m measured as of 30 days earlier.",
    "early_warning": "On native-frequency first differences: rolling 1y variance (var_1y) and lag-1 autocorrelation (ac1_1y); 1y = 252/52/12/4 obs for daily/weekly/monthly/quarterly; null if < 20 changes per window. Trend vs 6 months ago: var ratio >1.25 rising, <0.8 falling; ac1 delta >= +0.1 rising, <= -0.1 falling. ew_signal = both rising.",
    "first_use": "Binary facilities: max over last 30 days > threshold and no value > threshold in days 31-120 back. srf_usage also carries flags.turn_driven = all recent above-threshold days fall within 2 business days of a month end.",
    "sign_flip": "Stock-bond correlation: sign of latest 63d correlation differs from 21 trading days earlier.",
    "history": f"Last {HISTORY_YEARS}y; daily series downsampled to weekly (Friday, last value).",
}

# ── memoised fetch helpers (several indicators share inputs) ─────────────────
_memo: dict = {}
_memo_locks: dict = {}
_memo_guard = threading.Lock()


def _cached(key, fn):
    with _memo_guard:
        lock = _memo_locks.setdefault(key, threading.Lock())
    with lock:
        if key not in _memo:
            try:
                _memo[key] = ("ok", fn())
            except Exception as exc:  # cache failures too so we don't retry 5x
                _memo[key] = ("err", exc)
        status, val = _memo[key]
    if status == "err":
        raise val
    return val


def fred(sid: str) -> pd.Series:
    def go():
        for attempt in range(3):
            try:
                return fred_series(sid, START)
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
    return _cached(("fred", sid), go)


YF_TICKERS = ["^VIX", "^VIX3M", "^MOVE", "KRE", "KBE", "SPY", "TLT",
              "CEW", "BKLN", "GC=F"]


def yf_close(ticker: str) -> pd.Series:
    def go():
        import yfinance as yf
        df = yf.download(YF_TICKERS, start=START, progress=False,
                         auto_adjust=True, threads=True)["Close"]
        df.index = pd.to_datetime(df.index).tz_localize(None)
        return df
    df = _cached(("yf",), go)
    if ticker not in df.columns:
        raise ValueError(f"yfinance returned no column {ticker}")
    s = df[ticker].dropna()
    if s.empty:
        raise ValueError(f"yfinance returned empty series for {ticker}")
    return s


def ecb(key: str) -> pd.Series:
    """ECB Data Portal series 'FLOW/KEY' as float Series."""
    def go():
        r = http_get(f"https://data-api.ecb.europa.eu/service/data/{key}",
                     params={"format": "csvdata", "startPeriod": START[:7],
                             "detail": "dataonly"}, timeout=60)
        df = pd.read_csv(io.StringIO(r.text))
        tp = df["TIME_PERIOD"].astype(str)
        if tp.str.contains("-W").any():
            idx = pd.to_datetime(tp + "-5", format="%G-W%V-%u")
        elif tp.str.contains("-Q").any():
            idx = pd.PeriodIndex(tp.str.replace("-Q", "Q"), freq="Q").to_timestamp()
        else:
            idx = pd.to_datetime(tp)
        s = pd.Series(pd.to_numeric(df["OBS_VALUE"], errors="coerce").values,
                      index=idx).dropna()
        return s.sort_index()
    return _cached(("ecb", key), go)


def nyfed_sofr() -> pd.DataFrame:
    def go():
        end = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d")
        j = http_get("https://markets.newyorkfed.org/api/rates/secured/sofr/search.json",
                     params={"startDate": "2018-04-01", "endDate": end}, timeout=60).json()
        df = pd.DataFrame(j["refRates"])
        df.index = pd.to_datetime(df["effectiveDate"])
        return df.sort_index()
    return _cached(("nyfed_sofr",), go)


def ofr(mnemonic: str, api: str = "v1") -> pd.Series:
    def go():
        base = ("https://data.financialresearch.gov/v1" if api == "v1"
                else "https://data.financialresearch.gov/hf/v1")
        j = http_get(f"{base}/series/timeseries",
                     params={"mnemonic": mnemonic}, timeout=60).json()
        s = pd.Series({d: v for d, v in j if v is not None}, dtype=float)
        s.index = pd.to_datetime(s.index)
        return s.sort_index()
    return _cached(("ofr", api, mnemonic), go)


def boc(series: str) -> pd.Series:
    def go():
        j = http_get(f"https://www.bankofcanada.ca/valet/observations/{series}/json",
                     params={"start_date": START}).json()
        rows = {o["d"]: o[series]["v"] for o in j["observations"] if series in o}
        s = pd.to_numeric(pd.Series(rows), errors="coerce").dropna()
        s.index = pd.to_datetime(s.index)
        return s.sort_index()
    return _cached(("boc", series), go)


def boj(db: str, code: str) -> pd.Series:
    def go():
        out: dict = {}
        pos = None
        for _ in range(20):
            params = {"format": "json", "lang": "en", "db": db, "code": code,
                      "startDate": START[:4] + START[5:7]}
            if pos:
                params["startPosition"] = pos
            j = http_get("https://www.stat-search.boj.or.jp/api/v1/getDataCode",
                         params=params, timeout=60).json()
            for rs in j.get("RESULTSET", []):
                v = rs["VALUES"]
                for d, x in zip(v["SURVEY_DATES"], v["VALUES"]):
                    if x is not None and x != "":
                        out[str(d)] = float(x)
            pos = j.get("NEXTPOSITION")
            if not pos:
                break
        s = pd.Series(out)
        s.index = pd.to_datetime(s.index, format="%Y%m%d")
        return s.sort_index()
    return _cached(("boj", db, code), go)


# ── derived-series builders ─────────────────────────────────────────────────

def spread(a: pd.Series, b: pd.Series, scale: float = 100.0, ffill_b: bool = True) -> pd.Series:
    """a - b on a's dates (b forward-filled), scaled (default % -> bp)."""
    b2 = b.reindex(a.index.union(b.index)).ffill().reindex(a.index) if ffill_b else b.reindex(a.index)
    return ((a - b2) * scale).dropna()


def iorb_spliced() -> pd.Series:
    """IOER (to 2021-07-28) spliced with IORB."""
    iorb = fred("IORB")
    try:
        ioer = fred("IOER")
        ioer = ioer[ioer.index < iorb.index.min()]
        return pd.concat([ioer, iorb]).sort_index()
    except Exception:
        return iorb


def pct_change_days(s: pd.Series, days: int) -> pd.Series:
    prev = s.reindex(s.index - pd.Timedelta(days=days), method="ffill")
    prev.index = s.index
    return (s / prev - 1).mul(100).dropna()


def log_ratio(a: pd.Series, b: pd.Series) -> pd.Series:
    df = pd.concat([a, b], axis=1).dropna()
    return np.log(df.iloc[:, 0] / df.iloc[:, 1]) * 100


def rel_return(a: pd.Series, b: pd.Series, n: int = 63) -> pd.Series:
    """n-day log return of a minus that of b, in % (stationary relative performance)."""
    lr = log_ratio(a, b)
    return (lr - lr.shift(n)).dropna()


def ret_n(s: pd.Series, n: int = 63) -> pd.Series:
    """n-observation % change (3m for daily data)."""
    return (s / s.shift(n) - 1).mul(100).dropna()


def drawdown_1y(s: pd.Series) -> pd.Series:
    """% below trailing 252-day high (<= 0)."""
    return (s / s.rolling(252, min_periods=20).max() - 1).mul(100).dropna()


def stock_bond_corr() -> pd.Series:
    df = pd.concat([yf_close("SPY"), yf_close("TLT")], axis=1).dropna()
    r = np.log(df).diff().dropna()
    return r.iloc[:, 0].rolling(63).corr(r.iloc[:, 1]).dropna()


def euro_excess_liquidity() -> pd.Series:
    dep = ecb("ILM/W.U2.C.L020200.U2.EUR")
    ca = ecb("ILM/W.U2.C.L020100.U2.EUR")
    try:
        mr = ecb("ILM/W.U2.C.L020300.U2.EUR")  # minimum reserve requirements
    except Exception:
        mr = pd.Series(0.0, index=dep.index)
    df = pd.concat([dep, ca, mr.reindex(dep.index).ffill().fillna(0)], axis=1).dropna()
    return (df.iloc[:, 0] + df.iloc[:, 1] - df.iloc[:, 2]) / 1000.0


def euribor_estr() -> pd.Series:
    eur = ecb("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA")
    estr = ecb("EST/B.EU000A2X2A25.WT").resample("MS").mean()
    try:  # EONIA before €STR (EONIA = €STR + 8.5bp after 2019-10)
        eonia = ecb("FM/M.U2.EUR.4F.MM.EONIA.HSTA")
        eonia = eonia[eonia.index < estr.index.min()]
        on = pd.concat([eonia, estr]).sort_index()
    except Exception:
        on = estr
    return spread(eur, on, ffill_b=False)


def curve_fit_rmse() -> pd.Series:
    """RMSE (bp) of a daily Nelson-Siegel fit (fixed lambda=0.0609/month,
    Diebold-Li) to the 11 FRED CMT par yields.  Free proxy for Hu-Pan-Wang
    noise; CMT yields are already smoothed by Treasury's spline, so this
    understates true dislocations."""
    tenors = {"DGS1MO": 1, "DGS3MO": 3, "DGS6MO": 6, "DGS1": 12, "DGS2": 24,
              "DGS3": 36, "DGS5": 60, "DGS7": 84, "DGS10": 120, "DGS20": 240,
              "DGS30": 360}
    df = pd.concat({k: fred(k) for k in tenors}, axis=1).dropna()
    m = np.array([tenors[c] for c in df.columns], dtype=float)
    lam = 0.0609
    x1 = (1 - np.exp(-lam * m)) / (lam * m)
    X = np.column_stack([np.ones_like(m), x1, x1 - np.exp(-lam * m)])
    Y = df.values.T
    beta, *_ = np.linalg.lstsq(X, Y, rcond=None)
    resid = Y - X @ beta
    rmse = np.sqrt((resid ** 2).mean(axis=0)) * 100
    return pd.Series(rmse, index=df.index)


CFTC_UST = {  # code: (label, approx DV01 weight vs 10y note contract)
    "042601": ("2Y", 0.57), "044601": ("5Y", 0.66), "043602": ("10Y", 1.0),
    "043607": ("Ultra 10Y", 1.4), "020601": ("Bond", 2.0), "020604": ("Ultra Bond", 3.4),
}


def cftc_lev_net() -> pd.Series:
    codes = ",".join(f"'{c}'" for c in CFTC_UST)
    j = http_get("https://publicreporting.cftc.gov/resource/gpe5-46if.json", params={
        "$select": "report_date_as_yyyy_mm_dd,cftc_contract_market_code,"
                   "lev_money_positions_long,lev_money_positions_short",
        "$where": f"cftc_contract_market_code in ({codes}) AND "
                  f"report_date_as_yyyy_mm_dd >= '{START}T00:00:00'",
        "$limit": 50000}, timeout=60).json()
    df = pd.DataFrame(j)
    df["d"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"])
    df["w"] = df["cftc_contract_market_code"].map(lambda c: CFTC_UST[c][1])
    net = (pd.to_numeric(df["lev_money_positions_long"]) -
           pd.to_numeric(df["lev_money_positions_short"])) * df["w"]
    out = net.groupby(df["d"]).sum() / 1000.0
    # drop weeks where some contracts are missing (partial sums)
    cnt = df.groupby("d")["cftc_contract_market_code"].nunique()
    return out[cnt >= cnt.max() - 1].sort_index()


def nyfed_pd(keyid: str) -> pd.Series:
    j = http_get(f"https://markets.newyorkfed.org/api/pd/get/{keyid}.json", timeout=60).json()
    rows = {r["asofdate"]: r["value"] for r in j["pd"]["timeseries"]}
    s = pd.to_numeric(pd.Series(rows), errors="coerce").dropna()
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


def ofr_fsi() -> pd.Series:
    r = http_get("https://www.financialresearch.gov/financial-stress-index/data/fsi.csv", timeout=60)
    df = pd.read_csv(io.StringIO(r.text))
    s = pd.Series(pd.to_numeric(df["OFR FSI"], errors="coerce").values,
                  index=pd.to_datetime(df["Date"])).dropna()
    return s.sort_index()


def finra_margin() -> pd.Series:
    """FINRA margin debt YoY % (debit balances in customers' securities margin accounts)."""
    url = "https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx"
    try:
        import re
        page = http_get("https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics").text
        links = re.findall(r'href="([^"]+margin-statistics[^"]*\.xlsx)"', page)
        if links:
            url = links[0] if links[0].startswith("http") else "https://www.finra.org" + links[0]
    except Exception:
        pass
    df = pd.read_excel(io.BytesIO(http_get(url, timeout=60).content))
    col = next(c for c in df.columns if "debit" in str(c).lower())
    s = pd.Series(pd.to_numeric(df[col], errors="coerce").values,
                  index=pd.to_datetime(df.iloc[:, 0].astype(str))).dropna().sort_index()
    return s.pct_change(12).mul(100).dropna()


# ── catalog ─────────────────────────────────────────────────────────────────
# Each entry: id, label, group, regimes, direction, units, source, frequency,
# fetch (callable -> pd.Series) or stub=True, plus optional note / proxy /
# proxy_for / binary (threshold) / transform.

LT, FC, MF, SF, CS, PI = ("liquidity_tightening", "funding_crisis", "market_functioning",
                          "sovereign_fiscal", "credit_stress", "policy_intervention")


def _bn(sid, div=1000.0):
    return lambda: fred(sid) / div


CATALOG: list[dict] = [
    # Dollar funding
    dict(id="xccy_eurusd_3m", label="EUR/USD 3m cross-currency basis", group="dollar_funding",
         regimes=[FC, LT], direction=-1, units="bp", source="Bloomberg/LSEG (paid)",
         frequency="daily", stub=True,
         note="Paid data. CIP-implied basis from free FX forwards is impractical; "
              "watch fed_swap_lines / fima_repo usage as the free dollar-funding signals."),
    dict(id="xccy_usdjpy_3m", label="USD/JPY 3m cross-currency basis", group="dollar_funding",
         regimes=[FC, LT], direction=-1, units="bp", source="Bloomberg/LSEG (paid)",
         frequency="daily", stub=True, note="Paid data; no practical free proxy."),
    dict(id="xccy_usdcad_3m", label="USD/CAD 3m cross-currency basis", group="dollar_funding",
         regimes=[FC, LT], direction=-1, units="bp", source="Bloomberg/LSEG (paid)",
         frequency="daily", stub=True, note="Paid data; no practical free proxy."),
    dict(id="fed_swap_lines", label="Fed central bank liquidity swaps", group="dollar_funding",
         regimes=[FC, PI], direction=1, units="$bn", source="FRED SWPT (H.4.1)",
         frequency="weekly", fetch=_bn("SWPT"), binary=1.0,
         note="Small weekly ECB/BoJ test operations (~$0.1-0.5bn) are normal; threshold $1bn."),
    dict(id="fima_repo", label="FIMA repo facility", group="dollar_funding",
         regimes=[FC, PI], direction=1, units="$bn",
         source="FRED H41RESPPALGTRFNWW (H.4.1 repo, foreign official)",
         frequency="weekly", fetch=_bn("H41RESPPALGTRFNWW"), binary=0.1),

    # Money markets / repo
    dict(id="sofr_iorb", label="SOFR - IORB", group="money_repo", regimes=[LT, FC],
         direction=1, units="bp", source="FRED SOFR, IORB (IOER before 2021-07-29)",
         frequency="daily", fetch=lambda: spread(fred("SOFR"), iorb_spliced())),
    dict(id="sofr_p99_spread", label="SOFR 99th percentile - SOFR", group="money_repo",
         regimes=[LT, FC, MF], direction=1, units="bp",
         source="NY Fed Markets API (SOFR distribution)", frequency="daily",
         fetch=lambda: (lambda d: ((pd.to_numeric(d["percentPercentile99"], errors="coerce") -
                                    pd.to_numeric(d["percentRate"], errors="coerce")) * 100).dropna())(nyfed_sofr())),
    dict(id="srf_usage", label="Standing Repo Facility / Fed repo ops", group="money_repo",
         regimes=[FC, LT, PI], direction=1, units="$bn",
         source="FRED RPONTSYD (NY Fed repo operations)", frequency="daily",
         fetch=lambda: fred("RPONTSYD"), binary=1.0,
         note="Includes 2019-20 repo operations before the SRF existed; month/quarter-end "
              "take-up is partly calendar-driven."),
    dict(id="on_rrp", label="ON RRP balance", group="money_repo", regimes=[LT],
         direction=-1, units="$bn", source="FRED RRPONTSYD", frequency="daily",
         fetch=lambda: fred("RRPONTSYD"), note="Lower = less excess liquidity buffer."),
    dict(id="reserves", label="Bank reserves", group="money_repo", regimes=[LT, FC],
         direction=-1, units="$bn", source="FRED WRESBAL", frequency="weekly",
         fetch=_bn("WRESBAL")),
    dict(id="tga", label="Treasury General Account", group="money_repo", regimes=[LT],
         direction=1, units="$bn", source="FRED WTREGEN", frequency="weekly",
         fetch=_bn("WTREGEN"), note="Higher TGA drains reserves."),
    dict(id="estr_dfr", label="€STR - ECB deposit rate", group="money_repo", regimes=[LT, FC],
         direction=1, units="bp", source="ECB Data Portal (EST, FM DFR)", frequency="daily",
         fetch=lambda: spread(ecb("EST/B.EU000A2X2A25.WT"), ecb("FM/D.U2.EUR.4F.KR.DFR.LEV"))),
    dict(id="euro_excess_liquidity", label="Euro area excess liquidity", group="money_repo",
         regimes=[LT], direction=-1, units="€bn",
         source="ECB Data Portal ILM (deposit facility + current accounts - min. reserves)",
         frequency="weekly", fetch=euro_excess_liquidity),
    dict(id="corra_target", label="CORRA - BoC policy rate", group="money_repo",
         regimes=[LT, FC], direction=1, units="bp",
         source="Bank of Canada Valet (AVG.INTWO, V39079)", frequency="daily",
         fetch=lambda: spread(boc("AVG.INTWO"), boc("V39079"))),
    dict(id="tona", label="TONA (uncollateralized overnight call rate)", group="money_repo",
         regimes=[LT], direction=1, units="%", source="BoJ Time-Series API (FM01 STRDCLUCON)",
         frequency="daily", fetch=lambda: boj("FM01", "STRDCLUCON"),
         note="Level; rising yen funding cost pressures carry trades."),

    # Bank funding
    dict(id="fin_cp_premium", label="3m AA financial CP - nonfinancial CP", group="bank_funding",
         regimes=[FC, CS], direction=1, units="bp", source="FRED DCPF3M, DCPN3M",
         frequency="daily", fetch=lambda: spread(fred("DCPF3M"), fred("DCPN3M"), ffill_b=False),
         proxy_for="term_sofr_ois",
         note="Free proxy for term bank funding premium (FRA/term-SOFR-OIS): isolates bank "
              "credit/term risk by differencing against same-rated nonfinancial issuers."),
    dict(id="term_sofr_ois", label="Term SOFR - OIS", group="bank_funding", regimes=[FC],
         direction=1, units="bp", source="CME Term SOFR / OIS (licensed)", frequency="daily",
         stub=True, proxy="fin_cp_premium", note="Licensed data; see fin_cp_premium proxy."),
    dict(id="cp_tbill", label="3m AA financial CP - 3m T-bill", group="bank_funding",
         regimes=[FC, CS], direction=1, units="bp", source="FRED DCPF3M, DTB3",
         frequency="daily", fetch=lambda: spread(fred("DCPF3M"), fred("DTB3"), ffill_b=False)),
    dict(id="euribor_estr", label="Euribor 3m - €STR (monthly avg)", group="bank_funding",
         regimes=[FC], direction=1, units="bp",
         source="ECB Data Portal (FM Euribor 3m monthly, EST; EONIA before Oct-2019)",
         frequency="monthly", fetch=euribor_estr,
         note="Daily Euribor is licensed (EMMI); ECB publishes monthly averages."),
    dict(id="bank_cds", label="US/EU bank senior CDS", group="bank_funding", regimes=[FC, CS],
         direction=1, units="bp", source="Markit (paid)", frequency="daily", stub=True,
         proxy="kre_rel_spy", note="Paid data; proxied by regional-bank equity relative performance."),
    dict(id="kre_rel_spy", label="Regional banks vs S&P 500, 3m relative return (KRE-SPY)",
         group="bank_funding", regimes=[FC, CS], direction=-1, units="%",
         source="yfinance KRE, SPY", frequency="daily",
         fetch=lambda: rel_return(yf_close("KRE"), yf_close("SPY")), proxy_for="bank_cds",
         note="63-trading-day log-return differential (levels trend, so percentiles would be biased)."),
    dict(id="kbe_rel_spy", label="Banks vs S&P 500, 3m relative return (KBE-SPY)",
         group="bank_funding", regimes=[FC, CS], direction=-1, units="%",
         source="yfinance KBE, SPY", frequency="daily",
         fetch=lambda: rel_return(yf_close("KBE"), yf_close("SPY")), proxy_for="bank_cds",
         note="63-trading-day log-return differential."),
    dict(id="discount_window", label="Discount window primary credit", group="bank_funding",
         regimes=[FC, PI], direction=1, units="$bn", source="FRED WLCFLPCL (H.4.1)",
         frequency="weekly", fetch=_bn("WLCFLPCL"), binary=1.0),
    dict(id="btfp", label="Bank Term Funding Program", group="bank_funding",
         regimes=[FC, PI], direction=1, units="$bn", source="FRED H41RESPPALDKNWW (H.4.1)",
         frequency="weekly", fetch=_bn("H41RESPPALDKNWW"), binary=1.0,
         note="Program closed to new loans Mar-2024; kept as a template for BTFP-type facilities."),
    dict(id="deposits_13w", label="Commercial bank deposits, 13w % change", group="bank_funding",
         regimes=[FC], direction=-1, units="%", source="FRED DPSACBW027SBOG (H.8)",
         frequency="weekly", fetch=lambda: pct_change_days(fred("DPSACBW027SBOG"), 91)),
    dict(id="mmf_assets_yoy", label="Money market fund investments, YoY %", group="bank_funding",
         regimes=[FC], direction=1, units="%", source="OFR MMF Monitor (MMF-MMF_TOT-M)",
         frequency="monthly", fetch=lambda: ofr("MMF-MMF_TOT-M").pct_change(12).mul(100).dropna(),
         note="Surging MMF growth signals deposit flight / flight to safety."),

    # Sovereign functioning
    dict(id="ust_liquidity", label="UST bid-ask / market depth", group="sovereign_functioning",
         regimes=[MF], direction=1, units="ticks / $mm", source="BrokerTec/Bloomberg (paid)",
         frequency="daily", stub=True, proxy="move", note="Paid data; MOVE is the closest free proxy."),
    dict(id="curve_noise_hpw", label="Hu-Pan-Wang yield-curve noise", group="sovereign_functioning",
         regimes=[MF], direction=1, units="bp", source="CRSP-based (no free feed)",
         frequency="daily", stub=True, proxy="curve_fit_rmse",
         note="No public daily feed; curve_fit_rmse is a free Nelson-Siegel proxy."),
    dict(id="curve_fit_rmse", label="Nelson-Siegel fit error on CMT curve",
         group="sovereign_functioning", regimes=[MF], direction=1, units="bp",
         source="FRED DGS1MO..DGS30 (computed)", frequency="daily", fetch=curve_fit_rmse,
         proxy_for="curve_noise_hpw",
         note="RMSE of a daily 3-factor Nelson-Siegel fit; CMT yields are pre-smoothed so this "
              "understates true dislocations and partly reflects curve shape."),
    dict(id="swap_spreads", label="10y/30y SOFR swap spreads", group="sovereign_functioning",
         regimes=[MF, SF], direction=-1, units="bp", source="Bloomberg/ICE (paid)",
         frequency="daily", stub=True,
         note="FRED swap rates (DSWP*) were discontinued in 2016; no free proxy."),
    dict(id="cftc_lev_net", label="CFTC leveraged-fund UST futures net (10y-equiv)",
         group="sovereign_functioning", regimes=[MF, FC], direction=-1,
         units="k 10y-equiv contracts", source="CFTC TFF (publicreporting.cftc.gov gpe5-46if)",
         frequency="weekly", fetch=cftc_lev_net,
         note="Sum of 2Y/5Y/10Y/Ultra10/Bond/UltraBond net positions weighted by approx DV01 vs "
              "the 10y note (0.57/0.66/1/1.4/2.0/3.4). More negative = larger basis-trade shorts."),
    dict(id="pd_ust_net", label="Primary dealer net UST positions (ex-TIPS)",
         group="sovereign_functioning", regimes=[MF, SF], direction=1, units="$bn",
         source="NY Fed Primary Dealer Statistics (PDPOSGST-TOT)", frequency="weekly",
         fetch=lambda: nyfed_pd("PDPOSGST-TOT") / 1000.0,
         note="Higher = dealer balance sheets absorbing more supply."),
    dict(id="move", label="MOVE index", group="sovereign_functioning", regimes=[MF, SF],
         direction=1, units="index", source="yfinance ^MOVE", frequency="daily",
         fetch=lambda: yf_close("^MOVE")),
    dict(id="term_premium_10y", label="10y term premium (Kim-Wright)",
         group="sovereign_functioning", regimes=[SF], direction=1, units="%",
         source="FRED THREEFYTP10", frequency="daily", fetch=lambda: fred("THREEFYTP10"),
         note="FRED carries the Kim-Wright model (not ACM; the ACM xls needs an extra parser)."),

    # Sovereign stress
    dict(id="it_de_10y", label="Italy - Germany 10y spread", group="sovereign_stress",
         regimes=[SF], direction=1, units="bp", source="FRED IRLTLT01ITM156N, IRLTLT01DEM156N (OECD)",
         frequency="monthly",
         fetch=lambda: spread(fred("IRLTLT01ITM156N"), fred("IRLTLT01DEM156N"), ffill_b=False),
         note="Monthly average (same source as the dashboard's OAT-Bund spread)."),
    dict(id="fr_de_10y", label="France - Germany 10y spread", group="sovereign_stress",
         regimes=[SF], direction=1, units="bp", source="FRED IRLTLT01FRM156N, IRLTLT01DEM156N (OECD)",
         frequency="monthly",
         fetch=lambda: spread(fred("IRLTLT01FRM156N"), fred("IRLTLT01DEM156N"), ffill_b=False)),
    dict(id="sovereign_cds", label="Sovereign CDS (US/IT/FR/JP)", group="sovereign_stress",
         regimes=[SF], direction=1, units="bp", source="Markit (paid)", frequency="daily",
         stub=True, proxy="sovciss", note="Paid data; ECB SovCISS is the free composite proxy."),
    dict(id="target2_it", label="TARGET2 balance: Banca d'Italia", group="sovereign_stress",
         regimes=[SF, FC], direction=-1, units="€bn", source="ECB Data Portal TGB",
         frequency="monthly", fetch=lambda: ecb("TGB/M.IT.N.A094T.U2.EUR.E") / 1000.0,
         note="More negative = larger liabilities (capital flight from Italy)."),
    dict(id="target2_de", label="TARGET2 balance: Bundesbank", group="sovereign_stress",
         regimes=[SF, FC], direction=1, units="€bn", source="ECB Data Portal TGB",
         frequency="monthly", fetch=lambda: ecb("TGB/M.DE.N.A094T.U2.EUR.E") / 1000.0),

    # Credit
    dict(id="ig_oas", label="ICE BofA US IG OAS", group="credit", regimes=[CS], direction=1,
         units="bp", source="FRED BAMLC0A0CM", frequency="daily",
         fetch=lambda: fred("BAMLC0A0CM") * 100, note="FRED limits ICE data to ~3y of history."),
    dict(id="hy_oas", label="ICE BofA US HY OAS", group="credit", regimes=[CS], direction=1,
         units="bp", source="FRED BAMLH0A0HYM2", frequency="daily",
         fetch=lambda: fred("BAMLH0A0HYM2") * 100, note="FRED limits ICE data to ~3y of history."),
    dict(id="ccc_oas", label="ICE BofA US CCC OAS", group="credit", regimes=[CS], direction=1,
         units="bp", source="FRED BAMLH0A3HYC", frequency="daily",
         fetch=lambda: fred("BAMLH0A3HYC") * 100, note="FRED limits ICE data to ~3y of history."),
    dict(id="lev_loans", label="Leveraged loans: BKLN drawdown from 1y high", group="credit",
         regimes=[CS], direction=-1, units="%", source="yfinance BKLN", frequency="daily",
         fetch=lambda: drawdown_1y(yf_close("BKLN")), proxy_for="lev_loan_index",
         note="Total-return-adjusted ETF drawdown; proxy for the Morningstar LSTA index."),
    dict(id="lev_loan_index", label="Morningstar LSTA leveraged loan index", group="credit",
         regimes=[CS], direction=-1, units="price", source="PitchBook/LCD (paid)",
         frequency="daily", stub=True, proxy="lev_loans", note="Paid data."),
    dict(id="issuance", label="IG/HY/loan issuance volumes", group="credit", regimes=[CS],
         direction=-1, units="$bn", source="LCD/Dealogic (paid)", frequency="weekly",
         stub=True, note="Paid data; no free proxy."),
    dict(id="sloos_ci", label="SLOOS: net % tightening C&I standards (large/mid)",
         group="credit", regimes=[CS, LT], direction=1, units="net %",
         source="FRED DRTSCILM", frequency="quarterly", fetch=lambda: fred("DRTSCILM")),
    dict(id="ecb_bls", label="ECB BLS: credit standards to enterprises (net %)",
         group="credit", regimes=[CS, LT], direction=1, units="net %",
         source="ECB Data Portal BLS (Q.U2.ALL.O.E.Z.B3.ST.S.WFNET)", frequency="quarterly",
         fetch=lambda: ecb("BLS/Q.U2.ALL.O.E.Z.B3.ST.S.WFNET")),
    dict(id="boc_slos", label="BoC Senior Loan Officer Survey", group="credit",
         regimes=[CS, LT], direction=1, units="balance of opinion",
         source="Bank of Canada (PDF/CSV, no stable API)", frequency="quarterly", stub=True,
         note="No stable machine-readable endpoint found."),

    # Cross-asset
    dict(id="vix_term", label="VIX / VIX3M", group="cross_asset", regimes=[MF, CS],
         direction=1, units="ratio", source="yfinance ^VIX, ^VIX3M", frequency="daily",
         fetch=lambda: (lambda df: (df.iloc[:, 0] / df.iloc[:, 1]))(
             pd.concat([yf_close("^VIX"), yf_close("^VIX3M")], axis=1).dropna()),
         note="> 1 = backwardation (acute stress)."),
    dict(id="vix", label="VIX", group="cross_asset", regimes=[MF, CS], direction=1,
         units="index", source="yfinance ^VIX", frequency="daily", fetch=lambda: yf_close("^VIX")),
    dict(id="stock_bond_corr", label="63d SPY-TLT return correlation", group="cross_asset",
         regimes=[SF, MF], direction=1, units="corr", source="yfinance SPY, TLT (computed)",
         frequency="daily", fetch=stock_bond_corr,
         note="Positive = bonds no longer hedge equities (inflation/fiscal regime)."),
    dict(id="broad_dollar", label="Broad trade-weighted dollar, 3m % change", group="cross_asset",
         regimes=[FC, LT], direction=1, units="%", source="FRED DTWEXBGS",
         frequency="daily", fetch=lambda: ret_n(fred("DTWEXBGS")),
         note="Rapid dollar appreciation tightens global dollar funding."),
    dict(id="usdjpy", label="USD/JPY, 3m % change", group="cross_asset", regimes=[LT, MF], direction=-1,
         units="%", source="FRED DEXJPUS", frequency="daily", fetch=lambda: ret_n(fred("DEXJPUS")),
         note="Direction -1: sharp yen appreciation = carry-trade unwind. Extreme yen weakness "
              "(Japan fiscal stress) is not captured by this sign."),
    dict(id="em_oas", label="ICE BofA EM corporate OAS", group="cross_asset", regimes=[CS, FC],
         direction=1, units="bp", source="FRED BAMLEMCBPIOAS", frequency="daily",
         fetch=lambda: fred("BAMLEMCBPIOAS") * 100, note="FRED limits ICE data to ~3y of history."),
    dict(id="em_fx", label="EM currencies (CEW ETF), 3m % change", group="cross_asset",
         regimes=[FC], direction=-1, units="%", source="yfinance CEW", frequency="daily",
         fetch=lambda: ret_n(yf_close("CEW"))),
    dict(id="gold", label="Gold (front-month future), 3m % change", group="cross_asset",
         regimes=[SF], direction=1, units="%", source="yfinance GC=F", frequency="daily",
         fetch=lambda: ret_n(yf_close("GC=F")), note="Safe-haven / debasement bid momentum."),

    # Macro
    dict(id="t10y2y", label="2s10s Treasury curve", group="macro", regimes=[LT], direction=-1,
         units="bp", source="FRED T10Y2Y", frequency="daily", fetch=lambda: fred("T10Y2Y") * 100,
         note="Inversion = tight policy. Note bear/bull re-steepening after inversion often "
              "coincides with stress onset."),
    dict(id="t10y3m", label="3m10y Treasury curve", group="macro", regimes=[LT], direction=-1,
         units="bp", source="FRED T10Y3M", frequency="daily", fetch=lambda: fred("T10Y3M") * 100),
    dict(id="real_10y", label="10y TIPS real yield", group="macro", regimes=[LT, SF],
         direction=1, units="%", source="FRED DFII10", frequency="daily", fetch=lambda: fred("DFII10")),
    dict(id="breakeven_10y", label="10y breakeven inflation", group="macro", regimes=[SF],
         direction=1, units="%", source="FRED T10YIE", frequency="daily", fetch=lambda: fred("T10YIE")),
    dict(id="fwd_5y5y", label="5y5y forward inflation", group="macro", regimes=[SF],
         direction=1, units="%", source="FRED T5YIFR", frequency="daily", fetch=lambda: fred("T5YIFR")),
    dict(id="fed_bs_13w", label="Fed balance sheet, 13w change", group="macro",
         regimes=[LT, PI], direction=-1, units="$bn", source="FRED WALCL", frequency="weekly",
         fetch=lambda: (lambda s: (s - s.reindex(s.index - pd.Timedelta(days=91), method="ffill")
                                   .set_axis(s.index)).dropna())(fred("WALCL") / 1000.0),
         note="Negative = QT (liquidity drain). Large positive spikes flag policy intervention "
              "(read with direction reversed for policy_intervention)."),

    # Leverage
    dict(id="hf_leverage", label="Hedge fund gross leverage (top-10 funds, avg)", group="leverage",
         regimes=[MF, FC], direction=1, units="x", source="OFR Hedge Fund Monitor (Form PF)",
         frequency="quarterly", fetch=lambda: ofr("FPF-ALLQHF_GAVN10_LEVERAGERATIO_AVERAGE", "hf")),
    dict(id="hf_repo_borrowing", label="Hedge fund repo borrowing", group="leverage",
         regimes=[MF, FC], direction=1, units="$bn", source="OFR Hedge Fund Monitor (FPF-BORROW_REPO_SUM)",
         frequency="quarterly", fetch=lambda: ofr("FPF-BORROW_REPO_SUM", "hf") / 1e9),
    dict(id="sponsored_repo", label="FICC sponsored repo volume", group="leverage",
         regimes=[MF, FC], direction=1, units="$bn", source="OFR Hedge Fund Monitor (FICC-SPONSORED_REPO_VOL)",
         frequency="daily", fetch=lambda: ofr("FICC-SPONSORED_REPO_VOL", "hf") / 1e9,
         note="Proxy for basis-trade repo financing."),
    dict(id="margin_debt_yoy", label="FINRA margin debt, YoY %", group="leverage",
         regimes=[MF, CS], direction=1, units="%", source="FINRA margin statistics (xlsx)",
         frequency="monthly", fetch=finra_margin),

    # Composites
    dict(id="ofr_fsi", label="OFR Financial Stress Index", group="composite",
         regimes=[FC, MF, CS], direction=1, units="index", source="OFR (financialresearch.gov)",
         frequency="daily", fetch=ofr_fsi),
    dict(id="stlfsi", label="St. Louis Fed Financial Stress Index", group="composite",
         regimes=[FC, MF, CS], direction=1, units="index", source="FRED STLFSI4",
         frequency="weekly", fetch=lambda: fred("STLFSI4")),
    dict(id="nfci", label="Chicago Fed NFCI", group="composite", regimes=[LT, CS, FC],
         direction=1, units="index", source="FRED NFCI", frequency="weekly",
         fetch=lambda: fred("NFCI")),
    dict(id="ciss", label="ECB CISS (euro area)", group="composite", regimes=[FC, MF, CS],
         direction=1, units="index", source="ECB Data Portal CISS (D.U2.Z0Z.4F.EC.SS_CIN.IDX)",
         frequency="daily", fetch=lambda: ecb("CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX")),
    dict(id="sovciss", label="ECB SovCISS (euro area, GDP-weighted)", group="composite",
         regimes=[SF], direction=1, units="index",
         source="ECB Data Portal CISS (D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX)", frequency="daily",
         fetch=lambda: ecb("CISS/D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX")),
]

# ── statistics ──────────────────────────────────────────────────────────────
PERIODS_PER_YEAR = {"daily": 252, "weekly": 52, "monthly": 12, "quarterly": 4}
STALE_DAYS = {"daily": 14, "weekly": 30, "monthly": 100, "quarterly": 220}


def _f(x, sig: int = 5):
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(x):
        return None
    return float(f"{x:.{sig}g}")


def _asof(s: pd.Series, d: pd.Timestamp):
    sub = s[s.index <= d]
    return sub.iloc[-1] if len(sub) else None


def _diff(a, b):
    return None if a is None or b is None else a - b


def compute_stats(s: pd.Series, direction: int, frequency: str) -> dict:
    t = s.index[-1]
    s10 = s[s.index > t - pd.DateOffset(years=STATS_YEARS)]
    latest = float(s10.iloc[-1])
    # midrank percentile so long runs of identical values (e.g. zero usage) don't inflate it
    pct = float(((s10 < latest).mean() + 0.5 * (s10 == latest).mean()) * 100)
    s3 = s10[s10.index > t - pd.DateOffset(years=Z_YEARS)]
    z = None
    if len(s3) >= 12 and s3.std() > 0:
        z = (latest - s3.mean()) / s3.std()
    v7, v30, v60, v91 = (_asof(s10, t - pd.Timedelta(days=d)) for d in (7, 30, 60, 91))
    chg_1m = _diff(latest, v30)
    prev_1m = _diff(v30, v60)
    dir_pct = pct if direction > 0 else 100 - pct
    dir_z = None if z is None else z * direction
    out = {
        "z_3y": _f(z, 4), "pct_full": _f(pct, 4), "dir_pct": _f(dir_pct, 4),
        "dir_z": _f(dir_z, 4),
        "stress_zone": bool(dir_pct >= 90 or (dir_z is not None and dir_z >= 2)),
        "chg_1w": _f(_diff(latest, v7)) if frequency in ("daily", "weekly") else None,
        "chg_1m": _f(chg_1m), "chg_3m": _f(_diff(latest, v91)),
        "accel_1m": _f(_diff(chg_1m, prev_1m)),
        "n_obs": int(len(s10)), "history_start": s10.index[0].strftime("%Y-%m-%d"),
    }
    # early-warning stats on native-frequency changes
    w = PERIODS_PER_YEAR.get(frequency, 252)
    d = s10.diff().dropna()
    ew = dict(var_1y=None, ac1_1y=None, var_1y_6m_ago=None, ac1_1y_6m_ago=None,
              var_trend=None, ac1_trend=None, ew_signal=None)
    if w >= 20 and len(d) >= w + 5:
        var = d.rolling(w).var()
        ac1 = d.rolling(w).corr(d.shift(1)).replace([np.inf, -np.inf], np.nan)
        t6 = t - pd.Timedelta(days=182)
        vn, v6 = var.iloc[-1], _asof(var.dropna(), t6)
        an, a6 = ac1.iloc[-1], _asof(ac1.dropna(), t6)
        ew.update(var_1y=_f(vn), ac1_1y=_f(an, 4), var_1y_6m_ago=_f(v6), ac1_1y_6m_ago=_f(a6, 4))
        if v6 is not None and np.isfinite(vn) and v6 > 0:
            r = vn / v6
            ew["var_trend"] = "rising" if r > 1.25 else "falling" if r < 0.8 else "flat"
        if a6 is not None and np.isfinite(an) and np.isfinite(a6):
            dd = an - a6
            ew["ac1_trend"] = "rising" if dd >= 0.1 else "falling" if dd <= -0.1 else "flat"
        if ew["var_trend"] and ew["ac1_trend"]:
            ew["ew_signal"] = ew["var_trend"] == "rising" and ew["ac1_trend"] == "rising"
    out.update(ew)
    return out


def binary_flags(s: pd.Series, threshold: float) -> dict:
    t = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None).date())
    recent = s[s.index > t - pd.Timedelta(days=30)]
    prior = s[(s.index <= t - pd.Timedelta(days=30)) & (s.index > t - pd.Timedelta(days=120))]
    first = bool(len(recent) and recent.max() > threshold and len(prior) and prior.max() <= threshold)
    return {"first_use": first, "active": bool(s.iloc[-1] > threshold), "threshold": threshold}


def turn_driven(s: pd.Series, threshold: float) -> bool | None:
    """True if every above-threshold day in the last 30 days sits within
    2 business days of a month end (statement-date window dressing, not stress)."""
    t = pd.Timestamp(datetime.now(timezone.utc).date())
    hits = s[(s.index > t - pd.Timedelta(days=30)) & (s > threshold)]
    if hits.empty:
        return None
    bme = pd.date_range(t - pd.Timedelta(days=90), t + pd.Timedelta(days=40), freq="BME")
    def near(d):
        return any(abs(len(pd.bdate_range(min(d, m), max(d, m))) - 1) <= 2 for m in bme)
    return bool(all(near(d) for d in hits.index))


def history(s: pd.Series, frequency: str) -> list:
    t = s.index[-1]
    h = s[s.index > t - pd.DateOffset(years=HISTORY_YEARS)]
    if frequency == "daily" and len(h) > 200:
        last = h.iloc[[-1]]
        h = h.resample("W-FRI").last().dropna()
        h = h[h.index < last.index[0]]
        h = pd.concat([h, last])  # keep the true latest observation
    return [[d.strftime("%Y-%m-%d"), _f(v)] for d, v in h.items()]


# ── assembly ────────────────────────────────────────────────────────────────

def _base(spec: dict) -> dict:
    return {
        "id": spec["id"], "label": spec["label"], "group": spec["group"],
        "regimes": spec["regimes"], "direction": spec["direction"], "units": spec["units"],
        "source": spec["source"], "frequency": spec["frequency"], "status": "stub",
        "note": spec.get("note", ""), "proxy_for": spec.get("proxy_for"),
        "proxy": spec.get("proxy"), "binary": "binary" in spec, "as_of": None,
        "stale": None, "latest": None, "history": [], "stats": None, "flags": None,
    }


def build_one(spec: dict) -> dict:
    rec = _base(spec)
    if spec.get("stub"):
        return rec
    t0 = time.time()
    try:
        s = spec["fetch"]()
        s = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        s = s[~s.index.duplicated(keep="last")].sort_index()
        if s.empty:
            raise ValueError("empty series")
        t = s.index[-1]
        rec.update(
            status="ok", as_of=t.strftime("%Y-%m-%d"), latest=_f(s.iloc[-1]),
            stale=bool((pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None).date()) - t).days
                       > STALE_DAYS.get(spec["frequency"], 30)),
            history=history(s, spec["frequency"]),
            stats=compute_stats(s, spec["direction"], spec["frequency"]),
        )
        if "binary" in spec:
            rec["flags"] = binary_flags(s, spec["binary"])
            if spec["id"] == "srf_usage":
                rec["flags"]["turn_driven"] = turn_driven(s, spec["binary"])
        elif spec["id"] == "stock_bond_corr" and len(s) > 22:
            rec["flags"] = {"sign_flip": bool(np.sign(s.iloc[-1]) != np.sign(s.iloc[-22]))}
    except Exception as exc:
        logger.warning("indicator %s failed: %s", spec["id"], exc)
        rec.update(status="error", note=(rec["note"] + " | " if rec["note"] else "")
                   + f"error: {type(exc).__name__}: {str(exc)[:200]}")
    logger.debug("%s %s in %.1fs", spec["id"], rec["status"], time.time() - t0)
    return rec


def build_indicators() -> dict:
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        recs = list(ex.map(build_one, CATALOG))
    counts = {k: sum(r["status"] == k for r in recs) for k in ("ok", "error", "stub")}
    payload = {
        "generated_at": now_iso(),
        "sources": [source(f"{r['label']} [{r['id']}]", r["status"], r["as_of"], r["note"])
                    for r in recs],
        "summary": {
            **counts, "total": len(recs),
            "in_stress_zone": [r["id"] for r in recs if r["stats"] and r["stats"]["stress_zone"]],
            "first_use": [r["id"] for r in recs if r["flags"] and r["flags"].get("first_use")],
            "early_warning": [r["id"] for r in recs if r["stats"] and r["stats"].get("ew_signal")],
            "runtime_s": round(time.time() - t0, 1),
        },
        "groups": GROUPS, "regimes": REGIMES, "rules": STATS_RULES,
        "indicators": recs,
    }
    dump("indicators.json", payload)
    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = build_indicators()
    s = p["summary"]
    print(f"ok={s['ok']} error={s['error']} stub={s['stub']} runtime={s['runtime_s']}s")
    for r in p["indicators"]:
        if r["status"] != "ok":
            print(f"  {r['status']:5} {r['id']}: {r['note'][:160]}")
    print("stress zone:", s["in_stress_zone"])
    print("first use:", s["first_use"], "| early warning:", s["early_warning"])
