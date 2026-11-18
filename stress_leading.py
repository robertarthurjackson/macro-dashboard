"""Early-warning panel for the Stress & Auctions tab -> site/data/stress/leading.json.

Five slow "quantity and dispersion" tells that tend to move before the price
indicators in indicators.json do. They are deliberately kept OUT of the regime
scores (regime.json): none has a validated lead, and adding them would change
score history. Treat the panel as a watch list, not a signal.

  sofr_above_iorb   share of ordinary (ex month-end) days SOFR printed above IORB
  dealer_takedown   primary-dealer share of US nominal coupon auctions (3m, size-weighted)
  bill_sofr         1m T-bill yield minus SOFR (20d mean)
  ccc_hy_ratio      CCC OAS / HY index OAS
  btc_rel_spx       BTC 3m return minus S&P 500 3m return

Follows the stress_common contract: one failing series never fails the build.
"""
import logging
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import stress_indicators as si
from stress_common import dump, http_get, now_iso, source

logger = logging.getLogger("stress.leading")

OUT = "leading.json"
HISTORY_START = "2018-06-01"
TURN_BDAYS = 2          # business days either side of a month end treated as "turn"
SHARE_WINDOW = 60       # ordinary business days
TAKEDOWN_DAYS = 91
FISCAL_AUCTIONS = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
                   "/v1/accounting/od/auctions_query")
TD_AUCTIONED = "https://www.treasurydirect.gov/TA_WS/securities/auctioned"
REFS = (("before Sep 2019 repo spike", "2019-09-13"),
        ("before Mar 2020 dash for cash", "2020-02-21"))


# ── series builders ─────────────────────────────────────────────────────────

def _ex_turn(s: pd.Series) -> pd.Series:
    """Drop observations within TURN_BDAYS business days of a month end."""
    days = s.index.values.astype("datetime64[D]")
    bme = pd.date_range(s.index[0] - pd.Timedelta(days=45), s.index[-1] + pd.Timedelta(days=45),
                        freq="BME").values.astype("datetime64[D]")
    pos = np.clip(np.searchsorted(bme, days), 1, len(bme) - 1)
    dist = np.minimum(np.abs(np.busday_count(bme[pos - 1], days)),
                      np.abs(np.busday_count(days, bme[pos])))
    return s[dist > TURN_BDAYS]


def sofr_above_iorb() -> pd.Series:
    d = _ex_turn(si.spread(si.fred("SOFR"), si.iorb_spliced()))
    return (d > 0).astype(float).rolling(SHARE_WINDOW).mean().mul(100).dropna()


def _num(x):
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _fiscaldata_coupons() -> list[dict]:
    fields = ("auction_date,security_type,cusip,total_accepted,indirect_bidder_accepted,"
              "direct_bidder_accepted,primary_dealer_accepted,inflation_index_security,"
              "floating_rate")
    rows = http_get(FISCAL_AUCTIONS, timeout=60, params={
        "filter": "auction_date:gte:2014-01-01,security_type:in:(Note,Bond)",
        "fields": fields, "page[size]": 10000, "sort": "-auction_date"}).json()["data"]
    out = []
    for x in rows:
        if x.get("inflation_index_security") == "Yes" or x.get("floating_rate") == "Yes":
            continue
        out.append(dict(date=x["auction_date"], cusip=x["cusip"],
                        pd=_num(x.get("primary_dealer_accepted")),
                        direct=_num(x.get("direct_bidder_accepted")),
                        indirect=_num(x.get("indirect_bidder_accepted")),
                        size=_num(x.get("total_accepted"))))
    return out


def _treasurydirect_coupons() -> list[dict]:
    """Recent results straight from TreasuryDirect (capped at 250 per type), which
    keeps the series current when the FiscalData dataset lags."""
    out = []
    for typ in ("Note", "Bond"):
        rows = http_get(TD_AUCTIONED, timeout=90,
                        params={"format": "json", "type": typ, "days": 1500}).json()
        for x in rows:
            if x.get("floatingRate") == "Yes" or x.get("referenceCpiOnIssueDate"):
                continue
            date = (x.get("competitiveAuctionCloseDate") or "")[:10]
            if not date:
                continue
            pd_, direct, indirect = (_num(x.get(k)) for k in (
                "primaryDealersCompetitiveParAwarded", "directBiddersCompetitiveParAwarded",
                "indirectBiddersCompetitiveParAwarded"))
            comp = sum(v for v in (pd_, direct, indirect) if v is not None)
            out.append(dict(date=date, cusip=x.get("cusip"), pd=pd_, direct=direct,
                            indirect=indirect, size=comp or None))
    return out


def dealer_takedown() -> tuple[pd.Series, str]:
    """Size-weighted primary-dealer share of competitive awards at nominal coupon
    auctions over the trailing TAKEDOWN_DAYS, evaluated at each auction date."""
    recs, notes = {}, []
    for name, fn in (("FiscalData", _fiscaldata_coupons), ("TreasuryDirect", _treasurydirect_coupons)):
        try:
            rows = fn()
            for r in rows:  # later source wins: TreasuryDirect is the fresher one
                recs[(r["cusip"], r["date"])] = r
            notes.append(f"{name} {len(rows)} auctions to {max(r['date'] for r in rows)}")
        except Exception as exc:
            notes.append(f"{name} failed: {type(exc).__name__}")
            logger.warning("dealer_takedown source %s failed: %s", name, exc)
    rows = []
    for r in recs.values():
        parts = [r["pd"], r["direct"], r["indirect"]]
        if any(v is None for v in parts) or sum(parts) <= 0:
            continue  # announced but not yet auctioned, or incomplete
        rows.append((pd.Timestamp(r["date"]), r["pd"], sum(parts)))
    if not rows:
        raise ValueError("no auction results from either source; " + "; ".join(notes))
    df = pd.DataFrame(rows, columns=["date", "pd", "comp"]).groupby("date").sum().sort_index()
    roll = df.rolling(f"{TAKEDOWN_DAYS}D").sum()
    n = df["comp"].rolling(f"{TAKEDOWN_DAYS}D").count()
    s = (roll["pd"] / roll["comp"] * 100)[n >= 5].dropna()
    return s, "; ".join(notes)


def bill_sofr() -> pd.Series:
    return si.spread(si.fred("DGS1MO"), si.fred("SOFR")).rolling(20).mean().dropna()


def ccc_hy_ratio() -> pd.Series:
    df = pd.concat([si.fred("BAMLH0A3HYC"), si.fred("BAMLH0A0HYM2")], axis=1).dropna()
    return (df.iloc[:, 0] / df.iloc[:, 1]).dropna()


def btc_rel_spx() -> pd.Series:
    import yfinance as yf
    df = yf.download("BTC-USD", start=si.START, progress=False, auto_adjust=True)["Close"]
    btc = (df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df).dropna()
    btc.index = pd.to_datetime(btc.index).tz_localize(None)
    return si.rel_return(btc, si.yf_close("SPY"), 63)


def _state_pct(building: float = 75, elevated: float = 90):
    def rule(latest, st):
        p, z = st.get("dir_pct"), st.get("dir_z")
        if (p is not None and p >= elevated) or (z is not None and z >= 2):
            return "elevated"
        if (p is not None and p >= building) or (z is not None and z >= 1):
            return "building"
        return "quiet"
    return rule


def _state_abs(building: float, elevated: float):
    return lambda latest, st: ("elevated" if latest >= elevated
                               else "building" if latest >= building else "quiet")


PCT_RULE = "building if stress percentile >= 75 or 3y z >= 1; elevated if >= 90 or z >= 2"

CATALOG = [
    dict(id="sofr_above_iorb", label="Days SOFR above IORB (ex month-end)", units="% of days",
         direction=1, frequency="daily", fetch=sofr_above_iorb, state=_state_abs(20, 60),
         state_rule="building if >= 20% of days; elevated if >= 60%",
         source="FRED SOFR, IORB (IOER before 2021-07-29)",
         why="Repo clearing above the rate the Fed pays on reserves on ordinary days means "
             "cash is no longer abundant at the margin. Persistence matters more than level.",
         note=f"Share of the last {SHARE_WINDOW} business days, excluding days within "
              f"{TURN_BDAYS} business days of a month end, with SOFR strictly above IORB."),
    dict(id="dealer_takedown", label="Dealer takedown, US coupon auctions (3m)", units="%",
         direction=1, frequency="weekly", fetch=dealer_takedown, state=_state_pct(),
         state_rule=PCT_RULE,
         source="US Treasury FiscalData auctions_query + TreasuryDirect auctioned securities",
         why="A slow creep says dealers are absorbing what end buyers will not, which loads "
             "balance sheets before any single auction looks bad.",
         note=f"Primary-dealer share of competitive awards, nominal notes and bonds only "
              f"(no TIPS/FRN), size-weighted over the trailing {TAKEDOWN_DAYS} days. The share "
              "has drifted lower for a decade, so read the 3y z and the change, not the level."),
    dict(id="bill_sofr", label="1m T-bill yield - SOFR (20d mean)", units="bp",
         direction=1, frequency="daily", fetch=bill_sofr, state=_state_pct(),
         state_rule=PCT_RULE, source="FRED DGS1MO, SOFR",
         why="With ON RRP empty, money funds can only absorb bill supply by pulling cash "
             "from repo, so bills cheapen against SOFR before repo itself moves.",
         note="Also rises when a hike is priced inside the next month, so check the fed funds "
              "curve before reading a move as supply pressure. Sharp falls are flight to quality."),
    dict(id="ccc_hy_ratio", label="CCC OAS / HY index OAS", units="x",
         direction=1, frequency="daily", fetch=ccc_hy_ratio, state=_state_pct(),
         state_rule=PCT_RULE, source="FRED BAMLH0A3HYC, BAMLH0A0HYM2",
         why="The weakest credits lose market access before the index reprices; a rising "
             "ratio is the tail breaking while the headline spread still looks calm.",
         note="FRED limits ICE data to ~3y of history, so the percentile window is short."),
    dict(id="btc_rel_spx", label="BTC vs S&P 500, 3m relative return", units="%",
         direction=-1, frequency="daily", fetch=btc_rel_spx, state=_state_pct(),
         state_rule=PCT_RULE, source="yfinance BTC-USD, SPY (total return)",
         why="The most liquidity-sensitive asset tends to be sold first when cash gets "
             "scarce, while equities still hold up.",
         note="63-trading-day log return of BTC minus SPY. Very noisy; BTC has its own cycle."),
]


# ── assembly ────────────────────────────────────────────────────────────────

def _history(s: pd.Series, frequency: str) -> list:
    h = s[s.index >= HISTORY_START]
    if frequency == "daily" and len(h) > 300:
        last = h.iloc[[-1]]
        h = h.resample("W-FRI").last().dropna()
        h = pd.concat([h[h.index < last.index[0]], last])
    return [[d.strftime("%Y-%m-%d"), si._f(v)] for d, v in h.items()]


def _refs(s: pd.Series) -> list:
    out = []
    for label, d in REFS:
        t = pd.Timestamp(d)
        w = s[(s.index <= t) & (s.index > t - pd.Timedelta(days=21))]
        if len(w):
            out.append({"label": label, "date": w.index[-1].strftime("%Y-%m-%d"),
                        "value": si._f(w.iloc[-1])})
    return out


def build_one(spec: dict) -> dict:
    rec = {k: spec[k] for k in ("id", "label", "units", "direction", "frequency", "source",
                                "why", "note", "state_rule")}
    rec.update(status="error", as_of=None, stale=None, latest=None, state=None, stats=None,
               chg_6m=None, refs=[], history=[])
    try:
        got = spec["fetch"]()
        s, extra = got if isinstance(got, tuple) else (got, "")
        s = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        s = s[~s.index.duplicated(keep="last")].sort_index()
        if s.empty:
            raise ValueError("empty series")
        t, latest = s.index[-1], float(s.iloc[-1])
        stats = si.compute_stats(s, spec["direction"], spec["frequency"])
        today = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None).date())
        rec.update(
            status="ok", as_of=t.strftime("%Y-%m-%d"), latest=si._f(latest),
            stale=bool((today - t).days > si.STALE_DAYS.get(spec["frequency"], 30)),
            state=spec["state"](latest, stats), stats=stats,
            chg_6m=si._f(si._diff(latest, si._asof(s, t - pd.Timedelta(days=182)))),
            refs=_refs(s), history=_history(s, spec["frequency"]),
        )
        if extra:
            rec["source_detail"] = extra
    except Exception as exc:
        logger.warning("leading %s failed: %s", spec["id"], exc)
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    return rec


def build_leading() -> dict:
    t0 = time.time()
    recs = [build_one(spec) for spec in CATALOG]
    ok = [r for r in recs if r["status"] == "ok"]
    by_state = {st: [r["id"] for r in ok if r["state"] == st]
                for st in ("elevated", "building", "quiet")}
    payload = {
        "generated_at": now_iso(),
        "sources": [source(f"{r['label']} [{r['id']}]", r["status"], r["as_of"],
                           r.get("error") or r.get("source_detail") or "") for r in recs],
        "summary": {"ok": len(ok), "total": len(recs), **by_state,
                    "headline": f"{len(by_state['elevated'])} elevated, "
                                f"{len(by_state['building'])} building, "
                                f"{len(by_state['quiet'])} quiet of {len(ok)} live tells"},
        "method": {
            "purpose": "Slow quantity and dispersion measures that tend to move before price "
                       "indicators. Not part of the regime scores and not backtested: a watch "
                       "list for the build-up phase, when composite stress indices are still calm.",
            "state": "Per-series rule in state_rule. Percentile and z use the same definitions "
                     "as indicators.json (10y window where available, 3y z, stress direction).",
            "refs": "Reference readings are the last value on or before the listed date, for "
                    "comparison with the run-up to past funding events.",
            "history": f"From {HISTORY_START}; daily series downsampled to weekly (Friday).",
        },
        "series": recs,
        "runtime_s": round(time.time() - t0, 1),
    }
    dump(OUT, payload)
    return payload


if __name__ == "__main__":
    import fred_client  # noqa: F401  (loads FRED_API_KEY from .env)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    p = build_leading()
    print(p["summary"]["headline"])
    for r in p["series"]:
        st = r["stats"] or {}
        print(f"  {r['id']:16s} {r['status']:5s} {r['latest']} {r['units']} as of {r['as_of']} "
              f"state={r['state']} pct={st.get('dir_pct')} z={st.get('dir_z')} "
              f"3m={st.get('chg_3m')} 6m={r['chg_6m']} refs={[(x['date'], x['value']) for x in r['refs']]}"
              + (f" ERR {r['error']}" if r.get('error') else ""))
