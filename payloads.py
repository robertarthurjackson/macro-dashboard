"""Payload builders for the static site — mirrors the private dashboard's
FastAPI responses (api.py) so the extracted frontend works unchanged.
"""
from datetime import date, timedelta
from typing import Optional

import database as db
import forward_curves as _fc
import liquidity as _liq
import macro_collector


# ── shared helpers (ported from api.py) ───────────────────────────────────────

def _liq_filter_days(series, days):
    if days == 0 or not series:
        return series
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    return [(t, v) for t, v in series if t >= cutoff]


def _liq_to_obs(series):
    return [{"ts": t, "value": v} for t, v in series]


def _macro_obs(sid, days):
    rows = db.get_macro_series(sid, days=days if days else 30000)
    return [{"ts": r["ts"], "value": r["value"]} for r in rows]


# ── macro ─────────────────────────────────────────────────────────────────────

COMPUTED_SERIES = [
    "2s10s", "5s30s", "sofr_iorb", "reserves_gdp", "ample_reserves",
    "cpi_yoy", "pce_yoy", "core_cpi_yoy",
    "walcl_13w", "gld_flow_4w", "ibit_flow_4w",
    "rstar_market", "policy_gap", "rv10y_20d", "walcl_accel",
    "true_ie_gross", "true_ie_net", "interest_pct_rev", "avg_debt_rate",
    "srf_turn", "srf_stress",
]

COMPUTED_META = [
    ("2s10s",          "2s10s Spread",            "Global & Spreads",  "bps"),
    ("5s30s",          "5s30s Spread",            "Global & Spreads",  "bps"),
    ("sofr_iorb",      "SOFR − IORB",             "Global & Spreads",  "bps"),
    ("reserves_gdp",   "Bank Reserves / GDP",     "Fed Balance Sheet", "%"),
    ("ample_reserves", "Ample Reserves Indicator","Fed Balance Sheet", "%"),
    ("cpi_yoy",        "CPI (YoY)",               "Inflation",         "%"),
    ("core_cpi_yoy",   "Core CPI (YoY)",          "Inflation",         "%"),
    ("pce_yoy",        "Core PCE (YoY)",          "Inflation",         "%"),
    ("srf_stress",     "SRF Usage (ex-turn)",     "Money & Credit",    "$B"),
    ("rstar_market",   "R-star (market 5y5y real)","US Rates",         "%"),
    ("policy_gap",     "Policy Gap (real FF − r*)","US Rates",         "%"),
    ("true_ie_gross",  "True Interest Exp (Gromen, % receipts)", "Fiscal", "%"),
    ("true_ie_net",    "True Interest Exp (net payroll, % receipts)", "Fiscal", "%"),
    ("interest_pct_rev","Interest / Tax Receipts","Fiscal",            "%"),
    ("avg_debt_rate",  "Avg Rate on Federal Debt","Fiscal",            "%"),
    ("walcl_13w",      "Fed BS Growth (13W ann.)","Debasement Watch",  "%"),
    ("gld_flow_4w",    "GLD Flow (4W Δ tonnes)",  "Debasement Watch",  "%"),
    ("ibit_flow_4w",   "IBIT Flow (4W Δ BTC)",    "Debasement Watch",  "%"),
]

_SKIP_IN_OVERVIEW = {"CPIAUCNS", "PCEPILFE", "CPILFENS", "GDP", "TLAACBW027SBOG",
                     "W006RC1Q027SBEA", "W823RC1Q027SBEA", "W824RC1Q027SBEA",
                     "W729RC1Q027SBEA", "W826RC1Q027SBEA", "W780RC1Q027SBEA",
                     "GFDEBTN"}


def _build_indicator(meta: dict, rows: list[dict]) -> Optional[dict]:
    if not rows:
        return None
    today = date.today()
    ytd_anchor = date(today.year, 1, 1).isoformat()
    latest = rows[-1]
    latest_dt = date.fromisoformat(latest["ts"])
    prior = rows[-2] if len(rows) >= 2 else None
    month_ago = next((r for r in reversed(rows[:-1])
                      if (latest_dt - date.fromisoformat(r["ts"])).days >= 30), None)
    ytd = next((r for r in rows if r["ts"] >= ytd_anchor), None)
    return {
        **meta,
        "latest_ts": latest["ts"],
        "latest_value": latest["value"],
        "change_1d":  (latest["value"] - prior["value"])     if prior     else None,
        "change_30d": (latest["value"] - month_ago["value"]) if month_ago else None,
        "change_ytd": (latest["value"] - ytd["value"])       if ytd       else None,
    }


def build_macro_overview(index_rows: dict[str, list[dict]]) -> dict:
    """index_rows: {"VIX": [{ts,value}...], "VVIX": [...]} from yfinance."""
    out: list[dict] = []
    for meta in db.list_macro_metadata():
        if meta["series_id"] in _SKIP_IN_OVERVIEW:
            continue
        rows = db.get_macro_series(meta["series_id"])
        ind = _build_indicator(meta, rows)
        if ind:
            out.append(ind)
    for sid, name, cat, units in COMPUTED_META:
        try:
            rows = macro_collector.get_computed_series(sid)
        except ValueError:
            continue
        ind = _build_indicator(
            {"series_id": sid, "display_name": name, "category": cat,
             "units": units, "frequency": "daily", "source": "computed"}, rows)
        if ind:
            out.append(ind)
    for sym in ("VIX", "VVIX"):
        ind = _build_indicator(
            {"series_id": sym, "display_name": sym, "category": "Commodities & Vol",
             "units": "Index", "frequency": "daily", "source": "yfinance"},
            index_rows.get(sym, []))
        if ind:
            out.append(ind)
    return {"indicators": out}


def build_series(sid: str) -> dict:
    if sid.lower() in {s.lower() for s in COMPUTED_SERIES}:
        rows = macro_collector.get_computed_series(sid)
        return {"series_id": sid, "computed": True, "observations": rows, "count": len(rows)}
    rows = db.get_macro_series(sid)
    return {"series_id": sid, "computed": False, "observations": rows, "count": len(rows)}


# ── forward curves ────────────────────────────────────────────────────────────

def build_fwd_treasury() -> dict:
    return _fc.compute_forward_treasury_curves(None)


def build_fwd_fedfunds() -> dict:
    import cme_client
    rows = cme_client.get_fed_funds_settlements()
    return {
        "as_of": date.today().isoformat() if rows else None,
        "curve": rows,
        "fomc_meetings": _fc.FOMC_MEETINGS,
        "source": "yfinance",
    }


# ── liquidity (ported from api.py) ────────────────────────────────────────────

def build_liq_headline(days: int) -> dict:
    composite_raw = _liq.compute_global_liquidity_composite()
    smoothed_raw = _liq.compute_global_liquidity_smoothed()
    yoy_raw = _liq.compute_cycle_yoy(smoothed_raw)
    zscore_raw = _liq.compute_cycle_zscore(smoothed_raw)
    regime = _liq.classify_regime(smoothed_raw)
    current_yoy = yoy_raw[-1][1] if yoy_raw else None
    return {
        "composite": _liq_to_obs(_liq_filter_days(composite_raw, days)),
        "smoothed":  _liq_to_obs(_liq_filter_days(smoothed_raw, days)),
        "yoy":       _liq_to_obs(_liq_filter_days(yoy_raw, days)),
        "zscore":    _liq_to_obs(_liq_filter_days(zscore_raw, days)),
        "regime":    regime,
        "current_yoy": round(current_yoy, 2) if current_yoy is not None else None,
        "btc":  _macro_obs("BTC-USD", days),
        "spx":  _macro_obs("SPX", days),
        "gold": _macro_obs("GOLD", days),
    }


def build_liq_subindex(name: str, days: int) -> dict:
    if name == "cb":
        series = _liq.compute_cb_liquidity()
        ids = ["WALCL", "ECBASSETSW", "JPNASSETS"]
        components = {sid: _macro_obs(sid, days) for sid in ids}
        component_names = {"WALCL": "Fed Balance Sheet (WALCL)",
                           "ECBASSETSW": "ECB Assets (EUR→USD)",
                           "JPNASSETS": "BoJ Assets (JPY→USD)"}
        weight = 0.30
    elif name == "private":
        series = _liq.compute_private_liquidity()
        ids = ["M2SL", "TOTLL", "MMMFFAQ027S", "COMPOUT", "RRPONTSYD",
               "RPONTSYD", "BOGZ1FL892090005Q"]
        components = {sid: _macro_obs(sid, days) for sid in ids}
        component_names = {"M2SL": "M2 Money Supply", "TOTLL": "Total Bank Loans",
                           "MMMFFAQ027S": "MMF Total Assets", "COMPOUT": "Commercial Paper",
                           "RRPONTSYD": "RRP Usage", "RPONTSYD": "SRF Usage",
                           "BOGZ1FL892090005Q": "Z.1 All-Sector Credit"}
        weight = 0.50
    else:
        series = _liq.compute_xborder_liquidity()
        components = {}
        component_names = {"TIC": "Foreign UST Holdings (TIC)",
                           "BIS": "BIS LBS Cross-Border Claims"}
        weight = 0.20
    filtered = _liq_filter_days(series, days)
    latest_val = series[-1][1] if series else None
    return {
        "name": name, "weight": weight,
        "series": _liq_to_obs(filtered),
        "latest_value_b": round(latest_val, 1) if latest_val is not None else None,
        "components": components,
        "component_names": component_names,
    }


def build_liq_quality(days: int) -> dict:
    def _latest(obs):
        return obs[-1]["value"] if obs else None
    term_premium = _macro_obs("THREEFYTP10", days)
    hy_oas = _macro_obs("BAMLH0A0HYM2", days)
    move = _macro_obs("MOVE", days)
    dxy = _macro_obs("DXY", days)
    offshore = _liq_to_obs(_liq_filter_days(_liq.compute_offshore_spread(), days))
    return {"series": {
        "term_premium":    {"data": term_premium, "label": "Term Premium (10Y ACM)", "units": "%", "latest": _latest(term_premium)},
        "dxy":             {"data": dxy, "label": "DXY", "units": "Index", "latest": _latest(dxy)},
        "move":            {"data": move, "label": "MOVE Index", "units": "Index", "latest": _latest(move)},
        "hy_oas":          {"data": hy_oas, "label": "HY OAS (ICE BofA)", "units": "%", "latest": _latest(hy_oas)},
        "offshore_spread": {"data": offshore, "label": "Offshore $ Spread (OBFR−SOFR)", "units": "bps", "latest": _latest(offshore)},
    }}
