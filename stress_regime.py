"""Regime monitor for the Stress & Auctions tab.

build_regime(indicators=None, calendar=None) -> dict, written to
site/data/stress/regime.json.  Input is the stress_indicators payload
(site/data/stress/indicators.json); if `indicators` is None the file is loaded,
and if it is missing stress_indicators.build_indicators() is called.

Schema
------
Top level:
  generated_at, indicators_generated_at, runtime_s
  sources      [{name, status, as_of, note}]  (stress_common contract)
  headline     {regime, label, level, score, overall_stress, normal_score,
                summary, as_of}
  regimes      [{id, label, score, score_3y, level, n_members, n_ok,
                 n_stress_zone, breadth, breadth_text, chg_1w, chg_1m,
                 trend_1w, trend_1m, trend, members: [{id, label, pct,
                 weight, contribution, stress_zone, note}], history_basis}]
  history      {dates: [...weekly...], <regime_id>: [score, ...], overall: [...]}
  what_moved   {moves: [{id, label, group, latest, units, change, horizon,
                         z_move, effect: bad|good, as_of}],
                events: [{type, id, label, text, severity, caveat?}]}
  early_warning {signals: [...], watch: [...]}
  markov       {status, series, note, prob_high_now, regime_high_mean,
                regime_low_mean, history: [[date, prob], ...], runtime_s}
  upcoming_stress_events [{date, end_date, label, severity, type, country}]
  levels       {calm: [0, 55], ...}
  method       {rule_name: plain-text rule}

All rules are documented in METHOD below.  Every step is wrapped so a failure
in one section degrades to an "error" note and never raises.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from stress_common import DATA_DIR, dump, fred_series, now_iso, source

logger = logging.getLogger("stress.regime")

STRESS_REGIMES = {
    "liquidity_tightening": "Liquidity tightening",
    "funding_crisis": "Funding crisis",
    "market_functioning": "Market-functioning breakdown",
    "sovereign_fiscal": "Sovereign / fiscal stress",
    "credit_stress": "Credit stress",
    "policy_intervention": "Policy intervention",
}
ALL_REGIMES = {"normal": "Normal"} | STRESS_REGIMES

# level thresholds on the 0-100 score (score = weighted mean of stress-direction
# percentiles, so ~50 is a "typical" environment)
LEVELS = [("calm", 0, 55), ("elevated", 55, 65), ("high", 65, 75), ("extreme", 75, 101)]
NORMAL_LEVELS = [("weak", 0, 35), ("moderate", 35, 50), ("strong", 50, 101)]

W_BASE = 1.0
W_STRESS_ZONE = 1.5          # multiplier when the member is in its stress zone
W_COMPOSITE = 0.5            # composite indices (OFR FSI, STLFSI, NFCI, CISS...)
W_STALE = 0.5
W_LOW_FREQ = 0.75            # monthly / quarterly members
W_FACILITY_POLICY = 2.0      # emergency facilities inside policy_intervention
POLICY_FIRST_USE_FLOOR = 85.0  # policy score floor on a non-turn-driven first use

TREND_FLAT_1W = 2.0
TREND_FLAT_1M = 3.0
MARKOV_TIMEOUT_S = 20
CAL_DAYS = 14

METHOD = {
    "member_percentile": (
        "Each ok indicator contributes its stress-direction percentile: stats.dir_pct = "
        "percentile rank of the latest value within its own ~10y history, flipped "
        "(100 - pct) when lower values mean more stress. 100 = most stressed reading in 10y, "
        "50 = median."),
    "facility_percentile": (
        "Emergency facilities (binary: swap lines, FIMA repo, SRF, discount window, BTFP) "
        "are mostly zero so raw percentiles are noisy. Rule: if usage is below the facility "
        "threshold (flags.active false) the percentile is capped at 50 (neutral); if active "
        "the raw percentile is used; a first_use flag (usage > threshold in the last 30d after "
        "90 quiet days) lifts it to at least 90, or only to at least 65 when flags.turn_driven "
        "(all usage within 2 business days of a month/quarter end, i.e. routine balance-sheet "
        "window dressing rather than distress)."),
    "weights": (
        "Member weight = 1.0, x1.5 if in its stress zone (dir_pct >= 90 or dir_z >= 2), "
        "x0.5 for composite stress indices (they double-count the raw inputs), x0.75 for "
        "monthly/quarterly series, x0.5 if stale. Inside policy_intervention the emergency "
        "facilities get x2.0."),
    "regime_score": (
        "Regime score (0-100) = weighted mean of member percentiles over members with "
        "status ok. Interpretation: the average member sits at the Nth stress percentile of its "
        "own history. policy_intervention uses the same weighted mean (facilities weighted x2) "
        "but is floored at 85 whenever any facility shows a non-turn-driven first_use, because "
        "one facility being tapped for the first time is itself the signal."),
    "policy_direction": (
        "Fed balance sheet 13w change (fed_bs_13w) is a liquidity_tightening member with "
        "direction -1 (shrinking = tighter). Inside policy_intervention its direction is "
        "reversed: rapid balance-sheet expansion = intervention, so the raw pct_full is used."),
    "levels": "Level labels: calm < 55 <= elevated < 65 <= high < 75 <= extreme.",
    "breadth": (
        "Breadth = members currently in their stress zone / ok members (both counts shown). "
        "For facilities, a non-turn-driven first_use also counts as in-zone."),
    "overall_and_normal": (
        "Overall stress = 0.5 x max(six stress-regime scores) + 0.5 x mean(six scores). "
        "normal score = 100 - overall stress (labels: strong >= 50, moderate 35-50, weak < 35)."),
    "dominant_regime": (
        "Headline regime = the highest-scoring stress regime if its level is elevated or "
        "worse; otherwise 'normal'."),
    "history": (
        "Weekly regime-score history (last ~3y, Fridays) is recomputed from each indicator's "
        "shipped history array, forward-filled to each Friday. Because only ~3y of history is "
        "shipped, historical member percentiles are ranks within that 3y window (and the stress "
        "zone uses 3y pct >= 90 or 3y z >= 2), not the 10y ranks used for the current score. "
        "score_3y is the last point of that series. Facility first_use is re-derived from the "
        "weekly samples (turn_driven cannot be reconstructed historically, so SRF first-use is "
        "treated as turn-driven in history)."),
    "trend": (
        "Trend arrows compare the 3y-basis history series: chg_1w = score now - score 1 week "
        "ago, chg_1m = now - 4 weeks ago. up/down if |chg_1w| >= 2 points (|chg_1m| >= 3), "
        "else flat. 'trend' = trend_1w unless flat, then trend_1m."),
    "what_moved": (
        "Moves are ranked by |z_move| = |stats.chg_1w| / std of weekly changes in the shipped "
        "~3y history. Monthly/quarterly series (no chg_1w) appear only if their as_of is in the "
        "last 10 days, using chg_1m / std of period changes. effect = 'bad' when the move "
        "is in the stress direction (sign(change) x direction > 0), else 'good'. Top 10 shown."),
    "events": (
        "Binary events: (1) facility first_use flags (with turn-driven caveat); (2) stress-zone "
        "entries/exits vs last week: zone now = stats.stress_zone; zone one week ago is "
        "estimated as dir_z_1w >= 2 or dir_pct_1w >= 90, where dir_z_1w uses the 3y history mean/sd "
        "applied to (latest - chg_1w) and dir_pct_1w = dir_pct + (3y rank 1w ago - 3y rank now); "
        "(3) early-warning (ew_signal) triggers; (4) stock-bond correlation break when the 63d "
        "correlation sign differs from its sign ~1 month earlier (or flags.sign_flip)."),
    "early_warning": (
        "Critical slowing down: rolling 1y variance AND lag-1 autocorrelation of changes both "
        "rising vs 6 months ago (stats.ew_signal: variance ratio > 1.25 and autocorr +0.1). "
        "'watch' lists members in a stress regime with one of the two rising and the other flat."),
    "markov": (
        "2-state Markov-switching mean/variance model (statsmodels MarkovRegression, "
        "switching_variance=True) fitted on the weekly St. Louis Fed Financial Stress Index "
        "(STLFSI4, FRED, since 2000); fallbacks: the stlfsi history in indicators.json, then the "
        "overall-score history. The high-stress state is the one with the higher mean. "
        "Output: smoothed probability of the high-stress state, current and weekly (last 3y). "
        "Fit is time-boxed at 20 s; any failure yields status 'error'."),
    "upcoming_stress_events": (
        "From calendar.json (or the calendar argument): events dated within the next 14 days "
        "(or spanning today) with severity med/high. next_major_event = first high-severity "
        "event beyond the 14-day window (else the first med one)."),
}


# ----------------------------------------------------------------- helpers
def _f(x, nd=2):
    try:
        if x is None:
            return None
        x = float(x)
        return None if not math.isfinite(x) else round(x, nd)
    except Exception:
        return None


def _level(score, table=LEVELS):
    if score is None:
        return None
    for name, lo, hi in table:
        if lo <= score < hi:
            return name
    return table[-1][0]


def _trend(d, flat):
    if d is None:
        return "flat"
    return "up" if d >= flat else "down" if d <= -flat else "flat"


def _series(rec) -> pd.Series | None:
    h = rec.get("history") or []
    if len(h) < 4:
        return None
    try:
        s = pd.Series([v for _, v in h], index=pd.to_datetime([d for d, _ in h]), dtype=float)
        s = s[~s.index.duplicated(keep="last")].dropna().sort_index()
        return s if len(s) >= 4 else None
    except Exception:
        return None


def _is_facility(rec):
    return bool(rec.get("binary"))


def _facility_pct(base, flags):
    flags = flags or {}
    if base is None:
        base = 50.0
    p = base if flags.get("active") else min(base, 50.0)
    if flags.get("first_use"):
        p = max(p, 65.0 if flags.get("turn_driven") else 90.0)
    return p


def _member_weight(rec, regime, in_zone):
    w = W_BASE
    if in_zone:
        w *= W_STRESS_ZONE
    if rec.get("group") == "composite":
        w *= W_COMPOSITE
    if rec.get("frequency") in ("monthly", "quarterly"):
        w *= W_LOW_FREQ
    if rec.get("stale"):
        w *= W_STALE
    if regime == "policy_intervention" and _is_facility(rec):
        w *= W_FACILITY_POLICY
    return w


def _member_pct_now(rec, regime):
    """(pct, in_zone, note) for the current (10y-basis) score."""
    st = rec.get("stats") or {}
    flags = rec.get("flags") or {}
    pct = st.get("dir_pct")
    zone = bool(st.get("stress_zone"))
    note = None
    if regime == "policy_intervention" and rec["id"] == "fed_bs_13w":
        pct = st.get("pct_full")
        zone = pct is not None and pct >= 90
        note = "direction reversed: expansion = intervention"
    if _is_facility(rec):
        pct = _facility_pct(pct, flags)
        if flags.get("first_use"):
            note = "first use (turn-driven)" if flags.get("turn_driven") else "FIRST USE"
            zone = zone or not flags.get("turn_driven")
        elif flags.get("active"):
            note = "active (above threshold)"
        else:
            zone = False
    if pct is None:
        return None, False, None
    return float(pct), zone, note


def _aggregate(items, regime):
    """items: [(pct, weight)] -> score."""
    if not items:
        return None
    p = np.array([i[0] for i in items], float)
    w = np.array([i[1] for i in items], float)
    mean = float((p * w).sum() / w.sum())
    return mean


def _overall(scores: dict):
    v = [s for k, s in scores.items() if k in STRESS_REGIMES and s is not None]
    if not v:
        return None
    return 0.5 * max(v) + 0.5 * float(np.mean(v))


# ------------------------------------------------------------ current scores
def _current_regimes(inds):
    rows = {}
    for rid, label in STRESS_REGIMES.items():
        members = [r for r in inds if rid in (r.get("regimes") or [])]
        items, mem_out, n_zone = [], [], 0
        for r in members:
            if r.get("status") != "ok":
                mem_out.append({"id": r["id"], "label": r.get("label"), "status": r.get("status"),
                                "pct": None, "weight": 0, "contribution": None,
                                "stress_zone": None, "note": r.get("note") and "not scored"})
                continue
            pct, zone, note = _member_pct_now(r, rid)
            if pct is None:
                continue
            w = _member_weight(r, rid, zone)
            items.append((pct, w, r, zone, note))
            n_zone += int(zone)
        score = _aggregate([(i[0], i[1]) for i in items], rid)
        if rid == "policy_intervention" and score is not None and any(
                (i[2].get("flags") or {}).get("first_use") and not (i[2].get("flags") or {}).get("turn_driven")
                for i in items if _is_facility(i[2])):
            score = max(score, POLICY_FIRST_USE_FLOOR)
        wsum = sum(i[1] for i in items) or 1.0
        for pct, w, r, zone, note in items:
            mem_out.append({"id": r["id"], "label": r.get("label"), "status": "ok",
                            "pct": _f(pct, 1), "weight": _f(w, 3),
                            # share of the weighted mean, in score points
                            "contribution": _f(pct * w / wsum, 2),
                            "stress_zone": zone, "note": note})
        mem_out.sort(key=lambda m: -(m["contribution"] or -1))
        n_ok = len(items)
        rows[rid] = {
            "id": rid, "label": label, "score": _f(score, 1), "level": _level(score),
            "n_members": len(members), "n_ok": n_ok, "n_stress_zone": n_zone,
            "breadth": _f(n_zone / n_ok, 3) if n_ok else None,
            "breadth_text": f"{n_zone}/{n_ok} in stress zone",
            "members": mem_out,
        }
    return rows


# ------------------------------------------------------------ history scores
def _rank_pct(arr: np.ndarray) -> np.ndarray:
    """Percentile rank (0-100) of each value within the array (mid-rank)."""
    a = np.asarray(arr, float)
    srt = np.sort(a)
    lo = np.searchsorted(srt, a, side="left")
    hi = np.searchsorted(srt, a, side="right")
    return 100.0 * (lo + hi) / 2.0 / len(a)


def _hist_member_frame(rec, grid: pd.DatetimeIndex, regime: str):
    """Weekly (pct, in_zone) arrays on the grid, 3y basis; None if unusable."""
    s = _series(rec)
    if s is None:
        return None
    direction = rec.get("direction") or 1
    if regime == "policy_intervention" and rec["id"] == "fed_bs_13w":
        direction = 1
    raw_pct = _rank_pct(s.values)
    pct = raw_pct if direction > 0 else 100 - raw_pct
    sd = s.std()
    z = (s.values - s.mean()) / sd * direction if sd and sd > 0 else np.zeros(len(s))
    df = pd.DataFrame({"pct": pct, "zone": (pct >= 90) | (z >= 2), "v": s.values}, index=s.index)
    if _is_facility(rec):
        thr = float((rec.get("flags") or {}).get("threshold") or 1.0)
        active = s.values > thr
        fu = np.zeros(len(s), bool)
        idx = s.index
        for i, d in enumerate(idx):
            recent = (idx > d - pd.Timedelta(days=30)) & (idx <= d)
            prior = (idx > d - pd.Timedelta(days=120)) & (idx <= d - pd.Timedelta(days=30))
            if active[recent].any() and prior.any() and not active[prior].any():
                fu[i] = True
        turn = rec["id"] == "srf_usage"
        p = np.where(active, df["pct"].values, np.minimum(df["pct"].values, 50.0))
        p = np.where(fu, np.maximum(p, 65.0 if turn else 90.0), p)
        df["pct"] = p
        df["zone"] = (df["zone"].values & active) | (fu & (not turn))
        df["fu"] = fu & (not turn)
    out = df.reindex(df.index.union(grid)).ffill().reindex(grid)
    out.loc[grid < s.index[0]] = np.nan
    return out


def _history(inds):
    ok = [r for r in inds if r.get("status") == "ok" and _series(r) is not None]
    if not ok:
        return None
    last = max(_series(r).index[-1] for r in ok)
    start = last - pd.Timedelta(days=3 * 365)
    grid = pd.date_range(start, last, freq="W-FRI")
    if grid[-1] < last:
        grid = grid.append(pd.DatetimeIndex([last]))
    out = {"dates": [d.strftime("%Y-%m-%d") for d in grid]}
    for rid in STRESS_REGIMES:
        num = np.zeros(len(grid))
        den = np.zeros(len(grid))
        fu_any = np.zeros(len(grid), bool)
        for r in ok:
            if rid not in (r.get("regimes") or []):
                continue
            fr = _hist_member_frame(r, grid, rid)
            if fr is None:
                continue
            p = fr["pct"].values.astype(float)
            zone = fr["zone"].fillna(False).values.astype(bool)
            w = np.array([_member_weight(r, rid, z) for z in zone])
            m = ~np.isnan(p)
            num[m] += p[m] * w[m]
            den[m] += w[m]
            if "fu" in fr:
                fu_any |= fr["fu"].fillna(False).values.astype(bool)
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = np.where(den > 0, num / den, np.nan)
        if rid == "policy_intervention":
            mean = np.where(fu_any, np.fmax(mean, POLICY_FIRST_USE_FLOOR), mean)
        out[rid] = [_f(v, 1) for v in mean]
    ov = []
    for i in range(len(grid)):
        ov.append(_f(_overall({k: out[k][i] for k in STRESS_REGIMES}), 1))
    out["overall"] = ov
    out["normal"] = [None if v is None else _f(100 - v, 1) for v in ov]
    return out


def _hist_change(series, weeks):
    vals = [v for v in series]
    if len(vals) < weeks + 1 or vals[-1] is None or vals[-1 - weeks] is None:
        return None
    return vals[-1] - vals[-1 - weeks]


# --------------------------------------------------------------- what moved
def _weekly_change_std(s: pd.Series, lo=5, hi=9):
    d = s.diff().dropna()
    gaps = s.index.to_series().diff().dt.days.reindex(d.index)
    d = d[(gaps >= lo) & (gaps <= hi)]
    if len(d) < 8:
        return None
    sd = float(d.std())
    return sd if sd > 0 else None


def _what_moved(inds, today: date):
    moves = []
    for r in inds:
        if r.get("status") != "ok":
            continue
        st = r.get("stats") or {}
        s = _series(r)
        if s is None:
            continue
        chg, horizon = st.get("chg_1w"), "1w"
        if chg is None:
            try:
                fresh = (today - date.fromisoformat(r.get("as_of"))).days <= 10
            except Exception:
                fresh = False
            if not fresh or st.get("chg_1m") is None:
                continue
            chg, horizon = st.get("chg_1m"), "1m"
            sd = _weekly_change_std(s, 20, 100)
        else:
            sd = _weekly_change_std(s)
        if not sd or chg is None:
            continue
        z = chg / sd
        direction = r.get("direction") or 1
        moves.append({
            "id": r["id"], "label": r.get("label"), "group": r.get("group"),
            "latest": _f(r.get("latest"), 4), "units": r.get("units"),
            "change": _f(chg, 4), "horizon": horizon, "z_move": _f(z, 2),
            "effect": "bad" if chg * direction > 0 else ("good" if chg != 0 else "flat"),
            "dir_pct": _f(st.get("dir_pct"), 1), "as_of": r.get("as_of"),
        })
    moves.sort(key=lambda m: -abs(m["z_move"] or 0))
    return moves[:10]


def _zone_1w_ago(r):
    st = r.get("stats") or {}
    s = _series(r)
    if s is None or st.get("chg_1w") is None or st.get("dir_pct") is None:
        return None
    direction = r.get("direction") or 1
    v_now = r.get("latest")
    v_prev = v_now - st["chg_1w"]
    sd = s.std()
    dz = (v_prev - s.mean()) / sd * direction if sd and sd > 0 else 0.0
    vals = np.sort(s.values)

    def rk(v):
        return 100.0 * (np.searchsorted(vals, v, "left") + np.searchsorted(vals, v, "right")) / 2 / len(vals)
    d3 = rk(v_prev) - rk(v_now)
    dp = st["dir_pct"] + (d3 if direction > 0 else -d3)
    return bool(dz >= 2 or dp >= 90)


def _events(inds):
    ev = []
    for r in inds:
        if r.get("status") != "ok":
            continue
        st = r.get("stats") or {}
        fl = r.get("flags") or {}
        lab = r.get("label") or r["id"]
        if fl.get("first_use"):
            e = {"type": "first_use", "id": r["id"], "label": lab,
                 "severity": "med" if fl.get("turn_driven") else "high",
                 "text": f"{lab}: first use above threshold in 90+ days (latest {_f(r.get('latest'), 3)} {r.get('units') or ''})."}
            if fl.get("turn_driven"):
                e["caveat"] = ("Usage clustered within 2 business days of a month/quarter end: "
                               "likely routine turn-of-period demand, not distress.")
            ev.append(e)
        prev = _zone_1w_ago(r)
        now = bool(st.get("stress_zone"))
        if prev is not None and prev != now:
            ev.append({"type": "stress_zone_entry" if now else "stress_zone_exit", "id": r["id"],
                       "label": lab, "severity": "med" if now else "low",
                       "text": f"{lab} {'entered' if now else 'left'} its stress zone this week "
                               f"(stress percentile {_f(st.get('dir_pct'), 1)}, dir z {_f(st.get('dir_z'), 2)})."})
        if st.get("ew_signal"):
            ev.append({"type": "early_warning", "id": r["id"], "label": lab, "severity": "med",
                       "text": f"{lab}: rolling variance and autocorrelation both rising (critical slowing down)."})
    # stock-bond correlation break
    sb = next((r for r in inds if r["id"] == "stock_bond_corr" and r.get("status") == "ok"), None)
    if sb:
        s = _series(sb)
        flip_flag = bool((sb.get("flags") or {}).get("sign_flip"))
        flip_1m, prev = False, None
        if s is not None and len(s) > 5:
            prev_idx = s.index[s.index <= s.index[-1] - pd.Timedelta(days=28)]
            if len(prev_idx):
                prev = float(s.loc[prev_idx[-1]])
                flip_1m = np.sign(prev) != np.sign(sb.get("latest") or 0) and prev != 0
        if flip_flag or flip_1m:
            now = sb.get("latest") or 0
            ev.append({"type": "correlation_break", "id": "stock_bond_corr", "label": sb.get("label"),
                       "severity": "high" if now > 0 else "med",
                       "text": (f"Stock-bond correlation flipped sign in the last month "
                                f"({_f(prev, 2)} -> {_f(now, 2)}); "
                                + ("positive correlation = bonds no longer hedge equities."
                                   if now > 0 else "back to negative (bonds hedging again)."))})
    order = {"high": 0, "med": 1, "low": 2}
    ev.sort(key=lambda e: order.get(e["severity"], 3))
    return ev


def _early_warning(inds):
    sig, watch = [], []
    for r in inds:
        st = r.get("stats") or {}
        if r.get("status") != "ok" or not st:
            continue
        rec = {"id": r["id"], "label": r.get("label"), "regimes": r.get("regimes"),
               "var_1y": st.get("var_1y"), "var_1y_6m_ago": st.get("var_1y_6m_ago"),
               "var_ratio": _f(st["var_1y"] / st["var_1y_6m_ago"], 2)
               if st.get("var_1y") and st.get("var_1y_6m_ago") else None,
               "ac1_1y": st.get("ac1_1y"), "ac1_1y_6m_ago": st.get("ac1_1y_6m_ago"),
               "var_trend": st.get("var_trend"), "ac1_trend": st.get("ac1_trend"),
               "dir_pct": _f(st.get("dir_pct"), 1), "as_of": r.get("as_of")}
        if st.get("ew_signal") or (st.get("var_trend") == "rising" and st.get("ac1_trend") == "rising"):
            sig.append(rec)
        elif {st.get("var_trend"), st.get("ac1_trend")} == {"rising", "flat"}:
            watch.append(rec)
    watch.sort(key=lambda x: -(x["dir_pct"] or 0))
    return {"signals": sig, "watch": watch[:10]}


# ------------------------------------------------------------------- markov
def _markov_fit(y: pd.Series):
    import warnings
    from statsmodels.tsa.regime_switching.markov_regression import MarkovRegression
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        mod = MarkovRegression(y.values, k_regimes=2, trend="c", switching_variance=True)
        res = mod.fit(disp=False, search_reps=5)
    probs = np.asarray(res.smoothed_marginal_probabilities)
    params = dict(zip(mod.param_names, np.asarray(res.params)))
    means = [params.get("const[0]"), params.get("const[1]")]
    hi = int(np.argmax(means))
    return probs[:, hi], means[hi], means[1 - hi]


def _markov(hist: dict | None, inds):
    t0 = time.time()
    out = {"status": "error", "series": None, "note": "", "prob_high_now": None, "history": []}
    candidates = []

    def fred():
        s = fred_series("STLFSI4", "2000-01-01")
        return s.resample("W-FRI").last().dropna()
    candidates.append(("STLFSI4 (FRED, weekly since 2000)", fred))
    st = next((r for r in inds if r["id"] == "stlfsi" and r.get("status") == "ok"), None)
    if st:
        candidates.append(("stlfsi history in indicators.json (~3y)", lambda: _series(st)))
    if hist and hist.get("overall"):
        candidates.append(("overall regime-score history (~3y)", lambda: pd.Series(
            hist["overall"], index=pd.to_datetime(hist["dates"]), dtype=float).dropna()))
    notes = []
    for name, getter in candidates:
        remaining = MARKOV_TIMEOUT_S - (time.time() - t0)
        if remaining <= 1:
            notes.append("time budget exhausted")
            break
        box = {}

        def work():
            try:
                y = getter()
                if y is None or len(y) < 60:
                    raise ValueError(f"too few observations ({0 if y is None else len(y)})")
                p, mh, ml = _markov_fit(y)
                box["r"] = (y, p, mh, ml)
            except Exception as e:  # noqa: BLE001
                box["e"] = e
        th = threading.Thread(target=work, daemon=True)
        th.start()
        th.join(remaining)
        if th.is_alive():
            notes.append(f"{name}: timed out")
            break
        if "e" in box:
            notes.append(f"{name}: {type(box['e']).__name__}: {box['e']}"[:200])
            continue
        y, p, mh, ml = box["r"]
        cutoff = y.index[-1] - pd.Timedelta(days=3 * 365)
        hist_pts = [[d.strftime("%Y-%m-%d"), _f(v, 3)] for d, v in zip(y.index, p) if d >= cutoff]
        # count high-regime episodes over the full sample for context
        hi_share = float((p > 0.5).mean())
        out.update({"status": "ok", "series": name, "prob_high_now": _f(p[-1], 3),
                    "as_of": y.index[-1].strftime("%Y-%m-%d"), "latest_value": _f(y.iloc[-1], 3),
                    "regime_high_mean": _f(mh, 3), "regime_low_mean": _f(ml, 3),
                    "share_weeks_high": _f(hi_share, 3), "n_obs": int(len(y)),
                    "sample_start": y.index[0].strftime("%Y-%m-%d"), "history": hist_pts})
        break
    out["note"] = "; ".join(notes)
    out["runtime_s"] = round(time.time() - t0, 2)
    return out


# ----------------------------------------------------------------- calendar
def _upcoming(calendar, today: date):
    if calendar is None:
        p = DATA_DIR / "calendar.json"
        if not p.exists():
            return [], None, "calendar.json not found"
        calendar = json.loads(p.read_text())
    evs = calendar.get("events") if isinstance(calendar, dict) else calendar
    out, later = [], []
    for e in evs or []:
        try:
            sev = str(e.get("severity") or e.get("importance") or "").lower()
            if sev not in ("med", "medium", "high"):
                continue
            d = date.fromisoformat(str(e.get("date"))[:10])
            end = date.fromisoformat(str(e.get("end_date"))[:10]) if e.get("end_date") else d
            if end < today:
                continue
            (later if d > today + timedelta(days=CAL_DAYS) else out).append({"date": d.isoformat(), "end_date": e.get("end_date"),
                        "label": e.get("label") or e.get("title") or e.get("name"),
                        "severity": "med" if sev.startswith("med") else "high",
                        "type": e.get("type"), "country": e.get("country"),
                        "days_ahead": (d - today).days})
        except Exception:
            continue
    out.sort(key=lambda x: x["date"])
    later.sort(key=lambda x: (x["date"], x["severity"] != "high"))
    nxt = next((x for x in later if x["severity"] == "high"), later[0] if later else None)
    return out, nxt, f"{len(out)} med/high events in next {CAL_DAYS} days"


# --------------------------------------------------------------- headline
def _summary(dom, rows, overall, events, ew):
    if dom == "normal":
        top = max(STRESS_REGIMES, key=lambda k: rows[k]["score"] or 0)
        s = (f"No dominant stress regime: overall stress {overall:.0f}/100; "
             f"highest is {rows[top]['label'].lower()} at {rows[top]['score']:.0f} ({rows[top]['level']}).")
    else:
        r = rows[dom]
        zone = [m["label"] for m in r["members"] if m.get("stress_zone")][:3]
        s = (f"{r['label']} leads at {r['score']:.0f}/100 ({r['level']}, trend {r['trend']}), "
             f"{r['breadth_text']}" + (f" - led by {', '.join(zone)}" if zone else "") + ".")
    fu = [e for e in events if e["type"] == "first_use" and not e.get("caveat")]
    if fu:
        s += f" First use of {fu[0]['label']}."
    return s


def _safe_summary(*args):
    try:
        if all(args[1][k]["score"] is None for k in STRESS_REGIMES):
            return "Regime scores unavailable: no indicators returned usable data."
        return _summary(*args)
    except Exception as e:  # noqa: BLE001
        logger.warning("summary failed: %s", e)
        return "Regime summary unavailable."


# ------------------------------------------------------------------- build
def build_regime(indicators: dict | None = None, calendar: dict | None = None) -> dict:
    t0 = time.time()
    sources = []
    if indicators is None:
        p = DATA_DIR / "indicators.json"
        if p.exists():
            indicators = json.loads(p.read_text())
        else:
            from stress_indicators import build_indicators
            indicators = build_indicators()
    inds = indicators.get("indicators") or []
    today = datetime.now(timezone.utc).date()

    rows = _current_regimes(inds)
    try:
        hist = _history(inds)
        sources.append(source("Regime-score history (3y basis)", "ok" if hist else "error",
                              hist["dates"][-1] if hist else None))
    except Exception as e:  # noqa: BLE001
        logger.exception("history failed")
        hist = None
        sources.append(source("Regime-score history (3y basis)", "error", None, str(e)[:200]))

    for rid, r in rows.items():
        h = (hist or {}).get(rid) or []
        c1w, c1m = _hist_change(h, 1), _hist_change(h, 4)
        r["score_3y"] = h[-1] if h else None
        r["chg_1w"], r["chg_1m"] = _f(c1w, 1), _f(c1m, 1)
        r["trend_1w"], r["trend_1m"] = _trend(c1w, TREND_FLAT_1W), _trend(c1m, TREND_FLAT_1M)
        r["trend"] = r["trend_1w"] if r["trend_1w"] != "flat" else r["trend_1m"]
        r["history_basis"] = "3y-window percentiles (see method.history)"

    scores = {k: rows[k]["score"] for k in STRESS_REGIMES}
    overall = _overall(scores)
    normal = None if overall is None else 100 - overall
    hn = (hist or {}).get("normal") or []
    n1w, n1m = _hist_change(hn, 1), _hist_change(hn, 4)
    zone_total = sum(1 for r in inds if r.get("status") == "ok" and (r.get("stats") or {}).get("stress_zone"))
    n_ok = sum(1 for r in inds if r.get("status") == "ok")
    normal_row = {
        "id": "normal", "label": "Normal", "score": _f(normal, 1), "level": _level(normal, NORMAL_LEVELS),
        "score_3y": hn[-1] if hn else None, "n_members": len(inds), "n_ok": n_ok,
        "n_stress_zone": zone_total, "breadth": _f(1 - zone_total / n_ok, 3) if n_ok else None,
        "breadth_text": f"{n_ok - zone_total}/{n_ok} indicators outside stress zone",
        "chg_1w": _f(n1w, 1), "chg_1m": _f(n1m, 1),
        "trend_1w": _trend(n1w, TREND_FLAT_1W), "trend_1m": _trend(n1m, TREND_FLAT_1M),
        "members": [{"id": k, "label": STRESS_REGIMES[k], "pct": scores[k], "weight": None,
                     "contribution": None, "stress_zone": None, "note": "regime score"}
                    for k in STRESS_REGIMES],
        "history_basis": "100 - overall stress (3y basis)",
        "note": "normal = 100 - overall stress; overall = 0.5*max + 0.5*mean of the six stress regimes",
    }
    normal_row["trend"] = normal_row["trend_1w"] if normal_row["trend_1w"] != "flat" else normal_row["trend_1m"]

    top = max(STRESS_REGIMES, key=lambda k: scores[k] or 0)
    dom = top if (scores[top] or 0) >= LEVELS[1][1] else "normal"

    try:
        moves = _what_moved(inds, today)
        events = _events(inds)
    except Exception as e:  # noqa: BLE001
        logger.exception("what_moved failed")
        moves, events = [], [{"type": "error", "text": str(e)[:200], "severity": "low"}]
    try:
        ew = _early_warning(inds)
    except Exception as e:  # noqa: BLE001
        ew = {"signals": [], "watch": [], "error": str(e)[:200]}
    try:
        mk = _markov(hist, inds)
    except Exception as e:  # noqa: BLE001
        mk = {"status": "error", "note": str(e)[:200], "prob_high_now": None, "history": []}
    sources.append(source("Markov switching (" + str(mk.get("series")) + ")", mk["status"],
                          mk.get("as_of"), mk.get("note", "")))
    try:
        upcoming, next_major, cal_note = _upcoming(calendar, today)
        sources.append(source("Stress calendar", "ok", today.isoformat(), cal_note))
    except Exception as e:  # noqa: BLE001
        upcoming, next_major = [], None
        sources.append(source("Stress calendar", "error", None, str(e)[:200]))

    dom_row = normal_row if dom == "normal" else rows[dom]
    headline = {
        "regime": dom, "label": ALL_REGIMES[dom], "level": dom_row["level"],
        "score": dom_row["score"], "overall_stress": _f(overall, 1),
        "overall_level": _level(overall), "normal_score": _f(normal, 1),
        "summary": _safe_summary(dom, rows, overall, events, ew),
        "markov_prob_high": mk.get("prob_high_now"),
        "as_of": indicators.get("generated_at"),
    }

    out = {
        "generated_at": now_iso(),
        "indicators_generated_at": indicators.get("generated_at"),
        "sources": sources,
        "headline": headline,
        "regimes": [normal_row] + [rows[k] for k in STRESS_REGIMES],
        "history": hist,
        "what_moved": {"moves": moves, "events": events},
        "early_warning": ew,
        "markov": mk,
        "upcoming_stress_events": upcoming,
        "next_major_event": next_major,
        "levels": {n: [lo, min(hi, 100)] for n, lo, hi in LEVELS},
        "normal_levels": {n: [lo, min(hi, 100)] for n, lo, hi in NORMAL_LEVELS},
        "method": METHOD,
    }
    out["runtime_s"] = round(time.time() - t0, 2)
    dump("regime.json", out)
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    res = build_regime()
    h = res["headline"]
    print(f"[{h['regime']}] {h['summary']}")
    for r in res["regimes"]:
        print(f"  {r['label']:<30} {r['score']!s:>5} {r['level']:<9} {r['breadth_text']:<34} "
              f"1w {r['chg_1w']!s:>5} {r['trend_1w']:<5} 1m {r['chg_1m']!s:>5} {r['trend_1m']}")
    print("markov:", res["markov"]["status"], res["markov"].get("series"), res["markov"].get("prob_high_now"),
          res["markov"].get("note"))
    print("runtime", res["runtime_s"], "s")
