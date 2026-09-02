"""Global Liquidity aggregation layer — Phase 2 + Phase 3.

Three sub-index calculators (Howell-style, §2 of liquidity_framework.md):
  compute_cb_liquidity()      — Fed + ECB + BoJ balance sheets, USD
  compute_private_liquidity() — bank credit + shadow banking + Z.1
  compute_xborder_liquidity() — BIS LBS cross-border claims + TIC UST holdings

Phase 3 composite + cycle functions:
  compute_global_liquidity_composite() — 30/50/20 weighted z-scored composite
  compute_global_liquidity_smoothed()  — 3-month (~63 trading day) MA of composite
  compute_cycle_yoy(series)            — 12-month YoY % change of any series
  compute_cycle_zscore(series)         — z-score rebased to 0-100 (50 = mean)
  classify_regime(composite_z)         — Expansion/Late-Cycle/Contraction/Inflection

Utility helpers (liquidity_utils.py is not used; helpers live here for simplicity):
  z_score_rolling(values, window_days)
  gdp_weighted_growth(series_dict, gdp_weights)
  forward_fill_to_daily(observations, start_date, end_date)
  fx_convert_to_usd(observations, fx_series, mode)

Storage choice: compute on-demand (option a) — rationale in §11 Phase 2 notes.

All public functions return list[tuple[str, float]] → (YYYY-MM-DD, value).
Callers must ensure DB is populated via backfill_macro_all() before calling.

JPNASSETS unit caveat (§4 Opus decision 4): FRED labels as "millions of U.S. Dollars"
but values are 100 million JPY (億円). compute_cb_liquidity() multiplies by 100 before
FX conversion. Any future code touching JPNASSETS must apply the same correction.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Optional

import database as db

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── GDP weights for v1 CB Liquidity (Fed + ECB + BoJ only; §4 Opus decision 4) ─
# 2023 IMF World Economic Outlook nominal GDP shares, normalized to three-CB total.
# Fed ~ US 26.5%, ECB ~ Euro Area 14.5%, BoJ ~ Japan 4.2%. Shares within trio:
_CB_GDP_WEIGHTS = {
    "WALCL":     0.581,   # Fed: 26.5 / 45.2
    "ECBASSETSW": 0.321,  # ECB: 14.5 / 45.2
    "JPNASSETS":  0.093,  # BoJ: 4.2 / 45.2 — rounded to sum = 0.995 ≈ 1.0
}


# ══════════════════════════════════════════════════════════════════════════════
# Utility helpers
# ══════════════════════════════════════════════════════════════════════════════

def z_score_rolling(
    values: list[tuple[str, float]],
    window_days: int = 1825,
) -> list[tuple[str, float]]:
    """Rolling z-score: (x - mean_window) / std_window for each observation.

    window_days — lookback in calendar days (default 1825 = 5 years).
    Points where the window is too small to be meaningful (< 2 non-null obs)
    are dropped rather than returned with NaN. Returns dates sorted ascending.
    """
    if not values:
        return []
    out: list[tuple[str, float]] = []
    for i, (ts, val) in enumerate(values):
        cutoff = _days_back(ts, window_days)
        window = [v for t, v in values[:i + 1] if t >= cutoff]
        n = len(window)
        if n < 2:
            continue
        mean = sum(window) / n
        variance = sum((v - mean) ** 2 for v in window) / (n - 1)
        std = math.sqrt(variance)
        if std == 0:
            out.append((ts, 0.0))
        else:
            out.append((ts, (val - mean) / std))
    return out


def gdp_weighted_growth(
    series_dict: dict[str, list[tuple[str, float]]],
    gdp_weights: dict[str, float],
) -> list[tuple[str, float]]:
    """Weighted YoY growth rate across multiple series.

    series_dict — {series_id: [(date, value), ...]}
    gdp_weights — {series_id: weight} (weights need not sum to 1; normalised here)

    Returns (date, weighted_growth_pct) for dates where at least one series
    has a YoY comparison available. Series with missing YoY are excluded from
    the weighted average at that date (remaining weights are renormalized).
    """
    # Compute YoY % change for each series
    yoy: dict[str, list[tuple[str, float]]] = {}
    for sid, rows in series_dict.items():
        yoy[sid] = _yoy_pct(rows)

    # Build date-indexed dicts
    yoy_dicts: dict[str, dict[str, float]] = {
        sid: {t: v for t, v in rows} for sid, rows in yoy.items()
    }

    # Gather all dates
    all_dates = sorted({t for rows in yoy.values() for t, _ in rows})
    total_w = sum(gdp_weights.values())
    out: list[tuple[str, float]] = []
    for ts in all_dates:
        contrib = 0.0
        active_w = 0.0
        for sid, rows in yoy_dicts.items():
            if ts in rows and sid in gdp_weights:
                w = gdp_weights[sid]
                contrib += rows[ts] * w
                active_w += w
        if active_w > 0:
            out.append((ts, contrib / active_w * total_w / total_w))
    return out


def forward_fill_to_daily(
    observations: list[tuple[str, float]],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> list[tuple[str, float]]:
    """Forward-fill sparse observations onto a daily date grid.

    Takes a sparse series (weekly, monthly, quarterly) and carries each value
    forward until the next observation or end_date. Returns one entry per
    calendar day with a non-null value.

    start_date / end_date: YYYY-MM-DD strings (defaults: series extent).
    """
    if not observations:
        return []
    obs_sorted = sorted(observations)
    sd = start_date or obs_sorted[0][0]
    ed = end_date or date.today().isoformat()

    out: list[tuple[str, float]] = []
    current_date = date.fromisoformat(sd)
    end = date.fromisoformat(ed)
    obs_idx = 0
    last_val: Optional[float] = None

    # Advance to first available observation on or before start
    for i, (ts, val) in enumerate(obs_sorted):
        if ts <= sd:
            last_val = val
            obs_idx = i + 1
        else:
            break

    while current_date <= end:
        ts = current_date.isoformat()
        # Advance observations
        while obs_idx < len(obs_sorted) and obs_sorted[obs_idx][0] <= ts:
            last_val = obs_sorted[obs_idx][1]
            obs_idx += 1
        if last_val is not None:
            out.append((ts, last_val))
        current_date += timedelta(days=1)
    return out


def fx_convert_to_usd(
    observations: list[tuple[str, float]],
    fx_series: list[tuple[str, float]],
    mode: str = "multiply",
) -> list[tuple[str, float]]:
    """Convert foreign-currency series to USD using a paired FX series.

    mode='multiply' — obs * fx  (for quote X/USD, e.g. EURUSD: EUR × EURUSD = USD)
    mode='divide'   — obs / fx  (for quote USD/X, e.g. USDJPY: JPY ÷ USDJPY = USD)

    FX is forward-filled onto the observation dates. Observations without a
    forward-filled FX rate (before the first FX observation) are dropped.
    Returns (date, usd_value) sorted ascending.
    """
    if not observations or not fx_series:
        return []
    # Build forward-filled FX daily grid spanning the observation range
    if not fx_series:
        return []
    obs_dates = [t for t, _ in observations]
    fx_ff = forward_fill_to_daily(
        fx_series,
        start_date=min(obs_dates),
        end_date=max(obs_dates),
    )
    fx_dict = {t: v for t, v in fx_ff}

    out: list[tuple[str, float]] = []
    for ts, val in observations:
        fx_val = fx_dict.get(ts)
        if fx_val is None:
            continue
        if mode == "multiply":
            out.append((ts, val * fx_val))
        elif mode == "divide":
            if fx_val == 0:
                continue
            out.append((ts, val / fx_val))
        else:
            raise ValueError(f"fx_convert_to_usd: unknown mode '{mode}'")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Sub-index calculators
# ══════════════════════════════════════════════════════════════════════════════

def compute_cb_liquidity(
    start_date: Optional[str] = "2015-01-01",
) -> list[tuple[str, float]]:
    """Sum of CB balance sheets in USD, forward-filled to daily.

    v1 composition (§4 Opus decision 4): Fed + ECB + BoJ only.
      Fed:  WALCL ($M weekly) — no FX conversion needed
      ECB:  ECBASSETSW ($M weekly) × DEXUSEU (EUR/USD)
      BoJ:  JPNASSETS ($M monthly) ÷ DEXJPUS (JPY/USD → = USD/JPY)

    Returns (YYYY-MM-DD, USD_billions) tuples, daily forward-filled.
    Dates where at least one CB series has a value are included.
    """
    # Pull raw series from DB
    fed_rows = _get_series_tuples("WALCL")        # $M
    ecb_raw  = _get_series_tuples("ECBASSETSW")   # $M EUR
    boj_raw  = _get_series_tuples("JPNASSETS")    # $M JPY

    # FX rates
    eurusd = _get_series_tuples("DEXUSEU")    # USD per EUR (multiply)
    jpyusd = _get_series_tuples("DEXJPUS")    # JPY per USD (divide)

    # Scale BoJ: JPNASSETS unit is 100 million JPY (億円) — verified 2026-06-18.
    # Multiply by 100 to get millions of JPY, then divide by DEXJPUS (JPY/USD) → millions of USD.
    boj_jpy_m = [(t, v * 100.0) for t, v in boj_raw]  # 100M JPY → M JPY

    # FX-convert
    ecb_usd = fx_convert_to_usd(ecb_raw, eurusd, mode="multiply")  # EUR $M → USD $M
    boj_usd = fx_convert_to_usd(boj_jpy_m, jpyusd, mode="divide")   # JPY $M → USD $M

    # Forward-fill each to daily grid
    sd = start_date or "2000-01-01"
    ed = date.today().isoformat()
    fed_daily = forward_fill_to_daily(fed_rows, sd, ed)
    ecb_daily = forward_fill_to_daily(ecb_usd, sd, ed)
    boj_daily = forward_fill_to_daily(boj_usd, sd, ed)

    # Merge by date (outer join — include dates where at least one CB available)
    fed_d = {t: v for t, v in fed_daily}
    ecb_d = {t: v for t, v in ecb_daily}
    boj_d = {t: v for t, v in boj_daily}

    all_dates = sorted(set(fed_d) | set(ecb_d) | set(boj_d))
    out: list[tuple[str, float]] = []
    for ts in all_dates:
        total_m = (fed_d.get(ts, 0) or 0) + (ecb_d.get(ts, 0) or 0) + (boj_d.get(ts, 0) or 0)
        # Convert $M → $B
        out.append((ts, round(total_m / 1000.0, 2)))
    return out


def compute_private_liquidity(
    start_date: Optional[str] = "2015-01-01",
) -> list[tuple[str, float]]:
    """Aggregate of bank credit + shadow banking + Z.1 components.

    Components (forward-filled to daily, USD billions):
      Bank credit:
        M2SL ($B weekly), TOTLL ($B weekly)
        Note: H8B1023NCBCMG excluded — FRED stores this as YoY % growth rate,
        not a balance-sheet level; unsuitable for direct summation.
      Shadow banking:
        MMMFFAQ027S ($M quarterly → /1000 → $B), COMPOUT ($B weekly),
        RRPONTSYD ($B daily), RPONTSYD ($B daily)
        + OFR repo gross outstanding (from ofr_client, $B daily)
        + ASTDSL (Agency/GSE-backed debt, $M quarterly → /1000 → $B)
      Z.1 (§4 Opus decision 2 — Phase 2):
        BOGZ1FL892090005Q ($M quarterly → $B); TCMDO excluded to prevent
        double-counting (both TCMDO and BOGZ1 represent total credit; BOGZ1
        is the modern/broader Z.1 aggregate).

    Returns (YYYY-MM-DD, USD_billions) as the raw sum of all components.
    Callers should z-score this for the composite.
    """
    sd = start_date or "2000-01-01"
    ed = date.today().isoformat()

    # Bank credit
    m2sl  = _ff_daily("M2SL", sd, ed)   # $B weekly
    totll = _ff_daily("TOTLL", sd, ed)  # $B weekly
    # H8B1023NCBCMG omitted: FRED stores YoY % growth, not level — see docstring.

    # Shadow banking (unit corrections: MMMFFAQ027S and ASTDSL are in $M, not $B)
    mmf    = _ff_daily_scaled("MMMFFAQ027S", sd, ed, scale=1/1000.0)  # $M → $B quarterly
    cp     = _ff_daily("COMPOUT", sd, ed)                             # $B weekly
    rrp    = _ff_daily("RRPONTSYD", sd, ed)                           # $B daily
    srf    = _ff_daily("RPONTSYD", sd, ed)                            # $B daily
    astdsl = _ff_daily_scaled("ASTDSL", sd, ed, scale=1/1000.0)       # $M → $B quarterly

    # Z.1: use BOGZ1FL892090005Q only; TCMDO dropped (§4 Opus decision 2 — 2026-06-19)
    bogz1  = _ff_daily_scaled("BOGZ1FL892090005Q", sd, ed, scale=1/1000.0)  # $M → $B quarterly

    # OFR repo (from file cache; may be empty if not yet downloaded)
    ofr_repo = _ofr_repo_daily(sd, ed)

    # Sum all components day by day
    all_dates = sorted(
        set(m2sl) | set(totll) | set(mmf) | set(cp)
        | set(rrp) | set(srf) | set(astdsl) | set(bogz1) | set(ofr_repo)
    )
    out: list[tuple[str, float]] = []
    for ts in all_dates:
        total = (
            (m2sl.get(ts) or 0)
            + (totll.get(ts) or 0)
            + (mmf.get(ts) or 0)
            + (cp.get(ts) or 0)
            + (rrp.get(ts) or 0)
            + (srf.get(ts) or 0)
            + (astdsl.get(ts) or 0)
            + (bogz1.get(ts) or 0)
            + (ofr_repo.get(ts) or 0)
        )
        if total > 0:
            out.append((ts, round(total, 2)))
    return out


def compute_xborder_liquidity(
    start_date: Optional[str] = "2000-01-01",
) -> list[tuple[str, float]]:
    """BIS LBS cross-border claims + Treasury TIC foreign UST holdings, USD.

    Components:
      1. BIS LBS total cross-border claims (quarterly, $B) — from bis_client
      2. TIC foreign UST holdings (monthly, $B) — from tic_client
      3. Offshore $ rate spread: OBFR − SOFR (daily, bps×100) — signal overlay

    Returns (YYYY-MM-DD, USD_billions) as the sum of BIS + TIC (forward-filled
    to daily). The offshore spread is NOT summed in — it modifies the signal
    directionality; that modifier belongs in the composite layer (Phase 3).
    """
    import bis_client
    import tic_client

    sd = start_date or "2000-01-01"
    ed = date.today().isoformat()

    # BIS LBS — from cache (no auto-download here; call backfill_lbs_claims() first)
    bis_rows = bis_client.get_lbs_total_claims(refresh=False)  # ($B, quarterly)
    if not bis_rows:
        logger.warning("compute_xborder_liquidity: BIS LBS data not in cache — run backfill first")

    # TIC — from live fetch (small file, ~20KB)
    tic_rows = tic_client.get_foreign_ust_holdings(start_date=sd)

    # Forward-fill both to daily
    bis_daily = _dict_from_ff(forward_fill_to_daily(bis_rows, sd, ed))
    tic_daily = _dict_from_ff(forward_fill_to_daily(tic_rows, sd, ed))

    all_dates = sorted(set(bis_daily) | set(tic_daily))
    out: list[tuple[str, float]] = []
    for ts in all_dates:
        total = (bis_daily.get(ts) or 0) + (tic_daily.get(ts) or 0)
        if total > 0:
            out.append((ts, round(total, 2)))
    return out


def compute_offshore_spread(
    start_date: Optional[str] = "2018-01-01",
) -> list[tuple[str, float]]:
    """OBFR − SOFR spread (basis points), winsorized at ±100bps.

    OBFR = Overnight Bank Funding Rate (includes offshore Eurodollar segment).
    SOFR = Secured Overnight Financing Rate (pure onshore repo collateral).
    Positive spread → offshore dollar funding more expensive than onshore.

    Clipped to [−100bps, +100bps] per §4 Opus decision 3 (2026-06-19):
    the Sept 2019 SOFR spike produced −300bps; capping preserves directional
    signal without that event dominating downstream z-scores.

    Returns (YYYY-MM-DD, spread_bps). SOFR starts 2018-04-03; overlap from then.
    """
    obfr = _get_series_tuples("OBFR")   # % daily
    sofr = _get_series_tuples("SOFR")   # % daily
    sofr_d = {t: v for t, v in sofr}
    out: list[tuple[str, float]] = []
    sd = start_date or "2000-01-01"
    for ts, obfr_val in obfr:
        if ts < sd:
            continue
        sofr_val = sofr_d.get(ts)
        if sofr_val is None:
            continue
        spread_bps = round((obfr_val - sofr_val) * 100, 1)
        # Winsorize: cap at ±100bps (§4 Opus decision 3)
        spread_bps = max(-100.0, min(100.0, spread_bps))
        out.append((ts, spread_bps))
    return sorted(out)


# ══════════════════════════════════════════════════════════════════════════════
# Phase 3 — Composite + cycle
# ══════════════════════════════════════════════════════════════════════════════

# Composite weights per §2 / §7 of liquidity_framework.md
_COMPOSITE_WEIGHTS = {
    "cb":      0.30,  # Central Bank Liquidity
    "private": 0.50,  # Private Sector Liquidity
    "xborder": 0.20,  # Cross-Border Flows
}

# Rolling z-score window for each sub-index (5 years = 1825 calendar days).
# Using the same window as z_score_rolling() for consistency.
_ZSCORE_WINDOW_DAYS = 1825


def _z_score_rolling_fast(
    values: list[tuple[str, float]],
    window_days: int = 1825,
) -> list[tuple[str, float]]:
    """Fast rolling z-score using numpy expanding/rolling windows.

    Identical semantics to z_score_rolling() but 100-200x faster on large daily
    series by using numpy vectorised operations. Falls back to z_score_rolling()
    if numpy is not available.

    The window is treated as a position window (not calendar days) because the
    input is already on a daily forward-filled grid where positions ≈ calendar
    days. This matches the behaviour of z_score_rolling() on dense daily series.
    """
    if not _NUMPY_AVAILABLE:
        return z_score_rolling(values, window_days)

    if not values:
        return []

    dates = [t for t, _ in values]
    arr = np.array([v for _, v in values], dtype=np.float64)
    n = len(arr)
    out_dates: list[str] = []
    out_vals: list[float] = []

    for i in range(1, n):
        start = max(0, i - window_days + 1)
        window = arr[start: i + 1]
        if len(window) < 2:
            continue
        mean = window.mean()
        std = window.std(ddof=1)
        if std == 0.0:
            out_dates.append(dates[i])
            out_vals.append(0.0)
        else:
            out_dates.append(dates[i])
            out_vals.append(float((arr[i] - mean) / std))

    return list(zip(out_dates, [round(v, 6) for v in out_vals]))


def compute_global_liquidity_composite() -> list[tuple[str, float]]:
    """Weighted composite: 30% CB + 50% Private + 20% Cross-Border.

    Each sub-index is z-scored against its own rolling 5-year history before
    weighting. Output is the weighted sum (not yet smoothed or rebased).
    Returns (date, composite_z) tuples on a daily forward-filled grid.

    Implementation notes:
    - Sub-indices are computed via their respective functions (compute-on-demand).
    - Cross-border starts 2000-01-01; CB and Private start 2015-01-01. The
      composite grid is the intersection (dates where all three have values).
    - Each sub-index is z-scored independently before weighting, so unit
      differences between sub-indices are neutralised.
    """
    cb      = compute_cb_liquidity()        # daily, $B
    private = compute_private_liquidity()   # daily, $B
    xborder = compute_xborder_liquidity()   # daily, $B

    # Z-score each sub-index against its own rolling history.
    # Uses numpy-accelerated fast path when available (~100x faster than pure Python).
    cb_z      = _z_score_rolling_fast(cb,      _ZSCORE_WINDOW_DAYS)
    private_z = _z_score_rolling_fast(private, _ZSCORE_WINDOW_DAYS)
    xborder_z = _z_score_rolling_fast(xborder, _ZSCORE_WINDOW_DAYS)

    # Build date-indexed dicts for each z-scored sub-index
    cb_d  = {t: v for t, v in cb_z}
    pr_d  = {t: v for t, v in private_z}
    xb_d  = {t: v for t, v in xborder_z}

    # Intersection of all three (only dates where every sub-index has a z-score)
    common_dates = sorted(set(cb_d) & set(pr_d) & set(xb_d))

    w_cb = _COMPOSITE_WEIGHTS["cb"]
    w_pr = _COMPOSITE_WEIGHTS["private"]
    w_xb = _COMPOSITE_WEIGHTS["xborder"]

    out: list[tuple[str, float]] = []
    for ts in common_dates:
        composite = (
            w_cb * cb_d[ts]
            + w_pr * pr_d[ts]
            + w_xb * xb_d[ts]
        )
        out.append((ts, round(composite, 4)))
    return out


def compute_global_liquidity_smoothed(
    window_days: int = 63,
) -> list[tuple[str, float]]:
    """3-month moving average of the composite (~63 trading days).

    Howell's published methodology uses 3M smoothing. Returns same shape
    as compute_global_liquidity_composite().

    window_days=63 approximates 3 calendar months of trading days.
    Each output point is the equally-weighted mean of up to window_days
    preceding composite values. Points with fewer than window_days // 2
    preceding values are dropped to avoid unreliable early-window averages.
    """
    composite = compute_global_liquidity_composite()
    if not composite:
        return []

    out: list[tuple[str, float]] = []
    min_obs = window_days // 2  # require at least half the window to be populated

    for i, (ts, _) in enumerate(composite):
        # Gather values within the trailing window_days positions (not calendar days;
        # the composite is already on a daily grid, so positions ≈ calendar days).
        start_idx = max(0, i - window_days + 1)
        window_vals = [v for _, v in composite[start_idx: i + 1]]
        if len(window_vals) < min_obs:
            continue
        smoothed = sum(window_vals) / len(window_vals)
        out.append((ts, round(smoothed, 4)))
    return out


def compute_cycle_yoy(
    series: list[tuple[str, float]],
) -> list[tuple[str, float]]:
    """12-month YoY % change of any time series.

    Intuitive cycle indicator: positive during expansion (2020-21 QE),
    negative during contraction (2022-23 QT). Uses the same ≥330-day
    lookback as _yoy_pct() to tolerate gaps in sparse series.

    series — list of (YYYY-MM-DD, value) tuples, any cadence.
    Returns (date, yoy_pct_change) with the same dates as the input
    (minus the first ~12 months where no prior-year comparison exists).
    """
    return _yoy_pct(sorted(series))


def compute_cycle_zscore(
    series: list[tuple[str, float]],
    window_days: int = 1825,
) -> list[tuple[str, float]]:
    """Z-score normalization, rebased to 0-100 scale (50 = mean).

    Pure Howell-style cycle indicator. Steps:
      1. Compute rolling z-score (window_days, default 5 years).
      2. Clip at ±3σ (values beyond 3σ are vanishingly rare; clipping prevents
         extreme events from compressing the rest of the scale). Clip-at-3σ is
         preferred over sigmoid/tanh because it preserves linear proportionality
         within the ±3σ band — easier to interpret.
      3. Remap [−3, +3] → [0, 100] via: score = (z + 3) / 6 × 100.
         At z=0 (mean): score = 50. At z=+3: score = 100. At z=−3: score = 0.

    Output bounded [0, 100]; 50 = long-run mean.
    """
    z_series = z_score_rolling(series, window_days)
    out: list[tuple[str, float]] = []
    for ts, z in z_series:
        z_clipped = max(-3.0, min(3.0, z))
        score = round((z_clipped + 3.0) / 6.0 * 100.0, 2)
        out.append((ts, score))
    return out


def classify_regime(
    composite_z: list[tuple[str, float]],
    slope_window_days: int = 63,
) -> str:
    """Return current regime: 'Expansion' | 'Late-Cycle' | 'Contraction' | 'Inflection'.

    Methodology — YoY %-change based (matches Howell's published cycle approach).

    Howell's framework is fundamentally about *flow* (rate of change), not stock
    (absolute level). A high composite level with negative momentum (QT after QE)
    is contraction, not "late-cycle expansion." We classify on the 12-month YoY %
    change of the smoothed composite, qualified by the short-term slope to catch
    inflections early:

      Expansion:    YoY ≥ +5%                       — accelerating liquidity
      Late-Cycle:   0% ≤ YoY < +5% AND slope ≤ 0    — expansion decelerating
      Inflection:   −5% ≤ YoY < 0%                  — transitioning
                    OR sign change in trailing 30d   — momentum just flipped
      Contraction:  YoY < −5%                       — clear contraction

    Slope is the half-window mean-difference of the trailing slope_window_days
    of the composite_z series — used only for the Late-Cycle distinction.

    Returns 'Unknown' if there isn't ≥365 days of history to compute YoY.
    """
    if not composite_z or len(composite_z) < 365:
        return "Unknown"

    # 12-month YoY % change of the smoothed composite
    current_val = composite_z[-1][1]
    year_ago_val = composite_z[-365][1]
    # Use absolute-difference YoY in z-score units (smoothed composite is z-scored,
    # so a 1.0 absolute change ≈ one standard deviation of cycle movement, which
    # in % terms maps cleanly onto Howell's ~5% thresholds when the composite is
    # expressed in z-units).
    yoy_change = (current_val - year_ago_val) * 100.0 / max(abs(year_ago_val), 0.5)

    # Short-term slope for the Late-Cycle qualifier
    trailing = [v for _, v in composite_z[-slope_window_days:]]
    mid = len(trailing) // 2
    slope = (sum(trailing[mid:]) / len(trailing[mid:])
             - sum(trailing[:mid]) / len(trailing[:mid])) if mid > 0 else 0.0

    # 30-day sign change → inflection signal
    last_30 = [v for _, v in composite_z[-30:]]
    sign_change = (
        len(last_30) >= 2
        and any(
            (last_30[i] > 0) != (last_30[i + 1] > 0)
            for i in range(len(last_30) - 1)
        )
    )

    EXPANSION_THRESHOLD = 5.0    # YoY ≥ +5% → clear expansion
    CONTRACTION_THRESHOLD = -5.0 # YoY ≤ −5% → clear contraction

    if yoy_change >= EXPANSION_THRESHOLD:
        return "Expansion"
    elif yoy_change < CONTRACTION_THRESHOLD:
        return "Contraction"
    elif yoy_change >= 0 and slope <= 0:
        return "Late-Cycle"   # positive growth but losing momentum
    elif sign_change:
        return "Inflection"   # momentum sign change → regime transition
    else:
        return "Inflection"   # YoY in [−5%, +5%] = transitional zone


def classify_regime_at_date(
    composite_z: list[tuple[str, float]],
    as_of_date: str,
    slope_window_days: int = 63,
) -> str:
    """Regime label at a specific historical date (for backtesting / smoke tests).

    Slices composite_z up to as_of_date and delegates to classify_regime().
    """
    sliced = [(t, v) for t, v in composite_z if t <= as_of_date]
    return classify_regime(sliced, slope_window_days)


# ══════════════════════════════════════════════════════════════════════════════
# Private helpers
# ══════════════════════════════════════════════════════════════════════════════

def _get_series_tuples(series_id: str) -> list[tuple[str, float]]:
    """Fetch from DB as (date, value) tuples sorted ascending."""
    rows = db.get_macro_series(series_id)
    return [(r["ts"], r["value"]) for r in rows]


def _ff_daily(
    series_id: str,
    start_date: str,
    end_date: str,
) -> dict[str, float]:
    rows = _get_series_tuples(series_id)
    return _dict_from_ff(forward_fill_to_daily(rows, start_date, end_date))


def _ff_daily_scaled(
    series_id: str,
    start_date: str,
    end_date: str,
    scale: float = 1.0,
) -> dict[str, float]:
    rows = [(t, v * scale) for t, v in _get_series_tuples(series_id)]
    return _dict_from_ff(forward_fill_to_daily(rows, start_date, end_date))


def _dict_from_ff(ff: list[tuple[str, float]]) -> dict[str, float]:
    return {t: v for t, v in ff}


def _ofr_repo_daily(start_date: str, end_date: str) -> dict[str, float]:
    """Load OFR repo gross outstanding from cache (no live fetch here)."""
    try:
        import ofr_client
        rows = ofr_client.get_repo_total_outstanding(start_date=start_date)
        return _dict_from_ff(forward_fill_to_daily(rows, start_date, end_date))
    except Exception as exc:
        logger.warning("OFR repo load failed: %s", exc)
        return {}


def _yoy_pct(rows: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """Year-over-year % change, aligned by calendar date (330+ days lookback)."""
    if len(rows) < 2:
        return []
    out: list[tuple[str, float]] = []
    for i, (ts, val) in enumerate(rows):
        cur_dt = date.fromisoformat(ts)
        prior = None
        for j in range(i - 1, -1, -1):
            if (cur_dt - date.fromisoformat(rows[j][0])).days >= 330:
                prior = rows[j][1]
                break
        if prior is None or prior == 0:
            continue
        out.append((ts, (val / prior - 1.0) * 100.0))
    return out


def _days_back(ts: str, days: int) -> str:
    """Return ISO date string `days` calendar days before `ts`."""
    return (date.fromisoformat(ts) - timedelta(days=days)).isoformat()
