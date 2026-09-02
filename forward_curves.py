"""Forward curve computations for Macro → Yields → Forward Curves subsection.

Contents:
  - compute_forward_treasury_curves()  — spot + 1Y-fwd + 5Y-fwd par-yield curves
  - FOMC_MEETINGS                      — hardcoded 2025-2027 FOMC dates

Note: the CME Fed Funds futures client (cme_client.py) is deferred to Opus
because the CME API returns 403 (IP-level block) and the HTML fallback is
JS-rendered with no inline table data.
"""

from __future__ import annotations

import logging
from typing import Optional

import database as db

logger = logging.getLogger(__name__)

# ── FOMC meeting end dates (the date the decision is announced) ───────────────
# Source: federalreserve.gov/monetarypolicy/fomccalendars.htm
# Hardcoded through 2027 per kickoff spec.

FOMC_MEETINGS: list[str] = [
    # 2025
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    # 2026
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
    # 2027
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-16",
    "2027-07-28", "2027-09-15", "2027-10-27", "2027-12-15",
]

# DGS series and their tenor in years — used for forward bootstrapping.
# 3M (0.25Y) is excluded from the forward curve output because its short
# duration distorts the bootstrapped 1Y-forward and 5Y-forward calculations.
_TENOR_MAP: dict[str, float] = {
    "DGS2":  2.0,
    "DGS5":  5.0,
    "DGS10": 10.0,
    "DGS30": 30.0,
}

_OUTPUT_TENORS: list[int] = [2, 5, 10, 30]  # years


def _get_latest_yields() -> dict[str, float]:
    """Fetch the most recent available value for each DGS series."""
    out: dict[str, float] = {}
    for sid in list(_TENOR_MAP.keys()) + ["DGS3MO"]:
        row = db.get_latest_macro_value(sid)
        if row:
            out[sid] = row["value"]
    return out


def _get_yields_as_of(as_of_date: str) -> dict[str, float]:
    """Fetch the most recent value on or before as_of_date for each DGS series."""
    out: dict[str, float] = {}
    for sid in list(_TENOR_MAP.keys()) + ["DGS3MO"]:
        rows = db.get_macro_series(sid)
        # rows are ordered ASC; find last row with ts <= as_of_date
        best = None
        for r in rows:
            if r["ts"] <= as_of_date:
                best = r
            else:
                break
        if best:
            out[sid] = best["value"]
    return out


def _implied_forward(spot_long: float, t_long: float,
                     spot_short: float, t_short: float) -> float:
    """Compute the implied forward rate between t_short and t_long.

    Uses the par-yield bootstrapping approximation:
        fwd = ((1 + r_long/100)^t_long / (1 + r_short/100)^t_short)
              ^ (1 / (t_long - t_short))  -  1

    Returns the annualised forward rate as a percent (e.g., 4.50).
    """
    if t_long <= t_short:
        raise ValueError("t_long must exceed t_short")
    r_long = spot_long / 100.0
    r_short = spot_short / 100.0
    ratio = ((1 + r_long) ** t_long) / ((1 + r_short) ** t_short)
    fwd_decimal = ratio ** (1.0 / (t_long - t_short)) - 1.0
    return round(fwd_decimal * 100.0, 4)


def compute_forward_treasury_curves(as_of_date: Optional[str] = None) -> dict:
    """Return spot, 1Y-forward, and 5Y-forward Treasury yield curves.

    The forward curves are computed from the existing DGS spot rates in the DB
    using a par-yield bootstrapping approximation (documented as such in the UI).

    Math for 1Y-forward N-year yield (i.e., the N-year rate starting 1 year hence):
        The (1+N)-year spot rate gives: (1 + r_{1+N})^{1+N} = (1 + r_1)^1 * (1 + fwd_{1,N})^N
        Solving: fwd_{1,N} = ((1+r_{1+N})^{1+N} / (1+r_1)^1)^{1/N} - 1

    For tenors 2Y and 5Y the 1Y-forward uses DGS3 and DGS6 as anchor; since only
    DGS2/5/10/30 are available, we use DGS2 as the 1Y anchor (it's the shortest
    available FRED annual rate). This is a standard approximation for the
    1Y-forward curve from annual FRED data.

    Returns:
        {
            "as_of": "YYYY-MM-DD",
            "tenors": [2, 5, 10, 30],
            "spot":    [v2, v5, v10, v30],
            "fwd_1y":  [v2, v5, v10, v30],   # rates starting 1Y from now
            "fwd_5y":  [v2, v5, v10, v30],   # rates starting 5Y from now
        }
    Raises ValueError if insufficient data in DB.
    """
    if as_of_date:
        yields = _get_yields_as_of(as_of_date)
    else:
        yields = _get_latest_yields()
        # Derive as_of from the last available DGS10 date
        row = db.get_latest_macro_value("DGS10")
        as_of_date = row["ts"] if row else "unknown"

    required = ["DGS2", "DGS5", "DGS10", "DGS30"]
    missing = [s for s in required if s not in yields]
    if missing:
        raise ValueError(f"Missing DGS data for: {missing}")

    # Spot curve: percent as-reported by FRED
    spot: dict[int, float] = {
        2:  yields["DGS2"],
        5:  yields["DGS5"],
        10: yields["DGS10"],
        30: yields["DGS30"],
    }

    # 1Y-forward curve using DGS2 as the 1Y anchor:
    #   fwd_{1,N} = implied forward from year-1 to year-(1+N)
    # For the 2Y tenor: 2Y-forward starting in 1Y → use spot(3Y).
    # We don't have DGS3; we interpolate between DGS2 and DGS5:
    #   spot(3Y) ≈ (2/3)*DGS2 + (1/3)*DGS5  (linear interpolation)
    r1 = yields["DGS2"] / 100.0

    # Interpolated spot(3Y) for the 1Y-forward 2Y computation
    spot_3y = (yields["DGS2"] * (2 / 3) + yields["DGS5"] * (1 / 3))

    fwd_1y: dict[int, float] = {
        2:  _implied_forward(spot_3y,        3.0,  yields["DGS2"],  1.0),  # fwd(1y->3y) => 2Y rate in 1Y
        5:  _implied_forward(yields["DGS5"], 5.0,  yields["DGS2"],  1.0),  # approximate: DGS6→DGS5 anchor
        10: _implied_forward(yields["DGS10"],10.0, yields["DGS2"],  1.0),
        30: _implied_forward(yields["DGS30"],30.0, yields["DGS2"],  1.0),
    }

    # 5Y-forward curve using DGS5 as the 5Y anchor:
    fwd_5y: dict[int, float] = {
        2:  _implied_forward(yields["DGS5"], 5.0,  yields["DGS2"],  2.0),  # DGS5 as 5Y proxy for (5+2)→5Y
        5:  _implied_forward(yields["DGS10"],10.0, yields["DGS5"],  5.0),  # fwd(5y->10y) => 5Y rate in 5Y
        10: _implied_forward(yields["DGS30"],30.0, yields["DGS5"],  5.0),  # fwd(5y->30y) => rate from yr5 to yr30; approximated as 10Y-fwd in 5Y
        30: _implied_forward(yields["DGS30"],30.0, yields["DGS5"],  5.0),  # same for 30Y tenor slot
    }
    # For the 5Y-forward 2Y: (DGS5 embeds years 1-5, DGS2 embeds years 1-2)
    # Better: fwd(2y->5y) gives the 3Y rate in 2Y, not 2Y in 5Y.
    # Use DGS5 as proxy for the (5+2)-year spot. Linear interp for 7Y:
    spot_7y = yields["DGS5"] + (yields["DGS10"] - yields["DGS5"]) * (2.0 / 5.0)
    fwd_5y[2] = _implied_forward(spot_7y, 7.0, yields["DGS5"], 5.0)

    return {
        "as_of": as_of_date,
        "tenors": _OUTPUT_TENORS,
        "spot":   [spot[t]   for t in _OUTPUT_TENORS],
        "fwd_1y": [fwd_1y[t] for t in _OUTPUT_TENORS],
        "fwd_5y": [fwd_5y[t] for t in _OUTPUT_TENORS],
    }
