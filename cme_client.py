"""CME 30-day Fed Funds futures (ZQ contracts) via yfinance.

CME's direct public endpoints (`CmeWS/mvc/Settlements/Futures/Settlements/305/FUT`)
IP-block scrapers with HTTP 403, and their settlements page is a JS-rendered SPA
with no inline data. Yahoo Finance carries individual ZQ contracts under the
`.CBT` suffix (Fed Funds trade on CBOT, part of CME Group), which is the free
path we use here.

For higher resolution (real-time, official settlements, better fill quality),
subscribe to IBKR's CME market data package (~$10-15/mo) and pull via the
`ibapi` client on a FUT contract for ZQ. The storage schema and API surface
stay the same — only the data source swaps.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)

# CME month codes for futures delivery months
_MONTH_CODE = "FGHJKMNQUVXZ"  # F=Jan, G=Feb, H=Mar, ..., Z=Dec

# How many monthly contracts forward to pull. ZQ trades ~36 months out but
# liquidity thins past 24 months.
_MAX_MONTHS_FORWARD = 24

# ~30 days back for the yfinance history call — enough to guarantee the most
# recent settlement for each contract regardless of holidays.
_LOOKBACK_DAYS = 30


def _contract_ticker(delivery_month: date) -> str:
    """Yahoo Finance ticker for a ZQ contract expiring in `delivery_month`.

    Example: April 2027 → `ZQJ27.CBT`
    """
    letter = _MONTH_CODE[delivery_month.month - 1]
    yy = delivery_month.year % 100
    return f"ZQ{letter}{yy:02d}.CBT"


def _iterate_forward_months(n: int) -> list[date]:
    """Return the first-of-month dates for the next `n` months starting with
    the current calendar month.
    """
    today = date.today()
    year, month = today.year, today.month
    out: list[date] = []
    for _ in range(n):
        out.append(date(year, month, 1))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return out


def get_fed_funds_settlements() -> list[dict]:
    """Fetch ZQ contract settlements from Yahoo Finance.

    Returns a list of dicts sorted by delivery_month ascending:
        [
            {"contract": "ZQN26.CBT", "delivery_month": "2026-07-01",
             "settlement": 96.375, "implied_rate": 3.625},
            ...
        ]

    Contracts with no data (e.g. deep-out contracts that don't yet trade) are
    silently skipped. Returns an empty list if the whole call fails.
    """
    rows: list[dict] = []
    for month_start in _iterate_forward_months(_MAX_MONTHS_FORWARD):
        ticker = _contract_ticker(month_start)
        try:
            hist = yf.Ticker(ticker).history(period=f"{_LOOKBACK_DAYS}d")
            if hist is None or hist.empty:
                continue
            close = float(hist["Close"].iloc[-1])
            # Sanity: settlement must be under 100 (implied rate >= 0)
            if close <= 0 or close >= 100:
                continue
            rows.append({
                "contract": ticker,
                "delivery_month": month_start.isoformat(),
                "settlement": round(close, 4),
                "implied_rate": round(100.0 - close, 4),
            })
        except Exception as exc:
            logger.debug("ZQ %s fetch failed: %s", ticker, exc)
            continue

    if not rows:
        logger.warning("No ZQ contracts returned data from yfinance")
    return rows


def is_available() -> bool:
    """Cheap probe: try the front-month continuous ZQ ticker."""
    try:
        hist = yf.Ticker("ZQ=F").history(period="5d")
        return hist is not None and not hist.empty
    except Exception:
        return False
