"""Treasury TIC client — monthly foreign UST holdings from ticdata.treasury.gov.

Parses the fixed-width tab-separated Major Foreign Holders file (mfhhis01.txt).
The file contains multiple annual tables, each with monthly columns.
We extract the "Grand Total" row from each annual table.

TIC docs: https://home.treasury.gov/data/treasury-international-capital-tic-system
Source file: https://ticdata.treasury.gov/Publish/mfhhis01.txt
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

TIC_URL = "https://ticdata.treasury.gov/Publish/mfhhis01.txt"
_TIMEOUT = 20  # seconds
_INTER_CALL_DELAY = 0.5
_semaphore = threading.Semaphore(1)

# Month abbreviation → 2-digit month number
_MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def _parse_tic(text: str) -> list[tuple[str, float]]:
    """Parse the TIC mfhhis01.txt format into (YYYY-MM-DD, value) tuples.

    The file has one annual table per year. Each table has:
    - A month-name row (Dec, Nov, Oct ... Jan)
    - A "Country YEAR YEAR ..." header row that gives the year for each column
    - Data rows with "Country\tval1\tval2\t..."
    - A "Grand Total" row with the aggregate

    We extract Grand Total rows from each year's table.
    Returns values in USD billions.
    """
    lines = text.splitlines()
    out: list[tuple[str, float]] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect the "Country\t{year}..." header row
        if line.startswith("Country\t"):
            parts = [p.strip() for p in line.split("\t")]
            # parts[0] = 'Country', parts[1..] = year strings for each column
            # The month row is one line above this
            month_line = lines[i - 1] if i > 0 else ""
            months = [p.strip() for p in month_line.split("\t")]
            # months[0] is blank, months[1..] are month abbreviations
            year_parts = parts[1:]   # list of year strings per column

            # Collect months mapping: column index → YYYY-MM-DD
            col_dates: dict[int, str] = {}
            for col_idx, year_str in enumerate(year_parts):
                year_str = year_str.strip()
                if not year_str.isdigit():
                    continue
                # month is at the same col_idx in months (offset by 1 for blank)
                month_idx = col_idx + 1
                if month_idx >= len(months):
                    continue
                mo_abbr = months[month_idx].strip()
                mo_num = _MONTH_MAP.get(mo_abbr)
                if not mo_num:
                    continue
                col_dates[col_idx] = f"{year_str}-{mo_num}-01"

            # Scan forward to find Grand Total row for this table
            j = i + 1
            while j < len(lines) and j < i + 60:
                tline = lines[j].strip()
                if tline.startswith("Grand Total"):
                    tparts = [p.strip() for p in lines[j].split("\t")]
                    # tparts[0] = 'Grand Total', tparts[1..] = values
                    for col_idx, date_str in col_dates.items():
                        val_idx = col_idx + 1
                        if val_idx >= len(tparts):
                            continue
                        raw = tparts[val_idx].replace(",", "")
                        if not raw:
                            continue
                        try:
                            val = float(raw)
                        except ValueError:
                            continue
                        out.append((date_str, val))
                    break
                j += 1
            i = j + 1
        else:
            i += 1

    # Sort by date and deduplicate (last value wins for any given month)
    seen: dict[str, float] = {}
    for ts, val in out:
        seen[ts] = val
    return sorted(seen.items())


def get_foreign_ust_holdings(start_date: Optional[str] = None) -> list[tuple[str, float]]:
    """Fetch monthly total foreign UST holdings (Grand Total, USD billions).

    start_date: YYYY-MM-DD string; data before this date is excluded.
    Returns list of (YYYY-MM-DD, value) sorted ascending.
    """
    with _semaphore:
        try:
            resp = requests.get(TIC_URL, timeout=_TIMEOUT,
                                headers={"User-Agent": "vol-dashboard/1.0"})
        except requests.RequestException as exc:
            logger.warning("TIC fetch failed: %s", exc)
            return []
        finally:
            time.sleep(_INTER_CALL_DELAY)

    if resp.status_code != 200:
        logger.warning("TIC returned HTTP %s", resp.status_code)
        return []

    rows = _parse_tic(resp.text)
    if start_date:
        rows = [(ts, v) for ts, v in rows if ts >= start_date]
    return rows


def is_available() -> bool:
    """Return True if the TIC data file is reachable."""
    try:
        resp = requests.head(TIC_URL, timeout=10,
                             headers={"User-Agent": "vol-dashboard/1.0"})
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("TIC availability check failed: %s", exc)
        return False


def backfill_tic() -> list[tuple[str, float]]:
    """Fetch full TIC history. Returns all available monthly data."""
    return get_foreign_ust_holdings()
