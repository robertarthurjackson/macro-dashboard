"""BIS Locational Banking Statistics client — bulk CSV from data.bis.org.

Downloads the BIS LBS flat CSV ZIP (~350 MB) and extracts the one series we
need: total cross-border claims in USD. The ZIP is cached in data/bis_cache/
so subsequent calls are fast; cache is refreshed when the file is older than
`CACHE_TTL_DAYS` (default 35 days to align with quarterly release cadence).

BIS LBS docs: https://www.bis.org/statistics/bankstats.htm
Bulk download: https://data.bis.org/bulkdownload
"""

from __future__ import annotations

import csv
import io
import logging
import threading
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

import json

logger = logging.getLogger(__name__)

BIS_ZIP_URL = "https://data.bis.org/static/bulk/WS_LBS_D_PUB_csv_flat.zip"
_CACHE_DIR = Path(__file__).parent / "data" / "bis_cache"
_CACHE_FILE = _CACHE_DIR / "WS_LBS_D_PUB_csv_flat.zip"
# Parsed results cache: avoids re-parsing the 340MB ZIP on every call (~250s → <0.1s)
_PARSED_CACHE_FILE = _CACHE_DIR / "lbs_total_claims_parsed.json"
_TIMEOUT = 120  # seconds — large file
_INTER_CALL_DELAY = 1.0
_semaphore = threading.Semaphore(1)  # one BIS download at a time

# BIS LBS flat CSV actual column headers (verified 2026-06-18 from full parse):
#   Columns are "DIMENSION_ID:Label" — e.g. "L_MEASURE:Measure", NOT "L_MEASURE"
#   Cell values are "CODE: Description" — e.g. "S: Amounts outstanding / Stocks"
#   Key columns for our filter:
#     "FREQ:Frequency"                        → "Q: Quarterly"
#     "L_MEASURE:Measure"                     → "S: Amounts outstanding / Stocks"
#     "L_POSITION:Balance sheet position"     → "C: Total claims"
#     "L_CP_COUNTRY:Counterparty country"     → "5J: All countries"
#     "L_DENOM:Currency denomination"         → "USD: US dollar"
#     "L_REP_CTY:Reporting country"           → "5A: All reporting countries"
#     "L_PARENT_CTY:Parent country"           → "5J: All countries" (CRITICAL for dedup)
#     "L_CP_SECTOR:Counterparty sector"       → "A: All sectors"
#     "L_INSTR:Type of instruments"           → "A: All instruments"
#     "L_POS_TYPE:Position type"              → "N: Cross-border"
#   Value columns: "TIME_PERIOD:Time period or range", "OBS_VALUE:Observation Value"
#   Values in millions of USD (UNIT_MULT = 6 = millions). We convert to billions.
#   Note: L_PARENT_CTY is required to select the global aggregate (5J), otherwise
#   each parent-country breakdown (US, GB, JP...) is returned as a separate row.
CACHE_TTL_DAYS = 35

# Filter to: total cross-border claims, all sectors, all instruments, USD, stocks,
# all-reporting countries, all counterparties, parent=all (aggregate row).
# Prefix-matching on cell values (CODE: Description) — we match on the code prefix.
LBS_TOTAL_CLAIMS_FILTER = {
    "FREQ:Frequency":                                "Q:",   # Quarterly
    "L_MEASURE:Measure":                             "S:",   # Amounts outstanding / Stocks
    "L_POSITION:Balance sheet position":             "C:",   # Total claims
    "L_CP_COUNTRY:Counterparty country":             "5J:",  # All countries
    "L_DENOM:Currency denomination":                 "USD:", # US dollar
    "L_REP_CTY:Reporting country":                   "5A:",  # All reporting countries
    "L_PARENT_CTY:Parent country":                   "5J:",  # All (global aggregate)
    "L_CP_SECTOR:Counterparty sector":               "A:",   # All sectors
    "L_INSTR:Type of instruments":                   "A:",   # All instruments
    "L_POS_TYPE:Position type":                      "N:",   # Cross-border
    "L_CURR_TYPE:Currency type of reporting country": "A:",  # All currencies (=D+F+U)
}


def _cache_is_fresh() -> bool:
    """Return True if cached ZIP exists and is younger than CACHE_TTL_DAYS."""
    if not _CACHE_FILE.exists():
        return False
    age = (date.today() - date.fromtimestamp(_CACHE_FILE.stat().st_mtime)).days
    return age < CACHE_TTL_DAYS


def _download_zip() -> bool:
    """Download BIS LBS ZIP to cache. Returns True on success."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading BIS LBS ZIP (~350 MB) to cache...")
    try:
        resp = requests.get(
            BIS_ZIP_URL,
            timeout=_TIMEOUT,
            stream=True,
            headers={"User-Agent": "vol-dashboard/1.0"},
        )
        if resp.status_code != 200:
            logger.warning("BIS ZIP download failed HTTP %s", resp.status_code)
            return False
        with _CACHE_FILE.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)
        logger.info("BIS LBS ZIP cached at %s", _CACHE_FILE)
        return True
    except requests.RequestException as exc:
        logger.warning("BIS ZIP download error: %s", exc)
        return False


def _read_lbs_flat(filter_dims: dict[str, str]) -> list[tuple[str, float]]:
    """Read the cached flat CSV, filtering to series matching filter_dims.

    The flat CSV has one row per observation with columns for each dimension
    plus a TIME_PERIOD and OBS_VALUE column.

    Returns (YYYY-QN → YYYY-MM-DD first-of-quarter) tuples.
    """
    if not _CACHE_FILE.exists():
        logger.warning("BIS LBS cache file not found")
        return []
    try:
        zf = zipfile.ZipFile(_CACHE_FILE)
    except zipfile.BadZipFile as exc:
        logger.warning("BIS LBS cache ZIP corrupt: %s", exc)
        _CACHE_FILE.unlink(missing_ok=True)
        return []

    csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
    if not csv_name:
        logger.warning("No CSV found in BIS LBS ZIP")
        return []

    out: list[tuple[str, float]] = []
    with zf.open(csv_name) as raw:
        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"))
        for row in reader:
            # Prefix-match filter: cell values are "CODE: Description", we match on code prefix.
            match = all(row.get(k, "").strip().startswith(v) for k, v in filter_dims.items())
            if not match:
                continue
            period = row.get("TIME_PERIOD:Time period or range", "").strip()
            raw_val = row.get("OBS_VALUE:Observation Value", "").strip()
            if not period or not raw_val or raw_val == "NaN":
                continue
            try:
                val = float(raw_val)
            except ValueError:
                continue
            if val != val:  # NaN guard
                continue
            # Convert YYYY-QN to YYYY-MM-DD (first month of quarter)
            # Values are in millions of USD; convert to billions.
            ts = _quarter_to_date(period)
            if ts:
                out.append((ts, val / 1000.0))
    return out


def _quarter_to_date(period: str) -> Optional[str]:
    """Convert 'YYYY-Q1' style string to first-of-quarter ISO date."""
    import re
    m = re.match(r"(\d{4})-Q(\d)", period)
    if not m:
        return None
    year, q = int(m.group(1)), int(m.group(2))
    month = (q - 1) * 3 + 1
    return f"{year}-{month:02d}-01"


def _parsed_cache_is_fresh() -> bool:
    """Return True if the parsed JSON cache exists and matches the ZIP mtime."""
    if not _PARSED_CACHE_FILE.exists() or not _CACHE_FILE.exists():
        return False
    # Parsed cache is stale if the ZIP has been updated since it was written
    return _PARSED_CACHE_FILE.stat().st_mtime >= _CACHE_FILE.stat().st_mtime


def _load_parsed_cache() -> list[tuple[str, float]]:
    """Load pre-parsed LBS claims from JSON cache (fast path, <0.1s)."""
    try:
        data = json.loads(_PARSED_CACHE_FILE.read_text(encoding="utf-8"))
        return [(row[0], row[1]) for row in data]
    except Exception as exc:
        logger.warning("BIS parsed cache load failed: %s", exc)
        return []


def _save_parsed_cache(rows: list[tuple[str, float]]) -> None:
    """Persist parsed LBS claims to JSON so subsequent calls skip ZIP parsing."""
    try:
        _PARSED_CACHE_FILE.write_text(
            json.dumps(rows, separators=(",", ":")), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("BIS parsed cache save failed: %s", exc)


def get_lbs_total_claims(refresh: bool = False) -> list[tuple[str, float]]:
    """Return quarterly total cross-border bank claims worldwide (USD billions).

    Downloads and caches the BIS LBS ZIP if not already cached or if refresh=True.
    Cache TTL is 35 days (BIS publishes quarterly with ~3 month lag).

    Parsed results are stored in a JSON sidecar (_PARSED_CACHE_FILE) so repeated
    calls do not re-parse the 340MB ZIP. The sidecar is invalidated whenever the
    ZIP is re-downloaded (mtime comparison).

    Returns list of (YYYY-MM-DD, value) tuples, value in USD billions.
    Raw BIS data is in millions of USD (UNIT_MULT=6); _read_lbs_flat divides by 1000.
    Latest value (~2025-Q4) is ~$20,800B = $20.8T, consistent with BIS public tables.
    """
    with _semaphore:
        if refresh or not _cache_is_fresh():
            ok = _download_zip()
            if not ok:
                logger.warning("BIS LBS data unavailable — returning cached data if any")
        time.sleep(_INTER_CALL_DELAY)

    # Fast path: return pre-parsed results if sidecar is fresh
    if _parsed_cache_is_fresh():
        return _load_parsed_cache()

    # Slow path: parse 340MB ZIP, then save sidecar for future calls
    rows = _read_lbs_flat(LBS_TOTAL_CLAIMS_FILTER)
    if rows:
        _save_parsed_cache(rows)
    return rows


def is_available() -> bool:
    """Return True if the BIS ZIP URL is reachable."""
    try:
        resp = requests.head(BIS_ZIP_URL, timeout=10,
                             headers={"User-Agent": "vol-dashboard/1.0"})
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("BIS availability check failed: %s", exc)
        return False


def backfill_lbs_claims() -> list[tuple[str, float]]:
    """Download full BIS LBS history and return all total cross-border claims.

    Call this once during initial setup. Subsequent calls use cached data.
    """
    return get_lbs_total_claims(refresh=True)
