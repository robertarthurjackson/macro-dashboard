"""OFR Short-Term Funding Monitor client — XLSX download path (openpyxl).

The OFR STFM v1 data API (data.financialresearch.gov/v1) requires AWS auth.
The only unauthenticated data path is the full XLSX file (~8 MB) at:
  https://www.financialresearch.gov/short-term-funding-monitor/data/timeseries/all.xlsx

Phase 2 (2026-06-18): openpyxl approved (Opus decision 7). XLSX path is now active.

Gross repo outstanding = DVP overnight total (REPO-DVP_OV_TOT-P)
                       + Triparty total volume (REPO-TRI_TV_TOT-P)
Values are in USD; we convert to billions. Coverage starts 2018-05-07 for
the combined series; earlier dates have partial segments only.

OFR STFM docs: https://www.financialresearch.gov/short-term-funding-monitor/api/
XLSX download: https://www.financialresearch.gov/short-term-funding-monitor/data/timeseries/all.xlsx
"""

from __future__ import annotations

import io
import logging
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

OFR_XLSX_URL = (
    "https://www.financialresearch.gov"
    "/short-term-funding-monitor/data/timeseries/all.xlsx"
)
_CACHE_DIR = Path(__file__).parent / "data" / "ofr_cache"
_CACHE_FILE = _CACHE_DIR / "ofr_stfm_all.xlsx"
_TIMEOUT = 60  # seconds — ~8 MB file
_CACHE_TTL_DAYS = 3  # daily-frequency data; refresh more often than BIS
_INTER_CALL_DELAY = 0.5
_semaphore = threading.Semaphore(1)

# Column identifiers in the 'repo' sheet (row 3 = header):
#   REPO-DVP_OV_TOT-P: DVP settlement overnight total, preliminary (USD)
#   REPO-TRI_TV_TOT-P: Triparty total volume, preliminary (USD)
# Gross outstanding = DVP_OV_TOT + TRI_TV_TOT, converted to billions.
# Both columns confirmed from header inspection on 2026-06-18.
_DVP_COL = "REPO-DVP_OV_TOT-P"   # col idx 15
_TRI_COL = "REPO-TRI_TV_TOT-P"   # col idx 121


def _cache_is_fresh() -> bool:
    if not _CACHE_FILE.exists():
        return False
    age = (date.today() - date.fromtimestamp(_CACHE_FILE.stat().st_mtime)).days
    return age < _CACHE_TTL_DAYS


def _download_xlsx() -> bool:
    """Download OFR STFM XLSX to cache. Returns True on success."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading OFR STFM XLSX (~8 MB)...")
    try:
        resp = requests.get(
            OFR_XLSX_URL, timeout=_TIMEOUT, stream=True,
            headers={"User-Agent": "vol-dashboard/1.0"},
        )
        if resp.status_code != 200:
            logger.warning("OFR XLSX download failed HTTP %s", resp.status_code)
            return False
        with _CACHE_FILE.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)
        logger.info("OFR XLSX cached at %s (%d bytes)", _CACHE_FILE, _CACHE_FILE.stat().st_size)
        return True
    except requests.RequestException as exc:
        logger.warning("OFR XLSX download error: %s", exc)
        return False


def _parse_xlsx(start_date: Optional[str] = None) -> list[tuple[str, float]]:
    """Parse cached XLSX repo sheet. Returns (YYYY-MM-DD, gross_outstanding_billions)."""
    if not _CACHE_FILE.exists():
        logger.warning("OFR XLSX cache file not found")
        return []
    try:
        import openpyxl
    except ImportError:
        logger.warning("openpyxl not installed — cannot parse OFR XLSX")
        return []
    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(_CACHE_FILE.read_bytes()),
            read_only=True, data_only=True,
        )
    except Exception as exc:
        logger.warning("OFR XLSX parse error: %s", exc)
        _CACHE_FILE.unlink(missing_ok=True)
        return []

    if "repo" not in wb.sheetnames:
        logger.warning("OFR XLSX missing 'repo' sheet — got: %s", wb.sheetnames)
        return []

    ws = wb["repo"]
    dvp_idx = tri_idx = None
    header_found = False
    out: list[tuple[str, float]] = []

    for row in ws.iter_rows(values_only=True):
        if not header_found:
            if row[0] == "date":
                # Locate column indices
                for j, h in enumerate(row):
                    if h == _DVP_COL:
                        dvp_idx = j
                    elif h == _TRI_COL:
                        tri_idx = j
                if dvp_idx is None or tri_idx is None:
                    logger.warning(
                        "OFR XLSX: expected columns %s and %s not found in header",
                        _DVP_COL, _TRI_COL,
                    )
                    return []
                header_found = True
            continue

        ts = row[0]
        if ts is None:
            continue
        ts = str(ts)[:10]  # YYYY-MM-DD
        if start_date and ts < start_date:
            continue

        dvp_raw = row[dvp_idx] if dvp_idx < len(row) else None
        tri_raw = row[tri_idx] if tri_idx < len(row) else None
        if dvp_raw is None or tri_raw is None:
            continue
        try:
            gross_b = (float(dvp_raw) + float(tri_raw)) / 1e9
        except (TypeError, ValueError):
            continue
        if gross_b != gross_b:  # NaN guard
            continue
        out.append((ts, gross_b))

    return sorted(set(out))


def get_repo_total_outstanding(
    start_date: Optional[str] = None,
    refresh: bool = False,
) -> list[tuple[str, float]]:
    """Return daily gross repo outstanding (USD billions) from OFR STFM.

    Gross = DVP overnight outstanding + Triparty total volume.
    Coverage begins 2018-05-07 where both legs are available.

    Downloads and caches the XLSX if not cached or if refresh=True.
    Cache TTL is 3 days (data updates daily on business days).
    """
    with _semaphore:
        if refresh or not _cache_is_fresh():
            ok = _download_xlsx()
            if not ok:
                logger.warning("OFR XLSX unavailable — returning cached data if any")
        time.sleep(_INTER_CALL_DELAY)

    return _parse_xlsx(start_date=start_date)


def is_available() -> bool:
    """Return True if the OFR XLSX URL is reachable."""
    try:
        resp = requests.head(OFR_XLSX_URL, timeout=10,
                             headers={"User-Agent": "vol-dashboard/1.0"})
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("OFR availability check failed: %s", exc)
        return False


def backfill_ofr() -> list[tuple[str, float]]:
    """Fetch full OFR repo history. Returns all available daily data (2014+)."""
    return get_repo_total_outstanding(refresh=True)
