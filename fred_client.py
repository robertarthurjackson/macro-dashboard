"""FRED API client — single abstraction layer for FRED REST calls.

Loads FRED_API_KEY from a project-local .env file (or process env). All
external modules should call get_series() here rather than hitting FRED directly.

FRED API docs: https://fred.stlouisfed.org/docs/api/fred/
Rate limit: ~120 requests/min for the public API. We throttle to 2 concurrent
requests with a small inter-call delay; daily backfills stay well under the cap.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred"
_TIMEOUT = 15  # seconds
_INTER_CALL_DELAY = 0.05  # 50 ms between calls
_semaphore = threading.Semaphore(2)


def _load_env_file() -> None:
    """Load KEY=value pairs from project-local .env into os.environ if not set."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    try:
        with env_path.open() as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError as exc:
        logger.warning("Could not read .env file: %s", exc)


_load_env_file()


def _api_key() -> str:
    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FRED_API_KEY is not set — add it to .env in the project root "
            "(register a free key at fred.stlouisfed.org/docs/api/api_key.html)"
        )
    return key


def is_available() -> bool:
    """Return True if the FRED API responds to a tiny health-check call."""
    try:
        params = {"api_key": _api_key(), "file_type": "json", "limit": "1"}
        resp = requests.get(f"{FRED_BASE}/series/observations",
                            params={**params, "series_id": "DFF"},
                            timeout=5)
        return resp.status_code == 200
    except Exception as exc:
        logger.debug("FRED health check failed: %s", exc)
        return False


def get_series(
    series_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> list[tuple[str, float]]:
    """Fetch (date, value) tuples for a FRED series.

    Returns an empty list on error (logged). FRED uses "." for missing values;
    those rows are silently dropped.

    start_date / end_date are YYYY-MM-DD strings; both optional.
    """
    params: dict[str, str] = {
        "series_id": series_id,
        "api_key": _api_key(),
        "file_type": "json",
    }
    if start_date:
        params["observation_start"] = start_date
    if end_date:
        params["observation_end"] = end_date

    with _semaphore:
        try:
            resp = requests.get(f"{FRED_BASE}/series/observations",
                                params=params, timeout=_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("FRED get_series %s failed: %s", series_id, exc)
            return []
        finally:
            time.sleep(_INTER_CALL_DELAY)

    if resp.status_code == 429:
        logger.warning("FRED rate-limited on %s — backing off 30s", series_id)
        time.sleep(30)
        return []
    if resp.status_code != 200:
        logger.warning("FRED %s returned HTTP %s: %s",
                       series_id, resp.status_code, resp.text[:200])
        return []

    try:
        data = resp.json()
    except ValueError as exc:
        logger.warning("FRED %s returned non-JSON: %s", series_id, exc)
        return []

    out: list[tuple[str, float]] = []
    for obs in data.get("observations", []):
        ts = obs.get("date")
        raw = obs.get("value")
        if not ts or raw is None or raw == "." or raw == "":
            continue
        try:
            out.append((ts, float(raw)))
        except (TypeError, ValueError):
            continue
    return out


def get_latest_value(series_id: str) -> Optional[tuple[str, float]]:
    """Return the most recent (date, value) tuple for a series, or None."""
    params = {
        "series_id": series_id,
        "api_key": _api_key(),
        "file_type": "json",
        "sort_order": "desc",
        "limit": "1",
    }
    with _semaphore:
        try:
            resp = requests.get(f"{FRED_BASE}/series/observations",
                                params=params, timeout=_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("FRED get_latest_value %s failed: %s", series_id, exc)
            return None
        finally:
            time.sleep(_INTER_CALL_DELAY)
    if resp.status_code != 200:
        return None
    try:
        obs = resp.json().get("observations", [])
        if not obs:
            return None
        row = obs[0]
        if row.get("value") in (".", "", None):
            return None
        return (row["date"], float(row["value"]))
    except (ValueError, KeyError, TypeError):
        return None
