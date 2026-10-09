"""Shared helpers for the Stress & Auctions tab collectors (stress_*.py).

Contract every stress collector follows:
  - Payload is a JSON-serializable dict with top-level "generated_at",
    "sources" (list of {name, status: ok|error|stub, as_of, note}) and data.
  - Every figure carries an "as_of" date and a "source".
  - Paid-only data (Bloomberg/LSEG) appears with status "stub" and a note,
    never silently omitted.
  - One failing source must never fail the build: catch, log, mark "error".
"""
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger("stress")

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "site" / "data" / "stress"
SEED_DIR = ROOT / "seed" / "stress"

UA = {"User-Agent": "Mozilla/5.0 (macro-dashboard; +https://robroth.ca/macro-dashboard/)"}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def http_get(url: str, timeout: int = 30, **kw) -> requests.Response:
    headers = {**UA, **kw.pop("headers", {})}
    r = requests.get(url, headers=headers, timeout=timeout, **kw)
    r.raise_for_status()
    return r


def fred_series(series_id: str, start: str = "2000-01-01") -> pd.Series:
    """Daily/weekly/monthly FRED series as a float Series indexed by date.
    Uses the API when FRED_API_KEY is set, else the public fredgraph CSV."""
    key = os.environ.get("FRED_API_KEY")
    if key:
        r = http_get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id": series_id, "api_key": key,
                             "file_type": "json", "observation_start": start})
        obs = r.json()["observations"]
        s = pd.Series({o["date"]: o["value"] for o in obs})
    else:
        r = http_get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                     params={"id": series_id, "cosd": start})
        df = pd.read_csv(io.StringIO(r.text))
        s = pd.Series(df.iloc[:, 1].values, index=df.iloc[:, 0].values)
    s = pd.to_numeric(s.replace(".", None), errors="coerce").dropna()
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


def source(name: str, status: str, as_of: str | None = None, note: str = "") -> dict:
    return {"name": name, "status": status, "as_of": as_of, "note": note}


def dump(name: str, obj: dict) -> Path:
    """Write site/data/stress/<name>.json."""
    path = DATA_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, default=str, separators=(",", ":")))
    logger.info("wrote %s (%d bytes)", path.relative_to(ROOT), path.stat().st_size)
    return path


def load_seed(name: str, default=None):
    path = SEED_DIR / name
    if path.exists():
        return json.loads(path.read_text())
    return default


def save_seed(name: str, obj) -> None:
    """Persist accumulated history under seed/stress/ (the workflow commits
    seed/ back, since the daily build is otherwise stateless)."""
    path = SEED_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, default=str, indent=0))


def seed_age_days(name: str) -> float | None:
    """Days since the seed's own "fetched_at" stamp (for weekly cadences)."""
    seed = load_seed(name)
    if not seed or "fetched_at" not in seed:
        return None
    ts = datetime.fromisoformat(seed["fetched_at"].replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - ts).total_seconds() / 86400
