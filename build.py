#!/usr/bin/env python3
"""Build the static data for the public macro dashboard.

Stateless: every run creates a fresh SQLite workspace, backfills full history
from the free sources (FRED, yfinance, SPDR, iShares, NY Fed, Treasury
FiscalData, OFR/BIS/TIC), then exports every payload the frontend needs as
JSON under site/data/.

The one stateful exception is IBIT BTC-in-trust: iShares publishes only the
current day's holdings, so accumulated history lives in seed/ibit_btc.json,
which this script merges in and re-exports with today's point appended. The
CI workflow commits the updated seed back to the repo.
"""
import json
import logging
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("build")

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "site" / "data"
SEED_FILE = ROOT / "seed" / "ibit_btc.json"

import database as db          # noqa: E402
import macro_collector as mc   # noqa: E402
import payloads                # noqa: E402


def _dump(name: str, obj) -> None:
    path = DATA_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, separators=(",", ":")))
    logger.info("wrote %s (%.0f KB)", name, path.stat().st_size / 1024)


def _fetch_index_history(ticker: str, years: int = 12) -> list[dict]:
    """VIX/VVIX daily closes straight from yfinance (the private app keeps
    these in index_snapshots via its vol collector, which isn't ported)."""
    import yfinance as yf
    start = (date.today() - timedelta(days=int(years * 365.25))).isoformat()
    hist = yf.Ticker(ticker).history(start=start, auto_adjust=False)
    out = []
    for ts, row in hist.iterrows():
        v = float(row.get("Close"))
        if v == v:
            out.append({"ts": ts.strftime("%Y-%m-%d"), "value": round(v, 2)})
    return out


def main() -> None:
    db.init_db()

    logger.info("Backfilling macro catalog (50y where available)…")
    results = mc.backfill_macro_all(years=50)
    failed = [sid for sid, n in results.items()
              if n == 0 and not db.get_latest_macro_value(sid)]
    if failed:
        logger.warning("Series with no data at all: %s", failed)

    # IBIT seed: merge accumulated history, then re-export with today's point
    if SEED_FILE.exists():
        seed = json.loads(SEED_FILE.read_text())
        db.insert_macro_observations("IBIT_BTC", [(r["ts"], r["value"]) for r in seed])
    ibit_rows = db.get_macro_series("IBIT_BTC")
    SEED_FILE.parent.mkdir(parents=True, exist_ok=True)
    SEED_FILE.write_text(json.dumps(
        [{"ts": r["ts"], "value": r["value"]} for r in ibit_rows], indent=0))
    logger.info("IBIT seed: %d accumulated observations", len(ibit_rows))

    # VIX / VVIX from yfinance
    index_rows = {"VIX": _fetch_index_history("^VIX"),
                  "VVIX": _fetch_index_history("^VVIX")}

    # ── Macro exports ────────────────────────────────────────────────────────
    _dump("macro_overview.json", payloads.build_macro_overview(index_rows))
    _dump("macro_velocity.json", {"instruments": mc.get_velocity_board()})

    exported = set()
    for meta in db.list_macro_metadata():
        sid = meta["series_id"]
        _dump(f"series/{sid}.json", payloads.build_series(sid))
        exported.add(sid)
    for sid in payloads.COMPUTED_SERIES:
        _dump(f"series/{sid}.json", payloads.build_series(sid))
    for sym, rows in index_rows.items():
        _dump(f"series/{sym}.json",
              {"series_id": sym, "computed": False, "observations": rows, "count": len(rows)})

    # ── Forward curves ───────────────────────────────────────────────────────
    for name, fn in (("fwd_treasury.json", payloads.build_fwd_treasury),
                     ("fwd_fedfunds.json", payloads.build_fwd_fedfunds)):
        try:
            _dump(name, fn())
        except Exception as exc:
            logger.warning("%s failed: %s", name, exc)

    # ── Liquidity exports (pre-built per range bucket) ───────────────────────
    buckets = [(365, "365"), (730, "730"), (1825, "1825"), (3650, "3650"), (0, "all")]
    for days, label in buckets:
        try:
            _dump(f"liq_headline_{label}.json", payloads.build_liq_headline(days))
            for sub in ("cb", "private", "xborder"):
                _dump(f"liq_sub_{sub}_{label}.json", payloads.build_liq_subindex(sub, days))
        except Exception as exc:
            logger.warning("liquidity bucket %s failed: %s", label, exc)
    try:
        _dump("liq_quality.json", payloads.build_liq_quality(1825))
    except Exception as exc:
        logger.warning("liq_quality failed: %s", exc)

    logger.info("Build complete.")


if __name__ == "__main__":
    main()
