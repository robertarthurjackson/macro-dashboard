"""SQLite database layer — auto-creates data/vol_history.db on first run."""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "data" / "vol_history.db"


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS dvol_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                timestamp TEXT    NOT NULL,
                dvol      REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dvol
                ON dvol_snapshots (symbol, timestamp);

            CREATE TABLE IF NOT EXISTS iv_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                timestamp TEXT    NOT NULL,
                iv        REAL,
                rv_20d    REAL,
                rv_30d    REAL,
                vrp       REAL,
                source    TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_iv
                ON iv_snapshots (symbol, timestamp);

            CREATE TABLE IF NOT EXISTS trades (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol        TEXT    NOT NULL,
                mode          TEXT    NOT NULL DEFAULT 'PAPER',
                strategy      TEXT    NOT NULL,
                status        TEXT    NOT NULL DEFAULT 'open',
                opened_at     TEXT    NOT NULL,
                closed_at     TEXT,
                expiry_date   TEXT    NOT NULL,
                dte_at_entry  INTEGER,
                underlying_at_entry REAL,
                notes         TEXT    DEFAULT '',
                created_at    TEXT    NOT NULL,
                updated_at    TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_status
                ON trades (status, mode);

            CREATE TABLE IF NOT EXISTS trade_legs (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id           INTEGER NOT NULL REFERENCES trades(id) ON DELETE CASCADE,
                leg_label          TEXT    DEFAULT '',
                direction          TEXT    NOT NULL,
                option_type        TEXT    NOT NULL,
                strike             REAL    NOT NULL,
                iv_at_entry        REAL,
                iv_current         REAL,
                iv_current_ts      TEXT,
                premium            REAL    NOT NULL,
                contracts          INTEGER NOT NULL DEFAULT 1,
                close_premium      REAL,
                is_hedge           INTEGER NOT NULL DEFAULT 0,
                hedge_target_delta REAL
            );
            CREATE INDEX IF NOT EXISTS idx_trade_legs_trade
                ON trade_legs (trade_id);

            CREATE TABLE IF NOT EXISTS scanner_results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                scan_ts     TEXT    NOT NULL,
                rv_30d      REAL,
                rv_zscore   REAL,
                vol_momentum REAL,
                iv_rank     REAL,
                data_source TEXT,
                UNIQUE(symbol, scan_ts)
            );
            CREATE INDEX IF NOT EXISTS idx_scanner_scan_ts
                ON scanner_results (scan_ts);
            CREATE INDEX IF NOT EXISTS idx_scanner_symbol
                ON scanner_results (symbol, scan_ts);

            CREATE TABLE IF NOT EXISTS watchlist (
                symbol   TEXT PRIMARY KEY,
                added_ts TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS index_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                timestamp TEXT    NOT NULL,
                value     REAL    NOT NULL,
                source    TEXT    DEFAULT 'yfinance'
            );
            CREATE INDEX IF NOT EXISTS idx_index_snapshots
                ON index_snapshots (symbol, timestamp);

            CREATE TABLE IF NOT EXISTS pnl_history (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id      INTEGER NOT NULL,
                snapshot_ts   TEXT    NOT NULL,
                spot_price    REAL,
                delta_pnl     REAL,
                theta_pnl     REAL,
                vega_pnl      REAL,
                residual_pnl  REAL,
                total_pnl     REAL
            );
            CREATE INDEX IF NOT EXISTS idx_pnl_history_trade
                ON pnl_history (trade_id, snapshot_ts);

            CREATE TABLE IF NOT EXISTS macro_series (
                series_id  TEXT NOT NULL,
                ts         TEXT NOT NULL,
                value      REAL NOT NULL,
                PRIMARY KEY (series_id, ts)
            );
            CREATE INDEX IF NOT EXISTS idx_macro_series_id_ts
                ON macro_series (series_id, ts);

            CREATE TABLE IF NOT EXISTS macro_metadata (
                series_id     TEXT PRIMARY KEY,
                display_name  TEXT NOT NULL,
                category      TEXT,
                units         TEXT,
                frequency     TEXT,
                source        TEXT NOT NULL DEFAULT 'fred',
                last_updated  TEXT
            );

            CREATE TABLE IF NOT EXISTS fed_funds_futures (
                snapshot_date   TEXT NOT NULL,
                contract        TEXT NOT NULL,
                delivery_month  TEXT NOT NULL,
                settlement      REAL NOT NULL,
                implied_rate    REAL NOT NULL,
                PRIMARY KEY (snapshot_date, contract)
            );
            CREATE INDEX IF NOT EXISTS idx_ffz_delivery
                ON fed_funds_futures (delivery_month);

            CREATE TABLE IF NOT EXISTS trade_journal (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                idea_date       TEXT    NOT NULL,
                thesis          TEXT    NOT NULL,
                trade_decision  TEXT,
                status          TEXT    NOT NULL DEFAULT 'Open',
                symbol          TEXT,
                created_at      TEXT    NOT NULL,
                updated_at      TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_journal_idea_date ON trade_journal(idea_date DESC);
            CREATE INDEX IF NOT EXISTS idx_journal_status    ON trade_journal(status);
        """)
        # Migrate: add iv_current columns if missing (existing DBs)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(trade_legs)").fetchall()}
        if "iv_current" not in cols:
            conn.execute("ALTER TABLE trade_legs ADD COLUMN iv_current REAL")
            conn.execute("ALTER TABLE trade_legs ADD COLUMN iv_current_ts TEXT")

        # Migrate: add per-leg status and closed_at for individual leg closing
        if "status" not in cols:
            conn.execute("ALTER TABLE trade_legs ADD COLUMN status TEXT NOT NULL DEFAULT 'open'")
            conn.execute("ALTER TABLE trade_legs ADD COLUMN closed_at TEXT")

        # Migrate: add source column to iv_snapshots if missing
        cols_iv = {r[1] for r in conn.execute("PRAGMA table_info(iv_snapshots)").fetchall()}
        if "source" not in cols_iv:
            conn.execute("ALTER TABLE iv_snapshots ADD COLUMN source TEXT")
        if "rv_20d" not in cols_iv:
            conn.execute("ALTER TABLE iv_snapshots ADD COLUMN rv_20d REAL")
        if "iv_delta" not in cols_iv:
            conn.execute("ALTER TABLE iv_snapshots ADD COLUMN iv_delta REAL")
        if "iv_vega" not in cols_iv:
            conn.execute("ALTER TABLE iv_snapshots ADD COLUMN iv_vega REAL")
        if "iv_theta" not in cols_iv:
            conn.execute("ALTER TABLE iv_snapshots ADD COLUMN iv_theta REAL")

        # Migrate: add iv_percentile column to scanner_results if missing
        cols_scan = {r[1] for r in conn.execute("PRAGMA table_info(scanner_results)").fetchall()}
        if "iv_percentile" not in cols_scan:
            conn.execute("ALTER TABLE scanner_results ADD COLUMN iv_percentile REAL")


# ── DVOL ──────────────────────────────────────────────────────────────────────

def insert_dvol(symbol: str, dvol: float, ts: Optional[datetime] = None) -> None:
    t = (ts or datetime.now(timezone.utc)).isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO dvol_snapshots (symbol, timestamp, dvol) VALUES (?, ?, ?)",
            (symbol.upper(), t, dvol),
        )


def get_latest_dvol(symbol: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM dvol_snapshots WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
    return dict(row) if row else None


def get_dvol_history(symbol: str, days: int = 30) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM dvol_snapshots
               WHERE symbol=? AND timestamp >= datetime('now', ?)
               ORDER BY timestamp ASC""",
            (symbol.upper(), f"-{days} days"),
        ).fetchall()
    return [dict(r) for r in rows]


def get_dvol_range(symbol: str, days: int) -> tuple[Optional[float], Optional[float]]:
    """Return (min, max) DVOL over the last `days` days."""
    with get_conn() as conn:
        row = conn.execute(
            """SELECT MIN(dvol), MAX(dvol) FROM dvol_snapshots
               WHERE symbol=? AND timestamp >= datetime('now', ?)""",
            (symbol.upper(), f"-{days} days"),
        ).fetchone()
    if row and row[0] is not None:
        return row[0], row[1]
    return None, None


# ── IV snapshots (equity + crypto) ────────────────────────────────────────────

def insert_iv_snapshot(
    symbol: str,
    iv: Optional[float],
    rv_30d: Optional[float],
    vrp: Optional[float],
    ts: Optional[datetime] = None,
    source: Optional[str] = None,
    rv_20d: Optional[float] = None,
    iv_delta: Optional[float] = None,
    iv_vega: Optional[float] = None,
    iv_theta: Optional[float] = None,
) -> None:
    t = (ts or datetime.now(timezone.utc)).isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO iv_snapshots
               (symbol, timestamp, iv, rv_20d, rv_30d, vrp, source, iv_delta, iv_vega, iv_theta)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol.upper(), t, iv, rv_20d, rv_30d, vrp, source, iv_delta, iv_vega, iv_theta),
        )


def count_iv_snapshots(symbol: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM iv_snapshots WHERE symbol=?",
            (symbol.upper(),),
        ).fetchone()
    return row[0] if row else 0


def count_iv_by_source(symbol: str, source: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM iv_snapshots WHERE symbol=? AND source=?",
            (symbol.upper(), source),
        ).fetchone()
    return row[0] if row else 0


def delete_iv_by_symbol(symbol: str) -> int:
    """Delete ALL iv_snapshots rows for a symbol. Returns rows deleted."""
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM iv_snapshots WHERE symbol=?",
            (symbol.upper(),),
        )
        return cur.rowcount


def get_iv_dates(symbol: str) -> set[str]:
    """Return set of YYYY-MM-DD date prefixes already present for `symbol`."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT substr(timestamp,1,10) FROM iv_snapshots WHERE symbol=?",
            (symbol.upper(),),
        ).fetchall()
    return {r[0] for r in rows}


def get_latest_iv_snapshot(symbol: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM iv_snapshots WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
    return dict(row) if row else None


def get_iv_history(symbol: str, days: int = 365) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM iv_snapshots
               WHERE symbol=? AND timestamp >= datetime('now', ?)
               ORDER BY timestamp ASC""",
            (symbol.upper(), f"-{days} days"),
        ).fetchall()
    return [dict(r) for r in rows]


def get_iv_52w_stats(symbol: str) -> dict:
    """Return {lo, hi, n_days, values} for non-null IV over the last 365 days,
    deduped to one observation per calendar day. Used for IV Rank/Percentile.

    IV readings below 5% are excluded — no real equity ATM option has IV that low;
    such values are artifacts of bad strike selection on illiquid chains.

    `values` is the full sorted list of daily IV readings for percentile calculation.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT substr(timestamp,1,10) AS d, iv
               FROM iv_snapshots
               WHERE symbol=? AND iv IS NOT NULL AND iv >= 5
                 AND timestamp >= datetime('now', '-365 days')
               ORDER BY timestamp ASC""",
            (symbol.upper(),),
        ).fetchall()
    by_day: dict[str, float] = {}
    for r in rows:
        by_day[r[0]] = r[1]  # latest reading wins (rows ordered ASC)
    if not by_day:
        return {"lo": None, "hi": None, "n_days": 0, "values": []}
    vals = sorted(by_day.values())
    return {"lo": vals[0], "hi": vals[-1], "n_days": len(vals), "values": vals}


def get_dvol_52w_stats(symbol: str) -> dict:
    """Same as get_iv_52w_stats but for crypto DVOL snapshots."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT substr(timestamp,1,10) AS d, dvol
               FROM dvol_snapshots
               WHERE symbol=?
                 AND timestamp >= datetime('now', '-365 days')
               ORDER BY timestamp ASC""",
            (symbol.upper(),),
        ).fetchall()
    by_day: dict[str, float] = {}
    for r in rows:
        by_day[r[0]] = r[1]
    if not by_day:
        return {"lo": None, "hi": None, "n_days": 0, "values": []}
    vals = sorted(by_day.values())
    return {"lo": vals[0], "hi": vals[-1], "n_days": len(vals), "values": vals}


def update_rv20_by_date(symbol: str, by_date: dict[str, float]) -> int:
    """Bulk-update rv_20d in iv_snapshots, matching on date prefix (YYYY-MM-DD).

    Returns number of rows updated. Only touches rows whose date matches a key
    in `by_date`; rows without a match are left unchanged.
    """
    if not by_date:
        return 0
    sym = symbol.upper()
    updated = 0
    with get_conn() as conn:
        for date_key, val in by_date.items():
            cur = conn.execute(
                """UPDATE iv_snapshots
                   SET rv_20d=?
                   WHERE symbol=? AND substr(timestamp,1,10)=?""",
                (val, sym, date_key),
            )
            updated += cur.rowcount
    return updated


def get_latest_rv_before(symbol: str, ts_iso: str) -> dict:
    """Return the most recent {rv_20d, rv_30d} from iv_snapshots strictly before
    `ts_iso`. Used to seed forward-fill when a chart window opens before the
    first in-window RV row."""
    with get_conn() as conn:
        r20 = conn.execute(
            """SELECT rv_20d FROM iv_snapshots
               WHERE symbol=? AND rv_20d IS NOT NULL AND timestamp < ?
               ORDER BY timestamp DESC LIMIT 1""",
            (symbol.upper(), ts_iso),
        ).fetchone()
        r30 = conn.execute(
            """SELECT rv_30d FROM iv_snapshots
               WHERE symbol=? AND rv_30d IS NOT NULL AND timestamp < ?
               ORDER BY timestamp DESC LIMIT 1""",
            (symbol.upper(), ts_iso),
        ).fetchone()
    return {
        "rv_20d": r20[0] if r20 else None,
        "rv_30d": r30[0] if r30 else None,
    }


def get_iv_range(symbol: str, days: int) -> tuple[Optional[float], Optional[float]]:
    """Return (min_iv, max_iv) from iv_snapshots over the last `days` days."""
    with get_conn() as conn:
        row = conn.execute(
            """SELECT MIN(iv), MAX(iv) FROM iv_snapshots
               WHERE symbol=? AND timestamp >= datetime('now', ?) AND iv IS NOT NULL""",
            (symbol.upper(), f"-{days} days"),
        ).fetchone()
    if row and row[0] is not None:
        return row[0], row[1]
    return None, None


# ── Trades ────────────────────────────────────────────────────────────────────

def insert_trade(
    symbol: str,
    mode: str,
    strategy: str,
    expiry_date: str,
    dte_at_entry: int,
    underlying_at_entry: Optional[float] = None,
    notes: str = "",
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (symbol, mode, strategy, status, opened_at, expiry_date,
                dte_at_entry, underlying_at_entry, notes, created_at, updated_at)
               VALUES (?,?,?,'open',?,?,?,?,?,?,?)""",
            (symbol.upper(), mode.upper(), strategy, now, expiry_date,
             dte_at_entry, underlying_at_entry, notes, now, now),
        )
        return cur.lastrowid


def insert_trade_leg(
    trade_id: int,
    direction: str,
    option_type: str,
    strike: float,
    premium: float,
    contracts: int = 1,
    iv_at_entry: Optional[float] = None,
    leg_label: str = "",
    is_hedge: bool = False,
    hedge_target_delta: Optional[float] = None,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO trade_legs
               (trade_id, leg_label, direction, option_type, strike,
                iv_at_entry, premium, contracts, is_hedge, hedge_target_delta)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (trade_id, leg_label, direction.lower(), option_type.lower(),
             strike, iv_at_entry, premium, contracts,
             1 if is_hedge else 0, hedge_target_delta),
        )
        return cur.lastrowid


def get_trade(trade_id: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
        if not row:
            return None
        trade = dict(row)
        legs = conn.execute(
            "SELECT * FROM trade_legs WHERE trade_id=? ORDER BY id", (trade_id,)
        ).fetchall()
        trade["legs"] = [dict(l) for l in legs]
    return trade


def get_trades(mode: Optional[str] = None, status: Optional[str] = None) -> list[dict]:
    clauses, params = [], []
    if mode:
        clauses.append("mode=?")
        params.append(mode.upper())
    if status:
        clauses.append("status=?")
        params.append(status.lower())
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM trades{where} ORDER BY opened_at DESC", params
        ).fetchall()
        trades = []
        for r in rows:
            t = dict(r)
            legs = conn.execute(
                "SELECT * FROM trade_legs WHERE trade_id=? ORDER BY id", (t["id"],)
            ).fetchall()
            t["legs"] = [dict(l) for l in legs]
            trades.append(t)
    return trades


def update_trade_status(trade_id: int, status: str, closed_at: Optional[str] = None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE trades SET status=?, closed_at=?, updated_at=? WHERE id=?",
            (status.lower(), closed_at or now if status == "closed" else closed_at, now, trade_id),
        )


def update_trade_leg_close(leg_id: int, close_premium: float) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE trade_legs SET close_premium=? WHERE id=?",
            (close_premium, leg_id),
        )


def close_leg(leg_id: int, close_premium: float) -> None:
    """Close an individual leg: set status='closed', record close_premium and timestamp."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE trade_legs SET status='closed', close_premium=?, closed_at=? WHERE id=?",
            (close_premium, now, leg_id),
        )


def all_legs_closed(trade_id: int) -> bool:
    """Return True if every leg of the trade has status='closed'."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM trade_legs WHERE trade_id=? AND status='open'",
            (trade_id,),
        ).fetchone()
    return row[0] == 0


def update_trade_notes(trade_id: int, notes: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE trades SET notes=?, updated_at=? WHERE id=?",
            (notes, now, trade_id),
        )


def update_leg_iv_current(leg_id: int, iv_current: float) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE trade_legs SET iv_current=?, iv_current_ts=? WHERE id=?",
            (iv_current, now, leg_id),
        )


def delete_trade(trade_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM trade_legs WHERE trade_id=?", (trade_id,))
        conn.execute("DELETE FROM pnl_history WHERE trade_id=?", (trade_id,))
        conn.execute("DELETE FROM trades WHERE id=?", (trade_id,))


# ── P&L History ──────────────────────────────────────────────────────────────

def insert_pnl_snapshot(
    trade_id: int,
    snapshot_ts: str,
    spot_price: Optional[float],
    delta_pnl: Optional[float],
    theta_pnl: Optional[float],
    vega_pnl: Optional[float],
    residual_pnl: Optional[float],
    total_pnl: Optional[float],
) -> None:
    """Insert a P&L snapshot, deduplicating on trade_id + minute."""
    # Truncate to minute for dedup
    ts_minute = snapshot_ts[:16]  # "YYYY-MM-DDTHH:MM"
    with get_conn() as conn:
        existing = conn.execute(
            """SELECT 1 FROM pnl_history
               WHERE trade_id=? AND snapshot_ts >= ? AND snapshot_ts < datetime(?, '+1 minute')
               LIMIT 1""",
            (trade_id, ts_minute, ts_minute),
        ).fetchone()
        if existing:
            return
        conn.execute(
            """INSERT INTO pnl_history
               (trade_id, snapshot_ts, spot_price, delta_pnl, theta_pnl,
                vega_pnl, residual_pnl, total_pnl)
               VALUES (?,?,?,?,?,?,?,?)""",
            (trade_id, snapshot_ts, spot_price, delta_pnl, theta_pnl,
             vega_pnl, residual_pnl, total_pnl),
        )


# ── Scanner results ──────────────────────────────────────────────────────────

def insert_scanner_result(
    symbol: str,
    scan_ts: str,
    rv_30d: Optional[float],
    rv_zscore: Optional[float],
    vol_momentum: Optional[float],
    iv_rank: Optional[float],
    data_source: Optional[str],
    iv_percentile: Optional[float] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO scanner_results
               (symbol, scan_ts, rv_30d, rv_zscore, vol_momentum, iv_rank, data_source, iv_percentile)
               VALUES (?,?,?,?,?,?,?,?)""",
            (symbol.upper(), scan_ts, rv_30d, rv_zscore, vol_momentum, iv_rank, data_source, iv_percentile),
        )


def get_tracked_equity_symbols() -> list[str]:
    """Return all distinct equity symbols from iv_snapshots and scanner_results."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT DISTINCT symbol FROM (
                 SELECT symbol FROM iv_snapshots
                 UNION
                 SELECT symbol FROM scanner_results
               ) WHERE symbol NOT IN ('BTC', 'ETH')
               ORDER BY symbol"""
        ).fetchall()
    return [r[0] for r in rows]


def get_latest_scanner_results() -> list[dict]:
    """Return the most recent scan result per symbol."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT s.* FROM scanner_results s
               INNER JOIN (
                 SELECT symbol, MAX(scan_ts) AS max_ts
                 FROM scanner_results
                 GROUP BY symbol
               ) m ON s.symbol = m.symbol AND s.scan_ts = m.max_ts
               ORDER BY s.rv_zscore DESC"""
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_scan_ts() -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(scan_ts) FROM scanner_results"
        ).fetchone()
    return row[0] if row and row[0] else None


# ── Watchlist ────────────────────────────────────────────────────────────────

def add_watchlist(symbol: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (symbol, added_ts) VALUES (?, ?)",
            (symbol.upper(), now),
        )


def remove_watchlist(symbol: str) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM watchlist WHERE symbol=?", (symbol.upper(),))


def get_watchlist() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT symbol FROM watchlist ORDER BY added_ts ASC"
        ).fetchall()
    return [r[0] for r in rows]


def get_pnl_history(trade_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pnl_history WHERE trade_id=? ORDER BY snapshot_ts ASC",
            (trade_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_pnl_snapshot(trade_id: int) -> Optional[dict]:
    """Return the most recent P&L history snapshot for a trade, or None."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pnl_history WHERE trade_id=? ORDER BY snapshot_ts DESC LIMIT 1",
            (trade_id,),
        ).fetchone()
    return dict(row) if row else None


# ── Index snapshots (VIX, VVIX) ─────────────────────────────────────────────

def insert_index_snapshot(symbol: str, value: float, ts: Optional[datetime] = None,
                          source: str = "yfinance") -> None:
    t = (ts or datetime.now(timezone.utc)).isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO index_snapshots (symbol, timestamp, value, source) VALUES (?,?,?,?)",
            (symbol.upper(), t, value, source),
        )


def count_index_snapshots(symbol: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM index_snapshots WHERE symbol=?",
            (symbol.upper(),),
        ).fetchone()
    return row[0] if row else 0


def get_index_dates(symbol: str) -> set[str]:
    """Return set of YYYY-MM-DD date prefixes already present for `symbol`."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT substr(timestamp,1,10) FROM index_snapshots WHERE symbol=?",
            (symbol.upper(),),
        ).fetchall()
    return {r[0] for r in rows}


def get_latest_index_snapshot(symbol: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM index_snapshots WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
    return dict(row) if row else None


def get_index_history(symbol: str, days: int = 30) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM index_snapshots
               WHERE symbol=? AND timestamp >= datetime('now', ?)
               ORDER BY timestamp ASC""",
            (symbol.upper(), f"-{days} days"),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Macro series ──────────────────────────────────────────────────────────────

def upsert_macro_metadata(
    series_id: str, display_name: str, category: str,
    units: str, frequency: str, source: str = "fred",
) -> None:
    """Insert or update the metadata row for a macro series."""
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO macro_metadata
                 (series_id, display_name, category, units, frequency, source)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(series_id) DO UPDATE SET
                 display_name=excluded.display_name,
                 category=excluded.category,
                 units=excluded.units,
                 frequency=excluded.frequency,
                 source=excluded.source""",
            (series_id, display_name, category, units, frequency, source),
        )


def insert_macro_observations(series_id: str, rows: list[tuple[str, float]]) -> int:
    """Bulk-insert (date, value) tuples for a series. Returns rows actually inserted."""
    if not rows:
        return 0
    with get_conn() as conn:
        cur = conn.executemany(
            """INSERT OR IGNORE INTO macro_series (series_id, ts, value)
               VALUES (?, ?, ?)""",
            [(series_id, ts, value) for ts, value in rows],
        )
        conn.execute(
            "UPDATE macro_metadata SET last_updated = ? WHERE series_id = ?",
            (datetime.now(timezone.utc).isoformat(), series_id),
        )
        return cur.rowcount or 0


def get_macro_series(series_id: str, days: Optional[int] = None) -> list[dict]:
    """Return [{ts, value}, ...] for a series, optionally restricted to the last `days`."""
    with get_conn() as conn:
        if days is None:
            rows = conn.execute(
                "SELECT ts, value FROM macro_series WHERE series_id=? ORDER BY ts ASC",
                (series_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT ts, value FROM macro_series
                   WHERE series_id=? AND ts >= date('now', ?)
                   ORDER BY ts ASC""",
                (series_id, f"-{int(days)} days"),
            ).fetchall()
    return [dict(r) for r in rows]


def get_latest_macro_value(series_id: str) -> Optional[dict]:
    """Return the most recent {ts, value} for a series, or None."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT ts, value FROM macro_series WHERE series_id=? ORDER BY ts DESC LIMIT 1",
            (series_id,),
        ).fetchone()
    return dict(row) if row else None


def count_macro_observations(series_id: str) -> int:
    with get_conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM macro_series WHERE series_id=?", (series_id,)
        ).fetchone()[0]


def list_macro_metadata() -> list[dict]:
    """Return all rows from macro_metadata for the API overview endpoint."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM macro_metadata ORDER BY category, display_name"
        ).fetchall()
    return [dict(r) for r in rows]


# ── Trade Journal ─────────────────────────────────────────────────────────────

def journal_list(
    status: Optional[str] = None,
    symbol: Optional[str] = None,
    days: Optional[int] = None,
) -> list[dict]:
    """Return journal entries sorted by idea_date DESC, created_at DESC.
    Filters are AND-combined: status exact match, symbol prefix (uppercase),
    and idea_date within the last `days` calendar days.
    """
    clauses, params = [], []
    if status:
        clauses.append("status=?")
        params.append(status)
    if symbol:
        clauses.append("symbol LIKE ?")
        params.append(symbol.upper() + "%")
    if days is not None:
        clauses.append("idea_date >= date('now', ?)")
        params.append(f"-{days} days")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM trade_journal{where} ORDER BY idea_date DESC, created_at DESC",
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def journal_get(entry_id: int) -> Optional[dict]:
    """Return a single journal entry by id, or None."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM trade_journal WHERE id=?", (entry_id,)
        ).fetchone()
    return dict(row) if row else None


def journal_create(
    idea_date: str,
    thesis: str,
    trade_decision: Optional[str] = None,
    status: str = "Open",
    symbol: Optional[str] = None,
) -> int:
    """Insert a new journal entry. Returns the new id."""
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO trade_journal
               (idea_date, thesis, trade_decision, status, symbol, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (idea_date, thesis, trade_decision, status,
             symbol.upper() if symbol else None, now, now),
        )
        return cur.lastrowid


def journal_update(entry_id: int, **fields) -> bool:
    """Update only the fields provided; always bumps updated_at.
    Returns True if a row was updated, False if entry_id not found.
    """
    allowed = {"idea_date", "thesis", "trade_decision", "status", "symbol"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return bool(journal_get(entry_id))
    now = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k}=?" for k in updates) + ", updated_at=?"
    params = list(updates.values()) + [now, entry_id]
    with get_conn() as conn:
        cur = conn.execute(
            f"UPDATE trade_journal SET {set_clause} WHERE id=?", params
        )
        return cur.rowcount > 0


def journal_delete(entry_id: int) -> bool:
    """Hard-delete a journal entry. Returns True if a row was deleted."""
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM trade_journal WHERE id=?", (entry_id,))
        return cur.rowcount > 0


# ── Fed Funds Futures (ZQ contracts from CME) ────────────────────────────────

def insert_ff_snapshot(rows: list[dict]) -> int:
    """Insert CME 30-day Fed Funds futures settlement rows.

    Each dict must have: snapshot_date, contract, delivery_month, settlement,
    implied_rate. Uses INSERT OR REPLACE so re-running is idempotent.
    Returns number of rows written.
    """
    if not rows:
        return 0
    with get_conn() as conn:
        cur = conn.executemany(
            """INSERT OR REPLACE INTO fed_funds_futures
               (snapshot_date, contract, delivery_month, settlement, implied_rate)
               VALUES (:snapshot_date, :contract, :delivery_month,
                       :settlement, :implied_rate)""",
            rows,
        )
        return cur.rowcount or 0


def get_ff_latest_curve() -> list[dict]:
    """Return the most recent full curve — all contracts from the latest snapshot_date.

    Returns [{contract, delivery_month, settlement, implied_rate, snapshot_date}, ...]
    sorted by delivery_month ascending.
    """
    with get_conn() as conn:
        latest_date = conn.execute(
            "SELECT MAX(snapshot_date) FROM fed_funds_futures"
        ).fetchone()[0]
        if not latest_date:
            return []
        rows = conn.execute(
            """SELECT snapshot_date, contract, delivery_month, settlement, implied_rate
               FROM fed_funds_futures
               WHERE snapshot_date = ?
               ORDER BY delivery_month ASC""",
            (latest_date,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_ff_history(contract: str, days: int = 365) -> list[dict]:
    """Return daily settlement history for a specific ZQ contract.

    Returns [{snapshot_date, settlement, implied_rate}, ...] sorted ASC.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT snapshot_date, settlement, implied_rate
               FROM fed_funds_futures
               WHERE contract = ?
                 AND snapshot_date >= date('now', ?)
               ORDER BY snapshot_date ASC""",
            (contract, f"-{int(days)} days"),
        ).fetchall()
    return [dict(r) for r in rows]
