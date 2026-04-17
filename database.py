"""
database.py — CS2 Market Analytics Terminal
Schema + idempotent migration layer.

Migrations applied on every startup (safe, never drops data):
  • price_history.external_price  — Skinport price per record
  • simulated_trades.quantity     — number of units per position
"""

import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
DB_PATH = Path("cs2_market.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA cache_size=-32000;")
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r["name"] == column for r in rows)


def _safe_add_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    """Add column if it does not exist. Swallows race-condition errors."""
    if not _column_exists(conn, table, column):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition};")
            logger.info("Migration: added %s.%s", table, column)
        except sqlite3.OperationalError as e:
            logger.debug("ADD COLUMN skipped (already exists): %s", e)


def initialize_database() -> None:
    """
    Create all tables and run all pending migrations.
    Called by both harvester.py and app.py on startup — fully idempotent.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS watchlist (
        item_name TEXT NOT NULL,
        CONSTRAINT uq_watchlist_item UNIQUE (item_name)
    );

    CREATE TABLE IF NOT EXISTS price_history (
        id             INTEGER  PRIMARY KEY AUTOINCREMENT,
        timestamp      DATETIME NOT NULL DEFAULT (datetime('now')),
        item_name      TEXT     NOT NULL,
        steam_price    REAL     NOT NULL,
        volume         INTEGER,
        external_price REAL
    );

    CREATE INDEX IF NOT EXISTS idx_ph_item_ts
        ON price_history (item_name, timestamp DESC);

    CREATE TABLE IF NOT EXISTS simulated_trades (
        id        INTEGER  PRIMARY KEY AUTOINCREMENT,
        item_name TEXT     NOT NULL,
        buy_price REAL     NOT NULL,
        quantity  INTEGER  NOT NULL DEFAULT 1,
        timestamp DATETIME NOT NULL DEFAULT (datetime('now')),
        status    TEXT     NOT NULL DEFAULT 'OPEN'
                           CHECK(status IN ('OPEN', 'CLOSED'))
    );

    CREATE INDEX IF NOT EXISTS idx_st_item
        ON simulated_trades (item_name, status);
    """
    with get_connection() as conn:
        conn.executescript(ddl)
        # ── Migrations for databases created before these columns existed ─────
        _safe_add_column(conn, "price_history",    "external_price", "REAL")
        _safe_add_column(conn, "simulated_trades", "quantity",       "INTEGER NOT NULL DEFAULT 1")

    logger.info("DB ready: %s  (SQLite %s)", DB_PATH.resolve(), sqlite3.sqlite_version)


# ── Watchlist ─────────────────────────────────────────────────────────────────

def add_to_watchlist(item_name: str) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("INSERT OR IGNORE INTO watchlist (item_name) VALUES (?);", (item_name,))
            return conn.execute("SELECT changes();").fetchone()[0] == 1
    except sqlite3.Error as e:
        logger.error("add_to_watchlist(%r): %s", item_name, e)
        return False


def remove_from_watchlist(item_name: str) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM watchlist WHERE item_name = ?;", (item_name,))


def get_watchlist() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute("SELECT item_name FROM watchlist ORDER BY item_name;").fetchall()
    return [r["item_name"] for r in rows]


# ── Price history ─────────────────────────────────────────────────────────────

def insert_price_record(
    item_name:      str,
    steam_price:    float,
    volume:         int | None,
    external_price: float | None = None,
) -> None:
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO price_history (item_name, steam_price, volume, external_price)"
            " VALUES (?, ?, ?, ?);",
            (item_name, steam_price, volume, external_price),
        )


def get_latest_price(item_name: str) -> sqlite3.Row | None:
    with get_connection() as conn:
        return conn.execute(
            "SELECT steam_price, volume, external_price, timestamp"
            " FROM price_history WHERE item_name = ?"
            " ORDER BY timestamp DESC LIMIT 1;",
            (item_name,),
        ).fetchone()


def get_price_as_of(item_name: str, hours_ago: float) -> sqlite3.Row | None:
    with get_connection() as conn:
        return conn.execute(
            "SELECT steam_price, external_price, timestamp"
            " FROM price_history WHERE item_name = ?"
            "   AND timestamp <= datetime('now', ? || ' hours')"
            " ORDER BY timestamp DESC LIMIT 1;",
            (item_name, f"-{hours_ago}"),
        ).fetchone()


def get_price_history(item_name: str, limit: int = 2000) -> list[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT timestamp, steam_price, volume, external_price"
            " FROM price_history WHERE item_name = ?"
            " ORDER BY timestamp ASC LIMIT ?;",
            (item_name, limit),
        ).fetchall()


# ── Simulated trades (Duchowy Portfel) ────────────────────────────────────────

def open_trade(item_name: str, buy_price: float, quantity: int = 1) -> int:
    """
    Open a paper-trade position.
    buy_price: price per unit (can differ from current Steam price — e.g. buy order)
    quantity:  number of units purchased
    """
    with get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO simulated_trades (item_name, buy_price, quantity, status)"
            " VALUES (?, ?, ?, 'OPEN');",
            (item_name, buy_price, max(1, int(quantity))),
        )
        return cur.lastrowid


def close_trade(trade_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE simulated_trades SET status = 'CLOSED' WHERE id = ?;", (trade_id,)
        )


def get_open_trades() -> list[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT id, item_name, buy_price, quantity, timestamp"
            " FROM simulated_trades"
            " WHERE status = 'OPEN' ORDER BY timestamp DESC;",
        ).fetchall()


def get_closed_trades() -> list[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT id, item_name, buy_price, quantity, timestamp"
            " FROM simulated_trades"
            " WHERE status = 'CLOSED' ORDER BY timestamp DESC;",
        ).fetchall()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    initialize_database()
    print(f"[OK] {DB_PATH.resolve()}  SQLite {sqlite3.sqlite_version}")
