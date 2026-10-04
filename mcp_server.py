from mcp.server.fastmcp import FastMCP
import sqlite3

mcp = FastMCP("cs2-market")
DB_PATH = "cs2_market.db"


def authorizer(action, arg1, arg2, dbname, source):
    # Blokuje ATTACH/DETACH — zapobiega zapisowi do innego pliku przez przemycone zapytanie
    if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH):
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def get_conn():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.set_authorizer(authorizer)
    return conn


@mcp.tool()
def list_watchlist() -> list[dict]:
    """Pełna lista monitorowanych przedmiotów (item_name, steam_item_id)."""
    conn = get_conn()
    rows = conn.execute("SELECT item_name, steam_item_id FROM watchlist").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@mcp.tool()
def get_latest_price(item_name: str) -> dict | None:
    """Najnowszy wiersz price_history dla danego przedmiotu."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM price_history WHERE item_name = ? ORDER BY timestamp DESC LIMIT 1",
        (item_name,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


@mcp.tool()
def get_price_history(item_name: str, days: int = 7) -> list[dict]:
    """Historia cen przedmiotu z ostatnich N dni."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM price_history WHERE item_name = ? "
        "AND timestamp >= datetime('now', ?) ORDER BY timestamp ASC",
        (item_name, f"-{days} days"),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@mcp.tool()
def get_order_book_gap(limit: int = 10) -> list[dict]:
    """Przedmioty z największą rozbieżnością steam_price vs highest_bid (najnowszy wiersz na item)."""
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT item_name, steam_price, highest_bid, buy_order_volume,
               (steam_price - highest_bid) AS gap
        FROM price_history p
        WHERE timestamp = (SELECT MAX(timestamp) FROM price_history WHERE item_name = p.item_name)
          AND highest_bid IS NOT NULL
        ORDER BY gap DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@mcp.tool()
def run_query(sql: str) -> list[dict]:
    """Dowolne zapytanie SELECT na całej bazie (watchlist, price_history, simulated_trades).
    Połączenie fizycznie tylko-do-odczytu — nic innego niż SELECT nie ma prawa się wykonać."""
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("Tylko zapytania SELECT są dozwolone.")
    conn = get_conn()
    try:
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    mcp.run()
