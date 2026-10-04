from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import math
import re
import logging
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

app = FastAPI(title="CS2 Market Terminal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "/home/ubuntu/arbitraz/cs2_market.db"

# === STEAM QUANT ENGINE CONSTANTS ===
STEAM_FEE_MULTIPLIER = 1.15  # Net = Gross / 1.15 (13.04% efektywnego podatku)
PENNY_STOCK_THRESHOLD = 1.00  # PLN - minimalna prowizja Valve zabija zysk ponizej tej ceny
SPARKLINE_LIMIT = 30


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


# === QUANT ENGINE (funkcje czyste) ===

def calc_target_edge(volume: int) -> float:
    """Dynamiczny Target Edge zalezny od plynnosci (volume24h)."""
    if volume >= 10000:
        return 0.09   # hiperplynne skrzynki
    if volume >= 1000:
        return 0.13
    if volume >= 100:
        return 0.17
    return 0.22       # bufor ryzyka dla rzadkich przedmiotow


def calc_max_buy(steam_price: float, volume: int) -> tuple:
    """
    Max Buy = (steam_price / 1.15) / (1 + targetEdge)
    Zwraca (maxBuyPrice, realEdgePercent).
    """
    net_exit = steam_price / STEAM_FEE_MULTIPLIER
    target_edge = calc_target_edge(volume)
    max_buy_price = round(net_exit / (1 + target_edge), 2)
    if max_buy_price <= 0:
        return 0.0, 0.0
    real_edge_percent = round(((net_exit - max_buy_price) / max_buy_price) * 100, 1)
    return max_buy_price, real_edge_percent


def calc_liquidity_score(volume: int) -> int:
    """Skala logarytmiczna: min(100, max(10, round(log10(volume + 1) * 22)))."""
    return min(100, max(10, round(math.log10(volume + 1) * 22)))


def categorize_item(name: str) -> str:
    lower = name.lower()
    if "case" in lower:
        return "case"
    if "sticker" in lower or "capsule" in lower:
        return "sticker"
    return "skin"


def slugify(name: str) -> str:
    """'Dreams & Nightmares Case' -> 'dreams_nightmares_case'"""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower())
    return slug.strip("_")


def to_iso_z(ts: str) -> str:
    """'2026-10-03 14:04:56' -> '2026-10-03T14:04:56Z'"""
    if not ts:
        return ""
    return ts.replace(" ", "T") + "Z"

@app.get("/api/health")
def health():
    return {"status": "online", "mode": "steam_only"}

@app.get("/api/market-data")
def get_data():
    try:
        conn = get_db()
        cur = conn.cursor()

        # 1. Najnowszy stan dla kazdego unikalnego przedmiotu
        cur.execute(
            """
            SELECT p.item_name, p.steam_price, p.volume AS steam_volume,
                   p.highest_bid, p.timestamp
            FROM price_history p
            INNER JOIN (
                SELECT item_name, MAX(timestamp) AS max_ts
                FROM price_history
                GROUP BY item_name
            ) latest ON p.item_name = latest.item_name AND p.timestamp = latest.max_ts
            WHERE p.steam_price IS NOT NULL AND p.steam_price > 0
            ORDER BY p.volume DESC;
            """
        )
        latest_rows = cur.fetchall()

        results = []
        for item_name, steam_price, steam_volume, highest_bid, ts in latest_rows:
            steam_price = float(steam_price)
            steam_volume = int(steam_volume) if steam_volume is not None else 0

            # 2. Historia pod sparkline i dolki (ostatnie 30 odczytow)
            cur.execute(
                """
                SELECT steam_price, timestamp
                FROM price_history
                WHERE item_name = ? AND steam_price IS NOT NULL AND steam_price > 0
                ORDER BY timestamp DESC
                LIMIT ?;
                """,
                (item_name, SPARKLINE_LIMIT),
            )
            history = cur.fetchall()
            # Chronologicznie: od najstarszego do najnowszego
            sparkline = [round(float(r[0]), 2) for r in reversed(history)]

            # 3. Metryki Syzyfa
            net_exit = round(steam_price / STEAM_FEE_MULTIPLIER, 2)
            max_buy_price, real_edge_percent = calc_max_buy(steam_price, steam_volume)

            if len(sparkline) >= 2 and sparkline[0] > 0:
                delta24h = round(((sparkline[-1] - sparkline[0]) / sparkline[0]) * 100, 2)
            else:
                delta24h = 0.0

            min24h = round(min(sparkline), 2) if sparkline else steam_price
            max24h = round(max(sparkline), 2) if sparkline else steam_price

            spread_percent = round(
                ((steam_price - max_buy_price) / steam_price) * 100, 2
            ) if steam_price > 0 else 0.0

            liquidity_score = calc_liquidity_score(steam_volume)
            is_penny_stock = steam_price < PENNY_STOCK_THRESHOLD

            results.append({
                "id": slugify(item_name),
                "name": item_name,
                "category": categorize_item(item_name),
                "lowestAsk": round(steam_price, 2),
                "highestBid": round(float(highest_bid), 2) if highest_bid else max_buy_price,
                "spreadPercent": spread_percent,
                "volume24h": steam_volume,
                "liquidityScore": liquidity_score,
                "maxBuyPrice": max_buy_price,
                "expectedExit": round(steam_price, 2),
                "estimatedNetExit": net_exit,
                "estimatedEdgePercent": real_edge_percent,
                "delta24h": delta24h,
                "min24h": min24h,
                "max24h": max24h,
                "isPennyStock": is_penny_stock,
                "sparkline": sparkline,
                "updatedAt": to_iso_z(ts),
            })

        conn.close()
        return results
    except Exception as e:
        logger.error(f"BLAD API: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
