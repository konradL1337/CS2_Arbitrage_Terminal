import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import math

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

DB_PATH = "/home/ubuntu/arbitraz/cs2_market.db"

@app.get("/api/health")
def health(): return {"status": "ok"}

def calculate_delta(current_price: float, historical_prices: list, hours_back: int) -> float:
    """Calculate price delta percentage for given time period"""
    if not historical_prices or current_price is None:
        return 0.0
    
    # Find price closest to hours_back
    target_idx = min(hours_back, len(historical_prices) - 1)
    if target_idx >= len(historical_prices):
        target_idx = len(historical_prices) - 1
    
    old_price = historical_prices[target_idx]
    if old_price is None or old_price == 0:
        return 0.0
    
    return round(((current_price - old_price) / old_price) * 100, 2)

@app.get("/api/market-data")
def get_data():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL;")
        
        # Get latest price for each item
        latest_query = """
            SELECT 
                p.item_name, 
                p.steam_price, 
                p.external_price AS csfloat_price, 
                p.volume AS steam_volume, 
                p.timestamp
            FROM price_history p
            INNER JOIN (
                SELECT item_name, MAX(timestamp) as max_ts FROM price_history GROUP BY item_name
            ) tm ON p.item_name = tm.item_name AND p.timestamp = tm.max_ts
            ORDER BY p.volume DESC
        """
        df_latest = pd.read_sql_query(latest_query, conn)
        df_latest = df_latest.dropna(subset=['steam_price', 'csfloat_price'])
        
        # Enrich each item with sparkline and deltas
        enriched_items = []
        for _, row in df_latest.iterrows():
            item_name = row['item_name']
            current_price = row['steam_price']
            
            # Get historical prices (last 30 records for sparkline)
            history_query = """
                SELECT steam_price, timestamp 
                FROM price_history 
                WHERE item_name = ? AND steam_price IS NOT NULL
                ORDER BY timestamp DESC 
                LIMIT 30
            """
            cursor = conn.cursor()
            cursor.execute(history_query, (item_name,))
            history = cursor.fetchall()
            
            # Reverse to get chronological order (oldest to newest)
            history = list(reversed(history))
            sparkline = [float(h[0]) for h in history if h[0] is not None]
            
            # Calculate deltas (assuming ~1 record per hour)
            delta24h = calculate_delta(current_price, sparkline, 24) if len(sparkline) >= 24 else 0.0
            delta7d = calculate_delta(current_price, sparkline, min(168, len(sparkline) - 1)) if len(sparkline) > 1 else 0.0
            
            enriched_items.append({
                'item_name': item_name,
                'steam_price': float(row['steam_price']),
                'csfloat_price': float(row['csfloat_price']),
                'steam_volume': int(row['steam_volume']) if row['steam_volume'] else 0,
                'timestamp': row['timestamp'],
                'sparkline': sparkline[-30:],  # Keep last 30 points
                'delta24h': delta24h,
                'delta7d': delta7d
            })
        
        conn.close()
        return enriched_items
        
    except Exception as e:
        print(f"KRYTYCZNY BLAD BAZY: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
