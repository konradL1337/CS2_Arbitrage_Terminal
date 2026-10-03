
"""
harvester.py — CS2 Market Analytics Terminal (STEAM-ONLY HARVESTER)

ARCHITEKTURA:
    - Pobiera TYLKO dane z Steam API (priceoverview)
    - Ignoruje kolumny: steam_item_id, highest_bid, buy_order_volume (pozostają NULL)
    - external_price = NULL (bez CSFloat/Skinport)
    - Twarde zabezpieczenia: time.sleep(8), exponential backoff 429

WYMAGANIA:
    - Biblioteki: requests, sqlite3, time, logging
    - Endpoint: https://steamcommunity.com/market/priceoverview/?appid=730&currency=6&market_hash_name=...
    - Waluta: PLN (currency=6)
"""

import logging
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import quote

import requests

# ─────────────────────────────────────────────────────────────────────────────
# Konfiguracja
# ─────────────────────────────────────────────────────────────────────────────
DB_PATH = Path("cs2_market.db")
STEAM_API_URL = "https://steamcommunity.com/market/priceoverview/?appid=730&currency=6&market_hash_name={name}"

# Exponential backoff dla Steam 429: 5 min → 10 min → 15 min → skip
BACKOFF_DELAYS_SEC = [5 * 60, 10 * 60, 15 * 60]

# Twarde opóźnienie na koniec każdej iteracji
ITEM_DELAY_SEC = 8

# User-Agent (Chrome 124 Windows)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("harvester")

# ─────────────────────────────────────────────────────────────────────────────
# Baza danych — minimalne funkcje
# ─────────────────────────────────────────────────────────────────────────────

def get_db_connection():
    """Zwraca połączenie z bazą danych SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_watchlist():
    """Pobiera listę itemów z watchlist."""
    with get_db_connection() as conn:
        rows = conn.execute("SELECT item_name FROM watchlist ORDER BY item_name;").fetchall()
    return [row["item_name"] for row in rows]


def insert_price_record(item_name, steam_price, volume):
    """
    Zapisuje rekord ceny do price_history.
    external_price zostaje jako NULL.
    """
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO price_history (item_name, steam_price, volume, external_price)
            VALUES (?, ?, ?, NULL);
            """,
            (item_name, steam_price, volume),
        )
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Parsowanie cen Steam (PLN)
# ─────────────────────────────────────────────────────────────────────────────
_PRICE_STRIP_RE = re.compile(r"[\xa0\u202f\s]|zł|PLN", re.UNICODE)


def parse_steam_price(raw_price):
    """
    Parsuje cenę Steam (format: "12,34 zł" lub "1 234,56 zł").
    Zwraca float lub None.
    """
    if not raw_price:
        return None
    
    # Usuń znaki nie-numeryczne oprócz przecinków i kropek
    cleaned = _PRICE_STRIP_RE.sub("", raw_price).strip()
    
    # Zamień przecinek na kropkę (format PLN: "12,34")
    if "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    elif "," in cleaned and "." in cleaned:
        # Format z separatorem tysięcy: "1.234,56" → "1234.56"
        cleaned = cleaned.replace(".", "").replace(",", ".")
    
    try:
        price = float(cleaned)
        return price if price > 0 else None
    except ValueError:
        logger.warning("Nie można sparsować ceny: %r (oczyszczone: %r)", raw_price, cleaned)
        return None


def parse_volume(raw_volume):
    """
    Parsuje volume Steam (może zawierać przecinki w tysiącach).
    Zwraca int lub None.
    """
    if not raw_volume:
        return None
    
    # Usuń wszystkie znaki nie-numeryczne oprócz cyfr
    cleaned = re.sub(r"[^\d]", "", str(raw_volume))
    
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Pobieranie danych z Steam API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_steam_item(item_name):
    """
    Pobiera dane Steam dla jednego itemu.
    Zwraca True jeśli sukces, False w przeciwnym razie.
    Implementuje exponential backoff dla 429.
    """
    url = STEAM_API_URL.format(name=quote(item_name))
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "pl-PL,pl;q=0.9",
    }
    
    for attempt, backoff_delay in enumerate(BACKOFF_DELAYS_SEC + [None]):
        try:
            response = requests.get(url, headers=headers, timeout=15)
        except requests.RequestException as exc:
            logger.error("[STEAM] Błąd połączenia dla '%s': %s", item_name, exc)
            return False
        
        # Obsługa 429 (Too Many Requests)
        if response.status_code == 429:
            if backoff_delay is None:
                logger.warning(
                    "[STEAM] 429 po %d próbach dla '%s' — pomijam item.",
                    len(BACKOFF_DELAYS_SEC), item_name
                )
                return False
            
            logger.warning(
                "[STEAM] 429 (próba %d/%d) dla '%s' — czekam %d min...",
                attempt + 1, len(BACKOFF_DELAYS_SEC), item_name, backoff_delay // 60
            )
            time.sleep(backoff_delay)
            continue
        
        # Inne błędy HTTP
        if response.status_code != 200:
            logger.warning("[STEAM] HTTP %d dla '%s' — pomijam.", response.status_code, item_name)
            return False
        
        break  # Sukces
    else:
        return False
    
    # Parsowanie JSON
    try:
        data = response.json()
    except ValueError:
        logger.error("[STEAM] Nieprawidłowy JSON dla '%s'", item_name)
        return False
    
    if not data.get("success"):
        logger.warning("[STEAM] success=false dla '%s'", item_name)
        return False
    
    # Wyciąganie danych
    raw_price = data.get("lowest_price") or data.get("median_price", "")
    raw_volume = data.get("volume", "")
    
    steam_price = parse_steam_price(raw_price)
    volume = parse_volume(raw_volume)
    
    if steam_price is None:
        logger.warning("[STEAM] Nie udało się sparsować ceny dla '%s' — pomijam.", item_name)
        return False
    
    # Zapis do bazy
    insert_price_record(item_name, steam_price, volume)
    
    logger.info(
        "[OK] %-50s | Steam: %7.2f PLN | Volume: %s",
        item_name[:50], steam_price, volume if volume is not None else "—"
    )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Główna pętla harvestera
# ─────────────────────────────────────────────────────────────────────────────

def run_harvest_cycle():
    """Wykonuje jeden cykl harvestowania wszystkich itemów z watchlist."""
    watchlist = get_watchlist()
    
    if not watchlist:
        logger.info("Watchlist jest pusta — brak itemów do harvestowania.")
        return
    
    logger.info("═══ START CYKLU HARVESTOWANIA — %d itemów ═══", len(watchlist))
    
    success_count = 0
    failure_count = 0
    
    for item_name in watchlist:
        success = fetch_steam_item(item_name)
        
        if success:
            success_count += 1
        else:
            failure_count += 1
        
        # Twarde opóźnienie na koniec każdej iteracji
        time.sleep(ITEM_DELAY_SEC)
    
    logger.info(
        "═══ KONIEC CYKLU — Sukces: %d | Porażka: %d ═══",
        success_count, failure_count
    )


def main():
    """Główna funkcja uruchamiająca harvester w trybie ciągłym."""
    logger.info("╔═══════════════════════════════════════════════════════════════╗")
    logger.info("║  CS2 MARKET HARVESTER (STEAM-ONLY)                            ║")
    logger.info("║  Delay: %d s/item | Backoff: 5/10/15 min                     ║", ITEM_DELAY_SEC)
    logger.info("╚═══════════════════════════════════════════════════════════════╝")
    
    while True:
        try:
            run_harvest_cycle()
        except Exception as exc:
            logger.exception("Nieobsłużony błąd w cyklu harvestowania: %s", exc)
        
        # Cykl co 60 minut (można dostosować)
        logger.info("Czekam 60 minut do następnego cyklu...\n")
        time.sleep(60 * 60)


if __name__ == "__main__":
    main()
