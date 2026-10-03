
"""
harvester.py — CS2 Market Analytics Terminal (STEAM ORDER BOOK HARVESTER)

ARCHITEKTURA:
    - Pobiera dane z Steam API (priceoverview)
    - Scrapuje steam_item_id z HTML (jednokrotnie, z 10s delay)
    - Odpytuje Order Book Histogram (highest_bid, buy_order_volume)
    - external_price = NULL (bez CSFloat/Skinport)
    - Twarde zabezpieczenia: time.sleep(8) główny + time.sleep(10) HTML scraping
    - Fail-Safe: błędy histogramu nie przerywają głównego cyklu

WYMAGANIA:
    - Biblioteki: requests, sqlite3, time, logging, re
    - Endpoint Steam Price: https://steamcommunity.com/market/priceoverview/?appid=730&currency=6&market_hash_name=...
    - Endpoint Order Book: https://steamcommunity.com/market/itemordershistogram?country=PL&language=polish&currency=6&item_nameid={id}&two_factor=0
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
STEAM_LISTING_URL = "https://steamcommunity.com/market/listings/730/{name}"
STEAM_HISTOGRAM_URL = "https://steamcommunity.com/market/itemordershistogram?country=PL&language=polish&currency=6&item_nameid={id}&two_factor=0"

# RegEx do wyciągnięcia steam_item_id z HTML
STEAM_ITEM_ID_RE = re.compile(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)', re.IGNORECASE)

# Exponential backoff dla Steam 429: 5 min → 10 min → 15 min → skip
BACKOFF_DELAYS_SEC = [5 * 60, 10 * 60, 15 * 60]

# Twarde opóźnienie na koniec każdej iteracji
ITEM_DELAY_SEC = 8

# Opóźnienie po scrapowaniu HTML (rygorystyczny limit Valve)
HTML_SCRAPE_DELAY_SEC = 10

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
    """
    Pobiera listę itemów z watchlist wraz z steam_item_id.
    Zwraca listę tuple: (item_name, steam_item_id).
    """
    with get_db_connection() as conn:
        rows = conn.execute("SELECT item_name, steam_item_id FROM watchlist ORDER BY item_name;").fetchall()
    return [(row["item_name"], row["steam_item_id"]) for row in rows]


def update_steam_item_id(item_name, steam_item_id):
    """
    Aktualizuje steam_item_id w tabeli watchlist dla danego itemu.
    Wykonywane tylko raz, gdy ID zostanie zescrapowane z HTML.
    """
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE watchlist SET steam_item_id = ? WHERE item_name = ?;",
            (steam_item_id, item_name),
        )
        conn.commit()
    logger.info("[DB] Zapisano steam_item_id=%s dla '%s'", steam_item_id, item_name)


def insert_price_record(item_name, steam_price, volume, highest_bid=None, buy_order_volume=None):
    """
    Zapisuje rekord ceny do price_history.
    external_price zostaje jako NULL.
    highest_bid i buy_order_volume mogą być NULL jeśli histogram nie jest dostępny.
    """
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO price_history (item_name, steam_price, volume, external_price, highest_bid, buy_order_volume)
            VALUES (?, ?, ?, NULL, ?, ?);
            """,
            (item_name, steam_price, volume, highest_bid, buy_order_volume),
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
# Scrapowanie steam_item_id z HTML
# ─────────────────────────────────────────────────────────────────────────────

def scrape_steam_item_id(item_name):
    """
    Scrapuje steam_item_id z kodu HTML strony Market Listings.
    Zwraca numeryczne ID lub None w przypadku błędu.
    UWAGA: Ta funkcja jest wywoływana TYLKO RAZ na przedmiot i zawiera time.sleep(10).
    """
    url = STEAM_LISTING_URL.format(name=quote(item_name))
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "pl-PL,pl;q=0.9",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException as exc:
        logger.error("[HTML] Błąd połączenia podczas scrapowania '%s': %s", item_name, exc)
        return None
    
    if response.status_code != 200:
        logger.warning("[HTML] HTTP %d podczas scrapowania '%s'", response.status_code, item_name)
        return None
    
    # Szukaj Market_LoadOrderSpread(ITEM_ID)
    match = STEAM_ITEM_ID_RE.search(response.text)
    if not match:
        logger.warning("[HTML] Nie znaleziono steam_item_id w HTML dla '%s'", item_name)
        return None
    
    steam_item_id = match.group(1)
    logger.info("[HTML] Zescrapowano steam_item_id=%s dla '%s'", steam_item_id, item_name)
    
    # Zapisz do bazy
    update_steam_item_id(item_name, steam_item_id)
    
    # BEZWZGLĘDNY DELAY — Valve rygorystycznie limituje scraping HTML
    logger.info("[HTML] Czekam %d sekund (rygorystyczny limit Valve)...", HTML_SCRAPE_DELAY_SEC)
    time.sleep(HTML_SCRAPE_DELAY_SEC)
    
    return steam_item_id


# ─────────────────────────────────────────────────────────────────────────────
# Pobieranie Order Book Histogram
# ─────────────────────────────────────────────────────────────────────────────

def fetch_order_book_histogram(steam_item_id, item_name):
    """
    Pobiera dane z Order Book Histogram dla danego steam_item_id.
    Zwraca tuple: (highest_bid, buy_order_volume) lub (None, None) w przypadku błędu.
    Fail-Safe: błędy NIE przerywają głównego cyklu.
    """
    url = STEAM_HISTOGRAM_URL.format(id=steam_item_id)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "pl-PL,pl;q=0.9",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
    except requests.RequestException as exc:
        logger.warning("[HISTOGRAM] Błąd połączenia dla '%s': %s — pomijam histogram", item_name, exc)
        return None, None
    
    if response.status_code == 429:
        logger.warning("[HISTOGRAM] 429 dla '%s' — pomijam histogram", item_name)
        return None, None
    
    if response.status_code != 200:
        logger.warning("[HISTOGRAM] HTTP %d dla '%s' — pomijam histogram", response.status_code, item_name)
        return None, None
    
    try:
        data = response.json()
    except ValueError:
        logger.warning("[HISTOGRAM] Nieprawidłowy JSON dla '%s' — pomijam histogram", item_name)
        return None, None
    
    if not data.get("success"):
        logger.warning("[HISTOGRAM] success=false dla '%s' — pomijam histogram", item_name)
        return None, None
    
    # Wyciąganie highest_bid (w groszach, trzeba podzielić przez 100)
    highest_bid_raw = data.get("highest_buy_order")
    highest_bid = None
    if highest_bid_raw:
        try:
            highest_bid = float(highest_bid_raw) / 100.0
        except (ValueError, TypeError):
            logger.warning("[HISTOGRAM] Nie można sparsować highest_buy_order dla '%s': %r", item_name, highest_bid_raw)
    
    # Alternatywnie z buy_order_graph (pierwsza pozycja)
    if highest_bid is None:
        buy_order_graph = data.get("buy_order_graph", [])
        if buy_order_graph and len(buy_order_graph) > 0:
            try:
                highest_bid = float(buy_order_graph[0][0]) / 100.0
            except (ValueError, TypeError, IndexError):
                pass
    
    # Wyciąganie buy_order_volume z buy_order_summary (HTML z liczbą)
    buy_order_volume = None
    buy_order_summary = data.get("buy_order_summary", "")
    if buy_order_summary:
        # Format: "<span class=\"market_commodity_orders_header_promote\">12,345</span>"
        # Wyciągamy liczbę za pomocą RegEx
        volume_match = re.search(r'>([0-9,\.]+)<', buy_order_summary)
        if volume_match:
            volume_str = volume_match.group(1).replace(",", "").replace(".", "")
            try:
                buy_order_volume = int(volume_str)
            except ValueError:
                logger.warning("[HISTOGRAM] Nie można sparsować buy_order_volume dla '%s': %r", item_name, volume_str)
    
    return highest_bid, buy_order_volume


# ─────────────────────────────────────────────────────────────────────────────
# Pobieranie danych z Steam API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_steam_item(item_name, steam_item_id):
    """
    Pobiera dane Steam dla jednego itemu.
    Jeśli steam_item_id jest dostępne, dodatkowo pobiera Order Book Histogram.
    Zwraca True jeśli sukces, False w przeciwnym razie.
    Implementuje exponential backoff dla 429.
    """
    # ───────────────────────────────────────────────────────────────────────────
    # KROK 1: Sprawdź czy mamy steam_item_id, jeśli nie — zescrapuj
    # ───────────────────────────────────────────────────────────────────────────
    if not steam_item_id or steam_item_id.strip() == "":
        logger.info("[SCRAPE] Brak steam_item_id dla '%s' — rozpoczynam scrapowanie...", item_name)
        steam_item_id = scrape_steam_item_id(item_name)
        # Jeśli scraping się nie powiódł, kontynuujemy bez Order Book
        if not steam_item_id:
            logger.warning("[SCRAPE] Nie udało się uzyskać steam_item_id dla '%s' — kontynuuję bez histogramu", item_name)
    
    # ───────────────────────────────────────────────────────────────────────────
    # KROK 2: Pobierz standardowe dane z priceoverview
    # ───────────────────────────────────────────────────────────────────────────
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
    
    if steam_price is None or steam_price <= 0.0:
        logger.warning("[STEAM] Nieprawidłowa cena dla '%s' (%.2f) — pomijam.", item_name, steam_price if steam_price else 0.0)
        return False
    
    # ───────────────────────────────────────────────────────────────────────────
    # KROK 3: Pobierz Order Book Histogram (jeśli mamy steam_item_id)
    # ───────────────────────────────────────────────────────────────────────────
    highest_bid = None
    buy_order_volume = None
    
    if steam_item_id:
        highest_bid, buy_order_volume = fetch_order_book_histogram(steam_item_id, item_name)
    
    # ───────────────────────────────────────────────────────────────────────────
    # KROK 4: Zapis do bazy (Fail-Safe: histogram może być NULL)
    # ───────────────────────────────────────────────────────────────────────────
    insert_price_record(item_name, steam_price, volume, highest_bid, buy_order_volume)
    
    # Log wyników
    log_parts = [
        f"[OK] {item_name[:50]:<50}",
        f"| Steam: {steam_price:7.2f} PLN",
        f"| Volume: {volume if volume is not None else '—':>6}",
    ]
    
    if highest_bid is not None:
        log_parts.append(f"| Highest Bid: {highest_bid:7.2f} PLN")
    
    if buy_order_volume is not None:
        log_parts.append(f"| Buy Orders: {buy_order_volume:>6}")
    
    logger.info(" ".join(log_parts))
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
    
    for item_name, steam_item_id in watchlist:
        success = fetch_steam_item(item_name, steam_item_id)
        
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
    logger.info("║  CS2 MARKET HARVESTER (ORDER BOOK ANALYTICS)                  ║")
    logger.info("║  Features: Price + Volume + Highest Bid + Buy Order Volume    ║")
    logger.info("║  Delay: %d s/item | HTML Scrape: %d s | Backoff: 5/10/15 min ║", ITEM_DELAY_SEC, HTML_SCRAPE_DELAY_SEC)
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
