"""
harvester.py — CS2 Market Analytics Terminal

DIAGNOZA ARCHITEKTA:
    Skinport blokuje IP Oracle (403). Rezygnujemy z niego.
    Używamy CSFloat, ponieważ ich API akceptuje zapytania z datacenter
    przy użyciu klucza API.

CSFLOAT API:
    Endpoint: https://csfloat.com/api/v1/listings
    Wymagany nagłówek: Authorization z kluczem API
    Limit: 1.5s między zapytaniami (krytyczne, aby uniknąć 429)

EXPONENTIAL BACKOFF (Steam 429):
    Próba 1 → czekaj 5 min → próba 2
    Próba 2 → czekaj 10 min → próba 3
    Próba 3 → czekaj 15 min → skip item (pętla NIE przerywa się)
"""

import csv
import json
import logging
import logging.handlers
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

# ── python-dotenv: wczytanie .env ─────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # .env nie jest wymagany — zmienne mogą być w środowisku systemu

import requests   # standardowa biblioteka requests

from database import get_watchlist, insert_price_record, initialize_database

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
_file_handler = logging.handlers.RotatingFileHandler(
    "harvester.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(), _file_handler],
)
logger = logging.getLogger("harvester")

# ─────────────────────────────────────────────────────────────────────────────
# Stałe
# ─────────────────────────────────────────────────────────────────────────────
STEAM_API    = ("https://steamcommunity.com/market/priceoverview/"
                "?appid=730&currency=6&market_hash_name={name}")
CSFLOAT_API  = "https://csfloat.com/api/v1/listings?market_hash_name={name}&limit=1"
# USD_TO_PLN zastąpiony przez dynamiczny kurs z NBP API

HARVEST_INTERVAL_SEC = 60 * 60
PER_ITEM_DELAY_SEC   = 6
CSFLOAT_DELAY_SEC    = 1.5  # Obowiązkowe opóźnienie między zapytaniami do CSFloat

# Exponential backoff dla Steam 429
BACKOFF_STEPS_SEC = [5 * 60, 10 * 60, 15 * 60]

CSV_PATH    = Path("training_data.csv")
CSV_COLUMNS = ["timestamp", "item_name", "steam_price", "csfloat_price", "volume"]

# ─────────────────────────────────────────────────────────────────────────────
# Sesja Steam (zwykły requests)
# ─────────────────────────────────────────────────────────────────────────────
_steam_session: requests.Session | None = None


def _get_steam_session() -> requests.Session:
    global _steam_session
    if _steam_session is None:
        _steam_session = requests.Session()
        _steam_session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        })
    return _steam_session


# ─────────────────────────────────────────────────────────────────────────────
# Sanitizacja cen Steam
# ─────────────────────────────────────────────────────────────────────────────
_STRIP_RE = re.compile(r"\xa0|\u202f|\s|zł|PLN|€|\$|--", re.UNICODE)


def sanitize_steam_price(raw: str) -> float | None:
    """Zwraca float > 0 lub None. NIGDY nie zwraca 0.0."""
    if not raw:
        return None
    s = _STRIP_RE.sub("", raw).strip()
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    elif "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        logger.warning("Steam price parse error: cleaned=%r  raw=%r", s, raw)
        return None


def sanitize_volume(raw: str | None) -> int | None:
    if not raw:
        return None
    # Usuwamy białe znaki i wymuszamy typ string
    s = _STRIP_RE.sub("", str(raw)).strip()
    # KRYTYCZNA POPRAWKA: Usuwamy przecinki z tysięcy (np. "12,345" -> "12345")
    s = s.replace(",", "")
    try:
        return int(s)
    except ValueError:
        return None

# ─────────────────────────────────────────────────────────────────────────────
# Normalizacja nazw
# ─────────────────────────────────────────────────────────────────────────────
_ITEM_PREFIXES = sorted([
    "sticker | ", "patch | ", "graffiti | ",
    "sealed graffiti | ", "music kit | ",
    "collectible pin | ", "pin | ",
], key=len, reverse=True)

_NORM_RE = re.compile(r"[^a-z0-9]")


def normalize_name(name: str) -> str:
    n = unicodedata.normalize("NFC", name).lower().strip()
    for prefix in _ITEM_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):]
            break
    return _NORM_RE.sub("", n)


def _clean_ws(s: str) -> str:
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace("\u2009", " ").strip()
    return re.sub(r" {2,}", " ", s)




# pobieranie kursu 

def get_usd_pln_rate() -> float:
    """Pobiera aktualny kurs średni USD z NBP. Fallback: 4.0"""
    try:
        url = "http://api.nbp.pl/api/exchangerates/rates/a/usd/?format=json"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            rate = resp.json()['rates'][0]['mid']
            logger.info(f"[SYSTEM] Aktualny kurs USD z NBP: {rate}")
            return float(rate)
    except Exception as e:
        logger.error(f"[SYSTEM] Błąd pobierania kursu NBP: {e}. Używam 4.0")
    return 4.0
# ─────────────────────────────────────────────────────────────────────────────
# CSFloat API - pobieranie ceny jednego przedmiotu
# ─────────────────────────────────────────────────────────────────────────────

def fetch_csfloat_price(market_hash_name: str, usd_rate: float) -> float | None:
    """
    Pobiera cenę przedmiotu z CSFloat API.
    """
    api_key = os.getenv("CSFLOAT_API_KEY")
    if not api_key:
        logger.error("Brak CSFLOAT_API_KEY w .env!")
        return None
    
    # Kodowanie nazwy do URL
    encoded_name = quote(market_hash_name)
    url = f"https://csfloat.com/api/v1/listings?market_hash_name={encoded_name}&limit=1&type=buy_now"
    
    headers = {
        "Authorization": api_key,
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 429:
            logger.warning(f"[CSFLOAT] 429 Rate limit dla {market_hash_name}")
            return None
        
        if response.status_code != 200:
            return None

        # Wyciąganie ceny z klucza 'data'
        res_json = response.json()
        data = res_json.get("data", [])
        
        if data and len(data) > 0:
            price_cents = data[0].get("price", 0)
            if price_cents > 0:
                # USD Cents -> PLN (dynamiczny kurs z NBP)
                return round((price_cents / 100.0) * usd_rate, 2)
        
        return None
        
    except Exception as e:
        logger.error(f"[CSFLOAT] Błąd: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# CSV — dane treningowe ML
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_csv_header() -> None:
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_COLUMNS)


def append_csv_row(
    timestamp: str, item_name: str, steam_price: float,
    csfloat_price: float | None, volume: int | None,
) -> None:
    try:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                timestamp, item_name, steam_price,
                csfloat_price if csfloat_price is not None else "",
                volume        if volume        is not None else "",
            ])
    except OSError as exc:
        logger.warning("CSV write error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Steam — pobieranie ceny jednego itemu z exponential backoff
# ─────────────────────────────────────────────────────────────────────────────

def fetch_steam_item(item_name: str, usd_rate: float) -> bool:
    """
    Pobiera cenę Steam i CSFloat. Exponential backoff przy 429.
    NIGDY nie przerywa pętli harvestowania.
    NIGDY nie zapisuje 0.0 do bazy.
    """
    session = _get_steam_session()
    url     = STEAM_API.format(name=quote(item_name))

    for attempt, backoff_sec in enumerate(BACKOFF_STEPS_SEC + [None]):
        try:
            resp = session.get(url, timeout=15)
        except requests.RequestException as exc:
            logger.error("[STEAM] Błąd sieci dla %r: %s", item_name, exc)
            return False

        if resp.status_code == 429:
            if backoff_sec is None:
                logger.warning(
                    "[STEAM] 429 po %d próbach dla %r — pomijam, jadę dalej.",
                    len(BACKOFF_STEPS_SEC), item_name,
                )
                return False
            logger.warning(
                "[STEAM] 429 (próba %d/%d) dla %r — czekam %d min …",
                attempt + 1, len(BACKOFF_STEPS_SEC), item_name, backoff_sec // 60,
            )
            time.sleep(backoff_sec)
            continue

        if resp.status_code != 200:
            logger.warning("[STEAM] HTTP %d dla %r — skip", resp.status_code, item_name)
            return False

        break   # sukces
    else:
        return False

    try:
        payload = resp.json()
    except ValueError:
        logger.error("[STEAM] JSON decode error dla %r", item_name)
        return False

    if not payload.get("success"):
        logger.warning("[STEAM] success=false dla %r", item_name)
        return False

    steam_price = sanitize_steam_price(
        payload.get("lowest_price") or payload.get("median_price", "")
    )
    volume = sanitize_volume(payload.get("volume", ""))

    if steam_price is None:
        logger.warning("[STEAM] price=None dla %r — skip", item_name)
        return False

    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    # Pobierz cenę z CSFloat
    csfloat_price = fetch_csfloat_price(item_name, usd_rate)
    
    # Obowiązkowe opóźnienie po zapytaniu do CSFloat
    time.sleep(CSFLOAT_DELAY_SEC)

    # Asercja bezpieczeństwa — upewniamy się że nie zapisujemy 0.0
    if csfloat_price is not None and csfloat_price <= 0:
        logger.warning("[CSFLOAT] Nieprawidłowa cena %.4f dla %r — zapisuję NULL.",
                       csfloat_price, item_name)
        csfloat_price = None

    insert_price_record(item_name, steam_price, volume, csfloat_price)
    append_csv_row(ts_now, item_name, steam_price, csfloat_price, volume)

    if csfloat_price is not None:
        logger.info("[OK] %-50s | Steam: %7.2f PLN | CSFloat: %7.2f PLN | Vol: %s",
                     item_name, steam_price, csfloat_price, volume or "—")
    else:
        logger.info("[OK/NO-CSF] %-50s | Steam: %7.2f PLN | CSFloat: NULL | Vol: %s",
                     item_name, steam_price, volume or "—")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Cykl harvestowania
# ─────────────────────────────────────────────────────────────────────────────

def run_cycle() -> None:
    watchlist = get_watchlist()
    if not watchlist:
        logger.info("Watchlist pusta — nic do harvestowania.")
        return

    # Pobieramy kurs raz na początku cyklu
    current_usd_rate = get_usd_pln_rate()

    logger.info("═══ Cykl harvestowania — %d itemów | Kurs USD: %.4f ═══", 
                len(watchlist), current_usd_rate)

    successes = failures = 0
    for item_name in watchlist:
        # Przekazujemy kurs do funkcji niżej
        ok = fetch_steam_item(item_name, current_usd_rate)
        successes += int(ok)
        failures  += int(not ok)
        time.sleep(PER_ITEM_DELAY_SEC)

    logger.info("═══ Cykl zakończony — %d OK / %d pominiętych ═══",
                successes, failures)

# ─────────────────────────────────────────────────────────────────────────────
# Główna pętla
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    initialize_database()
    _ensure_csv_header()

    # Sprawdź czy klucz API CSFloat jest dostępny
    if not os.getenv("CSFLOAT_API_KEY"):
        logger.error(
            "BRAK CSFLOAT_API_KEY! Dodaj klucz API do pliku .env: "
            "CSFLOAT_API_KEY=twój_klucz_api"
        )
    else:
        logger.info("CSFloat API key: dostępny")

    logger.info("Harvester uruchomiony. Cykl: %d min. Delay: %d s/item. CSFloat delay: %.1f s",
                HARVEST_INTERVAL_SEC // 60, PER_ITEM_DELAY_SEC, CSFLOAT_DELAY_SEC)

    while True:
        t0 = time.monotonic()
        try:
            run_cycle()
        except Exception as exc:   # noqa: BLE001
            logger.exception("Nieobsłużony wyjątek w cyklu: %s", exc)

        elapsed   = time.monotonic() - t0
        sleep_for = max(0.0, HARVEST_INTERVAL_SEC - elapsed)
        logger.info("Następny cykl za %.1f min (ten trwał %.0f s).",
                     sleep_for / 60, elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()