"""
harvester.py — CS2 Market Analytics Terminal

DIAGNOZA PROBLEMU 403 (Oracle / Cloudflare):
    Serwery Oracle Cloud mają publiczne IP oznaczone jako datacenter.
    Cloudflare przed Skinport blokuje takie IP kodem 403.
    Rozwiązanie: curl_cffi — biblioteka która emuluje odcisk TLS
    prawdziwej przeglądarki Chrome, przez co Cloudflare nie rozróżnia
    żądania serwera od żądania zwykłego użytkownika.

    pip install curl_cffi python-dotenv

ENDPOINT /v1/items:
    Według oficjalnej dokumentacji Skinport:
    "No authorization required. Accept-Encoding: br is required."
    Basic Auth (CLIENT_ID / CLIENT_SECRET) NIE jest wymagane dla tego
    endpointu — jest potrzebne tylko do prywatnych endpointów
    (transakcje, portfel konta). Dodajemy je jako opcjonalne — jeśli
    plik .env zawiera klucze, zostaną wysłane (dla przyszłych potrzeb).

EXPONENTIAL BACKOFF (Steam 429):
    Próba 1 → czekaj 5 min → próba 2
    Próba 2 → czekaj 10 min → próba 3
    Próba 3 → czekaj 15 min → skip item (pętla NIE przerywa się)
"""

import base64
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

# ── curl_cffi: emulacja TLS przeglądarki — omija Cloudflare ─────────────────
try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False
    import requests as fallback_requests

# ── python-dotenv: wczytanie .env ─────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # .env nie jest wymagany — zmienne mogą być w środowisku systemu

import requests as requests_lib   # do sesji Steam (nie potrzebuje curl_cffi)

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
SKINPORT_API = "https://api.skinport.com/v1/items?app_id=730&currency=PLN&tradable=0"

HARVEST_INTERVAL_SEC = 30 * 60
PER_ITEM_DELAY_SEC   = 6

# Exponential backoff dla Steam 429
BACKOFF_STEPS_SEC = [5 * 60, 10 * 60, 15 * 60]

CSV_PATH    = Path("training_data.csv")
CSV_COLUMNS = ["timestamp", "item_name", "steam_price", "skinport_price", "volume"]

# ─────────────────────────────────────────────────────────────────────────────
# Klucze Skinport z .env (opcjonalne dla /v1/items, wymagane dla prywatnych)
# ─────────────────────────────────────────────────────────────────────────────
# Utwórz plik .env w tym samym folderze co harvester.py:
#   SKINPORT_CLIENT_ID=twoj_client_id
#   SKINPORT_CLIENT_SECRET=twoj_client_secret
# Jeśli plik .env nie istnieje lub klucze są puste — endpoint działa bez Auth.

def _build_skinport_auth_header() -> str | None:
    """
    Buduje nagłówek Authorization: Basic <base64(id:secret)>.
    Zwraca None jeśli klucze nie są skonfigurowane.
    """
    client_id     = os.getenv("SKINPORT_CLIENT_ID",     "").strip()
    client_secret = os.getenv("SKINPORT_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        return None

    credentials = f"{client_id}:{client_secret}"
    encoded     = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    return f"Basic {encoded}"


# ─────────────────────────────────────────────────────────────────────────────
# Sesja Steam (zwykły requests — Steam nie jest za Cloudflare)
# ─────────────────────────────────────────────────────────────────────────────
_steam_session: requests_lib.Session | None = None


def _get_steam_session() -> requests_lib.Session:
    global _steam_session
    if _steam_session is None:
        _steam_session = requests_lib.Session()
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
    s = _STRIP_RE.sub("", raw).strip()
    try:
        return int(s)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Normalizacja nazw Steam ↔ Skinport
# ─────────────────────────────────────────────────────────────────────────────
_SKP_PREFIXES = sorted([
    "sticker | ", "patch | ", "graffiti | ",
    "sealed graffiti | ", "music kit | ",
    "collectible pin | ", "pin | ",
], key=len, reverse=True)

_NORM_RE = re.compile(r"[^a-z0-9]")


def normalize_name(name: str) -> str:
    n = unicodedata.normalize("NFC", name).lower().strip()
    for prefix in _SKP_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):]
            break
    return _NORM_RE.sub("", n)


def _clean_ws(s: str) -> str:
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace("\u2009", " ").strip()
    return re.sub(r" {2,}", " ", s)


# ─────────────────────────────────────────────────────────────────────────────
# Skinport bulk fetch — curl_cffi omija Cloudflare 403
# ─────────────────────────────────────────────────────────────────────────────

def fetch_skinport_prices() -> tuple[dict[str, float], dict[str, float]]:
    logger.info("Skinport ── pobieranie cen (PLN, tradable=0) …")

    auth_header = _build_skinport_auth_header()
    if auth_header:
        logger.info("Skinport ── Basic Auth: aktywne")
    else:
        logger.info("Skinport ── Basic Auth: brak — endpoint publiczny, OK")

    resp = None

    if CURL_CFFI_AVAILABLE:
        try:
            # WHY: NIE ustawiamy Accept-Encoding w headers= gdy używamy impersonate.
            # curl_cffi z impersonate="chrome124" sam dobiera nagłówki kompresji
            # pasujące do fingerprinta Chrome — w tym "br".
            # Ręczne ustawienie Accept-Encoding może kolidować z impersonation.
            curl_headers = {
                "Accept":          "application/json, */*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            if auth_header:
                curl_headers["Authorization"] = auth_header

            resp = curl_requests.get(
                SKINPORT_API,
                headers=curl_headers,
                impersonate="chrome124",
                timeout=90,
            )
            logger.info(
                "Skinport ── curl_cffi HTTP %d | Content-Type: %s | %.1f KB",
                resp.status_code,
                resp.headers.get("Content-Type", "?"),
                len(resp.content) / 1024,
            )
        except Exception as exc:
            logger.warning("Skinport ── curl_cffi error: %s. Fallback.", exc)
            resp = None
    else:
        logger.warning("BRAK curl_cffi! pip install curl_cffi")

    # Fallback: zwykły requests
    if resp is None:
        try:
            import requests as _req
            fb_headers = {
                "Accept":          "application/json, */*;q=0.8",
                "Accept-Encoding": "br, gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
            }
            if auth_header:
                fb_headers["Authorization"] = auth_header
            resp = _req.get(SKINPORT_API, headers=fb_headers, timeout=90)
            logger.info(
                "Skinport ── fallback HTTP %d | %.1f KB",
                resp.status_code, len(resp.content) / 1024,
            )
        except Exception as exc:
            logger.warning("Skinport ── fallback zawiódł: %s", exc)
            return {}, {}

    if resp.status_code == 403:
        logger.error("Skinport ── 403 Cloudflare. Body: %.200s", resp.text[:200])
        return {}, {}
    if resp.status_code == 406:
        logger.error("Skinport ── 406 brak br encoding. Body: %.200s", resp.text[:200])
        return {}, {}
    if resp.status_code == 429:
        logger.warning("Skinport ── 429 rate-limited.")
        return {}, {}
    if resp.status_code != 200:
        logger.warning("Skinport ── HTTP %d. Body: %.200s",
                       resp.status_code, resp.text[:200])
        return {}, {}
    if "text/html" in resp.headers.get("Content-Type", ""):
        logger.warning("Skinport ── HTML zamiast JSON. Body: %.200s", resp.text[:200])
        return {}, {}

    try:
        data = resp.json()
    except Exception as exc:
        logger.warning("Skinport ── JSON parse error: %s | Body: %.300s",
                       exc, resp.text[:300])
        return {}, {}

    # Skinport może zwrócić listę LUB dict {"items": [...]} — obsługujemy oba
    if isinstance(data, dict):
        # Szukamy klucza który zawiera listę itemów
        for key in ("items", "data", "results"):
            if isinstance(data.get(key), list):
                logger.info("Skinport ── dict response, używam klucza %r", key)
                data = data[key]
                break
        else:
            logger.warning(
                "Skinport ── dict bez listy. Klucze: %s | Preview: %.300s",
                list(data.keys()), str(data)[:300],
            )
            return {}, {}

    if not isinstance(data, list):
        logger.warning("Skinport ── oczekiwano listy, dostałem %s. Preview: %.200s",
                       type(data).__name__, str(data)[:200])
        return {}, {}

    logger.info("Skinport ── JSON lista: %d wpisów.", len(data))

    # Próbka — pokaże dokładne nazwy pól i wartości
    if data and isinstance(data[0], dict):
        logger.info("Skinport ── próbka [0]: %s",
                    {k: repr(v)[:80] for k, v in list(data[0].items())[:10]})

    exact_map: dict[str, float] = {}
    fuzzy_map: dict[str, float] = {}
    cnt_null = cnt_bad = 0

    for entry in data:
        try:
            if not isinstance(entry, dict):
                cnt_bad += 1
                continue

            # Sprawdzamy oba możliwe nazwy pola
            name = (
                entry.get("market_hash_name")
                or entry.get("marketHashName")
                or entry.get("name")
                or ""
            )
            name = _clean_ws(str(name))
            if not name:
                cnt_bad += 1
                continue

            # Sprawdzamy oba możliwe nazwy pola ceny
            raw_price = (
                entry.get("min_price")
                or entry.get("minPrice")
                or entry.get("price")
            )
            if raw_price is None:
                cnt_null += 1
                continue

            price = float(raw_price)
            if price <= 0:
                cnt_null += 1
                continue

            exact_map[name]                = price
            fuzzy_map[normalize_name(name)] = price

        except (TypeError, ValueError, AttributeError) as exc:
            cnt_bad += 1
            logger.debug("Skinport entry error: %s  entry=%r", exc, entry)

    logger.info("Skinport ── ZAŁADOWANO: %d cen | %d null | %d błędnych",
                len(exact_map), cnt_null, cnt_bad)

    if len(exact_map) == 0 and len(data) > 0:
        logger.error(
            "Skinport ── ZERO cen z %d wpisów! "
            "Sprawdź próbkę [0] powyżej — nazwa pola min_price lub market_hash_name mogła się zmienić.",
            len(data),
        )

    return exact_map, fuzzy_map

# ─────────────────────────────────────────────────────────────────────────────
# Lookup Skinport — gwarancja NULL, nigdy 0.0
# ─────────────────────────────────────────────────────────────────────────────

def lookup_skinport(
    item_name: str,
    exact_map: dict[str, float],
    fuzzy_map: dict[str, float],
) -> float | None:
    """Zwraca cenę > 0 lub None. None → SQLite NULL. NIGDY 0.0."""
    cleaned = _clean_ws(item_name)

    price = exact_map.get(cleaned)
    if price is not None and price > 0:
        return price

    key   = normalize_name(cleaned)
    price = fuzzy_map.get(key)
    if price is not None and price > 0:
        logger.warning("[FUZZY MATCH] %r → key=%r → %.2f PLN", item_name, key, price)
        return price

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
    skinport_price: float | None, volume: int | None,
) -> None:
    try:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                timestamp, item_name, steam_price,
                skinport_price if skinport_price is not None else "",
                volume         if volume         is not None else "",
            ])
    except OSError as exc:
        logger.warning("CSV write error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Steam — pobieranie ceny jednego itemu z exponential backoff
# ─────────────────────────────────────────────────────────────────────────────

def fetch_steam_item(
    item_name: str,
    exact_map: dict[str, float],
    fuzzy_map: dict[str, float],
) -> bool:
    """
    Pobiera cenę Steam. Exponential backoff przy 429.
    NIGDY nie przerywa pętli harvestowania.
    NIGDY nie zapisuje 0.0 do bazy.
    """
    session = _get_steam_session()
    url     = STEAM_API.format(name=quote(item_name))

    for attempt, backoff_sec in enumerate(BACKOFF_STEPS_SEC + [None]):
        try:
            resp = session.get(url, timeout=15)
        except requests_lib.RequestException as exc:
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

    ts_now         = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    external_price = lookup_skinport(item_name, exact_map, fuzzy_map)

    # Asercja bezpieczeństwa — upewniamy się że nie zapisujemy 0.0
    if external_price is not None and external_price <= 0:
        logger.warning("[SKINPORT] Nieprawidłowa cena %.4f dla %r — zapisuję NULL.",
                       external_price, item_name)
        external_price = None

    insert_price_record(item_name, steam_price, volume, external_price)
    append_csv_row(ts_now, item_name, steam_price, external_price, volume)

    if external_price is not None:
        logger.info("[OK] %-50s | Steam: %7.2f PLN | Skinport: %7.2f PLN | Vol: %s",
                    item_name, steam_price, external_price, volume or "—")
    else:
        logger.info("[OK/NO-SKP] %-50s | Steam: %7.2f PLN | Skinport: NULL | Vol: %s",
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

    logger.info("═══ Cykl harvestowania — %d itemów ═══", len(watchlist))

    exact_map, fuzzy_map = fetch_skinport_prices()

    if exact_map:
        cleaned   = [_clean_ws(n) for n in watchlist]
        n_exact   = sum(1 for n in cleaned if n in exact_map)
        n_fuzzy   = sum(1 for n in cleaned
                        if n not in exact_map and normalize_name(n) in fuzzy_map)
        n_missing = len(watchlist) - n_exact - n_fuzzy
        logger.info("Skinport coverage: %d exact | %d fuzzy | %d not found",
                    n_exact, n_fuzzy, n_missing)
        if n_missing:
            missing = [n for n in cleaned
                       if n not in exact_map and normalize_name(n) not in fuzzy_map]
            logger.info("Nie na Skinport: %s", ", ".join(repr(n) for n in missing))
    else:
        logger.warning("Skinport 0 cen — external_price=NULL dla wszystkich.")

    successes = failures = 0
    for item_name in watchlist:
        ok = fetch_steam_item(item_name, exact_map, fuzzy_map)
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

    # Informacja o dostępności curl_cffi przy starcie
    if CURL_CFFI_AVAILABLE:
        logger.info("curl_cffi dostępne — Cloudflare bypass aktywny.")
    else:
        logger.error(
            "BRAK curl_cffi! Skinport może być blokowany przez Cloudflare (403). "
            "Zainstaluj: pip install curl_cffi"
        )

    logger.info("Harvester uruchomiony. Cykl: %d min. Delay: %d s/item.",
                HARVEST_INTERVAL_SEC // 60, PER_ITEM_DELAY_SEC)

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
