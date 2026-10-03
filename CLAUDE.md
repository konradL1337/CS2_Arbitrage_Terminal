# ROLE: PRINCIPAL QUANT ENGINEER & MCP ARCHITECT

## 1. PROJECT TARGET (v3.0 - Deep Steam Market)
- **Goal:** Build an MCP (Model Context Protocol) Server exposing deep Steam Community Market (SCM) data for LLMs.
- **Architecture:** Local SQLite database -> Python Harvester (Daemon) -> Python MCP Server.
- **Market Data Scope:** We ONLY operate on the Steam Market. We dropped external P2P markets (CSFloat/Skinsmonkey) due to Cloudflare blocks.

## 2. STRICT ENGINEERING RULES
- **Language & Stack:** Python 3, `requests`, `sqlite3`, `re`. NO async/aiohttp for Steam (it causes instant 429 bans).
- **Steam API Axioms:**
  1. `priceoverview`: Gets `lowest_price` and `volume`. Requires item name.
  2. `itemordershistogram`: Gets Deep Order Book (`highest_bid`, `buy_order_volume`). Requires `item_nameid`.
- **Anti-Ban Protocol:** Minimum 8 seconds `time.sleep()` between iterations. Exponential backoff on HTTP 429.
- **Code Style:** Zero placeholders. Functional, minimal, fail-safe code. Rely heavily on robust `try-except` blocks and `logging`.

## 3. DATA HYGIENE
- Never log or expose API keys (always use `os.getenv`).
- Convert Steam price strings (e.g., "16,50 zł") strictly to floats (16.50).
- If data fetch fails, return `None` (SQL NULL), never `0.0`.
