"""
skinport_diagnose.py — Isolated Skinport API Diagnostic
========================================================
Run this ONCE from your CS_MATRIX folder:
    python skinport_diagnose.py

It makes ONE request to Skinport, logs every observable fact,
and prints a structured report. Paste the full output to Claude.

No changes to harvester.py. No assumptions. No fallbacks.
"""

import gzip
import json
import sys
import time
import requests

URL = "https://api.skinport.com/v1/items?app_id=730&currency=PLN&tradable=0"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 0: Environment info
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("SKINPORT DIAGNOSTIC REPORT")
print(f"Time          : {time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Python        : {sys.version}")
print(f"requests ver  : {requests.__version__}")

try:
    import brotli
    print(f"brotli        : INSTALLED ({brotli.__version__})")
    BROTLI_AVAILABLE = True
except ImportError:
    print("brotli        : NOT INSTALLED (pip install brotli)")
    BROTLI_AVAILABLE = False

print("=" * 70)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Make the request — three variants tested in order
# ─────────────────────────────────────────────────────────────────────────────

variants = [
    {
        "label": "VARIANT A — No custom headers (bare requests.get)",
        "headers": {},
    },
    {
        "label": "VARIANT B — Browser UA only, no Accept-Encoding",
        "headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        },
    },
    {
        "label": "VARIANT C — Browser UA + explicit gzip only (no br)",
        "headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "application/json",
            "Accept-Encoding": "gzip, deflate",   # NO brotli
        },
    },
]

for variant in variants:
    print()
    print("─" * 70)
    print(variant["label"])
    print("─" * 70)

    # ── 1. HTTP request ───────────────────────────────────────────────────────
    print(f"[1] URL          : {URL}")
    print(f"[1] Sent headers : {variant['headers'] or '(default requests headers)'}")

    t0 = time.monotonic()
    try:
        resp = requests.get(URL, headers=variant["headers"], timeout=60, allow_redirects=True)
        elapsed = time.monotonic() - t0
    except requests.exceptions.Timeout:
        print("[1] RESULT       : TIMEOUT (>60 s)")
        continue
    except requests.RequestException as exc:
        print(f"[1] RESULT       : NETWORK ERROR — {exc}")
        continue

    # ── 2. Status and response headers ───────────────────────────────────────
    print(f"[1] Elapsed      : {elapsed:.2f} s")
    print(f"[2] HTTP Status  : {resp.status_code}")
    print(f"[2] Content-Type : {resp.headers.get('Content-Type', 'MISSING')}")
    print(f"[2] Content-Enc  : {resp.headers.get('Content-Encoding', 'MISSING/NONE')}")
    print(f"[2] Transfer-Enc : {resp.headers.get('Transfer-Encoding', 'MISSING/NONE')}")
    print(f"[2] Redirect hist: {[r.status_code for r in resp.history] or 'none'}")
    print(f"[2] Final URL    : {resp.url}")

    # ── 3. Raw body inspection ────────────────────────────────────────────────
    raw = resp.content
    print(f"[3] Raw body len : {len(raw)} bytes  ({len(raw)/1024:.1f} KB)")
    print(f"[3] First 50 hex : {raw[:50].hex()}")
    print(f"[3] First 50 repr: {raw[:50]!r}")

    # Classify the body
    GZIP_MAGIC   = b"\x1f\x8b"
    BROTLI_MAGIC = None   # brotli has no universal magic bytes
    JSON_STARTS  = (b"[", b"{")
    HTML_STARTS  = (b"<", b"<!",)

    if raw[:2] == GZIP_MAGIC:
        print("[3] Body type    : GZIP compressed — needs decompression")
        try:
            raw = gzip.decompress(raw)
            print(f"[3] After gunzip  : {len(raw)} bytes  ({len(raw)/1024:.1f} KB)")
            print(f"[3] Gunzip first50: {raw[:50]!r}")
        except Exception as e:
            print(f"[3] Gunzip FAILED : {e}")
            continue
    elif raw[:1] in (b"[", b"{"):
        print("[3] Body type    : Plain JSON (no compression)")
    elif raw[:1] == b"<":
        print("[3] Body type    : HTML (Cloudflare error page?)")
        print(f"[3] HTML preview : {raw[:300].decode('utf-8', errors='replace')!r}")
        continue
    else:
        # Could be Brotli
        print("[3] Body type    : UNKNOWN binary (possibly Brotli?)")
        if BROTLI_AVAILABLE:
            try:
                import brotli as _brotli
                raw = _brotli.decompress(raw)
                print(f"[3] After brotli  : {len(raw)} bytes")
                print(f"[3] Brotli first50: {raw[:50]!r}")
            except Exception as e:
                print(f"[3] Brotli decomp FAILED: {e}")
                print(f"[3] Raw as text  : {raw[:200].decode('utf-8', errors='replace')!r}")
                continue
        else:
            print("[3] Can't decompress — install brotli: pip install brotli")
            print(f"[3] Raw as text  : {raw[:200].decode('utf-8', errors='replace')!r}")
            continue

    # ── 4. JSON structure validation ──────────────────────────────────────────
    text = raw.decode("utf-8", errors="replace")
    print(f"[4] Text preview : {text[:200]!r}")

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[4] JSON parse   : FAILED — {e}")
        print(f"[4] Near error   : {text[max(0,e.pos-30):e.pos+50]!r}")
        continue

    print(f"[4] JSON type    : {type(data).__name__}")

    if isinstance(data, list):
        print(f"[4] List length  : {len(data)} items")
        if data:
            first = data[0]
            print(f"[4] Item[0] keys : {list(first.keys()) if isinstance(first, dict) else type(first).__name__}")
            if isinstance(first, dict):
                # Show the values for the fields we care about
                for field in ["market_hash_name", "marketHashName", "name",
                               "min_price", "minPrice", "price",
                               "currency", "quantity"]:
                    if field in first:
                        print(f"[4] Item[0][{field!r:20}] = {first[field]!r}")
        if len(data) > 1:
            print(f"[4] Item[1] name : {data[1].get('market_hash_name') or data[1].get('marketHashName','NO NAME FIELD')!r}")
    elif isinstance(data, dict):
        print(f"[4] Dict keys    : {list(data.keys())[:10]}")
        print(f"[4] Full preview : {json.dumps(data, ensure_ascii=False)[:400]}")
    else:
        print(f"[4] Unexpected   : {type(data)} = {str(data)[:200]!r}")

    # ── 5. Quick lookup test with actual data ─────────────────────────────────
    if isinstance(data, list) and data and isinstance(data[0], dict):
        print()
        print("[5] LOOKUP TEST — checking watchlist names against real data")
        name_field  = "market_hash_name" if "market_hash_name" in data[0] else "marketHashName"
        price_field = "min_price"        if "min_price"        in data[0] else "minPrice"

        # Build simple exact dict
        exact = {}
        for item in data:
            n = item.get(name_field, "")
            p = item.get(price_field)
            if n and p is not None:
                try:
                    exact[n.strip()] = float(p)
                except (ValueError, TypeError):
                    pass

        print(f"[5] Prices loaded: {len(exact)}")

        test_names = [
            "AK-47 | Redline (Field-Tested)",
            "AK-47 | Emerald Pinstripe (Minimal Wear)",
            "Kilowatt Case",
            "Dreams & Nightmares Case",
            "Sticker | ENCE (Holo) | Stockholm 2021",
            "Sticker | Evil Geniuses (Holo) | Stockholm 2021",
        ]
        for name in test_names:
            found = exact.get(name)
            status = f"✅ {found:.2f} PLN" if found else "❌ NOT FOUND"
            print(f"[5]   {status:20} — {name!r}")

        # Show 5 sample names from the real data
        sample_names = [v.get(name_field,"") for v in data[:5]]
        print(f"[5] Real names sample (first 5):")
        for n in sample_names:
            print(f"[5]   {n!r}")

    print(f"\n[VARIANT DONE] ✅")
    # If this variant worked, no need to try others
    if resp.status_code == 200 and isinstance(data, list) and len(data) > 100:
        print("[STOPPING] First working variant found — skipping remaining variants.")
        break

print()
print("=" * 70)
print("DIAGNOSTIC COMPLETE — paste everything above to Claude")
print("=" * 70)
