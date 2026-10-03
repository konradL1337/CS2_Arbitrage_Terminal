import os
import requests
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()
api_key = os.getenv("CSFLOAT_API_KEY")

# Testujemy na Redline - on MUSI tam być
item = "AK-47 | Redline (Field-Tested)"
encoded_item = quote(item)
url = f"https://csfloat.com/api/v1/listings?market_hash_name={encoded_item}&limit=1"

print(f"DEBUG: Uderzam do: {url}")
headers = {"Authorization": api_key}

try:
    resp = requests.get(url, headers=headers, timeout=10)
    print(f"DEBUG: Status Code: {resp.status_code}")
    print(f"DEBUG: Surowa odpowiedź: {resp.text[:500]}")
except Exception as e:
    print(f"DEBUG: Crash: {e}")