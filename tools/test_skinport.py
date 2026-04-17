from curl_cffi import requests

def test_skinport():
    url = "https://api.skinport.com/v1/items?app_id=730&currency=PLN"
    print(f"Uderzam do: {url}")
    try:
        response = requests.get(url, impersonate="chrome120", timeout=15)
        print(f"KOD ODPOWIEDZI: {response.status_code}")
        if response.status_code == 200:
            print("SUKCES! Skinport dziala!")
        else:
            print("BLAD:")
            print(response.text[:200])
    except Exception as e:
        print(f"CRASH: {e}")

if __name__ == "__main__":
    test_skinport()
