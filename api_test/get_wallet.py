import json
import requests
from _config import BASE_URL, API_KEY


def main():
    url = f"{BASE_URL.rstrip('/')}/api/wallet.php"
    headers = {"x-api-key": API_KEY}
    r = requests.get(url, headers=headers, timeout=30)
    print("HTTP", r.status_code)
    try:
        print(json.dumps(r.json(), indent=2, ensure_ascii=False))
    except Exception:
        print(r.text)


if __name__ == "__main__":
    main()
