import json
import requests
from _config import BASE_URL, API_KEY

# Set REFERENCE_ID from the order response
REFERENCE_ID = ""


def main():
    if not REFERENCE_ID:
        raise SystemExit("Set REFERENCE_ID from initiate response")

    url = f"{BASE_URL.rstrip('/')}/api/response_big_time.php"
    headers = {"x-api-key": API_KEY}
    params = {"reference_id": REFERENCE_ID}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    print("HTTP", r.status_code)
    try:
        print(json.dumps(r.json(), indent=2, ensure_ascii=False))
    except Exception:
        print(r.text)


if __name__ == "__main__":
    main()
