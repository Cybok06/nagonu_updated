import json
import requests
from _config import BASE_URL, API_KEY

# Fill these before running
SERVICE_NAME = "MTN NORMAL"
NETWORK = "MTN"
OFFER_ID = 1
RECIPIENT_NUMBER = "0554226398"


def main():
    if not NETWORK:
        raise SystemExit("Set NETWORK from /api/packages.php output")

    url = f"{BASE_URL.rstrip('/')}/api/initiate.php"
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    payload = {
        "recipient_number": RECIPIENT_NUMBER,
        "service_name": SERVICE_NAME,
        "network": NETWORK,
        "offer_id": OFFER_ID,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    print("HTTP", r.status_code)
    try:
        print(json.dumps(r.json(), indent=2, ensure_ascii=False))
    except Exception:
        print(r.text)


if __name__ == "__main__":
    main()
