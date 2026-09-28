import json
import requests
from _config import BASE_URL, API_KEY


def main():
    url = f"{BASE_URL.rstrip('/')}/api/packages.php"
    headers = {"x-api-key": API_KEY}
    r = requests.get(url, headers=headers, timeout=30)
    print("HTTP", r.status_code)
    try:
        data = r.json()
    except Exception:
        print(r.text)
        return

    print(json.dumps(data, indent=2, ensure_ascii=False))

    # Friendly summary: service_name, network, offer_id, package, amount
    try:
        packs = (data.get("data") or {}).get("data") or {}
        regular = packs.get("regular_packages") or []
        bigtime = packs.get("bigtime_packages") or []
        print("\nRegular packages (condensed):")
        for p in regular:
            print(
                {
                    "service_name": p.get("service_name"),
                    "network": p.get("network"),
                    "offer_id": p.get("offer_id"),
                    "package": p.get("package"),
                    "amount": p.get("amount"),
                }
            )
        print("\nBigTime packages (condensed):")
        for p in bigtime:
            print(
                {
                    "service_name": p.get("service_name"),
                    "network": p.get("network"),
                    "offer_id": p.get("offer_id"),
                    "package": p.get("package"),
                    "amount": p.get("amount"),
                }
            )
    except Exception:
        pass


if __name__ == "__main__":
    main()
