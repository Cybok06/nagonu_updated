"""
Runtime tester for the SkPlug bundle API.

Default behavior:
- POST one MTN order using the provided sample payload

Optional actions:
- `orders`: fetch your API order history
- `bundles`: fetch active bundle packages

Environment variables:
- SKPLUG_API_TOKEN   Optional override for the hardcoded bearer token
- SKPLUG_BASE_URL    Optional, defaults to https://skplug.onrender.com/api/v1
- SKPLUG_TIMEOUT     Optional, defaults to 45 seconds

Examples:
    python api_test/skplug_runtime.py
    python api_test/skplug_runtime.py --action bundles
    python api_test/skplug_runtime.py --action orders
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict

import requests


BASE_URL = os.getenv("SKPLUG_BASE_URL", "https://skplug.onrender.com/api/v1").strip()
API_TOKEN = os.getenv(
    "SKPLUG_API_TOKEN",
    "270103449bf5069c331eb4511845e6b43a9e9fd7d75d57d1ba317ca9342abcd3",
).strip()
TIMEOUT = int(os.getenv("SKPLUG_TIMEOUT", "45"))

DEFAULT_ORDER = {
    "recipient": "0532676186",
    "network": "MTN",
    "gb_size": "1",
}


def pretty(title: str, data: Any) -> None:
    print(f"\n{'=' * 22} {title} {'=' * 22}")
    try:
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    except Exception:
        print(data)


def build_url(path: str) -> str:
    return f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"


def build_headers() -> Dict[str, str]:
    token = API_TOKEN.strip()
    if not token:
        raise ValueError("SKPLUG_API_TOKEN is missing in your environment.")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def parse_response(response: requests.Response) -> Dict[str, Any]:
    raw_text = response.text.strip() if response.text else ""
    try:
        body = response.json() if raw_text else {}
    except Exception:
        body = {"raw_text": raw_text}

    return {
        "ok": response.ok,
        "http_status": response.status_code,
        "headers": dict(response.headers),
        "body": body,
    }


def place_order(payload: Dict[str, str]) -> Dict[str, Any]:
    url = build_url("/order/")
    pretty("Place Order Request", {"url": url, "payload": payload})
    response = requests.post(
        url,
        headers=build_headers(),
        json=payload,
        timeout=TIMEOUT,
    )
    result = parse_response(response)
    pretty("Place Order Response", result)
    return result


def list_orders() -> Dict[str, Any]:
    url = build_url("/orders/")
    pretty("List Orders Request", {"url": url})
    response = requests.get(
        url,
        headers=build_headers(),
        timeout=TIMEOUT,
    )
    result = parse_response(response)
    pretty("List Orders Response", result)
    return result


def list_bundles() -> Dict[str, Any]:
    url = build_url("/bundles/")
    pretty("List Bundles Request", {"url": url})
    response = requests.get(
        url,
        headers=build_headers(),
        timeout=TIMEOUT,
    )
    result = parse_response(response)
    pretty("List Bundles Response", result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Runtime tester for the SkPlug API.")
    parser.add_argument(
        "--action",
        choices=("order", "orders", "bundles"),
        default="order",
        help="API action to run. Default: order",
    )
    parser.add_argument(
        "--recipient",
        default=DEFAULT_ORDER["recipient"],
        help="Recipient phone number for the order action.",
    )
    parser.add_argument(
        "--network",
        default=DEFAULT_ORDER["network"],
        help="Network for the order action.",
    )
    parser.add_argument(
        "--gb-size",
        dest="gb_size",
        default=DEFAULT_ORDER["gb_size"],
        help="Bundle size in GB for the order action.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    pretty(
        "Runtime Config",
        {
            "base_url": BASE_URL,
            "api_token_present": bool(API_TOKEN),
            "timeout": TIMEOUT,
            "action": args.action,
        },
    )

    try:
        if args.action == "order":
            place_order(
                {
                    "recipient": str(args.recipient).strip(),
                    "network": str(args.network).strip().upper(),
                    "gb_size": str(args.gb_size).strip(),
                }
            )
        elif args.action == "orders":
            list_orders()
        else:
            list_bundles()
        print("\nDone.")
    except requests.RequestException as exc:
        pretty("Network Error", {"ok": False, "error": str(exc)})
    except Exception as exc:
        pretty("Runtime Error", {"ok": False, "error": str(exc)})


if __name__ == "__main__":
    main()
