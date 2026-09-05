"""Runtime diagnostic for the DataKazina console-balance endpoint."""

from __future__ import annotations

import json
import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv


load_dotenv(".env", override=True)

BASE_URL = os.getenv(
    "DATAKAZINA_BASE_URL",
    "https://reseller.dakazinabusinessconsult.com/api/v1",
).rstrip("/")
API_KEY = (os.getenv("DATAKAZINA_API_KEY") or "").strip()
TIMEOUT_SECONDS = int(os.getenv("DATAKAZINA_TIMEOUT", "45"))


def _print_body(response: requests.Response) -> None:
    text = (response.text or "").strip()
    if not text:
        print("Response body: <empty>")
        return

    try:
        payload: Any = response.json()
        print("Response body:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    except ValueError:
        print("Response body:")
        print(text)


def main() -> int:
    if not API_KEY:
        print("ERROR: DATAKAZINA_API_KEY is missing from .env.")
        return 2

    url = f"{BASE_URL}/check-console-balance"
    headers = {
        "Accept": "application/json",
        "x-api-key": API_KEY,
    }

    print("DataKazina console-balance runtime test")
    print(f"Method: GET")
    print(f"URL: {url}")
    print(f"API key loaded: yes (length {len(API_KEY)}; value hidden)")

    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        print(f"Network error: {type(exc).__name__}: {exc}")
        return 3

    print(f"HTTP status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type', '<missing>')}")
    _print_body(response)

    if response.ok:
        print("Result: request succeeded.")
        return 0

    print("Result: DataKazina returned an error response.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
