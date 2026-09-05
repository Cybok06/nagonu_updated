"""
DataKazina transaction status tester

Purpose:
- Read your DataKazina API key from environment
- Check one transaction status using /fetch-single-transaction

Required env var:
- DATAKAZINA_API_KEY

Run:
    python datakazina_check_transaction.py
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

import requests


# ============================================================
# RUNTIME CONFIG
# ============================================================
API_KEY = os.getenv("DATAKAZINA_API_KEY", "dk_wC8jFCnsbwJJmWcMwNlxXQWNojxX43Gy").strip()
BASE_URL = os.getenv("DATAKAZINA_BASE_URL", "https://reseller.dakazinabusinessconsult.com/api/v1").strip()
TIMEOUT = int(os.getenv("DATAKAZINA_TIMEOUT", "45"))

TEST_TRANSACTION_ID = "109NAN20762_1_d7c32c0555037200"


# ============================================================
# HELPERS
# ============================================================
def pretty(title: str, data: Any) -> None:
    print(f"\n{'=' * 22} {title} {'=' * 22}")
    try:
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    except Exception:
        print(data)


def clean_header_value(value: str) -> str:
    if value is None:
        return ""
    return (
        str(value)
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\ufeff", "")
        .strip()
    )


def build_headers() -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": clean_header_value(API_KEY),
    }


def safe_parse_response(response: requests.Response) -> Dict[str, Any]:
    raw_text = response.text.strip() if response.text else ""

    if raw_text:
        try:
            parsed_body: Any = response.json()
        except Exception:
            parsed_body = {"raw_text": raw_text}
    else:
        parsed_body = {}

    return {
        "ok": 200 <= response.status_code < 300,
        "http_status": response.status_code,
        "headers": dict(response.headers),
        "body": parsed_body,
        "body_is_empty": not bool(raw_text),
    }


def validate_setup() -> None:
    if not API_KEY:
        raise ValueError("DATAKAZINA_API_KEY is missing in your environment.")
    if not TEST_TRANSACTION_ID.strip():
        raise ValueError("TEST_TRANSACTION_ID is required.")


def check_single_transaction(transaction_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL.rstrip('/')}/fetch-single-transaction"
    payload = {
        "transaction_id": str(transaction_id).strip(),
    }

    print(f"\n--> POST {url}")
    pretty("Check Transaction Status Request Payload", payload)

    response = requests.post(
        url,
        headers=build_headers(),
        json=payload,
        timeout=TIMEOUT,
    )
    result = safe_parse_response(response)
    pretty("Check Transaction Status", result)
    return result


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    validate_setup()

    pretty(
        "Runtime Config",
        {
            "base_url": BASE_URL,
            "api_key_from_env": bool(API_KEY),
            "timeout": TIMEOUT,
            "transaction_id": TEST_TRANSACTION_ID,
        },
    )

    try:
        check_single_transaction(TEST_TRANSACTION_ID)
        print("\nDone.")
    except requests.RequestException as exc:
        pretty("Network Error", {"ok": False, "error": str(exc)})
    except Exception as exc:
        pretty("Runtime Error", {"ok": False, "error": str(exc)})


if __name__ == "__main__":
    main()
