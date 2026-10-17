"""Register Bundle Portal's status callback for the live Nagonu site.

Run once with: python runtime_set_bundleportal_webhook.py
Bundle Portal returns the signing secret only when set_webhook is called. This
script stores that secret in the project .env so it is not lost in terminal
output. Copy it to the live app's environment if this script runs elsewhere.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv, set_key


ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"
API_URL = "https://api.bundleportal.com/v2"
WEBHOOK_URL = "https://nagonu.site/webhooks/bundleportal"


def main() -> int:
    load_dotenv(ENV_FILE)
    api_key = (os.getenv("BUNDLE_PORTAL_KEY") or "").strip()
    if not api_key:
        print("ERROR: BUNDLE_PORTAL_KEY is missing from the environment or project .env.")
        return 2

    print(f"Registering Bundle Portal webhook: {WEBHOOK_URL}")
    print("Note: set_webhook rotates the signing secret for this API account.")
    try:
        response = requests.post(
            API_URL,
            headers={"x-api-key": api_key, "Content-Type": "application/json", "Accept": "application/json"},
            json={"action": "set_webhook", "webhook_url": WEBHOOK_URL},
            timeout=(5, 30),
        )
    except requests.RequestException as exc:
        print(f"Network error: {type(exc).__name__}: {exc}")
        return 3

    try:
        payload = response.json()
    except ValueError:
        print(f"Bundle Portal returned HTTP {response.status_code} with a non-JSON response.")
        return 1

    if not response.ok or not isinstance(payload, dict) or payload.get("success") is not True:
        print(f"Webhook registration failed (HTTP {response.status_code}):")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 1

    data = payload.get("data") or {}
    secret = data.get("webhook_secret") if isinstance(data, dict) else None
    if not isinstance(secret, str) or not secret.strip():
        print("Registration response did not include the one-time webhook secret.")
        print("Do not call set_webhook again unless you intend to rotate it.")
        return 1

    try:
        set_key(str(ENV_FILE), "BUNDLE_PORTAL_WEBHOOK_SECRET", secret.strip())
    except OSError as exc:
        # The provider only returns this secret once; show it if persistence fails.
        print(f"Could not save the secret to {ENV_FILE}: {exc}")
        print(f"Save this one-time value as BUNDLE_PORTAL_WEBHOOK_SECRET: {secret.strip()}")
        return 4

    print("Webhook registered successfully.")
    print(f"URL: {data.get('webhook_url') or WEBHOOK_URL}")
    print(f"Events: {', '.join(data.get('events') or [])}")
    print(f"Signing secret saved to {ENV_FILE} (value hidden).")
    print("Set BUNDLE_PORTAL_WEBHOOK_SECRET in the live site's environment and restart the app if this was run locally.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
