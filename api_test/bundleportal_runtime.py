"""Read-only account check: python api_test/bundleportal_runtime.py."""
import json
from pathlib import Path
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from bundle_portal import PROVIDERS, call, configured


def main():
    if not configured():
        print("Set BUNDLE_PORTAL_KEY in the server environment or .env first.")
        return 1
    failed = False
    for network in PROVIDERS.values():
        payload = call("get_bundles", network=network)
        print(json.dumps({"network": network, "result": payload}, indent=2))
        failed = failed or not payload.get("success")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
