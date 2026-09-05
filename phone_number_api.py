from __future__ import annotations

import hmac

from flask import Blueprint, jsonify, request

from phone_number_registry import register_phone_number


phone_number_api_bp = Blueprint("phone_number_api", __name__)
EXPECTED_REFERENCE = "ussd_number_245"


@phone_number_api_bp.post("/internal/phone-numbers/register")
def register_phone_number_endpoint():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "error": "invalid_json"}), 400
    supplied_reference = str(payload.get("reference") or "")
    if not hmac.compare_digest(supplied_reference, EXPECTED_REFERENCE):
        return jsonify({"success": False, "error": "forbidden"}), 403
    try:
        register_phone_number(payload.get("phone_number"), payload.get("agent_id"))
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception:
        return jsonify({"success": False, "error": "registry_unavailable"}), 503
    return jsonify({"success": True}), 200
