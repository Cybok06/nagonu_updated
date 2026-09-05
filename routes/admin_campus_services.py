from __future__ import annotations

from datetime import datetime
import json
import re
from ast import literal_eval

from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify, Request
from bson import ObjectId

from db import campus_db

admin_campus_services_bp = Blueprint("admin_campus_services", __name__)

campus_services_col = campus_db["services"]

ALLOWED_PROVIDERS = {"bundleportal", "codecraft", "datakazina"}
_ALLOWED_TYPES = {"API", "OFF"}


def _require_admin():
    return session.get("role") == "admin"


def _to_float(s):
    try:
        return float(s)
    except Exception:
        return None


def _to_int(s):
    try:
        if isinstance(s, str):
            s = s.replace(",", "").strip()
        return int(float(s))
    except Exception:
        return None


def _norm_type(t: str | None) -> str | None:
    if not t:
        return None
    t = t.strip().upper()
    return t if t in _ALLOWED_TYPES else None


_MB_RE = re.compile(r"^\s*([\d,]+(?:\.\d+)?)\s*MB\s*$", re.I)
_GB_RE = re.compile(r"^\s*([\d,]+(?:\.\d+)?)\s*G(?:B|IG)?\s*$", re.I)
_INT_RE = re.compile(r"^\s*[\d,]+\s*$")


def _parse_volume_to_mb(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(round(float(v)))
    txt = str(v).strip()

    m = _MB_RE.match(txt)
    if m:
        val = float(m.group(1).replace(",", ""))
        return int(round(val))

    m = _GB_RE.match(txt)
    if m:
        val = float(m.group(1).replace(",", ""))
        return int(round(val * 1000))

    if _INT_RE.match(txt):
        return int(txt.replace(",", ""))

    try:
        if txt.startswith("{") and txt.endswith("}"):
            as_json = json.loads(txt)
            if isinstance(as_json, dict) and "volume" in as_json:
                return _to_int(as_json["volume"])
    except Exception:
        pass

    try:
        d = literal_eval(txt)
        if isinstance(d, dict) and "volume" in d:
            return _to_int(d["volume"])
    except Exception:
        pass

    return None


def _format_volume(vol_mb):
    if vol_mb is None:
        return "-"
    try:
        vol_mb = float(vol_mb)
    except Exception:
        return "-"
    if vol_mb >= 1000:
        gb = vol_mb / 1000.0
        return f"{int(gb)}GB" if abs(gb - round(gb)) < 1e-9 else f"{gb:.2f}GB"
    return f"{int(vol_mb)}MB"


def _extract_pkg_id(value_raw):
    if value_raw is None:
        return None
    if isinstance(value_raw, (int, float)):
        return _to_int(value_raw)

    txt = str(value_raw).strip()
    if _INT_RE.match(txt):
        return _to_int(txt)

    try:
        if txt.startswith("{") and txt.endswith("}"):
            as_json = json.loads(txt)
            if isinstance(as_json, dict) and "id" in as_json:
                return _to_int(as_json["id"])
    except Exception:
        pass

    try:
        d = literal_eval(txt)
        if isinstance(d, dict) and "id" in d:
            return _to_int(d["id"])
    except Exception:
        pass

    return None


def _to_mtn_value_string(pkg_id: int | None, volume_mb: int | None, fallback_value_raw: str | None):
    if volume_mb is None:
        volume_mb = _parse_volume_to_mb(fallback_value_raw)
    volume_mb = _to_int(volume_mb) if volume_mb is not None else None
    pkg_id = _to_int(pkg_id) if pkg_id is not None else None
    if pkg_id is None or volume_mb is None:
        return None
    return f"{{'id': {pkg_id}, 'volume': {volume_mb}}}"


def _compute_value_text_from_mtn_string(value_str: str):
    if not isinstance(value_str, str):
        return "-"
    try:
        d = literal_eval(value_str)
        if not isinstance(d, dict):
            return value_str
        vol_mb = _to_int(d.get("volume"))
        pid = _to_int(d.get("id"))
        label = _format_volume(vol_mb)
        return f"{label} (Pkg {pid})" if pid else label
    except Exception:
        vol_mb = _parse_volume_to_mb(value_str)
        if vol_mb is not None:
            return _format_volume(vol_mb)
        return value_str or "-"


def _parse_offers(req: Request, prefix: str = "offers"):
    amount_key = f"{prefix}_amount[]"
    value_key = f"{prefix}_value[]"

    amounts = req.form.getlist(amount_key)
    values_freetext = req.form.getlist(value_key)

    n = max(len(amounts), len(values_freetext))
    offers = []
    auto_id_seed = 1

    for i in range(n):
        amount = (amounts[i] if i < len(amounts) else "").strip()
        value_txt = (values_freetext[i] if i < len(values_freetext) else "").strip()

        if not amount and not value_txt:
            continue

        base_amount = _to_float(amount)
        pkg_id = _extract_pkg_id(value_txt)
        vol_mb = _parse_volume_to_mb(value_txt)

        if pkg_id is None:
            pkg_id = auto_id_seed
            auto_id_seed += 1

        value_str = _to_mtn_value_string(pkg_id, vol_mb, value_txt)
        if value_str is None and (pkg_id is not None and vol_mb is not None):
            value_str = f"{{'id': {int(pkg_id)}, 'volume': {int(vol_mb)}}}"

        offers.append({"amount": base_amount, "value": value_str, "profit": None})

    return offers


def _norm_status_flag(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"open", "1", "true", "on", "yes"}:
        return "OPEN"
    if s in {"closed", "0", "false", "off", "no"}:
        return "CLOSED"
    return None


def _norm_availability_flag(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"available", "in_stock", "instock", "1", "true", "on", "yes"}:
        return "AVAILABLE"
    if s in {"out_of_stock", "outofstock", "oos", "unavailable", "0", "false", "off", "no"}:
        return "OUT_OF_STOCK"
    return None


@admin_campus_services_bp.route("/admin/campus-services", methods=["GET"])
def view_campus_services():
    if not _require_admin():
        return redirect(url_for("login.login"))

    services = list(
        campus_services_col.find(
            {},
            {
                "name": 1,
                "image_url": 1,
                "offers": 1,
                "store_offers": 1,
                "default_profit_percent": 1,
                "created_at": 1,
                "type": 1,
                "status": 1,
                "availability": 1,
                "provider": 1,
            },
        ).sort([("_id", -1)])
    )

    for s in services:
        s["_id_str"] = str(s["_id"])
        for key in ("offers", "store_offers"):
            if isinstance(s.get(key), list):
                for of in s[key]:
                    v = of.get("value")
                    of["value_text"] = _compute_value_text_from_mtn_string(v) if isinstance(v, str) else "-"

    return render_template("campus_services.html", services=services)


@admin_campus_services_bp.route("/admin/campus-services/<service_id>/update", methods=["POST"])
def update_campus_service(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        _id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_campus_services.view_campus_services"))

    service = campus_services_col.find_one({"_id": _id})
    if not service:
        flash("Service not found.", "danger")
        return redirect(url_for("admin_campus_services.view_campus_services"))

    service_name = (request.form.get("service_name") or "").strip()
    image_url = (request.form.get("image_url") or "").strip()
    default_profit_percent = _to_float(request.form.get("default_profit_percent"))
    service_type = _norm_type(request.form.get("service_type"))

    if not service_name:
        flash("Service name is required.", "danger")
        return redirect(url_for("admin_campus_services.view_campus_services"))

    offers = _parse_offers(request, "offers")
    store_offers = _parse_offers(request, "store_offers")

    update_doc = {
        "name": service_name,
        "offers": offers,
        "store_offers": store_offers,
        "updated_at": datetime.utcnow(),
    }
    if image_url:
        update_doc["image_url"] = image_url
    if default_profit_percent is not None:
        update_doc["default_profit_percent"] = default_profit_percent
    if service_type:
        update_doc["type"] = service_type

    campus_services_col.update_one({"_id": _id}, {"$set": update_doc})
    flash("Service updated successfully.", "success")
    return redirect(url_for("admin_campus_services.view_campus_services"))


@admin_campus_services_bp.route("/admin/campus-services/<service_id>/provider", methods=["POST"])
def set_campus_service_provider(service_id):
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        _id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    payload = request.get_json(silent=True) or {}
    provider = (payload.get("provider") or "").strip().lower()
    if provider not in ALLOWED_PROVIDERS:
        allowed = ", ".join(sorted(ALLOWED_PROVIDERS))
        return jsonify({"success": False, "error": f"provider must be one of: {allowed}"}), 400

    res = campus_services_col.update_one(
        {"_id": _id},
        {"$set": {"provider": provider, "updated_at": datetime.utcnow()}},
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    return jsonify({"success": True, "provider": provider})


@admin_campus_services_bp.route("/admin/campus-services/<service_id>/type", methods=["POST"])
def set_campus_service_type(service_id):
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        _id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    desired_raw = request.form.get("type")
    if desired_raw is None and request.is_json:
        payload = request.get_json(silent=True) or {}
        desired_raw = payload.get("type")
    desired = _norm_type(desired_raw)
    if not desired:
        return jsonify({"success": False, "error": "type must be 'API' or 'OFF'"}), 400

    res = campus_services_col.update_one(
        {"_id": _id},
        {"$set": {"type": desired, "updated_at": datetime.utcnow()}},
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    return jsonify({"success": True, "service_id": str(_id), "type": desired})


@admin_campus_services_bp.route("/admin/campus-services/<service_id>/status", methods=["POST"])
def set_campus_service_status(service_id):
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        _id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    raw = request.form.get("status")
    if raw is None and request.is_json:
        payload = request.get_json(silent=True) or {}
        raw = payload.get("status")
    status_val = _norm_status_flag(raw)
    if not status_val:
        return jsonify({"success": False, "error": "status must be 'OPEN' or 'CLOSED'"}), 400

    res = campus_services_col.update_one(
        {"_id": _id},
        {"$set": {"status": status_val, "updated_at": datetime.utcnow()}},
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    return jsonify({"success": True, "service_id": str(_id), "status": status_val})


@admin_campus_services_bp.route("/admin/campus-services/<service_id>/availability", methods=["POST"])
def set_campus_service_availability(service_id):
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        _id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    raw = request.form.get("availability")
    if raw is None and request.is_json:
        payload = request.get_json(silent=True) or {}
        raw = payload.get("availability")
    avail_val = _norm_availability_flag(raw)
    if not avail_val:
        return jsonify({"success": False, "error": "availability must be 'AVAILABLE' or 'OUT_OF_STOCK'"}), 400

    res = campus_services_col.update_one(
        {"_id": _id},
        {"$set": {"availability": avail_val, "updated_at": datetime.utcnow()}},
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    return jsonify({"success": True, "service_id": str(_id), "availability": avail_val})
