from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId
from datetime import datetime
from ast import literal_eval
import json

from db import campus_db

admin_campus_services_bp = Blueprint("admin_campus_services", __name__)

services_col = campus_db["services"]


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


def _parse_volume_to_mb(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(round(float(v)))
    txt = str(v).strip()
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


def _format_volume_gb(vol_mb):
    if vol_mb is None:
        return "-"
    try:
        vol_mb = float(vol_mb)
    except Exception:
        return "-"
    gb = vol_mb / 1000.0
    if abs(gb - round(gb)) < 1e-9:
        return f"{int(round(gb))}GB"
    return f"{gb:.2f}GB"


def _value_text(value_raw):
    vol_mb = _parse_volume_to_mb(value_raw)
    return _format_volume_gb(vol_mb)


def _parse_offers(req, prefix: str):
    amount_key = f"{prefix}_amount[]"
    value_key = f"{prefix}_value[]"
    amounts = req.form.getlist(amount_key)
    values = req.form.getlist(value_key)
    n = max(len(amounts), len(values))
    offers = []
    for i in range(n):
        amount = (amounts[i] if i < len(amounts) else "").strip()
        value_txt = (values[i] if i < len(values) else "").strip()
        if not amount and not value_txt:
            continue
        offers.append({
            "amount": _to_float(amount),
            "value": value_txt,
            "profit": None,
        })
    return offers


@admin_campus_services_bp.route("/admin/campus-services", methods=["GET"])
def campus_services_prices():
    if not _require_admin():
        return redirect(url_for("login.login"))

    services = list(services_col.find({}, {
        "name": 1,
        "offers": 1,
        "store_offers": 1,
        "provider": 1,
        "network": 1,
        "updated_at": 1,
    }).sort([("name", 1)]))

    for s in services:
        s["_id_str"] = str(s["_id"])
        for key in ("offers", "store_offers"):
            if isinstance(s.get(key), list):
                for of in s[key]:
                    of["value_text"] = _value_text(of.get("value"))

    return render_template("admin_campus_services.html", services=services)


@admin_campus_services_bp.route("/admin/campus-services/<service_id>/prices", methods=["POST"])
def update_campus_service_prices(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        _id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_campus_services.campus_services_prices"))

    svc = services_col.find_one({"_id": _id})
    if not svc:
        flash("Service not found.", "danger")
        return redirect(url_for("admin_campus_services.campus_services_prices"))

    offers = _parse_offers(request, "offers")
    store_offers = _parse_offers(request, "store_offers")

    services_col.update_one(
        {"_id": _id},
        {"$set": {
            "offers": offers,
            "store_offers": store_offers,
            "updated_at": datetime.utcnow(),
        }},
    )

    flash("Service prices updated.", "success")
    return redirect(url_for("admin_campus_services.campus_services_prices"))
