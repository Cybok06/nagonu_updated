from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify, Request
from flask import current_app
from db import db
from datetime import datetime
from bson import ObjectId
from werkzeug.utils import secure_filename
import os
import json
import uuid
import re
from ast import literal_eval
from collections import defaultdict

admin_services_bp = Blueprint("admin_services", __name__)
services_col = db["services"]
users_col = db["users"]                  # customers live here
service_profits_col = db["service_profits"]  # {service_id, customer_id, profit_percent, created_at, updated_at}

# ---- File upload config ----
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")  # match app.py

def _ensure_upload_folder():
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

# ---- Auth helper ----
def _require_admin():
    return session.get("role") == "admin"

# ---- Type guard (optional field from UI) ----
_ALLOWED_TYPES = {"API", "customer"}
def _norm_type(t: str | None) -> str | None:
    if not t:
        return None
    t = t.strip()
    return t if t in _ALLOWED_TYPES else None

# ---- Coercers ----
def _to_float(s):
    try:
        return float(s)
    except Exception:
        return None

def _to_int(s):
    try:
        # allow "1,000"
        if isinstance(s, str):
            s = s.replace(",", "").strip()
        return int(float(s))
    except Exception:
        return None

# ---- Volume parsing/formatting ----
_MB_RE = re.compile(r"^\s*([\d,]+(?:\.\d+)?)\s*MB\s*$", re.I)
_GB_RE = re.compile(r"^\s*([\d,]+(?:\.\d+)?)\s*G(?:B|IG)?\s*$", re.I)
_INT_RE = re.compile(r"^\s*[\d,]+\s*$")

def _parse_volume_to_mb(v):
    """
    Accept '1GB', '500 MB', '1024', '1,024MB', 1000 -> returns int MB
    Returns None if unknown.
    """
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
        # raw MB integer string
        return int(txt.replace(",", ""))

    # try JSON/dict with 'volume'
    try:
        if txt.startswith("{") and txt.endswith("}"):
            as_json = json.loads(txt)
            if isinstance(as_json, dict) and "volume" in as_json:
                return _to_int(as_json["volume"])
    except Exception:
        pass

    # try pythonic dict "{'id':5,'volume':1000}"
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
    """
    Try to extract 'id' from:
    - JSON: {"id":5,"volume":1000}
    - python dict string: "{'id': 5, 'volume': 1000}"
    - plain int string "5"
    """
    if value_raw is None:
        return None
    if isinstance(value_raw, (int, float)):
        return _to_int(value_raw)

    txt = str(value_raw).strip()
    # int-looking
    if _INT_RE.match(txt):
        return _to_int(txt)

    # try JSON dict
    try:
        if txt.startswith("{") and txt.endswith("}"):
            as_json = json.loads(txt)
            if isinstance(as_json, dict) and "id" in as_json:
                return _to_int(as_json["id"])
    except Exception:
        pass

    # try pythonic dict
    try:
        d = literal_eval(txt)
        if isinstance(d, dict) and "id" in d:
            return _to_int(d["id"])
    except Exception:
        pass

    return None

def _to_mtn_value_string(pkg_id: int | None, volume_mb: int | None, fallback_value_raw: str | None):
    """
    Always produce MTN-style string: "{'id': <int>, 'volume': <int>}"
    If id is None, assign later in _parse_offers (auto-increment).
    If volume is None but fallback has a parseable volume, use that.
    """
    if volume_mb is None:
        volume_mb = _parse_volume_to_mb(fallback_value_raw)
    if pkg_id is None:
        # will be set by caller; here just leave None for now
        pkg_id = None
    # Safety: ensure ints (or None)
    volume_mb = _to_int(volume_mb) if volume_mb is not None else None
    pkg_id = _to_int(pkg_id) if pkg_id is not None else None
    # Build string with single quotes like MTN doc
    if pkg_id is None or volume_mb is None:
        # Leave incompletely parsed content as-is (rare), but still attempt volume text for UI
        return None
    return f"{{'id': {pkg_id}, 'volume': {volume_mb}}}"

def _compute_value_text_from_mtn_string(value_str: str):
    """
    For UI: turn "{'id': 5, 'volume': 1000}" -> "1GB (Pkg 5)"
    """
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
        # If it isn't dict-like, show a best-effort human label
        vol_mb = _parse_volume_to_mb(value_str)
        if vol_mb is not None:
            return _format_volume(vol_mb)
        return value_str or "-"

# ---- Offers parser (NOW NORMALIZES TO MTN STYLE) ----
def _parse_offers(req: Request):
    """
    Supports either structured fields OR a single free-text field.
    Structured (preferred):
      offers_amount[]  -> float
      offers_profit[]  -> float (optional)
      offers_pkg_id[]  -> int
      offers_volume[]  -> int MB or label like "1GB"
    Legacy/freetext:
      offers_value[]   -> "1GB" / "500MB" / "1000" / JSON/dict / already MTN style
    """
    amounts = req.form.getlist("offers_amount[]")
    profits = req.form.getlist("offers_profit[]")
    pkg_ids = req.form.getlist("offers_pkg_id[]")  # optional
    volumes = req.form.getlist("offers_volume[]")  # optional (MB or label)
    values_freetext = req.form.getlist("offers_value[]")  # optional

    n = max(len(amounts), len(profits), len(pkg_ids), len(volumes), len(values_freetext))
    offers = []
    auto_id_seed = 1  # assign ids for rows missing pkg_id

    # find max existing provided id in this request to continue numbering after it
    provided_ids = []
    for x in pkg_ids:
        pid = _to_int(x)
        if pid is not None:
            provided_ids.append(pid)
    if provided_ids:
        auto_id_seed = max(provided_ids) + 1

    for i in range(n):
        amount = (amounts[i] if i < len(amounts) else "").strip()
        profit = (profits[i] if i < len(profits) else "").strip()
        pkg_id_raw = (pkg_ids[i] if i < len(pkg_ids) else "").strip()
        volume_raw = (volumes[i] if i < len(volumes) else "").strip()
        value_txt = (values_freetext[i] if i < len(values_freetext) else "").strip()

        # skip empty row
        if not amount and not profit and not pkg_id_raw and not volume_raw and not value_txt:
            continue

        base_amount = _to_float(amount)
        per_offer_profit = _to_float(profit)

        # Resolve package id: prefer structured pkg_id[], fallback to parsing freetext
        pkg_id = _to_int(pkg_id_raw) if pkg_id_raw else None
        if pkg_id is None:
            pkg_id = _extract_pkg_id(value_txt)

        # Resolve volume MB: prefer structured volumes[], fallback to parsing freetext
        vol_mb = _parse_volume_to_mb(volume_raw) if volume_raw else None
        if vol_mb is None:
            vol_mb = _parse_volume_to_mb(value_txt)

        # If still missing id, auto-assign
        if pkg_id is None:
            pkg_id = auto_id_seed
            auto_id_seed += 1

        # Build MTN-style value string
        value_str = _to_mtn_value_string(pkg_id, vol_mb, value_txt)
        # As a last resort (shouldn't happen often), if still None, try to salvage by forcing integers
        if value_str is None and (pkg_id is not None and vol_mb is not None):
            value_str = f"{{'id': {int(pkg_id)}, 'volume': {int(vol_mb)}}}"

        offers.append({
            "amount": base_amount,
            "value": value_str,      # ALWAYS MTN STYLE
            "profit": per_offer_profit
        })

    return offers

# =======================
#  PROFIT LOOKUP/QUOTES
# =======================
def _get_service_default_profit(service_doc) -> float:
    p = service_doc.get("default_profit_percent")
    try:
        return float(p)
    except Exception:
        return 0.0

def _get_customer_profit_percent(service_id: ObjectId, customer_id: ObjectId):
    sp = service_profits_col.find_one({"service_id": service_id, "customer_id": customer_id})
    if not sp:
        return None
    try:
        return float(sp.get("profit_percent"))
    except Exception:
        return None

def _effective_profit_percent(service_doc, customer_id: ObjectId | None) -> float:
    if customer_id:
        cp = _get_customer_profit_percent(service_doc["_id"], customer_id)
        if cp is not None:
            return cp
    return _get_service_default_profit(service_doc)

def _quote_total(amount: float, profit_percent: float) -> dict:
    if amount is None:
        return {"amount": None, "profit": None, "total": None}
    pp = max(0.0, float(profit_percent or 0.0))
    profit_amt = round(amount * (pp / 100.0), 2)
    total = round(amount + profit_amt, 2)
    return {"amount": round(amount, 2), "profit": profit_amt, "total": total, "profit_percent": pp}

# =======================
#   INTERNAL HELPERS
# =======================
def _display_name(user_doc):
    """Best-effort display name for a customer."""
    nm = (user_doc.get("business_name") or "").strip()
    if nm:
        return nm
    fn = (user_doc.get("first_name") or "").strip()
    ln = (user_doc.get("last_name") or "").strip()
    full = (" ".join([fn, ln])).strip()
    return full or (user_doc.get("username") or user_doc.get("phone") or str(user_doc.get("_id")))

# =======================
#      PAGE ROUTES
# =======================
@admin_services_bp.route("/admin/services", methods=["GET"])
def manage_services():
    if not _require_admin():
        return redirect(url_for("login.login"))

    # Fetch services (projection only what UI needs)
    services = list(services_col.find({}, {
        "name": 1, "image_url": 1, "offers": 1, "default_profit_percent": 1, "created_at": 1, "type": 1
    }).sort([("_id", -1)]))

    # Compute value_text and normalize
    for s in services:
        s["_id_str"] = str(s["_id"])
        s["default_profit_percent"] = _get_service_default_profit(s)
        if isinstance(s.get("offers"), list):
            for of in s["offers"]:
                v = of.get("value")
                if isinstance(v, str):
                    of["value_text"] = _compute_value_text_from_mtn_string(v)
                else:
                    of["value_text"] = "-"

    # ---- Batched load of all customer-profit overrides for the listed services
    service_ids = [s["_id"] for s in services]
    overrides_by_service = defaultdict(list)
    customer_id_set = set()

    if service_ids:
        for ov in service_profits_col.find({"service_id": {"$in": service_ids}}):
            overrides_by_service[ov["service_id"]].append(ov)
            customer_id_set.add(ov["customer_id"])

    # Load names for all unique customers referenced by overrides
    names_map = {}
    if customer_id_set:
        for u in users_col.find({"_id": {"$in": list(customer_id_set)}},
                                {"first_name": 1, "last_name": 1, "username": 1, "phone": 1, "business_name": 1}):
            names_map[u["_id"]] = _display_name(u)

    # Attach a uniform structure the template can read: svc.customer_profits
    for s in services:
        ov_list = overrides_by_service.get(s["_id"], [])
        s["customer_profits"] = [{
            "customer_id": str(ov["customer_id"]),
            "customer_name": names_map.get(ov["customer_id"], str(ov["customer_id"])),
            "profit_percent": float(ov.get("profit_percent") or 0),
            "updated_at": ov.get("updated_at")
        } for ov in sorted(ov_list, key=lambda x: (names_map.get(x.get("customer_id"), ""), str(x.get("customer_id"))))]

    # customers for “Set Customer Profit” modal (role=customer) — for the add/update section
    users_cursor = users_col.find(
        {"role": "customer"},
        {"first_name": 1, "last_name": 1, "username": 1, "phone": 1, "business_name": 1}
    ).sort([("first_name", 1), ("last_name", 1)])

    customers = [{"_id": str(u["_id"]), "name": _display_name(u)} for u in users_cursor]

    return render_template("admin_services.html", services=services, customers=customers)

@admin_services_bp.route("/admin/services/create", methods=["POST"])
def create_service():
    if not _require_admin():
        return redirect(url_for("login.login"))

    service_name = (request.form.get("service_name") or "").strip()
    image_url = (request.form.get("image_url") or "").strip()
    default_profit_percent = _to_float(request.form.get("default_profit_percent"))
    service_type = _norm_type(request.form.get("service_type"))  # optional select/radio in UI

    # Optional (to mirror MTN doc):
    network = (request.form.get("network") or "").strip() or None
    network_id = _to_int(request.form.get("network_id"))

    if not service_name:
        flash("Service name is required.", "danger")
        return redirect(url_for("admin_services.manage_services"))
    if not image_url:
        flash("Please upload/select an image for the service.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    offers = _parse_offers(request)

    doc = {
        "name": service_name,
        "image_url": image_url,
        "offers": offers,
        "default_profit_percent": default_profit_percent if default_profit_percent is not None else 0.0,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    if network:
        doc["network"] = network
    if network_id is not None:
        doc["network_id"] = network_id

    services_col.insert_one(doc)
    flash("Service added successfully.", "success")
    return redirect(url_for("admin_services.manage_services"))

@admin_services_bp.route("/admin/services/<service_id>/update", methods=["POST"])
def update_service(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        _id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    service = services_col.find_one({"_id": _id})
    if not service:
        flash("Service not found.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    service_name = (request.form.get("service_name") or "").strip()
    image_url = (request.form.get("image_url") or "").strip()
    default_profit_percent = _to_float(request.form.get("default_profit_percent"))
    service_type = _norm_type(request.form.get("service_type"))

    # Optional (align with MTN doc):
    network = (request.form.get("network") or "").strip() or None
    network_id = _to_int(request.form.get("network_id"))

    if not service_name:
        flash("Service name is required.", "danger")
        return redirect(url_for("admin_services.manage_services"))
    if not image_url:
        flash("Please upload/select an image for the service.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    offers = _parse_offers(request)

    update_doc = {
        "name": service_name,
        "image_url": image_url,
        "offers": offers,
        "updated_at": datetime.utcnow()
    }
    if default_profit_percent is not None:
        update_doc["default_profit_percent"] = default_profit_percent
    if service_type:
        update_doc["type"] = service_type
    if network is not None:
        update_doc["network"] = network
    if network_id is not None:
        update_doc["network_id"] = network_id

    services_col.update_one({"_id": _id}, {"$set": update_doc})
    flash("Service updated successfully.", "success")
    return redirect(url_for("admin_services.manage_services"))

@admin_services_bp.route("/admin/services/<service_id>/delete", methods=["POST"])
def delete_service(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        _id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    svc = services_col.find_one({"_id": _id})
    res = services_col.delete_one({"_id": _id})

    if res.deleted_count:
        # cleanup uploaded image (best‑effort)
        try:
            if svc and isinstance(svc.get("image_url"), str) and svc["image_url"].startswith("/uploads/"):
                _ensure_upload_folder()
                fname = svc["image_url"].replace("/uploads/", "")
                fpath = os.path.join(UPLOAD_FOLDER, fname)
                if os.path.isfile(fpath):
                    os.remove(fpath)
        except Exception:
            pass
        service_profits_col.delete_many({"service_id": _id})
        flash("Service deleted.", "info")
    else:
        flash("Service not found or already deleted.", "warning")

    return redirect(url_for("admin_services.manage_services"))

# =======================
#   FILE UPLOAD API
# =======================
@admin_services_bp.route("/upload_service_image", methods=["POST"])
def upload_service_image():
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if "image" not in request.files:
        return jsonify({"success": False, "error": "No file part 'image'"}), 400

    file = request.files["image"]
    if not file or file.filename.strip() == "":
        return jsonify({"success": False, "error": "No selected file"}), 400

    if not _allowed_file(file.filename):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    _ensure_upload_folder()

    # unique, safe filename
    base, ext = os.path.splitext(secure_filename(file.filename))
    filename = f"{base}_{uuid.uuid4().hex[:8]}{ext.lower()}"
    target_path = os.path.join(UPLOAD_FOLDER, filename)

    file.save(target_path)
    file_url = f"/uploads/{filename}"
    return jsonify({"success": True, "url": file_url}), 200

# =======================
#   PROFIT ENDPOINTS
# =======================
@admin_services_bp.route("/admin/services/<service_id>/profit/default", methods=["POST"])
def set_service_default_profit(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        _id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    p = _to_float(request.form.get("default_profit_percent"))
    if p is None or p < 0:
        flash("Default profit percentage must be a non-negative number.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    services_col.update_one({"_id": _id}, {"$set": {
        "default_profit_percent": float(p),
        "updated_at": datetime.utcnow()
    }})
    flash("Default profit percentage updated.", "success")
    return redirect(url_for("admin_services.manage_services"))

@admin_services_bp.route("/admin/services/<service_id>/profit/customer", methods=["POST"])
def set_customer_profit_for_service(service_id):
    """Upsert: create or update a customer-specific override for a service."""
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    customer_id_raw = (request.form.get("customer_id") or "").strip()
    try:
        c_id = ObjectId(customer_id_raw)
    except Exception:
        flash("Invalid customer id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    # validate customer exists in users
    customer = users_col.find_one({"_id": c_id, "role": "customer"})
    if not customer:
        flash("Customer not found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    p = _to_float(request.form.get("profit_percent"))
    if p is None or p < 0:
        flash("Profit percentage must be a non-negative number.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    now = datetime.utcnow()
    service_profits_col.update_one(
        {"service_id": s_id, "customer_id": c_id},
        {"$set": {"profit_percent": float(p), "updated_at": now},
         "$setOnInsert": {"created_at": now}},
        upsert=True
    )
    flash("Customer profit for service updated.", "success")
    return redirect(url_for("admin_services.manage_services"))

@admin_services_bp.route("/admin/services/<service_id>/profit/customer/<customer_id>/delete", methods=["POST"])
def delete_customer_profit_for_service(service_id, customer_id):
    """Remove a single customer override for a service."""
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
        c_id = ObjectId(customer_id)
    except Exception:
        flash("Invalid id(s).", "danger")
        return redirect(url_for("admin_services.manage_services"))

    res = service_profits_col.delete_one({"service_id": s_id, "customer_id": c_id})
    if res.deleted_count:
        flash("Customer profit override removed.", "info")
    else:
        flash("Override not found.", "warning")
    return redirect(url_for("admin_services.manage_services"))

# ------ JSON lookups ------
@admin_services_bp.route("/api/services/<service_id>/profit", methods=["GET"])
def get_effective_profit(service_id):
    if not _require_admin():  # relax if you want to expose for storefront
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    try:
        s_id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    service = services_col.find_one({"_id": s_id})
    if not service:
        return jsonify({"success": False, "error": "Service not found"}), 404

    customer_id = request.args.get("customer_id")
    c_id = None
    if customer_id:
        try:
            c_id = ObjectId(customer_id)
        except Exception:
            return jsonify({"success": False, "error": "Invalid customer id"}), 400

    eff = _effective_profit_percent(service, c_id)
    return jsonify({"success": True, "service_id": str(s_id), "customer_id": str(c_id) if c_id else None, "profit_percent": eff})

@admin_services_bp.route("/api/pricing/quote", methods=["GET"])
def quote_price():
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    service_id = request.args.get("service_id")
    amount = _to_float(request.args.get("amount"))
    customer_id = request.args.get("customer_id")

    if not service_id or amount is None:
        return jsonify({"success": False, "error": "service_id and amount are required"}), 400

    try:
        s_id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    service = services_col.find_one({"_id": s_id})
    if not service:
        return jsonify({"success": False, "error": "Service not found"}), 404

    c_id = None
    if customer_id:
        try:
            c_id = ObjectId(customer_id)
        except Exception:
            return jsonify({"success": False, "error": "Invalid customer id"}), 400

    eff = _effective_profit_percent(service, c_id)
    q = _quote_total(amount, eff)
    return jsonify({"success": True, "data": q})
