from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify, Request
from db import db, campus_db
from datetime import datetime
from bson import ObjectId
from werkzeug.utils import secure_filename
import os
import json
import uuid
import re
from ast import literal_eval

admin_services_bp = Blueprint("admin_services", __name__)
services_col = db["services"]
campus_services_col = campus_db["services"]
users_col = db["users"]                     # customers live here
service_profits_col = db["service_profits"] # {service_id, customer_id, profit_percent, created_at, updated_at}
service_offer_prices_col = db["service_offer_prices"]  # {service_id, customer_id, offer_id, customer_price, created_at, updated_at}

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
MTN_NORMAL_SERVICE_ID = "68b8b6a7eb0ced45901c68d2"

def _ensure_upload_folder():
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def _require_admin():
    return session.get("role") == "admin"

_ALLOWED_TYPES = {"API", "OFF"}
def _norm_type(t: str | None) -> str | None:
    if not t:
        return None
    t = t.strip().upper()
    return t if t in _ALLOWED_TYPES else None

def _to_float(s):
    try:
        return float(s)
    except Exception:
        return None

def _stage_price_from_offer(offer: dict, stage: str) -> float | None:
    prices = offer.get("stage_prices")
    if not isinstance(prices, dict):
        return None

    if stage in prices:
        return _to_float(prices.get(stage))

    aliases = {
        "normal_agent": ("normal", "normal_agent", "normal agent"),
        "elite_agent": ("elite", "elite_agent", "elite agent"),
        "premium": ("premium", "premium_agent"),
    }.get(stage, ())

    lowered = {str(k).strip().lower(): v for k, v in prices.items()}
    for a in aliases:
        if a in lowered:
            return _to_float(lowered.get(a))
    return None

def _to_int(s):
    try:
        if isinstance(s, str):
            s = s.replace(",", "").strip()
        return int(float(s))
    except Exception:
        return None

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

# ===========================
# OFFERS PARSER (WITH PREFIX)
# ===========================
def _parse_offers(req: Request, prefix: str = "offers"):
    """
    prefix='offers'         -> uses offers_amount[], offers_value[], offers_customer_price[]
    prefix='store_offers'   -> uses store_offers_amount[], store_offers_value[], store_offers_customer_price[]
    """
    amount_key = f"{prefix}_amount[]"
    value_key  = f"{prefix}_value[]"
    price_key  = f"{prefix}_customer_price[]"

    amounts = req.form.getlist(amount_key)
    values_freetext = req.form.getlist(value_key)
    customer_prices = req.form.getlist(price_key)

    n = max(len(amounts), len(values_freetext))
    offers = []
    auto_id_seed = 1

    for i in range(n):
        amount = (amounts[i] if i < len(amounts) else "").strip()
        value_txt = (values_freetext[i] if i < len(values_freetext) else "").strip()
        customer_price_raw = (customer_prices[i] if i < len(customer_prices) else "").strip()

        if not amount and not value_txt:
            continue

        base_amount = _to_float(amount)
        customer_price = _to_float(customer_price_raw)
        if customer_price is None and base_amount is not None:
            # Default customer price falls back to base amount if not provided
            customer_price = base_amount

        pkg_id = _extract_pkg_id(value_txt)
        vol_mb = _parse_volume_to_mb(value_txt)

        if pkg_id is None:
            pkg_id = auto_id_seed
            auto_id_seed += 1

        value_str = _to_mtn_value_string(pkg_id, vol_mb, value_txt)
        if value_str is None and (pkg_id is not None and vol_mb is not None):
            value_str = f"{{'id': {int(pkg_id)}, 'volume': {int(vol_mb)}}}"

        offers.append({
            "amount": base_amount,
            "value": value_str,
            "profit": None,
            "customer_price": customer_price
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

def _display_name(user_doc):
    nm = (user_doc.get("business_name") or "").strip()
    if nm:
        return nm
    fn = (user_doc.get("first_name") or "").strip()
    ln = (user_doc.get("last_name") or "").strip()
    full = (" ".join([fn, ln])).strip()
    return full or (user_doc.get("username") or user_doc.get("phone") or str(user_doc.get("_id")))

def _is_mtn_normal_service(service: dict | None, service_id: ObjectId | None = None) -> bool:
    if service and (service.get("name") or "").strip().lower() == "mtn normal":
        return True
    if service_id and str(service_id) == MTN_NORMAL_SERVICE_ID:
        return True
    if service and service.get("_id") and str(service.get("_id")) == MTN_NORMAL_SERVICE_ID:
        return True
    return False

def _is_mtn_express_service(service: dict | None) -> bool:
    if not service:
        return False
    for key in ("name", "service_network", "network"):
        val = (service.get(key) or "").strip().lower()
        if val == "mtn express":
            return True
    return False

def _is_telecel_service(service: dict | None) -> bool:
    if not service:
        return False
    name = " ".join(
        str(x)
        for x in (
            service.get("name"),
            service.get("service_network"),
            service.get("network"),
        )
        if x
    ).lower()
    return ("telecel" in name) or ("vodafone" in name)

# =======================
#      PAGE ROUTES
# =======================
@admin_services_bp.route("/admin/services", methods=["GET"])
def manage_services():
    if not _require_admin():
        return redirect(url_for("login.login"))

    services = list(services_col.find({}, {
        "name": 1,
        "image_url": 1,
        "offers": 1,
        "store_offers": 1,   # NEW
        "created_at": 1,
        "type": 1,
        "status": 1,
        "availability": 1,
        "provider": 1,
    }).sort([("_id", -1)]))

    for s in services:
        s["_id_str"] = str(s["_id"])
        # compute value_text for default + store
        for key in ("offers", "store_offers"):
            if isinstance(s.get(key), list):
                for idx, of in enumerate(s[key], start=1):
                    v = of.get("value")
                    of["value_text"] = _compute_value_text_from_mtn_string(v) if isinstance(v, str) else "-"
                    of["offer_id"] = _extract_pkg_id(v) or idx

    for s in services:
        for of in (s.get("offers") or []):
            of["stage_price_normal"] = _stage_price_from_offer(of, "normal_agent")
            of["stage_price_elite"] = _stage_price_from_offer(of, "elite_agent")
            of["stage_price_premium"] = _stage_price_from_offer(of, "premium")

        s["offer_options"] = [
            {"offer_id": str(of.get("offer_id")), "label": of.get("value_text") or of.get("value") or "-"}
            for of in (s.get("offers") or [])
        ]

    return render_template("admin_services.html", services=services)

@admin_services_bp.route("/admin/services/create", methods=["POST"])
def create_service():
    if not _require_admin():
        return redirect(url_for("login.login"))

    service_name = (request.form.get("service_name") or "").strip()
    image_url = (request.form.get("image_url") or "").strip()
    service_type = _norm_type(request.form.get("service_type")) or "API"

    if not service_name:
        flash("Service name is required.", "danger")
        return redirect(url_for("admin_services.manage_services"))
    if not image_url:
        flash("Please upload/select an image for the service.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    offers = _parse_offers(request, "offers")

    # NEW: optionally copy default to store on create
    copy_default_to_store = (request.form.get("copy_default_to_store") or "").strip()
    store_offers = offers if copy_default_to_store else []

    doc = {
        "name": service_name,
        "image_url": image_url,
        "offers": offers,
        "store_offers": store_offers,  # NEW
        "type": service_type,
        "status": "OPEN",
        "availability": "AVAILABLE",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }

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
    service_type = _norm_type(request.form.get("service_type"))

    if not service_name:
        flash("Service name is required.", "danger")
        return redirect(url_for("admin_services.manage_services"))
    if not image_url:
        flash("Please upload/select an image for the service.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    # NEW: parse both sets
    offers = _parse_offers(request, "offers")
    store_offers = _parse_offers(request, "store_offers")

    update_doc = {
        "name": service_name,
        "image_url": image_url,
        "offers": offers,
        "store_offers": store_offers,  # NEW
        "updated_at": datetime.utcnow()
    }
    if service_type:
        update_doc["type"] = service_type

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

    base, ext = os.path.splitext(secure_filename(file.filename))
    filename = f"{base}_{uuid.uuid4().hex[:8]}{ext.lower()}"
    target_path = os.path.join(UPLOAD_FOLDER, filename)

    file.save(target_path)
    file_url = f"/uploads/{filename}"
    return jsonify({"success": True, "url": file_url}), 200

# =======================
#   CUSTOMER PRICE OVERRIDES
# =======================
@admin_services_bp.route("/admin/services/<service_id>/price/customer", methods=["POST"])
def set_customer_price_for_service(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    customer_ids = request.form.getlist("customer_id[]")
    if not customer_ids:
        single = (request.form.get("customer_id") or "").strip()
        if single:
            customer_ids = [single]

    cust_objs: list[ObjectId] = []
    for cid in customer_ids:
        try:
            cust_objs.append(ObjectId(str(cid)))
        except Exception:
            continue
    if not cust_objs:
        flash("Invalid customer id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    valid_customers = list(users_col.find({"_id": {"$in": cust_objs}, "role": "customer"}, {"_id": 1}))
    if not valid_customers:
        flash("Customer not found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    offer_id_raw = (request.form.get("offer_id") or "").strip()
    try:
        offer_id = int(float(offer_id_raw))
    except Exception:
        flash("Invalid offer id.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    price = _to_float(request.form.get("customer_price"))
    if price is None or price < 0:
        flash("Customer price must be a non-negative number.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    now = datetime.utcnow()
    updated = 0
    for c in valid_customers:
        c_id = c["_id"]
        service_offer_prices_col.update_one(
            {"service_id": s_id, "customer_id": c_id, "offer_id": offer_id},
            {"$set": {"customer_price": float(price), "updated_at": now},
             "$setOnInsert": {"created_at": now}},
            upsert=True
        )
        updated += 1

    flash(f"Customer price override updated ({updated} customer(s)).", "success")
    return redirect(url_for("admin_services.manage_services"))

@admin_services_bp.route("/admin/services/<service_id>/price/customer/<customer_id>/<offer_id>/delete", methods=["POST"])
def delete_customer_price_for_service(service_id, customer_id, offer_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
        c_id = ObjectId(customer_id)
        offer_id_i = int(float(offer_id))
    except Exception:
        flash("Invalid id(s).", "danger")
        return redirect(url_for("admin_services.manage_services"))

    res = service_offer_prices_col.delete_one({"service_id": s_id, "customer_id": c_id, "offer_id": offer_id_i})
    if res.deleted_count:
        flash("Customer price override removed.", "info")
    else:
        flash("Override not found.", "warning")
    return redirect(url_for("admin_services.manage_services"))


@admin_services_bp.route("/admin/services/<service_id>/price/default/bulk", methods=["POST"])
def set_default_prices_bulk(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    svc = services_col.find_one({"_id": s_id})
    if not svc:
        flash("Service not found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    offer_ids = request.form.getlist("offer_id[]")
    prices = request.form.getlist("customer_price[]")
    if not offer_ids or not prices:
        flash("No prices provided.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    offers = svc.get("offers") or []
    offers_by_id = {}
    for idx, of in enumerate(offers, start=1):
        vid = _extract_pkg_id(of.get("value")) or idx
        offers_by_id[str(vid)] = of

    updated = 0
    for i in range(min(len(offer_ids), len(prices))):
        oid_raw = (offer_ids[i] or "").strip()
        price_raw = (prices[i] or "").strip()
        if not oid_raw:
            continue
        if price_raw == "":
            continue
        price = _to_float(price_raw)
        if price is None or price < 0:
            continue
        of = offers_by_id.get(oid_raw)
        if not of:
            continue
        of["customer_price"] = float(price)
        updated += 1

    services_col.update_one(
        {"_id": s_id},
        {"$set": {"offers": offers, "updated_at": datetime.utcnow()}},
    )
    flash(f"Default prices updated ({updated} offers).", "success")
    return redirect(url_for("admin_services.manage_services"))


@admin_services_bp.route("/admin/services/<service_id>/price/customer/bulk", methods=["POST"])
def set_customer_prices_bulk(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    customer_ids = request.form.getlist("customer_id[]")
    if not customer_ids:
        single = (request.form.get("customer_id") or "").strip()
        if single:
            customer_ids = [single]

    cust_objs: list[ObjectId] = []
    for cid in customer_ids:
        try:
            cust_objs.append(ObjectId(str(cid)))
        except Exception:
            continue
    if not cust_objs:
        flash("Invalid customer id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    valid_customers = list(users_col.find({"_id": {"$in": cust_objs}, "role": "customer"}, {"_id": 1}))
    if not valid_customers:
        flash("Customer not found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    offer_ids = request.form.getlist("offer_id[]")
    prices = request.form.getlist("customer_price[]")
    if not offer_ids:
        flash("No offers found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    now = datetime.utcnow()
    upserts = 0
    deletes = 0
    for c in valid_customers:
        c_id = c["_id"]
        for i in range(len(offer_ids)):
            oid_raw = (offer_ids[i] or "").strip()
            price_raw = (prices[i] or "").strip()
            if not oid_raw:
                continue
            try:
                offer_id = int(float(oid_raw))
            except Exception:
                continue

            if price_raw == "":
                res = service_offer_prices_col.delete_one(
                    {"service_id": s_id, "customer_id": c_id, "offer_id": offer_id}
                )
                if res.deleted_count:
                    deletes += 1
                continue

            price = _to_float(price_raw)
            if price is None or price < 0:
                continue

            service_offer_prices_col.update_one(
                {"service_id": s_id, "customer_id": c_id, "offer_id": offer_id},
                {"$set": {"customer_price": float(price), "updated_at": now},
                 "$setOnInsert": {"created_at": now}},
                upsert=True
            )
            upserts += 1

    flash(
        f"Customer prices updated for {len(valid_customers)} customer(s) "
        f"(saved {upserts}, removed {deletes}).",
        "success"
    )
    return redirect(url_for("admin_services.manage_services"))


@admin_services_bp.route("/admin/services/<service_id>/price/stage/bulk", methods=["POST"])
def set_stage_prices_bulk(service_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        s_id = ObjectId(service_id)
    except Exception:
        flash("Invalid service id.", "danger")
        return redirect(url_for("admin_services.manage_services"))

    svc = services_col.find_one({"_id": s_id})
    if not svc:
        flash("Service not found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    offer_ids = request.form.getlist("offer_id[]")
    normal_prices = request.form.getlist("normal_price[]")
    elite_prices = request.form.getlist("elite_price[]")
    premium_prices = request.form.getlist("premium_price[]")
    if not offer_ids:
        flash("No offers found.", "warning")
        return redirect(url_for("admin_services.manage_services"))

    offers = svc.get("offers") or []
    offers_by_id = {}
    for idx, of in enumerate(offers, start=1):
        vid = _extract_pkg_id(of.get("value")) or idx
        offers_by_id[str(vid)] = of

    updated = 0
    cleared = 0
    for i in range(len(offer_ids)):
        oid_raw = (offer_ids[i] or "").strip()
        if not oid_raw:
            continue

        of = offers_by_id.get(oid_raw)
        if not of:
            continue

        normal_raw = (normal_prices[i] if i < len(normal_prices) else "").strip()
        elite_raw = (elite_prices[i] if i < len(elite_prices) else "").strip()
        premium_raw = (premium_prices[i] if i < len(premium_prices) else "").strip()

        stage_prices = of.get("stage_prices") if isinstance(of.get("stage_prices"), dict) else {}
        stage_prices = dict(stage_prices)

        for key, raw_val in (
            ("normal_agent", normal_raw),
            ("elite_agent", elite_raw),
            ("premium", premium_raw),
        ):
            if raw_val == "":
                if key in stage_prices:
                    stage_prices.pop(key, None)
                    cleared += 1
                continue
            price = _to_float(raw_val)
            if price is None or price < 0:
                continue
            stage_prices[key] = float(price)
            updated += 1

        if stage_prices:
            of["stage_prices"] = stage_prices
        else:
            of.pop("stage_prices", None)

    services_col.update_one(
        {"_id": s_id},
        {"$set": {"offers": offers, "updated_at": datetime.utcnow()}},
    )
    flash(f"Stage prices updated (saved {updated}, cleared {cleared}).", "success")
    return redirect(url_for("admin_services.manage_services"))

@admin_services_bp.route("/admin/services/<service_id>/type", methods=["POST"])
def set_service_type(service_id):
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

    sync_campus_raw = None
    if request.is_json:
        sync_campus_raw = payload.get("sync_campus")
    if sync_campus_raw is None:
        sync_campus_raw = request.form.get("sync_campus")
    sync_campus = _parse_bool_flag(sync_campus_raw)

    res = services_col.update_one(
        {"_id": _id},
        {"$set": {"type": desired, "updated_at": datetime.utcnow()}}
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    campus_synced = False
    if sync_campus:
        try:
            campus_res = campus_services_col.update_one(
                {"_id": _id},
                {"$set": {"type": desired, "updated_at": datetime.utcnow()}}
            )
            campus_synced = bool(campus_res.matched_count)
        except Exception:
            campus_synced = False

    return jsonify({
        "success": True,
        "service_id": str(_id),
        "type": desired,
        "campus_sync_requested": sync_campus,
        "campus_synced": campus_synced
    })

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

def _parse_bool_flag(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "on"}

@admin_services_bp.route("/admin/services/<service_id>/status", methods=["POST"])
def set_service_status(service_id):
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

    sync_campus_raw = None
    if request.is_json:
        sync_campus_raw = payload.get("sync_campus")
    if sync_campus_raw is None:
        sync_campus_raw = request.form.get("sync_campus")
    sync_campus = _parse_bool_flag(sync_campus_raw)

    res = services_col.update_one(
        {"_id": _id},
        {"$set": {"status": status_val, "updated_at": datetime.utcnow()}}
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    campus_synced = False
    if sync_campus:
        try:
            campus_res = campus_services_col.update_one(
                {"_id": _id},
                {"$set": {"status": status_val, "updated_at": datetime.utcnow()}}
            )
            campus_synced = bool(campus_res.matched_count)
        except Exception:
            campus_synced = False

    return jsonify({
        "success": True,
        "service_id": str(_id),
        "status": status_val,
        "campus_sync_requested": sync_campus,
        "campus_synced": campus_synced
    })

@admin_services_bp.route("/admin/services/<service_id>/availability", methods=["POST"])
def set_service_availability(service_id):
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

    sync_campus_raw = None
    if request.is_json:
        sync_campus_raw = payload.get("sync_campus")
    if sync_campus_raw is None:
        sync_campus_raw = request.form.get("sync_campus")
    sync_campus = _parse_bool_flag(sync_campus_raw)

    res = services_col.update_one(
        {"_id": _id},
        {"$set": {"availability": avail_val, "updated_at": datetime.utcnow()}}
    )
    if not res.matched_count:
        return jsonify({"success": False, "error": "Service not found"}), 404

    campus_synced = False
    if sync_campus:
        try:
            campus_res = campus_services_col.update_one(
                {"_id": _id},
                {"$set": {"availability": avail_val, "updated_at": datetime.utcnow()}}
            )
            campus_synced = bool(campus_res.matched_count)
        except Exception:
            campus_synced = False

    return jsonify({
        "success": True,
        "service_id": str(_id),
        "availability": avail_val,
        "campus_sync_requested": sync_campus,
        "campus_synced": campus_synced
    })


@admin_services_bp.route("/admin/services/<service_id>/provider", methods=["POST"])
def set_service_provider(service_id):
    if not _require_admin():
        return jsonify({"success": False, "error": "Unauthorized"}), 401
    try:
        _id = ObjectId(service_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid service id"}), 400

    payload = request.get_json(silent=True) or {}
    provider = (payload.get("provider") or "").strip().lower()
    if provider not in {"portal02", "dataconnect", "codecraft"}:
        return jsonify(
            {
                "success": False,
                "error": "provider must be 'portal02', 'dataconnect', or 'codecraft'",
            }
        ), 400

    service = services_col.find_one({"_id": _id}, {"name": 1, "type": 1, "service_network": 1, "network": 1})
    if not service:
        return jsonify({"success": False, "error": "Service not found"}), 404

    is_mtn_normal = _is_mtn_normal_service(service, _id)
    is_mtn_express = _is_mtn_express_service(service)
    is_telecel = _is_telecel_service(service)
    if not is_mtn_normal and not is_mtn_express and not is_telecel:
        return jsonify(
            {"success": False, "error": "Provider switch only allowed for MTN NORMAL, MTN EXPRESS, or Telecel"}
        ), 400

    if is_telecel:
        svc_type = (service.get("type") or "").strip().upper()
        if svc_type not in {"ON", "API"}:
            return jsonify(
                {
                    "success": False,
                    "error": "Telecel service type must be 'ON' or 'API' to enable provider routing",
                }
            ), 400

    update_doc = {
        "provider": provider,
        "updated_at": datetime.utcnow(),
        "mtn_normal_use_portal02": True if provider == "portal02" else False,
        "mtn_express_use_portal02": True if provider == "portal02" else False,
    }

    sync_campus = _parse_bool_flag(payload.get("sync_campus"))

    services_col.update_one({"_id": _id}, {"$set": update_doc})

    campus_synced = False
    if sync_campus:
        try:
            campus_res = campus_services_col.update_one({"_id": _id}, {"$set": update_doc})
            campus_synced = bool(campus_res.matched_count)
        except Exception:
            campus_synced = False

    return jsonify({
        "success": True,
        "provider": provider,
        "campus_sync_requested": sync_campus,
        "campus_synced": campus_synced
    })
