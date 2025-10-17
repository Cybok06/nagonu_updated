# admin_afa.py
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from bson import ObjectId
from db import db
from datetime import datetime, timedelta
import re

admin_afa_bp = Blueprint("admin_afa", __name__)

# Collections
afa_col = db["afa_registrations"]
users_col = db["users"]
balances_col = db["balances"]
balance_logs_col = db["balance_logs"]
afa_settings_col = db["afa_settings"]
# Optional: if you want to reflect open/stock into a service doc (e.g., "AFA TALKTIME")
services_col = db["services"]

# Constants / defaults
SETTINGS_ID = "AFA_SETTINGS"
AMOUNT_DEFAULT = 2.00  # only used as a last-resort fallback

# ------------------ Helpers ------------------

def _now():
    return datetime.utcnow()

def _require_admin():
    return session.get("role") == "admin"

def _get_actor():
    actor_id = session.get("user_id")
    actor_name = session.get("username") or session.get("email") or "admin"
    if actor_id:
        try:
            u = users_col.find_one({"_id": ObjectId(actor_id)})
            if u:
                actor_name = (
                    u.get("username")
                    or f"{u.get('first_name','')} {u.get('last_name','')}".strip()
                    or actor_name
                )
        except Exception:
            pass
    return actor_id, actor_name

def _to_objectid(maybe):
    try:
        return ObjectId(maybe)
    except Exception:
        return None

def _get_settings():
    """
    Ensure there is a single settings doc.
    Structure: { _id, price: float, is_open: bool, in_stock: bool, updated_at: datetime }
    """
    s = afa_settings_col.find_one({"_id": SETTINGS_ID})
    if not s:
        s = {
            "_id": SETTINGS_ID,
            "price": AMOUNT_DEFAULT,
            "is_open": True,
            "in_stock": True,
            "updated_at": _now(),
        }
        afa_settings_col.insert_one(s)
    return s

def _save_settings(price: float, is_open: bool, in_stock: bool):
    doc = {
        "price": float(price),
        "is_open": bool(is_open),
        "in_stock": bool(in_stock),
        "updated_at": _now(),
    }
    afa_settings_col.update_one({"_id": SETTINGS_ID}, {"$set": doc}, upsert=True)
    return _get_settings()  # return hydrated (with _id)

# ------------------ PAGE ------------------

@admin_afa_bp.route("/admin/afa")
def admin_afa_page():
    if not _require_admin():
        return redirect(url_for("login.login"))
    return render_template("admin_afa.html")

# ------------------ SETTINGS API ------------------

@admin_afa_bp.route("/admin/api/afa/settings", methods=["GET"])
def admin_afa_get_settings():
    if not _require_admin():
        return jsonify(success=False, error="Unauthorized"), 401
    s = _get_settings()
    return jsonify(
        success=True,
        data={
            "price": float(s.get("price", AMOUNT_DEFAULT)),
            "is_open": bool(s.get("is_open", True)),
            "in_stock": bool(s.get("in_stock", True)),
            "updated_at": s.get("updated_at").strftime("%d %b %Y, %I:%M %p")
            if s.get("updated_at")
            else "",
        },
    )

@admin_afa_bp.route("/admin/api/afa/settings", methods=["POST"])
def admin_afa_set_settings():
    if not _require_admin():
        return jsonify(success=False, error="Unauthorized"), 401

    payload = request.get_json(silent=True) or {}
    try:
        price = float(payload.get("price", AMOUNT_DEFAULT))
        if price < 0:
            return jsonify(success=False, error="Price must be >= 0.00"), 400
        is_open = bool(payload.get("is_open", True))
        in_stock = bool(payload.get("in_stock", True))
    except Exception as e:
        return jsonify(success=False, error=str(e)), 400

    doc = _save_settings(price, is_open, in_stock)

    # (Optional) Mirror flags to a service doc if present (safe no-op otherwise)
    try:
        services_col.update_many(
            {"name": {"$in": ["AFA TALKTIME", "AFA Registration"]}},
            {
                "$set": {
                    "status": "OPEN" if is_open else "CLOSED",
                    "availability": "AVAILABLE" if in_stock else "OUT_OF_STOCK",
                    "updated_at": _now(),
                }
            },
        )
    except Exception:
        pass

    return jsonify(
        success=True,
        data={
            "price": float(doc["price"]),
            "is_open": bool(doc["is_open"]),
            "in_stock": bool(doc["in_stock"]),
            "updated_at": doc["updated_at"].strftime("%d %b %Y, %I:%M %p"),
        },
    )

# ------------- LIST / FILTER API -------------

@admin_afa_bp.route("/admin/api/afa/list", methods=["GET"])
def admin_afa_list():
    if not _require_admin():
        return jsonify(success=False, error="Unauthorized"), 401

    s = _get_settings()
    # Single source of truth: settings price
    current_price = float(s.get("price", AMOUNT_DEFAULT))

    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    charged = (request.args.get("charged") or "").strip().lower()  # '', 'true', 'false'
    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()

    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    try:
        page_size = int(request.args.get("page_size", 25))
    except Exception:
        page_size = 25
    page_size = max(1, min(page_size, 200))

    query = {}
    if status:
        query["status"] = status
    if charged in {"true", "false"}:
        query["charged"] = (charged == "true")

    if q:
        rx = re.compile(re.escape(q), re.I)
        query["$or"] = [
            {"name": rx},
            {"phone": rx},
            {"ghana_card": rx},
            {"location": rx},
        ]

    if date_from or date_to:
        rng = {}
        try:
            if date_from:
                rng["$gte"] = datetime.strptime(date_from, "%Y-%m-%d")
            if date_to:
                rng["$lt"] = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
        except Exception:
            pass
        if rng:
            query["created_at"] = rng

    total = afa_col.count_documents(query)

    # Status counts + total amount (ALWAYS using settings price)
    # sum_amount = count * current_price
    agg = list(
        afa_col.aggregate(
            [
                {"$match": query},
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1},
                        "sum_amount": {"$sum": current_price},
                    }
                },
            ]
        )
    )
    status_counts = {(d["_id"] or "pending"): d["count"] for d in agg}
    total_amount = sum(d.get("sum_amount", 0.0) for d in agg)

    cur = (
        afa_col.find(query)
        .sort([("created_at", -1)])
        .skip((page - 1) * page_size)
        .limit(page_size)
    )

    items = list(cur)

    # hydrate customers for display
    cust_ids = []
    for d in items:
        cid = d.get("customer_id")
        if isinstance(cid, ObjectId):
            cust_ids.append(cid)
    users_map = {}
    if cust_ids:
        for u in users_col.find({"_id": {"$in": list(set(cust_ids))}}):
            users_map[u["_id"]] = {
                "username": u.get("username"),
                "first_name": u.get("first_name"),
                "last_name": u.get("last_name"),
                "phone": u.get("phone"),
            }

    out_items = []
    for d in items:
        created = d.get("created_at")
        cid = d.get("customer_id")
        uinfo = users_map.get(cid) if isinstance(cid, ObjectId) else None

        # Always show settings price
        amount = float(current_price)

        out_items.append(
            {
                "id": str(d["_id"]),
                "customer": {
                    "id": str(cid) if cid is not None else None,
                    "name": (
                        uinfo.get("username")
                        or (
                            f"{uinfo.get('first_name','')} {uinfo.get('last_name','')}".strip()
                            if uinfo
                            else None
                        )
                    ),
                    "phone": uinfo.get("phone") if uinfo else None,
                },
                "name": d.get("name"),
                "phone": d.get("phone"),
                "ghana_card": d.get("ghana_card"),
                "dob": d.get("dob"),
                "location": d.get("location"),
                "amount": amount,
                "status": (d.get("status") or "pending"),
                "charged": bool(d.get("charged", False)),
                "created_at_display": created.strftime("%d %b %Y, %I:%M %p") if created else "",
            }
        )

    return jsonify(
        success=True,
        items=out_items,
        total=total,
        total_amount=round(float(total_amount or 0), 2),
        page=page,
        page_size=page_size,
        status_counts=status_counts,
    )

# ------------- UPDATE STATUS -------------

@admin_afa_bp.route("/admin/api/afa/<reg_id>/status", methods=["POST"])
def admin_afa_update_status(reg_id):
    if not _require_admin():
        return jsonify(success=False, error="Unauthorized"), 401

    payload = request.get_json(silent=True) or request.form or {}
    new_status = (payload.get("status") or "").strip().lower()
    if new_status not in {"pending", "processing", "delivered", "completed", "failed", "rejected"}:
        return jsonify(success=False, error="Invalid status"), 400

    oid = _to_objectid(reg_id)
    if not oid:
        return jsonify(success=False, error="Invalid id"), 400

    upd = {"status": new_status, "updated_at": _now()}
    res = afa_col.update_one({"_id": oid}, {"$set": upd})
    if not res.matched_count:
        return jsonify(success=False, error="Registration not found"), 404

    return jsonify(success=True, message="Status updated.")

# ------------- CHARGE CUSTOMER (ALWAYS use settings price) -------------

@admin_afa_bp.route("/admin/api/afa/<reg_id>/charge", methods=["POST"])
def admin_afa_charge(reg_id):
    if not _require_admin():
        return jsonify(success=False, error="Unauthorized"), 401

    oid = _to_objectid(reg_id)
    if not oid:
        return jsonify(success=False, error="Invalid id"), 400

    reg = afa_col.find_one({"_id": oid})
    if not reg:
        return jsonify(success=False, error="Registration not found"), 404

    if reg.get("charged"):
        return jsonify(success=False, error="Already charged"), 400

    settings = _get_settings()
    try:
        current_price = float(settings.get("price", AMOUNT_DEFAULT))
    except Exception:
        current_price = AMOUNT_DEFAULT

    # Always charge the current settings price
    amount = current_price
    if amount < 0:
        amount = AMOUNT_DEFAULT

    customer_id = reg.get("customer_id")

    # find the customer's balance doc (prefer ObjectId)
    bal = None
    if isinstance(customer_id, ObjectId):
        bal = balances_col.find_one({"user_id": customer_id})
    if not bal and customer_id:
        # fallback if stored as string
        bal = balances_col.find_one({"user_id": customer_id})

    if not bal:
        return jsonify(success=False, error="Customer balance not found"), 404

    old_amount = float(bal.get("amount", 0.0))
    new_amount = old_amount - amount
    if new_amount < 0:
        return jsonify(success=False, error="Insufficient funds"), 400

    # update balance
    balances_col.update_one(
        {"_id": bal["_id"]},
        {"$set": {"amount": new_amount, "updated_at": _now()}},
    )

    # log
    actor_id, actor_name = _get_actor()
    log_doc = {
        "balance_id": bal["_id"],
        "user_id": bal["user_id"],
        "action": "withdraw",
        "delta": -amount,
        "amount_before": old_amount,
        "amount_after": new_amount,
        "currency": bal.get("currency", "GHS"),
        "note": f"AFA registration charge ({reg_id})",
        "actor_id": ObjectId(actor_id) if actor_id else None,
        "actor_name": actor_name,
        "created_at": _now(),
    }
    log_res = balance_logs_col.insert_one(log_doc)

    # mark registration as charged and persist the settings price used
    afa_col.update_one(
        {"_id": oid},
        {
            "$set": {
                "charged": True,
                "charged_amount": amount,
                "charged_at": _now(),
                "charged_by": actor_name,
                "charge_log_id": log_res.inserted_id,
                "amount": amount,  # normalize to settings price for UI/reporting consistency
                "updated_at": _now(),
            }
        },
    )

    return jsonify(success=True, message="Customer charged successfully.")

# ---- AFA stats for dashboard ----

@admin_afa_bp.route("/admin/api/afa/stats", methods=["GET"])
def admin_afa_stats():
    if not _require_admin():
        return jsonify(success=False, error="Unauthorized"), 401

    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)

    try:
        total = afa_col.count_documents({})
        today_cnt = afa_col.count_documents({"created_at": {"$gte": start, "$lt": end}})
        pending = afa_col.count_documents({"status": "pending"})
        processing = afa_col.count_documents({"status": "processing"})
        delivered = afa_col.count_documents({"status": "delivered"})
        completed = afa_col.count_documents({"status": "completed"})
        failed = afa_col.count_documents({"status": {"$in": ["failed", "rejected"]}})
        uncharged = afa_col.count_documents({"$or": [{"charged": {"$exists": False}}, {"charged": False}]})
    except Exception:
        total = today_cnt = pending = processing = delivered = completed = failed = uncharged = 0

    return jsonify(
        success=True,
        data={
            "total": total,
            "today": today_cnt,
            "pending": pending,
            "processing": processing,
            "delivered": delivered,
            "completed": completed,
            "failed": failed,
            "rejected": failed,  # mirror key for front-end convenience
            "uncharged": uncharged,
        },
    )
