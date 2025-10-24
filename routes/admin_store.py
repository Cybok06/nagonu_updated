# routes/admin_store.py
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple, Optional

from bson import ObjectId
from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

from db import db

admin_store_bp = Blueprint("admin_store", __name__, url_prefix="/admin")

# Collections
stores_col        = db["stores"]
orders_col        = db["orders"]
users_col         = db["users"]
balances_col      = db["balances"]
transactions_col  = db["transactions"]

# NEW: payout settings & logs (read-only for admin)
store_payouts_col = db["store_payouts"]       # { owner_id, store_slug, recipient_name, msisdn, network, created_at, updated_at }
store_payout_logs = db["store_payout_logs"]   # { owner_id, store_slug, changes, created_at }


# ---------------- helpers ----------------
def _require_admin() -> bool:
    return bool(session.get("role") == "admin" and session.get("user_id"))


def _day_range(d: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end


def _fmt_money(x: Any) -> float:
    try:
        return round(float(x or 0), 2)
    except Exception:
        return 0.0


def _display_name(user_doc: Dict[str, Any] | None) -> str:
    if not user_doc:
        return "User"
    for key in ("full_name", "name"):
        if user_doc.get(key):
            return str(user_doc[key]).strip()
    if user_doc.get("username"):
        return str(user_doc["username"]).strip()
    if user_doc.get("email"):
        return str(user_doc["email"]).split("@", 1)[0]  # fixed split
    return "User"


def _withdrawn_so_far(store_slug: str) -> float:
    """
    Sum of successful withdrawals for this store.
    Stored in transactions: type='store_withdrawal', meta.store_slug=<slug>
    """
    pipeline = [
        {"$match": {
            "type": "store_withdrawal",
            "status": "success",
            "meta.store_slug": store_slug
        }},
        {"$group": {"_id": None, "amt": {"$sum": {"$toDouble": {"$ifNull": ["$amount", 0]}}}}}
    ]
    agg = list(transactions_col.aggregate(pipeline))
    return _fmt_money(agg[0]["amt"]) if agg else 0.0


def _profit_all_time(store_slug: str) -> float:
    pipeline = [
        {"$match": {"store_slug": store_slug}},
        {"$group": {"_id": None, "p": {"$sum": {"$toDouble": {"$ifNull": ["$profit_amount_total", 0]}}}}}
    ]
    agg = list(orders_col.aggregate(pipeline))
    return _fmt_money(agg[0]["p"]) if agg else 0.0


def _owner_wallet_balance(user_id: ObjectId) -> float:
    bal_doc = balances_col.find_one({"user_id": user_id}) or {}
    return _fmt_money(bal_doc.get("amount"))


# ---------------- pages ----------------
@admin_store_bp.route("/stores", methods=["GET"])
def admin_stores_page():
    if not _require_admin():
        return redirect(url_for("login.login"))
    # Page loads a lightweight view; data grid fetches via /api
    return render_template("admin_stores.html")


# ---------------- APIs ----------------
@admin_store_bp.route("/api/stores", methods=["GET"])
def api_admin_list_stores():
    """
    Returns a row per store with owner info, KPIs (today + all-time), status, withdrawable profit.
    Optional params:
      - q: search by store/owner/slug (case-insensitive substring)
      - status: published|suspended|draft
      - page, limit
    """
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    q = (request.args.get("q") or "").strip().lower()
    status_filter = (request.args.get("status") or "").strip().lower()
    try:
        limit = max(1, min(200, int(request.args.get("limit", 50))))
    except Exception:
        limit = 50
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    skip = (page - 1) * limit

    stores_query: Dict[str, Any] = {"status": {"$ne": "deleted"}}
    if status_filter in {"published", "suspended", "draft"}:
        stores_query["status"] = status_filter

    cur = stores_col.find(stores_query).sort([("updated_at", -1), ("created_at", -1)]).skip(skip).limit(limit)
    store_docs = list(cur)

    owner_ids = list({s.get("owner_id") for s in store_docs if s.get("owner_id")})
    owners: Dict[ObjectId, Dict[str, Any]] = {}
    if owner_ids:
        owners = {
            o["_id"]: o
            for o in users_col.find(
                {"_id": {"$in": owner_ids}},
                {"full_name": 1, "name": 1, "username": 1, "email": 1}
            )
        }

    today = datetime.utcnow().date()
    d0, d1 = _day_range(today)
    slugs = [s.get("slug") for s in store_docs if s.get("slug")]

    pipeline_all = [
        {"$match": {"store_slug": {"$in": slugs}}},
        {"$group": {
            "_id": "$store_slug",
            "total_sales": {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}},
            "total_profit": {"$sum": {"$toDouble": {"$ifNull": ["$profit_amount_total", 0]}}},
            "orders_count": {"$sum": 1}
        }},
    ]
    all_time_map = {x["_id"]: x for x in orders_col.aggregate(pipeline_all)} if slugs else {}

    pipeline_today = [
        {"$match": {"store_slug": {"$in": slugs}, "created_at": {"$gte": d0, "$lt": d1}}},
        {"$group": {
            "_id": "$store_slug",
            "sales":  {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}},
            "profit": {"$sum": {"$toDouble": {"$ifNull": ["$profit_amount_total", 0]}}},
            "orders": {"$sum": 1}
        }},
    ]
    today_map = {x["_id"]: x for x in orders_col.aggregate(pipeline_today)} if slugs else {}

    rows: List[Dict[str, Any]] = []
    for s in store_docs:
        slug = s.get("slug")
        if not slug:
            continue
        owner_id = s.get("owner_id")
        owner = owners.get(owner_id)
        owner_name = _display_name(owner)

        if q:
            blob = f"{s.get('name','').lower()} {slug.lower()} {owner_name.lower()}"
            if q not in blob:
                continue

        a = all_time_map.get(slug, {})
        t = today_map.get(slug, {})

        total_sales = _fmt_money(a.get("total_sales"))
        total_profit = _fmt_money(a.get("total_profit"))
        orders_count = int(a.get("orders_count", 0))

        sales_today = _fmt_money(t.get("sales"))
        profit_today = _fmt_money(t.get("profit"))
        orders_today = int(t.get("orders", 0))

        withdrawn = _withdrawn_so_far(slug)
        withdrawable = max(0.0, round(total_profit - withdrawn, 2))

        rows.append({
            "slug": slug,
            "name": s.get("name"),
            "status": s.get("status"),
            "owner_id": str(owner_id) if owner_id else None,
            "owner_name": owner_name,
            "sales_today": sales_today,
            "profit_today": profit_today,
            "orders_today": orders_today,
            "total_sales": total_sales,
            "total_profit": total_profit,
            "orders_count": orders_count,
            "withdrawn_so_far": withdrawn,
            "withdrawable": withdrawable,
            "updated_at": s.get("updated_at").isoformat() if isinstance(s.get("updated_at"), datetime) else s.get("updated_at"),
        })

    return jsonify({"success": True, "rows": rows, "page": page, "limit": limit})


@admin_store_bp.route("/api/stores/<slug>", methods=["GET"])
def api_admin_store_summary(slug: str):
    """
    Single store summary (for side drawer or details page).
    Returns: store basics, owner, wallet, KPIs (today & all-time), withdrawable,
             + payout details (recipient_name, msisdn, network, timestamps, history_count).
    """
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    store = stores_col.find_one({"slug": slug, "status": {"$ne": "deleted"}})
    if not store:
        return jsonify({"success": False, "message": "Store not found"}), 404

    owner = users_col.find_one(
        {"_id": store.get("owner_id")},
        {"full_name": 1, "name": 1, "username": 1, "email": 1}
    )
    owner_name = _display_name(owner)
    owner_wallet = _owner_wallet_balance(store.get("owner_id"))

    # KPIs
    today = datetime.utcnow().date()
    d0, d1 = _day_range(today)

    pipeline_all = [
        {"$match": {"store_slug": slug}},
        {"$group": {
            "_id": None,
            "total_sales": {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}},
            "total_profit": {"$sum": {"$toDouble": {"$ifNull": ["$profit_amount_total", 0]}}},
            "orders_count": {"$sum": 1}
        }},
    ]
    agg_all = list(orders_col.aggregate(pipeline_all))
    total_sales = _fmt_money(agg_all[0].get("total_sales") if agg_all else 0)
    total_profit = _fmt_money(agg_all[0].get("total_profit") if agg_all else 0)
    orders_count = int(agg_all[0].get("orders_count") if agg_all else 0)

    pipeline_today = [
        {"$match": {"store_slug": slug, "created_at": {"$gte": d0, "$lt": d1}}},
        {"$group": {
            "_id": None,
            "sales":  {"$sum": {"$toDouble": {"$ifNull": ["$total_amount", 0]}}},
            "profit": {"$sum": {"$toDouble": {"$ifNull": ["$profit_amount_total", 0]}}},
            "orders": {"$sum": 1}
        }},
    ]
    agg_today = list(orders_col.aggregate(pipeline_today))
    sales_today = _fmt_money(agg_today[0].get("sales") if agg_today else 0)
    profit_today = _fmt_money(agg_today[0].get("profit") if agg_today else 0)
    orders_today = int(agg_today[0].get("orders") if agg_today else 0)

    withdrawn = _withdrawn_so_far(slug)
    withdrawable = max(0.0, round(total_profit - withdrawn, 2))

    # Payout details (+ basic history count)
    owner_id: Optional[ObjectId] = store.get("owner_id")
    payout = store_payouts_col.find_one({"owner_id": owner_id, "store_slug": slug}) or {}
    history_count = store_payout_logs.count_documents({"owner_id": owner_id, "store_slug": slug}) if owner_id else 0

    payout_payload = {
        "recipient_name": payout.get("recipient_name"),
        "msisdn": payout.get("msisdn"),
        "network": payout.get("network"),
        "created_at": payout.get("created_at").isoformat() if isinstance(payout.get("created_at"), datetime) else payout.get("created_at"),
        "updated_at": payout.get("updated_at").isoformat() if isinstance(payout.get("updated_at"), datetime) else payout.get("updated_at"),
        "history_count": history_count,
        # convenience strings for copy buttons / UI
        "name": payout.get("recipient_name"),
        "phone": payout.get("msisdn"),
        "provider": payout.get("network"),
    }

    return jsonify({
        "success": True,
        "store": {
            "slug": slug,
            "name": store.get("name"),
            "status": store.get("status"),
            "owner_id": str(store.get("owner_id")) if store.get("owner_id") else None,
            "owner_name": owner_name,
            "owner_wallet": owner_wallet,
            "today": {
                "sales": sales_today,
                "profit": profit_today,
                "orders": orders_today,
            },
            "all_time": {
                "sales": total_sales,
                "profit": total_profit,
                "orders": orders_count,
            },
            "withdrawn_so_far": withdrawn,
            "withdrawable": withdrawable,
            "updated_at": store.get("updated_at").isoformat() if isinstance(store.get("updated_at"), datetime) else store.get("updated_at"),
            "payout": payout_payload,
        }
    })


@admin_store_bp.route("/api/stores/<slug>/withdrawals", methods=["GET"])
def api_admin_store_withdrawals(slug: str):
    """
    Paginated list of withdrawals for a store (most recent first).
    Query params: page (default 1), limit (default 20)
    """
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    store = stores_col.find_one({"slug": slug})
    if not store:
        return jsonify({"success": False, "message": "Store not found"}), 404

    try:
        limit = max(1, min(100, int(request.args.get("limit", 20))))
    except Exception:
        limit = 20
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    skip = (page - 1) * limit

    cur = transactions_col.find(
        {"type": "store_withdrawal", "meta.store_slug": slug}
    ).sort("created_at", -1).skip(skip).limit(limit)

    items: List[Dict[str, Any]] = []
    for t in cur:
        items.append({
            "reference": t.get("reference"),
            "amount": _fmt_money(t.get("amount")),
            "status": t.get("status"),
            "created_at": t.get("created_at").isoformat() if isinstance(t.get("created_at"), datetime) else t.get("created_at"),
            "verified_at": t.get("verified_at").isoformat() if isinstance(t.get("verified_at"), datetime) else t.get("verified_at"),
            "note": (t.get("meta") or {}).get("note"),
            "user_id": str(t.get("user_id")) if t.get("user_id") else None,
            "gateway": t.get("gateway") or "Internal",
            "currency": t.get("currency") or "GHS",
        })

    total_count = transactions_col.count_documents({"type": "store_withdrawal", "meta.store_slug": slug})

    return jsonify({
        "success": True,
        "slug": slug,
        "page": page,
        "limit": limit,
        "total": total_count,
        "items": items
    })


# Lightweight payout endpoint (used by modal & fallbacks)
@admin_store_bp.route("/api/stores/<slug>/payout", methods=["GET"])
def api_admin_store_payout(slug: str):
    """
    Return current payout details for this store so admin can copy them during withdrawal.
    Looks in `store_payouts` by (owner_id, store_slug).
    """
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    store = stores_col.find_one({"slug": slug, "status": {"$ne": "deleted"}})
    if not store:
        return jsonify({"success": False, "message": "Store not found"}), 404

    owner_id: Optional[ObjectId] = store.get("owner_id")
    payout = store_payouts_col.find_one({"owner_id": owner_id, "store_slug": slug}) or {}
    history_count = store_payout_logs.count_documents({"owner_id": owner_id, "store_slug": slug}) if owner_id else 0

    payload = {
        "recipient_name": payout.get("recipient_name"),
        "msisdn": payout.get("msisdn"),
        "network": payout.get("network"),
        "created_at": payout.get("created_at").isoformat() if isinstance(payout.get("created_at"), datetime) else payout.get("created_at"),
        "updated_at": payout.get("updated_at").isoformat() if isinstance(payout.get("updated_at"), datetime) else payout.get("updated_at"),
        "history_count": history_count,
        # convenience fields
        "copy_line": "Name: {name} | Number: {num} | Network: {net}".format(
            name=payout.get("recipient_name") or "-",
            num=payout.get("msisdn") or "-",
            net=payout.get("network") or "-"
        ),
        "copy_number": payout.get("msisdn") or "",
        "copy_name": payout.get("recipient_name") or "",
        "name": payout.get("recipient_name"),
        "phone": payout.get("msisdn"),
        "provider": payout.get("network"),
    }
    return jsonify({"success": True, "slug": slug, "payout": payload})


@admin_store_bp.route("/api/stores/<slug>/status", methods=["POST"])
def api_admin_update_store_status(slug: str):
    """
    body: { "action": "suspend"|"resume"|"delete" }
    """
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    action = (body.get("action") or "").strip().lower()

    if action == "suspend":
        res = stores_col.update_one({"slug": slug, "status": {"$ne": "deleted"}}, {"$set": {"status": "suspended", "updated_at": datetime.utcnow()}})
    elif action == "resume":
        res = stores_col.update_one({"slug": slug, "status": {"$ne": "deleted"}}, {"$set": {"status": "published", "updated_at": datetime.utcnow()}})
    elif action == "delete":
        res = stores_col.update_one({"slug": slug}, {"$set": {"status": "deleted", "updated_at": datetime.utcnow()}})
    else:
        return jsonify({"success": False, "message": "Invalid action"}), 400

    if res.matched_count == 0:
        return jsonify({"success": False, "message": "Store not found"}), 404
    return jsonify({"success": True, "slug": slug, "action": action})


@admin_store_bp.route("/api/stores/suspend_all", methods=["POST"])
def api_admin_suspend_all():
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    res = stores_col.update_many({"status": {"$ne": "deleted"}}, {"$set": {"status": "suspended", "updated_at": datetime.utcnow()}})
    return jsonify({"success": True, "affected": res.modified_count})


@admin_store_bp.route("/api/stores/<slug>/withdraw", methods=["POST"])
def api_admin_store_withdraw(slug: str):
    """
    Credit the store owner's wallet with (amount) from accumulated profit.
    body: { "amount": <number|null|'all'> }
    withdrawable = sum(profit_amount_total) - sum(previous successful withdrawals)
    """
    if not _require_admin():
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    store = stores_col.find_one({"slug": slug, "status": {"$ne": "deleted"}})
    if not store:
        return jsonify({"success": False, "message": "Store not found"}), 404
    owner_id: Optional[ObjectId] = store.get("owner_id")
    if not owner_id:
        return jsonify({"success": False, "message": "Store missing owner"}), 400

    total_profit = _profit_all_time(slug)
    withdrawn = _withdrawn_so_far(slug)
    withdrawable = max(0.0, round(total_profit - withdrawn, 2))

    body = request.get_json(silent=True) or {}
    amt_req = body.get("amount", "all")

    if isinstance(amt_req, str) and amt_req.strip().lower() == "all":
        amount = withdrawable
    else:
        amount = _fmt_money(amt_req)

    if amount <= 0:
        return jsonify({"success": False, "message": "Nothing to withdraw"}), 400
    if amount - withdrawable > 1e-9:
        return jsonify({"success": False, "message": "Amount exceeds withdrawable"}), 400

    # credit owner's wallet
    balances_col.update_one(
        {"user_id": owner_id},
        {"$inc": {"amount": amount}, "$set": {"updated_at": datetime.utcnow()}},
        upsert=True
    )

    # record transaction
    transactions_col.insert_one({
        "user_id": owner_id,
        "amount": amount,
        "reference": f"WDR-{slug}-{int(datetime.utcnow().timestamp())}",
        "status": "success",
        "type": "store_withdrawal",
        "gateway": "Internal",
        "currency": "GHS",
        "created_at": datetime.utcnow(),
        "verified_at": datetime.utcnow(),
        "meta": {"store_slug": slug, "note": "Admin credited store profit to owner wallet"}
    })

    new_wallet = _owner_wallet_balance(owner_id)

    # Include payout info so UI can display/copy immediately
    payout = store_payouts_col.find_one({"owner_id": owner_id, "store_slug": slug}) or {}
    copy_line = "Name: {name} | Number: {num} | Network: {net}".format(
        name=payout.get("recipient_name") or "-",
        num=payout.get("msisdn") or "-",
        net=payout.get("network") or "-"
    )

    return jsonify({
        "success": True,
        "slug": slug,
        "credited": amount,
        "withdrawable_left": max(0.0, round(withdrawable - amount, 2)),
        "owner_wallet": new_wallet,
        "payout": {
            "recipient_name": payout.get("recipient_name"),
            "msisdn": payout.get("msisdn"),
            "network": payout.get("network"),
            "copy_line": copy_line,
            "copy_number": payout.get("msisdn") or "",
            "copy_name": payout.get("recipient_name") or ""
        }
    })
