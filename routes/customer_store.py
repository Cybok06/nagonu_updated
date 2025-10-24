# routes/customer_store.py
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple, Optional

from bson import ObjectId
from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

from db import db

customer_store_bp = Blueprint("customer_store", __name__)

# Collections
stores_col          = db["stores"]
orders_col          = db["orders"]
users_col           = db["users"]
balances_col        = db["balances"]
transactions_col    = db["transactions"]
store_payouts_col   = db["store_payouts"]       # one doc per (owner, store_slug)
store_payout_logs   = db["store_payout_logs"]   # append-only history


# ---------- helpers ----------
def _day_range(d: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end

def _fmt_money(x: Any) -> float:
    try:
        return round(float(x or 0), 2)
    except Exception:
        return 0.0

def _ensure_owner_store(user_id: ObjectId, slug: str) -> Dict[str, Any] | None:
    return stores_col.find_one({"owner_id": user_id, "slug": slug, "status": {"$ne": "deleted"}})

def _latest_owner_store(user_id: ObjectId) -> Dict[str, Any] | None:
    """Return the most recent (non-deleted) store for this owner, or None."""
    return stores_col.find_one(
        {"owner_id": user_id, "status": {"$ne": "deleted"}},
        sort=[("updated_at", -1), ("created_at", -1)]
    )

def _owner_display_name(user_doc: Dict[str, Any] | None) -> str:
    if not user_doc:
        return "Customer"
    for key in ("full_name", "name"):
        if user_doc.get(key):
            return str(user_doc[key]).strip()
    if user_doc.get("username"):
        return str(user_doc["username"]).strip()
    if user_doc.get("email"):
        return str(user_doc["email"]).split("@", 1)[0]
    return "Customer"

def _owner_wallet_balance(user_id: ObjectId) -> float:
    bal = balances_col.find_one({"user_id": user_id}) or {}
    return _fmt_money(bal.get("amount"))

def _profit_all_time(slug: str) -> float:
    pipeline = [
        {"$match": {"store_slug": slug}},
        {"$group": {"_id": None, "p": {"$sum": {"$ifNull": ["$profit_amount_total", 0]}}}}
    ]
    agg = list(orders_col.aggregate(pipeline))
    return _fmt_money(agg[0]["p"]) if agg else 0.0

def _withdrawn_so_far(slug: str) -> float:
    """
    Sum of successful withdrawals for this store (credited to owner's wallet).
    Stored in transactions: type='store_withdrawal', meta.store_slug=<slug>
    """
    pipeline = [
        {"$match": {"type": "store_withdrawal", "status": "success", "meta.store_slug": slug}},
        {"$group": {"_id": None, "amt": {"$sum": {"$ifNull": ["$amount", 0]}}}},
    ]
    agg = list(transactions_col.aggregate(pipeline))
    return _fmt_money(agg[0]["amt"]) if agg else 0.0

def _withdrawable(slug: str) -> float:
    return max(0.0, round(_profit_all_time(slug) - _withdrawn_so_far(slug), 2))

def _gather_dashboard(slug: str) -> Dict[str, Any]:
    """Compute KPIs, top offers, recent orders, withdrawable & wallet."""
    today = datetime.utcnow().date()
    d0, d1 = _day_range(today)

    # All-time totals
    pipeline_totals = [
        {"$match": {"store_slug": slug}},
        {"$group": {
            "_id": None,
            "total_sales": {"$sum": {"$ifNull": ["$total_amount", 0]}},
            "total_profit": {"$sum": {"$ifNull": ["$profit_amount_total", 0]}},
            "orders_count": {"$sum": 1},
        }},
    ]
    agg_tot = list(orders_col.aggregate(pipeline_totals))
    all_time_sales  = _fmt_money(agg_tot[0].get("total_sales") if agg_tot else 0)
    all_time_profit = _fmt_money(agg_tot[0].get("total_profit") if agg_tot else 0)
    orders_count    = int(agg_tot[0].get("orders_count") if agg_tot else 0)

    # Today's profit
    pipeline_today_profit = [
        {"$match": {"store_slug": slug, "created_at": {"$gte": d0, "$lt": d1}}},
        {"$group": {"_id": None, "profit_today": {"$sum": {"$ifNull": ["$profit_amount_total", 0]}}}},
    ]
    agg_today = list(orders_col.aggregate(pipeline_today_profit))
    profit_today = _fmt_money(agg_today[0].get("profit_today") if agg_today else 0)

    # Top offers
    pipeline_top_offers = [
        {"$match": {"store_slug": slug}},
        {"$unwind": {"path": "$items", "preserveNullAndEmptyArrays": False}},
        {"$group": {
            "_id": {
                "service": {"$ifNull": ["$items.serviceName", "Unknown Service"]},
                "label":   {"$ifNull": ["$items.value", "-"]},
            },
            "count":   {"$sum": 1},
            "revenue": {"$sum": {"$ifNull": ["$items.amount", 0]}},
        }},
        {"$sort": {"count": -1, "revenue": -1}},
        {"$limit": 8},
    ]
    top_offers_raw = list(orders_col.aggregate(pipeline_top_offers))
    top_offers = [{
        "service": x["_id"]["service"],
        "label":   x["_id"]["label"],
        "count":   int(x.get("count", 0)),
        "revenue": _fmt_money(x.get("revenue", 0)),
    } for x in top_offers_raw]

    # Recent orders (last 10)
    recent_orders_cur = (
        orders_col.find({"store_slug": slug})
        .sort("created_at", -1)
        .limit(10)
    )
    recent_orders: List[Dict[str, Any]] = []
    for o in recent_orders_cur:
        recent_orders.append({
            "order_id": o.get("order_id"),
            "status": o.get("status"),
            "total_amount":        _fmt_money(o.get("total_amount", 0)),
            "profit_amount_total": _fmt_money(o.get("profit_amount_total", 0)),
            "charged_amount":      _fmt_money(o.get("charged_amount", 0)),
            "items_count": len(o.get("items") or []),
            "created_at": o.get("created_at"),
        })

    return {
        "today": today,
        "all_time_sales": all_time_sales,
        "profit_today": profit_today,
        "all_time_profit": all_time_profit,
        "orders_count": orders_count,
        "top_offers": top_offers,
        "recent_orders": recent_orders,
        "withdrawable": _withdrawable(slug),
    }


# ---------- slug-less page route ----------
@customer_store_bp.route("/customer/store", methods=["GET"])
def customer_store_home():
    """
    Store dashboard without a slug in the URL.
    It picks the latest non-deleted store owned by the logged-in customer.
    """
    if session.get("role") != "customer" or not session.get("user_id"):
        return redirect(url_for("login.login"))

    owner_id = ObjectId(session["user_id"])
    store_doc = _latest_owner_store(owner_id)
    if not store_doc:
        return "Store not found", 404

    slug = store_doc.get("slug")
    k = _gather_dashboard(slug)
    owner = users_col.find_one({"_id": owner_id}, {"full_name": 1, "name": 1, "username": 1, "email": 1})

    return render_template(
        "customer_store.html",
        store=store_doc,
        owner_name=_owner_display_name(owner),
        all_time_sales=k["all_time_sales"],
        profit_today=k["profit_today"],
        all_time_profit=k["all_time_profit"],
        orders_count=k["orders_count"],
        top_offers=k["top_offers"],
        recent_orders=k["recent_orders"],
        withdrawable=k["withdrawable"],
        wallet_balance=_owner_wallet_balance(owner_id),
        today_str=k["today"].strftime("%b %d, %Y"),
        slug=slug,
    )


# ---------- existing slugged page route ----------
@customer_store_bp.route("/customer/store/<slug>", methods=["GET"])
def customer_store_dashboard(slug: str):
    if session.get("role") != "customer" or not session.get("user_id"):
        return redirect(url_for("login.login"))

    owner_id = ObjectId(session["user_id"])
    store_doc = _ensure_owner_store(owner_id, slug)
    if not store_doc:
        return "Store not found", 404

    k = _gather_dashboard(slug)
    owner = users_col.find_one({"_id": owner_id}, {"full_name": 1, "name": 1, "username": 1, "email": 1})

    return render_template(
        "customer_store.html",
        store=store_doc,
        owner_name=_owner_display_name(owner),
        all_time_sales=k["all_time_sales"],
        profit_today=k["profit_today"],
        all_time_profit=k["all_time_profit"],
        orders_count=k["orders_count"],
        top_offers=k["top_offers"],
        recent_orders=k["recent_orders"],
        withdrawable=k["withdrawable"],
        wallet_balance=_owner_wallet_balance(owner_id),
        today_str=k["today"].strftime("%b %d, %Y"),
        slug=slug,
    )


# ---------- JSON summaries / history ----------
@customer_store_bp.route("/api/customer/store/summary", methods=["GET"])
def api_customer_store_summary_noslug():
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    owner_id = ObjectId(session["user_id"])
    store = _latest_owner_store(owner_id)
    if not store:
        return jsonify({"success": False, "message": "Store not found"}), 404

    slug = store.get("slug")
    k = _gather_dashboard(slug)
    return jsonify({
        "success": True,
        "store": {"slug": slug, "name": store.get("name")},
        "kpis": {
            "all_time_sales": k["all_time_sales"],
            "all_time_profit": k["all_time_profit"],
            "profit_today": k["profit_today"],
            "orders_count": k["orders_count"],
            "withdrawable": k["withdrawable"],
            "wallet_balance": _owner_wallet_balance(owner_id),
        }
    })


@customer_store_bp.route("/api/customer/store/<slug>/summary", methods=["GET"])
def api_customer_store_summary(slug: str):
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    owner_id = ObjectId(session["user_id"])
    store = _ensure_owner_store(owner_id, slug)
    if not store:
        return jsonify({"success": False, "message": "Store not found"}), 404

    k = _gather_dashboard(slug)
    return jsonify({
        "success": True,
        "store": {"slug": slug, "name": store.get("name")},
        "kpis": {
            "all_time_sales": k["all_time_sales"],
            "all_time_profit": k["all_time_profit"],
            "profit_today": k["profit_today"],
            "orders_count": k["orders_count"],
            "withdrawable": k["withdrawable"],
            "wallet_balance": _owner_wallet_balance(ObjectId(session["user_id"])),
        }
    })


@customer_store_bp.route("/api/customer/store/<slug>/withdrawals", methods=["GET"])
def api_customer_store_withdrawals(slug: str):
    """Customer-facing list of their store withdrawals (credited to wallet)."""
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    owner_id = ObjectId(session["user_id"])
    if not _ensure_owner_store(owner_id, slug):
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
        {"type": "store_withdrawal", "status": "success", "meta.store_slug": slug}
    ).sort("created_at", -1).skip(skip).limit(limit)

    items = []
    for t in cur:
        items.append({
            "reference": t.get("reference"),
            "amount": _fmt_money(t.get("amount")),
            "created_at": t.get("created_at").strftime("%Y-%m-%d %H:%M") if t.get("created_at") else "",
            "verified_at": t.get("verified_at").strftime("%Y-%m-%d %H:%M") if t.get("verified_at") else "",
            "note": (t.get("meta") or {}).get("note", "Credited to wallet"),
        })
    total = transactions_col.count_documents({"type": "store_withdrawal", "status": "success", "meta.store_slug": slug})

    return jsonify({"success": True, "items": items, "page": page, "limit": limit, "total": total})


# ---------- Payout settings (page + save) ----------
@customer_store_bp.route("/customer/store/<slug>/payout", methods=["GET"])
def customer_store_payout_page(slug: str):
    """Show + edit Mobile Money payout account for this store."""
    if session.get("role") != "customer" or not session.get("user_id"):
        return redirect(url_for("login.login"))
    owner_id = ObjectId(session["user_id"])
    store = _ensure_owner_store(owner_id, slug)
    if not store:
        return "Store not found", 404

    payout = store_payouts_col.find_one({"owner_id": owner_id, "store_slug": slug}) or {}
    # history (latest first)
    hist = list(
        store_payout_logs.find({"owner_id": owner_id, "store_slug": slug}).sort("created_at", -1).limit(100)
    )

    return render_template(
        "customer_store_payout.html",
        store=store,
        current=payout,
        history=hist,
        withdrawable=_withdrawable(slug),
        wallet_balance=_owner_wallet_balance(owner_id),
    )


@customer_store_bp.route("/customer/store/<slug>/payout", methods=["POST"])
def customer_store_payout_save(slug: str):
    """Save payout details and log every change."""
    if session.get("role") != "customer" or not session.get("user_id"):
        return redirect(url_for("login.login"))
    owner_id = ObjectId(session["user_id"])
    store = _ensure_owner_store(owner_id, slug)
    if not store:
        return "Store not found", 404

    name = (request.form.get("recipient_name") or "").strip()
    phone = (request.form.get("msisdn") or "").strip()
    network = (request.form.get("network") or "").strip().upper()

    # Basic validation
    valid_nets = {"MTN", "VODAFONE", "AIRTELTIGO"}
    if network not in valid_nets:
        return render_template(
            "customer_store_payout.html",
            store=store,
            current={"recipient_name": name, "msisdn": phone, "network": network},
            history=list(store_payout_logs.find({"owner_id": owner_id, "store_slug": slug}).sort("created_at", -1)),
            error="Select a valid network.",
            withdrawable=_withdrawable(slug),
            wallet_balance=_owner_wallet_balance(owner_id),
        )

    # Normalize Ghana MSISDN as 233XXXXXXXXX where possible
    def _normalize_phone(raw: str) -> str:
        p = raw.replace(" ", "").replace("-", "").replace("+", "")
        if p.startswith("0") and len(p) == 10:
            p = "233" + p[1:]
        if p.startswith("233") and len(p) == 12:
            return p
        return raw.strip()

    phone_norm = _normalize_phone(phone)

    prev = store_payouts_col.find_one({"owner_id": owner_id, "store_slug": slug}) or {}
    doc = {
        "owner_id": owner_id,
        "store_slug": slug,
        "recipient_name": name,
        "msisdn": phone_norm,
        "network": network,
        "updated_at": datetime.utcnow(),
        "created_at": prev.get("created_at") or datetime.utcnow(),
    }
    store_payouts_col.update_one(
        {"owner_id": owner_id, "store_slug": slug},
        {"$set": doc},
        upsert=True
    )

    # Log change (diff-ish log)
    changes: Dict[str, Dict[str, Any]] = {}
    for k in ("recipient_name", "msisdn", "network"):
        old_v = prev.get(k)
        new_v = doc.get(k)
        if old_v != new_v:
            changes[k] = {"from": old_v, "to": new_v}
    if changes:
        store_payout_logs.insert_one({
            "owner_id": owner_id,
            "store_slug": slug,
            "changes": changes,
            "created_at": datetime.utcnow(),
        })

    return redirect(url_for("customer_store.customer_store_payout_page", slug=slug))
