# admin_dashboard.py
from flask import Blueprint, render_template, session, redirect, url_for
from db import db
from bson import ObjectId
from typing import Dict, Any, List, Tuple, Union
from datetime import datetime, timedelta  # NEW

admin_dashboard_bp = Blueprint("admin_dashboard", __name__)

# Collections
orders_col = db["orders"]
users_col = db["users"]
balance_logs_col = db["balance_logs"]          # audit logs to compute deposits/deductions
afa_col = db["afa_registrations"]
transactions_col = db["transactions"]          # NEW: for transaction KPIs

# ----------------------------
# Helpers
# ----------------------------

def _users_display_map(user_ids: List[ObjectId]) -> Dict[ObjectId, str]:
    users_map: Dict[ObjectId, str] = {}
    if not user_ids:
        return users_map
    for u in users_col.find({"_id": {"$in": user_ids}}, {"username": 1, "name": 1, "phone": 1}):
        display = u.get("username") or u.get("name") or u.get("phone")
        if not display:
            display = f"User {str(u['_id'])[:6].upper()}"
        users_map[u["_id"]] = display
    return users_map


def top_customers_by_orders(limit: int = 10) -> Tuple[List[str], List[int]]:
    pipeline = [
        {"$group": {"_id": "$user_id", "order_count": {"$sum": 1}}},
        {"$sort": {"order_count": -1}},
        {"$limit": int(limit)},
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    user_ids = [doc["_id"] for doc in agg if isinstance(doc.get("_id"), ObjectId)]
    users_map = _users_display_map(user_ids)

    labels, values = [], []
    for doc in agg:
        uid = doc["_id"]
        count = int(doc.get("order_count", 0))
        label = users_map.get(uid) or (f"User {str(uid)[:6].upper()}" if isinstance(uid, ObjectId) else "Unknown")
        labels.append(label)
        values.append(count)
    return labels, values


def top_customers_by_profit(limit: int = 10) -> Tuple[List[str], List[float]]:
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "profit_sum": {"$sum": {"$ifNull": ["$profit_amount_total", 0]}}
        }},
        {"$sort": {"profit_sum": -1}},
        {"$limit": int(limit)},
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    user_ids = [doc["_id"] for doc in agg if isinstance(doc.get("_id"), ObjectId)]
    users_map = _users_display_map(user_ids)

    labels, values = [], []
    for doc in agg:
        uid = doc["_id"]
        profit = float(doc.get("profit_sum", 0) or 0)
        label = users_map.get(uid) or (f"User {str(uid)[:6].upper()}" if isinstance(uid, ObjectId) else "Unknown")
        labels.append(label)
        values.append(profit)
    return labels, values


def top_offers_by_purchases(limit: int = 3, status_filter: Union[str, List[str], None] = "completed") -> List[Dict[str, Any]]:
    match_stage: Dict[str, Any] = {}
    if status_filter:
        if isinstance(status_filter, (list, tuple, set)):
            match_stage = {"status": {"$in": list(status_filter)}}
        else:
            match_stage = {"status": status_filter}

    pipeline: List[Dict[str, Any]] = []
    if match_stage:
        pipeline.append({"$match": match_stage})

    pipeline += [
        {"$unwind": "$items"},
        {"$project": {
            "service": {"$ifNull": ["$items.serviceName", "Unknown"]},
            "offer": {"$ifNull": ["$items.value", "N/A"]},
        }},
        {"$group": {"_id": {"service": "$service", "offer": "$offer"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
    ]

    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    results: List[Dict[str, Any]] = []
    for doc in agg:
        _id = doc.get("_id") or {}
        results.append({
            "service": _id.get("service", "Unknown"),
            "offer": _id.get("offer", "N/A"),
            "count": int(doc.get("count", 0)),
        })
    return results


def compute_totals() -> Dict[str, float]:
    pipeline = [{
        "$group": {
            "_id": None,
            "sum_total_amount": {"$sum": {"$ifNull": ["$total_amount", 0]}},
            "sum_charged_amount": {"$sum": {"$ifNull": ["$charged_amount", 0]}},
            "sum_profit_amount": {"$sum": {"$ifNull": ["$profit_amount_total", 0]}},
        }
    }]
    try:
        doc = next(orders_col.aggregate(pipeline), None)
    except Exception:
        doc = None

    return {
        "sum_total_amount": float((doc or {}).get("sum_total_amount", 0) or 0),
        "sum_charged_amount": float((doc or {}).get("sum_charged_amount", 0) or 0),
        "sum_profit_amount": float((doc or {}).get("sum_profit_amount", 0) or 0),
    }


def compute_customer_counts() -> Dict[str, int]:
    """Return counts for customers by status."""
    try:
        total_customers = users_col.count_documents({"role": "customer"})
        blocked_customers = users_col.count_documents({"role": "customer", "status": "blocked"})
        active_customers = users_col.count_documents({
            "role": "customer",
            "$or": [
                {"status": {"$exists": False}},
                {"status": {"$ne": "blocked"}}
            ]
        })
    except Exception:
        total_customers = blocked_customers = active_customers = 0

    return {
        "total_customers": int(total_customers),
        "blocked_customers": int(blocked_customers),
        "active_customers": int(active_customers),
    }


def compute_balance_flow_totals() -> Dict[str, float]:
    """
    Uses balance_logs:
      action = 'deposit' (positive delta)
      action = 'withdraw' (negative delta)
    Ignores 'set' actions.
    Returns overall + today's totals (UTC). Ghana is UTC±0, so this matches local.
    """
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)

    def _sum(pipeline: List[Dict[str, Any]]) -> float:
        try:
            doc = next(balance_logs_col.aggregate(pipeline), None)
            return float((doc or {}).get("total", 0) or 0)
        except Exception:
            return 0.0

    # Overall deposits
    deposits_overall = _sum([
        {"$match": {"action": "deposit"}},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$delta", 0]}}}},
    ])

    # Overall withdrawals (as positive total)
    withdrawals_overall = _sum([
        {"$match": {"action": "withdraw"}},
        {"$group": {"_id": None, "total": {"$sum": {"$abs": {"$ifNull": ["$delta", 0]}}}}},  # abs of negative deltas
    ])

    # Today's deposits
    deposits_today = _sum([
        {"$match": {"action": "deposit", "created_at": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$delta", 0]}}}},
    ])

    # Today's withdrawals (as positive total)
    withdrawals_today = _sum([
        {"$match": {"action": "withdraw", "created_at": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": None, "total": {"$sum": {"$abs": {"$ifNull": ["$delta", 0]}}}}}
    ])

    return {
        "deposits_overall": deposits_overall,
        "withdrawals_overall": withdrawals_overall,
        "deposits_today": deposits_today,
        "withdrawals_today": withdrawals_today,
    }


def compute_transaction_kpis() -> Dict[str, float]:
    """
    Purchase-only KPIs (exclude Paystack deposits):
      - txn_total_count:   count of successful purchase transactions (all-time)
      - txn_today_count:   count of successful purchase transactions today
      - txn_total_amount:  sum(amount) of successful purchase transactions (all-time)
      - txn_today_amount:  sum(amount) of successful purchase transactions today

    Uses verified_at as the transaction timestamp (inclusive of today).
    """
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)

    base_match = {"status": "success", "type": "purchase"}  # <-- filter to purchases only

    # All-time count
    try:
        txn_total_count = transactions_col.count_documents(base_match)
    except Exception:
        txn_total_count = 0

    # All-time sum(amount)
    try:
        total_sum_doc = next(transactions_col.aggregate([
            {"$match": base_match},
            {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$amount", 0]}}}}
        ]), None)
        txn_total_amount = float((total_sum_doc or {}).get("total", 0) or 0)
    except Exception:
        txn_total_amount = 0.0

    # Today's count
    try:
        txn_today_count = transactions_col.count_documents({
            **base_match,
            "verified_at": {"$gte": start, "$lt": end}
        })
    except Exception:
        txn_today_count = 0

    # Today's sum(amount)
    try:
        today_sum_doc = next(transactions_col.aggregate([
            {"$match": {**base_match, "verified_at": {"$gte": start, "$lt": end}}},
            {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$amount", 0]}}}}
        ]), None)
        txn_today_amount = float((today_sum_doc or {}).get("total", 0) or 0)
    except Exception:
        txn_today_amount = 0.0

    return {
        "txn_total_count": int(txn_total_count),
        "txn_today_count": int(txn_today_count),
        "txn_total_amount": txn_total_amount,
        "txn_today_amount": txn_today_amount,
    }

# ----------------------------
# Route
# ----------------------------

@admin_dashboard_bp.route("/admin/dashboard")
def admin_dashboard():
    # Protect route: only accessible if admin is logged in
    if not session.get("admin_logged_in"):
        return redirect(url_for("login.login"))

    # Orders totals
    try:
        total_orders = orders_col.estimated_document_count()
    except Exception:
        total_orders = 0

    totals = compute_totals()
    sum_total_amount = totals["sum_total_amount"]
    sum_charged_amount = totals["sum_charged_amount"]
    sum_profit_amount = totals["sum_profit_amount"]

    # Top customers (orders & profit)
    chart_labels, chart_values = top_customers_by_orders(limit=10)
    profit_chart_labels, profit_chart_values = top_customers_by_profit(limit=10)

    # Top offers (service + offer), from completed orders
    top_offers = top_offers_by_purchases(limit=3, status_filter="completed")

    # Customer counts
    cust_counts = compute_customer_counts()

    # Wallet flows (overall + today)
    flow = compute_balance_flow_totals()

    # AFA registration KPIs
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)
    try:
        afa_total  = afa_col.count_documents({})
        afa_pending = afa_col.count_documents({"status": "pending"})
        afa_today   = afa_col.count_documents({"created_at": {"$gte": start, "$lt": end}})
    except Exception:
        afa_total = afa_pending = afa_today = 0

    # Transactions KPIs
    tx = compute_transaction_kpis()

    return render_template(
        "admin_dashboard.html",
        # KPIs
        total_orders=total_orders,
        sum_total_amount=sum_total_amount,
        sum_charged_amount=sum_charged_amount,
        sum_profit_amount=sum_profit_amount,

        # Charts
        chart_labels=chart_labels,
        chart_values=chart_values,
        profit_chart_labels=profit_chart_labels,
        profit_chart_values=profit_chart_values,

        # Lists
        top_offers=top_offers,

        # Customer counters
        total_customers=cust_counts["total_customers"],
        blocked_customers=cust_counts["blocked_customers"],
        active_customers=cust_counts["active_customers"],

        # Balance flows
        deposits_overall=flow["deposits_overall"],
        withdrawals_overall=flow["withdrawals_overall"],
        deposits_today=flow["deposits_today"],
        withdrawals_today=flow["withdrawals_today"],

        # AFA stats
        afa_total=afa_total,
        afa_pending=afa_pending,
        afa_today=afa_today,

        # Transactions KPIs (counts + sums)
        txn_total_count=tx["txn_total_count"],
        txn_today_count=tx["txn_today_count"],
        txn_total_amount=tx["txn_total_amount"],
        txn_today_amount=tx["txn_today_amount"],
    )
