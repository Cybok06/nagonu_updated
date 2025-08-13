# admin_dashboard.py
from flask import Blueprint, render_template, session, redirect, url_for
from db import db
from bson import ObjectId
from typing import Dict, Any, List, Tuple, Union

admin_dashboard_bp = Blueprint("admin_dashboard", __name__)

# Collections
orders_col = db["orders"]
users_col = db["users"]

# ----------------------------
# Helpers
# ----------------------------

def _users_display_map(user_ids: List[ObjectId]) -> Dict[ObjectId, str]:
    """Return {user_id: display_name} for the given user_ids."""
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
    """
    Build top-N customers by order count.
    Returns (labels, values).
    """
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
    """
    Top-N customers by total profit (sum of profit_amount_total per order).
    Returns (labels, profit_values).
    """
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
    """
    Top-N offers purchased, grouped by (serviceName, items.value).
    By default, considers only orders with status == 'completed'.
    Returns [{service, offer, count}, ...]
    """
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
    """
    Compute overall totals:
    - sum_total_amount: sum of total_amount
    - sum_charged_amount: sum of charged_amount
    - sum_profit_amount: sum of profit_amount_total
    """
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

# ----------------------------
# Route
# ----------------------------

@admin_dashboard_bp.route("/admin/dashboard")
def admin_dashboard():
    # Protect route: only accessible if admin is logged in
    if not session.get("admin_logged_in"):
        return redirect(url_for("auth.login"))

    # Totals
    try:
        total_orders = orders_col.estimated_document_count()
    except Exception:
        total_orders = 0

    totals = compute_totals()
    sum_total_amount = totals["sum_total_amount"]
    sum_charged_amount = totals["sum_charged_amount"]
    sum_profit_amount = totals["sum_profit_amount"]

    # Top customers (orders)
    chart_labels, chart_values = top_customers_by_orders(limit=10)

    # Top customers (profit)
    profit_chart_labels, profit_chart_values = top_customers_by_profit(limit=10)

    # Top offers (service + offer), from completed orders
    top_offers = top_offers_by_purchases(limit=3, status_filter="completed")

    return render_template(
        "admin_dashboard.html",
        # KPIs
        total_orders=total_orders,
        sum_total_amount=sum_total_amount,
        sum_charged_amount=sum_charged_amount,
        sum_profit_amount=sum_profit_amount,

        # Chart: top customers by orders
        chart_labels=chart_labels,
        chart_values=chart_values,

        # Chart: top customers by profit
        profit_chart_labels=profit_chart_labels,
        profit_chart_values=profit_chart_values,

        # Top 3 offers purchased
        top_offers=top_offers,
    )
