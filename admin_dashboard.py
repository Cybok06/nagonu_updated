from flask import Blueprint, render_template, session, redirect, url_for
from db import db
from bson import ObjectId
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta

admin_dashboard_bp = Blueprint("admin_dashboard", __name__)

# Collections
orders_col = db["orders"]
users_col = db["users"]
balance_logs_col = db["balance_logs"]          # audit logs to compute deposits/deductions
afa_col = db["afa_registrations"]
transactions_col = db["transactions"]          # for transaction KPIs

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


def top_offers_by_purchases(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Rank most-purchased offers across all orders by counting each order line.
    Ignores order status entirely (counts processing, completed, etc).

    Offer label resolution priority:
      1) items.value_obj.label
      2) items.value_obj.volume
      3) items.value_obj.id
      4) items.value  (stringified if needed)
      5) items.shared_bundle
      else "N/A"

    Additional formatting:
      - If the final offer label is a number:
          * >= 1000 and a clean multiple of 1000  → show as "XGB" (1000→1GB, 2000→2GB, ...)
          * 0 < number < 1000 and integer        → show as "XMB"
        Otherwise keep as-is.
    """
    pipeline: List[Dict[str, Any]] = [
        {"$unwind": "$items"},

        # Build a raw candidate for offer, handling both scalars and embedded objects
        {"$project": {
            "service": {"$ifNull": ["$items.serviceName", "Unknown"]},
            "offer_raw": {
                "$ifNull": [
                    {"$ifNull": ["$items.value_obj.label", None]},
                    {"$ifNull": [
                        "$items.value_obj.volume",
                        {"$ifNull": [
                            "$items.value_obj.id",
                            {"$ifNull": [
                                "$items.value",        # can be scalar or object
                                {"$ifNull": ["$items.shared_bundle", "N/A"]}
                            ]}
                        ]}
                    ]}
                ]
            }
        }},

        # If offer_raw is an object, try label > volume > id; else stringify
        {"$addFields": {
            "offer": {
                "$cond": [
                    {"$eq": [{"$type": "$offer_raw"}, "object"]},
                    {"$ifNull": [
                        "$offer_raw.label",
                        {"$ifNull": [
                            "$offer_raw.volume",
                            {"$ifNull": ["$offer_raw.id", "N/A"]}
                        ]}
                    ]},
                    {"$toString": "$offer_raw"}
                ]
            }
        }},

        # Normalize again in case we still have an object
        {"$addFields": {
            "offer": {
                "$cond": [
                    {"$eq": [{"$type": "$offer"}, "object"]},
                    {"$toString": "$offer"},
                    "$offer"
                ]
            }
        }},

        # Try to parse offer as a number (double). If not numeric, offer_num will be null.
        {"$addFields": {
            "offer_num": {
                "$convert": {"input": "$offer", "to": "double", "onError": None, "onNull": None}
            }
        }},

        # Format MB/GB:
        # - if offer_num is not null AND >= 1000 AND divisible by 1000 -> "<offer_num/1000>GB" (integer GB)
        # - elif offer_num is not null AND 0 < offer_num < 1000 and integer -> "<offer_num>MB"
        # - else keep original "offer"
        {"$addFields": {
            "offer_fmt": {
                "$cond": [
                    {"$ne": ["$offer_num", None]},
                    {
                        "$cond": [
                            {"$and": [
                                {"$gte": ["$offer_num", 1000]},
                                {"$eq": [{"$mod": ["$offer_num", 1000]}, 0]}
                            ]},
                            {
                                "$concat": [
                                    {"$toString": {
                                        "$toInt": {"$divide": ["$offer_num", 1000]}
                                    }},
                                    "GB"
                                ]
                            },
                            {
                                "$cond": [
                                    {"$and": [
                                        {"$gt": ["$offer_num", 0]},
                                        {"$lt": ["$offer_num", 1000]},
                                        {"$eq": [{"$mod": ["$offer_num", 1]}, 0]}
                                    ]},
                                    {"$concat": [
                                        {"$toString": {"$toInt": "$offer_num"}},
                                        "MB"
                                    ]},
                                    "$offer"
                                ]
                            }
                        ]
                    },
                    "$offer"
                ]
            }
        }},

        {"$group": {"_id": {"service": "$service", "offer": "$offer_fmt"}, "count": {"$sum": 1}}},
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
    Returns overall + today's totals (UTC).
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

    deposits_overall = _sum([
        {"$match": {"action": "deposit"}},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$delta", 0]}}}},
    ])

    withdrawals_overall = _sum([
        {"$match": {"action": "withdraw"}},
        {"$group": {"_id": None, "total": {"$sum": {"$abs": {"$ifNull": ["$delta", 0]}}}}},  # abs of negative deltas
    ])

    deposits_today = _sum([
        {"$match": {"action": "deposit", "created_at": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$delta", 0]}}}},
    ])

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
    Purchase-only KPIs:
      - txn_total_count  (all-time)
      - txn_today_count
      - txn_total_amount (all-time)
      - txn_today_amount
    Uses verified_at as timestamp.
    """
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)

    base_match = {"status": "success", "type": "purchase"}

    try:
        txn_total_count = transactions_col.count_documents(base_match)
    except Exception:
        txn_total_count = 0

    try:
        total_sum_doc = next(transactions_col.aggregate([
            {"$match": base_match},
            {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$amount", 0]}}}}
        ]), None)
        txn_total_amount = float((total_sum_doc or {}).get("total", 0) or 0)
    except Exception:
        txn_total_amount = 0.0

    try:
        txn_today_count = transactions_col.count_documents({
            **base_match,
            "verified_at": {"$gte": start, "$lt": end}
        })
    except Exception:
        txn_today_count = 0

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


# ===== NEW: Profit by day (today + last 5 full days) ==========================

def _day_range(d: datetime.date):
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end

def compute_daily_profits(days_back: int = 6) -> Dict[str, Any]:
    """
    Returns:
      labels:   ["Oct 10", ..., "Today"]
      values:   [profit_per_day...]
      today_profit
      yesterday_profit
      change_pct      (float, +/-)
      trend           ("up" | "down" | "flat")
      statement       (string for UI)
    Uses orders.profit_amount_total summed by created_at day.
    """
    # Build date buckets: last (days_back) days ending today (inclusive of today)
    today = datetime.utcnow().date()
    days = [today - timedelta(days=i) for i in range(days_back)][::-1]  # chronological

    # Query just once for the window
    window_start, _ = _day_range(days[0])
    _, window_end = _day_range(days[-1])  # end of today
    pipeline = [
        {"$match": {"created_at": {"$gte": window_start, "$lt": window_end}}},
        {"$project": {
            "d": {"$dateTrunc": {"date": "$created_at", "unit": "day"}},
            "p": {"$ifNull": ["$profit_amount_total", 0]}
        }},
        {"$group": {"_id": "$d", "profit": {"$sum": "$p"}}}
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    # Map aggregation by date (date-only)
    by_day = {}
    for row in agg:
        dt = row.get("_id")
        if isinstance(dt, datetime):
            by_day[dt.date()] = float(row.get("profit", 0) or 0)

    # Build aligned series
    labels, values = [], []
    for d in days:
        labels.append("Today" if d == today else d.strftime("%b %d"))
        values.append(round(by_day.get(d, 0.0), 2))

    today_profit = values[-1] if values else 0.0
    yesterday_profit = values[-2] if len(values) >= 2 else 0.0

    # Compute change %
    if yesterday_profit == 0:
        change_pct = 100.0 if today_profit > 0 else 0.0
    else:
        change_pct = ((today_profit - yesterday_profit) / abs(yesterday_profit)) * 100.0

    # Trend + human statement
    if abs(today_profit - yesterday_profit) < 1e-9:
        trend = "flat"
        statement = "Today’s profit is the same as yesterday."
    elif today_profit > yesterday_profit:
        trend = "up"
        diff = round(today_profit - yesterday_profit, 2)
        pct = round(change_pct, 2)
        statement = f"Today’s profit has risen by {pct}% compared to yesterday (up GHS {diff:,.2f})."
    else:
        trend = "down"
        diff = round(yesterday_profit - today_profit, 2)
        pct = round(abs(change_pct), 2)
        statement = f"Today’s profit has fallen by {pct}% compared to yesterday (down GHS {diff:,.2f})."

    return {
        "labels": labels,
        "values": values,
        "today_profit": round(today_profit, 2),
        "yesterday_profit": round(yesterday_profit, 2),
        "change_pct": round(change_pct, 2),
        "trend": trend,
        "statement": statement,
    }

# ----------------------------
# Route
# ----------------------------

@admin_dashboard_bp.route("/admin/dashboard")
def admin_dashboard():
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

    # NEW: daily profits (today + previous 5)
    dp = compute_daily_profits(days_back=6)

    # Top customers (orders & profit)
    chart_labels, chart_values = top_customers_by_orders(limit=10)
    profit_chart_labels, profit_chart_values = top_customers_by_profit(limit=10)

    # Top offers (service + offer) — use ALL orders, no status filter, Top 10
    top_offers = top_offers_by_purchases(limit=10)

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

        # NEW: Profit trend + last 5 days (plus today)
        today_profit=dp["today_profit"],
        yesterday_profit=dp["yesterday_profit"],
        profit_change_pct=dp["change_pct"],
        profit_trend=dp["trend"],
        profit_statement=dp["statement"],
        daily_profit_labels=dp["labels"],
        daily_profit_values=dp["values"],

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
