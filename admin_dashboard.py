from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify
from db import db
from bson import ObjectId
from typing import Dict, Any, List, Tuple, Optional, Union
from datetime import datetime, timedelta
from withdraw_requests import update_withdraw_request_status

admin_dashboard_bp = Blueprint("admin_dashboard", __name__)

# Collections
orders_col = db["orders"]
users_col = db["users"]
balance_logs_col = db["balance_logs"]          # audit logs to compute deposits/deductions
balances_col = db["balances"]                  # for USER ACCOUNT BALANCE total
afa_col = db["afa_registrations"]
transactions_col = db["transactions"]          # for transaction KPIs

# ✅ Store withdrawal requests collection
store_withdraw_requests_col = db["store_withdraw_requests"]
store_accounts_col = db["store_accounts"]


# ----------------------------
# Helpers
# ----------------------------

def _users_display_map(user_ids: List[ObjectId]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not user_ids:
        return out
    try:
        for u in users_col.find({"_id": {"$in": user_ids}}, {"username": 1, "name": 1, "phone": 1}):
            disp = (u.get("username") or u.get("name") or u.get("phone") or "").strip()
            if not disp:
                disp = f"User {str(u['_id'])[:6].upper()}"
            out[str(u["_id"])] = disp
    except Exception:
        pass
    return out


def top_customers_by_orders(limit: int = 10) -> Tuple[List[str], List[int]]:
    pipeline = [
        {"$match": {"user_id": {"$ne": None}}},
        {"$group": {"_id": "$user_id", "order_count": {"$sum": 1}}},
        {"$sort": {"order_count": -1}},
        {"$limit": int(limit)},
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    obj_ids = [oid for oid in (doc.get("_id") for doc in agg) if isinstance(oid, ObjectId)]
    users_map = _users_display_map(obj_ids)

    labels: List[str] = []
    values: List[int] = []
    for doc in agg:
        uid = doc.get("_id")
        count = int(doc.get("order_count", 0) or 0)
        if isinstance(uid, ObjectId):
            label = users_map.get(str(uid), f"User {str(uid)[:6].upper()}")
        else:
            label = "Unknown"
        labels.append(label)
        values.append(count)
    return labels, values


def top_customers_by_profit(limit: int = 10) -> Tuple[List[str], List[float]]:
    pipeline = [
        {"$match": {"user_id": {"$ne": None}}},
        {"$group": {
            "_id": "$user_id",
            "profit_sum": {"$sum": {"$convert": {"input": "$profit_amount_total", "to": "double", "onError": 0, "onNull": 0}}}
        }},
        {"$sort": {"profit_sum": -1}},
        {"$limit": int(limit)},
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    obj_ids = [oid for oid in (doc.get("_id") for doc in agg) if isinstance(oid, ObjectId)]
    users_map = _users_display_map(obj_ids)

    labels: List[str] = []
    values: List[float] = []
    for doc in agg:
        uid = doc.get("_id")
        profit = float(doc.get("profit_sum", 0) or 0)
        if isinstance(uid, ObjectId):
            label = users_map.get(str(uid), f"User {str(uid)[:6].upper()}")
        else:
            label = "Unknown"
        labels.append(label)
        values.append(profit)
    return labels, values


# ✅ FIXED FOREVER: Top offers purchased (safe pipeline; no bracket chaos)
def top_offers_by_purchases(limit: int = 10) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = [
        {"$unwind": "$items"},

        {"$addFields": {
            "service": {"$ifNull": ["$items.serviceName", "Unknown"]},
            "offer_label": {"$ifNull": ["$items.value_obj.label", None]},
            "offer_volume": {"$ifNull": ["$items.value_obj.volume", None]},
            "offer_id": {"$ifNull": ["$items.value_obj.id", None]},
            "offer_value": {"$ifNull": ["$items.value", None]},
            "offer_bundle": {"$ifNull": ["$items.shared_bundle", None]},
        }},

        {"$addFields": {
            "offer_raw": {
                "$ifNull": [
                    {"$cond": [{"$and": [{"$ne": ["$offer_label", None]}, {"$ne": ["$offer_label", ""]}]}, "$offer_label", None]},
                    {"$ifNull": [
                        {"$cond": [{"$and": [{"$ne": ["$offer_volume", None]}, {"$ne": ["$offer_volume", ""]}]}, "$offer_volume", None]},
                        {"$ifNull": [
                            {"$cond": [{"$and": [{"$ne": ["$offer_id", None]}, {"$ne": ["$offer_id", ""]}]}, "$offer_id", None]},
                            {"$ifNull": [
                                {"$cond": [{"$and": [{"$ne": ["$offer_value", None]}, {"$ne": ["$offer_value", ""]}]}, "$offer_value", None]},
                                {"$ifNull": ["$offer_bundle", "N/A"]}
                            ]}
                        ]}
                    ]}
                ]
            }
        }},

        {"$addFields": {"offer": {"$toString": "$offer_raw"}}},

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
            "service": (_id.get("service") or "Unknown") or "Unknown",
            "offer": (_id.get("offer") or "N/A"),
            "count": int(doc.get("count", 0) or 0),
        })
    return results


def compute_totals() -> Dict[str, float]:
    pipeline = [{
        "$group": {
            "_id": None,
            "sum_total_amount": {"$sum": {"$convert": {"input": "$total_amount", "to": "double", "onError": 0, "onNull": 0}}},
            "sum_charged_amount": {"$sum": {"$convert": {"input": "$charged_amount", "to": "double", "onError": 0, "onNull": 0}}},
            "sum_profit_amount": {"$sum": {"$convert": {"input": "$profit_amount_total", "to": "double", "onError": 0, "onNull": 0}}},
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
    try:
        total_customers = users_col.count_documents({"role": "customer"})
        blocked_customers = users_col.count_documents({"role": "customer", "status": "blocked"})
        active_customers = users_col.count_documents({
            "role": "customer",
            "$or": [{"status": {"$exists": False}}, {"status": {"$ne": "blocked"}}]
        })
    except Exception:
        total_customers = blocked_customers = active_customers = 0
    return {
        "total_customers": int(total_customers),
        "blocked_customers": int(blocked_customers),
        "active_customers": int(active_customers),
    }


def compute_balance_flow_totals() -> Dict[str, float]:
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
        {"$group": {"_id": None, "total": {"$sum": {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}}}}}
    ])

    withdrawals_overall = _sum([
        {"$match": {"action": "withdraw"}},
        {"$group": {"_id": None, "total": {"$sum": {"$abs": {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}}}}}}
    ])

    deposits_today = _sum([
        {"$match": {"action": "deposit", "created_at": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": None, "total": {"$sum": {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}}}}}
    ])

    withdrawals_today = _sum([
        {"$match": {"action": "withdraw", "created_at": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": None, "total": {"$sum": {"$abs": {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}}}}}}
    ])

    return {
        "deposits_overall": deposits_overall,
        "withdrawals_overall": withdrawals_overall,
        "deposits_today": deposits_today,
        "withdrawals_today": withdrawals_today,
    }


def compute_transaction_kpis() -> Dict[str, float]:
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)

    base_match = {
        "status": "success",
        "$or": [
            {"type": "purchase"},
            {"source": "paystack_inline"},
            {"type": "debit"},
        ],
    }

    try:
        txn_total_count = transactions_col.count_documents(base_match)
    except Exception:
        txn_total_count = 0

    try:
        total_sum_doc = next(transactions_col.aggregate([
            {"$match": base_match},
            {"$group": {"_id": None, "total": {"$sum": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}}}
        ]), None)
        txn_total_amount = float((total_sum_doc or {}).get("total", 0) or 0)
    except Exception:
        txn_total_amount = 0.0

    try:
        txn_today_count = transactions_col.count_documents({**base_match, "verified_at": {"$gte": start, "$lt": end}})
    except Exception:
        txn_today_count = 0

    try:
        today_sum_doc = next(transactions_col.aggregate([
            {"$match": {**base_match, "verified_at": {"$gte": start, "$lt": end}}},
            {"$group": {"_id": None, "total": {"$sum": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}}}
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


def compute_user_balances_summary() -> Dict[str, Union[float, int]]:
    try:
        doc = next(balances_col.aggregate([
            {"$group": {
                "_id": None,
                "total_balance_amount": {"$sum": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}},
                "doc_count": {"$sum": 1},
                "positive_count": {"$sum": {"$cond": [
                    {"$gt": [{"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}, 0]}, 1, 0
                ]}}
            }}
        ]), None)
    except Exception:
        doc = None
    return {
        "total_balance_amount": float((doc or {}).get("total_balance_amount", 0) or 0.0),
        "balance_doc_count": int((doc or {}).get("doc_count", 0) or 0),
        "positive_balance_count": int((doc or {}).get("positive_count", 0) or 0),
    }

def compute_store_accounts_outstanding() -> float:
    try:
        doc = next(store_accounts_col.aggregate([
            {"$group": {
                "_id": None,
                "total": {"$sum": {"$convert": {"input": "$total_profit_balance", "to": "double", "onError": 0, "onNull": 0}}}
            }}
        ]), None)
    except Exception:
        doc = None
    return float((doc or {}).get("total", 0) or 0.0)


def _day_range(d: datetime.date):
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end


def compute_daily_profits(days_back: int = 6) -> Dict[str, Any]:
    today = datetime.utcnow().date()
    days = [today - timedelta(days=i) for i in range(days_back)][::-1]
    if not days:
        return {
            "labels": [],
            "values": [],
            "today_profit": 0.0,
            "yesterday_profit": 0.0,
            "change_pct": 0.0,
            "trend": "flat",
            "statement": "No data."
        }

    window_start, _ = _day_range(days[0])
    _, window_end = _day_range(days[-1])

    pipeline = [
        {"$match": {"created_at": {"$gte": window_start, "$lt": window_end}}},
        {"$project": {
            "d": {"$dateTrunc": {"date": "$created_at", "unit": "day"}},
            "p": {"$ifNull": ["$profit_amount_total", 0]}
        }},
        {"$group": {"_id": "$d", "profit": {"$sum": {"$convert": {"input": "$p", "to": "double", "onError": 0, "onNull": 0}}}}}
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    by_day: Dict[Any, float] = {}
    for row in agg:
        dt = row.get("_id")
        if isinstance(dt, datetime):
            by_day[dt.date()] = float(row.get("profit", 0) or 0)

    labels: List[str] = []
    values: List[float] = []
    for d in days:
        labels.append("Today" if d == today else d.strftime("%b %d"))
        values.append(round(by_day.get(d, 0.0), 2))

    today_profit = values[-1] if values else 0.0
    yesterday_profit = values[-2] if len(values) >= 2 else 0.0

    if yesterday_profit == 0:
        change_pct = 100.0 if today_profit > 0 else 0.0
    else:
        change_pct = ((today_profit - yesterday_profit) / abs(yesterday_profit)) * 100.0

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


def _display_for_actor(actor_id: str, users_map: Dict[str, str], source: str) -> str:
    label = None
    try:
        oid = ObjectId(actor_id)
        label = users_map.get(str(oid))
    except Exception:
        pass
    if not label:
        prefix = "Agent" if source == "agent" else "Customer"
        label = f"{prefix} {actor_id[:6].upper()}"
    return label


def agents_cumulative_sales(limit: int = 10) -> Tuple[List[str], List[float], List[Dict[str, Any]]]:
    pipeline: List[Dict[str, Any]] = [
        {"$unwind": "$items"},
        {"$addFields": {
            "amount_num": {"$convert": {"input": {"$ifNull": ["$items.amount", 0]}, "to": "double", "onError": 0, "onNull": 0}},
            "agent1": {"$ifNull": ["$items.agent_id", None]},
            "agent2": {"$ifNull": ["$items.agentId", None]},
            "agent3": {"$ifNull": ["$items.value_obj.agent_id", None]},
            "agent4": {"$ifNull": ["$items.value_obj.agentId", None]},
        }},
        {"$addFields": {
            "agent_coalesced": {
                "$let": {
                    "vars": {"a1": "$agent1", "a2": "$agent2", "a3": "$agent3", "a4": "$agent4"},
                    "in": {"$ifNull": [
                        {"$cond": [{"$ne": ["$$a1", ""]}, "$$a1", None]},
                        {"$ifNull": [
                            {"$cond": [{"$ne": ["$$a2", ""]}, "$$a2", None]},
                            {"$ifNull": [
                                {"$cond": [{"$ne": ["$$a3", ""]}, "$$a3", None]},
                                {"$cond": [{"$ne": ["$$a4", ""]}, "$$a4", None]}
                            ]}
                        ]}
                    ]}
                }
            }
        }},
        {"$addFields": {
            "actor_id": {"$toString": {"$ifNull": ["$agent_coalesced", "$user_id"]}},
            "actor_source": {"$cond": [{"$ne": ["$agent_coalesced", None]}, "agent", "customer"]}
        }},
        {"$match": {"amount_num": {"$gt": 0}}},
        {"$group": {
            "_id": {"actor_id": "$actor_id", "actor_source": "$actor_source"},
            "total_sales": {"$sum": "$amount_num"},
            "line_count": {"$sum": 1}
        }},
        {"$sort": {"total_sales": -1}},
        {"$limit": int(limit)},
    ]

    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    to_resolve: List[ObjectId] = []
    for doc in agg:
        actor_id = (doc.get("_id") or {}).get("actor_id")
        try:
            to_resolve.append(ObjectId(actor_id))
        except Exception:
            pass
    users_map = _users_display_map(to_resolve)

    labels: List[str] = []
    values: List[float] = []
    table_rows: List[Dict[str, Any]] = []

    for doc in agg:
        _id = doc.get("_id") or {}
        actor_id = str(_id.get("actor_id"))
        actor_source = _id.get("actor_source")
        total_sales = float(doc.get("total_sales", 0) or 0)
        line_count = int(doc.get("line_count", 0) or 0)

        label = _display_for_actor(actor_id, users_map, actor_source)

        labels.append(label)
        values.append(round(total_sales, 2))
        table_rows.append({
            "agent_id": actor_id,
            "agent": label if actor_source == "agent" else f"{label} (Customer)",
            "sales": round(total_sales, 2),
            "lines": line_count
        })

    return labels, values, table_rows


# ✅ Withdrawal Requests KPI counters
def compute_withdraw_requests_pending() -> int:
    try:
        return int(store_withdraw_requests_col.count_documents({"status": "pending"}))
    except Exception:
        return 0


def compute_withdraw_requests_total_open() -> int:
    # “open” = pending or processing
    try:
        return int(store_withdraw_requests_col.count_documents({"status": {"$in": ["pending", "processing"]}}))
    except Exception:
        return 0


# ----------------------------
# API for modal (dashboard will call these)
# ----------------------------

@admin_dashboard_bp.route("/admin/withdrawals/list")
def admin_withdrawals_list():
    if not session.get("admin_logged_in"):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    # return latest 50
    try:
        docs = list(store_withdraw_requests_col.find({}, sort=[("created_at", -1)], limit=50))
    except Exception:
        docs = []

    def _safe_str(x):
        try:
            return str(x)
        except Exception:
            return ""

    out: List[Dict[str, Any]] = []
    for d in docs:
        out.append({
            "_id": _safe_str(d.get("_id")),
            "status": (d.get("status") or "pending"),
            "amount": d.get("amount", 0),
            "currency": d.get("currency", "GHS"),
            "owner_id": _safe_str(d.get("owner_id") or d.get("user_id") or ""),
            "store_slug": d.get("store_slug") or d.get("store") or "",
            "method": d.get("method") or d.get("payout_method") or d.get("type") or "",
            "account": d.get("account") or d.get("msisdn") or d.get("wallet") or "",
            "network": d.get("network") or "",
            "recipient_name": d.get("recipient_name") or "",
            "created_at": (d.get("created_at").isoformat() if isinstance(d.get("created_at"), datetime) else ""),
        })
    return jsonify({"ok": True, "items": out})


@admin_dashboard_bp.route("/admin/withdrawals/update", methods=["POST"])
def admin_withdrawals_update():
    if not session.get("admin_logged_in"):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    req_id = (data.get("id") or "").strip()
    new_status = (data.get("status") or "").strip().lower()
    note = (data.get("note") or "").strip()

    ok, payload, code = update_withdraw_request_status(
        req_id=req_id,
        new_status=new_status,
        actor_id=session.get("admin_id") or session.get("user_id") or "admin",
        note=note,
    )
    if ok:
        return jsonify({"ok": True, **payload}), code
    return jsonify({"ok": False, "error": payload.get("message")}), code


# ----------------------------
# Dashboard Route
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

    # Total amount at USER ACCOUNT BALANCE
    bal_summary = compute_user_balances_summary()
    total_user_balance_amount = float(bal_summary["total_balance_amount"])
    balance_doc_count = int(bal_summary["balance_doc_count"])
    positive_balance_count = int(bal_summary["positive_balance_count"])

    # Outstanding payouts across all store accounts
    outstanding_payouts = compute_store_accounts_outstanding()

    # Daily profits (today + previous 5)
    dp = compute_daily_profits(days_back=6)

    # Top customers (orders & profit)
    chart_labels, chart_values = top_customers_by_orders(limit=10)
    profit_chart_labels, profit_chart_values = top_customers_by_profit(limit=10)

    # Top offers
    top_offers = top_offers_by_purchases(limit=10)

    # Accumulative sales (agent first, fallback to customer)
    agent_sales_labels, agent_sales_values, top_agents_rows = agents_cumulative_sales(limit=10)

    # Customer counts
    cust_counts = compute_customer_counts()

    # Balance flows (overall + today)
    flow = compute_balance_flow_totals()

    # AFA registration KPIs
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)
    try:
        afa_total = afa_col.count_documents({})
        afa_pending = afa_col.count_documents({"status": "pending"})
        afa_today = afa_col.count_documents({"created_at": {"$gte": start, "$lt": end}})
    except Exception:
        afa_total = afa_pending = afa_today = 0

    # Transactions KPIs
    tx = compute_transaction_kpis()

    # ✅ Withdrawal requests KPI
    withdraw_requests_pending = compute_withdraw_requests_pending()
    withdraw_requests_open = compute_withdraw_requests_total_open()

    return render_template(
        "admin_dashboard.html",
        # KPIs
        total_orders=total_orders,
        sum_total_amount=sum_total_amount,
        sum_charged_amount=sum_charged_amount,
        sum_profit_amount=sum_profit_amount,

        # user balances KPI
        total_user_balance_amount=total_user_balance_amount,
        balance_doc_count=balance_doc_count,
        positive_balance_count=positive_balance_count,
        outstanding_payouts=outstanding_payouts,

        # ✅ withdrawal requests KPI
        withdraw_requests_pending=withdraw_requests_pending,
        withdraw_requests_open=withdraw_requests_open,

        # Profit trend + last 5 days (plus today)
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

        # Accumulative sales (chart + table)
        agent_sales_labels=agent_sales_labels,
        agent_sales_values=agent_sales_values,
        top_agents_rows=top_agents_rows,

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

        # Transactions KPIs
        txn_total_count=tx["txn_total_count"],
        txn_today_count=tx["txn_today_count"],
        txn_total_amount=tx["txn_total_amount"],
        txn_today_amount=tx["txn_today_amount"],
    )
