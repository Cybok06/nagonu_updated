from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from db import db
from datetime import datetime, timedelta
import re

admin_transactions_bp = Blueprint("admin_transactions", __name__)

transactions_col = db["transactions"]
users_col = db["users"]
orders_col = db["orders"]


@admin_transactions_bp.route("/admin/transactions")
def admin_view_transactions():
    # Auth
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    search_term = (request.args.get("search") or request.args.get("customer") or "").strip()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    range_preset = (request.args.get("range") or "").strip().lower()
    gateway = (request.args.get("gateway") or "").strip().lower()

    # pagination
    try:
        page = int(request.args.get("page", 1))
    except Exception:
        page = 1
    page = max(page, 1)

    per_page = 20

    filters = []

    # Flexible search: transaction type, customer identity, order ID, or reference.
    if search_term:
        pattern = {"$regex": re.escape(search_term), "$options": "i"}
        customer_tokens = [token for token in re.split(r"\s+", search_term) if token]
        customer_filters = []
        for token in customer_tokens:
            token_pattern = {"$regex": re.escape(token), "$options": "i"}
            customer_filters.append(
                {
                    "$or": [
                        {"first_name": token_pattern},
                        {"last_name": token_pattern},
                        {"business_name": token_pattern},
                        {"username": token_pattern},
                        {"phone": token_pattern},
                    ]
                }
            )
        matching_user_ids = [
            user["_id"]
            for user in users_col.find(
                {"role": "customer", "$and": customer_filters},
                {"_id": 1},
            )
        ] if customer_filters else []

        # Store checkout transactions use the Paystack reference, while the
        # human-facing Nagonu order number lives in orders. Resolve matching
        # orders first so either value can find the same transaction.
        linked_references = set()
        matching_orders = list(
            orders_col.find(
                {
                    "$or": [
                        {"order_id": pattern},
                        {"paystack_reference": pattern},
                        {"api_reference_id": pattern},
                        {"reference": pattern},
                        {"items.provider_reference": pattern},
                        {"items.provider_order_id": pattern},
                        {"items.provider_request_order_id": pattern},
                    ]
                },
                {"order_id": 1, "paystack_reference": 1, "api_reference_id": 1, "reference": 1},
            ).limit(250)
        )
        for order in matching_orders:
            for field in ("order_id", "paystack_reference", "api_reference_id", "reference"):
                value = str(order.get(field) or "").strip()
                if value:
                    linked_references.add(value)

        search_options = [
            {"type": pattern},
            {"transaction_type": pattern},
            {"source": pattern},
            {"gateway": pattern},
            {"order_id": pattern},
            {"reference": pattern},
            {"api_reference_id": pattern},
            {"paystack_reference": pattern},
            {"provider_reference": pattern},
            {"transaction_id": pattern},
            {"meta.order_id": pattern},
            {"meta.reference": pattern},
            {"meta.paystack_reference": pattern},
            {"metadata.order_id": pattern},
            {"metadata.reference": pattern},
            {"raw.reference": pattern},
            {"raw.data.reference": pattern},
        ]
        if linked_references:
            linked = list(linked_references)
            search_options.extend(
                [
                    {"reference": {"$in": linked}},
                    {"order_id": {"$in": linked}},
                    {"paystack_reference": {"$in": linked}},
                    {"api_reference_id": {"$in": linked}},
                ]
            )
        if matching_user_ids:
            search_options.append({"user_id": {"$in": matching_user_ids}})
        filters.append({"$or": search_options})

    # Date range filter (verified_at)
    verified_filter = {}
    now = datetime.utcnow()
    start_dt = None
    end_dt = None

    if range_preset in ("today", "yesterday", "last7"):
        today = datetime(now.year, now.month, now.day)
        if range_preset == "today":
            start_dt = today
            end_dt = today + timedelta(days=1)
        elif range_preset == "yesterday":
            start_dt = today - timedelta(days=1)
            end_dt = today
        else:
            start_dt = today - timedelta(days=6)
            end_dt = today + timedelta(days=1)
    else:
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            except Exception:
                flash("Invalid start date.", "warning")
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            except Exception:
                flash("Invalid end date.", "warning")

    if start_dt:
        verified_filter["$gte"] = start_dt
    if end_dt:
        verified_filter["$lt"] = end_dt
    if verified_filter:
        filters.append({"verified_at": verified_filter})

    if gateway:
        filters.append(
            {
                "$or": [
                    {"gateway": gateway},
                    {"source": gateway},
                ]
            }
        )

    query = {"$and": filters} if filters else {}

    # Count
    total_txns = transactions_col.count_documents(query)
    total_pages = max((total_txns + per_page - 1) // per_page, 1)

    # Clamp page to range (prevents dead pages when filters reduce results)
    if page > total_pages:
        page = total_pages

    skip = (page - 1) * per_page

    # Fetch transactions
    transactions = list(
        transactions_col.find(query)
        .sort("verified_at", -1)
        .skip(skip)
        .limit(per_page)
    )

    gateways_raw = transactions_col.distinct("gateway")
    sources_raw = transactions_col.distinct("source")
    gateways = sorted({g for g in (gateways_raw + sources_raw) if g})

    # Attach user info efficiently
    user_ids = [t.get("user_id") for t in transactions if t.get("user_id")]
    users_map = {}
    if user_ids:
        for u in users_col.find({"_id": {"$in": list(set(user_ids))}}):
            users_map[u["_id"]] = u

    for txn in transactions:
        txn["user"] = users_map.get(txn.get("user_id"), {}) or {}

    # Resolve an order number for display without issuing one query per row.
    txn_references = {
        str(value).strip()
        for txn in transactions
        for value in (
            txn.get("reference"),
            txn.get("order_id"),
            txn.get("paystack_reference"),
            (txn.get("meta") or {}).get("order_id") if isinstance(txn.get("meta"), dict) else None,
        )
        if value not in (None, "") and str(value).strip()
    }
    order_by_reference = {}
    if txn_references:
        for order in orders_col.find(
            {
                "$or": [
                    {"order_id": {"$in": list(txn_references)}},
                    {"paystack_reference": {"$in": list(txn_references)}},
                    {"api_reference_id": {"$in": list(txn_references)}},
                    {"reference": {"$in": list(txn_references)}},
                ]
            },
            {"order_id": 1, "paystack_reference": 1, "api_reference_id": 1, "reference": 1},
        ):
            order_number = str(order.get("order_id") or "").strip()
            for field in ("order_id", "paystack_reference", "api_reference_id", "reference"):
                value = str(order.get(field) or "").strip()
                if value:
                    order_by_reference[value] = order_number

    for txn in transactions:
        direct_order_id = str(txn.get("order_id") or "").strip()
        meta = txn.get("meta") if isinstance(txn.get("meta"), dict) else {}
        candidates = [
            direct_order_id,
            str(meta.get("order_id") or "").strip(),
            str(txn.get("reference") or "").strip(),
            str(txn.get("paystack_reference") or "").strip(),
        ]
        txn["resolved_order_id"] = direct_order_id or next(
            (order_by_reference[value] for value in candidates if value and order_by_reference.get(value)),
            "",
        )

    return render_template(
        "admin_transactions.html",
        transactions=transactions,
        search_term=search_term,
        start_date=start_date,
        end_date=end_date,
        selected_gateway=gateway,
        range_preset=range_preset,
        gateways=gateways,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )
