# admin_orders.py
from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId, Regex
from db import db
from datetime import datetime
from urllib.parse import urlencode

admin_orders_bp = Blueprint("admin_orders", __name__)
orders_col = db["orders"]
users_col = db["users"]

ALLOWED_STATUSES = {"pending", "processing", "completed", "failed"}
ALLOWED_SORTS = {"newest", "oldest", "amount_desc", "amount_asc"}
DEFAULT_PER_PAGE = 10


def _parse_date(dstr):
    """Parse YYYY-MM-DD to a UTC datetime (start of day)."""
    if not dstr:
        return None
    try:
        return datetime.strptime(dstr, "%Y-%m-%d")
    except Exception:
        return None


def _build_preserved_query(args, exclude=("page",)):
    """Keep all current filters in pagination links."""
    kept = {k: v for k, v in args.items() if k not in exclude and v not in (None, "", "None")}
    return urlencode(kept)


@admin_orders_bp.route("/admin/orders")
def admin_view_orders():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # ------------- Incoming filters -------------
    status_filter = request.args.get("status", "").strip().lower()
    order_id_q = request.args.get("order_id", "").strip()          # paste Order ID (partial ok)
    customer_q = request.args.get("customer", "").strip()           # name/email/phone
    paid_from = request.args.get("paid_from", "").strip().lower()   # wallet, card, momo, etc.

    # totals
    min_total = request.args.get("min_total", "").strip()
    max_total = request.args.get("max_total", "").strip()

    # dates
    date_from = _parse_date(request.args.get("date_from", "").strip())
    date_to_raw = _parse_date(request.args.get("date_to", "").strip())
    # If date_to provided, include the whole day by advancing 1 day and using < next_day
    date_to = None
    if date_to_raw:
        date_to = datetime(date_to_raw.year, date_to_raw.month, date_to_raw.day)  # normalize
        # inclusive end-of-day via exclusive next-day at 00:00
        from datetime import timedelta
        date_to = date_to + timedelta(days=1)

    # sort / pagination
    sort = request.args.get("sort", "newest").strip().lower()
    if sort not in ALLOWED_SORTS:
        sort = "newest"

    try:
        per_page = int(request.args.get("per_page", DEFAULT_PER_PAGE))
        per_page = max(1, min(per_page, 100))
    except Exception:
        per_page = DEFAULT_PER_PAGE

    try:
        page = int(request.args.get("page", 1))
        page = max(1, page)
    except Exception:
        page = 1

    skip = (page - 1) * per_page

    # ------------- Build Mongo query -------------
    query = {}

    # Status
    if status_filter and status_filter in ALLOWED_STATUSES:
        query["status"] = status_filter

    # Paid from
    if paid_from:
        query["paid_from"] = paid_from

    # Order ID (case-insensitive regex, fast prefix/substring)
    if order_id_q:
        query["order_id"] = Regex(order_id_q, "i")

    # Date range
    if date_from or date_to:
        dt_cond = {}
        if date_from:
            dt_cond["$gte"] = date_from
        if date_to:
            dt_cond["$lt"] = date_to
        query["created_at"] = dt_cond

    # Total amount range
    amt_cond = {}
    try:
        if min_total != "":
            amt_cond["$gte"] = float(min_total)
    except Exception:
        pass
    try:
        if max_total != "":
            amt_cond["$lte"] = float(max_total)
    except Exception:
        pass
    if amt_cond:
        # Your schema uses total_amount as float/Decimal128; query works for both
        query["total_amount"] = amt_cond

    # Customer search -> resolve user_ids first
    user_ids = None
    if customer_q:
        rx = Regex(customer_q, "i")
        # Match on first_name, last_name, email, phone; extend as needed
        users_cursor = users_col.find(
            {
                "$or": [
                    {"first_name": rx},
                    {"last_name": rx},
                    {"email": rx},
                    {"phone": rx},
                    {"username": rx},
                ]
            },
            {"_id": 1},
        )
        user_ids = [u["_id"] for u in users_cursor]
        # If no matching users and customer_q present => no orders
        if not user_ids:
            total_orders = 0
            return render_template(
                "admin_orders.html",
                orders=[],
                page=1,
                total_pages=1,
                status_filter=status_filter,
                # extras for your template to optionally use:
                sort=sort,
                per_page=per_page,
                filters_query=_build_preserved_query(request.args),
            )
        query["user_id"] = {"$in": user_ids}

    # ------------- Sorting -------------
    sort_spec = [("created_at", -1)]  # default newest
    if sort == "oldest":
        sort_spec = [("created_at", 1)]
    elif sort == "amount_desc":
        sort_spec = [("total_amount", -1), ("created_at", -1)]
    elif sort == "amount_asc":
        sort_spec = [("total_amount", 1), ("created_at", -1)]

    # ------------- Query + pagination -------------
    try:
        total_orders = orders_col.count_documents(query)
        total_pages = max(1, (total_orders + per_page - 1) // per_page)

        orders = list(
            orders_col.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(per_page)
        )

        # Attach user doc for display
        for order in orders:
            uid = order.get("user_id")
            if isinstance(uid, str):
                try:
                    uid = ObjectId(uid)
                except Exception:
                    uid = None
            user = users_col.find_one({"_id": uid}) if uid else None
            order["user"] = user or {}

    except Exception as e:
        flash("Error loading orders.", "danger")
        orders = []
        total_pages = 1

    return render_template(
        "admin_orders.html",
        orders=orders,
        page=page,
        total_pages=total_pages,
        status_filter=status_filter,  # backward compat with your current template
        # Expose all filters so you can render them in the UI
        order_id_q=order_id_q,
        customer_q=customer_q,
        paid_from=paid_from,
        min_total=min_total,
        max_total=max_total,
        date_from=request.args.get("date_from", ""),
        date_to=request.args.get("date_to", ""),
        sort=sort,
        per_page=per_page,
        total_orders=total_orders,
        filters_query=_build_preserved_query(request.args),
    )


@admin_orders_bp.route("/admin/orders/<order_id>/update", methods=["POST"])
def update_order_status(order_id):
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ALLOWED_STATUSES:
        flash("Invalid status.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    try:
        res = orders_col.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"status": new_status, "updated_at": datetime.utcnow()}},
        )
        if res.modified_count:
            flash("✅ Order status updated successfully.", "success")
        else:
            flash("⚠️ No change was made to the order.", "warning")
    except Exception:
        flash("❌ Error updating order status.", "danger")

    # Preserve current filters when redirecting back
    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)
