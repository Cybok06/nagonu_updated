# admin_orders.py (updated)
from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId, Regex
from db import db
from datetime import datetime, timedelta
from urllib.parse import urlencode

admin_orders_bp = Blueprint("admin_orders", __name__)

orders_col = db["orders"]
users_col = db["users"]
balances_col = db["balances"]         # NEW: for refunds
transactions_col = db["transactions"]  # NEW: for refund ledger

# Keep 'pending'/'completed' for legacy reads; main live options include 'refunded'
ALLOWED_STATUSES = {"pending", "processing", "delivered", "failed", "completed", "refunded"}
ALLOWED_SORTS = {"newest", "oldest", "amount_desc", "amount_asc"}
DEFAULT_PER_PAGE = 10


def _parse_date(dstr):
    if not dstr:
        return None
    try:
        return datetime.strptime(dstr, "%Y-%m-%d")
    except Exception:
        return None


def _build_preserved_query(args, exclude=("page",)):
    kept = {k: v for k, v in args.items() if k not in exclude and v not in (None, "", "None")}
    return urlencode(kept)


def _build_query_from_params(args):
    """Central builder so list + bulk share identical filters."""
    status_filter = (args.get("status") or "").strip().lower()
    order_id_q = (args.get("order_id") or "").strip()
    customer_q = (args.get("customer") or "").strip()
    paid_from = (args.get("paid_from") or "").strip().lower()
    min_total = (args.get("min_total") or "").strip()
    max_total = (args.get("max_total") or "").strip()
    date_from = _parse_date((args.get("date_from") or "").strip())
    date_to_raw = _parse_date((args.get("date_to") or "").strip())
    date_to = datetime(date_to_raw.year, date_to_raw.month, date_to_raw.day) + timedelta(days=1) if date_to_raw else None

    # similar item filters
    item_service = (args.get("item_service") or "").strip()
    item_offer = (args.get("item_offer") or "").strip()
    item_phone = (args.get("item_phone") or "").strip()

    query = {}

    if status_filter and status_filter in ALLOWED_STATUSES:
        query["status"] = status_filter

    if paid_from:
        query["paid_from"] = paid_from

    if order_id_q:
        query["order_id"] = Regex(order_id_q, "i")

    if date_from or date_to:
        dt = {}
        if date_from:
            dt["$gte"] = date_from
        if date_to:
            dt["$lt"] = date_to
        query["created_at"] = dt

    amt = {}
    try:
        if min_total != "":
            amt["$gte"] = float(min_total)
    except Exception:
        pass
    try:
        if max_total != "":
            amt["$lte"] = float(max_total)
    except Exception:
        pass
    if amt:
        query["total_amount"] = amt

    # Customer → resolve user ids first
    if customer_q:
        rx = Regex(customer_q, "i")
        user_ids = [u["_id"] for u in users_col.find(
            {"$or": [
                {"first_name": rx}, {"last_name": rx}, {"email": rx},
                {"phone": rx}, {"username": rx},
            ]},
            {"_id": 1},
        )]
        query["user_id"] = {"$in": user_ids or []}

    # similar item filters (any match in items[])
    item_and = []
    if item_service:
        item_and.append({"items.serviceName": Regex(item_service, "i")})
    if item_offer:
        item_and.append({"items.value": Regex(item_offer, "i")})
    if item_phone:
        item_and.append({"items.phone": Regex(item_phone, "i")})
    if item_and:
        query["$and"] = (query.get("$and") or []) + item_and

    return query


@admin_orders_bp.route("/admin/orders")
def admin_view_orders():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # list controls
    sort = (request.args.get("sort") or "newest").strip().lower()
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

    # Build query from filters
    query = _build_query_from_params(request.args)

    # Sorting
    sort_spec = [("created_at", -1)]
    if sort == "oldest":
        sort_spec = [("created_at", 1)]
    elif sort == "amount_desc":
        sort_spec = [("total_amount", -1), ("created_at", -1)]
    elif sort == "amount_asc":
        sort_spec = [("total_amount", 1), ("created_at", -1)]

    try:
        total_orders = orders_col.count_documents(query)
        total_pages = max(1, (total_orders + per_page - 1) // per_page)

        orders = list(
            orders_col.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(per_page)
        )

        # attach user profile to each order
        for o in orders:
            uid = o.get("user_id")
            if isinstance(uid, str):
                try:
                    uid = ObjectId(uid)
                except Exception:
                    uid = None
            o["user"] = users_col.find_one({"_id": uid}) if uid else {}

    except Exception:
        flash("Error loading orders.", "danger")
        orders, total_pages, total_orders = [], 1, 0

    return render_template(
        "admin_orders.html",
        orders=orders,
        page=page, total_pages=total_pages,
        total_orders=total_orders,

        # echo filters
        status_filter=(request.args.get("status") or "").strip().lower(),
        order_id_q=(request.args.get("order_id") or "").strip(),
        customer_q=(request.args.get("customer") or "").strip(),
        paid_from=(request.args.get("paid_from") or "").strip().lower(),
        min_total=(request.args.get("min_total") or "").strip(),
        max_total=(request.args.get("max_total") or "").strip(),
        date_from=(request.args.get("date_from") or "").strip(),
        date_to=(request.args.get("date_to") or "").strip(),
        sort=sort, per_page=per_page,

        # item filters
        item_service=(request.args.get("item_service") or "").strip(),
        item_offer=(request.args.get("item_offer") or "").strip(),
        item_phone=(request.args.get("item_phone") or "").strip(),

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

    # fetch order
    try:
        oid = ObjectId(order_id)
    except Exception:
        flash("Invalid order id.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    order = orders_col.find_one({"_id": oid})
    if not order:
        flash("Order not found.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    old_status = (order.get("status") or "").lower()
    now = datetime.utcnow()
    update_doc = {"status": new_status, "updated_at": now}

    # status-specific side effects
    if new_status == "delivered":
        if not order.get("delivered_at"):
            update_doc["delivered_at"] = now

    elif new_status == "refunded":
        # idempotent full refund based on what was charged for this order
        charged_amount = float(order.get("charged_amount") or 0.0)
        user_id = order.get("user_id")
        already_refunded = bool(order.get("refunded_at")) or (old_status == "refunded")

        if charged_amount > 0 and user_id and not already_refunded:
            try:
                balances_col.update_one(
                    {"user_id": user_id},
                    {"$inc": {"amount": charged_amount}, "$set": {"updated_at": now}},
                    upsert=True
                )
                transactions_col.insert_one({
                    "user_id": user_id,
                    "amount": charged_amount,
                    "reference": order.get("order_id"),
                    "status": "success",
                    "type": "refund",
                    "gateway": "Wallet",
                    "currency": "GHS",
                    "created_at": now,
                    "verified_at": now,
                    "meta": {"note": "Admin refund", "order_db_id": oid}
                })
            except Exception:
                # We still mark status, but inform admin
                flash("Refund ledger update encountered an error.", "warning")

        update_doc["refunded_at"] = now

    # failed/processing/pending/completed: no wallet movement
    try:
        res = orders_col.update_one({"_id": oid}, {"$set": update_doc})
        if res.modified_count:
            msg = {
                "processing": "✅ Order marked as Processing.",
                "delivered": "✅ Order marked as Delivered.",
                "failed": "✅ Order marked as Failed.",
                "refunded": "✅ Order marked as Refunded (wallet credited if not already).",
                "pending": "✅ Order marked as Pending.",
                "completed": "✅ Order marked as Completed.",
            }.get(new_status, "✅ Order updated.")
            flash(msg, "success")
        else:
            flash("⚠️ No change to order.", "warning")
    except Exception:
        flash("❌ Error updating order status.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)


@admin_orders_bp.route("/admin/orders/bulk-deliver", methods=["POST"])
def bulk_deliver_orders():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # Reuse the page filters but force status=processing
    args = request.args.to_dict(flat=True)
    args["status"] = "processing"
    query = _build_query_from_params(args)

    try:
        now = datetime.utcnow()
        res = orders_col.update_many(
            query,
            {"$set": {"status": "delivered", "delivered_at": now, "updated_at": now}}
        )
        modified = getattr(res, "modified_count", 0)

        # Optional: flip any line items flagged as processing to delivered
        try:
            orders_col.update_many(
                {"_id": {"$in": [o["_id"] for o in orders_col.find(query, {"_id": 1})]}},
                {"$set": {"items.$[it].line_status": "delivered"}},
                array_filters=[{"it.line_status": "processing"}]
            )
        except Exception:
            pass

        flash(f"✅ Marked {modified} processing order(s) as Delivered.", "success")
    except Exception:
        flash("❌ Bulk update failed.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)
