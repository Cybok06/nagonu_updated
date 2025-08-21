from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId, Regex
from db import db
from datetime import datetime, timedelta
from urllib.parse import urlencode

admin_orders_bp = Blueprint("admin_orders", __name__)
orders_col = db["orders"]
users_col = db["users"]

# Keep 'completed' for legacy records, but prefer 'delivered'
ALLOWED_STATUSES = {"pending", "processing", "delivered", "failed", "completed"}
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
    """Central place to build the Mongo query so view + bulk share same filters."""
    status_filter = (args.get("status") or "").strip().lower()
    order_id_q = (args.get("order_id") or "").strip()
    customer_q = (args.get("customer") or "").strip()
    paid_from = (args.get("paid_from") or "").strip().lower()
    min_total = (args.get("min_total") or "").strip()
    max_total = (args.get("max_total") or "").strip()
    date_from = _parse_date((args.get("date_from") or "").strip())
    date_to_raw = _parse_date((args.get("date_to") or "").strip())
    date_to = None
    if date_to_raw:
        date_to = datetime(date_to_raw.year, date_to_raw.month, date_to_raw.day) + timedelta(days=1)

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
        if not user_ids:
            query["user_id"] = {"$in": []}
        else:
            query["user_id"] = {"$in": user_ids}

    # similar item filters (any item in items[] that matches)
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

    # read list controls
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

    # Sorting spec
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

        # attach user
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

        # expose all filters back to template
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

    try:
        update_doc = {"status": new_status, "updated_at": datetime.utcnow()}
        if new_status == "delivered":
            update_doc["delivered_at"] = datetime.utcnow()

        res = orders_col.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": update_doc},
        )
        if res.modified_count:
            flash("✅ Order status updated.", "success")
        else:
            flash("⚠️ No change to order.", "warning")
    except Exception:
        flash("❌ Error updating order status.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)


# NEW: BULK deliver all processing in current filter set
@admin_orders_bp.route("/admin/orders/bulk-deliver", methods=["POST"])
def bulk_deliver_orders():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # Build the same query as the list view, but enforce status=processing
    args = request.args.to_dict(flat=True)
    args["status"] = "processing"
    query = _build_query_from_params(args)

    try:
        # 1) Set the order status fields
        res = orders_col.update_many(
            query,
            {"$set": {"status": "delivered", "delivered_at": datetime.utcnow(), "updated_at": datetime.utcnow()}}
        )
        modified = getattr(res, "modified_count", 0)

        # 2) (Optional) Flip any processing line items to delivered too.
        #    Requires MongoDB 3.6+ with arrayFilters support. If not supported, it's safe to ignore failures.
        try:
            orders_col.update_many(
                {"_id": {"$in": [o["_id"] for o in orders_col.find(query, {"_id": 1})]}},
                {"$set": {"items.$[it].line_status": "delivered"}},
                array_filters=[{"it.line_status": "processing"}]
            )
        except Exception:
            # silently ignore if not supported
            pass

        flash(f"✅ Marked {modified} processing order(s) as delivered.", "success")
    except Exception:
        flash("❌ Bulk update failed.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)
