from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId
from db import db
from datetime import datetime

admin_orders_bp = Blueprint("admin_orders", __name__)
orders_col = db["orders"]
users_col = db["users"]

@admin_orders_bp.route("/admin/orders")
def admin_view_orders():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    status_filter = request.args.get("status")
    page = int(request.args.get("page", 1))
    per_page = 10
    skip = (page - 1) * per_page

    query = {}
    if status_filter:
        query["status"] = status_filter

    try:
        total_orders = orders_col.count_documents(query)
        total_pages = (total_orders + per_page - 1) // per_page

        orders = list(
            orders_col.find(query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(per_page)
        )

        for order in orders:
            user = users_col.find_one({"_id": ObjectId(order.get("user_id"))})
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
        status_filter=status_filter,
    )

@admin_orders_bp.route("/admin/orders/<order_id>/update", methods=["POST"])
def update_order_status(order_id):
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    new_status = request.form.get("status", "").strip()

    if not new_status:
        flash("Status is required.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    try:
        result = orders_col.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {
                "status": new_status,
                "updated_at": datetime.utcnow()
            }}
        )

        if result.modified_count:
            flash("✅ Order status updated successfully.", "success")
        else:
            flash("⚠️ No change was made to the order.", "warning")

    except Exception as e:
        flash("❌ Error updating order status.", "danger")

    return redirect(url_for("admin_orders.admin_view_orders"))
