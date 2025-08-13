from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId
from db import db
from datetime import datetime

admin_transactions_bp = Blueprint("admin_transactions", __name__)
transactions_col = db["transactions"]
users_col = db["users"]

@admin_transactions_bp.route("/admin/transactions")
def admin_view_transactions():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    customer_id = request.args.get("customer")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    page = int(request.args.get("page", 1))
    per_page = 10

    query = {}

    # Filter by customer ID
    if customer_id:
        try:
            query["user_id"] = ObjectId(customer_id)
        except:
            flash("Invalid customer ID.", "warning")

    # Filter by date range
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            query.setdefault("verified_at", {})["$gte"] = start_dt
        except:
            flash("Invalid start date.", "warning")

    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            query.setdefault("verified_at", {})["$lte"] = end_dt
        except:
            flash("Invalid end date.", "warning")

    # Get total count for pagination
    total_txns = transactions_col.count_documents(query)
    total_pages = (total_txns + per_page - 1) // per_page
    skip = (page - 1) * per_page

    # Fetch transactions with pagination
    transactions = list(
        transactions_col.find(query)
        .sort("verified_at", -1)
        .skip(skip)
        .limit(per_page)
    )

    # Get all customers for dropdown
    customers = list(users_col.find({"role": "customer"}).sort("first_name"))

    # Attach user info to each transaction
    for txn in transactions:
        user = next((c for c in customers if c["_id"] == txn.get("user_id")), None)
        txn["user"] = user or {}

    return render_template(
        "admin_transactions.html",
        transactions=transactions,
        customers=customers,
        selected_customer=customer_id,
        start_date=start_date,
        end_date=end_date,
        page=page,
        total_pages=total_pages
    )
