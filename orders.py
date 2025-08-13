# routes/orders.py
from flask import Blueprint, render_template, session, redirect, url_for, request
from bson import ObjectId
from db import db
from datetime import datetime, timedelta
import math

orders_bp = Blueprint("orders", __name__)
orders_col = db["orders"]

@orders_bp.route("/customer/orders")
def view_orders():
    # auth
    if session.get("role") != "customer":
        return redirect(url_for("login.login"))
    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("login.login"))

    # ----- Filters -----
    status = (request.args.get("status") or "all").strip().lower()
    start_date_str = (request.args.get("start_date") or "").strip()
    end_date_str   = (request.args.get("end_date") or "").strip()

    # pagination
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except ValueError:
        page = 1
    PER_PAGE = 10

    # build query
    query = {"user_id": ObjectId(user_id)}
    if status and status != "all":
        query["status"] = status

    # date filter
    date_filter = {}
    def parse_ymd(s):
        return datetime.strptime(s, "%Y-%m-%d")

    try:
        if start_date_str:
            date_filter["$gte"] = parse_ymd(start_date_str)
        if end_date_str:
            date_filter["$lt"] = parse_ymd(end_date_str) + timedelta(days=1)
    except ValueError:
        date_filter = {}

    if date_filter:
        query["created_at"] = date_filter

    # counts + page data
    total_count = orders_col.count_documents(query)
    total_pages = max(math.ceil(total_count / PER_PAGE), 1)
    if page > total_pages:
        page = total_pages

    # fetch current page
    cursor = (
        orders_col.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * PER_PAGE)
        .limit(PER_PAGE)
    )
    orders = list(cursor)

    # status list for dropdown
    available_statuses = orders_col.distinct("status", {"user_id": ObjectId(user_id)}) or []
    preferred_order = ["pending", "completed", "failed", "cancelled"]
    ordered_statuses = [s for s in preferred_order if s in available_statuses]
    for s in available_statuses:
        if s not in ordered_statuses:
            ordered_statuses.append(s)

    # PREPARE PAGE WINDOW HERE (avoid max/min in Jinja)
    window = 2
    start = page - window
    if start < 1:
        start = 1
    end = page + window
    if end > total_pages:
        end = total_pages
    page_numbers = list(range(start, end + 1))

    return render_template(
        "orders.html",
        orders=orders,
        page=page,
        per_page=PER_PAGE,
        total_count=total_count,
        total_pages=total_pages,
        page_numbers=page_numbers,  # <-- pass to template
        status=status,
        start_date=start_date_str,
        end_date=end_date_str,
        statuses=ordered_statuses
    )
