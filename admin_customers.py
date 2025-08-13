# admin_customers.py
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from db import db
from urllib.parse import urlencode
from bson import ObjectId
import math
import re
from werkzeug.security import generate_password_hash

admin_customers_bp = Blueprint("admin_customers", __name__)
users_col = db["users"]

# View Customers Page
@admin_customers_bp.route("/admin/customers")
def view_customers():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # --- Filters ---
    q = (request.args.get("q") or "").strip()
    referral = (request.args.get("referral") or "").strip()
    has_whatsapp = request.args.get("has_whatsapp")
    has_email = request.args.get("has_email")
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = 15

    # Build query
    conditions = [{"role": "customer"}]

    if q:
        regex = {"$regex": re.escape(q), "$options": "i"}
        conditions.append({
            "$or": [
                {"first_name": regex},
                {"last_name": regex},
                {"username": regex},
                {"email": regex},
                {"phone": regex},
                {"business_name": regex},
                {"whatsapp": regex},
                {"referral": regex},
            ]
        })

    if referral:
        conditions.append({"referral": {"$regex": re.escape(referral), "$options": "i"}})

    if has_whatsapp == "1":
        conditions.append({"whatsapp": {"$exists": True, "$ne": ""}})
    elif has_whatsapp == "0":
        conditions.append({"$or": [
            {"whatsapp": {"$exists": False}},
            {"whatsapp": ""},
            {"whatsapp": None},
        ]})

    if has_email == "1":
        conditions.append({"email": {"$exists": True, "$ne": ""}})
    elif has_email == "0":
        conditions.append({"$or": [
            {"email": {"$exists": False}},
            {"email": ""},
            {"email": None},
        ]})

    query = {"$and": conditions} if len(conditions) > 1 else conditions[0]

    total = users_col.count_documents(query)
    total_pages = max(math.ceil(total / per_page), 1)
    if page > total_pages:
        page = total_pages

    skip = (page - 1) * per_page

    customers = list(
        users_col.find(query)
        .sort([("_id", -1)])
        .skip(skip)
        .limit(per_page)
    )

    # Base query string for pagination
    qs = request.args.to_dict(flat=True)
    qs.pop("page", None)
    base_qs = urlencode(qs)

    return render_template(
        "admin_customers.html",
        customers=customers,
        q=q,
        referral=referral,
        has_whatsapp=has_whatsapp,
        has_email=has_email,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        base_qs=base_qs
    )

# Update Customer API (AJAX)
@admin_customers_bp.route("/admin/customers/update/<customer_id>", methods=["POST"])
def update_customer(customer_id):
    if session.get("role") != "admin":
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    data = {k: v.strip() for k, v in request.form.items() if v.strip()}

    # Handle password hashing if provided
    if "password" in data and data["password"]:
        data["password"] = generate_password_hash(data["password"])

    # Prevent changing Mongo _id or role accidentally
    data.pop("_id", None)
    data.pop("role", None)

    users_col.update_one({"_id": ObjectId(customer_id)}, {"$set": data})

    return jsonify({"status": "success", "message": "Customer updated successfully"})
