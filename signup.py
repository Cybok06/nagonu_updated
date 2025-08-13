from flask import Blueprint, render_template, request, redirect, url_for, flash
from db import db
from werkzeug.security import generate_password_hash
from datetime import datetime
from bson.objectid import ObjectId

signup_bp = Blueprint("signup", __name__)
users_col = db["users"]
balances_col = db["balances"]

@signup_bp.route("/signup", methods=["GET", "POST"])
def signup():
    referral_code = request.args.get("ref", "").strip()

    if request.method == "POST":
        # Form data
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        username = (request.form.get("username") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        phone = (request.form.get("phone") or "").strip()
        business_name = (request.form.get("business_name") or "").strip()
        whatsapp = (request.form.get("whatsapp") or "").strip()
        referral = (request.form.get("referral") or "").strip()
        password = request.form.get("password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        if password != confirm_password:
            flash("❌ Passwords do not match", "danger")
            return redirect(url_for("signup.signup", ref=referral))

        if users_col.find_one({"username": username}):
            flash("❌ Username already exists", "danger")
            return redirect(url_for("signup.signup", ref=referral))
        if email and users_col.find_one({"email": email}):
            flash("❌ Email already exists", "danger")
            return redirect(url_for("signup.signup", ref=referral))

        now = datetime.utcnow()
        new_user = {
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
            "email": email,
            "phone": phone,
            "business_name": business_name,
            "whatsapp": whatsapp,
            "referral": referral,
            "password": generate_password_hash(password),
            "role": "customer",
            "status": "active",
            "created_at": now,
            "updated_at": now,
        }

        try:
            res = users_col.insert_one(new_user)
            user_id = res.inserted_id

            balances_col.insert_one({
                "user_id": user_id,
                "amount": 0.00,
                "currency": "GHS",
                "created_at": now,
                "updated_at": now,
            })

        except Exception:
            try:
                if 'user_id' in locals():
                    users_col.delete_one({"_id": user_id})
            except:
                pass
            flash("❌ Could not complete signup. Please try again.", "danger")
            return redirect(url_for("signup.signup", ref=referral))

        flash("✅ Account created successfully! You can now log in.", "success")
        return redirect(url_for("login.login"))

    return render_template("signup.html", referral_code=referral_code)
