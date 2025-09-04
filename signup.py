from flask import Blueprint, render_template, request, redirect, url_for, flash
from db import db
from werkzeug.security import generate_password_hash
from datetime import datetime
from pymongo.errors import DuplicateKeyError
import re

signup_bp = Blueprint("signup", __name__)
users_col = db["users"]
balances_col = db["balances"]

# NOTE: No users_col.create_index(...) calls here to avoid deploy crashes.

def normalize_phone(raw: str) -> str:
    """Normalize Ghana numbers to '0XXXXXXXXX' where possible."""
    digits = re.sub(r"\D", "", raw or "")
    if not digits:
        return ""
    if digits.startswith("233") and len(digits) >= 12:
        return "0" + digits[-9:]
    if len(digits) == 9:
        return "0" + digits
    if len(digits) == 10 and digits.startswith("0"):
        return digits
    return digits  # fallback

@signup_bp.route("/signup", methods=["GET", "POST"])
def signup():
    referral_code = (request.args.get("ref") or "").strip()

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

        # Basic checks
        if password != confirm_password:
            flash("❌ Passwords do not match", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        phone_normalized = normalize_phone(phone)

        # ---- Simple pre-insert duplicate checks ----
        if username and users_col.find_one({"username": username}):
            flash("❌ Username already exists.", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        if email and users_col.find_one({"email": email}):
            flash("❌ Email already exists.", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        if phone_normalized and users_col.find_one({"phone_normalized": phone_normalized}):
            flash("❌ Phone number already exists.", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))
        # -------------------------------------------

        now = datetime.utcnow()
        new_user = {
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
            "email": email,
            "phone": phone,                 # keep raw for display if needed
            "phone_normalized": phone_normalized,
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

        except DuplicateKeyError as e:
            # If your DB already has unique indexes, this catches races.
            try:
                kv = (e.details or {}).get("keyValue") or {}
                if "username" in kv:
                    msg = "❌ Username already exists."
                elif "email" in kv:
                    msg = "❌ Email already exists."
                elif "phone_normalized" in kv:
                    msg = "❌ Phone number already exists."
                else:
                    msg = "❌ That credential is already registered."
            except Exception:
                msg = "❌ That credential is already registered."
            # best-effort cleanup if user doc got created before error
            users_col.delete_one({"username": username})
            flash(msg, "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        except Exception:
            flash("❌ Could not complete signup. Please try again.", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        flash("✅ Account created successfully! You can now log in.", "success")
        return redirect(url_for("login.login"))

    return render_template("signup.html", referral_code=referral_code)
