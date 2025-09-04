from flask import Blueprint, render_template, request, redirect, url_for, flash
from db import db
from werkzeug.security import generate_password_hash
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
import re

signup_bp = Blueprint("signup", __name__)
users_col = db["users"]
balances_col = db["balances"]

# ---- ONE-TIME (safe to keep here; it no-ops if indexes already exist) ----
# Make sure we can never store duplicates even under race conditions.
users_col.create_index("username", unique=True, sparse=True)
users_col.create_index("email", unique=True, sparse=True)
users_col.create_index("phone_normalized", unique=True, sparse=True)

def normalize_phone(raw: str) -> str:
    """
    Normalize Ghana numbers to a consistent local '0XXXXXXXXX' form.
    Accepts +233XXXXXXXXX, 233XXXXXXXXX, or 0XXXXXXXXX (with/without spaces/dashes).
    Falls back to just digits if it can't infer.
    """
    digits = re.sub(r"\D", "", raw or "")
    if not digits:
        return ""
    # If begins with '233', keep last 9 and prefix with '0'
    if digits.startswith("233") and len(digits) >= 12:
        return "0" + digits[-9:]
    # If exactly 9 digits (missing leading 0), prefix it
    if len(digits) == 9:
        return "0" + digits
    # If already 10 and starts with 0, keep as is
    if len(digits) == 10 and digits.startswith("0"):
        return digits
    # Otherwise return digits as a fallback (still unique-indexed)
    return digits

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

        # Pre-insert uniqueness checks (nice UX; DB enforces too)
        conflicts = []
        if username and users_col.find_one({"username": username}):
            conflicts.append("username")
        if email and users_col.find_one({"email": email}):
            conflicts.append("email")
        if phone_normalized and users_col.find_one({"phone_normalized": phone_normalized}):
            conflicts.append("phone number")

        if conflicts:
            nice = ", ".join(conflicts)
            flash(f"❌ That {nice} is already registered.", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        now = datetime.utcnow()
        new_user = {
            "first_name": first_name,
            "last_name": last_name,
            "username": username,
            "email": email,
            "phone": phone,  # keep raw for display if you like
            "phone_normalized": phone_normalized,  # enforced unique
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
            # Rollback partial user if needed
            try:
                if 'user_id' in locals():
                    users_col.delete_one({"_id": user_id})
            except Exception:
                pass

            # Try to show a precise message (Mongo includes key info)
            msg = "❌ That credential is already registered."
            try:
                kv = (e.details or {}).get("keyValue") or {}
                if "username" in kv:
                    msg = "❌ Username already exists."
                elif "email" in kv:
                    msg = "❌ Email already exists."
                elif "phone_normalized" in kv:
                    msg = "❌ Phone number already exists."
            except Exception:
                pass

            flash(msg, "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        except Exception:
            try:
                if 'user_id' in locals():
                    users_col.delete_one({"_id": user_id})
            except Exception:
                pass
            flash("❌ Could not complete signup. Please try again.", "danger")
            return redirect(url_for("signup.signup", ref=referral_code))

        flash("✅ Account created successfully! You can now log in.", "success")
        return redirect(url_for("login.login"))

    return render_template("signup.html", referral_code=referral_code)
