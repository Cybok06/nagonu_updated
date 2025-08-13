# admin_referals.py
from flask import Blueprint, render_template, session, redirect, url_for
from bson import ObjectId
from db import db

admin_referrals_bp = Blueprint("admin_referrals", __name__)
users_col = db["users"]
referrals_col = db["referrals"]

@admin_referrals_bp.route("/admin/referrals")
def admin_referrals():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # Get all referral documents
    referrals = list(referrals_col.find())

    # Build a list of referrers with their referral code and referred users
    results = []
    total_referred = 0

    for r in referrals:
        referrer = users_col.find_one({"_id": r["user_id"]})
        if not referrer:
            continue

        ref_code = referrer.get("username")  # Assuming username is used as referral code in user document

        referred_users = list(users_col.find({"referral": {"$regex": f"^{ref_code}$", "$options": "i"}}))
        total_referred += len(referred_users)

        results.append({
            "referrer": referrer,
            "ref_code": r["ref_code"],
            "created_at": r.get("created_at"),
            "referred_users": referred_users
        })

    return render_template("admin_referrals.html", referrals=results, total_referred=total_referred)
