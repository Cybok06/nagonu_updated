from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId
from datetime import datetime
import os, requests

from db import db

deposit_bp = Blueprint("deposit", __name__)
balances_col = db["balances"]
transactions_col = db["transactions"]
users_col = db["users"]

# 1) Deposit page (now also passes paystack_pk from env as a fallback for the template)
@deposit_bp.route("/deposit")
def deposit_page():
    if session.get("role") != "customer" or "user_id" not in session:
        return redirect(url_for("login.login"))

    email = session.get("email")
    if not email:
        user = users_col.find_one({"_id": ObjectId(session["user_id"])})
        email = user.get("email", "") if user else ""

    # Fallback pass-through (template prefers global PAYSTACK_PUBLIC_KEY if injected by app)
    paystack_pk = os.getenv("PAYSTACK_PUBLIC_KEY", "")

    return render_template("deposit.html", user_id=session["user_id"], email=email, paystack_pk=paystack_pk)


# 2) Verify Paystack transaction (uses secret key from ENV)
@deposit_bp.route("/verify_transaction")
def verify_transaction():
    reference = request.args.get("reference", type=str)
    user_id = session.get("user_id")

    if not reference or not user_id:
        flash("❌ Invalid deposit request", "danger")
        return redirect(url_for("customer_dashboard.customer_dashboard"))

    paystack_sk = os.getenv("PAYSTACK_SECRET_KEY")  # ← from ENV
    if not paystack_sk:
        flash("❌ Payment processor not configured. Contact support.", "danger")
        return redirect(url_for("customer_dashboard.customer_dashboard"))

    headers = {"Authorization": f"Bearer {paystack_sk}"}
    url = f"https://api.paystack.co/transaction/verify/{reference}"

    try:
        r = requests.get(url, headers=headers, timeout=20)
        result = r.json()
        print("🧾 Paystack Verification Response:", result)

        ok = result.get("status") and result.get("data", {}).get("status") == "success"
        if not ok:
            fail_msg = result.get("message") or result.get("data", {}).get("gateway_response") or "Verification failed."
            flash(f"❌ Payment verification failed: {fail_msg}", "danger")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        data = result["data"]

        # Amount & currency from Paystack (kobo → GHS)
        amount_ghs = round((data.get("amount", 0) or 0) / 100.0, 2)
        currency = data.get("currency", "GHS")
        channel = data.get("channel", "")
        paid_ref = data.get("reference")

        if amount_ghs <= 0 or currency != "GHS":
            flash("❌ Invalid payment amount/currency.", "danger")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        # Idempotency
        existing = transactions_col.find_one({"reference": paid_ref, "status": "success"})
        if existing:
            flash("✅ Deposit already verified earlier.", "success")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        # Update balance
        balances_col.update_one(
            {"user_id": ObjectId(user_id)},
            {"$inc": {"amount": amount_ghs}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True,
        )

        # Record transaction
        transactions_col.insert_one({
            "user_id": ObjectId(user_id),
            "amount": amount_ghs,
            "reference": paid_ref,
            "status": "success",
            "type": "deposit",
            "gateway": "Paystack",
            "currency": currency,
            "channel": channel,
            "raw": data,
            "verified_at": datetime.utcnow(),
            "created_at": datetime.utcnow(),
        })

        flash("✅ Deposit successful! Your balance has been updated.", "success")

    except Exception as e:
        print("❌ Paystack Exception:", str(e))
        flash("❌ Could not verify payment. Please try again.", "danger")

    return redirect(url_for("customer_dashboard.customer_dashboard"))
