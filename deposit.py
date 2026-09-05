from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from bson import ObjectId
from datetime import datetime, timedelta
import requests, json, uuid

from db import db
from admin_balance import ARKESEL_API_KEY, SENDER_ID, _normalize_phone, _send_sms

deposit_bp = Blueprint("deposit", __name__)
balances_col = db["balances"]
transactions_col = db["transactions"]
users_col = db["users"]

# ==========================
# ✅ HARDCODED PAYSTACK KEYS
# ==========================
PAYSTACK_PUBLIC_KEY = "pk_live_9bfdd68d9b3205e311a3709b19143081ecaf74ee"
PAYSTACK_SECRET_KEY = "sk_live_e8b4e4a02b170e36ee385b839517ce4f1d0bd92b"

# ✅ Hardcode fee rate too (0.5%)
DEPOSIT_FEE_RATE = 0.005
MIN_PAYSTACK_DEPOSIT_GHS = 20.0
MIN_MANUAL_TOPUP_GHS = 50.0


def _get_active_admin():
    cursor = users_col.find(
        {"role": "admin", "status": {"$ne": "deleted"}, "manual_topup.active": True}
    ).sort("updated_at", -1).limit(1)
    admin_doc = next(cursor, None)
    if admin_doc:
        return admin_doc

    cursor = users_col.find(
        {"role": "admin", "status": {"$ne": "deleted"}, "manual_topup": {"$exists": True}}
    ).sort("updated_at", -1).limit(1)
    admin_doc = next(cursor, None)
    if admin_doc:
        return admin_doc

    return users_col.find_one({"role": "admin", "status": {"$ne": "deleted"}})


def _r2(x: float) -> float:
    return round(float(x or 0), 2)


def _full_name(user: dict) -> str:
    if not user:
        return ""
    name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
    return name or user.get("name") or user.get("username") or ""


@deposit_bp.route("/admin/manual-deposits", methods=["GET"])
def admin_manual_deposits():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    status_filter = (request.args.get("status") or "pending").strip().lower()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()

    query = {"source": "manual_topup"}
    if status_filter and status_filter != "all":
        query["status"] = status_filter

    date_filter = {}
    if start_date:
        try:
            date_filter["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
        except Exception:
            flash("Invalid start date format.", "warning")
    if end_date:
        try:
            date_filter["$lt"] = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        except Exception:
            flash("Invalid end date format.", "warning")
    if date_filter:
        query["created_at"] = date_filter

    txns = list(
        transactions_col.find(query).sort([("status", 1), ("created_at", -1)])
    )

    user_ids = [t.get("user_id") for t in txns if t.get("user_id")]
    users_map = {}
    if user_ids:
        for user_doc in users_col.find({"_id": {"$in": user_ids}}, {"first_name": 1, "last_name": 1, "username": 1, "phone": 1}):
            users_map[user_doc["_id"]] = user_doc

    view_rows = []
    for txn in txns:
        user_doc = users_map.get(txn.get("user_id")) or {}
        view_rows.append({
            "reference": txn.get("reference") or "",
            "amount": _r2(txn.get("amount") or 0),
            "status": (txn.get("status") or "pending").lower(),
            "created_at": txn.get("created_at"),
            "payer_name": ((txn.get("meta") or {}).get("payer_name") or ""),
            "customer_name": _full_name(user_doc) or ((txn.get("meta") or {}).get("customer_name") or "Unknown User"),
            "phone": user_doc.get("phone") or "",
            "confirm_url": url_for("deposit.confirm_manual_topup_page", reference=txn.get("reference")),
        })

    counts = {
        "all": transactions_col.count_documents({"source": "manual_topup"}),
        "pending": transactions_col.count_documents({"source": "manual_topup", "status": "pending"}),
        "success": transactions_col.count_documents({"source": "manual_topup", "status": "success"}),
    }

    return render_template(
        "admin_manual_deposits.html",
        deposits=view_rows,
        status_filter=status_filter,
        start_date=start_date,
        end_date=end_date,
        counts=counts,
    )


@deposit_bp.route("/deposit")
def deposit_page():
    if session.get("role") != "customer" or "user_id" not in session:
        return redirect(url_for("login.login"))

    email = session.get("email")
    if not email:
        user = users_col.find_one({"_id": ObjectId(session["user_id"])})
        email = user.get("email", "") if user else ""

    admin_doc = _get_active_admin() or {}
    manual_topup = admin_doc.get("manual_topup") or {}
    deposit_methods = admin_doc.get("deposit_methods") or {}
    paystack_active = bool(deposit_methods.get("paystack_active", True))
    manual_active = bool(manual_topup.get("active"))
    requested_tab = (request.args.get("tab") or "").strip().lower()
    if requested_tab not in {"manual", "paystack"}:
        requested_tab = ""
    if requested_tab == "manual" and not manual_active:
        requested_tab = ""
    if requested_tab == "paystack" and not paystack_active:
        requested_tab = ""
    active_tab = requested_tab or ("manual" if manual_active else ("paystack" if paystack_active else "manual"))
    deposit_history = list(
        transactions_col.find(
            {"user_id": ObjectId(session["user_id"]), "type": "deposit"},
            {"amount": 1, "reference": 1, "status": 1, "gateway": 1, "source": 1, "created_at": 1},
        ).sort("created_at", -1).limit(25)
    )

    return render_template(
        "deposit.html",
        user_id=session["user_id"],
        email=email,
        paystack_pk=PAYSTACK_PUBLIC_KEY,     # ✅ send hardcoded PK to UI
        deposit_fee_rate=DEPOSIT_FEE_RATE,   # 0.5% sent to UI
        min_paystack_deposit=MIN_PAYSTACK_DEPOSIT_GHS,
        min_manual_topup=MIN_MANUAL_TOPUP_GHS,
        manual_topup=manual_topup,
        manual_topup_active=manual_active,
        paystack_active=paystack_active,
        active_tab=active_tab,
        deposit_history=deposit_history,
    )


@deposit_bp.route("/deposit/manual", methods=["POST"])
def submit_manual_topup():
    if session.get("role") != "customer" or "user_id" not in session:
        return redirect(url_for("login.login"))

    admin_doc = _get_active_admin() or {}
    manual_topup = admin_doc.get("manual_topup") or {}
    if not manual_topup.get("active"):
        flash("Manual Top Up is not available right now.", "danger")
        return redirect(url_for("deposit.deposit_page"))

    payer_name = (request.form.get("payer_name") or "").strip()
    amount_raw = (request.form.get("amount") or "").strip()

    if not payer_name:
        flash("Enter the MoMo name you used to pay.", "danger")
        return redirect(url_for("deposit.deposit_page", tab="manual"))

    try:
        amount = _r2(float(amount_raw))
    except Exception:
        amount = 0.0

    if amount < MIN_MANUAL_TOPUP_GHS:
        flash(f"Minimum manual top up is GHS {MIN_MANUAL_TOPUP_GHS:.2f}.", "danger")
        return redirect(url_for("deposit.deposit_page", tab="manual"))

    existing_pending = transactions_col.find_one(
        {
            "user_id": ObjectId(session["user_id"]),
            "source": "manual_topup",
            "status": "pending",
        },
        {"_id": 1, "reference": 1},
    )
    if existing_pending:
        flash("You already have a pending manual top up. Wait for it to be confirmed or rejected before sending another one.", "warning")
        return redirect(url_for("deposit.deposit_page", tab="manual"))

    reference = f"MTU-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"
    user_doc = users_col.find_one({"_id": ObjectId(session["user_id"])}) or {}
    confirm_link = url_for("deposit.confirm_manual_topup_page", reference=reference, _external=True)

    transactions_col.insert_one({
        "user_id": ObjectId(session["user_id"]),
        "amount": amount,
        "reference": reference,
        "status": "pending",
        "type": "deposit",
        "gateway": "Manual Top Up",
        "source": "manual_topup",
        "created_at": datetime.utcnow(),
        "meta": {
            "payer_name": payer_name,
            "customer_name": _full_name(user_doc),
            "recipient_name": manual_topup.get("name") or "",
            "recipient_number": manual_topup.get("number") or "",
            "recipient_network": manual_topup.get("network") or "",
            "notification_number": manual_topup.get("notification_number") or "",
            "confirm_link": confirm_link,
        },
    })

    notification_number = _normalize_phone(manual_topup.get("notification_number") or "")
    if notification_number:
        sms_message = (
            f"Requested Deposit - {confirm_link} "
            f"Amount: GHS{amount:.2f} "
            f"Name: {payer_name}"
        )
        try:
            _send_sms(notification_number, sms_message)
        except Exception:
            pass

    flash("Manual top up submitted. Please wait while admin confirms your payment.", "success")
    return redirect(url_for("deposit.deposit_page", tab="manual"))


@deposit_bp.route("/admin/manual-topup/<reference>/confirm", methods=["GET"])
def confirm_manual_topup_page(reference: str):
    txn = transactions_col.find_one({"reference": reference, "source": "manual_topup"})
    if not txn:
        flash("Deposit request not found.", "danger")
        return redirect(url_for("deposit.deposit_page"))

    user_doc = users_col.find_one({"_id": txn.get("user_id")}) or {}
    return render_template(
        "admin_confirm_deposit.html",
        txn=txn,
        user_doc=user_doc,
        user_full_name=_full_name(user_doc),
    )


@deposit_bp.route("/admin/manual-topup/<reference>/credit", methods=["POST"])
def credit_manual_topup(reference: str):
    txn = transactions_col.find_one({"reference": reference, "source": "manual_topup"})
    if not txn:
        flash("Deposit request not found.", "danger")
        return redirect(url_for("deposit.deposit_page"))

    if txn.get("status") == "success":
        flash("This deposit request has already been credited.", "info")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))
    if txn.get("status") == "rejected":
        flash("This deposit request has already been rejected.", "warning")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))

    user_id = txn.get("user_id")
    if not user_id:
        flash("This deposit request has no linked user.", "danger")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))

    amount = _r2(txn.get("amount") or 0)
    now = datetime.utcnow()

    claim_result = transactions_col.update_one(
        {"_id": txn["_id"], "status": "pending"},
        {
            "$set": {
                "status": "processing",
                "updated_at": now,
                "processing_started_at": now,
                "processing_by": session.get("username") or "manual_topup_link",
            }
        },
    )
    if claim_result.modified_count != 1:
        refreshed_txn = transactions_col.find_one({"_id": txn["_id"]}, {"status": 1}) or {}
        status_now = refreshed_txn.get("status") or "pending"
        if status_now == "success":
            flash("This deposit request has already been credited.", "info")
        elif status_now == "rejected":
            flash("This deposit request has already been rejected.", "warning")
        else:
            flash("This deposit request is already being processed.", "warning")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))

    balances_col.update_one(
        {"user_id": user_id},
        {"$inc": {"amount": amount}, "$set": {"updated_at": now}},
        upsert=True,
    )

    transactions_col.update_one(
        {"_id": txn["_id"], "status": "processing"},
        {
            "$set": {
                "status": "success",
                "verified_at": now,
                "credited_at": now,
                "credited_by": session.get("username") or "manual_topup_link",
                "updated_at": now,
            }
        },
    )

    flash(f"Manual top up confirmed. Wallet credited with GHS {amount:.2f}.", "success")
    return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))


@deposit_bp.route("/admin/manual-topup/<reference>/reject", methods=["POST"])
def reject_manual_topup(reference: str):
    txn = transactions_col.find_one({"reference": reference, "source": "manual_topup"})
    if not txn:
        flash("Deposit request not found.", "danger")
        return redirect(url_for("deposit.deposit_page"))

    if txn.get("status") == "success":
        flash("This deposit request has already been credited.", "warning")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))

    if txn.get("status") == "rejected":
        flash("This deposit request has already been rejected.", "info")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))

    now = datetime.utcnow()
    result = transactions_col.update_one(
        {"_id": txn["_id"], "status": "pending"},
        {
            "$set": {
                "status": "rejected",
                "rejected_at": now,
                "rejected_by": session.get("username") or "manual_topup_link",
                "updated_at": now,
            }
        },
    )
    if result.modified_count != 1:
        refreshed_txn = transactions_col.find_one({"_id": txn["_id"]}, {"status": 1}) or {}
        status_now = refreshed_txn.get("status") or "pending"
        if status_now == "success":
            flash("This deposit request has already been credited.", "warning")
        elif status_now == "rejected":
            flash("This deposit request has already been rejected.", "info")
        else:
            flash("This deposit request is already being processed.", "warning")
        return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))

    flash("Manual top up request rejected.", "success")
    return redirect(url_for("deposit.confirm_manual_topup_page", reference=reference))


@deposit_bp.route("/verify_transaction")
def verify_transaction():
    reference = request.args.get("reference", type=str)
    user_id = session.get("user_id")

    if not reference or not user_id:
        flash("❌ Invalid deposit request", "danger")
        return redirect(url_for("customer_dashboard.customer_dashboard"))

    headers = {"Authorization": f"Bearer {PAYSTACK_SECRET_KEY}"}
    url = f"https://api.paystack.co/transaction/verify/{reference}"

    try:
        r = requests.get(url, headers=headers, timeout=20)
        result = r.json()
        print("🧾 Paystack Verification Response:", json.dumps(result, indent=2))

        ok = result.get("status") and result.get("data", {}).get("status") == "success"
        if not ok:
            fail_msg = result.get("message") or result.get("data", {}).get("gateway_response") or "Verification failed."
            flash(f"❌ Payment verification failed: {fail_msg}", "danger")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        data = result["data"]

        # Amount from Paystack is in pesewas → GHS
        paid_gross_ghs = _r2((data.get("amount", 0) or 0) / 100.0)
        currency = data.get("currency", "GHS")
        channel = data.get("channel", "")
        paid_ref = data.get("reference")
        metadata = data.get("metadata") or {}

        if paid_gross_ghs <= 0 or currency != "GHS":
            flash("❌ Invalid payment amount/currency.", "danger")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        # Idempotency
        if transactions_col.find_one({"reference": paid_ref, "status": "success"}):
            flash("✅ Deposit already verified earlier.", "success")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        # STRICT RULE: credit EXACTLY what the user entered (net_amount_ghs)
        fee_rate = float(metadata.get("fee_rate", DEPOSIT_FEE_RATE) or 0.0)

        meta_net = metadata.get("net_amount_ghs")
        try:
            net_credit_ghs = _r2(float(meta_net)) if meta_net is not None else None
        except Exception:
            net_credit_ghs = None

        if net_credit_ghs is None:
            # Fallback only if metadata is missing (old clients)
            net_credit_ghs = _r2(paid_gross_ghs / (1.0 + fee_rate))

        # Enforce minimum deposit (GHS)
        if net_credit_ghs < MIN_PAYSTACK_DEPOSIT_GHS:
            flash(f"❌ Minimum Paystack deposit is GHS {MIN_PAYSTACK_DEPOSIT_GHS:.2f}.", "danger")
            return redirect(url_for("customer_dashboard.customer_dashboard"))

        # Guardrails — never over-credit
        if net_credit_ghs < 0:
            net_credit_ghs = 0.0
        if net_credit_ghs > paid_gross_ghs:
            net_credit_ghs = paid_gross_ghs

        # For records: compute fee for audit
        fee_ghs = _r2(paid_gross_ghs - net_credit_ghs)

        # Credit NET to wallet
        balances_col.update_one(
            {"user_id": ObjectId(user_id)},
            {"$inc": {"amount": net_credit_ghs}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True,
        )

        # Store full audit details
        transactions_col.insert_one({
            "user_id": ObjectId(user_id),
            "amount": net_credit_ghs,          # NET credited (exact user entry)
            "reference": paid_ref,
            "status": "success",
            "type": "deposit",
            "gateway": "Paystack",
            "currency": currency,
            "channel": channel,
            "raw": data,
            "verified_at": datetime.utcnow(),
            "created_at": datetime.utcnow(),
            "meta": {
                "paid_gross_ghs": paid_gross_ghs,
                "net_credit_ghs": net_credit_ghs,
                "fee_ghs": fee_ghs,
                "fee_rate": fee_rate,
                "source": "deposit_fee_0p5_strict_net_credit"
            }
        })

        flash(f"✅ Deposit successful! Credited ₵{net_credit_ghs:.2f}.", "success")

    except Exception as e:
        print("❌ Paystack Exception:", str(e))
        flash("❌ Could not verify payment. Please try again.", "danger")

    return redirect(url_for("customer_dashboard.customer_dashboard"))
