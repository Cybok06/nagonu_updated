# admin_balance.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from bson.objectid import ObjectId
from db import db
from datetime import datetime

admin_balance_bp = Blueprint("admin_balance", __name__)
balances_col = db["balances"]
users_col = db["users"]
balance_logs_col = db["balance_logs"]  # NEW: audit trail

def _now():
    return datetime.utcnow()

def _get_actor():
    """Return (actor_id, actor_name) for the admin doing the change."""
    actor_id = session.get("user_id")
    actor_name = session.get("username") or session.get("email") or "admin"
    if actor_id:
        try:
            u = users_col.find_one({"_id": ObjectId(actor_id)})
            if u:
                actor_name = u.get("username") or f"{u.get('first_name','')} {u.get('last_name','')}".strip() or actor_name
        except Exception:
            pass
    return actor_id, actor_name

@admin_balance_bp.route("/admin/balances")
def view_balances():
    balances = []
    cursor = balances_col.find()
    for bal in cursor:
        user = users_col.find_one({"_id": bal["user_id"]})
        if not user:
            # Skip orphan records
            continue
        balances.append({
            "_id": bal["_id"],
            "user": {
                "_id": user["_id"],
                "first_name": user.get("first_name", ""),
                "last_name": user.get("last_name", ""),
                "phone": user.get("phone", ""),
            },
            "amount": float(bal.get("amount", 0.0)),
            "currency": bal.get("currency", "GHS"),
            "updated_at": bal.get("updated_at"),
        })
    return render_template("admin_balance.html", balances=balances)

@admin_balance_bp.route("/admin/balances/update/<balance_id>", methods=["POST"])
def update_balance(balance_id):
    """
    Preserve your existing direct set route (sets absolute amount).
    Also writes a log entry with action='set'.
    """
    new_amount = request.form.get("amount")
    if not new_amount or not balance_id:
        flash("Invalid data provided.", "danger")
        return redirect(url_for("admin_balance.view_balances"))

    try:
        bal = balances_col.find_one({"_id": ObjectId(balance_id)})
        if not bal:
            flash("Balance not found.", "danger")
            return redirect(url_for("admin_balance.view_balances"))

        old_amount = float(bal.get("amount", 0.0))
        new_amount_f = float(new_amount)
        currency = bal.get("currency", "GHS")

        # Update
        balances_col.update_one(
            {"_id": ObjectId(balance_id)},
            {"$set": {"amount": new_amount_f, "updated_at": _now()}}
        )

        # Log
        actor_id, actor_name = _get_actor()
        balance_logs_col.insert_one({
            "balance_id": bal["_id"],
            "user_id": bal["user_id"],
            "action": "set",
            "delta": new_amount_f - old_amount,
            "amount_before": old_amount,
            "amount_after": new_amount_f,
            "currency": currency,
            "note": request.form.get("note") or "",
            "actor_id": ObjectId(actor_id) if actor_id else None,
            "actor_name": actor_name,
            "created_at": _now(),
        })

        flash("Balance updated successfully!", "success")
    except Exception as e:
        print("Update Error:", e)
        flash("Error updating balance.", "danger")

    return redirect(url_for("admin_balance.view_balances"))

@admin_balance_bp.route("/admin/balances/deposit/<balance_id>", methods=["POST"])
def deposit_balance(balance_id):
    """
    Increment by a positive amount (deposit / plus sign).
    """
    delta = request.form.get("amount")
    note = request.form.get("note") or ""
    if not delta:
        flash("Enter an amount to deposit.", "warning")
        return redirect(url_for("admin_balance.view_balances"))

    try:
        delta_f = float(delta)
        if delta_f <= 0:
            flash("Deposit amount must be greater than zero.", "warning")
            return redirect(url_for("admin_balance.view_balances"))

        bal = balances_col.find_one({"_id": ObjectId(balance_id)})
        if not bal:
            flash("Balance not found.", "danger")
            return redirect(url_for("admin_balance.view_balances"))

        old_amount = float(bal.get("amount", 0.0))
        new_amount = old_amount + delta_f
        currency = bal.get("currency", "GHS")

        balances_col.update_one(
            {"_id": bal["_id"]},
            {"$set": {"amount": new_amount, "updated_at": _now()}}
        )

        actor_id, actor_name = _get_actor()
        balance_logs_col.insert_one({
            "balance_id": bal["_id"],
            "user_id": bal["user_id"],
            "action": "deposit",
            "delta": delta_f,
            "amount_before": old_amount,
            "amount_after": new_amount,
            "currency": currency,
            "note": note,
            "actor_id": ObjectId(actor_id) if actor_id else None,
            "actor_name": actor_name,
            "created_at": _now(),
        })

        flash("Deposit successful.", "success")
    except Exception as e:
        print("Deposit Error:", e)
        flash("Error processing deposit.", "danger")

    return redirect(url_for("admin_balance.view_balances"))

@admin_balance_bp.route("/admin/balances/withdraw/<balance_id>", methods=["POST"])
def withdraw_balance(balance_id):
    """
    Decrement by a positive amount (withdraw / minus sign).
    Prevents going below zero; adjust if you allow overdrafts.
    """
    delta = request.form.get("amount")
    note = request.form.get("note") or ""
    if not delta:
        flash("Enter an amount to withdraw.", "warning")
        return redirect(url_for("admin_balance.view_balances"))

    try:
        delta_f = float(delta)
        if delta_f <= 0:
            flash("Withdrawal amount must be greater than zero.", "warning")
            return redirect(url_for("admin_balance.view_balances"))

        bal = balances_col.find_one({"_id": ObjectId(balance_id)})
        if not bal:
            flash("Balance not found.", "danger")
            return redirect(url_for("admin_balance.view_balances"))

        old_amount = float(bal.get("amount", 0.0))
        new_amount = old_amount - delta_f

        if new_amount < 0:
            flash("Insufficient funds: cannot withdraw more than the current balance.", "danger")
            return redirect(url_for("admin_balance.view_balances"))

        currency = bal.get("currency", "GHS")

        balances_col.update_one(
            {"_id": bal["_id"]},
            {"$set": {"amount": new_amount, "updated_at": _now()}}
        )

        actor_id, actor_name = _get_actor()
        balance_logs_col.insert_one({
            "balance_id": bal["_id"],
            "user_id": bal["user_id"],
            "action": "withdraw",
            "delta": -delta_f,
            "amount_before": old_amount,
            "amount_after": new_amount,
            "currency": currency,
            "note": note,
            "actor_id": ObjectId(actor_id) if actor_id else None,
            "actor_name": actor_name,
            "created_at": _now(),
        })

        flash("Withdrawal successful.", "success")
    except Exception as e:
        print("Withdraw Error:", e)
        flash("Error processing withdrawal.", "danger")

    return redirect(url_for("admin_balance.view_balances"))

@admin_balance_bp.route("/admin/balances/history/<user_id>")
def balance_history(user_id):
    """
    Return JSON history for a given user (sorted newest first).
    Consumed by the modal in the UI.
    """
    try:
        uid = ObjectId(user_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid user id"}), 400

    logs = []
    cursor = balance_logs_col.find({"user_id": uid}).sort("created_at", -1).limit(200)
    for lg in cursor:
        logs.append({
            "id": str(lg["_id"]),
            "action": lg.get("action"),
            "delta": float(lg.get("delta", 0.0)),
            "amount_before": float(lg.get("amount_before", 0.0)),
            "amount_after": float(lg.get("amount_after", 0.0)),
            "currency": lg.get("currency", "GHS"),
            "note": lg.get("note", ""),
            "actor_name": lg.get("actor_name", "admin"),
            "created_at": lg.get("created_at").strftime("%Y-%m-%d %H:%M"),
        })
    return jsonify({"success": True, "logs": logs})
