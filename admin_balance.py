# admin_balance.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from bson.objectid import ObjectId
from db import db
from datetime import datetime

admin_balance_bp = Blueprint("admin_balance", __name__)
balances_col = db["balances"]
users_col = db["users"]

@admin_balance_bp.route("/admin/balances")
def view_balances():
    balances = []
    for bal in balances_col.find():
        user = users_col.find_one({"_id": bal["user_id"]})
        if user:
            balances.append({
                "_id": bal["_id"],
                "user": user,
                "amount": float(bal["amount"]),
                "currency": bal.get("currency", "GHS")
            })
    return render_template("admin_balance.html", balances=balances)


@admin_balance_bp.route("/admin/balances/update/<balance_id>", methods=["POST"])
def update_balance(balance_id):
    new_amount = request.form.get("amount")
    if not new_amount or not balance_id:
        flash("Invalid data provided.", "danger")
        return redirect(url_for("admin_balance.view_balances"))

    try:
        balances_col.update_one(
            {"_id": ObjectId(balance_id)},
            {"$set": {
                "amount": float(new_amount),
                "updated_at": datetime.utcnow()
            }}
        )
        flash("Balance updated successfully!", "success")
    except Exception as e:
        flash("Error updating balance.", "danger")
        print("Update Error:", e)

    return redirect(url_for("admin_balance.view_balances"))
