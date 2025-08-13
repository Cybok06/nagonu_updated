from flask import Blueprint, render_template, session, redirect, url_for
from bson import ObjectId
from db import db

transactions_bp = Blueprint("transactions", __name__)
transactions_col = db["transactions"]

@transactions_bp.route("/customer/transactions")
def view_transactions():
    if session.get("role") != "customer":
        return redirect(url_for("login.login"))

    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("login.login"))

    transactions = list(
        transactions_col.find({"user_id": ObjectId(user_id)})
        .sort("verified_at", -1)
    )

    return render_template("transactions.html", transactions=transactions)
