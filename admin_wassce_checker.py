from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from bson.objectid import ObjectId
from db import db
from datetime import datetime
import re

admin_wassce_checker_bp = Blueprint("admin_wassce_checker", __name__)
wassce_col = db["wassce_checker"]
checker_settings_col = db["results_checker_settings"]


def _delivery_phone(value):
    value = str(value or "").strip()
    if not re.fullmatch(r"\+?[\d\s()-]+", value):
        return ""
    digits = re.sub(r"\D", "", value)
    if digits.startswith("233") and len(digits) == 12:
        digits = "0" + digits[3:]
    return digits if re.fullmatch(r"0\d{9}", digits) else ""


def _add_delivery_badges(messages):
    sold = [message for message in messages if message.get("status") == "sold"]
    missing = []
    for message in sold:
        phone = _delivery_phone(message.get("delivery_phone"))
        if not phone:
            phone = _delivery_phone(message.get("sold_to"))
        message["delivery_label"] = phone
        if not phone:
            missing.append(message)

    # Older store sales kept the recipient only on the purchase record.
    # Match checker IDs (including legacy string IDs), never a shared payment.
    if missing:
        ids = [message["_id"] for message in missing]
        lookup_ids = ids + [str(checker_id) for checker_id in ids]
        recipients = {}
        for collection_name in ("store_checker_purchases", "public_checker_purchases"):
            for purchase in db[collection_name].find(
                {"checker_id": {"$in": lookup_ids}}, {"checker_id": 1, "phone": 1}
            ):
                phone = _delivery_phone(purchase.get("phone"))
                if phone:
                    recipients[str(purchase["checker_id"])] = phone
        for message in missing:
            message["delivery_label"] = recipients.get(str(message["_id"]), "")
            if not message["delivery_label"]:
                is_dashboard = (
                    message.get("sold_channel") == "customer_dashboard"
                    or (not message.get("sold_channel")
                        and not message.get("sold_to_store")
                        and ObjectId.is_valid(str(message.get("sold_to") or "")))
                )
                message["delivery_label"] = "Customer Dashboard" if is_dashboard else "Number unavailable"


def _checker_prices():
    settings = checker_settings_col.find_one({"_id": "checker_prices"}) or {}
    prices = settings.get("prices") or {}
    out = {}
    for checker_type in ("wassce", "bece"):
        try:
            configured = float(prices.get(checker_type) or 0)
        except Exception:
            configured = 0.0
        if configured <= 0:
            sample = wassce_col.find_one(
                {"type": checker_type, "status": "not_sold"}, sort=[("created_at", 1)]
            )
            try:
                configured = float((sample or {}).get("amount") or 0)
            except Exception:
                configured = 0.0
        out[checker_type] = round(configured, 2)
    return out

@admin_wassce_checker_bp.route("/admin/wassce_checker", methods=["GET", "POST"])
def admin_wassce_checker():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # Authoritative prices used by dashboard, store, and public checker pages.
    if request.method == "POST" and request.form.get("action") == "save_prices":
        prices = {}
        for checker_type in ("wassce", "bece"):
            try:
                price = round(float(request.form.get(f"price_{checker_type}") or 0), 2)
            except Exception:
                price = 0.0
            if price <= 0:
                flash(f"Enter a valid price for {checker_type.upper()}.", "danger")
                return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))
            prices[checker_type] = price

        now = datetime.utcnow()
        checker_settings_col.update_one(
            {"_id": "checker_prices"},
            {"$set": {"prices": prices, "updated_at": now}},
            upsert=True,
        )
        for checker_type, price in prices.items():
            wassce_col.update_many(
                {"type": checker_type, "status": "not_sold"},
                {"$set": {"amount": price, "price_updated_at": now}},
            )
        flash("WASSCE and BECE prices updated successfully.", "success")
        return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

    prices = _checker_prices()

    # Handle new checker creation
    if request.method == "POST" and request.form.get("action") == "add":
        message = request.form.get("message", "").strip()
        amount = request.form.get("amount")
        profit = request.form.get("profit")
        checker_type = request.form.get("type", "wassce").lower()

        if not message or not amount or not profit:
            flash("All fields are required.", "warning")
            return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

        try:
            amount = float(amount)
            profit = float(profit)
        except ValueError:
            flash("Amount and Profit must be numeric.", "danger")
            return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

        # Once a type price is configured it is authoritative for new inventory.
        amount = prices.get(checker_type) or amount
        wassce_col.insert_one({
            "message": message,
            "amount": amount,
            "profit": profit,
            "status": "not_sold",
            "type": checker_type,
            "created_at": datetime.utcnow()
        })

        flash(f"{checker_type.upper()} checker added successfully!", "success")
        return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

    # Handle update
    if request.method == "POST" and request.form.get("action") == "update":
        checker_id = request.form.get("checker_id")
        if checker_id:
            try:
                updated_type = request.form.get("type", "").lower()
                updated_amount = prices.get(updated_type) or float(request.form.get("amount"))
                wassce_col.update_one(
                    {"_id": ObjectId(checker_id)},
                    {
                        "$set": {
                            "message": request.form.get("message", "").strip(),
                            "amount": updated_amount,
                            "profit": float(request.form.get("profit")),
                            "type": updated_type,
                        }
                    }
                )
                flash("Checker updated successfully!", "success")
            except Exception as e:
                flash(f"Error updating checker: {str(e)}", "danger")
        return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

    # Handle delete single
    if request.args.get("delete_id"):
        try:
            wassce_col.delete_one({"_id": ObjectId(request.args.get("delete_id"))})
            flash("Checker deleted successfully!", "success")
        except Exception as e:
            flash(f"Error deleting checker: {str(e)}", "danger")
        return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

    # Handle delete all sold
    if request.args.get("delete_sold") == "1":
        result = wassce_col.delete_many({"status": "sold"})
        flash(f"Deleted {result.deleted_count} sold checkers.", "info")
        return redirect(url_for("admin_wassce_checker.admin_wassce_checker"))

    # Filters from GET params
    filter_status = request.args.get("status")
    filter_type = request.args.get("type")

    query = {}
    if filter_status in ["sold", "not_sold"]:
        query["status"] = filter_status
    if filter_type in ["wassce", "bece"]:
        query["type"] = filter_type

    messages = list(wassce_col.find(query).sort("created_at", -1))
    _add_delivery_badges(messages)
    for message in messages:
        if message.get("status") != "sold":
            message["sale_channel_label"] = ""
            message["sale_channel_class"] = ""
        elif message.get("sold_channel") == "public_results_checker":
            message["sale_channel_label"] = "Results Checker Page"
            message["sale_channel_class"] = "public"
        elif message.get("sold_to_store") or message.get("sold_channel") == "store_page":
            message["sale_channel_label"] = "Store Page"
            message["sale_channel_class"] = "store"
        else:
            message["sale_channel_label"] = "Customer Dashboard"
            message["sale_channel_class"] = "dashboard"

    return render_template(
        "admin_wassce_checker.html",
        messages=messages,
        selected_status=filter_status,
        selected_type=filter_type,
        checker_prices=prices,
    )
