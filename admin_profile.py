from __future__ import annotations

from datetime import datetime

from bson import ObjectId
from flask import Blueprint, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from db import db

admin_profile_bp = Blueprint("admin_profile", __name__)
users_col = db["users"]


def _normalize_ghana_phone(raw: str) -> str | None:
    p = (raw or "").strip().replace(" ", "").replace("-", "").replace("+", "")
    if p.startswith("233") and len(p) == 12:
        return "0" + p[3:]
    if p.startswith("0") and len(p) == 10:
        return p
    return None


@admin_profile_bp.route("/admin/profile", methods=["GET", "POST"])
def admin_profile():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("login.login"))

    admin = users_col.find_one({"_id": ObjectId(user_id), "role": "admin"})
    if not admin:
        flash("Admin account not found.", "danger")
        return redirect(url_for("login.login"))

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "password":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")

            if not check_password_hash(admin.get("password", ""), current_password):
                flash("Current password is incorrect.", "danger")
                return redirect(url_for("admin_profile.admin_profile"))

            if len(new_password) < 8 or not any(ch.isalpha() for ch in new_password) or not any(ch.isdigit() for ch in new_password):
                flash("New password must be at least 8 characters and include letters and numbers.", "danger")
                return redirect(url_for("admin_profile.admin_profile"))

            if new_password != confirm_password:
                flash("New passwords do not match.", "danger")
                return redirect(url_for("admin_profile.admin_profile"))

            users_col.update_one(
                {"_id": admin["_id"]},
                {"$set": {"password": generate_password_hash(new_password), "updated_at": datetime.utcnow()}},
            )
            flash("Admin password updated successfully.", "success")
            return redirect(url_for("admin_profile.admin_profile"))

        if action == "avatar":
            avatar_url = (request.form.get("avatar_url") or "").strip()
            if not avatar_url:
                flash("Enter an image URL first.", "danger")
                return redirect(url_for("admin_profile.admin_profile"))
            users_col.update_one(
                {"_id": admin["_id"]},
                {"$set": {"avatar_url": avatar_url, "updated_at": datetime.utcnow()}},
            )
            session["avatar_url"] = avatar_url
            session.modified = True
            flash("Admin avatar updated successfully.", "success")
            return redirect(url_for("admin_profile.admin_profile"))

        if action == "manual_topup":
            network = (request.form.get("manual_network") or "").strip()
            number = (request.form.get("manual_number") or "").strip()
            recipient_name = (request.form.get("manual_name") or "").strip()
            notification_number = (request.form.get("manual_notification_number") or "").strip()
            active = (request.form.get("manual_active") or "").strip().lower() == "on"
            paystack_active = (request.form.get("paystack_active") or "").strip().lower() == "on"

            if active and (not network or not number or not recipient_name):
                flash("Network, MoMo number, and recipient name are required when Manual Top Up is active.", "danger")
                return redirect(url_for("admin_profile.admin_profile", tab="manual-topup"))

            users_col.update_one(
                {"_id": admin["_id"]},
                {
                    "$set": {
                        "manual_topup": {
                            "active": active,
                            "network": network,
                            "number": number,
                            "name": recipient_name,
                            "notification_number": notification_number,
                            "updated_at": datetime.utcnow(),
                        },
                        "deposit_methods": {
                            "paystack_active": paystack_active,
                            "manual_topup_active": active,
                            "updated_at": datetime.utcnow(),
                        },
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            flash("Manual Top Up settings updated successfully.", "success")
            return redirect(url_for("admin_profile.admin_profile", tab="manual-topup"))

        if action == "order_sms":
            order_sms_number = (request.form.get("order_sms_number") or "").strip()
            normalized_number = _normalize_ghana_phone(order_sms_number)

            if order_sms_number and not normalized_number:
                flash("Enter a valid Ghana phone number for Order SMS alerts.", "danger")
                return redirect(url_for("admin_profile.admin_profile", tab="order-sms"))

            users_col.update_one(
                {"_id": admin["_id"]},
                {
                    "$set": {
                        "order_sms": {
                            "number": normalized_number or "",
                            "updated_at": datetime.utcnow(),
                        },
                        "updated_at": datetime.utcnow(),
                    }
                },
            )
            flash("Order SMS settings updated successfully.", "success")
            return redirect(url_for("admin_profile.admin_profile", tab="order-sms"))

        flash("Unsupported profile action.", "warning")
        return redirect(url_for("admin_profile.admin_profile"))

    return render_template("admin_profile.html", admin=admin)
