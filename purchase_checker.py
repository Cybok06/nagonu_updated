from __future__ import annotations

import random
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List

from bson.objectid import ObjectId
from flask import Blueprint, abort, flash, jsonify, make_response, redirect, render_template, request, session, url_for
from pymongo import ReturnDocument

from db import db


purchase_checker_bp = Blueprint("purchase_checker", __name__)

CHECKER_TYPES = ("wassce", "bece")

wassce_col = db["wassce_checker"]
balances_col = db["balances"]
users_col = db["users"]
purchase_history_col = db["purchase_history"]
stores_col = db["stores"]
ussd_checker_sales_col = db["ussd_results_checker_sales"]
public_checker_purchases_col = db["public_checker_purchases"]
transactions_col = db["transactions"]
checker_settings_col = db["results_checker_settings"]


def _configured_checker_price(checker_type: str, fallback: float = 0.0) -> float:
    row = checker_settings_col.find_one({"_id": "checker_prices"}, {"prices": 1}) or {}
    try:
        price = float((row.get("prices") or {}).get(checker_type) or 0)
    except Exception:
        price = 0.0
    return round(price if price > 0 else float(fallback or 0), 2)


def _public_inventory() -> List[Dict[str, Any]]:
    cards: List[Dict[str, Any]] = []
    descriptions = {
        "wassce": "Access your WASSCE result checker securely and instantly after payment.",
        "bece": "Purchase your BECE result checker and receive it immediately online.",
    }
    for checker_type in CHECKER_TYPES:
        sample = wassce_col.find_one(
            {"type": checker_type, "status": "not_sold"}, sort=[("created_at", 1)]
        )
        try:
            price = _configured_checker_price(
                checker_type, float((sample or {}).get("amount") or 0)
            )
        except Exception:
            price = 0.0
        cards.append({
            "type": checker_type,
            "label": checker_type.upper() + " Checker",
            "description": descriptions[checker_type],
            "price": price,
            "available": wassce_col.count_documents({"type": checker_type, "status": "not_sold"}),
        })
    return cards


def _public_result_url(token: str) -> str:
    return url_for("purchase_checker.public_checker_result", token=token)


@purchase_checker_bp.route("/results-checker", methods=["GET"])
def public_results_checker():
    # Lazy import avoids changing blueprint initialization order while sharing the
    # exact Paystack profile used by public store checkout.
    from routes.store_page import PAYSTACK_PUBLIC_KEY, _is_pk

    return render_template(
        "results_checker_public.html",
        checker_cards=_public_inventory(),
        paystack_pk=PAYSTACK_PUBLIC_KEY if _is_pk(PAYSTACK_PUBLIC_KEY) else "",
    )


@purchase_checker_bp.route("/api/results-checker/paystack-config", methods=["GET"])
def public_results_checker_paystack_config():
    """Use the exact live Paystack public-key profile used by store checkout."""
    from routes.store_page import PAYSTACK_PUBLIC_KEY, _is_pk

    public_key = str(PAYSTACK_PUBLIC_KEY or "").strip()
    if not _is_pk(public_key):
        return jsonify({"success": False, "message": "Payment is not configured."}), 503

    response = make_response(jsonify({"success": True, "public_key": public_key}))
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@purchase_checker_bp.route("/results-checker/checkout", methods=["POST"])
def public_results_checker_checkout():
    payload = request.get_json(silent=True) or {}
    checker_type = str(payload.get("checker_type") or "").strip().lower()
    reference = str(payload.get("reference") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    phone = re.sub(r"\D+", "", str(payload.get("phone") or ""))
    if checker_type not in CHECKER_TYPES:
        return jsonify({"success": False, "message": "Select a valid checker type."}), 400
    if not reference or not re.fullmatch(r"[A-Za-z0-9._-]{6,100}", reference):
        return jsonify({"success": False, "message": "Invalid payment reference."}), 400
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        return jsonify({"success": False, "message": "Enter a valid email address."}), 400
    if phone.startswith("233") and len(phone) == 12:
        phone = "0" + phone[3:]
    if not re.fullmatch(r"0\d{9}", phone):
        return jsonify({"success": False, "message": "Phone must be 10 digits, starting with 0."}), 400

    prior = public_checker_purchases_col.find_one({"paystack_reference": reference})
    if prior:
        return jsonify({
            "success": True, "message": "Checker already delivered.",
            "result_url": _public_result_url(str(prior.get("token") or "")),
            "idempotent": True,
        })

    if transactions_col.find_one({"reference": reference}, {"_id": 1}):
        return jsonify({"success": False, "message": "This payment reference has already been used."}), 409

    sample = wassce_col.find_one(
        {"type": checker_type, "status": "not_sold"}, sort=[("created_at", 1)]
    )
    if not sample:
        return jsonify({"success": False, "message": f"{checker_type.upper()} checker is out of stock."}), 409
    expected = _configured_checker_price(checker_type, float(sample.get("amount") or 0))
    if expected <= 0:
        return jsonify({"success": False, "message": "Checker price is not configured."}), 400

    from routes.store_page import _verify_paystack
    ok, verify_data, verify_message, _raw = _verify_paystack(reference)
    paid_pesewas = int((verify_data or {}).get("amount") or 0)
    currency = str((verify_data or {}).get("currency") or "").upper()
    if not ok:
        return jsonify({"success": False, "message": f"Payment verification failed: {verify_message}"}), 400
    if currency != "GHS" or paid_pesewas < int(round(expected * 100)):
        return jsonify({"success": False, "message": "Payment amount is less than the checker price."}), 400

    checker = wassce_col.find_one_and_update(
        {"type": checker_type, "status": "not_sold"},
        {"$set": {
            "status": "sold", "sold_to": phone, "sold_channel": "public_results_checker",
            "delivery_phone": phone,
            "sold_reference": reference, "sold_at": datetime.utcnow(),
        }},
        sort=[("created_at", 1)], return_document=ReturnDocument.AFTER,
    )
    if not checker:
        return jsonify({
            "success": False,
            "message": f"{checker_type.upper()} checker became unavailable. Contact support with reference {reference}.",
        }), 409

    token = uuid.uuid4().hex + uuid.uuid4().hex
    now = datetime.utcnow()
    purchase = {
        "token": token, "checker_id": checker.get("_id"), "checker_type": checker_type,
        "message": checker.get("message") or "", "amount": expected, "email": email,
        "phone": phone, "paystack_reference": reference, "created_at": now,
    }
    purchase_inserted = False
    try:
        public_checker_purchases_col.insert_one(purchase)
        purchase_inserted = True
        transactions_col.update_one(
            {"reference": reference, "source": "public_results_checker"},
            {"$setOnInsert": {
                "amount": expected, "reference": reference, "status": "success", "type": "payment",
                "source": "public_results_checker", "gateway": "Paystack", "currency": "GHS",
                "phone": phone, "email": email, "created_at": now, "verified_at": now,
            }}, upsert=True,
        )
    except Exception:
        if purchase_inserted:
            public_checker_purchases_col.delete_one({"token": token, "paystack_reference": reference})
        wassce_col.update_one(
            {"_id": checker["_id"], "sold_reference": reference},
            {"$set": {"status": "not_sold"}, "$unset": {"sold_to": "", "sold_channel": "", "sold_reference": "", "sold_at": "", "delivery_phone": ""}},
        )
        raise

    return jsonify({
        "success": True, "message": "Payment verified. Your checker is ready.",
        "result_url": _public_result_url(token),
    })


@purchase_checker_bp.route("/results-checker/result/<token>", methods=["GET"])
def public_checker_result(token: str):
    if not re.fullmatch(r"[a-f0-9]{64}", token or ""):
        abort(404)
    purchase = public_checker_purchases_col.find_one(
        {"token": token}, {"message": 1, "checker_type": 1, "amount": 1, "created_at": 1}
    )
    if not purchase:
        abort(404)
    return render_template("public_checker_result.html", purchase=purchase)


def _owner_store(user_id: ObjectId) -> Dict[str, Any] | None:
    return stores_col.find_one(
        {"owner_id": user_id, "status": {"$ne": "deleted"}},
        sort=[("updated_at", -1), ("created_at", -1)],
    )


def _inventory_summary() -> Dict[str, Dict[str, float]]:
    summary: Dict[str, Dict[str, float]] = {}
    for checker_type in CHECKER_TYPES:
        sample = wassce_col.find_one({"type": checker_type, "status": "not_sold"}, sort=[("created_at", 1)])
        try:
            cost_price = _configured_checker_price(
                checker_type, float((sample or {}).get("amount") or 0)
            )
        except Exception:
            cost_price = 0.0
        summary[checker_type] = {
            "available": wassce_col.count_documents({"type": checker_type, "status": "not_sold"}),
            "cost_price": cost_price,
        }
    return summary


def _store_checker_config(store: Dict[str, Any] | None, inventory: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    cfg = (store or {}).get("ussd_results_checker") or {}
    by_type = {str(row.get("type") or "").strip().lower(): row for row in cfg.get("items") or []}
    items: List[Dict[str, Any]] = []
    for checker_type in CHECKER_TYPES:
        row = by_type.get(checker_type) or {}
        items.append(
            {
                "type": checker_type,
                "label": checker_type.upper(),
                "enabled": bool(row.get("enabled")),
                "selling_price": round(float(row.get("selling_price") or 0), 2) if row.get("selling_price") not in (None, "") else 0.0,
                "cost_price": round(float((inventory.get(checker_type) or {}).get("cost_price") or 0), 2),
                "available": int((inventory.get(checker_type) or {}).get("available") or 0),
            }
        )
    return {
        "enabled": bool(cfg.get("enabled")),
        "items": items,
    }


def _ussd_checker_stats(store: Dict[str, Any] | None) -> Dict[str, Any]:
    if not store or not store.get("slug"):
        return {"total_purchased": 0, "total_profit": 0.0}
    pipeline = [
        {"$match": {"store_slug": store.get("slug"), "source": "ussd_results_checker"}},
        {
            "$group": {
                "_id": None,
                "total_purchased": {"$sum": 1},
                "total_profit": {"$sum": {"$toDouble": {"$ifNull": ["$profit_amount", 0]}}},
            }
        },
    ]
    row = next(iter(ussd_checker_sales_col.aggregate(pipeline)), None)
    if not row:
        return {"total_purchased": 0, "total_profit": 0.0}
    return {
        "total_purchased": int(row.get("total_purchased") or 0),
        "total_profit": round(float(row.get("total_profit") or 0), 2),
    }


@purchase_checker_bp.route("/purchase_checker", methods=["GET", "POST"])
def purchase_checker():
    if "user_id" not in session:
        return redirect(url_for("login.login"))

    user_id = ObjectId(session["user_id"])
    balance_doc = balances_col.find_one({"user_id": user_id})
    balance = float(balance_doc["amount"]) if balance_doc else 0.0
    store_doc = _owner_store(user_id) if session.get("role") == "customer" else None
    inventory = _inventory_summary()

    if request.method == "POST" and request.form.get("action") == "save_ussd_settings":
        if not store_doc:
            flash("Create a store first before enabling Results Checker on USSD.", "warning")
            return redirect(url_for("purchase_checker.purchase_checker"))

        overall_enabled = (request.form.get("ussd_enabled") or "").strip().lower() in {"1", "true", "on", "yes", "enabled"}
        items = []
        errors = []
        for checker_type in CHECKER_TYPES:
            enabled = (request.form.get(f"enabled_{checker_type}") or "").strip().lower() in {"1", "true", "on", "yes", "enabled"}
            raw_price = (request.form.get(f"selling_price_{checker_type}") or "").strip()
            try:
                selling_price = round(float(raw_price or 0), 2)
            except Exception:
                selling_price = -1
            cost_price = round(float((inventory.get(checker_type) or {}).get("cost_price") or 0), 2)
            available = int((inventory.get(checker_type) or {}).get("available") or 0)
            if enabled:
                if available <= 0:
                    errors.append(f"{checker_type.upper()} has no available inventory.")
                if selling_price <= 0:
                    errors.append(f"Enter a valid selling price for {checker_type.upper()}.")
                if cost_price > 0 and selling_price < cost_price:
                    errors.append(f"{checker_type.upper()} selling price cannot be below cost price.")
            items.append(
                {
                    "type": checker_type,
                    "enabled": bool(enabled),
                    "selling_price": max(0.0, selling_price if selling_price >= 0 else 0.0),
                    "updated_at": datetime.utcnow(),
                }
            )
        if errors:
            for msg in errors:
                flash(msg, "danger")
            return redirect(url_for("purchase_checker.purchase_checker"))

        stores_col.update_one(
            {"_id": store_doc["_id"], "owner_id": user_id},
            {
                "$set": {
                    "ussd_results_checker": {
                        "enabled": bool(overall_enabled),
                        "items": items,
                        "updated_at": datetime.utcnow(),
                    },
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        flash("USSD Results Checker settings saved.", "success")
        return redirect(url_for("purchase_checker.purchase_checker"))

    if request.method == "POST":
        checker_id = request.form.get("checker_id")
        checker = wassce_col.find_one({"_id": ObjectId(checker_id), "status": "not_sold"})
        if not checker:
            flash("Checker not available or already sold.", "danger")
            return redirect(url_for("purchase_checker.purchase_checker"))

        price = float(checker["amount"])
        if balance < price:
            flash("Insufficient balance. Please top up.", "danger")
            return redirect("http://127.0.0.1:5000/deposit")

        new_balance = balance - price
        balances_col.update_one(
            {"user_id": user_id},
            {"$set": {"amount": new_balance, "updated_at": datetime.utcnow()}},
        )
        wassce_col.update_one(
            {"_id": ObjectId(checker_id)},
            {"$set": {"status": "sold", "sold_to": str(user_id), "sold_channel": "customer_dashboard", "sold_at": datetime.utcnow()}},
        )
        purchase_history_col.insert_one(
            {
                "user_id": str(user_id),
                "checker_id": str(checker["_id"]),
                "type": checker.get("type", ""),
                "amount": price,
                "message": checker.get("message", ""),
                "purchased_at": datetime.utcnow(),
            }
        )

        flash("Purchase successful!", "success")
        return redirect(url_for("purchases.view_purchases"))

    selected_type = request.args.get("type")
    checkers = []
    if selected_type in CHECKER_TYPES:
        unsold = list(wassce_col.find({"type": selected_type, "status": "not_sold"}))
        if unsold:
            checkers = [random.choice(unsold)]

    return render_template(
        "purchase_checker.html",
        balance=balance,
        checkers=checkers,
        selected_type=selected_type,
        ussd_store=store_doc,
        ussd_checker_config=_store_checker_config(store_doc, inventory),
        ussd_checker_stats=_ussd_checker_stats(store_doc),
    )
