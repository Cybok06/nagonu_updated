from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from datetime import datetime
import uuid
from db import campus_db

admin_campus_balance_bp = Blueprint("admin_campus_balance", __name__)

provider_accounts_col = campus_db["provider_accounts"]
provider_transactions_col = campus_db["provider_transactions"]

ALLOWED_PROVIDERS = {"codecraft", "dataconnect", "portal02"}


def _is_admin() -> bool:
    return session.get("role") == "admin"


def _now():
    return datetime.utcnow()


def _to_float_safe(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _normalize_provider(p: str) -> str | None:
    if not p:
        return None
    p = p.strip().lower()
    if p in ALLOWED_PROVIDERS:
        return p
    return None


def _make_reference(provider: str) -> str:
    ts = _now().strftime("%Y%m%d%H%M%S")
    return f"PROV-{provider.upper()}-{ts}-{uuid.uuid4().hex[:6]}"


@admin_campus_balance_bp.route("/admin/campus-balances")
def view_campus_balances():
    if not _is_admin():
        return redirect(url_for("login.login"))

    provider_filter = _normalize_provider(request.args.get("provider") or "")
    try:
        limit = max(1, min(int(request.args.get("limit", "100")), 500))
    except Exception:
        limit = 100

    accounts = list(
        provider_accounts_col.find(
            {},
            {"provider": 1, "balance": 1, "created_at": 1, "updated_at": 1},
        ).sort("provider", 1)
    )
    accounts_map = {a.get("provider"): a for a in accounts}

    # Ensure all known providers exist in the UI
    cards = []
    for p in sorted(ALLOWED_PROVIDERS):
        acc = accounts_map.get(p) or {"provider": p, "balance": 0.0, "created_at": None, "updated_at": None}
        cards.append({
            "provider": p,
            "balance": _to_float_safe(acc.get("balance")),
            "created_at": acc.get("created_at"),
            "updated_at": acc.get("updated_at"),
        })

    tx_query = {}
    if provider_filter:
        tx_query["provider"] = provider_filter

    tx_cursor = (
        provider_transactions_col.find(
            tx_query,
            {
                "provider": 1,
                "amount": 1,
                "direction": 1,
                "reason": 1,
                "reference": 1,
                "order_id": 1,
                "line_index": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
        .limit(limit)
    )
    transactions = []
    for t in tx_cursor:
        transactions.append({
            "id": str(t.get("_id")),
            "provider": t.get("provider"),
            "amount": _to_float_safe(t.get("amount")),
            "direction": t.get("direction"),
            "reason": t.get("reason"),
            "reference": t.get("reference"),
            "order_id": t.get("order_id"),
            "line_index": t.get("line_index"),
            "created_at": t.get("created_at"),
        })

    return render_template(
        "admin_campus_balance.html",
        cards=cards,
        transactions=transactions,
        provider_filter=provider_filter or "",
        limit=limit,
    )


@admin_campus_balance_bp.route("/admin/campus-balances/deposit/<provider>", methods=["POST"])
def campus_deposit(provider):
    if not _is_admin():
        return redirect(url_for("login.login"))

    prov = _normalize_provider(provider)
    if not prov:
        flash("Invalid provider.", "danger")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    amount_raw = request.form.get("amount")
    note = (request.form.get("note") or "").strip()
    if not amount_raw:
        flash("Enter an amount to deposit.", "warning")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    try:
        amount = float(amount_raw)
        if amount <= 0:
            raise ValueError("amount <= 0")
    except Exception:
        flash("Deposit amount must be greater than zero.", "warning")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    acc = provider_accounts_col.find_one({"provider": prov})
    if not acc:
        acc = {
            "provider": prov,
            "balance": 0.0,
            "created_at": _now(),
            "updated_at": _now(),
        }
        provider_accounts_col.insert_one(acc)

    provider_accounts_col.update_one(
        {"provider": prov},
        {"$inc": {"balance": amount}, "$set": {"updated_at": _now()}},
    )

    reference = _make_reference(prov)
    provider_transactions_col.insert_one({
        "provider": prov,
        "amount": float(amount),
        "direction": "CREDIT",
        "reason": "MANUAL_TOPUP",
        "order_id": None,
        "reference": reference,
        "line_index": None,
        "dedupe_key": f"TOPUP:{reference}:{prov}:{float(amount)}",
        "created_at": _now(),
        "meta": {"note": note[:240]} if note else {},
    })

    flash("Deposit successful.", "success")
    return redirect(url_for("admin_campus_balance.view_campus_balances"))


@admin_campus_balance_bp.route("/admin/campus-balances/withdraw/<provider>", methods=["POST"])
def campus_withdraw(provider):
    if not _is_admin():
        return redirect(url_for("login.login"))

    prov = _normalize_provider(provider)
    if not prov:
        flash("Invalid provider.", "danger")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    amount_raw = request.form.get("amount")
    note = (request.form.get("note") or "").strip()
    if not amount_raw:
        flash("Enter an amount to withdraw.", "warning")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    try:
        amount = float(amount_raw)
        if amount <= 0:
            raise ValueError("amount <= 0")
    except Exception:
        flash("Withdrawal amount must be greater than zero.", "warning")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    acc = provider_accounts_col.find_one({"provider": prov})
    current = _to_float_safe(acc.get("balance")) if acc else 0.0
    new_balance = current - amount
    if new_balance < 0:
        flash("Insufficient funds: cannot withdraw more than current balance.", "danger")
        return redirect(url_for("admin_campus_balance.view_campus_balances"))

    if not acc:
        acc = {
            "provider": prov,
            "balance": 0.0,
            "created_at": _now(),
            "updated_at": _now(),
        }
        provider_accounts_col.insert_one(acc)

    provider_accounts_col.update_one(
        {"provider": prov},
        {"$set": {"balance": new_balance, "updated_at": _now()}},
    )

    reference = _make_reference(prov)
    provider_transactions_col.insert_one({
        "provider": prov,
        "amount": float(amount),
        "direction": "DEBIT",
        "reason": "MANUAL_WITHDRAW",
        "order_id": None,
        "reference": reference,
        "line_index": None,
        "dedupe_key": f"WITHDRAW:{reference}:{prov}:{float(amount)}",
        "created_at": _now(),
        "meta": {"note": note[:240]} if note else {},
    })

    flash("Withdrawal successful.", "success")
    return redirect(url_for("admin_campus_balance.view_campus_balances"))


@admin_campus_balance_bp.route("/admin/campus-balances/history/<provider>")
def campus_balance_history(provider):
    if not _is_admin():
        return jsonify({"success": False, "error": "unauthorized"}), 401

    prov = _normalize_provider(provider)
    if not prov:
        return jsonify({"success": False, "error": "invalid provider"}), 400

    try:
        logs = []
        cursor = (
            provider_transactions_col.find(
                {"provider": prov},
                {
                    "provider": 1,
                    "amount": 1,
                    "direction": 1,
                    "reason": 1,
                    "reference": 1,
                    "order_id": 1,
                    "line_index": 1,
                    "created_at": 1,
                },
            )
            .sort("created_at", -1)
            .limit(200)
        )
        for t in cursor:
            logs.append({
                "id": str(t.get("_id")),
                "provider": t.get("provider"),
                "amount": _to_float_safe(t.get("amount")),
                "direction": t.get("direction"),
                "reason": t.get("reason"),
                "reference": t.get("reference"),
                "order_id": t.get("order_id"),
                "line_index": t.get("line_index"),
                "created_at": t.get("created_at").strftime("%Y-%m-%d %H:%M") if t.get("created_at") else "",
            })
        return jsonify({"success": True, "logs": logs})
    except Exception:
        return jsonify({"success": False, "error": "Server error loading history"}), 500
