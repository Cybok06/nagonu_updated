from __future__ import annotations

from datetime import datetime, timedelta
import uuid
from typing import Dict, Any

from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from bson import Regex

from db import campus_db

admin_campus_balance_bp = Blueprint("admin_campus_balance", __name__, url_prefix="/admin")

provider_accounts_col = campus_db["provider_accounts"]
provider_transactions_col = campus_db["provider_transactions"]

PROVIDER_NAME = "provider_wallet"
DEFAULT_PER_PAGE = 20


def _now() -> datetime:
    return datetime.utcnow()


def _require_admin() -> bool:
    return session.get("role") in {"admin", "superadmin"}


def _parse_date(dstr: str | None) -> datetime | None:
    if not dstr:
        return None
    try:
        s = dstr.strip()
        if len(s) <= 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except Exception:
        return None


def _build_query_from_params(args) -> Dict[str, Any]:
    direction = (args.get("direction") or "").strip().upper()
    reason_q = (args.get("reason") or "").strip()
    ref_q = (args.get("ref") or "").strip()
    date_from = _parse_date((args.get("date_from") or "").strip())
    date_to_raw = _parse_date((args.get("date_to") or "").strip())
    date_to = datetime(date_to_raw.year, date_to_raw.month, date_to_raw.day) + timedelta(days=1) if date_to_raw else None

    query: Dict[str, Any] = {"provider": PROVIDER_NAME}

    if direction in {"CREDIT", "DEBIT"}:
        query["direction"] = direction

    if reason_q:
        query["reason"] = Regex(reason_q, "i")

    if ref_q:
        rx = Regex(ref_q, "i")
        query["$or"] = [{"reference": rx}, {"order_id": rx}]

    if date_from or date_to:
        dt = {}
        if date_from:
            dt["$gte"] = date_from
        if date_to:
            dt["$lt"] = date_to
        query["created_at"] = dt

    return query


def _make_reference(prefix: str) -> str:
    ts = _now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}-{ts}-{uuid.uuid4().hex[:6].upper()}"


def _to_amount(v) -> float | None:
    try:
        amt = float(v)
        if amt <= 0:
            return None
        return round(amt, 2)
    except Exception:
        return None


@admin_campus_balance_bp.route("/campus-balance", methods=["GET"])
def view_campus_balance():
    if not _require_admin():
        return redirect(url_for("login.login"))

    try:
        per_page = int(request.args.get("per_page", DEFAULT_PER_PAGE))
        per_page = max(1, min(per_page, 100))
    except Exception:
        per_page = DEFAULT_PER_PAGE

    try:
        page = int(request.args.get("page", 1))
        page = max(1, page)
    except Exception:
        page = 1

    skip = (page - 1) * per_page
    query = _build_query_from_params(request.args)

    acct = provider_accounts_col.find_one({"provider": PROVIDER_NAME}) or {}
    balance = float(acct.get("balance") or 0.0)

    total_count = provider_transactions_col.count_documents(query)
    total_pages = max(1, (total_count + per_page - 1) // per_page)

    txns = list(
        provider_transactions_col.find(query).sort("created_at", -1).skip(skip).limit(per_page)
    )
    for t in txns:
        t["meta"] = t.get("meta") or {}

    return render_template(
        "campus_balance.html",
        balance=balance,
        transactions=txns,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        direction=(request.args.get("direction") or "all").strip().lower(),
        date_from=(request.args.get("date_from") or "").strip(),
        date_to=(request.args.get("date_to") or "").strip(),
        reason_q=(request.args.get("reason") or "").strip(),
        ref_q=(request.args.get("ref") or "").strip(),
        per_page=per_page,
    )


@admin_campus_balance_bp.route("/campus-balance/deposit", methods=["POST"])
def campus_balance_deposit():
    if not _require_admin():
        return redirect(url_for("login.login"))

    amount = _to_amount(request.form.get("amount"))
    note = (request.form.get("note") or "").strip()
    if amount is None:
        flash("Enter a valid deposit amount.", "warning")
        return redirect(url_for("admin_campus_balance.view_campus_balance"))

    now = _now()
    res = provider_accounts_col.update_one(
        {"provider": PROVIDER_NAME},
        {
            "$inc": {"balance": amount},
            "$set": {"updated_at": now},
            "$setOnInsert": {"created_at": now, "provider": PROVIDER_NAME},
        },
        upsert=True,
    )

    if not res.acknowledged:
        flash("Deposit failed. Please try again.", "danger")
        return redirect(url_for("admin_campus_balance.view_campus_balance"))

    reference = _make_reference("PROV-PROVIDER_WALLET")
    dedupe_key = f"TOPUP:{reference}:{PROVIDER_NAME}:{amount:.2f}"
    provider_transactions_col.insert_one(
        {
            "provider": PROVIDER_NAME,
            "amount": amount,
            "direction": "CREDIT",
            "reason": "MANUAL_TOPUP",
            "order_id": None,
            "line_index": None,
            "reference": reference,
            "dedupe_key": dedupe_key,
            "created_at": now,
            "meta": {
                "note": note,
                "actor_admin_id": session.get("user_id"),
                "method": "MANUAL",
            },
        }
    )

    flash("Deposit successful.", "success")
    return redirect(url_for("admin_campus_balance.view_campus_balance"))


@admin_campus_balance_bp.route("/campus-balance/withdraw", methods=["POST"])
def campus_balance_withdraw():
    if not _require_admin():
        return redirect(url_for("login.login"))

    amount = _to_amount(request.form.get("amount"))
    note = (request.form.get("note") or "").strip()
    if amount is None:
        flash("Enter a valid withdrawal amount.", "warning")
        return redirect(url_for("admin_campus_balance.view_campus_balance"))

    now = _now()
    res = provider_accounts_col.update_one(
        {"provider": PROVIDER_NAME, "balance": {"$gte": amount}},
        {"$inc": {"balance": -amount}, "$set": {"updated_at": now}},
        upsert=True,
    )

    if res.upserted_id is not None:
        # rollback unexpected insert (treat as insufficient funds)
        try:
            provider_accounts_col.delete_one({"_id": res.upserted_id})
        except Exception:
            pass
        flash("Insufficient funds for this withdrawal.", "danger")
        return redirect(url_for("admin_campus_balance.view_campus_balance"))

    if res.modified_count == 0:
        flash("Insufficient funds for this withdrawal.", "danger")
        return redirect(url_for("admin_campus_balance.view_campus_balance"))

    reference = _make_reference("PROV-WITHDRAW")
    dedupe_key = f"WITHDRAW:{reference}:{PROVIDER_NAME}:{amount:.2f}"
    provider_transactions_col.insert_one(
        {
            "provider": PROVIDER_NAME,
            "amount": amount,
            "direction": "DEBIT",
            "reason": "MANUAL_WITHDRAW",
            "order_id": None,
            "line_index": None,
            "reference": reference,
            "dedupe_key": dedupe_key,
            "created_at": now,
            "meta": {
                "note": note,
                "actor_admin_id": session.get("user_id"),
                "method": "MANUAL",
            },
        }
    )

    flash("Withdrawal successful.", "success")
    return redirect(url_for("admin_campus_balance.view_campus_balance"))
