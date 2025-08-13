from flask import Blueprint, render_template, session, redirect, url_for
from bson import ObjectId
from db import db
import json, ast, re

customer_dashboard_bp = Blueprint("customer_dashboard", __name__)
services_col = db["services"]
balances_col = db["balances"]
orders_col = db["orders"]
service_profits_col = db["service_profits"]  # <-- per-customer overrides

# ---------- helpers ----------
def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _format_volume(vol_mb):
    try:
        v = float(vol_mb)
    except Exception:
        return "-"
    if v >= 1000:
        gb = v / 1000.0
        return f"{int(round(gb))}GB" if abs(gb - round(gb)) < 1e-9 else f"{gb:.2f}GB"
    return f"{int(v)}MB"

_mapping_like = re.compile(r"^\s*\{.*\}\s*$", re.DOTALL)

def _parse_value_field(value):
    """
    Accepts:
      - dict like {"id": 50, "volume": 20000}
      - Python-like string "{'id': 50, 'volume': 20000}"
      - raw string "1GB"
    Returns either dict or string.
    """
    if isinstance(value, dict) or value is None:
        return value
    if isinstance(value, str):
        vt = value.strip()
        # JSON first
        if vt.startswith("{") and vt.endswith("}"):
            # try JSON
            try:
                data = json.loads(vt)
                if isinstance(data, dict):
                    return data
            except Exception:
                # try safe eval for python-like dicts
                try:
                    if _mapping_like.match(vt):
                        data = ast.literal_eval(vt)
                        if isinstance(data, dict):
                            return data
                except Exception:
                    pass
        return value  # plain string like "1GB"
    return value

def _value_text_for_display(value):
    """
    Return a clean label for the offer's value with NO package id.
    Accepts a dict like {'volume': 2000, 'id': 5} or a plain string like '1GB (Pkg 5)'.
    """
    if isinstance(value, dict):
        vol = value.get("volume")
        # show only the size (e.g., '2GB' or '500MB'), never the package id
        return _format_volume(vol) if vol is not None else "-"

    if isinstance(value, str):
        # if any '(Pkg 123)' text sneaks in, strip it
        cleaned = re.sub(r"\s*\(Pkg\s*\d+\)\s*$", "", value, flags=re.IGNORECASE).strip()
        return cleaned or "-"

    return value or "-"


def _get_service_default_profit(service_doc):
    return _to_float(service_doc.get("default_profit_percent")) or 0.0

def _get_customer_profit_override(service_id, customer_id_obj):
    ov = service_profits_col.find_one({"service_id": service_id, "customer_id": customer_id_obj})
    return _to_float(ov.get("profit_percent")) if ov else None

def _effective_profit_percent(service_doc, customer_id_obj):
    override = _get_customer_profit_override(service_doc["_id"], customer_id_obj)
    return override if override is not None else _get_service_default_profit(service_doc)

def _price_with_profit(amount, profit_percent):
    """
    total = amount + (amount * profit%)
    """
    a = _to_float(amount)
    p = _to_float(profit_percent) or 0.0
    if a is None:
        return None
    return round(a + (a * (p / 100.0)), 2)

# ---------- route ----------
@customer_dashboard_bp.route("/customer/dashboard")
def customer_dashboard():
    if session.get("role") != "customer":
        return redirect(url_for("login.login"))

    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("login.login"))
    user_oid = ObjectId(user_id)

    # fetch services newest first
    services = list(services_col.find().sort("created_at", -1))

    for s in services:
        s["_id_str"] = str(s["_id"])
        eff_profit = _effective_profit_percent(s, user_oid)

        # normalize & compute per-offer totals
        offers = s.get("offers") or []
        new_offers = []
        for of in offers:
            # parse value
            parsed_value = _parse_value_field(of.get("value"))
            value_text = _value_text_for_display(parsed_value)

            # amount & total
            amount = _to_float(of.get("amount"))
            total = _price_with_profit(amount, eff_profit) if amount is not None else None

            new_offers.append({
                "amount": amount,
                "value": parsed_value,      # keep original (now normalized)
                "value_text": value_text,   # always present for UI
                "legacy_profit": _to_float(of.get("profit")),  # legacy per-offer profit if present (unused in calc)
                "profit_percent_used": eff_profit,             # what we used for calc
                "total": total                                  # final price for customer
            })
        s["offers"] = new_offers
        s["effective_profit_percent"] = eff_profit  # handy if you want to show "Your profit rate"

    # Balance
    balance_doc = balances_col.find_one({"user_id": ObjectId(user_id)})
    balance = balance_doc["amount"] if balance_doc else 0.00

    # Recent orders
    recent_orders = list(
        orders_col.find({"user_id": ObjectId(user_id)})
        .sort("created_at", -1)
        .limit(5)
    )

    return render_template(
        "customer_dashboard.html",
        services=services,
        balance=balance,
        recent_orders=recent_orders
    )
