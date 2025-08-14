from flask import Blueprint, render_template, session, redirect, url_for, request
from bson import ObjectId
from db import db
import json, ast, re
from datetime import datetime

customer_dashboard_bp = Blueprint("customer_dashboard", __name__)
services_col = db["services"]
balances_col = db["balances"]
orders_col = db["orders"]
service_profits_col = db["service_profits"]  # per-customer overrides
users_col = db["users"]                      # for display name

# ---------- helpers ----------
_NUM = re.compile(r"^\s*-?\d+(\.\d+)?\s*$", re.IGNORECASE)
_GB = re.compile(r"(\d+(?:\.\d+)?)\s*GB\b", re.IGNORECASE)
_MB = re.compile(r"(\d+(?:\.\d+)?)\s*MB\b", re.IGNORECASE)
_PKG_TAIL = re.compile(r"\s*\(Pkg\s*\d+\)\s*$", re.IGNORECASE)

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
        # pretty print: 1GB not 1.00GB
        return f"{int(gb)}GB" if abs(gb - int(gb)) < 1e-9 else f"{gb:.2f}GB"
    return f"{int(v)}MB"

_mapping_like = re.compile(r"^\s*\{.*\}\s*$", re.DOTALL)

def _parse_value_field(value):
    """
    Accepts:
      - dict like {"id": 50, "volume": 20000}
      - Python-like string "{'id': 50, 'volume': 20000}"
      - raw string like "1GB" or "1000MB" or "GHS 160 — 1GB (Pkg 2)"
    Returns either dict or string.
    """
    if isinstance(value, dict) or value is None:
        return value
    if isinstance(value, str):
        vt = value.strip()
        if vt.startswith("{") and vt.endswith("}"):
            # try JSON first
            try:
                data = json.loads(vt)
                if isinstance(data, dict):
                    return data
            except Exception:
                # then tolerant Python-literal
                try:
                    if _mapping_like.match(vt):
                        data = ast.literal_eval(vt)
                        if isinstance(data, dict):
                            return data
                except Exception:
                    pass
        return vt
    return value

def _extract_volume_mb(value) -> float | None:
    """
    Try to get a numeric volume in MB for sorting/price display.
    Priority:
      - dict {"volume": <mb or gb-intended?>}
      - strings: "1GB", "1000MB", "... — 1GB", etc.
    If dict volume looks like GB (e.g., <= 100 and amount looks large), we still treat it literally (no guesses).
    """
    # dict case
    if isinstance(value, dict):
        vol = value.get("volume")
        if vol is None:
            return None
        # If it is clearly numeric, assume MB unless the surrounding schema is GB-only (we won't assume that).
        if isinstance(vol, (int, float)) or (_NUM.match(str(vol))):
            return float(vol)
        # Could be a string like "1GB"
        vol = str(vol)
        m = _GB.search(vol)
        if m:
            return float(m.group(1)) * 1000.0
        m = _MB.search(vol)
        if m:
            return float(m.group(1))
        return None

    # string case
    if isinstance(value, str):
        s = value
        m = _GB.search(s)
        if m:
            return float(m.group(1)) * 1000.0
        m = _MB.search(s)
        if m:
            return float(m.group(1))
        # sometimes value is just "1", which could be GB in your UI, but we will not guess
        return None

    return None

def _value_text_for_display(value):
    """Return a clean label: volume formatted w/o package id suffixes."""
    if isinstance(value, dict):
        vol = _extract_volume_mb(value)
        return _format_volume(vol) if vol is not None else "-"
    if isinstance(value, str):
        cleaned = _PKG_TAIL.sub("", value).strip()
        # If it contains a volume, normalize its print:
        vol = _extract_volume_mb(cleaned)
        return _format_volume(vol) if vol is not None else cleaned or "-"
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
    a = _to_float(amount)
    p = _to_float(profit_percent) or 0.0
    if a is None:
        return None
    return round(a + (a * (p / 100.0)), 2)

# ---- service ordering ----
# Desired top order; match loosely (case-insensitive)
PREFERRED_ORDER = [
    "MTN",
    "AT - iShare",
    "AT - BigTime",
    "AFA TALKTIME",
]

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _name_rank(name: str) -> int | None:
    """Return index in PREFERRED_ORDER if name matches (case-insensitive, hyphen/space tolerant)."""
    n = _norm(name)
    # allow small spelling variants like iSHare/iShare etc.
    for i, want in enumerate(PREFERRED_ORDER):
        if _norm(want) == n:
            return i
    # also try minimal normalization (remove repeated spaces)
    n2 = " ".join(n.split())
    for i, want in enumerate(PREFERRED_ORDER):
        if " ".join(_norm(want).split()) == n2:
            return i
    return None

def _created_ts(service_doc) -> float:
    """
    Return epoch SECONDS (float) for created_at (best effort).
    Accepts datetime, epoch seconds, or epoch milliseconds.
    """
    ca = service_doc.get("created_at")
    # datetime
    if isinstance(ca, datetime):
        return ca.timestamp()
    # numeric-like
    try:
        val = float(ca)
        # Heuristic: if it's very large, treat as milliseconds
        if val > 1e12:  # ~ Sat Nov 20 33658 🤭
            return val / 1000.0
        return val
    except Exception:
        return 0.0

def _service_priority_tuple(svc):
    """
    Order by:
      1) explicit numeric 'priority' (lower first) if present
      2) then preferred-name rank (MTN, iShare, BigTime, AFA)
      3) then 'display_order' if present (lower first)
      4) then newest first by created_at
      5) then name A→Z
    """
    # explicit numeric priority override from DB
    prio = _to_float(svc.get("priority"))
    prio = prio if prio is not None else float("inf")

    # preferred name rank
    name = svc.get("name") or ""
    nrank = _name_rank(name)
    nrank = nrank if nrank is not None else 10_000

    # optional display_order from DB
    display_order = _to_float(svc.get("display_order"))
    display_order = display_order if display_order is not None else float("inf")

    # created: newest first -> use negative timestamp
    ts = -_created_ts(svc)

    # tie-break alphabetically
    alpha = _norm(name)

    return (prio, nrank, display_order, ts, alpha)

def _display_name(user_doc):
    """Pick the best display name from users doc."""
    if not user_doc:
        return "Customer"
    for key in ("full_name", "name"):
        if user_doc.get(key):
            return str(user_doc[key]).strip()
    first = (user_doc.get("first_name") or "").strip()
    last  = (user_doc.get("last_name") or "").strip()
    if first or last:
        return (first + " " + last).strip()
    if user_doc.get("username"):
        return str(user_doc["username"]).strip()
    if user_doc.get("email"):
        return str(user_doc["email"]).split("@", 1)[0]
    return "Customer"

# ---------- make username/balance available in all templates ----------
@customer_dashboard_bp.app_context_processor
def inject_customer_globals():
    bal = 0.0
    uname = session.get("username")  # optional session hint
    try:
        if session.get("role") == "customer" and session.get("user_id"):
            uid = ObjectId(session["user_id"])
            # balance
            bal_doc = balances_col.find_one({"user_id": uid})
            if bal_doc and bal_doc.get("amount") is not None:
                bal = float(bal_doc["amount"])
            # name from users collection
            user_doc = users_col.find_one({"_id": uid}, {
                "full_name": 1, "name": 1, "first_name": 1, "last_name": 1, "username": 1, "email": 1
            })
            uname = _display_name(user_doc)
    except Exception:
        pass
    return {"customer_balance": bal, "customer_username": uname or "Customer"}

# ---------- route ----------
@customer_dashboard_bp.route("/customer/dashboard")
def customer_dashboard():
    if session.get("role") != "customer":
        return redirect(url_for("login.login"))

    user_id = session.get("user_id")
    if not user_id:
        return redirect(url_for("login.login"))
    user_oid = ObjectId(user_id)

    # fetch user (for direct use on dashboard)
    user_doc = users_col.find_one({"_id": user_oid}, {
        "full_name": 1, "name": 1, "first_name": 1, "last_name": 1, "username": 1, "email": 1
    })
    customer_name = _display_name(user_doc)

    # fetch & sort services with robust priority
    raw_services = list(services_col.find({}))
    raw_services.sort(key=_service_priority_tuple)

    services = []
    for s in raw_services:
        s["_id_str"] = str(s["_id"])
        eff_profit = _effective_profit_percent(s, user_oid)

        offers = s.get("offers") or []

        # normalize offers; sort by volume (asc), then amount (asc)
        normalized_offers = []
        for of in offers:
            parsed_value = _parse_value_field(of.get("value"))
            value_text = _value_text_for_display(parsed_value)

            amount = _to_float(of.get("amount"))
            total = _price_with_profit(amount, eff_profit) if amount is not None else None

            vol_mb = _extract_volume_mb(parsed_value)

            normalized_offers.append({
                "amount": amount,
                "value": parsed_value,
                "value_text": value_text,
                "legacy_profit": _to_float(of.get("profit")),
                "profit_percent_used": eff_profit,
                "total": total,
                "_sort_vol": vol_mb if vol_mb is not None else float("inf"),
                "_sort_amt": amount if amount is not None else float("inf"),
            })

        normalized_offers.sort(key=lambda x: (x["_sort_vol"], x["_sort_amt"]))

        s["offers"] = [
            {k: v for k, v in o.items() if not k.startswith("_sort_")}
            for o in normalized_offers
        ]
        s["effective_profit_percent"] = eff_profit
        services.append(s)

    # Balance
    balance_doc = balances_col.find_one({"user_id": user_oid})
    balance = float(balance_doc["amount"]) if (balance_doc and balance_doc.get("amount") is not None) else 0.00

    # Recent orders
    recent_orders = list(
        orders_col.find({"user_id": user_oid})
        .sort("created_at", -1)
        .limit(5)
    )

    return render_template(
        "customer_dashboard.html",
        services=services,
        balance=balance,
        recent_orders=recent_orders,
        customer_name=customer_name,
    )
