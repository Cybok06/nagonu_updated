from flask import Blueprint, render_template, session, redirect, url_for, request
from bson import ObjectId
from db import db
import json, ast, re
from datetime import datetime
from typing import Optional, Any, Dict, List  # <-- for Python < 3.10 union hints

customer_dashboard_bp = Blueprint("customer_dashboard", __name__)
services_col = db["services"]
balances_col = db["balances"]
orders_col = db["orders"]
service_profits_col = db["service_profits"]  # per-customer overrides
users_col = db["users"]                      # for display name
settings_col = db["settings"]                # <-- NEW: for AFA settings (price/open/stock)

# ---------- helpers ----------
_NUM = re.compile(r"^\s*-?\d+(\.\d+)?\s*$", re.IGNORECASE)
_GB  = re.compile(r"(\d+(?:\.\d+)?)[\s]*G(?:B|IG)?\b", re.IGNORECASE)
_MB  = re.compile(r"(\d+(?:\.\d+)?)[\s]*MB\b", re.IGNORECASE)
_MIN = re.compile(r"(\d+(?:\.\d+)?)[\s]*(?:MIN|MINS|MINUTE|MINUTES)\b", re.IGNORECASE)
_PKG_TAIL = re.compile(r"\s*\(Pkg\s*\d+\)\s*$", re.IGNORECASE)
_mapping_like = re.compile(r"^\s*\{.*\}\s*$", re.DOTALL)

def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

# ---- unit helpers ------------------------------------------------------------

def _service_unit(svc: Dict[str, Any]) -> str:
    """
    Returns the unit for a service:
      - 'minutes' for AFA talktime (by name or optional svc['unit']=='minutes')
      - 'data' (MB/GB) for everything else
    """
    unit = (svc.get("unit") or "").strip().lower()
    name = (svc.get("name") or "").strip().lower()
    if unit in ("min", "mins", "minute", "minutes"):
        return "minutes"
    if name == "afa talktime":
        return "minutes"
    return "data"

def _format_volume_unit(value: Optional[float], unit: str) -> str:
    if value is None:
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"
    if unit == "minutes":
        return f"{int(round(v))} mins"
    # default 'data': MB
    if v >= 1000:
        gb = v / 1000.0
        return f"{int(gb)}GB" if abs(gb - int(gb)) < 1e-9 else f"{gb:.2f}GB"
    return f"{int(v)}MB"

def _parse_value_field(value: Any) -> Any:
    """
    Accepts:
      - dict like {"id": 50, "volume": 20000}
      - Python-like string "{'id': 50, 'volume': 20000}"
      - raw string like "1GB" or "1000MB" or "250 MIN"
      - display string like "GHS 160 — 1GB (Pkg 2)"
    Returns either dict (preferred) or the original string.
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

def _extract_volume(value: Any, unit: str) -> Optional[float]:
    """Return numeric volume for sorting (MB for data, minutes for talktime)."""
    if isinstance(value, dict):
        vol = value.get("volume")
        if vol is None:
            return None
        if isinstance(vol, (int, float)) or (_NUM.match(str(vol))):
            return float(vol)
        # textual volume
        vol_s = str(vol)
        if unit == "minutes":
            m = _MIN.search(vol_s)
            if m: return float(m.group(1))
            if _NUM.match(vol_s): return float(vol_s)
            return None
        else:
            m = _GB.search(vol_s)
            if m: return float(m.group(1)) * 1000.0
            m = _MB.search(vol_s)
            if m: return float(m.group(1))
            if _NUM.match(vol_s): return float(vol_s)  # assume MB
            return None

    if isinstance(value, str):
        s = value
        if unit == "minutes":
            m = _MIN.search(s)
            if m: return float(m.group(1))
            if _NUM.match(s): return float(s)
            s2 = _PKG_TAIL.sub("", s)
            m = _MIN.search(s2)
            if m: return float(m.group(1))
            return None
        else:
            m = _GB.search(s)
            if m: return float(m.group(1)) * 1000.0
            m = _MB.search(s)
            if m: return float(m.group(1))
            s2 = _PKG_TAIL.sub("", s)
            m = _GB.search(s2)
            if m: return float(m.group(1)) * 1000.0
            m = _MB.search(s2)
            if m: return float(m.group(1))
            if _NUM.match(s2): return float(s2)  # assume MB
            return None
    return None

def _value_text_for_display(value: Any, unit: str) -> str:
    if isinstance(value, dict):
        vol = _extract_volume(value, unit)
        return _format_volume_unit(vol, unit) if vol is not None else "-"
    if isinstance(value, str):
        cleaned = _PKG_TAIL.sub("", value).strip()
        vol = _extract_volume(cleaned, unit)
        return _format_volume_unit(vol, unit) if vol is not None else (cleaned or "-")
    return value or "-"

def _get_service_default_profit(service_doc: Dict[str, Any]) -> float:
    return _to_float(service_doc.get("default_profit_percent")) or 0.0

def _get_customer_profit_override(service_id: ObjectId, customer_id_obj: ObjectId) -> Optional[float]:
    ov = service_profits_col.find_one({"service_id": service_id, "customer_id": customer_id_obj})
    return _to_float(ov.get("profit_percent")) if ov else None

def _effective_profit_percent(service_doc: Dict[str, Any], customer_id_obj: ObjectId) -> float:
    override = _get_customer_profit_override(service_doc["_id"], customer_id_obj)
    return override if override is not None else _get_service_default_profit(service_doc)

def _price_with_profit(amount: Optional[float], profit_percent: Optional[float]) -> Optional[float]:
    a = _to_float(amount)
    p = _to_float(profit_percent) or 0.0
    if a is None:
        return None
    return round(a + (a * (p / 100.0)), 2)

# ---- service ordering ----
PREFERRED_ORDER: List[str] = [
    "MTN",
    "AT - iShare",
    "AT - BigTime",
    "AFA TALKTIME",
]

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _name_rank(name: str) -> Optional[int]:
    n = _norm(name)
    for i, want in enumerate(PREFERRED_ORDER):
        if _norm(want) == n:
            return i
    n2 = " ".join(n.split())
    for i, want in enumerate(PREFERRED_ORDER):
        if " ".join(_norm(want).split()) == n2:
            return i
    return None

def _created_ts(service_doc: Dict[str, Any]) -> float:
    ca = service_doc.get("created_at")
    if isinstance(ca, datetime):
        return ca.timestamp()
    try:
        val = float(ca)
        if val > 1e12:
            return val / 1000.0
        return val
    except Exception:
        return 0.0

def _service_priority_tuple(svc: Dict[str, Any]):
    prio = _to_float(svc.get("priority"))
    prio = prio if prio is not None else float("inf")
    name = svc.get("name") or ""
    nrank = _name_rank(name)
    nrank = nrank if nrank is not None else 10_000
    display_order = _to_float(svc.get("display_order"))
    display_order = display_order if display_order is not None else float("inf")
    ts = -_created_ts(svc)
    alpha = _norm(name)
    return (prio, nrank, display_order, ts, alpha)

def _display_name(user_doc: Optional[Dict[str, Any]]) -> str:
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

# ---- new: service-state helper ------------------------------------------------

def _service_state(svc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize flags + derive if the service can be ordered.
    """
    t = (svc.get("type") or "API").upper()
    status = (svc.get("status") or "OPEN").upper()               # OPEN | CLOSED
    availability = (svc.get("availability") or "AVAILABLE").upper()  # AVAILABLE | OUT_OF_STOCK

    # optional custom messages stored on the service doc
    closed_msg = (svc.get("closed_message") or "This service is temporarily closed.")
    oos_msg = (svc.get("out_of_stock_message") or "This service is currently out of stock.")

    can_order = (t == "API" and status == "OPEN" and availability == "AVAILABLE")

    disabled_reason = None
    if not can_order:
        if status != "OPEN":
            disabled_reason = closed_msg
        elif availability != "AVAILABLE":
            disabled_reason = oos_msg
        elif t != "API":
            disabled_reason = "This service is currently unavailable."

    return {
        "type": t,
        "status": status,
        "availability": availability,
        "closed_message": closed_msg,
        "out_of_stock_message": oos_msg,
        "can_order": can_order,
        "disabled_reason": disabled_reason
    }

# ---------- AFA settings loader (price / open / stock) ----------
def _load_afa_settings() -> Dict[str, Any]:
    """
    Reads configurable AFA price/open/stock from db.settings.
    Falls back to defaults and to flags on 'AFA TALKTIME' service if present.
    """
    defaults: Dict[str, Any] = {
        "price": 2.00,
        "is_open": True,
        "in_stock": True,
        "status": "OPEN",
        "availability": "AVAILABLE",
        "disabled_reason": "This service is currently unavailable."
    }
    doc = settings_col.find_one({"key": "afa_settings"}) or settings_col.find_one({"key": "afa"})
    if doc:
        price = _to_float(doc.get("price"))
        if price is not None:
            defaults["price"] = price
        is_open = bool(doc.get("is_open", True))
        in_stock = bool(doc.get("in_stock", True))
        defaults["is_open"] = is_open
        defaults["in_stock"] = in_stock
        defaults["status"] = "OPEN" if is_open else "CLOSED"
        defaults["availability"] = "AVAILABLE" if in_stock else "OUT_OF_STOCK"
        if doc.get("disabled_reason"):
            defaults["disabled_reason"] = str(doc["disabled_reason"])

    # Optional: reflect flags from a service named AFA TALKTIME
    svc = services_col.find_one({"name": {"$regex": r"^\s*AFA\s+TALKTIME\s*$", "$options": "i"}})
    if svc:
        st = _service_state(svc)
        defaults["status"] = st["status"]
        defaults["availability"] = st["availability"]
        defaults["is_open"] = (st["status"] == "OPEN")
        defaults["in_stock"] = (st["availability"] == "AVAILABLE")
        if st.get("disabled_reason"):
            defaults["disabled_reason"] = st["disabled_reason"]

    return defaults

# ---------- globals ----------
@customer_dashboard_bp.app_context_processor
def inject_customer_globals():
    bal = 0.0
    uname = session.get("username")
    try:
        if session.get("role") == "customer" and session.get("user_id"):
            uid = ObjectId(session["user_id"])
            bal_doc = balances_col.find_one({"user_id": uid})
            if bal_doc and bal_doc.get("amount") is not None:
                bal = float(bal_doc["amount"])
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

    # user doc
    user_doc = users_col.find_one({"_id": user_oid}, {
        "full_name": 1, "name": 1, "first_name": 1, "last_name": 1, "username": 1, "email": 1
    })
    customer_name = _display_name(user_doc)

    # services (sorted)
    raw_services = list(services_col.find({}))
    raw_services.sort(key=_service_priority_tuple)

    services: List[Dict[str, Any]] = []
    for s in raw_services:
        s["_id_str"] = str(s["_id"])
        eff_profit = _effective_profit_percent(s, user_oid)

        # attach flags/state for UI
        st = _service_state(s)
        s.update(st)  # type, status, availability, can_order, disabled_reason, messages

        unit = _service_unit(s)  # minutes for AFA TALKTIME, data otherwise
        offers = s.get("offers") or []

        normalized_offers: List[Dict[str, Any]] = []
        for of in offers:
            parsed_value = _parse_value_field(of.get("value"))
            vol_num = _extract_volume(parsed_value, unit)  # for sorting
            value_text = _value_text_for_display(parsed_value, unit)

            amount = _to_float(of.get("amount"))
            total = _price_with_profit(amount, eff_profit) if amount is not None else None

            normalized_offers.append({
                "amount": amount,
                "value": parsed_value,
                "value_text": value_text,
                "legacy_profit": _to_float(of.get("profit")),
                "profit_percent_used": eff_profit,
                "total": total,
                "_sort_vol": vol_num if vol_num is not None else float("inf"),
                "_sort_amt": amount if amount is not None else float("inf"),
            })

        # sort by volume asc, then amount asc
        normalized_offers.sort(key=lambda x: (x["_sort_vol"], x["_sort_amt"]))
        s["offers"] = [{k: v for k, v in o.items() if not k.startswith("_sort_")} for o in normalized_offers]
        s["effective_profit_percent"] = eff_profit
        s["unit"] = unit

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

    # ---- split into categories (Express vs others) ----
    def _is_express(svc: Dict[str, Any]) -> bool:
        cat = (svc.get("service_category") or "").strip().lower()
        cat2 = (svc.get("category") or "").strip().lower()
        return cat == "express services" or cat2 == "express"

    express_services = [s for s in services if _is_express(s)]
    regular_services = [s for s in services if not _is_express(s)]

    # NEW: AFA settings (price / open / stock) for template
    afa = _load_afa_settings()

    return render_template(
        "customer_dashboard.html",
        services=regular_services,         # keep old variable working for existing section
        express_services=express_services, # NEW
        balance=balance,
        recent_orders=recent_orders,
        customer_name=customer_name,
        afa=afa,                           # pass settings for the AFA block in your HTML
    )
