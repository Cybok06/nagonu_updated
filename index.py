# index.py — Public landing with buy flow charging a single house account
from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import json, ast, re, traceback, os
import requests  # Paystack verify

from db import db

# --- Reuse helpers from checkout.py ---
try:
    from checkout import (
        # pricing / base-profit helpers
        _effective_profit_percent, _pick_offer_base_amount_from_service,
        _derive_base_profit, _coerce_value_obj, _to_float, _money, generate_order_id,
        # provider field resolvers + callers
        _resolve_network_id, _resolve_shared_bundle_express, _resolve_shared_bundle_toppily,
        _send_express_single, _send_toppily_shared_bundle, _service_unavailability_reason,
        # tiny logger
        jlog,
    )
except Exception:  # pragma: no cover
    from .checkout import (  # type: ignore
        _effective_profit_percent, _pick_offer_base_amount_from_service,
        _derive_base_profit, _coerce_value_obj, _to_float, _money, generate_order_id,
        _resolve_network_id, _resolve_shared_bundle_express, _resolve_shared_bundle_toppily,
        _send_express_single, _send_toppily_shared_bundle, _service_unavailability_reason,
        jlog,
    )

index_bp = Blueprint("index", __name__)

# --- DB collections ---
services_col = db["services"]
balances_col = db["balances"]
orders_col = db["orders"]
transactions_col = db["transactions"]
service_profits_col = db["service_profits"]
users_col = db["users"]

# --- The house account to be charged for public purchases ---
# You can set HOUSE_USER_ID via env var HOUSE_USER_ID (as a Mongo ObjectId string) if needed.
HOUSE_USER_ID = ObjectId(os.getenv("HOUSE_USER_ID", "6892c12eaecf4fd8d6fce9e6"))

# --- Paystack keys from env ---
PAYSTACK_PUBLIC_KEY = os.getenv("PAYSTACK_PUBLIC_KEY", "")
PAYSTACK_SECRET_KEY = os.getenv("PAYSTACK_SECRET_KEY", "")

# ---------------- helpers (mirrors customer_dashboard normalizers) ----------------
_NUM = re.compile(r"^\s*-?\d+(\.\d+)?\s*$", re.IGNORECASE)
_GB  = re.compile(r"(\d+(?:\.\d+)?)[\s]*G(?:B|IG)?\b", re.IGNORECASE)
_MB  = re.compile(r"(\d+(?:\.\d+)?)[\s]*MB\b", re.IGNORECASE)
_MIN = re.compile(r"(\d+(?:\.\d+)?)[\s]*(?:MIN|MINS|MINUTE|MINUTES)\b", re.IGNORECASE)
_PKG_TAIL = re.compile(r"\s*\(Pkg\s*\d+\)\s*$", re.IGNORECASE)
_mapping_like = re.compile(r"^\s*\{.*\}\s*$", re.DOTALL)

def _service_unit(svc: Dict[str, Any]) -> str:
    unit = (svc.get("unit") or "").strip().lower()
    name = (svc.get("name") or "").strip().lower()
    if unit in ("min", "mins", "minute", "minutes"):
        return "minutes"
    if name == "afa talktime":
        return "minutes"
    return "data"

def _parse_value_field(value: Any) -> Any:
    if isinstance(value, dict) or value is None:
        return value
    if isinstance(value, str):
        vt = value.strip()
        if vt.startswith("{") and vt.endswith("}"):
            try:
                data = json.loads(vt)
                if isinstance(data, dict):
                    return data
            except Exception:
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
    if isinstance(value, dict):
        vol = value.get("volume")
        if vol is None:
            return None
        if isinstance(vol, (int, float)) or (_NUM.match(str(vol))):
            return float(vol)
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
            if _NUM.match(vol_s): return float(vol_s)
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
            if _NUM.match(s2): return float(s2)
            return None
    return None

def _format_volume_unit(value: Optional[float], unit: str) -> str:
    if value is None:
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"
    if unit == "minutes":
        return f"{int(round(v))} mins"
    if v >= 1000:
        gb = v / 1000.0
        return f"{int(gb)}GB" if abs(gb - int(gb)) < 1e-9 else f"{gb:.2f}GB"
    return f"{int(v)}MB"

def _value_text_for_display(value: Any, unit: str) -> str:
    if isinstance(value, dict):
        vol = _extract_volume(value, unit)
        return _format_volume_unit(vol, unit) if vol is not None else "-"
    if isinstance(value, str):
        cleaned = _PKG_TAIL.sub("", value).strip()
        vol = _extract_volume(cleaned, unit)
        return _format_volume_unit(vol, unit) if vol is not None else (cleaned or "-")
    return value or "-"

def _norm(s: str) -> str:
    return (s or "").strip().lower()

PREFERRED_ORDER: List[str] = ["MTN", "AT - iShare", "AT - BigTime", "AFA TALKTIME"]

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

def _service_state(svc: Dict[str, Any]) -> Dict[str, Any]:
    t = (svc.get("type") or "API").upper()
    status = (svc.get("status") or "OPEN").upper()
    availability = (svc.get("availability") or "AVAILABLE").upper()
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
        "type": t, "status": status, "availability": availability,
        "closed_message": closed_msg, "out_of_stock_message": oos_msg,
        "can_order": can_order, "disabled_reason": disabled_reason
    }

def _is_express(svc: Dict[str, Any]) -> bool:
    cat = (svc.get("service_category") or "").strip().lower()
    cat2 = (svc.get("category") or "").strip().lower()
    return cat == "express services" or cat2 == "express"

# ------------------ data prep for landing page ------------------

def load_services_for_house_user() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    raw = list(services_col.find({}))
    raw.sort(key=_service_priority_tuple)

    services: List[Dict[str, Any]] = []
    for s in raw:
        s = dict(s)
        s["_id_str"] = str(s["_id"])
        st = _service_state(s)
        s.update(st)

        eff_profit = _effective_profit_percent(s, HOUSE_USER_ID)
        unit = _service_unit(s)
        offers = s.get("offers") or []

        normalized_offers: List[Dict[str, Any]] = []
        for of in offers:
            parsed_value = _parse_value_field(of.get("value"))
            vol_num = _extract_volume(parsed_value, unit)
            value_text = _value_text_for_display(parsed_value, unit)

            amount = _to_float(of.get("amount"))
            total = (
                round((amount or 0.0) + ((amount or 0.0) * (eff_profit or 0.0) / 100.0), 2)
                if amount is not None else None
            )

            normalized_offers.append({
                "amount": amount,
                "value": parsed_value,
                "value_text": value_text,
                "profit_percent_used": eff_profit,
                "total": total,
                "_sort_vol": vol_num if vol_num is not None else float("inf"),
                "_sort_amt": amount if amount is not None else float("inf"),
            })

        normalized_offers.sort(key=lambda x: (x["_sort_vol"], x["_sort_amt"]))
        s["offers"] = [{k: v for k, v in o.items() if not k.startswith("_sort_")} for o in normalized_offers]
        s["effective_profit_percent"] = eff_profit
        s["unit"] = unit

        services.append(s)

    express_services = [s for s in services if _is_express(s)]
    regular_services = [s for s in services if not _is_express(s)]
    return regular_services, express_services

# ------------------ Paystack helpers ------------------

def _nearest_whole(x: float) -> int:
    # "nearest whole" with .5 up — avoid Python's bankers rounding
    xf = float(x or 0.0)
    return int(xf + 0.5)

def _paystack_item_price_ghs(item: Dict[str, Any]) -> float:
    """Rule: round(amount) + 1 per line. 'amount' is the system (DB) price."""
    amt = _money(item.get("amount"))
    return float(_nearest_whole(amt) + 1)

def _paystack_cart_total_ghs(cart: List[Dict[str, Any]]) -> float:
    return round(sum(_paystack_item_price_ghs(i) for i in cart), 2)

def _verify_paystack(reference: str) -> Tuple[bool, Dict[str, Any], str]:
    """Verify Paystack transaction using secret key. Returns (ok, data, fail_reason)."""
    if not PAYSTACK_SECRET_KEY:
        return (False, {}, "Payment processor not configured.")
    try:
        headers = {"Authorization": f"Bearer {PAYSTACK_SECRET_KEY}"}
        url = f"https://api.paystack.co/transaction/verify/{reference}"
        r = requests.get(url, headers=headers, timeout=25)
        result = r.json()
        if not result.get("status"):
            return (False, result, result.get("message") or "Verification failed.")

        data = result.get("data") or {}
        ok = (data.get("status") == "success")
        if not ok:
            return (False, data, data.get("gateway_response") or "Payment not successful.")
        return (True, data, "")
    except Exception as e:
        return (False, {}, f"Verify error: {str(e)}")

def _paid_enough(paid_pesewas: int, expected_pesewas: int) -> bool:
    """Mirror frontend: accept paid >= expected (fees may be added on top by gateway)."""
    return int(paid_pesewas or 0) >= int(expected_pesewas or 0)

# ------------------ routes ------------------

@index_bp.route("/", methods=["GET"])
def landing():
    try:
        regular, express = load_services_for_house_user()
    except Exception:
        regular, express = [], []
    return render_template(
        "index.html",
        services=regular,
        express_services=express,
        paystack_pk=PAYSTACK_PUBLIC_KEY,
    )

@index_bp.route("/public-checkout", methods=["POST"])
def public_checkout():
    """
    Public checkout (inline Paystack required):
    - Verifies Paystack reference server-side (amount/currency/status)
    - Recomputes expected Paystack total from cart using: nearest whole + 1 per item
    - Accepts payments >= expected (gateway may add fees). Rejects if paid < expected.
    - If verified, proceeds to place order and charge the HOUSE wallet (fulfillment spend)
    """
    try:
        data = request.get_json(silent=True) or {}
        cart = data.get("cart", [])
        method = (data.get("method") or "paystack_inline").strip().lower()
        ps_info = data.get("paystack") or {}
        ps_ref = (ps_info.get("reference") or "").strip()

        jlog("public_checkout_incoming", payload=data)

        # Basic cart validation
        if not cart or not isinstance(cart, list):
            return jsonify({"success": False, "message": "Cart is empty or invalid"}), 400

        # System total (DB price) — what the house wallet will actually spend
        total_requested = sum(_money(item.get("amount")) for item in cart)
        if total_requested <= 0:
            return jsonify({"success": False, "message": "Total amount must be greater than zero"}), 400

        # --- Paystack verification is mandatory ---
        if method != "paystack_inline" or not ps_ref:
            return jsonify({"success": False, "message": "Payment missing. Please pay first."}), 400

        # Idempotency: if we already created an order for this reference, return it
        prior_order = orders_col.find_one({"paystack_reference": ps_ref})
        if prior_order:
            return jsonify({
                "success": True,
                "message": f"✅ Order already created. Order ID: {prior_order.get('order_id')}",
                "order_id": prior_order.get("order_id"),
                "status": prior_order.get("status"),
                "charged_amount": prior_order.get("charged_amount"),
                "profit_amount_total": prior_order.get("profit_amount_total", 0.0),
                "items": prior_order.get("items", []),
                "idempotent": True
            }), 200

        ok, verify_data, fail_reason = _verify_paystack(ps_ref)
        if not ok:
            return jsonify({"success": False, "message": f"Payment verification failed: {fail_reason}"}), 400

        # Validate amount and currency
        paid_pes = int(verify_data.get("amount") or 0)
        paid_ghs = round(paid_pes / 100.0, 2)
        currency = (verify_data.get("currency") or "GHS").upper()
        if paid_pes <= 0 or currency != "GHS":
            return jsonify({"success": False, "message": "Invalid payment amount/currency."}), 400

        # Recompute expected Paystack total from cart (server-authoritative)
        expected_pay_ghs = _paystack_cart_total_ghs(cart)
        expected_pay_pes = int(round(expected_pay_ghs * 100))

        # ACCEPT paid >= expected; REJECT if paid < expected
        if not _paid_enough(paid_pes, expected_pay_pes):
            jlog("public_checkout_amount_underpaid",
                 paid_pes=paid_pes, expected_pay_pes=expected_pay_pes,
                 paid_ghs=paid_ghs, expected_pay_ghs=expected_pay_ghs, cart=cart)
            return jsonify({
                "success": False,
                "message": "Payment amount is less than required. Please complete full payment.",
                "paid": paid_ghs,
                "required": expected_pay_ghs
            }), 400

        # Record the Paystack payment transaction (gross customer payment)
        # Idempotency for the transaction itself
        if not transactions_col.find_one({"reference": ps_ref, "type": "payment", "status": "success"}):
            transactions_col.insert_one({
                "user_id": HOUSE_USER_ID,  # attribute income to house
                "amount": round(paid_ghs, 2),
                "reference": ps_ref,
                "status": "success",
                "type": "payment",
                "gateway": "Paystack",
                "currency": "GHS",
                "channel": verify_data.get("channel"),
                "verified_at": datetime.utcnow(),
                "created_at": datetime.utcnow(),
                "raw": verify_data,
                "meta": {
                    "public_checkout": True,
                    "expected_pay_total_ghs": expected_pay_ghs,
                    "note": "Customer payment captured via public inline checkout"
                }
            })

        # Everything checks out → create order and spend from HOUSE wallet

        order_id = generate_order_id()
        results: List[Dict[str, Any]] = []
        debug_events: List[Dict[str, Any]] = []
        total_processing_amount = 0.0
        profit_amount_total = 0.0

        for idx, item in enumerate(cart, start=1):
            phone = (item.get("phone") or "").strip()
            value_obj = _coerce_value_obj(item.get("value_obj") or item.get("value"))
            amt_total = _money(item.get("amount"))
            service_id_raw = item.get("serviceId")
            svc_doc: Optional[Dict[str, Any]] = None
            svc_type: Optional[str] = None
            svc_name = item.get("serviceName") or None
            service_category: Optional[str] = None

            if service_id_raw:
                try:
                    from bson import ObjectId as _OID
                    svc_doc = services_col.find_one(
                        {"_id": _OID(service_id_raw)},
                        {
                            "type": 1, "network_id": 1, "name": 1, "network": 1,
                            "offers": 1, "default_profit_percent": 1, "service_category": 1,
                            "status": 1, "availability": 1
                        }
                    )
                    if svc_doc:
                        st = svc_doc.get("type")
                        svc_type = (st.strip().upper() if isinstance(st, str) else st)
                        svc_name = svc_doc.get("name") or svc_doc.get("network") or svc_name
                        service_category = (svc_doc.get("service_category") or "").strip().lower()
                except Exception:
                    svc_doc = None
                    svc_type = None

            # Hard gate: availability
            is_unavail, reason_text = _service_unavailability_reason(svc_doc)
            if is_unavail:
                return jsonify({
                    "success": False,
                    "message": reason_text,
                    "unavailable": {"serviceId": service_id_raw, "serviceName": svc_name, "reason": reason_text}
                }), 400

            # base + profit allocation (absolute)
            base_hint = _to_float(item.get("base_amount"))
            if base_hint is None:
                base_hint = _pick_offer_base_amount_from_service(svc_doc or {}, value_obj, item.get("value"))
            eff_p = _effective_profit_percent(svc_doc, HOUSE_USER_ID) if svc_doc else 0.0
            base_amount, profit_amount = _derive_base_profit(amt_total, base_hint, eff_p)
            profit_amount_total += profit_amount

            # NON-API → processing
            if not svc_doc or (svc_type and str(svc_type).upper() != "API"):
                total_processing_amount += amt_total
                results.append({
                    "phone": phone, "base_amount": base_amount, "amount": amt_total,
                    "profit_amount": profit_amount, "profit_percent_used": eff_p,
                    "value": item.get("value"), "value_obj": value_obj,
                    "serviceId": service_id_raw, "serviceName": svc_name,
                    "service_type": svc_type if svc_type else ("unknown" if not svc_doc else None),
                    "line_status": "processing", "api_status": "not_applicable",
                    "api_response": {"note": "Service not API; queued for processing"}
                })
                continue

            # API path
            is_express = (service_category == "express services")
            network_id = _resolve_network_id(item, value_obj, svc_doc)
            if is_express:
                shared_bundle = _resolve_shared_bundle_express(item, value_obj)   # offer ID
            else:
                shared_bundle = _resolve_shared_bundle_toppily(item, value_obj)   # MB

            if not phone or network_id is None or shared_bundle is None:
                total_processing_amount += amt_total
                results.append({
                    "phone": phone, "base_amount": base_amount, "amount": amt_total,
                    "profit_amount": profit_amount, "profit_percent_used": eff_p,
                    "value": item.get("value"), "value_obj": value_obj,
                    "serviceId": service_id_raw, "serviceName": svc_name, "service_type": svc_type,
                    "line_status": "processing", "api_status": "skipped_missing_fields",
                    "api_response": {
                        "note": "API fields missing; queued for processing",
                        "got": {"phone": bool(phone), "network_id": network_id, "shared_bundle": shared_bundle}
                    }
                })
                continue

            trx_ref = f"{order_id}_{idx}"
            if is_express:
                ok2, payload = _send_express_single(phone, network_id, shared_bundle, trx_ref, order_id, debug_events)
                api_tag = "express"
            else:
                ok2, payload = _send_toppily_shared_bundle(phone, network_id, shared_bundle, trx_ref, order_id, debug_events)
                api_tag = "toppily"

            total_processing_amount += amt_total
            results.append({
                "phone": phone, "base_amount": base_amount, "amount": amt_total,
                "profit_amount": profit_amount, "profit_percent_used": eff_p,
                "value": item.get("value"), "value_obj": value_obj,
                "serviceId": service_id_raw, "serviceName": svc_name, "service_type": svc_type,
                "provider": api_tag, "trx_ref": trx_ref,
                "line_status": "processing", "api_status": ("success" if ok2 else "processing"),
                "api_response": payload
            })

        if len(debug_events) > 10:
            debug_events = debug_events[-10:]

        # Spend from house wallet only what we're processing now
        total_to_charge_now = round(total_processing_amount, 2)

        # Check balance & deduct
        bal_doc = balances_col.find_one({"user_id": HOUSE_USER_ID}) or {}
        current_balance = _money(bal_doc.get("amount", 0))
        jlog("public_checkout_balance", balance=current_balance, spend=total_to_charge_now)
        if current_balance < total_to_charge_now:
            return jsonify({"success": False, "message": "House wallet has insufficient balance"}), 400

        balances_col.update_one(
            {"user_id": HOUSE_USER_ID},
            {"$inc": {"amount": -total_to_charge_now}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True
        )

        status = "processing"
        orders_col.insert_one({
            "user_id": HOUSE_USER_ID,
            "order_id": order_id,
            "items": results,
            "total_amount": total_requested,            # system price total
            "charged_amount": total_to_charge_now,      # actually spent now
            "profit_amount_total": round(profit_amount_total, 2),
            "status": status,
            "paid_from": "paystack_inline",
            "paystack_reference": ps_ref,               # link to payment
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "debug": {
                "public_checkout": True,
                "events": debug_events,
                "paystack_paid_ghs": round(paid_pes / 100.0, 2),
                "paystack_expected_ghs": expected_pay_ghs
            }
        })

        # Record the house wallet spend (purchase)
        transactions_col.insert_one({
            "user_id": HOUSE_USER_ID,
            "amount": total_to_charge_now,
            "reference": order_id,
            "status": "success",
            "type": "purchase",
            "gateway": "Wallet",
            "currency": "GHS",
            "created_at": datetime.utcnow(),
            "verified_at": datetime.utcnow(),
            "meta": {
                "public_checkout": True,
                "order_status": status,
                "api_delivered_amount": 0.0,
                "processing_amount": round(total_processing_amount, 2),
                "profit_amount_total": round(profit_amount_total, 2),
                "paystack_reference": ps_ref
            }
        })

        msg = f"✅ Order received and is processing. Order ID: {order_id}"
        return jsonify({
            "success": True,
            "message": msg,
            "order_id": order_id,
            "status": status,
            "charged_amount": total_to_charge_now,
            "profit_amount_total": round(profit_amount_total, 2),
            "items": results
        }), 200

    except Exception:
        jlog("public_checkout_uncaught", error=traceback.format_exc())
        return jsonify({"success": False, "message": "Server error"}), 500
