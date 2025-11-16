from flask import Blueprint, request, jsonify, session
from bson import ObjectId
from datetime import datetime, timedelta
import os, uuid, random, requests, traceback, json, ast, re

from db import db

checkout_bp = Blueprint("checkout", __name__)

# MongoDB Collections
balances_col = db["balances"]
orders_col = db["orders"]
transactions_col = db["transactions"]
services_col = db["services"]
service_profits_col = db["service_profits"]  # per-customer overrides

# ===== DataVerse Provider Config (HARDCODED as requested) ====================
DATAVERSE_BASE_URL = "https://dataversegh.pro/wp-json/custom/v1"
DATAVERSE_USERNAME = "Nyebro"
DATAVERSE_PASSWORD = "TazgH924s29FaF1UUOzxyzPT"

# Network ID fallback (kept for internal use / reporting, not for provider)
NETWORK_ID_FALLBACK = {
    "MTN": 3,
    "VODAFONE": 2,
    "AIRTELTIGO": 1,
}

# ===== Tiny JSON logger =======================================================
def jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")

# ===== Helpers ================================================================
def generate_order_id():
    return f"NAN{random.randint(10000, 99999)}"

def _money(v):
    try:
        return float(v)
    except Exception:
        return 0.0

def _to_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def _coerce_value_obj(v):
    """
    Accepts dict, JSON string, or python-dict-like string.
    Returns a dict (possibly empty).
    """
    if isinstance(v, dict):
        return v
    if not v:
        return {}
    s = str(v).strip()
    if s.startswith("{") and s.endswith("}"):
        try:
            d = json.loads(s)
            return d if isinstance(d, dict) else {}
        except Exception:
            try:
                d = ast.literal_eval(s)
                return d if isinstance(d, dict) else {}
            except Exception:
                return {}
    return {}

# ===== Profit helpers (absolute profit amount) ================================
def _get_service_default_profit_percent(service_doc):
    return _to_float(service_doc.get("default_profit_percent"), 0.0) or 0.0

def _get_customer_profit_override_percent(service_id, customer_id_obj):
    ov = service_profits_col.find_one({"service_id": service_id, "customer_id": customer_id_obj})
    return _to_float(ov.get("profit_percent"), None) if ov else None

def _effective_profit_percent(service_doc, customer_id_obj):
    override = _get_customer_profit_override_percent(service_doc["_id"], customer_id_obj)
    return override if override is not None else _get_service_default_profit_percent(service_doc)

def _pick_offer_base_amount_from_service(svc_doc, value_obj, raw_value):
    """
    Try to recover the base (wholesale) amount from the selected offer in svc_doc.offers.
    """
    try:
        offers = svc_doc.get("offers") or []
        vid = (value_obj or {}).get("id")
        vvol = (value_obj or {}).get("volume")
        for of in offers:
            of_val = of.get("value")
            of_amt = _to_float(of.get("amount"))
            if isinstance(of_val, str) and of_val.strip().startswith("{") and of_val.strip().endswith("}"):
                try:
                    of_val = json.loads(of_val)
                except Exception:
                    try:
                        of_val = ast.literal_eval(of_val)
                    except Exception:
                        pass
            if isinstance(of_val, dict):
                if (vid is not None and of_val.get("id") == vid) or (vvol is not None and of_val.get("volume") == vvol):
                    return of_amt
            else:
                if raw_value is not None and of_val == raw_value:
                    return of_amt
    except Exception:
        pass
    return None

def _derive_base_profit(amount_total, base_amount_hint, eff_percent):
    a = _money(amount_total)
    if a <= 0:
        return 0.0, 0.0
    if base_amount_hint is not None and base_amount_hint > 0:
        base = float(base_amount_hint)
        profit = round(a - base, 2)
        if profit < 0:
            profit = 0.0
            base = a
        return round(base, 2), profit
    p = _to_float(eff_percent, 0.0) or 0.0
    try:
        base = round(a / (1.0 + (p / 100.0)), 2) if p > 0 else a
    except Exception:
        base = a
    profit = round(a - base, 2)
    if profit < 0:
        profit = 0.0
        base = a
    return base, profit

# ===== Field resolvers =======================================================
def _resolve_network_id(item: dict, value_obj: dict, svc_doc: dict | None):
    """
    Internal numeric network ID, used only for duplicate guards / reporting.
    Not sent to DataVerse.
    """
    nid = (item or {}).get("network_id") or (value_obj or {}).get("network_id")
    if nid not in (None, "", []):
        try:
            return int(nid)
        except Exception:
            pass
    if svc_doc:
        try:
            if "network_id" in svc_doc and svc_doc["network_id"] not in (None, ""):
                return int(svc_doc["network_id"])
            guess = (svc_doc.get("name") or svc_doc.get("network") or "").strip().upper()
            if guess and guess in NETWORK_ID_FALLBACK:
                return int(NETWORK_ID_FALLBACK[guess])
        except Exception:
            pass
    if not svc_doc:
        name = (item.get("serviceName") or "").strip().upper()
        if name in NETWORK_ID_FALLBACK:
            return int(NETWORK_ID_FALLBACK[name])
    return None

def _resolve_dataverse_network(svc_doc: dict | None, item: dict) -> str | None:
    """
    Resolve DataVerse 'network' string.
    For now we only support MTN via the API.
    Uses:
      - svc_doc.service_network
      - svc_doc.network / name
      - stored service by name (fallback)
      - item fields
    Returns 'mtn' or None.
    """
    doc = svc_doc

    # Fallback: look up by service name if svc_doc is missing
    if not doc:
        sname = (item.get("serviceName") or "").strip()
        if sname:
            try:
                doc = services_col.find_one(
                    {"name": sname},
                    {"service_network": 1, "network": 1, "name": 1},
                )
            except Exception:
                doc = None

    candidates = []
    if doc:
        candidates.append(doc.get("service_network"))
        candidates.append(doc.get("network"))
        candidates.append(doc.get("name"))

    candidates.append(item.get("network"))
    candidates.append(item.get("network_name"))
    candidates.append(item.get("serviceName"))

    joined = " ".join(str(c) for c in candidates if c).lower()
    if "mtn" in joined:
        return "mtn"
    return None

def _resolve_package_size_gb(value_obj: dict, item: dict) -> int | None:
    """
    Resolve DataVerse 'package_size' (integer GB).
    Tries (in order):
      - explicit gb/size fields in value_obj
      - volume field (interpreting >50 as MB, otherwise GB)
      - parsing numbers from item['value'] like '1GB', '5 GB'
    """
    if not isinstance(value_obj, dict):
        value_obj = value_obj or {}

    # 1) explicit GB fields
    for key in ("gb", "gb_size", "package_size", "volume_gb", "size_gb"):
        val = value_obj.get(key)
        if val not in (None, "", []):
            try:
                return int(float(val))
            except Exception:
                pass

    # 2) 'volume' field (can be GB or MB)
    vol = value_obj.get("volume")
    if vol not in (None, "", []):
        try:
            vol_f = float(vol)
            # Heuristic: if looks large, assume MB and convert
            if vol_f > 50:
                gb = max(1, round(vol_f / 1024.0))
            else:
                gb = vol_f
            return int(gb)
        except Exception:
            pass

    # 3) Parse from item['value'] string like '1GB', '5 GB'
    raw_val = item.get("value") or ""
    if isinstance(raw_val, str):
        m = re.search(r"(\d+(?:\.\d+)?)\s*gb", raw_val.lower())
        if m:
            try:
                return int(float(m.group(1)))
            except Exception:
                pass
        # fallback: first number
        m2 = re.search(r"(\d+(?:\.\d+)?)", raw_val)
        if m2:
            try:
                return int(float(m2.group(1)))
            except Exception:
                pass

    return None

def _build_bundle_key(value_obj: dict, item: dict):
    """
    Build a generic bundle key for duplicate detection.
    We don't care about provider-specific semantics anymore.
    Returns ('bundle', <normalized_value>) or None.
    """
    val = None
    if isinstance(value_obj, dict):
        for key in ("id", "volume", "code", "package_size", "gb"):
            if value_obj.get(key) not in (None, "", []):
                val = value_obj.get(key)
                break
    if val is None:
        val = item.get("value") or item.get("label")

    if val is None:
        return None

    try:
        norm = int(float(val))
    except Exception:
        norm = str(val).strip()

    return ("bundle", norm)

# ===== DataVerse provider caller =============================================
def _send_dataverse_order(phone: str, network: str, package_size_gb: int,
                          external_ref: str, order_id: str, debug_events: list):
    """
    Call DataVerse place-order endpoint for a single MTN bundle.
    Body:
      {
        "network": "mtn",
        "recipient": "<phone>",
        "package_size": <int GB>,
        "order_id": "<unique-ref>"
      }
    """
    if not DATAVERSE_USERNAME or not DATAVERSE_PASSWORD:
        err = {"status": "error", "message": "DATAVERSE credentials not configured", "http_status": 500}
        jlog("dataverse_config_error", order_id=order_id, ref=external_ref)
        return False, err

    url = f"{DATAVERSE_BASE_URL.rstrip('/')}/place-order"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = {
        "network": network,
        "recipient": phone,
        "package_size": int(package_size_gb),
        "order_id": external_ref,
    }

    masked = phone[:3] + "***" + phone[-2:] if phone and len(phone) >= 5 else "***"
    jlog(
        "dataverse_request_body",
        order_id=order_id,
        ref=external_ref,
        body={
            "network": network,
            "recipient": masked,
            "package_size": body["package_size"],
            "order_id": external_ref,
        },
    )

    try:
        resp = requests.post(
            url,
            headers=headers,
            json=body,
            auth=(DATAVERSE_USERNAME, DATAVERSE_PASSWORD),
            timeout=45,
        )
        text = resp.text or ""
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": text} if text else {}

        ok = (
            resp.status_code == 200
            and isinstance(payload, dict)
            and str(payload.get("status", "")).lower() in ("success", "true")
        )
        if isinstance(payload, dict):
            payload.setdefault("http_status", resp.status_code)

        dbg = {
            "status": resp.status_code,
            "body_len": len(text),
        }
        # 🔍 log full response payload for debugging
        jlog("dataverse_response", order_id=order_id, ref=external_ref, payload=payload)
        jlog("dataverse_call", order_id=order_id, ref=external_ref, ok=ok, debug=dbg)
        debug_events.append(
            {
                "when": datetime.utcnow(),
                "stage": "dataverse-place-order",
                "ok": ok,
                "http_status": resp.status_code,
            }
        )
        return ok, payload

    except requests.RequestException as e:
        jlog("dataverse_network_error", order_id=order_id, ref=external_ref, error=str(e))
        return False, {"status": "error", "message": str(e), "http_status": 599}

# ===== Unavailability checker ================================================
def _service_unavailability_reason(svc_doc: dict):
    """
    Returns (is_unavailable, reason_text)
    reason_text is exactly 'Out of stock' or 'Closed'.
    Missing service is treated as 'Closed'.
    """
    if not svc_doc:
        return True, "Closed"

    status = (svc_doc.get("status") or "").strip().upper()
    availability = (svc_doc.get("availability") or "").strip().upper()

    if availability in {"OUT_OF_STOCK", "OUT OF STOCK", "OUTOFSTOCK"}:
        return True, "Out of stock"

    if status == "CLOSED":
        return True, "Closed"

    return False, ""

# ===== Duplicate-in-processing guard =========================================
DUP_WINDOW_MINUTES = 30

def _normalize_amount_key(v):
    """Use a stable numeric value for matching the requested line amount."""
    try:
        return float(f"{float(v):.2f}")
    except Exception:
        return 0.0

def _has_processing_conflict_strict(
    phone: str,
    service_id_raw: str | None,
    svc_name: str | None,
    network_id: int | None,
    bundle_key: tuple | None,
    amount_key: float,
) -> bool:
    """
    True if there exists an order in 'processing' (within window) for:
      - same phone
      - same network_id
      - same bundle_key
      - same amount (line amount the customer pays)
    Optionally narrowed by same serviceId when available.

    ✅ This ensures:
      - If 0530393625 + 1GB + same amount is already processing,
        a new identical request is SKIPPED and never hits the API.
    """
    if not phone or network_id is None or bundle_key is None:
        return False  # cannot assert strict duplicate without the triad

    window_start = datetime.utcnow() - timedelta(minutes=DUP_WINDOW_MINUTES)
    kind, bval = bundle_key

    # Preferred match: documents that already store network_id/bundle_key/amount in items
    elem = {
        "phone": phone,
        "network_id": network_id,
        "bundle_key.kind": kind,
        "bundle_key.value": bval,
        "amount": amount_key,
    }
    if service_id_raw:
        elem["serviceId"] = service_id_raw

    q = {
        "status": "processing",
        "created_at": {"$gte": window_start},
        "items": {"$elemMatch": elem},
    }
    if orders_col.find_one(q, {"_id": 1}):
        return True

    # Fallback compatibility for older orders (match via value_obj and amount)
    alt = {
        "phone": phone,
        "network_id": network_id,
        "amount": amount_key,
    }
    if kind == "offer":
        alt["value_obj.id"] = bval
    else:
        alt["value_obj.volume"] = bval
    if service_id_raw:
        alt["serviceId"] = service_id_raw

    q2 = {
        "status": "processing",
        "created_at": {"$gte": window_start},
        "items": {"$elemMatch": alt},
    }
    return bool(orders_col.find_one(q2, {"_id": 1}))

# ===== Route (NO background auto-update) =====================================
@checkout_bp.route("/checkout", methods=["POST"])
def process_checkout():
    try:
        # Auth
        if "user_id" not in session or session.get("role") != "customer":
            jlog("checkout_auth_fail", session_keys=list(session.keys()))
            return jsonify({"success": False, "message": "Not authorized"}), 401

        try:
            user_id = ObjectId(session["user_id"])
        except Exception:
            return jsonify({"success": False, "message": "Invalid user ID"}), 400

        data = request.get_json(silent=True) or {}
        cart = data.get("cart", [])
        method = data.get("method", "wallet")
        jlog("checkout_incoming", payload=data)

        if not cart or not isinstance(cart, list):
            return jsonify({"success": False, "message": "Cart is empty or invalid"}), 400

        # Total requested (customer-facing price — includes profit)
        total_requested = sum(_money(item.get("amount")) for item in cart)
        if total_requested <= 0:
            return jsonify({"success": False, "message": "Total amount must be greater than zero"}), 400

        order_id = generate_order_id()

        bal_doc = balances_col.find_one({"user_id": user_id}) or {}
        current_balance = _money(bal_doc.get("amount", 0))
        jlog("checkout_balance", order_id=order_id, balance=current_balance, total=total_requested)
        if current_balance < total_requested:
            return jsonify({"success": False, "message": "❌ Insufficient wallet balance"}), 400

        results = []
        debug_events = []

        # We will charge for: successful provider lines + any line we keep "processing"
        total_delivered_api_amount = 0.0   # stays 0 because we do not mark anything delivered immediately
        total_processing_amount = 0.0
        api_requested_total = 0.0
        has_processing = False

        # Rollup profit
        profit_amount_total = 0.0

        # Prevent same-cart duplicates for (phone, network_id, bundle_key, amount)
        seen_keys = set()  # (phone, network_id, bundle_value, kind, amount_key)

        for idx, item in enumerate(cart, start=1):
            phone = (item.get("phone") or "").strip()
            value_obj = _coerce_value_obj(item.get("value_obj") or item.get("value"))
            amt_total = _money(item.get("amount"))  # customer-facing total (base + profit)
            amount_key = _normalize_amount_key(amt_total)

            service_id_raw = item.get("serviceId")
            svc_doc = None
            svc_type = None
            svc_name = item.get("serviceName") or None

            # Resolve service and its type/category (+ offers for base recovery)
            if service_id_raw:
                try:
                    svc_doc = services_col.find_one(
                        {"_id": ObjectId(service_id_raw)},
                        {
                            "type": 1,
                            "network_id": 1,
                            "name": 1,
                            "network": 1,
                            "offers": 1,
                            "default_profit_percent": 1,
                            "service_category": 1,
                            "status": 1,
                            "availability": 1,
                            "service_network": 1,
                        },
                    )
                    if svc_doc:
                        st = svc_doc.get("type")
                        svc_type = (st.strip().upper() if isinstance(st, str) else st)
                        # prefer svc_doc name
                        svc_name = svc_doc.get("name") or svc_doc.get("network") or svc_name
                except Exception:
                    svc_doc = None
                    svc_type = None

            # ===== HARD GATE: availability / status =====
            is_unavail, reason_text = _service_unavailability_reason(svc_doc)
            if is_unavail:
                # Stop the entire checkout and return explicit reason
                return jsonify(
                    {
                        "success": False,
                        "message": reason_text,  # "Out of stock" or "Closed"
                        "unavailable": {
                            "serviceId": service_id_raw,
                            "serviceName": svc_name,
                            "reason": reason_text,
                        },
                    }
                ), 400

            # ---------- Resolve fields used for duplicate guard ----------
            network_id = _resolve_network_id(item, value_obj, svc_doc)
            bundle_key = _build_bundle_key(value_obj, item)   # generic duplication key

            # ----- IN-CART duplicate guard (same number + same network + same bundle + same amount) -----
            if phone and (network_id is not None) and (bundle_key is not None):
                cart_key = (phone, int(network_id), bundle_key[1], bundle_key[0], amount_key)
                if cart_key in seen_keys:
                    results.append(
                        {
                            "phone": phone,
                            "base_amount": 0.0,
                            "amount": 0.0,  # not charged
                            "originally_requested_amount": amt_total,
                            "profit_amount": 0.0,
                            "profit_percent_used": 0.0,
                            "value": item.get("value"),
                            "value_obj": value_obj,
                            "serviceId": service_id_raw,
                            "serviceName": svc_name,
                            "service_type": svc_type if svc_type else ("unknown" if not svc_doc else None),
                            "network_id": network_id,
                            "bundle_key": {"kind": bundle_key[0], "value": bundle_key[1]},
                            "line_amount_key": amount_key,
                            "line_status": "skipped_duplicate_in_cart",
                            "api_status": "skipped",
                            "api_response": {
                                "note": "Duplicate line in this cart (same number, network, bundle, amount)"
                            },
                        }
                    )
                    continue
                seen_keys.add(cart_key)

            # ----- DUPLICATE-IN-PROCESSING GUARD (strict + amount) -------------------------
            is_dup_strict = _has_processing_conflict_strict(
                phone, service_id_raw, svc_name, network_id, bundle_key, amount_key
            )
            if is_dup_strict:
                # ✅ This line is NOT charged and NOT sent to API
                results.append(
                    {
                        "phone": phone,
                        "base_amount": 0.0,
                        "amount": 0.0,  # not charged
                        "originally_requested_amount": amt_total,  # for audit
                        "profit_amount": 0.0,
                        "profit_percent_used": 0.0,
                        "value": item.get("value"),
                        "value_obj": value_obj,
                        "serviceId": service_id_raw,
                        "serviceName": svc_name,
                        "service_type": svc_type if svc_type else ("unknown" if not svc_doc else None),
                        "network_id": network_id,
                        "bundle_key": ({"kind": bundle_key[0], "value": bundle_key[1]} if bundle_key else None),
                        "line_amount_key": amount_key,
                        "line_status": "skipped_duplicate_processing",
                        "api_status": "skipped",
                        "api_response": {
                            "note": "Same number + same network + same bundle + same amount already processing; skipping."
                        },
                    }
                )
                continue

            # --- Compute base_amount & profit_amount (ABSOLUTE) ---
            base_hint = _to_float(item.get("base_amount"))
            if base_hint is None:
                base_hint = _pick_offer_base_amount_from_service(svc_doc or {}, value_obj, item.get("value"))
            eff_p = 0.0
            if svc_doc:
                eff_p = _effective_profit_percent(svc_doc, user_id)
            base_amount, profit_amount = _derive_base_profit(amt_total, base_hint, eff_p)
            profit_amount_total += profit_amount

            # ---------------- DETERMINE DATAVERSE vs MANUAL ----------------
            # No service doc at all → manual processing (still charge)
            if not svc_doc:
                has_processing = True
                total_processing_amount += amt_total
                results.append(
                    {
                        "phone": phone,
                        "base_amount": base_amount,
                        "amount": amt_total,
                        "profit_amount": profit_amount,
                        "profit_percent_used": eff_p,
                        "value": item.get("value"),
                        "value_obj": value_obj,
                        "serviceId": service_id_raw,
                        "serviceName": svc_name,
                        "service_type": svc_type if svc_type else "unknown",
                        "network_id": network_id,
                        "bundle_key": ({"kind": bundle_key[0], "value": bundle_key[1]} if bundle_key else None),
                        "line_amount_key": amount_key,
                        "line_status": "processing",
                        "api_status": "not_applicable",
                        "api_response": {"note": "Service not found; queued for processing"},
                    }
                )
                continue

            # ✅ Only MTN EXPRESS + MTN network + TYPE=ON should go through DataVerse
            dataverse_network = _resolve_dataverse_network(svc_doc, item)
            svc_name_norm = (svc_name or "").strip().lower()
            is_mtn_express = (svc_name_norm == "mtn express")

            # NEW: respect service.type as ON/OFF toggle for API
            svc_type_flag = (svc_type or "").strip().upper() if isinstance(svc_type, str) else ""
            type_allows_api = (svc_type_flag == "ON")

            use_dataverse = (dataverse_network == "mtn" and is_mtn_express and type_allows_api)

            if not use_dataverse:
                # Any case where:
                #  - not MTN, or
                #  - not MTN EXPRESS by name, or
                #  - TYPE is OFF (API disabled)
                # → manual processing
                has_processing = True
                total_processing_amount += amt_total

                # Note reason for debugging
                if svc_type_flag == "OFF":
                    note = (
                        "Dataverse API disabled for this service because type is OFF; "
                        "queued for manual processing."
                    )
                    api_status = "not_applicable_type_off"
                else:
                    note = (
                        "Dataverse API is used only for 'MTN EXPRESS' MTN bundles with type=ON; "
                        "queued for manual processing."
                    )
                    api_status = "not_applicable_network"

                results.append(
                    {
                        "phone": phone,
                        "base_amount": base_amount,
                        "amount": amt_total,
                        "profit_amount": profit_amount,
                        "profit_percent_used": eff_p,
                        "value": item.get("value"),
                        "value_obj": value_obj,
                        "serviceId": service_id_raw,
                        "serviceName": svc_name,
                        "service_type": svc_type,
                        "network_id": network_id,
                        "bundle_key": ({"kind": bundle_key[0], "value": bundle_key[1]} if bundle_key else None),
                        "line_amount_key": amount_key,
                        "line_status": "processing",
                        "api_status": api_status,
                        "api_response": {
                            "note": note,
                            "dataverse_network": dataverse_network,
                            "serviceName": svc_name,
                            "service_type_flag": svc_type_flag,
                        },
                    }
                )
                continue

            # From here: ONLY MTN EXPRESS (MTN) lines with TYPE=ON reach this point → API-eligible
            api_requested_total += amt_total

            # MTN DataVerse path: need phone + package_size_gb
            package_size_gb = _resolve_package_size_gb(value_obj, item)
            if not phone or package_size_gb is None:
                has_processing = True
                total_processing_amount += amt_total
                results.append(
                    {
                        "phone": phone,
                        "base_amount": base_amount,
                        "amount": amt_total,
                        "profit_amount": profit_amount,
                        "profit_percent_used": eff_p,
                        "value": item.get("value"),
                        "value_obj": value_obj,
                        "serviceId": service_id_raw,
                        "serviceName": svc_name,
                        "service_type": svc_type,
                        "network_id": network_id,
                        "bundle_key": ({"kind": bundle_key[0], "value": bundle_key[1]} if bundle_key else None),
                        "line_amount_key": amount_key,
                        "line_status": "processing",
                        "api_status": "skipped_missing_fields",
                        "api_response": {
                            "note": "API fields missing; queued for processing",
                            "got": {
                                "phone": bool(phone),
                                "dataverse_network": dataverse_network,
                                "package_size_gb": package_size_gb,
                            },
                        },
                    }
                )
                continue

            # We have a valid MTN EXPRESS → DataVerse line → call provider
            external_ref = f"{order_id}_{idx}_{uuid.uuid4().hex[:6]}"
            ok, payload = _send_dataverse_order(phone, "mtn", package_size_gb, external_ref, order_id, debug_events)

            provider_ref = None
            provider_order_id = None
            if isinstance(payload, dict):
                # Some responses use "reference", others "order_reference"
                provider_ref = payload.get("reference") or payload.get("order_reference")
                provider_order_id = payload.get("order_id")

            # Regardless of provider success, we keep status as processing and charge
            has_processing = True
            total_processing_amount += amt_total
            results.append(
                {
                    "phone": phone,
                    "base_amount": base_amount,
                    "amount": amt_total,
                    "profit_amount": profit_amount,
                    "profit_percent_used": eff_p,
                    "value": item.get("value"),
                    "value_obj": value_obj,
                    "serviceId": service_id_raw,
                    "serviceName": svc_name,
                    "service_type": svc_type,
                    "provider": "dataverse",
                    "provider_network": "mtn",
                    "provider_reference": provider_ref,            # reference from provider response
                    "provider_order_id": provider_order_id,        # numeric provider order_id, if any
                    "provider_request_order_id": external_ref,     # what we sent as order_id
                    "network_id": network_id,
                    "bundle_key": ({"kind": bundle_key[0], "value": bundle_key[1]} if bundle_key else None),
                    "line_amount_key": amount_key,
                    "line_status": "processing",     # important: not 'delivered'
                    "api_status": "success" if ok else "processing",
                    "api_response": payload,
                }
            )

        # keep last debug entries
        if len(debug_events) > 10:
            debug_events = debug_events[-10:]

        # Charge now for: ALL processing lines (which includes all successful provider hits)
        total_to_charge_now = round(total_delivered_api_amount + total_processing_amount, 2)

        # ==== Nothing to charge now (e.g., all lines were skipped as duplicates) ====
        if total_to_charge_now <= 0:
            orders_col.insert_one(
                {
                    "user_id": user_id,
                    "order_id": order_id,
                    "items": results,  # contains skipped_duplicate lines (amount 0)
                    "total_amount": 0.0,
                    "charged_amount": 0.0,
                    "profit_amount_total": 0.0,
                    "status": "skipped",
                    "paid_from": method,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "debug": {"events": debug_events},
                }
            )

            skipped_count = sum(
                1
                for it in results
                if it.get("line_status") in ("skipped_duplicate_processing", "skipped_duplicate_in_cart")
            )

            return (
                jsonify(
                    {
                        "success": True,
                        "message": (
                            "No charge taken. {n} item(s) were skipped because the same phone, network, bundle, "
                            "and amount already has an order in processing or duplicated in cart."
                        ).format(n=skipped_count),
                        "order_id": order_id,
                        "status": "skipped",
                        "charged_amount": 0.0,
                        "profit_amount_total": 0.0,
                        "skipped_count": skipped_count,
                        "items": results,
                    }
                ),
                200,
            )

        # Deduct balance for the amount we are charging now
        balances_col.update_one(
            {"user_id": user_id},
            {"$inc": {"amount": -total_to_charge_now}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True,
        )

        # Overall order status: ALWAYS processing (even if all provider calls succeeded)
        status = "processing"

        # Persist order
        orders_col.insert_one(
            {
                "user_id": user_id,
                "order_id": order_id,
                "items": results,
                "total_amount": total_requested,  # cart grand total (UI computed)
                "charged_amount": total_to_charge_now,  # charged now
                "profit_amount_total": round(profit_amount_total, 2),
                "status": status,  # processing
                "paid_from": method,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "debug": {"events": debug_events},
            }
        )

        # Record transaction for the amount actually charged now
        transactions_col.insert_one(
            {
                "user_id": user_id,
                "amount": total_to_charge_now,
                "reference": order_id,
                "status": "success",
                "type": "purchase",
                "gateway": "Wallet",
                "currency": "GHS",
                "created_at": datetime.utcnow(),
                "verified_at": datetime.utcnow(),
                "meta": {
                    "order_status": status,
                    "api_delivered_amount": round(total_delivered_api_amount, 2),  # likely 0.0
                    "processing_amount": round(total_processing_amount, 2),
                    "profit_amount_total": round(profit_amount_total, 2),
                },
            }
        )

        skipped_count = sum(
            1
            for it in results
            if it.get("line_status") in ("skipped_duplicate_processing", "skipped_duplicate_in_cart")
        )
        processing_count = sum(1 for it in results if it.get("line_status") == "processing")

        # Response message (always processing to frontend)
        msg = (
            "📝 Order received and is processing. "
            "We’ve charged your wallet. Order ID: {oid}"
        ).format(oid=order_id)

        return (
            jsonify(
                {
                    "success": True,
                    "message": msg,
                    "order_id": order_id,
                    "status": status,
                    "charged_amount": total_to_charge_now,
                    "profit_amount_total": round(profit_amount_total, 2),
                    "processing_count": processing_count,
                    "skipped_count": skipped_count,
                    "items": results,
                }
            ),
            200,
        )

    except Exception:
        jlog("checkout_uncaught", error=traceback.format_exc())
        # Only truly exceptional cases return a 500; normal API failures never surface as 'failed'
        return jsonify({"success": False, "message": "Server error"}), 500
