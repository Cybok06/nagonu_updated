# checkout.py — Toppily shared-bundle flow only (network_id + shared_bundle)
from flask import Blueprint, request, jsonify, session
from bson import ObjectId
from datetime import datetime, timedelta
import os, uuid, random, requests, certifi, traceback, json, hashlib, pathlib, shutil, ast

from apscheduler.schedulers.background import BackgroundScheduler  # NEW

from db import db

checkout_bp = Blueprint("checkout", __name__)

# MongoDB Collections
balances_col = db["balances"]
orders_col = db["orders"]
transactions_col = db["transactions"]
services_col = db["services"]
service_profits_col = db["service_profits"]  # NEW: per-customer overrides

# ===== Toppily config (HARD-CODED) =====
TOPPILY_URL = "https://toppily.com/api/v1/buy-other-package"
TOPPILY_API_KEY = "0e7434520859996d4b758c7c77e22013690fc9ae"  # keep secret

# TLS + CF toggles
USE_CUSTOM_CA_BUNDLE = True
USE_CLOUDSCRAPER_FALLBACK = True
PRIMARY_VERIFY_SSL = True

TOPPILY_INTERMEDIATE_PEM = r""  # optional PEM if provider’s chain is incomplete

# Fallback map (used only if DB lookup fails)
NETWORK_ID_FALLBACK = {
    "MTN": 3,
    "VODAFONE": 2,
    "AIRTELTIGO": 1,
}

# ===== Startup: custom CA bundle =====
def _setup_custom_ca_bundle():
    if not USE_CUSTOM_CA_BUNDLE:
        return certifi.where()
    try:
        ca_dir = pathlib.Path(os.getcwd()) / "vendor_certs"
        ca_dir.mkdir(parents=True, exist_ok=True)
        base_ca = certifi.where()
        custom_ca = ca_dir / "custom_ca_bundle.pem"
        with open(custom_ca, "wb") as out:
            with open(base_ca, "rb") as base:
                shutil.copyfileobj(base, out)
            pem = TOPPILY_INTERMEDIATE_PEM.strip().encode("utf-8")
            if pem:
                out.write(b"\n"); out.write(pem)
        os.environ["SSL_CERT_FILE"] = str(custom_ca)
        os.environ["REQUESTS_CA_BUNDLE"] = str(custom_ca)
        print("[SSL] Using custom CA bundle:", custom_ca)
        return str(custom_ca)
    except Exception as e:
        print("[SSL] Failed to build custom CA bundle:", e)
        return certifi.where()

_CA_BUNDLE = _setup_custom_ca_bundle()

# ===== Tiny JSON logger =====
def jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")

# ===== Helpers =====
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
    Returns a dict (possibly empty). Example inputs handled:
      - {"id": 5, "volume": 1000}
      - "{'id': 5, 'volume': 1000}"
      - {"volume": "1000"}
    """
    if isinstance(v, dict):
        return v
    if not v:
        return {}
    s = str(v).strip()
    if s.startswith("{") and s.endswith("}"):
        # Try JSON
        try:
            d = json.loads(s)
            return d if isinstance(d, dict) else {}
        except Exception:
            # Try pythonic dict
            try:
                d = ast.literal_eval(s)
                return d if isinstance(d, dict) else {}
            except Exception:
                return {}
    return {}

def _is_cloudflare_block(text: str, headers: dict, status: int) -> bool:
    return (status in (403, 503)) and (
        "Just a moment..." in text or "__cf_chl_" in text or "challenge-platform" in text
    )

def _resp_debug(resp: requests.Response, body_text: str):
    redacted_headers = {}
    for k, v in resp.headers.items():
        lk = k.lower()
        redacted_headers[k] = "***" if lk in ("authorization","cookie","set-cookie","x-api-key") else v
    return {
        "status": resp.status_code,
        "headers": redacted_headers,
        "body_len": len(body_text or ""),
        "body_sha256_16": hashlib.sha256((body_text or "").encode("utf-8","ignore")).hexdigest()[:16],
        "body_snippet": (body_text or "")[:140].replace("\n"," "),
    }

def _post_requests(body, verify):
    headers = {
        "x-api-key": TOPPILY_API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "NanDataApp/1.0 (+server)",
    }
    resp = requests.post(TOPPILY_URL, headers=headers, json=body, timeout=45, verify=verify)
    text = resp.text or ""
    try:
        data = resp.json()
    except Exception:
        data = {"raw": text}
    # keep original ok detection; downstream we normalize "failed" to processing anyway
    ok = resp.ok and bool(data.get("success", False))
    return ok, data, resp, text

def _post_cloudscraper(body):
    try:
        import cloudscraper
    except Exception as e:
        return False, {"success": False, "error": f"cloudscraper not installed: {e}"}, None, ""
    scraper = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","mobile":False})
    headers = {"x-api-key": TOPPILY_API_KEY, "Accept":"application/json","Content-Type":"application/json"}
    resp = scraper.post(TOPPILY_URL, headers=headers, json=body, timeout=60)
    text = resp.text or ""
    try:
        data = resp.json()
    except Exception:
        data = {"raw": text}
    ok = resp.ok and bool(data.get("success", False))
    return ok, data, resp, text

# ===== Profit helpers (absolute profit amount) =====
def _get_service_default_profit_percent(service_doc):
    """
    Service-level default percent — only used to INFER base_amount if the client
    didn't send it AND we couldn't match an offer. We still persist profit_amount,
    not the percent.
    """
    return _to_float(service_doc.get("default_profit_percent"), 0.0) or 0.0

def _get_customer_profit_override_percent(service_id, customer_id_obj):
    ov = service_profits_col.find_one({"service_id": service_id, "customer_id": customer_id_obj})
    return _to_float(ov.get("profit_percent"), None) if ov else None

def _effective_profit_percent(service_doc, customer_id_obj):
    override = _get_customer_profit_override_percent(service_doc["_id"], customer_id_obj)
    return override if override is not None else _get_service_default_profit_percent(service_doc)

def _pick_offer_base_amount_from_service(svc_doc, value_obj, raw_value):
    """
    Try to locate the matching offer in the service and return its base amount.
    Match by value_obj.id or value_obj.volume or raw value text.
    Returns None if no match.
    """
    try:
        offers = svc_doc.get("offers") or []
        vid = (value_obj or {}).get("id")
        vvol = (value_obj or {}).get("volume")
        for of in offers:
            of_val = of.get("value")
            of_amt = _to_float(of.get("amount"))
            # normalize dictionary-ish value fields
            if isinstance(of_val, str) and of_val.strip().startswith("{") and of_val.strip().endswith("}"):
                try:
                    of_val = json.loads(of_val)
                except Exception:
                    try:
                        of_val = ast.literal_eval(of_val)
                    except Exception:
                        pass
            # Match by id/volume when possible, else by string equality
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
    """
    Return (base_amount, profit_amount)
    Priority:
      1) base_amount_hint (from client) → profit = amount_total - base_hint
      2) If no hint, back out base from percent: base = amount_total / (1 + p)
         (only as a last resort; we still persist absolute profit_amount)
    """
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
    # fallback: infer base from percent
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

# ===== Resolve fields for shared-bundle =====
def _resolve_network_id(item: dict, value_obj: dict, svc_doc: dict | None):
    # 1) explicit in payload
    nid = (item or {}).get("network_id") or (value_obj or {}).get("network_id")
    if nid not in (None, "", []):
        try:
            return int(nid)
        except Exception:
            pass

    # 2) from bound service doc
    if svc_doc:
        try:
            if "network_id" in svc_doc and svc_doc["network_id"] not in (None, ""):
                return int(svc_doc["network_id"])
            guess = (svc_doc.get("name") or svc_doc.get("network") or "").strip().upper()
            if guess and guess in NETWORK_ID_FALLBACK:
                return int(NETWORK_ID_FALLBACK[guess])
        except Exception:
            pass

    # 3) from serviceName fallback (only if we have no service doc)
    if not svc_doc:
        name = (item.get("serviceName") or "").strip().upper()
        if name in NETWORK_ID_FALLBACK:
            return int(NETWORK_ID_FALLBACK[name])

    return None

def _resolve_shared_bundle(item: dict, value_obj: dict):
    vol = (value_obj or {}).get("volume")
    if vol not in (None, "", []):
        try:
            return int(vol)
        except Exception:
            pass
    sb = (item or {}).get("shared_bundle")
    if sb not in (None, "", []):
        try:
            return int(sb)
        except Exception:
            pass
    return None

# ===== Call Toppily with shared-bundle body =====
def _send_toppily_shared_bundle(phone: str, network_id: int, shared_bundle: int, trx_ref: str,
                                order_id: str, debug_events: list):
    if not TOPPILY_API_KEY.strip():
        err = {"success": False, "message": "API key not set", "http_status": 500}
        jlog("toppily_config_error", order_id=order_id, trx_ref=trx_ref)
        return False, err

    masked = phone[:3] + "***" + phone[-2:] if phone else ""
    body = {"recipient_msisdn": phone, "network_id": int(network_id), "shared_bundle": int(shared_bundle), "trx_ref": trx_ref}
    jlog("toppily_request_body", order_id=order_id, trx_ref=trx_ref,
         body={"recipient_msisdn": masked, "network_id": body["network_id"], "shared_bundle": body["shared_bundle"], "trx_ref": trx_ref})

    try:
        verify_val = (_CA_BUNDLE if PRIMARY_VERIFY_SSL else False)
        ok, data, resp, text = _post_requests(body, verify_val)
        dbg = _resp_debug(resp, text)
        blocked = _is_cloudflare_block(text, resp.headers, resp.status_code)
        payload = {**data, "http_status": resp.status_code}
        if blocked:
            payload.setdefault("error", "Cloudflare challenge blocked the request")
            payload["blocked_by_cloudflare"] = True
        jlog("toppily_call", order_id=order_id, trx_ref=trx_ref, verify=bool(verify_val), ok=ok,
             status=resp.status_code, blocked_by_cloudflare=blocked, debug=dbg)
        debug_events.append({"when": datetime.utcnow(), "stage":"primary","verify":bool(verify_val),
                             "ok":ok,"blocked_by_cloudflare":blocked,"debug":dbg})

        if blocked and USE_CLOUDSCRAPER_FALLBACK:
            ok2, data2, resp2, text2 = _post_cloudscraper(body)
            if resp2 is not None:
                dbg2 = _resp_debug(resp2, text2)
                blocked2 = _is_cloudflare_block(text2, resp2.headers, resp2.status_code)
                payload2 = {**data2, "http_status": resp2.status_code, "note": "cloudscraper fallback used"}
                if blocked2:
                    payload2.setdefault("error","Cloudflare challenge blocked the request (cloudscraper)")
                    payload2["blocked_by_cloudflare"] = True
                jlog("toppily_cloudscraper", order_id=order_id, trx_ref=trx_ref, ok=ok2,
                     status=resp2.status_code, blocked_by_cloudflare=blocked2, debug=dbg2)
                debug_events.append({"when": datetime.utcnow(), "stage":"cloudscraper","verify":None,
                                     "ok":ok2,"blocked_by_cloudflare":blocked2,"debug":dbg2})
                return (ok2 and not blocked2), payload2

        return (ok and not blocked), payload

    except requests.exceptions.SSLError as e:
        jlog("toppily_ssl_error", order_id=order_id, trx_ref=trx_ref, error=str(e))
        try:
            ok, data, resp, text = _post_requests(body, False)
            dbg = _resp_debug(resp, text)
            blocked = _is_cloudflare_block(text, resp.headers, resp.status_code)
            payload = {**data, "http_status": resp.status_code, "note":"insecure verify=False (diagnostic)"}
            if blocked:
                payload.setdefault("error","Cloudflare challenge blocked the request")
                payload["blocked_by_cloudflare"] = True
            jlog("toppily_insecure_diag", order_id=order_id, trx_ref=trx_ref, ok=ok,
                 status=resp.status_code, blocked_by_cloudflare=blocked, debug=dbg)
            debug_events.append({"when": datetime.utcnow(), "stage":"insecure-diagnostic","verify":False,
                                 "ok":ok,"blocked_by_cloudflare":blocked,"debug":dbg})

            if blocked and USE_CLOUDSCRAPER_FALLBACK:
                ok2, data2, resp2, text2 = _post_cloudscraper(body)
                if resp2 is not None:
                    dbg2 = _resp_debug(resp2, text2)
                    blocked2 = _is_cloudflare_block(text2, resp2.headers, resp2.status_code)
                    payload2 = {**data2, "http_status": resp2.status_code, "note":"cloudscraper fallback used"}
                    if blocked2:
                        payload2.setdefault("error","Cloudflare challenge blocked the request (cloudscraper)")
                        payload2["blocked_by_cloudflare"] = True
                    jlog("toppily_cloudscraper", order_id=order_id, trx_ref=trx_ref, ok=ok2,
                         status=resp2.status_code, blocked_by_cloudflare=blocked2, debug=dbg2)
                    debug_events.append({"when": datetime.utcnow(), "stage":"cloudscraper","verify":None,
                                         "ok":ok2,"blocked_by_cloudflare":blocked2,"debug":dbg2})
                    return (ok2 and not blocked2), payload2

            return (ok and not blocked), payload
        except requests.RequestException as e2:
            return False, {"success": False, "error": f"SSL + insecure diag failed: {e2}", "http_status": 597}

    except requests.RequestException as e:
        jlog("toppily_network_error", order_id=order_id, trx_ref=trx_ref, error=str(e))
        return False, {"success": False, "error": str(e), "http_status": 599}

# ===== Auto-deliver (processing → delivered after 30 minutes) =====
_scheduler: BackgroundScheduler | None = None

def _autodeliver_due_orders():
    """Flip processing → delivered when 30+ mins old."""
    cutoff = datetime.utcnow() - timedelta(minutes=30)
    now = datetime.utcnow()
    res = orders_col.update_many(
        {"status": "processing", "created_at": {"$lte": cutoff}},
        {"$set": {"status": "delivered", "delivered_at": now, "updated_at": now}}
    )
    if getattr(res, "modified_count", 0):
        jlog("autodeliver_run", modified=res.modified_count)

def _ensure_scheduler_started():
    global _scheduler
    if _scheduler and _scheduler.running:
        return
    try:
        _scheduler = BackgroundScheduler(daemon=True)
        _scheduler.add_job(_autodeliver_due_orders, "interval", minutes=1, id="autodeliver", replace_existing=True)
        _scheduler.start()
        jlog("scheduler_started", job="autodeliver_every_minute")
    except Exception as e:
        jlog("scheduler_error", error=str(e))

# Start the scheduler on import (safe; no-op if already running)
_ensure_scheduler_started()

# ===== Route =====
@checkout_bp.route("/checkout", methods=["POST"])
def process_checkout():
    try:
        # Opportunistic sweep to catch up (in case scheduler paused/restarted)
        _autodeliver_due_orders()

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

        # Total requested (this should already be the customer-facing price — includes profit)
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

        # We will charge for: successful API lines + any line we keep "processing"
        total_delivered_api_amount = 0.0    # amount for API lines that are delivered (formerly 'completed')
        total_processing_amount = 0.0       # amount for non-API, API-missing-fields, or API "failed" (forced to processing)
        api_requested_total = 0.0
        has_processing = False

        # NEW: rollup profit
        profit_amount_total = 0.0

        for idx, item in enumerate(cart, start=1):
            phone = (item.get("phone") or "").strip()
            value_obj = _coerce_value_obj(item.get("value_obj") or item.get("value"))
            amt_total = _money(item.get("amount"))  # customer-facing total (base + profit)
            service_id_raw = item.get("serviceId")
            svc_doc = None
            svc_type = None
            svc_name = item.get("serviceName") or None

            # Resolve service and its type (+ offers for base recovery)
            if service_id_raw:
                try:
                    svc_doc = services_col.find_one(
                        {"_id": ObjectId(service_id_raw)},
                        {"type": 1, "network_id": 1, "name": 1, "network": 1, "offers": 1, "default_profit_percent": 1}
                    )
                    if svc_doc:
                        st = svc_doc.get("type")
                        svc_type = (st.strip().upper() if isinstance(st, str) else st)
                        svc_name = svc_doc.get("name") or svc_doc.get("network") or svc_name
                except Exception:
                    svc_doc = None
                    svc_type = None

            # --- Compute base_amount & profit_amount (ABSOLUTE), not just a percent ---
            base_hint = _to_float(item.get("base_amount"))
            if base_hint is None:
                # try to pick from offers
                base_hint = _pick_offer_base_amount_from_service(svc_doc or {}, value_obj, item.get("value"))
            eff_p = 0.0
            if svc_doc:
                eff_p = _effective_profit_percent(svc_doc, user_id)
            base_amount, profit_amount = _derive_base_profit(amt_total, base_hint, eff_p)
            profit_amount_total += profit_amount

            # ---------- NON-API path → FORCE processing and CHARGE ----------
            if not svc_doc or (svc_type and str(svc_type).upper() != "API"):
                has_processing = True
                total_processing_amount += amt_total
                results.append({
                    "phone": phone,
                    "base_amount": base_amount,
                    "amount": amt_total,
                    "profit_amount": profit_amount,
                    "profit_percent_used": eff_p,
                    "value": item.get("value"),
                    "value_obj": value_obj,
                    "serviceId": service_id_raw,
                    "serviceName": svc_name,
                    "service_type": svc_type if svc_type else ("unknown" if not svc_doc else None),
                    "line_status": "processing",                    # <— was 'pending'
                    "api_status": "not_applicable",
                    "api_response": {"note": "Service not API; queued for processing"}
                })
                continue

            # ---------- API path ----------
            api_requested_total += amt_total

            # Validate fields for API call
            network_id = _resolve_network_id(item, value_obj, svc_doc)
            shared_bundle = _resolve_shared_bundle(item, value_obj)

            if not phone or network_id is None or shared_bundle is None:
                # Missing API fields → treat as processing and CHARGE
                has_processing = True
                total_processing_amount += amt_total
                results.append({
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
                    "line_status": "processing",
                    "api_status": "skipped_missing_fields",
                    "api_response": {
                        "note": "API fields missing; queued for processing",
                        "got": {"phone": bool(phone), "network_id": network_id, "shared_bundle": shared_bundle}
                    }
                })
                continue

            trx_ref = f"{order_id}_{idx}_{uuid.uuid4().hex[:6]}"
            ok, payload = _send_toppily_shared_bundle(phone, network_id, shared_bundle, trx_ref, order_id, debug_events)

            if ok:
                # Delivered immediately
                results.append({
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
                    "trx_ref": trx_ref,
                    "line_status": "delivered",          # <— rename completed → delivered
                    "api_status": "success",
                    "api_response": payload
                })
                total_delivered_api_amount += amt_total
            else:
                # FORCE processing (no failure surfaced) and CHARGE
                has_processing = True
                total_processing_amount += amt_total
                results.append({
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
                    "trx_ref": trx_ref,
                    "line_status": "processing",          # <— was 'failed'
                    "api_status": "processing",
                    "api_response": payload
                })

        # keep last debug entries
        if len(debug_events) > 10:
            debug_events = debug_events[-10:]

        # Total to charge now = delivered API + all processing lines (totals already include profit)
        total_to_charge_now = round(total_delivered_api_amount + total_processing_amount, 2)

        # ==== If nothing counted yet (extreme case) → charge full cart and mark processing ====
        if total_to_charge_now <= 0:
            status = "processing"

            # Deduct the full cart total (balance already verified >= total_requested)
            balances_col.update_one(
                {"user_id": user_id},
                {"$inc": {"amount": -total_requested}, "$set": {"updated_at": datetime.utcnow()}},
                upsert=True
            )

            # Persist order
            orders_col.insert_one({
                "user_id": user_id,
                "order_id": order_id,
                "items": results,
                "total_amount": total_requested,
                "charged_amount": total_requested,      # charged entire cart
                "profit_amount_total": round(profit_amount_total, 2),
                "status": status,                       # "processing"
                "paid_from": method,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "debug": {"events": debug_events}
            })

            # Record transaction for full cart amount
            transactions_col.insert_one({
                "user_id": user_id,
                "amount": total_requested,
                "reference": order_id,
                "status": "success",
                "type": "purchase",
                "gateway": "Wallet",
                "currency": "GHS",
                "created_at": datetime.utcnow(),
                "verified_at": datetime.utcnow(),
                "meta": {
                    "order_status": status,
                    "api_delivered_amount": 0.0,
                    "processing_amount": round(total_requested, 2),
                    "profit_amount_total": round(profit_amount_total, 2)
                }
            })

            friendly_msg = (
                f"✅ We’ve received your order. It’s now processing and will be delivered within 30 minutes. "
                f"Order ID: {order_id}"
            )

            return jsonify({
                "success": True,
                "message": friendly_msg,
                "order_id": order_id,
                "status": status,
                "charged_amount": total_requested,
                "profit_amount_total": round(profit_amount_total, 2),
                "items": results
            }), 200
        # ==== END extreme case ====

        # Deduct now for: (delivered API + ALL processing)
        balances_col.update_one(
            {"user_id": user_id},
            {"$inc": {"amount": -total_to_charge_now}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True
        )

        # Determine overall status (no 'failed'/'partial' exposed)
        status = "delivered" if (not has_processing and total_delivered_api_amount >= api_requested_total) else "processing"

        # Persist order
        orders_col.insert_one({
            "user_id": user_id,
            "order_id": order_id,
            "items": results,
            "total_amount": total_requested,                # cart grand total (UI computed)
            "charged_amount": total_to_charge_now,          # charged now
            "profit_amount_total": round(profit_amount_total, 2),
            "status": status,                               # delivered | processing
            "paid_from": method,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "debug": {"events": debug_events}
        })

        # Record transaction for the amount actually charged now
        transactions_col.insert_one({
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
                "api_delivered_amount": round(total_delivered_api_amount, 2),
                "processing_amount": round(total_processing_amount, 2),
                "profit_amount_total": round(profit_amount_total, 2)
            }
        })

        # Response message (always success to frontend)
        if status == "delivered":
            msg = "✅ Order delivered. Order ID: {oid}".format(oid=order_id)
        else:
            msg = ("📝 Order received and is processing. "
                   "We’ve charged your wallet. Order ID: {oid}").format(oid=order_id)

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
        jlog("checkout_uncaught", error=traceback.format_exc())
        # Still keep a generic failure here for truly exceptional cases (auth/validation/500)
        return jsonify({"success": False, "message": "Server error"}), 500
