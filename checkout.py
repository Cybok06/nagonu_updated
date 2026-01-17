from flask import Blueprint, request, jsonify, session, render_template, abort
from bson import ObjectId
from datetime import datetime, timedelta
import os, uuid, random, requests, traceback, json, ast, re, threading

from db import db

checkout_bp = Blueprint("checkout", __name__)

# MongoDB Collections
balances_col        = db["balances"]
orders_col          = db["orders"]
transactions_col    = db["transactions"]
services_col        = db["services"]
service_profits_col = db["service_profits"]  # per-customer overrides
users_col           = db["users"]  # ✅ for invoice view


# ===== DataConnect Provider Config (replaces old DataVerse) ===================
DATACONNECT_BASE_URL = "https://dataconnectgh.com/api/v1"
DATACONNECT_API_KEY = os.getenv(
    "DATACONNECT_API_KEY",
    "90bcf2f236b8c95547b58b531f5c597df8a061a8",  # fallback; you can remove/harden
)


# ===== Portal-02 Provider Config ==============================================
PORTAL02_BASE_URL = "https://www.portal-02.com/api/v1"
PORTAL02_API_KEY = os.getenv("PORTAL02_API_KEY", "dk_mJmQDFQWmDId4RT_c5HrEghcgwujPAFf")
PORTAL02_WEBHOOK_URL = os.getenv(
    "PORTAL02_WEBHOOK_URL",
    "https://www.portal-02.com/api/webhooks/orders",
)

# Default offer slugs (can be overridden per-service or per-item)
PORTAL02_OFFER_SLUG_MTN_NORMAL = "master_beneficiary_data_bundle"  # MTN normal
PORTAL02_OFFER_SLUG_TELECEL    = "telecel_expiry_bundle"
PORTAL02_OFFER_SLUG_ISHARE     = "ishare_data_bundle"

# Network ID fallback (internal use)
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
    Not sent to providers.
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


def _resolve_dataconnect_network(svc_doc: dict | None, item: dict) -> str | None:
    """
    Resolve generic 'network' slug we also reuse:
      - 'mtn'
      - 'telecel'
      - 'airteltigo'
    Used for routing (DataConnect vs Portal-02).
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

    # Telecel / Vodafone rebrand
    if "telecel" in joined or "vodafone" in joined:
        return "telecel"

    # AirtelTigo / AT / iShare
    if (
        "airteltigo" in joined
        or "airtel tigo" in joined
        or "airtel-tigo" in joined
        or "at - ishare" in joined
        or "i share" in joined
        or "ishare" in joined
    ):
        return "airteltigo"

    return None


def _resolve_package_size_gb(value_obj: dict, item: dict) -> int | None:
    """
    Resolve bundle size (integer GB) to use as Portal/DataConnect "volume".
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


def _normalize_msisdn_gh(phone: str) -> str:
    """
    Convert Ghana numbers to international format for Portal-02.
    """
    p = re.sub(r"\D", "", phone or "")
    if not p:
        return phone
    if p.startswith("0") and len(p) == 10:
        return "233" + p[1:]
    if p.startswith("233") and len(p) == 12:
        return p
    return p


def _resolve_portal02_offer_slug(svc_doc: dict | None, item: dict) -> str:
    """
    Decide which offerSlug to send to Portal-02.
    """
    if item.get("offerSlug"):
        return str(item["offerSlug"])

    if svc_doc:
        if svc_doc.get("portal02_offer_slug"):
            return str(svc_doc["portal02_offer_slug"])
        if svc_doc.get("offerSlug"):
            return str(svc_doc["offerSlug"])

        nm = str(svc_doc.get("name", "")).lower()
        net = str(svc_doc.get("network", "")).lower()
        combo = f"{nm} {net}"

        if "telecel" in combo:
            return PORTAL02_OFFER_SLUG_TELECEL

        if (
            "ishare" in combo
            or "i share" in combo
            or "at - ishare" in combo
            or ("airtel" in combo and "tigo" in combo)
        ):
            return PORTAL02_OFFER_SLUG_ISHARE

    return PORTAL02_OFFER_SLUG_MTN_NORMAL


# ===== Provider callers (used by background worker) ==========================
def _send_dataconnect_order(
    phone: str,
    network_id: int,
    shared_bundle: int,
    external_ref: str,
    order_id: str,
    debug_events: list,
):
    """
    Sends a single bundle order to DataConnect.

    POST https://dataconnectgh.com/api/v1/buy-other-package

    Body JSON:
        {
            "recipient_msisdn": "0551053716",
            "network_id": 3,
            "shared_bundle": 1000
        }
    """
    if not DATACONNECT_API_KEY:
        err = {
            "success": False,
            "message": "DATACONNECT API key not configured",
            "http_status": 500,
        }
        jlog("dataconnect_config_error", order_id=order_id, ref=external_ref)
        return False, err

    url = f"{DATACONNECT_BASE_URL.rstrip('/')}/buy-other-package"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": DATACONNECT_API_KEY,
    }
    body = {
        "recipient_msisdn": phone,
        "network_id": int(network_id),
        "shared_bundle": int(shared_bundle),
    }

    masked = phone[:3] + "***" + phone[-2:] if phone and len(phone) >= 5 else "***"

    jlog(
        "dataconnect_request_body",
        order_id=order_id,
        ref=external_ref,
        url=url,
        body={
            "recipient_msisdn": masked,
            "network_id": body["network_id"],
            "shared_bundle": body["shared_bundle"],
        },
    )

    try:
        resp = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=45,
        )
        text = resp.text or ""
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": text} if text else {}

        ok = (
            resp.status_code in (200, 201)
            and isinstance(payload, dict)
            and bool(payload.get("success")) is True
        )
        if isinstance(payload, dict):
            payload.setdefault("http_status", resp.status_code)

        dbg = {
            "status": resp.status_code,
            "body_len": len(text),
        }
        jlog("dataconnect_response", order_id=order_id, ref=external_ref, payload=payload)
        jlog("dataconnect_call", order_id=order_id, ref=external_ref, ok=ok, debug=dbg)

        debug_events.append(
            {
                "when": datetime.utcnow(),
                "stage": "dataconnect-buy-other-package",
                "ok": ok,
                "http_status": resp.status_code,
            }
        )
        return ok, payload

    except requests.RequestException as e:
        jlog(
            "dataconnect_network_error",
            order_id=order_id,
            ref=external_ref,
            error=str(e),
        )
        return False, {
            "success": False,
            "error": str(e),
            "type": "NETWORK_ERROR",
            "http_status": 599,
        }


def _send_portal02_order(phone: str, network: str, volume_gb: int,
                         offer_slug: str,
                         external_ref: str, order_id: str, debug_events: list):
    if not PORTAL02_API_KEY or PORTAL02_API_KEY == "dk_your_api_key_here":
        err = {
            "success": False,
            "error": "PORTAL02 API key not configured",
            "type": "CONFIG_ERROR",
            "http_status": 500,
        }
        jlog("portal02_config_error", order_id=order_id, ref=external_ref)
        return False, err

    url = f"{PORTAL02_BASE_URL.rstrip('/')}/order/{network}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": PORTAL02_API_KEY,
    }
    body = {
        "type": "single",
        "volume": int(volume_gb),
        "phone": phone,
        "offerSlug": offer_slug,
        "webhookUrl": PORTAL02_WEBHOOK_URL,
    }

    masked = phone[:5] + "***" + phone[-2:] if phone and len(phone) >= 7 else "***"
    jlog(
        "portal02_request_body",
        order_id=order_id,
        ref=external_ref,
        body={
            "network": network,
            "phone": masked,
            "volume": body["volume"],
            "offerSlug": body["offerSlug"],
        },
    )

    try:
        resp = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=45,
        )
        text = resp.text or ""
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": text} if text else {}

        ok = (
            resp.status_code in (200, 201)
            and isinstance(payload, dict)
            and bool(payload.get("success")) is True
        )
        if isinstance(payload, dict):
            payload.setdefault("http_status", resp.status_code)

        dbg = {
            "status": resp.status_code,
            "body_len": len(text),
        }
        jlog("portal02_response", order_id=order_id, ref=external_ref, payload=payload)
        jlog("portal02_call", order_id=order_id, ref=external_ref, ok=ok, debug=dbg)
        debug_events.append(
            {
                "when": datetime.utcnow(),
                "stage": "portal02-place-order",
                "ok": ok,
                "http_status": resp.status_code,
            }
        )
        return ok, payload

    except requests.RequestException as e:
        jlog("portal02_network_error", order_id=order_id, ref=external_ref, error=str(e))
        return False, {"success": False, "error": str(e), "type": "NETWORK_ERROR", "http_status": 599}


# ===== Unavailability checker ================================================
def _service_unavailability_reason(svc_doc: dict):
    """
    Returns (is_unavailable, reason_text)
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
    if not phone or network_id is None or bundle_key is None:
        return False

    window_start = datetime.utcnow() - timedelta(minutes=DUP_WINDOW_MINUTES)
    kind, bval = bundle_key

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


# ===== BACKGROUND WORKER =====================================================
def _background_process_providers(order_id: str, api_jobs: list[dict]):
    """
    Runs in a separate thread AFTER the HTTP response is sent.
    It picks queued lines and calls DataConnect / Portal-02, then updates the order doc.
    """
    jlog("checkout_bg_worker_start", order_id=order_id, jobs=len(api_jobs))
    local_debug = []

    for job in api_jobs:
        try:
            line_ref = job["provider_request_order_id"]
            phone = job["phone"]
            package_size_gb = job.get("package_size_gb")
            provider = job["provider"]
            portal_network_slug = job.get("portal02_network_slug")
            svc_id = job.get("service_id")

            dataconnect_network_id = job.get("network_id")
            dataconnect_shared_bundle = job.get("shared_bundle")

            svc_doc = None
            if svc_id:
                try:
                    svc_doc = services_col.find_one(
                        {"_id": svc_id},
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
                            "portal02_offer_slug": 1,
                            "offerSlug": 1,
                        },
                    )
                except Exception:
                    svc_doc = None

            ok = False
            payload = {}

            if provider == "dataconnect":
                # DataConnect order
                ok, payload = _send_dataconnect_order(
                    phone=phone,
                    network_id=dataconnect_network_id,
                    shared_bundle=dataconnect_shared_bundle,
                    external_ref=line_ref,
                    order_id=order_id,
                    debug_events=local_debug,
                )

            elif provider == "portal02":
                offer_slug = _resolve_portal02_offer_slug(svc_doc or {}, job.get("raw_item") or {})
                normalized_phone = _normalize_msisdn_gh(phone)
                ok, payload = _send_portal02_order(
                    phone=normalized_phone,
                    network=portal_network_slug,
                    volume_gb=package_size_gb,
                    offer_slug=offer_slug,
                    external_ref=line_ref,
                    order_id=order_id,
                    debug_events=local_debug,
                )

            provider_ref = None
            provider_order_id = None
            if isinstance(payload, dict):
                provider_ref = (
                    payload.get("transaction_code")
                    or payload.get("reference")
                    or payload.get("order_reference")
                )
                provider_order_id = (
                    payload.get("orderId")
                    or payload.get("order_id")
                    or payload.get("transaction_code")
                )

            # Update this specific line inside the order items
            orders_col.update_one(
                {
                    "order_id": order_id,
                    "items.provider_request_order_id": line_ref,
                },
                {
                    "$set": {
                        "items.$.api_status": "success" if ok else "processing",
                        "items.$.api_response": payload,
                        "items.$.provider_reference": provider_ref,
                        "items.$.provider_order_id": provider_order_id,
                    }
                },
            )
        except Exception as e:
            jlog("checkout_bg_worker_line_error", order_id=order_id, error=str(e))

    if local_debug:
        # append debug entries
        try:
            orders_col.update_one(
                {"order_id": order_id},
                {"$push": {"debug.events": {"$each": local_debug}}},
            )
        except Exception:
            pass

    jlog("checkout_bg_worker_end", order_id=order_id, jobs=len(api_jobs))


# ===== Route (FAST RESPONSE, PROVIDERS IN BACKGROUND) ========================
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

        # Total requested (customer-facing)
        total_requested = sum(_money(item.get("amount")) for item in cart)
        if total_requested <= 0:
            return jsonify({"success": False, "message": "Total amount must be greater than zero"}), 400

        order_id = generate_order_id()

        # Balance check
        bal_doc = balances_col.find_one({"user_id": user_id}) or {}
        current_balance = _money(bal_doc.get("amount", 0))
        jlog("checkout_balance", order_id=order_id, balance=current_balance, total=total_requested)
        if current_balance < total_requested:
            return jsonify({"success": False, "message": "❌ Insufficient wallet balance"}), 400

        results = []
        debug_events = []

        total_delivered_api_amount = 0.0  # stays 0.0 (we don't mark delivered immediately)
        total_processing_amount = 0.0
        api_requested_total = 0.0
        has_processing = False
        profit_amount_total = 0.0

        seen_keys = set()
        api_jobs = []  # lines to be sent to providers in the background worker

        for idx, item in enumerate(cart, start=1):
            phone = (item.get("phone") or "").strip()
            value_obj = _coerce_value_obj(item.get("value_obj") or item.get("value"))
            amt_total = _money(item.get("amount"))
            amount_key = _normalize_amount_key(amt_total)

            service_id_raw = item.get("serviceId")
            svc_doc = None
            svc_type = None
            svc_name = item.get("serviceName") or None

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
                            "portal02_offer_slug": 1,
                            "offerSlug": 1,
                        },
                    )
                    if svc_doc:
                        st = svc_doc.get("type")
                        svc_type = (st.strip().upper() if isinstance(st, str) else st)
                        svc_name = svc_doc.get("name") or svc_doc.get("network") or svc_name
                except Exception:
                    svc_doc = None
                    svc_type = None

            # HARD GATE: availability
            is_unavail, reason_text = _service_unavailability_reason(svc_doc)
            if is_unavail:
                return jsonify(
                    {
                        "success": False,
                        "message": reason_text,
                        "unavailable": {
                            "serviceId": service_id_raw,
                            "serviceName": svc_name,
                            "reason": reason_text,
                        },
                    }
                ), 400

            # Duplicate guards
            network_id = _resolve_network_id(item, value_obj, svc_doc)
            bundle_key = _build_bundle_key(value_obj, item)

            if phone and (network_id is not None) and (bundle_key is not None):
                cart_key = (phone, int(network_id), bundle_key[1], bundle_key[0], amount_key)
                if cart_key in seen_keys:
                    results.append(
                        {
                            "phone": phone,
                            "base_amount": 0.0,
                            "amount": 0.0,
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

            is_dup_strict = _has_processing_conflict_strict(
                phone, service_id_raw, svc_name, network_id, bundle_key, amount_key
            )
            if is_dup_strict:
                results.append(
                    {
                        "phone": phone,
                        "base_amount": 0.0,
                        "amount": 0.0,
                        "originally_requested_amount": amt_total,
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

            # base & profit (requested): profit = amount - base_amount
            base_hint = _to_float(item.get("base_amount"))
            base_amount = round(float(base_hint if base_hint is not None else 0.0), 2)
            profit_amount = max(0.0, round(amt_total - base_amount, 2))
            profit_percent_used = round((profit_amount / base_amount) * 100.0, 2) if base_amount > 0 else 0.0
            profit_amount_total += profit_amount

            # No service doc → manual processing
            if not svc_doc:
                has_processing = True
                total_processing_amount += amt_total
                results.append(
                    {
                        "phone": phone,
                        "base_amount": base_amount,
                        "amount": amt_total,
                        "profit_amount": profit_amount,
                        "profit_percent_used": profit_percent_used,
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

            # Provider selection
            resolved_network = _resolve_dataconnect_network(svc_doc, item)
            svc_name_norm = (svc_name or "").strip().lower()
            svc_network_norm = (svc_doc.get("network") or "").strip().lower() if svc_doc else ""
            combo_name_net = f"{svc_name_norm} {svc_network_norm}"

            is_mtn_express = (svc_name_norm == "mtn express")
            is_mtn_normal = (svc_name_norm == "mtn normal")
            is_telecel_bundle = ("telecel" in combo_name_net)
            is_ishare_bundle = (
                "ishare" in combo_name_net
                or "i share" in combo_name_net
                or "at - ishare" in combo_name_net
            )

            svc_type_flag = (svc_type or "").strip().upper() if isinstance(svc_type, str) else ""
            type_allows_api = svc_type_flag in ("ON", "API")
            api_allowed = type_allows_api or is_telecel_bundle or is_ishare_bundle
            if svc_type_flag == "OFF":
                api_allowed = False

            # DataConnect: currently only MTN Express uses DataConnect (like old DataVerse slot)
            use_dataconnect = (resolved_network == "mtn" and is_mtn_express and api_allowed)

            portal02_network_slug = None
            if api_allowed:
                if resolved_network == "mtn" and is_mtn_normal:
                    portal02_network_slug = "mtn"
                elif resolved_network == "telecel" and is_telecel_bundle:
                    portal02_network_slug = "telecel"
                elif resolved_network == "airteltigo" and is_ishare_bundle:
                    portal02_network_slug = "airteltigo"

            use_portal02 = portal02_network_slug is not None

            jlog(
                "checkout_line_routing",
                order_id=order_id,
                idx=idx,
                serviceId=service_id_raw,
                svc_name=svc_name,
                resolved_network=resolved_network,
                svc_type_flag=svc_type_flag,
                is_mtn_express=is_mtn_express,
                is_mtn_normal=is_mtn_normal,
                is_telecel_bundle=is_telecel_bundle,
                is_ishare_bundle=is_ishare_bundle,
                api_allowed=api_allowed,
                use_dataconnect=use_dataconnect,
                use_portal02=use_portal02,
                portal02_network_slug=portal02_network_slug,
            )

            if not (use_dataconnect or use_portal02):
                has_processing = True
                total_processing_amount += amt_total

                if not api_allowed:
                    note = (
                        "API calls disabled for this service (type OFF and not a mapped Telecel/iShare); "
                        "queued for manual processing."
                    )
                    api_status = "not_applicable_type_off"
                else:
                    note = (
                        "API is used for MTN EXPRESS (DataConnect) and MTN NORMAL / TELECEL / AIRTELTIGO iShare "
                        "via Portal-02, but this line did not match any mapped combination; queued for manual processing."
                    )
                    api_status = "not_applicable_network"

                results.append(
                    {
                        "phone": phone,
                        "base_amount": base_amount,
                        "amount": amt_total,
                        "profit_amount": profit_amount,
                        "profit_percent_used": profit_percent_used,
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
                            "resolved_network": resolved_network,
                            "serviceName": svc_name,
                            "service_type_flag": svc_type_flag,
                        },
                    }
                )
                continue

            # From here: API-eligible line → we will send it via BACKGROUND worker
            api_requested_total += amt_total

            package_size_gb = _resolve_package_size_gb(value_obj, item)

            # Resolve shared_bundle for DataConnect from your stored offer structure
            shared_bundle = None
            if isinstance(value_obj, dict):
                sb = value_obj.get("volume") or value_obj.get("shared_bundle") or value_obj.get("mb")
                if sb not in (None, "", []):
                    try:
                        shared_bundle = int(float(sb))
                    except Exception:
                        shared_bundle = None
            if shared_bundle is None and package_size_gb is not None:
                shared_bundle = int(package_size_gb * 1000)

            if not phone or package_size_gb is None:
                has_processing = True
                total_processing_amount += amt_total
                results.append(
                    {
                        "phone": phone,
                        "base_amount": base_amount,
                        "amount": amt_total,
                        "profit_amount": profit_amount,
                        "profit_percent_used": profit_percent_used,
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
                                "resolved_network": resolved_network,
                                "package_size_gb": package_size_gb,
                            },
                        },
                    }
                )
                continue

            # Prepare background job meta
            external_ref = f"{order_id}_{idx}_{uuid.uuid4().hex[:6]}"

            if use_dataconnect:
                provider_name = "dataconnect"
                provider_network_slug = resolved_network  # for debug only
            else:
                provider_name = "portal02"
                provider_network_slug = portal02_network_slug

            has_processing = True
            total_processing_amount += amt_total

            # store line with "queued" status; background worker will update
            line_record = {
                "phone": phone,
                "base_amount": base_amount,
                "amount": amt_total,
                "profit_amount": profit_amount,
                "profit_percent_used": profit_percent_used,
                "value": item.get("value"),
                "value_obj": value_obj,
                "serviceId": service_id_raw,
                "serviceName": svc_name,
                "service_type": svc_type,
                "provider": provider_name,
                "provider_network": provider_network_slug,
                "provider_reference": None,
                "provider_order_id": None,
                "provider_request_order_id": external_ref,
                "network_id": network_id,
                "bundle_key": ({"kind": bundle_key[0], "value": bundle_key[1]} if bundle_key else None),
                "line_amount_key": amount_key,
                "line_status": "processing",
                "api_status": "queued",      # <--- queued for background call
                "api_response": {"note": "Queued for background API call"},
            }

            # For transparency/debug you can store shared_bundle on the line as well
            if use_dataconnect:
                line_record["shared_bundle"] = shared_bundle

            results.append(line_record)

            job_payload = {
                "provider_request_order_id": external_ref,
                "phone": phone,
                "provider": provider_name,
                "portal02_network_slug": portal02_network_slug,
                "package_size_gb": package_size_gb,
                "service_id": svc_doc["_id"],
                "raw_item": item,
            }

            if provider_name == "dataconnect":
                job_payload["network_id"] = network_id
                job_payload["shared_bundle"] = shared_bundle

            api_jobs.append(job_payload)

        if len(debug_events) > 10:
            debug_events = debug_events[-10:]

        total_to_charge_now = round(total_delivered_api_amount + total_processing_amount, 2)

        # If nothing to charge (all skipped)
        if total_to_charge_now <= 0:
            orders_col.insert_one(
                {
                    "user_id": user_id,
                    "order_id": order_id,
                    "items": results,
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
                        "redirect_url": f"/invoice/{order_id}",
                        "status": "skipped",
                        "charged_amount": 0.0,
                        "profit_amount_total": 0.0,
                        "skipped_count": skipped_count,
                        "items": results,
                    }
                ),
                200,
            )

        # Deduct balance NOW
        balances_col.update_one(
            {"user_id": user_id},
            {"$inc": {"amount": -total_to_charge_now}, "$set": {"updated_at": datetime.utcnow()}},
            upsert=True,
        )

        status = "processing"

        # Persist order
        orders_col.insert_one(
            {
                "user_id": user_id,
                "order_id": order_id,
                "items": results,
                "total_amount": total_requested,
                "charged_amount": total_to_charge_now,
                "profit_amount_total": round(profit_amount_total, 2),
                "status": status,
                "paid_from": method,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "debug": {"events": debug_events},
            }
        )

        # Record transaction
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
                    "api_delivered_amount": round(total_delivered_api_amount, 2),
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

        # 🔥 Spawn background worker for provider calls (does not block response)
        if api_jobs:
            try:
                t = threading.Thread(
                    target=_background_process_providers,
                    args=(order_id, api_jobs),
                    daemon=True,
                )
                t.start()
            except Exception as e:
                jlog("checkout_bg_spawn_error", order_id=order_id, error=str(e))

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
                    "redirect_url": f"/invoice/{order_id}",  # frontend already uses this
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
        return jsonify({"success": False, "message": "Server error"}), 500


# ===== Invoice view (same blueprint) =========================================
@checkout_bp.route("/invoice/<order_id>")
def invoice_view(order_id):
    """
    Render a single invoice by Nagonu Order ID (e.g. NAN12345)
    Uses invoice.html template you already created.
    """
    order = orders_col.find_one({"order_id": order_id})
    if not order:
        abort(404)

    user = {}
    try:
        uid = order.get("user_id")
        if uid:
            user = users_col.find_one({"_id": uid}) or {}
    except Exception:
        user = {}

    customer_name = (
        user.get("name")
        or user.get("full_name")
        or user.get("username")
        or "Customer"
    )

    return render_template(
        "invoice.html",
        order=order,
        user=user,
        customer=customer_name,
    )
