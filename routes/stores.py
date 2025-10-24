# routes/stores.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import os, json, re, ast, traceback
import requests  # Paystack verify

from bson import ObjectId
from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for, send_file, abort

from db import db

# --- GridFS
import gridfs

# --- Collections
services_col = db["services"]
stores_col = db["stores"]
balances_col = db["balances"]           # kept for other parts of app; we won't deduct here
orders_col = db["orders"]
transactions_col = db["transactions"]
users_col = db["users"]

# --- GridFS bucket
fs = gridfs.GridFS(db)

stores_bp = Blueprint("stores", __name__)

# ===== import helpers already in your app =====
try:
    from checkout import (
        _effective_profit_percent,
        _pick_offer_base_amount_from_service,
        _derive_base_profit,
        _coerce_value_obj,
        _to_float,
        _money,
        generate_order_id,
        _resolve_network_id,
        _resolve_shared_bundle_express,
        _resolve_shared_bundle_toppily,
        _send_express_single,
        _send_toppily_shared_bundle,
        _service_unavailability_reason,
        jlog,
    )
except Exception:  # pragma: no cover
    from .checkout import (  # type: ignore
        _effective_profit_percent,
        _pick_offer_base_amount_from_service,
        _derive_base_profit,
        _coerce_value_obj,
        _to_float,
        _money,
        generate_order_id,
        _resolve_network_id,
        _resolve_shared_bundle_express,
        _resolve_shared_bundle_toppily,
        _send_express_single,
        _send_toppily_shared_bundle,
        _service_unavailability_reason,
        jlog,
    )

# -----------------------------------------------------------------------------
# Paystack keys (HARDCODED — NOT FROM .env)
# -----------------------------------------------------------------------------
PAYSTACK_PUBLIC_KEY: str = "pk_live_4c909336372002195e900f36649a37c56d0b8cdb"
PAYSTACK_SECRET_KEY: str = "sk_live_4316292a9beb8d5e619f6f97864bed7ed7f19fb7"

# ---------- small utils ----------
def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _slugify(s: str) -> str:
    s2 = (s or "").lower().strip()
    s2 = re.sub(r"[^a-z0-9]+", "-", s2).strip("-")
    return s2 or "store"

def _service_state(svc: Dict[str, Any]) -> Dict[str, Any]:
    t = (svc.get("type") or "API").upper()
    status = (svc.get("status") or "OPEN").upper()
    availability = (svc.get("availability") or "AVAILABLE").upper()
    closed_msg = svc.get("closed_message") or "This service is temporarily closed."
    oos_msg = svc.get("out_of_stock_message") or "This service is currently out of stock."
    can_order = t in {"API", "OFF", "MANUAL"} and status == "OPEN" and availability == "AVAILABLE"
    disabled_reason = None
    if not can_order:
        if status != "OPEN":
            disabled_reason = closed_msg
        elif availability != "AVAILABLE":
            disabled_reason = oos_msg
        else:
            disabled_reason = "This service is currently unavailable."
    return {
        "type": t,
        "status": status,
        "availability": availability,
        "can_order": can_order,
        "disabled_reason": disabled_reason,
    }

def _sorted_services(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def prio_tuple(s: Dict[str, Any]) -> Tuple[float, float, float, str]:
        prio = _to_float(s.get("priority")) or float("inf")
        display_order = _to_float(s.get("display_order")) or float("inf")
        created = s.get("created_at")
        ts = 0.0
        if isinstance(created, datetime):
            ts = -created.timestamp()
        else:
            try:
                v = float(created)
                ts = -(v / 1000.0 if v > 1e12 else v)
            except Exception:
                ts = 0.0
        alpha = _norm(s.get("name") or "")
        return (prio, display_order, ts, alpha)

    raw.sort(key=prio_tuple)
    return raw

# ---------- “data/minutes” parsing & labels ----------
_NUM = re.compile(r"^\s*-?\d+(\.\d+)?\s*$", re.IGNORECASE)
_GB  = re.compile(r"(\d+(?:\.\d+)?)[\s]*G(?:B|IG)?\b", re.IGNORECASE)
_MB  = re.compile(r"(\d+(?:\.\d+)?)[\s]*MB\b", re.IGNORECASE)
_MIN = re.compile(r"(\d+(?:\.\d+)?)[\s]*(?:MIN|MINS|MINUTE|MINUTES)\b", re.IGNORECASE)
_PKG_TAIL = re.compile(r"\s*\(Pkg\s*\d+\)\s*$", re.IGNORECASE)

def _service_unit(svc: Dict[str, Any]) -> str:
    unit = (svc.get("unit") or "").strip().lower()
    name = (svc.get("name") or "").strip().lower()
    if unit in ("min", "mins", "minute", "minutes") or name == "afa talktime":
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
                    data = ast.literal_eval(vt)
                    if isinstance(data, dict):
                        return data
                except Exception:
                    pass
        return vt
    return value

def _extract_volume(value: Any, unit: str) -> Optional[float]:
    if isinstance(value, dict):
        vol = value.get("volume") or value.get("offer") or value.get("gb")
        if vol is None:
            return None
        if isinstance(vol, (int, float)) or (_NUM.match(str(vol))):
            v = float(vol)
            return v if unit == "minutes" else (v if "MB" in str(vol).upper() else v * 1000.0 if "GB" in str(vol).upper() else v)
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

# ---------- pricing map builder ----------
def _build_pricing_map(pricing: Dict[str, Any]) -> Tuple[float, Dict[str, Dict[str, Any]]]:
    percent_default = float(pricing.get("percent_default") or 0.0)
    per_map: Dict[str, Dict[str, Any]] = {}
    for x in (pricing.get("per_service") or []):
        sid = str(x.get("service_id") or "")
        if not sid:
            continue
        entry: Dict[str, Any] = {"percent": None, "offers": {}}
        if x.get("percent") is not None:
            try:
                entry["percent"] = float(x.get("percent"))
            except Exception:
                entry["percent"] = None
        for o in (x.get("offers") or []):
            try:
                idx = int(o.get("index"))
                tot = _to_float(o.get("total"))
                if tot is not None:
                    entry["offers"][idx] = float(tot)
            except Exception:
                continue
        per_map[sid] = entry
    return percent_default, per_map

# ---------- apply pricing to a service (for page render) ----------
def _offer_value_text(o: Dict[str, Any]) -> str:
    vt = o.get("value_text")
    if isinstance(vt, str) and vt.strip():
        try:
            unit = "data"
            cleaned = _PKG_TAIL.sub("", vt).strip()
            vol = _extract_volume(cleaned, unit)
            if vol is not None:
                return _format_volume_unit(vol, unit)
        except Exception:
            pass
    lab = _value_text_for_display(o.get("value"), "data")
    return lab or "-"

def _apply_store_pricing_to_service(
    svc: Dict[str, Any], percent_default: float, per_service_map: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    s = dict(svc)
    offers = s.get("offers") or []
    svc_id_str = str(s.get("_id"))
    per_entry = per_service_map.get(svc_id_str, {})
    svc_percent: Optional[float] = per_entry.get("percent")
    offer_overrides: Dict[int, float] = per_entry.get("offers") or {}

    norm_offers: List[Dict[str, Any]] = []
    for idx, of in enumerate(offers):
        base_amount = _to_float(of.get("amount"))
        if idx in offer_overrides:
            total = round(float(offer_overrides[idx]), 2)
        else:
            pct = svc_percent if (svc_percent is not None) else percent_default
            total = round(base_amount + (base_amount * pct / 100.0), 2) if base_amount is not None else None
        vt = _offer_value_text(of)
        norm_offers.append(
            {
                "value_text": vt,
                "total": total,
                "amount": base_amount,
                "value": of.get("value"),
            }
        )
    s["offers"] = norm_offers
    return s

# ---------- DB loads for editor/view ----------
def _load_all_services_for_store_edit() -> List[Dict[str, Any]]:
    fields = {"_id": 1, "name": 1, "offers": 1}
    raw = list(services_col.find({}, fields))
    raw.sort(key=lambda x: _norm(x.get("name") or ""))
    clean: List[Dict[str, Any]] = []
    for r in raw:
        s: Dict[str, Any] = {"_id_str": str(r.get("_id")), "name": r.get("name") or ""}
        new_off: List[Dict[str, Any]] = []
        for o in (r.get("offers") or []):
            new_off.append(
                {"amount": _to_float(o.get("amount")), "value": o.get("value"), "value_text": _offer_value_text(o)}
            )
        s["offers"] = new_off
        clean.append(s)
    return clean

def _load_services_for_store_view(scope: str, ids: List[str]) -> List[Dict[str, Any]]:
    q: Dict[str, Any] = {}
    if scope == "selected" and ids:
        try:
            q = {"_id": {"$in": [ObjectId(x) for x in ids if x]}}
        except Exception:
            q = {"_id": {"$in": []}}
    fields = {
        "_id": 1, "name": 1, "type": 1, "status": 1, "availability": 1,
        "image_url": 1, "offers": 1, "service_category": 1,
        "priority": 1, "display_order": 1, "created_at": 1, "unit": 1,
        "default_profit_percent": 1, "network_id": 1, "network": 1
    }
    raw = list(services_col.find(q, fields)) if q else list(services_col.find({}, fields))
    raw = _sorted_services(raw)
    for s in raw:
        s["_id_str"] = str(s["_id"])
        s.update(_service_state(s))
    return raw

# ---------- JSON-safe converter ----------
def _store_to_client(s: Optional[dict]) -> dict:
    if not s:
        return {}
    out: Dict[str, Any] = {}
    for k, v in s.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, list):
            out[k] = [
                (str(x) if isinstance(x, ObjectId) else x.isoformat() if isinstance(x, datetime) else x) for x in v
            ]
        elif isinstance(v, dict):
            if k == "pricing":
                per = []
                for row in (v.get("per_service") or []):
                    row2 = dict(row)
                    if isinstance(row2.get("service_id"), ObjectId):
                        row2["service_id"] = str(row2["service_id"])
                    per.append(row2)
                out[k] = {**v, "per_service": per}
            else:
                out[k] = {
                    kk: (str(vv) if isinstance(vv, ObjectId) else vv.isoformat() if isinstance(vv, datetime) else vv)
                    for kk, vv in v.items()
                }
        else:
            out[k] = v
    if "service_ids" in out:
        out["service_ids"] = [str(x) for x in (out.get("service_ids") or [])]
    return out

# ---------- helper: find current user's store ----------
def _find_user_store(user_id: ObjectId, slug: Optional[str] = None) -> Optional[Dict[str, Any]]:
    q: Dict[str, Any] = {"owner_id": user_id, "status": {"$ne": "deleted"}}
    if slug:
        q["slug"] = slug
    return stores_col.find_one(q, sort=[("updated_at", -1), ("created_at", -1)])

# ---------- shared upsert ----------
def _upsert_store_from_payload(owner_id: ObjectId, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    name = (data.get("name") or "").strip()
    slug = _slugify(data.get("slug") or name)
    status = (data.get("status") or "published").strip()
    if not name or not slug:
        return False, {"message": "Name and slug are required"}

    existing = stores_col.find_one({"slug": slug, "status": {"$ne": "deleted"}})
    if existing and str(existing.get("owner_id")) != str(owner_id):
        return False, {"message": "Slug already taken"}

    doc = {
        "owner_id": owner_id,
        "name": name,
        "slug": slug,
        "logo_url": (data.get("logo_url") or "").strip(),  # can be /media/<id>
        "layout": (data.get("layout") or "grid-2").strip(),
        "theme": data.get("theme") or {},
        "hero": data.get("hero") or {},                    # may include bg_image: /media/<id>
        "service_scope": data.get("service_scope") or "all",
        "service_ids": data.get("service_ids") or [],
        "pricing": data.get("pricing") or {"mode": "percent", "percent_default": 0.0, "per_service": []},
        "status": status,
        "updated_at": datetime.utcnow(),
    }
    stores_col.update_one(
        {"slug": slug, "owner_id": owner_id},
        {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}},
        upsert=True,
    )
    return True, {"slug": slug, "status": status}

# ============================================================================ PAGES
@stores_bp.route("/create-store", methods=["GET"])
def create_store_page():
    if session.get("role") != "customer" or not session.get("user_id"):
        return redirect(url_for("login.login"))
    services_min = _load_all_services_for_store_edit()
    user_id = ObjectId(session["user_id"])
    slug = (request.args.get("slug") or "").strip() or None
    store_doc = _find_user_store(user_id, slug)
    return render_template("store_create.html", services=services_min, store=_store_to_client(store_doc))

@stores_bp.route("/s/<slug>", methods=["GET"])
def store_public_page(slug: str):
    store_doc = stores_col.find_one({"slug": slug, "status": "published"})
    if not store_doc:
        if request.args.get("preview") == "1" and session.get("user_id"):
            store_doc = stores_col.find_one({"slug": slug, "owner_id": ObjectId(session["user_id"])})
            if not store_doc:
                return "Store not found", 404
        else:
            return "Store not found", 404

    scope = store_doc.get("service_scope") or "all"
    service_ids = store_doc.get("service_ids") or []
    services = _load_services_for_store_view(scope, service_ids)

    percent_default, per_map = _build_pricing_map(store_doc.get("pricing") or {})
    priced = [_apply_store_pricing_to_service(s, percent_default, per_map) for s in services]

    # Pass Paystack PK for inline checkout on the store page (hardcoded above)
    return render_template("store_page.html", store=store_doc, services=priced, paystack_pk=PAYSTACK_PUBLIC_KEY)

# ============================================================================ API: media (GridFS)
@stores_bp.route("/api/media", methods=["POST"])
def api_upload_media():
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    f = (request.files or {}).get("file")
    if not f:
        return jsonify({"success": False, "message": "No file"}), 400
    data = f.read()
    if not data:
        return jsonify({"success": False, "message": "Empty file"}), 400
    fid = fs.put(
        data,
        filename=f.filename or "upload",
        content_type=f.mimetype or "application/octet-stream",
        uploaded_by=str(session.get("user_id")),
        created_at=datetime.utcnow(),
    )
    return jsonify({"success": True, "id": str(fid), "url": url_for("stores.get_media", file_id=str(fid))})

@stores_bp.route("/media/<file_id>", methods=["GET"])
def get_media(file_id: str):
    try:
        oid = ObjectId(file_id)
    except Exception:
        abort(404)
    try:
        gfile = fs.get(oid)
    except Exception:
        abort(404)
    return send_file(gfile, mimetype=(getattr(gfile, "content_type", None) or "application/octet-stream"),
                     as_attachment=False, download_name=getattr(gfile, "filename", None) or "file")

# ============================================================================ API
@stores_bp.route("/api/stores/mine", methods=["GET"])
def api_get_my_store():
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    user_id = ObjectId(session["user_id"])
    slug = (request.args.get("slug") or "").strip() or None
    store = _find_user_store(user_id, slug)
    return jsonify({"success": True, "store": _store_to_client(store) if store else None})

@stores_bp.route("/api/stores", methods=["POST"])
def api_upsert_store():
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    owner_id = ObjectId(session["user_id"])
    data = request.get_json(silent=True) or {}
    ok, payload = _upsert_store_from_payload(owner_id, data)
    if not ok:
        return jsonify({"success": False, **payload}), 400
    return jsonify({"success": True, **payload})

@stores_bp.route("/api/stores/preview", methods=["POST"])
def api_save_draft_for_preview():
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    owner_id = ObjectId(session["user_id"])
    data = request.get_json(silent=True) or {}
    data["status"] = "draft"
    ok, payload = _upsert_store_from_payload(owner_id, data)
    if not ok:
        return jsonify({"success": False, **payload}), 400
    return jsonify({"success": True, **payload})

@stores_bp.route("/api/stores/<slug>/status", methods=["POST"])
def api_update_status(slug: str):
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    body = request.get_json(silent=True) or {}
    status = (body.get("status") or "").strip()
    if status not in {"published", "suspended", "draft"}:
        return jsonify({"success": False, "message": "Invalid status"}), 400
    res = stores_col.update_one(
        {"slug": slug, "owner_id": ObjectId(session["user_id"]), "status": {"$ne": "deleted"}},
        {"$set": {"status": status, "updated_at": datetime.utcnow()}},
    )
    if res.matched_count == 0:
        return jsonify({"success": False, "message": "Store not found"}), 404
    return jsonify({"success": True, "slug": slug, "status": status})

@stores_bp.route("/api/stores/<slug>", methods=["DELETE"])
def api_delete_store(slug: str):
    if session.get("role") != "customer" or not session.get("user_id"):
        return jsonify({"success": False, "message": "Login required"}), 401
    res = stores_col.update_one(
        {"slug": slug, "owner_id": ObjectId(session["user_id"])},
        {"$set": {"status": "deleted", "updated_at": datetime.utcnow()}},
    )
    if res.matched_count == 0:
        return jsonify({"success": False, "message": "Store not found"}), 404
    return jsonify({"success": True})

# ============================================================================ PAYSTACK FLOW (no wallet deduction)

# (Old helpers kept for reference; not used by the new check)
def _nearest_whole(x: float) -> int:
    xf = float(x or 0.0)
    return int(xf + 0.5)  # .5 up

def _paystack_item_price_ghs(item: Dict[str, Any]) -> float:
    """Legacy rule: round(system total) + 1 per line (no longer used)."""
    amt = _money(item.get("amount"))
    return float(_nearest_whole(amt) + 1)

def _paystack_cart_total_ghs(cart: List[Dict[str, Any]]) -> float:
    return round(sum(_paystack_item_price_ghs(i) for i in cart), 2)

def _verify_paystack(reference: str) -> Tuple[bool, Dict[str, Any], str]:
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
    return int(paid_pesewas or 0) >= int(expected_pesewas or 0)

def _canonical_store_total_for_offer(store_doc: Dict[str, Any], svc_doc: Dict[str, Any], value_obj: Any, value_raw: Any) -> Optional[float]:
    """
    Recompute the store-facing 'system total' for a selected offer using store pricing
    (percent_default / per_service overrides). Match by volume/label similarly to page render.
    """
    if not svc_doc:
        return None
    percent_default, per_map = _build_pricing_map(store_doc.get("pricing") or {})
    svc_id_str = str(svc_doc.get("_id"))
    per_entry = per_map.get(svc_id_str, {})
    svc_percent = per_entry.get("percent")

    offers = svc_doc.get("offers") or []
    if not offers:
        return None

    unit = _service_unit(svc_doc)
    vol_needed = _extract_volume(value_obj if isinstance(value_obj, dict) else value_raw, unit)

    best_idx = None
    best_diff = float("inf")
    for idx, of in enumerate(offers):
        parsed = _parse_value_field(of.get("value"))
        vol = _extract_volume(parsed, unit)
        if vol_needed is not None and vol is not None:
            diff = abs(float(vol) - float(vol_needed))
            if diff < best_diff:
                best_idx, best_diff = idx, diff
        elif best_idx is None:
            best_idx = idx

    if best_idx is None:
        return None

    base_amount = _to_float(offers[best_idx].get("amount"))
    if base_amount is None:
        return None

    # Apply per-offer override if present
    offer_overrides = per_entry.get("offers") or {}
    if best_idx in offer_overrides:
        return round(float(offer_overrides[best_idx]), 2)

    pct = svc_percent if (svc_percent is not None) else percent_default
    return round(base_amount + (base_amount * float(pct) / 100.0), 2)

def _server_reprice_store_cart(store_doc: Dict[str, Any], cart: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], float]:
    revised = []
    sys_total = 0.0
    for item in cart:
        service_id_raw = item.get("serviceId")
        value_obj = _coerce_value_obj(item.get("value_obj") or item.get("value"))
        svc_doc = None
        if service_id_raw:
            try:
                svc_doc = services_col.find_one(
                    {"_id": ObjectId(service_id_raw)},
                    {"offers": 1, "unit": 1, "name": 1, "type": 1, "service_category": 1,
                     "default_profit_percent": 1, "status": 1, "availability": 1, "network_id": 1, "network": 1}
                )
            except Exception:
                svc_doc = None

        canonical = _canonical_store_total_for_offer(store_doc or {}, svc_doc or {}, value_obj, item.get("value"))
        if canonical is None:
            canonical = _money(item.get("amount"))  # last resort

        revised.append({**item, "amount": canonical})
        sys_total += canonical
    return revised, round(sys_total, 2)

# ============================================================================ CHECKOUT (Paystack — NO WALLET DEDUCTION)
@stores_bp.route("/store-checkout/<slug>", methods=["POST"])
def store_checkout_paystack(slug: str):
    """
    Store checkout via Paystack inline (no wallet deduction):
    - Reprice cart on server using store pricing (percent/overrides) to get system totals
    - Verify Paystack reference; accept paid >= server total (gateway fee allowed)
    - Place order; DO NOT deduct customer wallet; record a 'payment' transaction
    """
    try:
        body = request.get_json(silent=True) or {}
        cart = body.get("cart") or []
        method = (body.get("method") or "paystack_inline").strip().lower()
        ps_info = body.get("paystack") or {}
        ps_ref = (ps_info.get("reference") or "").strip()

        jlog("store_public_checkout_incoming", slug=slug, payload=body)

        store_doc = stores_col.find_one({"slug": slug, "status": {"$ne": "deleted"}})
        if not store_doc:
            return jsonify({"success": False, "message": "Store not found"}), 404

        if not cart or not isinstance(cart, list):
            return jsonify({"success": False, "message": "Cart is empty or invalid"}), 400

        # Idempotency: existing order for this reference?
        if ps_ref:
            prior = orders_col.find_one({"store_slug": slug, "paystack_reference": ps_ref})
            if prior:
                return jsonify({
                    "success": True,
                    "message": f"✅ Order already created. Order ID: {prior.get('order_id')}",
                    "order_id": prior.get("order_id"),
                    "status": prior.get("status"),
                    "charged_amount": prior.get("charged_amount"),
                    "profit_amount_total": prior.get("profit_amount_total", 0.0),
                    "items": prior.get("items", []),
                    "idempotent": True
                }), 200

        # Server-authoritative pricing (store pricing rules)
        cart, total_requested = _server_reprice_store_cart(store_doc, cart)
        if total_requested <= 0:
            return jsonify({"success": False, "message": "Total amount must be greater than zero"}), 400

        # Payment required + verify
        if method != "paystack_inline" or not ps_ref:
            return jsonify({"success": False, "message": "Payment missing. Please pay first."}), 400

        ok, verify_data, fail_reason = _verify_paystack(ps_ref)
        if not ok:
            return jsonify({"success": False, "message": f"Payment verification failed: {fail_reason}"}), 400

        paid_pes = int(verify_data.get("amount") or 0)
        paid_ghs = round(paid_pes / 100.0, 2)
        currency = (verify_data.get("currency") or "GHS").upper()
        if paid_pes <= 0 or currency != "GHS":
            return jsonify({"success": False, "message": "Invalid payment amount/currency."}), 400

        # NEW: Expected = exact server total (no +1/line). We allow any paid >= expected (covers Paystack 2% + rounding)
        expected_pay_ghs = round(total_requested, 2)
        expected_pay_pes = int(round(expected_pay_ghs * 100))

        if not _paid_enough(paid_pes, expected_pay_pes):
            jlog("store_public_checkout_amount_underpaid",
                 slug=slug, paid_pes=paid_pes, expected_pes=expected_pay_pes,
                 paid_ghs=paid_ghs, expected_ghs=expected_pay_ghs, cart=cart)
            return jsonify({
                "success": False,
                "message": "Payment amount is less than required. Please complete full payment.",
                "paid": paid_ghs,
                "required": expected_pay_ghs
            }), 400

        # Fee delta for logs/analytics (what user paid minus our system price)
        fee_delta_ghs = max(0.0, round(paid_ghs - expected_pay_ghs, 2))

        # Record the Paystack payment transaction (income) — no wallet spend here
        if not transactions_col.find_one({"reference": ps_ref, "type": "payment", "status": "success"}):
            transactions_col.insert_one({
                "user_id": None,  # optional; could attribute to store owner later if needed
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
                    "store_checkout": True,
                    "store_slug": slug,
                    "expected_pay_total_ghs": expected_pay_ghs,
                    "paid_total_ghs": paid_ghs,
                    "gateway_fee_overage_ghs": fee_delta_ghs,
                    "note": "Customer payment captured via store inline checkout (paid may include gateway fees)."
                }
            })

        # Build order lines and attempt API sends (when available)
        order_id = generate_order_id()
        results: List[Dict[str, Any]] = []
        debug_events: List[Dict[str, Any]] = []
        profit_amount_total = 0.0
        total_processing_amount = 0.0

        for idx, item in enumerate(cart, start=1):
            phone = (item.get("phone") or "").strip()
            amt_total = _money(item.get("amount"))
            service_id_raw = item.get("serviceId")
            svc_doc: Optional[Dict[str, Any]] = None
            svc_type: Optional[str] = None
            svc_name = item.get("serviceName") or None
            service_category: Optional[str] = None

            if service_id_raw:
                try:
                    svc_doc = services_col.find_one(
                        {"_id": ObjectId(service_id_raw)},
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

            # Allow OFF/MANUAL to pass availability if OPEN/AVAILABLE (queue as processing if not API)
            svc_check = dict(svc_doc) if svc_doc else None
            if svc_check:
                t = (svc_check.get("type") or "").upper()
                status = (svc_check.get("status") or "OPEN").upper()
                avail = (svc_check.get("availability") or "AVAILABLE").upper()
                if t in {"OFF", "MANUAL"} and status == "OPEN" and avail == "AVAILABLE":
                    svc_check["type"] = "API"

            is_unavail, reason_text = _service_unavailability_reason(svc_check)
            if is_unavail:
                return jsonify({
                    "success": False,
                    "message": reason_text,
                    "unavailable": {"serviceId": service_id_raw, "serviceName": svc_name, "reason": reason_text}
                }), 400

            # Profit allocation
            base_hint = _to_float(item.get("base_amount"))
            value_obj = _coerce_value_obj(item.get("value_obj") or item.get("value"))
            if base_hint is None and svc_doc:
                base_hint = _pick_offer_base_amount_from_service(svc_doc, value_obj, item.get("value"))

            eff_p = _effective_profit_percent(svc_doc, None) if svc_doc else 0.0
            base_amount, profit_amount = _derive_base_profit(amt_total, base_hint, eff_p)
            profit_amount_total += profit_amount

            # API path if possible
            is_express = (service_category == "express services")
            network_id = _resolve_network_id(item, value_obj, svc_doc) if svc_doc else None
            shared_bundle = None
            if svc_doc:
                shared_bundle = _resolve_shared_bundle_express(item, value_obj) if is_express else _resolve_shared_bundle_toppily(item, value_obj)

            api_status = "not_applicable"
            api_tag = None
            trx_ref = None
            api_payload: Dict[str, Any] = {}

            total_processing_amount += amt_total

            if svc_doc and phone and network_id is not None and shared_bundle is not None:
                trx_ref = f"{order_id}_{idx}"
                if is_express:
                    ok2, api_payload = _send_express_single(phone, network_id, int(shared_bundle), trx_ref, order_id, debug_events)
                    api_tag = "express"
                else:
                    ok2, api_payload = _send_toppily_shared_bundle(phone, network_id, int(shared_bundle), trx_ref, order_id, debug_events)
                    api_tag = "toppily"
                api_status = "success" if ok2 else "processing"
            else:
                api_status = "processing"
                api_payload = {
                    "note": "API fields missing; queued for processing",
                    "got": {"phone": bool(phone), "network_id": network_id, "shared_bundle": shared_bundle}
                }

            results.append({
                "phone": phone,
                "amount": amt_total,
                "base_amount": base_amount,
                "profit_amount": profit_amount,
                "profit_percent_used": eff_p,
                "value": item.get("value"),
                "value_obj": value_obj,
                "serviceId": service_id_raw,
                "serviceName": svc_name,
                "service_type": svc_type,
                "provider": api_tag,
                "trx_ref": trx_ref,
                "line_status": "processing",
                "api_status": api_status,
                "api_response": api_payload
            })

        # Insert order — NO WALLET CHARGE
        orders_col.insert_one({
            "user_id": (ObjectId(session["user_id"]) if session.get("user_id") else None),
            "store_slug": slug,
            "order_id": order_id,
            "items": results,
            "total_amount": round(total_requested, 2),          # system price total (store-priced)
            "charged_amount": round(total_processing_amount, 2),
            "profit_amount_total": round(profit_amount_total, 2),
            "status": "processing",
            "paid_from": "paystack_inline",
            "paystack_reference": ps_ref,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "debug": {
                "store_checkout": True,
                "events": debug_events[-10:],
                "paystack_paid_ghs": paid_ghs,
                "paystack_expected_ghs": expected_pay_ghs,
                "gateway_fee_overage_ghs": fee_delta_ghs
            }
        })

        return jsonify({
            "success": True,
            "message": f"✅ Order received and is processing. Order ID: {order_id}",
            "order_id": order_id,
            "status": "processing",
            "charged_amount": round(total_processing_amount, 2),
            "profit_amount_total": round(profit_amount_total, 2),
            "items": results,
            "paid_ghs": paid_ghs,
            "expected_ghs": expected_pay_ghs
        }), 200

    except Exception:
        jlog("store_public_checkout_uncaught", slug=slug, error=traceback.format_exc())
        return jsonify({"success": False, "message": "Server error"}), 500
