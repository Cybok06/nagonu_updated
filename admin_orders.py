# admin_orders.py  — Admin Orders + DB-Backed Scheduler (Render-safe) + Bulk Deliver (Selected)
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify, make_response
from bson import ObjectId, Regex
from db import db, campus_db
from datetime import datetime, timedelta
import json
import os
import time
import threading
import hashlib
from urllib.parse import urlencode
import uuid
from typing import List, Tuple
from collections import OrderedDict
from order_status import _compute_order_status_from_items, _normalize_status

admin_orders_bp = Blueprint("admin_orders", __name__)

orders_col        = db["orders"]
campus_orders_col = campus_db["orders"]
users_col         = db["users"]
balances_col      = db["balances"]         # for refunds
transactions_col  = db["transactions"]     # for refund ledger
schedules_col     = db["order_schedules"]  # NEW: persistent job queue

# Keep legacy; primary set includes refunded
ALLOWED_STATUSES   = {"pending", "processing", "delivered", "failed", "completed", "refunded"}
ALLOWED_SORTS      = {"newest", "oldest", "amount_desc", "amount_asc"}
DEFAULT_PER_PAGE   = 10
FINAL_STATUS       = "completed"
API_PROVIDERS = {"codecraft", "dataconnect", "portal02"}
ALLOWED_TRANSITIONS = {
    "pending": {"processing"},
    "processing": {"delivered", "failed", "refunded"},
    "delivered": {"completed"},
    "failed": set(),
    "refunded": set(),
    "completed": set(),
}

# --------- CACHING (Render-safe) ----------
_ORDERS_CACHE_TTL_SECONDS = 45
_ORDERS_CACHE_MAX_ITEMS = 512

class _MemoryTTLCache:
    def __init__(self, max_items: int = _ORDERS_CACHE_MAX_ITEMS):
        self.max_items = max_items
        self._lock = threading.Lock()
        self._store = OrderedDict()  # key -> (expires_at, value)

    def get(self, key: str):
        now = time.time()
        with self._lock:
            if key not in self._store:
                return None
            exp, val = self._store[key]
            if exp < now:
                del self._store[key]
                return None
            self._store.move_to_end(key)
            return val

    def set(self, key: str, value, ttl: int):
        exp = time.time() + max(1, int(ttl))
        with self._lock:
            self._store[key] = (exp, value)
            self._store.move_to_end(key)
            while len(self._store) > self.max_items:
                self._store.popitem(last=False)

_memory_cache = _MemoryTTLCache()
_redis_client = None

def _get_redis_client():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    redis_url = os.getenv("REDIS_URL") or ""
    if not redis_url:
        _redis_client = False
        return _redis_client
    try:
        import redis  # type: ignore
        _redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception:
        _redis_client = False
    return _redis_client

def get_cached_json(key: str):
    client = _get_redis_client()
    if client:
        try:
            raw = client.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None
    return _memory_cache.get(key)

def set_cached_json(key: str, value, ttl: int = _ORDERS_CACHE_TTL_SECONDS):
    client = _get_redis_client()
    if client:
        try:
            client.setex(key, int(ttl), json.dumps(value, separators=(",", ":"), ensure_ascii=False))
            return
        except Exception:
            pass
    _memory_cache.set(key, value, ttl)

def _jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")

def _can_transition(old_status: str, new_status: str) -> bool:
    if old_status == new_status:
        return True
    return new_status in ALLOWED_TRANSITIONS.get(old_status, set())

def _log_status_blocked(order, attempted_status: str, reason: str, source: str, actor_admin_id=None):
    _jlog(
        "order_status_blocked",
        order_id=order.get("order_id"),
        mongo_id=str(order.get("_id")),
        attempted_status=attempted_status,
        current_status=(order.get("status") or ""),
        reason=reason,
        source=source,
        actor_admin_id=actor_admin_id,
    )

# --------- HELPERS ----------
def _parse_date(dstr):
    if not dstr:
        return None
    try:
        s = dstr.strip()
        if len(s) <= 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except Exception:
        return None

def _build_preserved_query(args, exclude=("page",)):
    kept = {k: v for k, v in args.items() if k not in exclude and v not in (None, "", "None")}
    return urlencode(kept)

def _append_and(query: dict, clause: dict) -> dict:
    query["$and"] = (query.get("$and") or []) + [clause]
    return query

def _build_query_from_params(args):
    """Central builder so list + bulk share identical filters."""
    status_filter = (args.get("status") or "").strip().lower()
    order_id_q    = (args.get("order_id") or "").strip()
    customer_q    = (args.get("customer") or "").strip()
    paid_from     = (args.get("paid_from") or "").strip().lower()
    min_total     = (args.get("min_total") or "").strip()
    max_total     = (args.get("max_total") or "").strip()
    date_from     = _parse_date((args.get("date_from") or "").strip())
    date_to_raw   = _parse_date((args.get("date_to") or "").strip())
    date_to       = datetime(date_to_raw.year, date_to_raw.month, date_to_raw.day) + timedelta(days=1) if date_to_raw else None

    item_service  = (args.get("item_service") or "").strip()
    item_offer    = (args.get("item_offer") or "").strip()
    item_phone    = (args.get("item_phone") or "").strip()

    query = {}

    if status_filter and status_filter in ALLOWED_STATUSES:
        query["status"] = status_filter
    if paid_from:
        if paid_from == "paystack":
            query["paid_from"] = {"$in": ["paystack", "paystack_inline"]}
        else:
            query["paid_from"] = paid_from
    if order_id_q:
        query["order_id"] = Regex(order_id_q, "i")

    if date_from or date_to:
        dt = {}
        if date_from: dt["$gte"] = date_from
        if date_to:   dt["$lt"]  = date_to
        query["created_at"] = dt

    amt = {}
    try:
        if min_total != "": amt["$gte"] = float(min_total)
    except Exception:
        pass
    try:
        if max_total != "": amt["$lte"] = float(max_total)
    except Exception:
        pass
    if amt:
        query["total_amount"] = amt

    if customer_q:
        rx = Regex(customer_q, "i")
        user_ids = [u["_id"] for u in users_col.find(
            {"$or": [
                {"first_name": rx}, {"last_name": rx}, {"email": rx},
                {"phone": rx}, {"username": rx},
            ]},
            {"_id": 1},
        )]
        query["user_id"] = {"$in": user_ids or []}

    item_and = []
    if item_service: item_and.append({"items.serviceName": Regex(item_service, "i")})
    if item_offer:   item_and.append({"items.value": Regex(item_offer, "i")})
    if item_phone:   item_and.append({"items.phone": Regex(item_phone, "i")})
    if item_and:
        query["$and"] = (query.get("$and") or []) + item_and

    return query

def _apply_api_filter(query: dict, api_filter: str) -> None:
    if api_filter in {"passed", "not_passed"}:
        api_elem = {
            "items": {
                "$elemMatch": {
                    "provider": {"$in": ["codecraft", "dataconnect", "portal02"]},
                    "api_status": {"$ne": "skipped"},
                    "line_status": {"$nin": ["skipped_duplicate_processing", "skipped_duplicate_in_cart"]},
                }
            }
        }
        if api_filter == "passed":
            _append_and(query, api_elem)
        else:
            _append_and(query, {"$nor": [api_elem]})

def _format_dt(dt: datetime | None) -> str | None:
    if not dt:
        return None
    return dt.strftime("%Y-%m-%d %H:%M")

def _serialize_user(user: dict) -> dict:
    return {
        "first_name": user.get("first_name") or "",
        "last_name": user.get("last_name") or "",
        "email": user.get("email") or "",
        "phone": user.get("phone") or "",
        "username": user.get("username") or "",
    }

def _serialize_item(item: dict) -> dict:
    return {
        "serviceName": item.get("serviceName") or "",
        "value": item.get("value") or "",
        "phone": item.get("phone") or "",
        "amount": item.get("amount") or 0,
        "provider": item.get("provider") or "",
        "api_status": item.get("api_status") or "",
        "line_status": item.get("line_status") or "",
    }

def _serialize_order(order: dict) -> dict:
    created_text = _format_dt(order.get("created_at"))
    return {
        "order_id": order.get("order_id"),
        "order_id_param": order.get("order_id_param"),
        "order_id_key": str(order.get("_id")) if order.get("_id") is not None else "",
        "source": order.get("source") or "main",
        "user": _serialize_user(order.get("user") or {}),
        "items": [_serialize_item(i) for i in (order.get("items") or [])],
        "total_amount": order.get("total_amount") or 0,
        "status": order.get("status") or "",
        "paid_from": order.get("paid_from") or "",
        "api_passed": bool(order.get("api_passed")),
        "api_providers": order.get("api_providers") or [],
        "created_at_display": created_text,
        "created_at_text": created_text,
        "created_at_iso": order.get("created_at").isoformat() if order.get("created_at") else None,
    }

def _serialize_line(line: dict) -> dict:
    created_text = _format_dt(line.get("created_at"))
    return {
        "order_id": line.get("order_id"),
        "order_mongo_id_param": line.get("order_mongo_id_param"),
        "item_index": line.get("item_index"),
        "line_id": line.get("line_id"),
        "source": line.get("source") or "main",
        "user": _serialize_user(line.get("user") or {}),
        "item": _serialize_item(line.get("item") or {}),
        "created_at_display": created_text,
        "created_at_text": created_text,
        "created_at_iso": line.get("created_at").isoformat() if line.get("created_at") else None,
    }

def _load_users_for_orders(orders: List[dict]) -> dict:
    ids = []
    for o in orders:
        uid = o.get("user_id")
        if isinstance(uid, str):
            try:
                uid = ObjectId(uid)
            except Exception:
                uid = None
        if uid:
            ids.append(uid)
    if not ids:
        return {}
    unique_ids = list({i for i in ids})
    users = users_col.find(
        {"_id": {"$in": unique_ids}},
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1, "username": 1},
    )
    return {u["_id"]: u for u in users}

def _build_orders_cache_key(args) -> str:
    keys = [
        "source", "view", "page", "per_page", "sort", "status", "customer", "order_id",
        "paid_from", "min_total", "max_total", "date_from", "date_to",
        "item_service", "item_offer", "item_phone", "api_filter",
    ]
    normalized = {}
    for k in keys:
        normalized[k] = (args.get(k) or "").strip()
    normalized["source"] = _normalize_source_filter(normalized.get("source"))
    normalized["view"] = (normalized.get("view") or "lines").strip().lower()
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return f"admin_orders:data:{payload}"

def _etag_for_payload(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"\"{digest}\""

def _build_orders_data_payload(args) -> dict:
    sort = (args.get("sort") or "newest").strip().lower()
    if sort not in ALLOWED_SORTS:
        sort = "newest"

    try:
        per_page = int(args.get("per_page", DEFAULT_PER_PAGE))
        per_page = max(1, min(per_page, 100))
    except Exception:
        per_page = DEFAULT_PER_PAGE

    try:
        page = int(args.get("page", 1))
        page = max(1, page)
    except Exception:
        page = 1

    skip = (page - 1) * per_page
    query = _build_query_from_params(args)
    api_filter = (args.get("api_filter") or "").strip().lower()
    _apply_api_filter(query, api_filter)

    sort_spec = [("created_at", -1)]
    if sort == "oldest":
        sort_spec = [("created_at", 1)]
    elif sort == "amount_desc":
        sort_spec = [("total_amount", -1), ("created_at", -1)]
    elif sort == "amount_asc":
        sort_spec = [("total_amount", 1), ("created_at", -1)]

    source_filter = _normalize_source_filter(args.get("source"))
    view_mode = (args.get("view") or "lines").strip().lower()
    if view_mode not in {"lines", "orders"}:
        view_mode = "lines"

    projection = {
        "order_id": 1,
        "user_id": 1,
        "items": 1,
        "paid_from": 1,
        "created_at": 1,
        "status": 1,
        "total_amount": 1,
        "profit_amount": 1,
        "charged_amount": 1,
        "delivered_at": 1,
        "refunded_at": 1,
    }

    orders = []
    order_lines = []
    total_orders = 0
    total_pages = 1

    if source_filter == "main":
        total_orders = orders_col.count_documents(query)
        total_pages = max(1, (total_orders + per_page - 1) // per_page)
        orders = list(orders_col.find(query, projection).sort(sort_spec).skip(skip).limit(per_page))
        user_map = _load_users_for_orders(orders)
        for o in orders:
            _prepare_order(o, "main", order_lines, orders_col, user_map=user_map)
    elif source_filter == "campus":
        total_orders = campus_orders_col.count_documents(query)
        total_pages = max(1, (total_orders + per_page - 1) // per_page)
        orders = list(campus_orders_col.find(query, projection).sort(sort_spec).skip(skip).limit(per_page))
        user_map = _load_users_for_orders(orders)
        for o in orders:
            _prepare_order(o, "campus", order_lines, campus_orders_col, user_map=user_map)
    else:
        count_main = orders_col.count_documents(query)
        count_campus = campus_orders_col.count_documents(query)
        total_orders = count_main + count_campus
        total_pages = max(1, (total_orders + per_page - 1) // per_page)

        fetch_limit = skip + per_page
        main_batch = list(orders_col.find(query, projection).sort(sort_spec).limit(fetch_limit))
        campus_batch = list(campus_orders_col.find(query, projection).sort(sort_spec).limit(fetch_limit))

        for o in main_batch:
            _decorate_order_source(o, "main")
        for o in campus_batch:
            _decorate_order_source(o, "campus")

        merged = sorted(main_batch + campus_batch, key=lambda x: _sort_key_for_spec(x, sort_spec))
        page_orders = merged[skip:skip + per_page]
        user_map = _load_users_for_orders(page_orders)
        for o in page_orders:
            src = _normalize_source(o.get("source"), "main")
            col = _get_orders_collection(src)
            _prepare_order(o, src, order_lines, col, user_map=user_map)
        orders = page_orders

    payload = {
        "ok": True,
        "view_mode": view_mode,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total_orders": total_orders,
        "order_lines_count": len(order_lines) if view_mode != "orders" else 0,
    }
    if view_mode == "orders":
        payload["orders"] = [_serialize_order(o) for o in orders]
        payload["order_lines"] = []
    else:
        payload["order_lines"] = [_serialize_line(l) for l in order_lines]
        payload["orders"] = []
    return payload

def _json_with_cache(payload: dict, etag: str | None):
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "private, max-age=30"
    if etag:
        resp.headers["ETag"] = etag
    return resp

def _require_admin():
    return session.get("role") == "admin"

def _money(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def _compute_api_fields(order: dict) -> None:
    items = order.get("items") or []
    providers = set()
    for item in items:
        prov = (item.get("provider") or "").strip().lower()
        if prov not in API_PROVIDERS:
            continue
        if (item.get("api_status") or "").strip().lower() == "skipped":
            continue
        line_status = (item.get("line_status") or "").strip().lower()
        if line_status in ("skipped_duplicate_processing", "skipped_duplicate_in_cart"):
            continue
        providers.add(prov)

    order["api_passed"] = bool(providers)
    order["api_providers"] = sorted(providers)
    labels = []
    for p in order["api_providers"]:
        if p == "codecraft":
            labels.append("CodeCraft")
        elif p == "dataconnect":
            labels.append("DataConnect")
        elif p == "portal02":
            labels.append("Portal-02")
    order["api_providers_label"] = ", ".join(labels)

def _normalize_line_status(s: str | None) -> str:
    try:
        return _normalize_status(s)
    except Exception:
        return (s or "").strip().lower()

def _normalize_source(src: str | None, default: str = "main") -> str:
    v = (src or "").strip().lower()
    if v in {"main", "campus"}:
        return v
    return default

def _normalize_source_filter(src: str | None) -> str:
    v = (src or "").strip().lower()
    if v in {"all", "main", "campus"}:
        return v
    return "all"

def _split_source_prefix(raw: str) -> Tuple[str, str]:
    raw = (raw or "").strip()
    if ":" in raw:
        left, right = raw.split(":", 1)
        if left in {"main", "campus"}:
            return left, right
    return "main", raw

def _parse_order_id_param(raw: str):
    source, oid_str = _split_source_prefix(raw)
    try:
        return source, ObjectId(oid_str.strip())
    except Exception:
        return None

def _get_orders_collection(source: str):
    return campus_orders_col if source == "campus" else orders_col

def _is_final_line_status(s: str | None) -> bool:
    return _normalize_line_status(s) == "delivered"

def _parse_line_id(line_id: str):
    if not line_id:
        return None
    source, rest = _split_source_prefix(line_id)
    if ":" not in rest:
        return None
    left, right = rest.split(":", 1)
    try:
        oid = ObjectId(left.strip())
        idx = int(right.strip())
        if idx < 0:
            return None
        return source, oid, idx
    except Exception:
        return None

def _decorate_order_source(order: dict, source: str) -> None:
    order["source"] = source
    order["source_label"] = "Campus Data" if source == "campus" else "Main"
    order["order_id_param"] = f"{source}:{order.get('_id')}" if source == "campus" else str(order.get("_id"))

def _sort_value(v):
    if isinstance(v, datetime):
        return v.timestamp()
    try:
        return float(v)
    except Exception:
        return 0.0

def _sort_key_for_spec(order: dict, sort_spec: List[Tuple[str, int]]):
    key = []
    for field, direction in sort_spec:
        v = order.get(field)
        if field == "created_at" and not isinstance(v, datetime):
            v = None
        val = _sort_value(v)
        key.append(-val if direction < 0 else val)
    return tuple(key)

def _prepare_order(order: dict, source: str, order_lines: List[dict], orders_collection, user_map: dict | None = None):
    _decorate_order_source(order, source)
    uid = order.get("user_id")
    if isinstance(uid, str):
        try:
            uid = ObjectId(uid)
        except Exception:
            uid = None
    if user_map is not None:
        order["user"] = user_map.get(uid) or {}
    else:
        order["user"] = users_col.find_one({"_id": uid}) if uid else {}

    items = order.get("items") or []
    now = datetime.utcnow()
    changed_indexes = []
    for idx, item in enumerate(items):
        updates = _extract_duplicate_delivered_updates(item)
        if updates:
            item.update(updates)
            changed_indexes.append((idx, updates))

    if changed_indexes:
        for idx, updates in changed_indexes:
            set_doc = {f"items.{idx}.{k}": v for k, v in updates.items()}
            set_doc["updated_at"] = now
            orders_collection.update_one({"_id": order["_id"]}, {"$set": set_doc})

        current_status = (order.get("status") or "").lower()
        if current_status != "completed":
            new_status = _compute_order_status_from_items(items, current_status=current_status)
            if new_status and new_status != current_status:
                set_doc = {"status": new_status, "updated_at": now}
                if new_status == "delivered" and not order.get("delivered_at"):
                    set_doc["delivered_at"] = now
                orders_collection.update_one({"_id": order["_id"]}, {"$set": set_doc})
                order["status"] = new_status

    _compute_api_fields(order)
    for idx, item in enumerate(items):
        line_id = f"{source}:{order.get('_id')}:{idx}" if source == "campus" else f"{order.get('_id')}:{idx}"
        order_lines.append({
            "order_mongo_id": str(order.get("_id")),
            "order_mongo_id_param": order.get("order_id_param"),
            "order_id": order.get("order_id"),
            "user": order.get("user") or {},
            "paid_from": order.get("paid_from"),
            "created_at": order.get("created_at"),
            "status": order.get("status"),
            "order_total_amount": order.get("total_amount"),
            "profit_amount_total": order.get("profit_amount"),
            "item_index": idx,
            "item": item,
            "line_id": line_id,
            "source": source,
        })

def _apply_line_status_change(line_ids: List[str], new_status: str, api_status: str | None = None, reason: str = "manual", actor_admin_id=None, orders_collection=orders_col, target_source: str | None = None) -> Tuple[int, List[str]]:
    updated_lines = 0
    errors = []
    now = datetime.utcnow()
    if new_status not in {"pending", "processing", "delivered", "failed", "refunded"}:
        return 0, [f"invalid line status: {new_status}"]

    grouped = {}
    for lid in line_ids:
        parsed = _parse_line_id(lid)
        if not parsed:
            errors.append(f"{lid}: invalid line id")
            continue
        source, oid, idx = parsed
        if target_source and source != target_source:
            errors.append(f"{lid}: source mismatch")
            continue
        grouped.setdefault(oid, set()).add(idx)

    for oid, idxs in grouped.items():
        try:
            order = orders_collection.find_one({"_id": oid})
            if not order:
                errors.append(f"{oid}: not found")
                continue

            items = order.get("items") or []
            set_doc = {"updated_at": now}
            any_changed = False

            for idx in sorted(idxs):
                if idx < 0 or idx >= len(items):
                    errors.append(f"{oid}:{idx}: item not found")
                    continue
                item = items[idx]
                current_line = _normalize_line_status(item.get("line_status"))
                if _is_final_line_status(current_line) and _normalize_line_status(new_status) != "delivered":
                    errors.append(f"{oid}:{idx}: line is delivered and cannot be changed")
                    continue

                item["line_status"] = new_status
                set_doc[f"items.{idx}.line_status"] = new_status
                if api_status:
                    item["api_status"] = api_status
                    set_doc[f"items.{idx}.api_status"] = api_status
                set_doc[f"items.{idx}.provider_status_checked_at"] = now
                any_changed = True
                updated_lines += 1

            if not any_changed:
                continue

            current_status = (order.get("status") or "").lower()
            if current_status != "completed":
                new_order_status = _compute_order_status_from_items(items, current_status=current_status)
                if new_order_status and new_order_status != current_status:
                    set_doc["status"] = new_order_status
                    if new_order_status == "delivered" and not order.get("delivered_at"):
                        set_doc["delivered_at"] = now

            orders_collection.update_one({"_id": oid}, {"$set": set_doc})
        except Exception as e:
            errors.append(f"{oid}: {e}")

    return updated_lines, errors

def _extract_duplicate_delivered_updates(item: dict) -> dict | None:
    api_resp = item.get("api_response")
    if not isinstance(api_resp, dict):
        return None
    if api_resp.get("http_status") != 409:
        return None
    dup = api_resp.get("duplicate_order") or {}
    dup_status = (dup.get("status") or "").strip().upper()
    if dup_status != "DELIVERED":
        return None

    updates = {}
    if _normalize_line_status(item.get("line_status")) != "delivered":
        updates["line_status"] = "delivered"
    if (item.get("api_status") or "").strip().lower() not in ("success", "duplicate_delivered"):
        updates["api_status"] = "success"
    if not item.get("provider_reference"):
        ref = dup.get("transaction_code") or dup.get("provider_reference") or dup.get("reference") or dup.get("id")
        if ref:
            updates["provider_reference"] = ref
    return updates or None

# ---------- CORE: apply status change (used by manual, bulk, scheduled) ----------
def _apply_status_change(order_ids: List[ObjectId], new_status: str, reason: str = "manual", actor_admin_id=None, orders_collection=orders_col, source: str = "main") -> Tuple[int, List[str]]:
    """
    Idempotent per-order updates, including wallet credit for refunds.
    Returns (updated_count, errors)
    """
    updated = 0
    errors  = []

    now = datetime.utcnow()
    for oid in order_ids:
        try:
            order = orders_collection.find_one({"_id": oid})
            if not order:
                errors.append(f"{oid}: not found")
                continue

            old_status = (order.get("status") or "").lower()
            if old_status == FINAL_STATUS and new_status != FINAL_STATUS:
                _log_status_blocked(order, new_status, "final_status", reason, source, actor_admin_id)
                errors.append(f"{oid}: order is completed and cannot be changed")
                continue
            if not _can_transition(old_status, new_status):
                _log_status_blocked(order, new_status, "invalid_transition", reason, source, actor_admin_id)
                errors.append(f"{oid}: invalid transition {old_status} -> {new_status}")
                continue
            update_doc = {"status": new_status, "updated_at": now}
            # Delivered → set delivered_at if missing
            if new_status == "delivered" and not order.get("delivered_at"):
                update_doc["delivered_at"] = now

            # Refunded → single wallet credit based on charged_amount
            if new_status == "refunded":
                charged_amount = _money(order.get("charged_amount"), 0.0)
                user_id = order.get("user_id")
                already_refunded = bool(order.get("refunded_at")) or (old_status == "refunded")

                if charged_amount > 0 and user_id and not already_refunded:
                    try:
                        balances_col.update_one(
                            {"user_id": user_id},
                            {"$inc": {"amount": charged_amount}, "$set": {"updated_at": now}},
                            upsert=True
                        )
                        transactions_col.insert_one({
                            "user_id": user_id,
                            "amount": charged_amount,
                            "reference": order.get("order_id"),
                            "status": "success",
                            "type": "refund",
                            "gateway": "Wallet",
                            "currency": "GHS",
                            "created_at": now,
                            "verified_at": now,
                            "meta": {
                                "note": f"{reason.capitalize()} refund",
                                "order_db_id": oid,
                                "actor_admin_id": actor_admin_id,
                            }
                        })
                    except Exception as e:
                        errors.append(f"{oid}: refund ledger err: {e}")
                update_doc["refunded_at"] = now

            update_filter = {"_id": oid}
            if new_status != FINAL_STATUS:
                update_filter["status"] = {"$ne": FINAL_STATUS}
            res = orders_collection.update_one(update_filter, {"$set": update_doc})
            if res.modified_count:
                # Flip line_status in items from processing -> delivered when marking delivered
                if new_status == "delivered":
                    try:
                        orders_collection.update_one(
                            {"_id": oid, "status": {"$ne": FINAL_STATUS}},
                            {"$set": {"items.$[it].line_status": "delivered"}},
                            array_filters=[{"it.line_status": "processing"}]
                        )
                    except Exception:
                        pass
                updated += 1
            else:
                if new_status != FINAL_STATUS:
                    _log_status_blocked(order, new_status, "db_guard", reason, source, actor_admin_id)

        except Exception as e:
            errors.append(f"{oid}: {e}")

    return updated, errors

# ---------- DB-backed scheduler utilities ----------
def _enqueue_status_job(order_id_strs: List[str], new_status: str, run_time: datetime, admin_id: str | None, note: str | None, line_ids: List[str] | None = None):
    """
    Persist a job document that can be executed later (Render-safe).
    """
    now = datetime.utcnow()
    doc = {
        "job_key": str(uuid.uuid4()),
        "order_ids": order_id_strs,     # strings
        "line_ids": line_ids or [],     # strings "orderId:itemIndex"
        "status": new_status,
        "note": note or "",
        "admin_id": admin_id,
        "state": "scheduled",           # scheduled | running | done | error | cancelled
        "attempts": 0,
        "max_attempts": 3,
        "created_at": now,
        "run_at": run_time,             # UTC datetime
        "started_at": None,
        "finished_at": None,
        "result": None,                 # {updated, errors:[], ...}
        "lock_token": None,             # for cooperative locking
        "locked_at": None
    }
    schedules_col.insert_one(doc)
    return doc

def _process_due_jobs(max_batch: int = 25):
    """
    Cooperatively process due jobs. Safe to call at the top of admin routes
    and/or from a Render Cron ping.
    """
    now = datetime.utcnow()
    # pick up to max_batch jobs that are due and not locked/running/cancelled
    cursor = schedules_col.find({
        "state": {"$in": ["scheduled", "error"]},
        "run_at": {"$lte": now},
        "$or": [{"lock_token": None}, {"locked_at": {"$lt": now - timedelta(minutes=5)}}]
    }).sort([("run_at", 1)]).limit(max_batch)

    for job in cursor:
        lock_token = str(uuid.uuid4())
        # try to acquire lock
        claimed = schedules_col.update_one(
            {"_id": job["_id"], "lock_token": job.get("lock_token")},
            {"$set": {"lock_token": lock_token, "locked_at": now, "state": "running", "started_at": now}}
        )
        if not claimed.modified_count:
            continue

        # Execute
        try:
            updated = 0
            errors = []

            line_ids = [s for s in (job.get("line_ids") or []) if s]
            if line_ids:
                by_source = {"main": [], "campus": []}
                for lid in line_ids:
                    parsed = _parse_line_id(lid)
                    if not parsed:
                        errors.append(f"{lid}: invalid line id")
                        continue
                    source, _, _ = parsed
                    by_source[source].append(lid)
                for source, ids in by_source.items():
                    if not ids:
                        continue
                    line_updated, line_errors = _apply_line_status_change(
                        ids,
                        job.get("status"),
                        reason="scheduled",
                        actor_admin_id=job.get("admin_id"),
                        orders_collection=_get_orders_collection(source),
                        target_source=source,
                    )
                    updated += line_updated
                    errors += line_errors

            by_source = {"main": [], "campus": []}
            for s in (job.get("order_ids") or []):
                parsed = _parse_order_id_param(s)
                if not parsed:
                    continue
                source, oid = parsed
                by_source[source].append(oid)
            for source, ids in by_source.items():
                if not ids:
                    continue
                order_updated, order_errors = _apply_status_change(
                    ids,
                    job.get("status"),
                    reason="scheduled",
                    actor_admin_id=job.get("admin_id"),
                    orders_collection=_get_orders_collection(source),
                    source=source,
                )
                updated += order_updated
                errors += order_errors
            schedules_col.update_one(
                {"_id": job["_id"], "lock_token": lock_token},
                {"$set": {
                    "state": "done" if not errors else "error",
                    "finished_at": datetime.utcnow(),
                    "attempts": (job.get("attempts", 0) + 1),
                    "result": {"updated": updated, "error_count": len(errors), "errors": errors}
                }}
            )
        except Exception as e:
            schedules_col.update_one(
                {"_id": job["_id"], "lock_token": lock_token},
                {"$set": {
                    "state": "error",
                    "finished_at": datetime.utcnow(),
                    "attempts": (job.get("attempts", 0) + 1),
                    "result": {"updated": 0, "error_count": 1, "errors": [str(e)]}
                }}
            )

# =========================================================
#                       ROUTES
# =========================================================
@admin_orders_bp.route("/admin/orders/data")
def admin_orders_data():
    if not _require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    cache_key = _build_orders_cache_key(request.args)
    cached = get_cached_json(cache_key)
    if cached and isinstance(cached, dict):
        payload = cached.get("payload")
        etag = cached.get("etag")
        inm = request.headers.get("If-None-Match")
        if inm and etag and inm == etag:
            resp = make_response("", 304)
            resp.headers["Cache-Control"] = "private, max-age=30"
            resp.headers["ETag"] = etag
            return resp
        if payload is not None:
            return _json_with_cache(payload, etag)

    payload = _build_orders_data_payload(request.args)
    etag = _etag_for_payload(payload)
    set_cached_json(cache_key, {"payload": payload, "etag": etag}, _ORDERS_CACHE_TTL_SECONDS)
    return _json_with_cache(payload, etag)

@admin_orders_bp.route("/admin/orders")
def admin_view_orders():
    if not _require_admin():
        return redirect(url_for("login.login"))

    view_mode = (request.args.get("view") or "lines").strip().lower()
    if view_mode not in {"lines", "orders"}:
        view_mode = "lines"

    sort = (request.args.get("sort") or "newest").strip().lower()
    if sort not in ALLOWED_SORTS:
        sort = "newest"

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

    source_filter = _normalize_source_filter(request.args.get("source"))

    view_line_url = url_for("admin_orders.admin_view_orders")
    view_order_url = url_for("admin_orders.admin_view_orders")
    try:
        view_line_query = _build_preserved_query({**request.args.to_dict(flat=True), "view": "lines"})
        view_order_query = _build_preserved_query({**request.args.to_dict(flat=True), "view": "orders"})
        if view_line_query:
            view_line_url = f"{view_line_url}?{view_line_query}"
        if view_order_query:
            view_order_url = f"{view_order_url}?{view_order_query}"
    except Exception:
        pass

    return render_template(
        "admin_orders.html",
        orders=[],
        order_lines=[],
        order_lines_count=0,
        page=page, total_pages=1, total_orders=0,
        status_filter=(request.args.get("status") or "").strip().lower(),
        order_id_q=(request.args.get("order_id") or "").strip(),
        customer_q=(request.args.get("customer") or "").strip(),
        paid_from=(request.args.get("paid_from") or "").strip().lower(),
        min_total=(request.args.get("min_total") or "").strip(),
        max_total=(request.args.get("max_total") or "").strip(),
        date_from=(request.args.get("date_from") or "").strip(),
        date_to=(request.args.get("date_to") or "").strip(),
        sort=sort, per_page=per_page,
        item_service=(request.args.get("item_service") or "").strip(),
        item_offer=(request.args.get("item_offer") or "").strip(),
        item_phone=(request.args.get("item_phone") or "").strip(),
        api_filter=(request.args.get("api_filter") or "").strip().lower(),
        source=source_filter,
        filters_query=_build_preserved_query(request.args),
        view_mode=view_mode,
        view_line_url=view_line_url,
        view_order_url=view_order_url,
    )

@admin_orders_bp.route("/admin/orders/<order_id>/items/<int:item_index>/status", methods=["POST"])
def update_order_line_status(order_id, item_index):
    if not _require_admin():
        return redirect(url_for("login.login"))

    payload = {}
    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        payload = {}

    new_status = (request.form.get("line_status") or payload.get("line_status") or "").strip().lower()
    api_status = (request.form.get("api_status") or payload.get("api_status") or "").strip().lower()

    if new_status not in {"delivered", "processing", "failed", "pending", "refunded"}:
        flash("Invalid line status.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    parsed = _parse_order_id_param(order_id)
    if not parsed:
        flash("Invalid order id.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))
    source, oid = parsed
    orders_collection = _get_orders_collection(source)

    order = orders_collection.find_one({"_id": oid})
    if not order:
        flash("Order not found.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    items = order.get("items") or []
    if item_index < 0 or item_index >= len(items):
        flash("Line item not found.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    item = items[item_index]
    current_line = _normalize_line_status(item.get("line_status"))
    if _is_final_line_status(current_line) and _normalize_line_status(new_status) != "delivered":
        flash("This line is delivered and cannot be downgraded.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    now = datetime.utcnow()
    item["line_status"] = new_status
    if api_status:
        item["api_status"] = api_status

    update_doc = {
        f"items.{item_index}.line_status": new_status,
        f"items.{item_index}.provider_status_checked_at": now,
        "updated_at": now,
    }
    if api_status:
        update_doc[f"items.{item_index}.api_status"] = api_status

    current_status = (order.get("status") or "").lower()
    if current_status != "completed":
        new_order_status = _compute_order_status_from_items(items, current_status=current_status)
        if new_order_status and new_order_status != current_status:
            update_doc["status"] = new_order_status
            if new_order_status == "delivered" and not order.get("delivered_at"):
                update_doc["delivered_at"] = now

    orders_collection.update_one({"_id": oid}, {"$set": update_doc})

    flash("✅ Line status updated.", "success")
    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

@admin_orders_bp.route("/admin/orders/<order_id>/update", methods=["POST"])
def update_order_status(order_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ALLOWED_STATUSES:
        flash("Invalid status.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    parsed = _parse_order_id_param(order_id)
    if not parsed:
        flash("Invalid order id.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))
    source, oid = parsed
    orders_collection = _get_orders_collection(source)

    updated, errors = _apply_status_change([oid], new_status, reason="manual", actor_admin_id=session.get("user_id"), orders_collection=orders_collection, source=source)
    if updated:
        msg = {
            "processing": "✅ Order marked as Processing.",
            "delivered": "✅ Order marked as Delivered.",
            "failed": "✅ Order marked as Failed.",
            "refunded": "✅ Order marked as Refunded (wallet credited if not already).",
            "pending": "✅ Order marked as Pending.",
            "completed": "✅ Order marked as Completed.",
        }.get(new_status, "✅ Order updated.")
        flash(msg, "success")
    else:
        if errors:
            flash(" | ".join(errors[:3]), "warning")
        else:
            flash("ℹ️ No change to order.", "warning")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

@admin_orders_bp.route("/admin/orders/bulk-deliver", methods=["POST"])
def bulk_deliver_orders():
    """
    Existing behavior: mark all orders that match CURRENT FILTERS and are processing -> delivered.
    """
    if not _require_admin():
        return redirect(url_for("login.login"))
    args = request.args.to_dict(flat=True)
    args["status"] = "processing"
    query = _build_query_from_params(args)
    source_filter = _normalize_source_filter(request.args.get("source"))

    try:
        updated_total = 0
        errors = []

        if source_filter in {"main", "all"}:
            ids_main = [o["_id"] for o in orders_col.find(query, {"_id": 1})]
            updated, errs = _apply_status_change(
                ids_main,
                "delivered",
                reason="bulk_deliver",
                actor_admin_id=session.get("user_id"),
                orders_collection=orders_col,
                source="main",
            )
            updated_total += updated
            errors += errs

        if source_filter in {"campus", "all"}:
            ids_campus = [o["_id"] for o in campus_orders_col.find(query, {"_id": 1})]
            updated, errs = _apply_status_change(
                ids_campus,
                "delivered",
                reason="bulk_deliver",
                actor_admin_id=session.get("user_id"),
                orders_collection=campus_orders_col,
                source="campus",
            )
            updated_total += updated
            errors += errs

        if updated_total:
            flash(f"Marked {updated_total} processing order(s) as Delivered.", "success")
        else:
            flash("No eligible processing orders to deliver.", "warning")
        if errors:
            flash(" | ".join(errors[:3]), "warning")
    except Exception:
        flash("Bulk update failed.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

# NEW: mark SELECTED ids as delivered (from checkboxes / floating bar)
@admin_orders_bp.route("/admin/orders/bulk-deliver-selected", methods=["POST"])
def bulk_deliver_selected():
    if not _require_admin():
        return redirect(url_for("login.login"))

    line_ids = []
    if "line_ids" in request.form:
        line_ids += [request.form.get("line_ids") or ""]
    line_ids += request.form.getlist("line_ids[]")
    line_ids = ",".join([s for s in line_ids if s]).split(",")
    line_ids = [s.strip() for s in line_ids if s.strip()]

    if line_ids:
        try:
            updated_total = 0
            errors = []
            by_source = {"main": [], "campus": []}
            for lid in line_ids:
                parsed = _parse_line_id(lid)
                if not parsed:
                    errors.append(f"{lid}: invalid line id")
                    continue
                source, _, _ = parsed
                by_source[source].append(lid)

            for source, ids in by_source.items():
                if not ids:
                    continue
                updated, errs = _apply_line_status_change(
                    ids,
                    "delivered",
                    reason="bulk_deliver_selected",
                    actor_admin_id=session.get("user_id"),
                    orders_collection=_get_orders_collection(source),
                    target_source=source,
                )
                updated_total += updated
                errors += errs

            if updated_total:
                flash(f"Marked {updated_total} selected line(s) as Delivered.", "success")
            else:
                flash("No eligible lines to deliver.", "warning")
            if errors:
                flash(" | ".join(errors[:3]), "warning")
        except Exception:
            flash("Failed to bulk deliver selected lines.", "danger")

        back_to = url_for("admin_orders.admin_view_orders")
        qs = _build_preserved_query(request.args)
        return redirect(f"{back_to}?{qs}" if qs else back_to)

    # Accept: order_ids (comma string) OR order_ids[] OR order_id[]
    raw_list = []
    if "order_ids" in request.form:
        raw_list += [request.form.get("order_ids") or ""]
    raw_list += request.form.getlist("order_ids[]")
    raw_list += request.form.getlist("order_id[]")
    raw_list = ",".join([s for s in raw_list if s]).split(",")

    by_source = {"main": [], "campus": []}
    for s in raw_list:
        parsed = _parse_order_id_param((s or "").strip())
        if not parsed:
            continue
        source, oid = parsed
        by_source[source].append(oid)

    if not by_source["main"] and not by_source["campus"]:
        flash("Please select at least one order.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    try:
        updated_total = 0
        errors = []
        for source, ids in by_source.items():
            if not ids:
                continue
            updated, errs = _apply_status_change(
                ids,
                "delivered",
                reason="bulk_deliver_selected",
                actor_admin_id=session.get("user_id"),
                orders_collection=_get_orders_collection(source),
                source=source,
            )
            updated_total += updated
            errors += errs

        if updated_total:
            flash(f"Marked {updated_total} selected order(s) as Delivered.", "success")
        else:
            flash("No eligible orders to deliver.", "warning")
        if errors:
            flash(" | ".join(errors[:3]), "warning")
    except Exception:
        flash("Failed to bulk deliver selected.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

# =========================================================

# =========================================================
#            DB-BACKED SCHEDULING ENDPOINTS (Admin)
# =========================================================
@admin_orders_bp.route("/admin/orders/schedule-status", methods=["POST"])
def schedule_status():
    """
    Form fields:
      - order_ids: comma-separated string OR multiple order_ids[] fields OR order_id[]
      - status: one of ALLOWED_STATUSES
      - delay_minutes: int (optional)
      - run_at: "YYYY-MM-DD HH:MM" (UTC, optional)
      - note: optional
    One of delay_minutes or run_at is required.
    """
    if not _require_admin():
        return redirect(url_for("login.login"))

    status = (request.form.get("status") or "").strip().lower()
    if status not in ALLOWED_STATUSES:
        flash("Invalid status for scheduling.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    # collect order ids
    raw_list = []
    if "order_ids" in request.form:
        raw_list += [request.form.get("order_ids") or ""]
    raw_list += request.form.getlist("order_ids[]")
    raw_list += request.form.getlist("order_id[]")
    raw_list = ",".join([s for s in raw_list if s]).split(",")

    order_id_strs = []
    bad_ids = []
    for s in raw_list:
        s2 = (s or "").strip()
        if not s2:
            continue
        parsed = _parse_order_id_param(s2)
        if not parsed:
            bad_ids.append(s2)
            continue
        source, oid = parsed
        normalized = f"{source}:{oid}" if source == "campus" else str(oid)
        order_id_strs.append(normalized)

    # collect line ids
    line_ids = []
    if "line_ids" in request.form:
        line_ids += [request.form.get("line_ids") or ""]
    line_ids += request.form.getlist("line_ids[]")
    line_ids = ",".join([s for s in line_ids if s]).split(",")
    line_ids = [s.strip() for s in line_ids if s.strip()]

    valid_line_ids = []
    for lid in line_ids:
        parsed = _parse_line_id(lid)
        if parsed:
            source, oid, idx = parsed
            normalized = f"{source}:{oid}:{idx}" if source == "campus" else f"{oid}:{idx}"
            valid_line_ids.append(normalized)

    if not order_id_strs and not valid_line_ids:
        flash("Please select at least one valid order or line.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))
    if valid_line_ids and status == "completed" and not order_id_strs:
        flash("Completed is an order-only status. Choose another status for lines.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    # compute run time
    delay_str  = (request.form.get("delay_minutes") or "").strip()
    run_at_str = (request.form.get("run_at") or "").strip()
    run_time   = None

    if delay_str:
        try:
            mins = int(delay_str)
            run_time = datetime.utcnow() + timedelta(minutes=max(0, mins))
        except Exception:
            flash("Invalid delay minutes.", "danger")
            return redirect(url_for("admin_orders.admin_view_orders"))
    elif run_at_str:
        dt = _parse_date(run_at_str)
        if not dt:
            flash("Invalid run_at datetime. Use 'YYYY-MM-DD HH:MM' (UTC).", "danger")
            return redirect(url_for("admin_orders.admin_view_orders"))
        run_time = dt
        if run_time < datetime.utcnow():
            flash("Run time must be in the future.", "warning")
            return redirect(url_for("admin_orders.admin_view_orders"))
    else:
        flash("Provide either delay_minutes or run_at.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    note = (request.form.get("note") or "").strip()
    admin_id = (session.get("user_id") or None)
    job = _enqueue_status_job(order_id_strs, status, run_time, str(admin_id) if admin_id else None, note, line_ids=valid_line_ids)

    target_count = len(order_id_strs) or len(valid_line_ids)
    target_label = "order(s)" if order_id_strs else "line(s)"
    flash(f"⏱️ Scheduled {target_count} {target_label} → {status} at {run_time.strftime('%Y-%m-%d %H:%M')} UTC.", "success")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

@admin_orders_bp.route("/admin/orders/schedules", methods=["GET"])
def list_schedules():
    """Returns JSON of recent schedules (for the offcanvas in the UI)."""
    if not _require_admin():
        return redirect(url_for("login.login"))
    # Also opportunistically process due jobs when viewing the list
    try:
        _process_due_jobs(max_batch=25)
    except Exception:
        pass

    jobs = []
    for j in schedules_col.find({}).sort([("created_at", -1)]).limit(100):
        jobs.append({
            "id": str(j.get("_id")),
            "job_key": j.get("job_key"),
            "next_run_time": j.get("run_at").strftime("%Y-%m-%d %H:%M:%S UTC") if j.get("run_at") else None,
            "state": j.get("state"),
            "status": j.get("status"),
            "args": [j.get("order_ids"), j.get("status")],
            "result": j.get("result"),
            "attempts": j.get("attempts", 0),
        })
    return jsonify({"jobs": jobs})

@admin_orders_bp.route("/admin/orders/schedules/<job_id>/cancel", methods=["POST"])
def cancel_schedule(job_id):
    if not _require_admin():
        return redirect(url_for("login.login"))
    try:
        res = schedules_col.update_one({"_id": ObjectId(job_id)}, {"$set": {"state": "cancelled"}})
        if res.modified_count:
            flash("🗑️ Schedule cancelled.", "success")
        else:
            flash("Schedule not found.", "warning")
    except Exception as e:
        flash(f"Failed to cancel schedule: {e}", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

# Optional: endpoint you can ping from Render Cron every minute
@admin_orders_bp.route("/admin/orders/schedules/run-due", methods=["POST", "GET"])
def run_due_schedules():
    if not _require_admin():
        # If you want cron w/o session, you can protect via secret token instead
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        _process_due_jobs(max_batch=50)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
