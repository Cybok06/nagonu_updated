# admin_orders.py  — Admin Orders + DB-Backed Scheduler (Render-safe) + Bulk Deliver (Selected)
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify, make_response, send_file
from bson import ObjectId, Regex
from db import db, campus_db
from datetime import datetime, timedelta
import json
from ast import literal_eval
import os
import re
import time
import threading
import hashlib
from io import BytesIO
from urllib.parse import urlencode
import uuid
from typing import List, Tuple
from collections import OrderedDict
import heapq
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from order_status import AUTO_UPDATE_SETTINGS_ID, _compute_order_status_from_items, _normalize_status

admin_orders_bp = Blueprint("admin_orders", __name__)

orders_col        = db["orders"]
campus_orders_col = campus_db["orders"]
services_col      = db["services"]
campus_services_col = campus_db["services"]
users_col         = db["users"]
balances_col      = db["balances"]         # for refunds
transactions_col  = db["transactions"]     # for refund ledger
campus_balances_col = campus_db["balances"]
campus_transactions_col = campus_db["transactions"]
campus_provider_accounts_col = campus_db["provider_accounts"]
campus_provider_transactions_col = campus_db["provider_transactions"]
schedules_col     = db["order_schedules"]  # NEW: persistent job queue
export_batches_col = db["order_export_batches"]
auto_update_settings_col = db["order_auto_update_settings"]

# Keep legacy; primary set includes refunded
ALLOWED_STATUSES   = {"pending", "processing", "delivered", "failed", "completed", "refunded", "partially_refunded"}
ALLOWED_SORTS      = {"newest", "oldest", "amount_desc", "amount_asc"}
DEFAULT_PER_PAGE   = 10
FINAL_STATUS       = "completed"
API_PROVIDERS = {"codecraft", "datakazina", "skplug", "bundleportal"}
API_PROVIDER_LABELS = {
    "codecraft": "CodeCraft",
    "bundleportal": "BundlePortal",
    "datakazina": "DataKazina",
    "skplug": "SkPlug",
}
EXPORT_FINAL_STATUSES = {"delivered", "completed", "refunded", "partially_refunded"}
EXPORT_NETWORK_KEYWORDS = OrderedDict([
    ("MTN", ("mtn",)),
    ("TELECEL", ("telecel", "vodafone")),
    ("AIRTELTIGO", ("airteltigo", "airtel", "tigo", "at")),
])
ALLOWED_TRANSITIONS = {
    "pending": ALLOWED_STATUSES,
    "processing": ALLOWED_STATUSES,
    "failed": ALLOWED_STATUSES,
    "refunded": ALLOWED_STATUSES,
    "partially_refunded": ALLOWED_STATUSES,
    "delivered": {"delivered", "completed", "refunded"},
    "completed": {"completed", "refunded", "partially_refunded"},
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
_orders_cache_version = 1

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

def _get_orders_cache_version() -> int:
    global _orders_cache_version
    client = _get_redis_client()
    if client:
        try:
            raw = client.get("admin_orders:cache_version")
            if raw:
                return max(1, int(raw))
            client.set("admin_orders:cache_version", "1")
        except Exception:
            pass
    return max(1, int(_orders_cache_version))

def bump_orders_cache_version() -> int:
    global _orders_cache_version
    client = _get_redis_client()
    if client:
        try:
            return int(client.incr("admin_orders:cache_version"))
        except Exception:
            pass
    _orders_cache_version = max(1, int(_orders_cache_version)) + 1
    return _orders_cache_version

def _jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")

def _can_transition(old_status: str, new_status: str) -> bool:
    old_status = (old_status or "").strip().lower()
    new_status = (new_status or "").strip().lower()
    if old_status == new_status:
        return True
    return new_status in ALLOWED_TRANSITIONS.get(old_status, set())

def _is_final_order_status(status: str | None) -> bool:
    return (status or "").strip().lower() in {"delivered", "completed"}

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
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return None
    except Exception:
        return None


def _parse_time(tstr):
    try:
        return datetime.strptime((tstr or "").strip(), "%H:%M").time()
    except (TypeError, ValueError):
        return None


def _filter_datetime_bounds(args):
    """Return an inclusive lower and exclusive upper bound for date/time filters."""
    start_date = _parse_date((args.get("date_from") or "").strip())
    end_date = _parse_date((args.get("date_to") or "").strip())
    start_time = _parse_time(args.get("time_from"))
    end_time = _parse_time(args.get("time_to"))

    start = None
    if start_date:
        start = datetime.combine(start_date.date(), start_time or datetime.min.time())

    end = None
    if end_date:
        if end_time:
            # A time input has minute precision, so include that whole minute.
            end = datetime.combine(end_date.date(), end_time) + timedelta(minutes=1)
        else:
            end = datetime.combine(end_date.date(), datetime.min.time()) + timedelta(days=1)
    return start, end

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
    date_from, date_to = _filter_datetime_bounds(args)

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
                    "provider": {"$in": sorted(API_PROVIDERS)},
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
        "refund_amount": item.get("refund_amount") or 0,
        "refunded_at": _format_dt(item.get("refunded_at")),
    }

def _serialize_order(order: dict) -> dict:
    created_text = _format_dt(order.get("created_at"))
    is_ussd_order = str(order.get("paid_from") or "").strip().lower() == "ussd" or bool(order.get("ussd"))
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
        "is_store_order": _is_store_order(order),
        "store_slug": order.get("store_slug") or "",
        "is_ussd_order": is_ussd_order,
        "api_passed": bool(order.get("api_passed")),
        "api_providers": order.get("api_providers") or [],
        "created_at_display": created_text,
        "created_at_text": created_text,
        "created_at_iso": order.get("created_at").isoformat() if order.get("created_at") else None,
    }

def _serialize_line(line: dict) -> dict:
    created_text = _format_dt(line.get("created_at"))
    is_ussd_order = str(line.get("paid_from") or "").strip().lower() == "ussd" or bool(line.get("ussd"))
    return {
        "order_id": line.get("order_id"),
        "order_mongo_id_param": line.get("order_mongo_id_param"),
        "item_index": line.get("item_index"),
        "line_id": line.get("line_id"),
        "source": line.get("source") or "main",
        "user": _serialize_user(line.get("user") or {}),
        "item": _serialize_item(line.get("item") or {}),
        "paid_from": line.get("paid_from") or "",
        "is_store_order": bool(line.get("is_store_order")),
        "store_slug": line.get("store_slug") or "",
        "is_ussd_order": is_ussd_order,
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
        "paid_from", "min_total", "max_total", "date_from", "date_to", "time_from", "time_to",
        "item_service", "item_offer", "item_phone", "api_filter",
    ]
    normalized = {}
    for k in keys:
        normalized[k] = (args.get(k) or "").strip()
    normalized["source"] = _normalize_source_filter(normalized.get("source"))
    normalized["view"] = (normalized.get("view") or "lines").strip().lower()
    normalized["cv"] = _get_orders_cache_version()
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
        "store_slug": 1,
        "debug.store_checkout": 1,
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
            _prepare_order(o, "main", order_lines, orders_col, user_map=user_map, persist_changes=False)
    elif source_filter == "campus":
        total_orders = campus_orders_col.count_documents(query)
        total_pages = max(1, (total_orders + per_page - 1) // per_page)
        orders = list(campus_orders_col.find(query, projection).sort(sort_spec).skip(skip).limit(per_page))
        user_map = _load_users_for_orders(orders)
        for o in orders:
            _prepare_order(o, "campus", order_lines, campus_orders_col, user_map=user_map, persist_changes=False)
    else:
        count_main = orders_col.count_documents(query)
        count_campus = campus_orders_col.count_documents(query)
        total_orders = count_main + count_campus
        total_pages = max(1, (total_orders + per_page - 1) // per_page)
        page_orders = _fetch_merged_orders_page(
            query=query,
            projection=projection,
            sort_spec=sort_spec,
            skip=skip,
            limit=per_page,
        )
        user_map = _load_users_for_orders(page_orders)
        for o in page_orders:
            src = _normalize_source(o.get("source"), "main")
            col = _get_orders_collection(src)
            _prepare_order(o, src, order_lines, col, user_map=user_map, persist_changes=False)
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
    labels = [API_PROVIDER_LABELS[p] for p in order["api_providers"] if p in API_PROVIDER_LABELS]
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


def _resolve_source_filter(src: str | None, default: str = "main") -> str:
    v = (src or "").strip().lower()
    if v in {"all", "main", "campus"}:
        return v
    return default

def _clean_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()

def _normalize_export_network(value: str | None) -> str:
    raw = _clean_text(value).upper()
    if not raw:
        return ""
    for label, needles in EXPORT_NETWORK_KEYWORDS.items():
        if raw == label:
            return label
        for needle in needles:
            if needle.upper() in raw:
                return label
    return raw

def _guess_network_from_item(item: dict, service_name: str | None = None) -> str:
    candidates = [
        item.get("network"),
        item.get("network_name"),
        item.get("provider_network"),
        item.get("ported_expected_network"),
        item.get("ported_detected_network"),
        service_name,
        item.get("serviceName"),
    ]
    for candidate in candidates:
        normalized = _normalize_export_network(candidate)
        if normalized:
            return normalized
    return "UNKNOWN"

def _effective_export_line_status(order_status: str | None, item: dict) -> str:
    line_status = _normalize_line_status(item.get("line_status"))
    if line_status:
        return line_status
    return _normalize_line_status(order_status)

def _is_export_skipped_or_duplicate(item: dict) -> bool:
    api_status = (item.get("api_status") or "").strip().lower()
    if api_status == "skipped":
        return True
    line_status = _normalize_line_status(item.get("line_status"))
    return line_status in {"skipped_duplicate_processing", "skipped_duplicate_in_cart"}

def _is_undelivered_export_line(order_status: str | None, item: dict) -> bool:
    if _normalize_line_status(order_status) in EXPORT_FINAL_STATUSES:
        return False
    return _effective_export_line_status(order_status, item) not in EXPORT_FINAL_STATUSES

def _parse_export_date_range(args):
    timeframe = (args.get("timeframe") or "today").strip().lower()
    now = datetime.utcnow()
    if timeframe == "today":
        day_start = datetime(now.year, now.month, now.day)
        start_time_raw = (args.get("today_start_time") or "").strip()
        end_time_raw = (args.get("today_end_time") or "").strip()

        if start_time_raw:
            try:
                hh, mm = [int(part) for part in start_time_raw.split(":", 1)]
                start = day_start.replace(hour=hh, minute=mm)
            except Exception:
                raise ValueError("Invalid start time for today.")
        else:
            start = day_start

        if end_time_raw:
            try:
                hh, mm = [int(part) for part in end_time_raw.split(":", 1)]
                end = day_start.replace(hour=hh, minute=mm)
            except Exception:
                raise ValueError("Invalid end time for today.")
        else:
            end = day_start + timedelta(days=1)

        if end <= start:
            raise ValueError("Today end time must be later than the start time.")

        return timeframe, start, end

    if timeframe != "custom":
        raise ValueError("Invalid timeframe selected.")

    start_raw = (args.get("date_from") or "").strip()
    end_raw = (args.get("date_to") or "").strip()
    if not start_raw or not end_raw:
        raise ValueError("Choose both start and end date/time for custom range.")

    start = _parse_date(start_raw)
    end = _parse_date(end_raw)
    if not start or not end:
        raise ValueError("Invalid custom date range.")

    if end <= start:
        raise ValueError("End date/time must be later than the start date/time.")

    return timeframe, start, end

def _collect_export_catalog():
    services = set()
    network_map = {}

    for col in (services_col, campus_services_col):
        try:
            docs = col.find({}, {"name": 1, "service_network": 1, "network": 1})
        except Exception:
            docs = []
        for doc in docs:
            name = _clean_text(doc.get("name"))
            if not name:
                continue
            services.add(name)
            network = _normalize_export_network(doc.get("service_network") or doc.get("network") or name)
            if network:
                network_map.setdefault(name, network)

    service_list = sorted(services, key=lambda x: x.lower())
    network_list = sorted({v for v in network_map.values() if v}, key=lambda x: x.lower())
    return service_list, network_list, network_map


def _collect_auto_update_service_names() -> List[str]:
    names = set()

    for col in (services_col, campus_services_col):
        try:
            docs = col.find({}, {"name": 1})
        except Exception:
            docs = []
        for doc in docs:
            name = _clean_text(doc.get("name"))
            if name:
                names.add(name)

    return sorted(names, key=lambda x: x.lower())


def _get_auto_update_settings_doc() -> dict:
    doc = auto_update_settings_col.find_one({"_id": AUTO_UPDATE_SETTINGS_ID}) or {}
    service_names = []
    seen = set()
    for value in doc.get("service_names") or []:
        name = _clean_text(value)
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        service_names.append(name)
    try:
        minutes = int(doc.get("minutes") or 0)
    except Exception:
        minutes = 0
    return {
        "active": bool(doc.get("active")),
        "minutes": max(0, minutes),
        "service_names": service_names,
        "updated_at": doc.get("updated_at"),
    }

def _extract_export_service_names(args) -> List[str]:
    values: List[str] = []
    if hasattr(args, "getlist"):
        try:
            values.extend(args.getlist("service_name"))
        except Exception:
            pass
        try:
            values.extend(args.getlist("service_names"))
        except Exception:
            pass
    single = _clean_text(args.get("service_name")) if hasattr(args, "get") else ""
    if single and single not in values:
        values.append(single)
    normalized = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized

def _collect_undelivered_export_rows(args):
    service_names = _extract_export_service_names(args)
    if not service_names:
        raise ValueError("Choose at least one service to export.")
    service_names_lower = {name.lower() for name in service_names}

    network_filter = _normalize_export_network(args.get("network"))
    source_filter = _resolve_source_filter(args.get("source"), default="main")
    timeframe, start_dt, end_dt = _parse_export_date_range(args)

    query = {
        "created_at": {"$gte": start_dt, "$lt": end_dt},
        "status": {"$nin": ["refunded"]},
        "$or": [{"items.serviceName": Regex(re.escape(name), "i")} for name in service_names],
    }
    projection = {
        "order_id": 1,
        "user_id": 1,
        "items": 1,
        "paid_from": 1,
        "created_at": 1,
        "status": 1,
        "total_amount": 1,
    }

    source_batches = []
    if source_filter in {"all", "main"}:
        source_batches.append(("main", list(orders_col.find(query, projection).sort([("created_at", 1), ("_id", 1)]))))
    if source_filter in {"all", "campus"}:
        source_batches.append(("campus", list(campus_orders_col.find(query, projection).sort([("created_at", 1), ("_id", 1)]))))

    all_orders = []
    for source, batch in source_batches:
        for order in batch:
            _decorate_order_source(order, source)
            all_orders.append(order)

    user_map = _load_users_for_orders(all_orders)
    rows = []

    for order in all_orders:
        user = user_map.get(order.get("user_id")) or {}
        order_status = order.get("status")
        created_at = order.get("created_at")

        for idx, item in enumerate(order.get("items") or []):
            service_text = _clean_text(item.get("serviceName"))
            if service_text.lower() not in service_names_lower:
                continue

            if _is_export_skipped_or_duplicate(item):
                continue

            if not _is_undelivered_export_line(order_status, item):
                continue

            network_text = _guess_network_from_item(item, service_text)
            if network_filter and network_filter != "ANY" and network_text != network_filter:
                continue

            line_status = _effective_export_line_status(order_status, item)
            rows.append({
                "source": order.get("source") or "main",
                "source_label": "Campus Data" if (order.get("source") == "campus") else "Main",
                "order_id": order.get("order_id") or "",
                "line_id": f"{order.get('source')}:{order.get('_id')}:{idx}" if (order.get("source") == "campus") else f"{order.get('_id')}:{idx}",
                "line_number": idx + 1,
                "customer_name": _clean_text(f"{user.get('first_name', '')} {user.get('last_name', '')}") or (user.get("username") or ""),
                "customer_phone": _clean_text(user.get("phone")),
                "customer_email": _clean_text(user.get("email")),
                "service": service_text,
                "offer": _clean_text(item.get("value")),
                "network": network_text,
                "phone": _clean_text(item.get("phone")),
                "amount": _money(item.get("amount")),
                "paid_from": _clean_text(order.get("paid_from")).lower(),
                "order_status": _normalize_line_status(order_status),
                "line_status": line_status,
                "provider": _clean_text(item.get("provider")),
                "api_status": _clean_text(item.get("api_status")),
                "created_at": created_at,
                "created_at_display": _format_dt(created_at) or "",
                "total_amount": _money(order.get("total_amount")),
            })

    rows.sort(key=lambda x: (
        x.get("created_at") or datetime.min,
        str(x.get("order_id") or ""),
        int(x.get("line_number") or 0),
    ))
    meta = {
        "service_name": ", ".join(service_names),
        "service_names": service_names,
        "network": network_filter or "ANY",
        "source": source_filter,
        "timeframe": timeframe,
        "date_from": start_dt,
        "date_to": end_dt,
        "count": len(rows),
    }
    return rows, meta

def _compact_export_offer(value: str | None) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*gb\s*$", text, re.IGNORECASE)
    if match:
        return match.group(1)
    return text

def _export_filename(meta: dict, ext: str) -> str:
    service = re.sub(r"[^a-z0-9]+", "-", (meta.get("service_name") or "service").strip().lower()).strip("-") or "service"
    network = re.sub(r"[^a-z0-9]+", "-", (meta.get("network") or "any").strip().lower()).strip("-") or "any"
    date_part = (meta.get("date_from") or datetime.utcnow()).strftime("%Y%m%d")
    return f"undelivered-{service}-{network}-{date_part}.{ext}"

def _next_export_badge_no() -> int:
    try:
        latest = export_batches_col.find_one({}, {"badge_no": 1}, sort=[("badge_no", -1)])
        return int((latest or {}).get("badge_no") or 0) + 1
    except Exception:
        return 1

def _save_export_batch(rows: List[dict], meta: dict, export_format: str, admin_id: str | None):
    created_at = datetime.utcnow()
    badge_no = _next_export_badge_no()
    lines = [{
        "line_id": row.get("line_id"),
        "phone": row.get("phone"),
        "offer": row.get("offer"),
        "network": row.get("network"),
        "source": row.get("source"),
        "order_id": row.get("order_id"),
    } for row in rows if row.get("line_id")]

    doc = {
        "badge_no": badge_no,
        "label": f"Badge {badge_no}",
        "service_name": meta.get("service_name"),
        "network": meta.get("network"),
        "source": meta.get("source"),
        "timeframe": meta.get("timeframe"),
        "date_from": meta.get("date_from"),
        "date_to": meta.get("date_to"),
        "count": len(lines),
        "format": export_format,
        "created_at": created_at,
        "created_by": admin_id,
        "lines": lines,
    }
    res = export_batches_col.insert_one(doc)
    doc["_id"] = res.inserted_id
    return doc

def _load_export_batch_lines(batch: dict) -> List[dict]:
    details = []
    for line in (batch.get("lines") or []):
        line_id = line.get("line_id")
        parsed = _parse_line_id(line_id)
        phone = _clean_text(line.get("phone"))
        offer = _compact_export_offer(line.get("offer"))
        network = _clean_text(line.get("network"))
        status = "unknown"
        order_status = ""
        if parsed:
            source, oid, idx = parsed
            order = _get_orders_collection(source).find_one({"_id": oid}, {"items": 1, "status": 1, "order_id": 1})
            if order:
                items = order.get("items") or []
                if 0 <= idx < len(items):
                    item = items[idx] or {}
                    status = _effective_export_line_status(order.get("status"), item)
                    phone = _clean_text(item.get("phone")) or phone
                    offer = _compact_export_offer(item.get("value")) or offer
                    network = _guess_network_from_item(item, item.get("serviceName")) or network
                order_status = _normalize_line_status(order.get("status"))
        details.append({
            "line_id": line_id,
            "phone": phone,
            "offer": offer,
            "network": network,
            "status": status,
            "order_status": order_status,
            "source": line.get("source") or "",
            "order_id": line.get("order_id") or "",
        })
    return details

def _serialize_export_batch_summary(batch: dict) -> dict:
    created_at = batch.get("created_at")
    return {
        "id": str(batch.get("_id")),
        "badge_no": batch.get("badge_no") or 0,
        "label": batch.get("label") or f"Badge {batch.get('badge_no') or ''}",
        "network": batch.get("network") or "",
        "count": batch.get("count") or 0,
        "format": batch.get("format") or "",
        "created_at_display": _format_dt(created_at) or "",
        "created_at_time_display": created_at.strftime("%I:%M %p") if isinstance(created_at, datetime) else "",
        "created_at_iso": created_at.isoformat() if isinstance(created_at, datetime) else None,
    }

def _export_rows_to_txt(rows: List[dict], meta: dict):
    lines = [str(meta.get("network") or "NETWORK"), ""]
    lines.extend([
        f"{_clean_text(row.get('phone'))} {_compact_export_offer(row.get('offer'))}".strip()
        for row in rows
        if _clean_text(row.get("phone"))
    ])

    payload = "\n".join(lines).encode("utf-8")
    output = BytesIO(payload)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=_export_filename(meta, "txt"), mimetype="text/plain; charset=utf-8")

def _export_rows_to_excel(rows: List[dict], meta: dict):
    lines = [str(meta.get("network") or "NETWORK"), ""]
    lines.extend([
        f"{_clean_text(row.get('phone'))} {_compact_export_offer(row.get('offer'))}".strip()
        for row in rows
        if _clean_text(row.get("phone"))
    ])
    df = pd.DataFrame({"Export": lines})
    output = BytesIO()
    writer_engine = "xlsxwriter"
    try:
        __import__("xlsxwriter")
    except ModuleNotFoundError:
        try:
            __import__("openpyxl")
            writer_engine = "openpyxl"
        except ModuleNotFoundError as exc:
            raise RuntimeError("Excel export requires either xlsxwriter or openpyxl to be installed.") from exc

    with pd.ExcelWriter(output, engine=writer_engine) as writer:
        df.to_excel(writer, index=False, header=False, sheet_name="Undelivered Orders")
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=_export_filename(meta, "xlsx"))

def _export_rows_to_pdf(rows: List[dict], meta: dict):
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=landscape(letter), leftMargin=24, rightMargin=24, topMargin=28, bottomMargin=24)
    styles = getSampleStyleSheet()

    header = [
        Paragraph(str(meta.get("network") or "NETWORK"), styles["Title"]),
        Spacer(1, 8),
        Spacer(1, 12),
    ]

    table_data = []
    for row in rows:
        if _clean_text(row.get("phone")):
            table_data.append([f"{_clean_text(row.get('phone'))} {_compact_export_offer(row.get('offer'))}".strip()])

    table = Table(table_data or [[""]])
    table.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
    ]))

    doc.build(header + [table])
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=_export_filename(meta, "pdf"), mimetype="application/pdf")

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
    # Delivered lines may still be refunded. Only an already-refunded line is
    # immutable, which prevents accidental repeat credits.
    return _normalize_line_status(s) == "refunded"

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


def _line_refund_reference(order: dict, item_index: int) -> str:
    order_key = order.get("order_id") or str(order.get("_id"))
    return f"{order_key}:LINE:{item_index}:REFUND"


def _is_store_order(order: dict) -> bool:
    return bool((order.get("store_slug") or "").strip()) or bool(
        (order.get("debug") or {}).get("store_checkout")
    )


def _refund_collections(source: str):
    if (source or "").strip().lower() == "campus":
        return campus_balances_col, campus_transactions_col
    return balances_col, transactions_col


def _is_campus_mtn(item):
    return str(item.get("serviceName") or "").strip().upper() in {
        "MTN NORMAL", "MTN NORMA", "MTN EXPRESS",
    }


def _campus_package(value):
    if isinstance(value, str):
        try:
            value = literal_eval(value)
        except (ValueError, SyntaxError):
            return None
    if not isinstance(value, dict):
        return None
    try:
        return int(value["id"]), int(value["volume"])
    except (KeyError, ValueError, TypeError):
        return None


def _campus_missing_base(order, item):
    """Recover MTN base prices without treating the retail price as cost."""
    if not _is_campus_mtn(item):
        return 0.0, "campus_base_unavailable"
    items = order.get("items") or []
    indexes = [index for index, candidate in enumerate(items) if candidate is item]
    if not order.get("order_id") or len(indexes) != 1:
        return 0.0, "campus_base_unavailable"
    query = {"provider": "provider_wallet", "direction": "DEBIT",
             "reason": "ORDER_RESERVE", "order_id": order["order_id"]}
    if len(items) > 1:
        query["line_index"] = indexes[0] + 1
    debits = list(campus_provider_transactions_col.find(query))
    # More than one debit can mean a reused legacy order ID. Do not guess
    # or sum unrelated purchases to recover a single line's base price.
    if len(debits) == 1:
        amount = round(_money(debits[0].get("amount"), 0.0), 2)
        if amount > 0:
            return amount, "campus_original_provider_debit"
    if debits:
        return 0.0, "campus_ambiguous_provider_debit"
    service_id = str(item.get("serviceId") or "")
    package = _campus_package(item.get("value_obj") or item.get("value"))
    if not ObjectId.is_valid(service_id) or not package:
        return 0.0, "campus_base_unavailable"
    service = campus_services_col.find_one({"_id": ObjectId(service_id)}, {"offers": 1}) or {}
    matches = [offer for offer in service.get("offers", [])
               if _campus_package(offer.get("value")) == package]
    if len(matches) == 1:
        amount = round(_money(matches[0].get("amount"), 0.0), 2)
        if amount > 0:
            return amount, "campus_current_offer_base_fallback"
    return 0.0, "campus_base_unavailable"


def _line_refundable_amount(order: dict, item: dict, source: str = "main") -> Tuple[float, str]:
    """Return the wallet refund amount and the basis used to calculate it."""
    if (source or "").strip().lower() == "campus":
        base = round(_money(item.get("base_amount"), 0.0), 2)
        if base > 0:
            return base, "campus_line_base_amount"
        return _campus_missing_base(order, item)

    if not _is_store_order(order):
        return round(_money(item.get("amount"), 0.0), 2), "line_amount"

    # Store customers pay item.amount. The agent's refundable cost excludes
    # store_profit_amount (their markup). base_amount is the checkout snapshot
    # of that same agent-facing price and covers admin-complaint store orders.
    amount = round(_money(item.get("amount"), 0.0), 2)
    if item.get("store_profit_amount") is not None:
        base = round(amount - _money(item.get("store_profit_amount"), 0.0), 2)
        if base > 0:
            return base, "store_amount_less_markup"

    base = round(_money(item.get("base_amount"), 0.0), 2)
    if base > 0:
        return base, "store_base_amount"
    return 0.0, "store_base_unavailable"


def _credit_campus_provider_refund(
    order: dict,
    *,
    item_index: int | None = None,
    actor_admin_id=None,
    reason: str = "manual",
) -> Tuple[float, str | None]:
    """Restore only the exact provider-wallet debit recorded for a Campus order."""
    order_id = str(order.get("order_id") or "").strip()
    if not order_id:
        return 0.0, "campus order has no order ID"

    debit_query = {
        "provider": "provider_wallet",
        "direction": "DEBIT",
        "reason": "ORDER_RESERVE",
        "order_id": order_id,
    }
    # Legacy Campus bulk orders shared one order ID across many lines. New
    # split orders have one debit per order, so the order ID alone is exact.
    if item_index is not None and len(order.get("items") or []) > 1:
        debit_query["line_index"] = item_index + 1

    debits = list(campus_provider_transactions_col.find(debit_query, {"amount": 1, "line_index": 1, "dedupe_key": 1}))
    debit_amount = round(sum(_money(doc.get("amount"), 0.0) for doc in debits), 2)
    refund_basis = "original_provider_debit"
    scope = f"LINE:{item_index}" if item_index is not None else "ORDER"
    dedupe_key = f"CAMPUS_PROVIDER_REFUND:{order_id}:{scope}"
    existing = campus_provider_transactions_col.find_one({"dedupe_key": dedupe_key}, {"amount": 1})
    if existing:
        return round(_money(existing.get("amount"), 0.0), 2), None

    items = order.get("items") or []
    target_items = (
        [items[item_index]]
        if item_index is not None and 0 <= item_index < len(items)
        else [item for item in items if not _is_export_skipped_or_duplicate(item)]
    )
    if len(items) == 1 and len(debits) > 1 and any(_is_campus_mtn(item) for item in target_items):
        return 0.0, "multiple Campus debits match this order; refund requires reconciliation"
    if not debits:
        if not target_items or not all(_is_campus_mtn(item) for item in target_items):
            return 0.0, None
        resolved = [_line_refundable_amount(order, item, "campus") for item in target_items]
        if any(amount <= 0 for amount, basis in resolved):
            return 0.0, "Campus MTN base price could not be resolved"
        debit_amount = round(sum(amount for amount, basis in resolved), 2)
        refund_basis = (
            "campus_current_offer_base_fallback"
            if any(basis == "campus_current_offer_base_fallback" for amount, basis in resolved)
            else "mtn_normal_base_amount_fallback"
            if all(str(item.get("serviceName") or "").strip().lower() == "mtn normal" for item in target_items)
            else "campus_mtn_base_amount_fallback"
        )

    prior_credits = list(campus_provider_transactions_col.find({
        "provider": "provider_wallet", "direction": "CREDIT",
        "reason": "ORDER_REFUND", "order_id": order_id,
    }))
    if item_index is not None and len(items) > 1:
        # A completed whole-order credit also covers this line.
        if any(credit.get("line_index") is None for credit in prior_credits):
            return 0.0, None
        prior_credits = [credit for credit in prior_credits
                         if credit.get("line_index") == item_index + 1]
    debit_amount = max(0.0, round(
        debit_amount - sum(_money(credit.get("amount"), 0.0) for credit in prior_credits), 2,
    ))
    if debit_amount <= 0:
        return 0.0, None

    now = datetime.utcnow()
    credit_doc = {
        "provider": "provider_wallet",
        "amount": debit_amount,
        "direction": "CREDIT",
        "reason": "ORDER_REFUND",
        "order_id": order_id,
        "reference": order_id,
        "line_index": (item_index + 1) if item_index is not None else None,
        "dedupe_key": dedupe_key,
        "created_at": now,
        "meta": {
            "note": f"{reason.capitalize()} Campus provider-balance refund",
            "order_db_id": order.get("_id"),
            "actor_admin_id": actor_admin_id,
            "refund_basis": refund_basis,
            "original_debit_total": debit_amount,
            "original_debit_keys": [doc.get("dedupe_key") for doc in debits if doc.get("dedupe_key")],
        },
    }
    try:
        inserted = campus_provider_transactions_col.insert_one(credit_doc)
    except Exception:
        # A concurrent/refreshed request may have inserted the unique refund.
        existing = campus_provider_transactions_col.find_one({"dedupe_key": dedupe_key}, {"amount": 1})
        if existing:
            return round(_money(existing.get("amount"), 0.0), 2), None
        return 0.0, "failed to record Campus Balance refund"

    try:
        campus_provider_accounts_col.update_one(
            {"provider": "provider_wallet"},
            {
                "$inc": {"balance": debit_amount},
                "$set": {"updated_at": now, "currency": "GHS"},
                "$setOnInsert": {"created_at": now, "provider": "provider_wallet"},
            },
            upsert=True,
        )
    except Exception as exc:
        try:
            campus_provider_transactions_col.delete_one({"_id": inserted.inserted_id})
        except Exception:
            pass
        return 0.0, f"Campus Balance credit failed: {exc}"
    return debit_amount, None


def _credit_line_refund(
    order: dict,
    item: dict,
    item_index: int,
    *,
    reason: str,
    actor_admin_id=None,
    source: str = "main",
) -> Tuple[float, str | None]:
    """Credit one order line to the owner's wallet exactly once."""
    if item.get("refunded_at") or _normalize_line_status(item.get("line_status")) == "refunded":
        return 0.0, None

    refund_amount, refund_basis = _line_refundable_amount(order, item, source)
    refund_balances_col, refund_transactions_col = _refund_collections(source)
    user_id = order.get("user_id")
    if refund_amount <= 0:
        if (source or "").strip().lower() == "campus":
            return 0.0, "campus base amount is unavailable"
        if _is_store_order(order):
            return 0.0, "store base amount is unavailable; customer-paid amount was not refunded"
        return 0.0, "line refund amount is zero"
    if not user_id:
        return 0.0, "order has no wallet owner"

    reference = _line_refund_reference(order, item_index)
    if (source or "").strip().lower() == "campus":
        _provider_credit, provider_error = _credit_campus_provider_refund(
            order,
            item_index=item_index,
            actor_admin_id=actor_admin_id,
            reason=reason,
        )
        if provider_error:
            return 0.0, provider_error
    if refund_transactions_col.find_one({"type": "refund", "reference": reference}, {"_id": 1}):
        return refund_amount, None

    now = datetime.utcnow()
    wallet_credited = False
    try:
        refund_balances_col.update_one(
            {"user_id": user_id},
            {"$inc": {"amount": refund_amount}, "$set": {"updated_at": now}},
            upsert=True,
        )
        wallet_credited = True
        refund_transactions_col.insert_one(
            {
                "user_id": user_id,
                "amount": refund_amount,
                "reference": reference,
                "order_id": order.get("order_id"),
                "status": "success",
                "type": "refund",
                "gateway": "Wallet",
                "source": "admin_order_refund",
                "currency": "GHS",
                "created_at": now,
                "verified_at": now,
                "meta": {
                    "note": f"{reason.capitalize()} line refund",
                    "order_db_id": order.get("_id"),
                    "line_index": item_index,
                    "phone": item.get("phone"),
                    "refund_basis": refund_basis,
                    "order_source": (source or "main").strip().lower(),
                    "store_slug": order.get("store_slug"),
                    "actor_admin_id": actor_admin_id,
                },
            }
        )
        return refund_amount, None
    except Exception as exc:
        # Do not leave an untracked wallet credit behind. Without the refund
        # ledger entry a retry cannot determine that the balance was already
        # changed and could credit the customer twice.
        if wallet_credited:
            try:
                refund_balances_col.update_one(
                    {"user_id": user_id},
                    {"$inc": {"amount": -refund_amount}, "$set": {"updated_at": datetime.utcnow()}},
                )
            except Exception as rollback_exc:
                return 0.0, f"{exc}; wallet rollback failed: {rollback_exc}"
        return 0.0, str(exc)

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

def _fetch_merged_orders_page(query: dict, projection: dict, sort_spec: List[Tuple[str, int]], skip: int, limit: int) -> List[dict]:
    if limit <= 0:
        return []

    chunk_size = max(limit * 2, 50)
    consumed = 0
    results = []
    source_states = {
        "main": {
            "collection": orders_col,
            "offset": 0,
            "buffer": [],
            "index": 0,
            "exhausted": False,
        },
        "campus": {
            "collection": campus_orders_col,
            "offset": 0,
            "buffer": [],
            "index": 0,
            "exhausted": False,
        },
    }

    def refill_source(source_name: str):
        state = source_states[source_name]
        if state["exhausted"] or state["index"] < len(state["buffer"]):
            return
        batch = list(
            state["collection"]
            .find(query, projection)
            .sort(sort_spec)
            .skip(state["offset"])
            .limit(chunk_size)
        )
        state["offset"] += len(batch)
        state["buffer"] = batch
        state["index"] = 0
        if not batch:
            state["exhausted"] = True
            return
        for order in batch:
            _decorate_order_source(order, source_name)

    while len(results) < limit:
        heap = []
        for source_name in ("main", "campus"):
            refill_source(source_name)
            state = source_states[source_name]
            if state["index"] < len(state["buffer"]):
                order = state["buffer"][state["index"]]
                heapq.heappush(heap, (_sort_key_for_spec(order, sort_spec), source_name, order))

        if not heap:
            break

        _, winner_source, winner = heapq.heappop(heap)
        state = source_states[winner_source]
        state["index"] += 1

        if consumed >= skip:
            results.append(winner)
        consumed += 1

    return results

def _prepare_order(order: dict, source: str, order_lines: List[dict], orders_collection, user_map: dict | None = None, persist_changes: bool = True):
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
        current_status = (order.get("status") or "").lower()
        if current_status != "completed":
            new_status = _compute_order_status_from_items(items, current_status=current_status)
            if new_status and new_status != current_status:
                order["status"] = new_status
                if new_status == "delivered" and not order.get("delivered_at"):
                    order["delivered_at"] = now

        if persist_changes:
            for idx, updates in changed_indexes:
                set_doc = {f"items.{idx}.{k}": v for k, v in updates.items()}
                set_doc["updated_at"] = now
                orders_collection.update_one({"_id": order["_id"]}, {"$set": set_doc})

            if current_status != "completed" or new_status == "refunded":
                new_status = _compute_order_status_from_items(items, current_status=current_status)
                if new_status and new_status != current_status:
                    set_doc = {"status": new_status, "updated_at": now}
                    if new_status == "delivered" and not order.get("delivered_at"):
                        set_doc["delivered_at"] = now
                    orders_collection.update_one({"_id": order["_id"]}, {"$set": set_doc})
            bump_orders_cache_version()

    _compute_api_fields(order)
    for idx, item in enumerate(items):
        line_id = f"{source}:{order.get('_id')}:{idx}" if source == "campus" else f"{order.get('_id')}:{idx}"
        order_lines.append({
            "order_mongo_id": str(order.get("_id")),
            "order_mongo_id_param": order.get("order_id_param"),
            "order_id": order.get("order_id"),
            "user": order.get("user") or {},
            "paid_from": order.get("paid_from"),
            "is_store_order": _is_store_order(order),
            "store_slug": order.get("store_slug"),
            "ussd": order.get("ussd"),
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
    refund_source = target_source or ("campus" if orders_collection is campus_orders_col else "main")
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
            changed_indices = set()

            for idx in sorted(idxs):
                if idx < 0 or idx >= len(items):
                    errors.append(f"{oid}:{idx}: item not found")
                    continue
                item = items[idx]
                current_line = _normalize_line_status(item.get("line_status"))
                if _is_final_line_status(current_line):
                    errors.append(f"{oid}:{idx}: line is already refunded and cannot be changed")
                    continue

                refund_amount = 0.0
                if new_status == "refunded":
                    # Atomically claim this line before crediting the wallet so
                    # two simultaneous refund clicks cannot both issue credit.
                    claim = orders_collection.update_one(
                        {
                            "_id": oid,
                            f"items.{idx}.refunded_at": {"$exists": False},
                            f"items.{idx}.refund_claimed_at": {"$exists": False},
                        },
                        {"$set": {
                            f"items.{idx}.refund_claimed_at": now,
                            f"items.{idx}.refund_claimed_by": actor_admin_id,
                        }},
                    )
                    if not claim.modified_count:
                        errors.append(f"{oid}:{idx}: refund is already processed or in progress")
                        continue
                    refund_amount, refund_error = _credit_line_refund(
                        order,
                        item,
                        idx,
                        reason=reason,
                        actor_admin_id=actor_admin_id,
                        source=refund_source,
                    )
                    if refund_error:
                        orders_collection.update_one(
                            {"_id": oid},
                            {"$unset": {
                                f"items.{idx}.refund_claimed_at": "",
                                f"items.{idx}.refund_claimed_by": "",
                            }},
                        )
                        errors.append(f"{oid}:{idx}: refund failed: {refund_error}")
                        continue

                item["line_status"] = new_status
                set_doc[f"items.{idx}.line_status"] = new_status
                if new_status == "refunded":
                    item["refunded_at"] = now
                    item["refund_amount"] = refund_amount or _money(item.get("refund_amount"), 0.0)
                    item["refunded_by"] = actor_admin_id
                    set_doc[f"items.{idx}.refunded_at"] = now
                    set_doc[f"items.{idx}.refund_amount"] = item["refund_amount"]
                    set_doc[f"items.{idx}.refunded_by"] = actor_admin_id
                    set_doc[f"items.{idx}.refund_claimed_at"] = now
                if api_status:
                    item["api_status"] = api_status
                    set_doc[f"items.{idx}.api_status"] = api_status
                set_doc[f"items.{idx}.provider_status_checked_at"] = now
                any_changed = True
                changed_indices.add(idx)

            if not any_changed:
                continue

            current_status = (order.get("status") or "").lower()
            if current_status != "completed":
                active_statuses = [
                    _normalize_line_status(candidate.get("line_status"))
                    for candidate in items
                    if not _is_export_skipped_or_duplicate(candidate)
                ]
                if active_statuses and all(status == "refunded" for status in active_statuses):
                    new_order_status = "refunded"
                    set_doc["refunded_at"] = now
                elif any(status == "refunded" for status in active_statuses):
                    new_order_status = "partially_refunded"
                    set_doc["partially_refunded_at"] = now
                else:
                    new_order_status = _compute_order_status_from_items(items, current_status=current_status)
                if new_order_status and new_order_status != current_status:
                    set_doc["status"] = new_order_status
                    if new_order_status == "delivered" and not order.get("delivered_at"):
                        set_doc["delivered_at"] = now

            try:
                result = orders_collection.update_one({"_id": oid}, {"$set": set_doc})
            except Exception as primary_error:
                if new_status != "delivered":
                    raise
                # A legacy line may contain a field shape that conflicts with
                # one of the auxiliary dotted updates. Delivery itself must
                # still work, so retry only the essential status fields.
                minimal_set = {"updated_at": now}
                for idx in sorted(changed_indices):
                    minimal_set[f"items.{idx}.line_status"] = new_status
                    if api_status:
                        minimal_set[f"items.{idx}.api_status"] = api_status
                if set_doc.get("status"):
                    minimal_set["status"] = set_doc["status"]
                if set_doc.get("delivered_at"):
                    minimal_set["delivered_at"] = set_doc["delivered_at"]
                try:
                    result = orders_collection.update_one({"_id": oid}, {"$set": minimal_set})
                    _jlog(
                        "admin_order_line_status_minimal_fallback",
                        order_mongo_id=str(oid),
                        new_status=new_status,
                        primary_error=str(primary_error),
                    )
                except Exception as fallback_error:
                    errors.append(f"{oid}: update failed: {fallback_error}")
                    continue

            if getattr(result, "matched_count", 1) == 0:
                errors.append(f"{oid}: order disappeared before update")
                continue
            updated_lines += len(changed_indices)
        except Exception as e:
            errors.append(f"{oid}: {e}")

    if updated_lines:
        bump_orders_cache_version()
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
            delivered_refund = old_status in {"delivered", "completed", "partially_refunded"} and new_status == "refunded"
            if _is_final_order_status(old_status) and not _is_final_order_status(new_status) and not delivered_refund:
                _log_status_blocked(order, new_status, "final_status", reason, source, actor_admin_id)
                errors.append(f"{oid}: delivered orders cannot be reversed")
                continue
            if not _can_transition(old_status, new_status):
                _log_status_blocked(order, new_status, "invalid_transition", reason, source, actor_admin_id)
                errors.append(f"{oid}: invalid transition {old_status} -> {new_status}")
                continue
            update_doc = {"status": new_status, "updated_at": now}
            # Delivered → set delivered_at if missing
            if new_status == "delivered" and not order.get("delivered_at"):
                update_doc["delivered_at"] = now

            # Refunded → credit the normal charged amount, but for store orders
            # credit only the agent-facing base and never the customer markup.
            if new_status == "refunded":
                is_store_order = _is_store_order(order)
                campus_amounts = {
                    idx: _line_refundable_amount(order, item, source)
                    for idx, item in enumerate(order.get("items") or [])
                    if source == "campus" and not _is_export_skipped_or_duplicate(item)
                }
                if source == "campus" and any(amount <= 0 for amount, basis in campus_amounts.values()):
                    errors.append(f"{oid}: campus base amount unresolved or ambiguous; wallet was not refunded")
                    continue
                refundable_total = round(
                    sum(
                        _line_refundable_amount(order, item, source)[0]
                        for item in (order.get("items") or [])
                        if not _is_export_skipped_or_duplicate(item)
                    ),
                    2,
                ) if (is_store_order or source == "campus") else round(_money(order.get("charged_amount"), 0.0), 2)
                if source == "campus":
                    refundable_total = round(sum(amount for amount, basis in campus_amounts.values()), 2)
                user_id = order.get("user_id")
                already_refunded = bool(order.get("refunded_at")) or (old_status == "refunded")
                prior_line_refunds = sum(
                    _money(item.get("refund_amount"), 0.0)
                    for item in (order.get("items") or [])
                    if item.get("refunded_at") or _normalize_line_status(item.get("line_status")) == "refunded"
                )
                refund_due = max(0.0, round(refundable_total - prior_line_refunds, 2))

                if (is_store_order or source == "campus") and refundable_total <= 0 and not already_refunded:
                    label = "campus" if source == "campus" else "store"
                    errors.append(f"{oid}: {label} base amount unavailable; wallet was not refunded")
                    continue

                if source == "campus" and not already_refunded:
                    _provider_credit, provider_error = _credit_campus_provider_refund(
                        order,
                        actor_admin_id=actor_admin_id,
                        reason=reason,
                    )
                    if provider_error:
                        errors.append(f"{oid}: {provider_error}")
                        continue

                if refund_due > 0 and user_id and not already_refunded:
                    wallet_credited = False
                    try:
                        refund_balances_col, refund_transactions_col = _refund_collections(source)
                        refund_balances_col.update_one(
                            {"user_id": user_id},
                            {"$inc": {"amount": refund_due}, "$set": {"updated_at": now}},
                            upsert=True
                        )
                        wallet_credited = True
                        refund_transactions_col.insert_one({
                            "user_id": user_id,
                            "amount": refund_due,
                            "reference": order.get("order_id"),
                            "order_id": order.get("order_id"),
                            "status": "success",
                            "type": "refund",
                            "gateway": "Wallet",
                            "source": "admin_order_refund",
                            "currency": "GHS",
                            "created_at": now,
                            "verified_at": now,
                            "meta": {
                                "note": f"{reason.capitalize()} refund",
                                "order_db_id": oid,
                                "prior_line_refunds": prior_line_refunds,
                                "campus_line_refund_bases": {str(idx): basis for idx, (amount, basis) in campus_amounts.items()},
                                "refund_basis": (
                                    "campus_line_base_amount"
                                    if source == "campus"
                                    else "store_base_amount" if is_store_order else "charged_amount"
                                ),
                                "order_source": source,
                                "store_slug": order.get("store_slug"),
                                "actor_admin_id": actor_admin_id,
                            }
                        })
                    except Exception as e:
                        if wallet_credited:
                            try:
                                refund_balances_col.update_one(
                                    {"user_id": user_id},
                                    {"$inc": {"amount": -refund_due}, "$set": {"updated_at": datetime.utcnow()}},
                                )
                            except Exception as rollback_error:
                                errors.append(f"{oid}: wallet rollback failed: {rollback_error}")
                        errors.append(f"{oid}: refund ledger err: {e}")
                        # A refund is only complete when its wallet credit is
                        # recorded. In particular, Campus/MTN Normal orders
                        # must not display Refunded after a failed credit.
                        continue
                update_doc["refunded_at"] = now
                for idx, item in enumerate(order.get("items") or []):
                    if _is_export_skipped_or_duplicate(item):
                        continue
                    update_doc[f"items.{idx}.line_status"] = "refunded"
                    update_doc[f"items.{idx}.refunded_at"] = item.get("refunded_at") or now
                    update_doc[f"items.{idx}.refund_amount"] = _money(
                        item.get("refund_amount"),
                        _line_refundable_amount(order, item, source)[0],
                    )
                    update_doc[f"items.{idx}.refunded_by"] = actor_admin_id
                    if source == "campus":
                        update_doc[f"items.{idx}.refund_basis"] = campus_amounts[idx][1]

            update_filter = {"_id": oid}
            if not _is_final_order_status(new_status) and not delivered_refund:
                update_filter["status"] = {"$nin": ["delivered", "completed"]}
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

    if updated:
        bump_orders_cache_version()
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
    # Opportunistically process due scheduled jobs on data refresh.
    try:
        _process_due_jobs(max_batch=10)
    except Exception:
        pass

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
    # Opportunistically process due scheduled jobs on page load.
    try:
        _process_due_jobs(max_batch=10)
    except Exception:
        pass

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

    source_filter = _resolve_source_filter(request.args.get("source"), default="main")

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

    auto_update_settings = _get_auto_update_settings_doc()
    auto_update_service_names = _collect_auto_update_service_names()

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
        time_from=(request.args.get("time_from") or "").strip(),
        time_to=(request.args.get("time_to") or "").strip(),
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
        auto_update_settings=auto_update_settings,
        auto_update_service_names=auto_update_service_names,
    )


@admin_orders_bp.route("/admin/orders/auto-update-settings", methods=["POST"])
def save_auto_update_settings():
    if not _require_admin():
        return redirect(url_for("login.login"))

    active = (request.form.get("active") or "").strip().lower() in {"1", "true", "on", "yes"}
    try:
        minutes = int(request.form.get("minutes") or 0)
    except Exception:
        minutes = 0
    minutes = max(0, minutes)

    raw_service_names = request.form.getlist("service_names")
    service_names = []
    seen = set()
    for value in raw_service_names:
        name = _clean_text(value)
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        service_names.append(name)

    if active and minutes <= 0:
        flash("Auto Update minutes must be greater than 0.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    if active and not service_names:
        flash("Choose at least one service for Auto Update.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    auto_update_settings_col.update_one(
        {"_id": AUTO_UPDATE_SETTINGS_ID},
        {
            "$set": {
                "active": active,
                "minutes": minutes,
                "service_names": service_names,
                "updated_at": datetime.utcnow(),
                "updated_by": str(session.get("user_id") or ""),
            }
        },
        upsert=True,
    )

    if active:
        flash(f"Auto Update saved for {len(service_names)} service(s) at {minutes} minute(s).", "success")
    else:
        flash("Auto Update disabled.", "success")

    return redirect(url_for("admin_orders.admin_view_orders"))

@admin_orders_bp.route("/admin/orders/export-catalog", methods=["GET"])
def export_catalog():
    if not _require_admin():
        return jsonify({"ok": False, "message": "unauthorized"}), 401

    try:
        export_services, export_networks, export_service_network_map = _collect_export_catalog()
    except Exception:
        return jsonify({"ok": False, "message": "Could not load export catalog."}), 500

    return jsonify({
        "ok": True,
        "services": export_services,
        "networks": export_networks,
        "service_network_map": export_service_network_map,
    })

@admin_orders_bp.route("/admin/orders/export-undelivered", methods=["POST"])
def export_undelivered_orders():
    if not _require_admin():
        return jsonify({"ok": False, "message": "unauthorized"}), 401

    export_format = (request.form.get("format") or "txt").strip().lower()
    if export_format not in {"txt", "excel", "pdf"}:
        return jsonify({"ok": False, "message": "Choose txt, excel, or pdf format."}), 400

    try:
        rows, meta = _collect_undelivered_export_rows(request.form)
    except ValueError as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400
    except Exception:
        return jsonify({"ok": False, "message": "Could not prepare export right now."}), 500

    if not rows:
        return jsonify({"ok": False, "message": "No undelivered orders matched that service, network, and time range."}), 404

    batch = _save_export_batch(rows, meta, export_format, str(session.get("user_id") or ""))

    if export_format == "txt":
        response = _export_rows_to_txt(rows, meta)
    elif export_format == "excel":
        response = _export_rows_to_excel(rows, meta)
    else:
        response = _export_rows_to_pdf(rows, meta)

    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Export-Count"] = str(meta.get("count") or 0)
    response.headers["X-Export-Batch-Id"] = str(batch.get("_id"))
    response.headers["X-Export-Badge-No"] = str(batch.get("badge_no") or 0)
    return response

@admin_orders_bp.route("/admin/orders/export-batches", methods=["GET"])
def list_export_batches():
    if not _require_admin():
        return jsonify({"ok": False, "message": "unauthorized"}), 401

    batches = []
    try:
        for batch in export_batches_col.find({}).sort([("created_at", -1)]).limit(5):
            batches.append(_serialize_export_batch_summary(batch))
    except Exception:
        return jsonify({"ok": False, "message": "Could not load exported badges."}), 500
    return jsonify({"ok": True, "batches": batches})

@admin_orders_bp.route("/admin/orders/export-batches/<batch_id>", methods=["GET"])
def get_export_batch(batch_id):
    if not _require_admin():
        return jsonify({"ok": False, "message": "unauthorized"}), 401
    try:
        batch = export_batches_col.find_one({"_id": ObjectId(batch_id)})
    except Exception:
        return jsonify({"ok": False, "message": "Invalid exported badge."}), 400
    if not batch:
        return jsonify({"ok": False, "message": "Exported badge not found."}), 404

    details = _load_export_batch_lines(batch)
    payload = _serialize_export_batch_summary(batch)
    payload.update({
        "service_name": batch.get("service_name") or "",
        "timeframe": batch.get("timeframe") or "",
        "date_from": _format_dt(batch.get("date_from")) or "",
        "date_to": _format_dt(batch.get("date_to")) or "",
        "lines": details,
    })
    return jsonify({"ok": True, "batch": payload})

@admin_orders_bp.route("/admin/orders/export-batches/<batch_id>/mark-delivered", methods=["POST"])
def mark_export_batch_delivered(batch_id):
    if not _require_admin():
        return jsonify({"ok": False, "message": "unauthorized"}), 401
    try:
        batch = export_batches_col.find_one({"_id": ObjectId(batch_id)})
    except Exception:
        return jsonify({"ok": False, "message": "Invalid exported badge."}), 400
    if not batch:
        return jsonify({"ok": False, "message": "Exported badge not found."}), 404

    by_source = {"main": [], "campus": []}
    for line in (batch.get("lines") or []):
        parsed = _parse_line_id(line.get("line_id"))
        if not parsed:
            continue
        source, _, _ = parsed
        by_source[source].append(line.get("line_id"))

    updated_total = 0
    errors = []
    for source, line_ids in by_source.items():
        if not line_ids:
            continue
        updated, errs = _apply_line_status_change(
            line_ids,
            "delivered",
            reason="export_badge_mark_delivered",
            actor_admin_id=session.get("user_id"),
            orders_collection=_get_orders_collection(source),
            target_source=source,
        )
        updated_total += updated
        errors += errs

    export_batches_col.update_one(
        {"_id": batch["_id"]},
        {"$set": {
            "last_mark_delivered_at": datetime.utcnow(),
            "last_mark_delivered_by": str(session.get("user_id") or ""),
            "last_mark_delivered_count": updated_total,
        }}
    )

    if updated_total <= 0 and errors:
        return jsonify({"ok": False, "message": "No exported numbers were eligible to mark delivered.", "errors": errors[:5]}), 400
    return jsonify({"ok": True, "updated": updated_total, "errors": errors[:5]})

@admin_orders_bp.route("/admin/orders/export-batches/<batch_id>/mark-delivered-selected", methods=["POST"])
def mark_export_batch_delivered_selected(batch_id):
    if not _require_admin():
        return jsonify({"ok": False, "message": "unauthorized"}), 401
    try:
        batch = export_batches_col.find_one({"_id": ObjectId(batch_id)})
    except Exception:
        return jsonify({"ok": False, "message": "Invalid exported badge."}), 400
    if not batch:
        return jsonify({"ok": False, "message": "Exported badge not found."}), 404

    payload = request.get_json(silent=True) or {}
    selected_line_ids = payload.get("line_ids") or []
    if not isinstance(selected_line_ids, list):
        return jsonify({"ok": False, "message": "Invalid selection."}), 400

    allowed_line_ids = {str(line.get("line_id") or "").strip() for line in (batch.get("lines") or []) if str(line.get("line_id") or "").strip()}
    chosen_line_ids = [str(line_id).strip() for line_id in selected_line_ids if str(line_id).strip() in allowed_line_ids]
    if not chosen_line_ids:
        return jsonify({"ok": False, "message": "Select at least one exported number."}), 400

    by_source = {"main": [], "campus": []}
    for line_id in chosen_line_ids:
        parsed = _parse_line_id(line_id)
        if not parsed:
            continue
        source, _, _ = parsed
        by_source[source].append(line_id)

    updated_total = 0
    errors = []
    for source, line_ids in by_source.items():
        if not line_ids:
            continue
        updated, errs = _apply_line_status_change(
            line_ids,
            "delivered",
            reason="export_badge_mark_delivered_selected",
            actor_admin_id=session.get("user_id"),
            orders_collection=_get_orders_collection(source),
            target_source=source,
        )
        updated_total += updated
        errors += errs

    export_batches_col.update_one(
        {"_id": batch["_id"]},
        {"$set": {
            "last_mark_delivered_at": datetime.utcnow(),
            "last_mark_delivered_by": str(session.get("user_id") or ""),
            "last_mark_delivered_count": updated_total,
            "last_mark_delivered_selected_count": len(chosen_line_ids),
        }}
    )

    if updated_total <= 0 and errors:
        return jsonify({"ok": False, "message": "No selected exported numbers were eligible to mark delivered.", "errors": errors[:5]}), 400
    return jsonify({"ok": True, "updated": updated_total, "errors": errors[:5]})

@admin_orders_bp.route("/admin/orders/<order_id>/items/<int:item_index>/status", methods=["POST"])
def update_order_line_status(order_id, item_index):
    if not _require_admin():
        return redirect(url_for("login.login"))
    correlation_id = uuid.uuid4().hex
    try:
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
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

        line_id = f"{source}:{oid}:{item_index}" if source == ORDER_SOURCE_CAMPUS else f"{oid}:{item_index}"
        updated, errors = _apply_line_status_change(
            [line_id],
            new_status,
            api_status=api_status or None,
            reason="manual_line",
            actor_admin_id=session.get("user_id"),
            orders_collection=orders_collection,
            target_source=source,
        )

        _jlog(
            "admin_order_line_status_update",
            correlation_id=correlation_id,
            order_id=order_id,
            item_index=item_index,
            source=source,
            new_status=new_status,
            updated=updated,
            errors=errors[:3],
        )

        if updated:
            if new_status == "refunded":
                flash(
                    "Campus line refunded; customer wallet and Campus Balance restored from the original debit."
                    if source == "campus" else
                    "Line refunded and agent wallet credited.",
                    "success",
                )
            else:
                flash("Line status updated.", "success")
        elif errors:
            flash(" | ".join(errors[:3]), "warning")
        else:
            flash("No change to line item.", "warning")

        back_to = url_for("admin_orders.admin_view_orders")
        qs = _build_preserved_query(request.args)
        return redirect(f"{back_to}?{qs}" if qs else back_to)
    except Exception as exc:
        _jlog(
            "admin_order_line_status_error",
            correlation_id=correlation_id,
            order_id=order_id,
            item_index=item_index,
            error=str(exc),
        )
        flash(f"Could not update this line. Reference: {correlation_id[:8]}", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

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
        refunded_message = (
            "✅ Campus order refunded; customer wallet and Campus Balance restored from the original debit."
            if source == "campus" else
            "✅ Order marked as Refunded (wallet credited if not already)."
        )
        msg = {
            "processing": "✅ Order marked as Processing.",
            "delivered": "✅ Order marked as Delivered.",
            "failed": "✅ Order marked as Failed.",
            "refunded": refunded_message,
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
