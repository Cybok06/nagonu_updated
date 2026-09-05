from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify
import os
import json
import re
import time
import threading
import requests
from db import db
from bson import ObjectId
from typing import Dict, Any, List, Tuple, Optional, Union
from datetime import datetime, timedelta
from withdraw_requests import update_withdraw_request_status

admin_dashboard_bp = Blueprint("admin_dashboard", __name__)

# Collections
orders_col = db["orders"]
users_col = db["users"]
balance_logs_col = db["balance_logs"]          # audit logs to compute deposits/deductions
balances_col = db["balances"]                  # for USER ACCOUNT BALANCE total
afa_col = db["afa_registrations"]
transactions_col = db["transactions"]          # for transaction KPIs

# ✅ Store withdrawal requests collection
store_withdraw_requests_col = db["store_withdraw_requests"]
store_accounts_col = db["store_accounts"]

CODECRAFT_BASE_URL = os.getenv("CODECRAFT_BASE_URL", "https://api.codecraftnetwork.com/api")
CODECRAFT_API_KEY = os.getenv("CODECRAFT_API_KEY")

_CODECRAFT_WALLET_CACHE = {"value": None, "ts": None, "raw": None}
CODECRAFT_WALLET_TTL_SECONDS = 60

DATAKAZINA_BASE_URL = os.getenv(
    "DATAKAZINA_BASE_URL",
    "https://reseller.dakazinabusinessconsult.com/api/v1",
)
DATAKAZINA_API_KEY = os.getenv("DATAKAZINA_API_KEY")
DATAKAZINA_TIMEOUT = int(os.getenv("DATAKAZINA_TIMEOUT", "45"))

_DATAKAZINA_WALLET_CACHE = {"wallet": None, "ts": None, "raw": None}
DATAKAZINA_WALLET_TTL_SECONDS = 60

BUNDLEPORTAL_BASE_URL = os.getenv("BUNDLEPORTAL_BASE_URL", "https://api.bundleportal.com/v1")
BUNDLEPORTAL_API_KEY = os.getenv("BUNDLEPORTAL_API_KEY", "bp_live_3aac2b1cf1fb49c081f598406220c9c2")
BUNDLEPORTAL_AUTH_HEADER = os.getenv("BUNDLEPORTAL_AUTH_HEADER", "x-api-key")
BUNDLEPORTAL_AUTH_PREFIX = os.getenv("BUNDLEPORTAL_AUTH_PREFIX", "")
BUNDLEPORTAL_TIMEOUT = int(os.getenv("BUNDLEPORTAL_TIMEOUT", "45"))
_BUNDLEPORTAL_WALLET_CACHE = {"wallet": None, "currency": "GHS", "ts": None, "raw": None}
BUNDLEPORTAL_WALLET_TTL_SECONDS = 60

DASHBOARD_CACHE_TTL_SECONDS = int(os.getenv("ADMIN_DASHBOARD_CACHE_TTL_SECONDS", "60"))
_redis_client = None


class _MemoryTTLCache:
    def __init__(self, max_items: int = 16):
        self.max_items = max_items
        self._lock = threading.Lock()
        self._store = {}

    def get(self, key: str):
        now = time.time()
        with self._lock:
            rec = self._store.get(key)
            if not rec:
                return None
            expires_at, value = rec
            if expires_at < now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value, ttl_seconds: int):
        expires_at = time.time() + max(1, int(ttl_seconds))
        with self._lock:
            if len(self._store) >= self.max_items and key not in self._store:
                oldest_key = next(iter(self._store.keys()), None)
                if oldest_key is not None:
                    self._store.pop(oldest_key, None)
            self._store[key] = (expires_at, value)


_dashboard_cache = _MemoryTTLCache()


# ----------------------------
# Helpers
# ----------------------------

def jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")


def _clean_api_key(value) -> str:
    if not value:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return "".join(ch for ch in value if 32 <= ord(ch) <= 126).strip()


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


def _cache_get_json(key: str):
    client = _get_redis_client()
    if client:
        try:
            raw = client.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None
    return _dashboard_cache.get(key)


def _cache_set_json(key: str, value, ttl_seconds: int):
    client = _get_redis_client()
    if client:
        try:
            client.setex(key, int(ttl_seconds), json.dumps(value, separators=(",", ":"), ensure_ascii=False))
            return
        except Exception:
            pass
    _dashboard_cache.set(key, value, ttl_seconds)


def _cache_delete(key: str):
    client = _get_redis_client()
    if client:
        try:
            client.delete(key)
        except Exception:
            pass
    _dashboard_cache._store.pop(key, None)


def _users_display_map(user_ids: List[ObjectId]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not user_ids:
        return out
    try:
        for u in users_col.find({"_id": {"$in": user_ids}}, {"username": 1, "name": 1, "phone": 1}):
            disp = (u.get("username") or u.get("name") or u.get("phone") or "").strip()
            if not disp:
                disp = f"User {str(u['_id'])[:6].upper()}"
            out[str(u["_id"])] = disp
    except Exception:
        pass
    return out


def codecraft_get_wallet_balance(refresh: bool = False) -> Dict[str, Any]:
    now = datetime.utcnow()
    ts = _CODECRAFT_WALLET_CACHE.get("ts")
    if not refresh and ts and (now - ts).total_seconds() < CODECRAFT_WALLET_TTL_SECONDS:
        return {
            "ok": True,
            "wallet": _CODECRAFT_WALLET_CACHE.get("value"),
            "cached": True,
            "ts": ts,
            "raw": _CODECRAFT_WALLET_CACHE.get("raw"),
        }

    url = f"{CODECRAFT_BASE_URL.rstrip('/')}/wallet.php"
    headers = {"Accept": "application/json"}
    if CODECRAFT_API_KEY:
        headers["x-api-key"] = CODECRAFT_API_KEY

    try:
        resp = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException:
        return {"ok": False, "message": "Network error"}

    try:
        payload = resp.json()
    except Exception:
        return {"ok": False, "message": "Invalid response"}

    if not isinstance(payload, dict):
        return {"ok": False, "message": "Invalid response"}

    status = str(payload.get("status") or "").strip().lower()
    if status not in {"success", "200"} and not resp.ok:
        return {"ok": False, "message": payload.get("message") or "Failed"}

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        data = {}
    wallet = data.get("wallet")
    try:
        wallet_val = float(wallet)
    except Exception:
        return {"ok": False, "message": "Invalid response"}

    _CODECRAFT_WALLET_CACHE["value"] = wallet_val
    _CODECRAFT_WALLET_CACHE["ts"] = now
    _CODECRAFT_WALLET_CACHE["raw"] = payload
    return {"ok": True, "wallet": wallet_val, "cached": False, "ts": now, "raw": payload}


def datakazina_get_console_balance(force_refresh: bool = False) -> Dict[str, Any]:
    now = datetime.utcnow()
    ts = _DATAKAZINA_WALLET_CACHE.get("ts")
    if not force_refresh and ts and (now - ts).total_seconds() < DATAKAZINA_WALLET_TTL_SECONDS:
        return {
            "ok": True,
            "wallet": _DATAKAZINA_WALLET_CACHE.get("wallet"),
            "cached": True,
            "ts": ts,
            "raw": _DATAKAZINA_WALLET_CACHE.get("raw"),
        }

    api_key = _clean_api_key(DATAKAZINA_API_KEY)
    if not api_key:
        return {"ok": False, "message": "DATAKAZINA API key not configured"}

    url = f"{DATAKAZINA_BASE_URL.rstrip('/')}/check-console-balance"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }

    jlog("datakazina_balance_request", url=url)

    try:
        resp = requests.get(url, headers=headers, timeout=DATAKAZINA_TIMEOUT)
    except requests.RequestException as e:
        jlog("datakazina_balance_error", error=str(e))
        return {"ok": False, "message": "Network error"}

    text = (resp.text or "").strip()
    if not text:
        jlog("datakazina_balance_error", error="empty_body", http_status=resp.status_code)
        return {"ok": False, "message": "Empty response from DataKazina"}

    try:
        payload = resp.json()
    except Exception:
        # Allow plain-number body as a fallback
        try:
            wallet_val = float(text)
        except Exception:
            jlog("datakazina_balance_error", error="invalid_json", http_status=resp.status_code, body_len=len(text))
            return {"ok": False, "message": "Invalid response"}
        _DATAKAZINA_WALLET_CACHE["wallet"] = wallet_val
        _DATAKAZINA_WALLET_CACHE["ts"] = now
        _DATAKAZINA_WALLET_CACHE["raw"] = {"raw": text}
        return {
            "ok": True,
            "wallet": wallet_val,
            "cached": False,
            "ts": now,
            "raw": {"raw": text},
            "http_status": resp.status_code,
        }

    if not isinstance(payload, dict):
        jlog("datakazina_balance_error", error="invalid_payload", http_status=resp.status_code)
        return {"ok": False, "message": "Invalid response"}

    if not resp.ok:
        provider_message = payload.get("message") or payload.get("error") or "DataKazina request failed"
        jlog(
            "datakazina_balance_error",
            error="provider_error",
            http_status=resp.status_code,
            message=provider_message,
        )
        return {
            "ok": False,
            "message": "DataKazina provider error — balance temporarily unavailable.",
            "http_status": resp.status_code,
        }

    wallet_keys = {
        "walletbalance",
        "consolewalletbalance",
        "userwalletbalance",
        "userconsolewalletbalance",
        "balance",
        "wallet",
    }

    def _norm_key(key: Any) -> str:
        return re.sub(r"[^a-z0-9]", "", str(key or "").strip().lower())

    def _find_wallet_value(value: Any) -> Tuple[Any, Optional[str]]:
        if not isinstance(value, dict):
            return None, None
        for key, candidate in value.items():
            if _norm_key(key) in wallet_keys and candidate not in (None, ""):
                return candidate, str(key)
        for candidate in value.values():
            if isinstance(candidate, dict):
                found, found_key = _find_wallet_value(candidate)
                if found_key:
                    return found, found_key
        return None, None

    wallet_raw, wallet_key = _find_wallet_value(payload)
    try:
        wallet_val = float(wallet_raw)
    except Exception:
        jlog(
            "datakazina_balance_error",
            error="invalid_amount",
            payload=payload,
            used_key=wallet_key,
        )
        return {"ok": False, "message": "Invalid response"}

    _DATAKAZINA_WALLET_CACHE["wallet"] = wallet_val
    _DATAKAZINA_WALLET_CACHE["ts"] = now
    _DATAKAZINA_WALLET_CACHE["raw"] = payload

    jlog(
        "datakazina_balance_response",
        ok=True,
        http_status=resp.status_code,
        used_key=wallet_key,
    )
    return {
        "ok": True,
        "wallet": wallet_val,
        "cached": False,
        "ts": now,
        "raw": payload,
        "http_status": resp.status_code,
    }


def bundleportal_get_wallet_balance(force_refresh: bool = False) -> Dict[str, Any]:
    now = datetime.utcnow()
    ts = _BUNDLEPORTAL_WALLET_CACHE.get("ts")
    if not force_refresh and ts and (now - ts).total_seconds() < BUNDLEPORTAL_WALLET_TTL_SECONDS:
        return {
            "ok": True,
            "wallet": _BUNDLEPORTAL_WALLET_CACHE.get("wallet"),
            "currency": _BUNDLEPORTAL_WALLET_CACHE.get("currency") or "GHS",
            "cached": True,
            "ts": ts,
        }

    token = _clean_api_key(BUNDLEPORTAL_API_KEY)
    if not token:
        return {"ok": False, "message": "BundlePortal API key not configured"}
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    headers[BUNDLEPORTAL_AUTH_HEADER] = f"{BUNDLEPORTAL_AUTH_PREFIX.strip()} {token}".strip()
    try:
        resp = requests.post(
            BUNDLEPORTAL_BASE_URL.rstrip("/"),
            headers=headers,
            json={"action": "check_balance"},
            timeout=BUNDLEPORTAL_TIMEOUT,
        )
        payload = resp.json()
    except requests.RequestException:
        return {"ok": False, "message": "Network error"}
    except Exception:
        return {"ok": False, "message": "Invalid response"}

    data = payload.get("data") if isinstance(payload, dict) else None
    if not resp.ok or not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(data, dict):
        return {"ok": False, "message": (payload.get("message") if isinstance(payload, dict) else None) or "BundlePortal request failed"}
    try:
        wallet = float(data.get("wallet_balance"))
    except (TypeError, ValueError):
        return {"ok": False, "message": "Invalid response"}

    currency = str(data.get("currency") or "GHS").strip().upper()
    _BUNDLEPORTAL_WALLET_CACHE.update({"wallet": wallet, "currency": currency, "ts": now, "raw": payload})
    return {"ok": True, "wallet": wallet, "currency": currency, "cached": False, "ts": now}


def top_customers_by_orders(limit: int = 10) -> Tuple[List[str], List[int]]:
    pipeline = [
        {"$match": {"user_id": {"$ne": None}}},
        {"$group": {"_id": "$user_id", "order_count": {"$sum": 1}}},
        {"$sort": {"order_count": -1}},
        {"$limit": int(limit)},
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    obj_ids = [oid for oid in (doc.get("_id") for doc in agg) if isinstance(oid, ObjectId)]
    users_map = _users_display_map(obj_ids)

    labels: List[str] = []
    values: List[int] = []
    for doc in agg:
        uid = doc.get("_id")
        count = int(doc.get("order_count", 0) or 0)
        if isinstance(uid, ObjectId):
            label = users_map.get(str(uid), f"User {str(uid)[:6].upper()}")
        else:
            label = "Unknown"
        labels.append(label)
        values.append(count)
    return labels, values


def top_customers_by_profit(limit: int = 10) -> Tuple[List[str], List[float]]:
    pipeline = [
        {"$match": {"user_id": {"$ne": None}}},
        {"$group": {
            "_id": "$user_id",
            "profit_sum": {"$sum": {"$convert": {"input": {"$ifNull": ["$profit_amount_total", 0]}, "to": "double", "onError": 0, "onNull": 0}}}
        }},
        {"$sort": {"profit_sum": -1}},
        {"$limit": int(limit)},
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    obj_ids = [oid for oid in (doc.get("_id") for doc in agg) if isinstance(oid, ObjectId)]
    users_map = _users_display_map(obj_ids)

    labels: List[str] = []
    values: List[float] = []
    for doc in agg:
        uid = doc.get("_id")
        profit = float(doc.get("profit_sum", 0) or 0)
        if isinstance(uid, ObjectId):
            label = users_map.get(str(uid), f"User {str(uid)[:6].upper()}")
        else:
            label = "Unknown"
        labels.append(label)
        values.append(profit)
    return labels, values


# ✅ FIXED FOREVER: Top offers purchased (safe pipeline; no bracket chaos)
def top_offers_by_purchases(limit: int = 10) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = [
        {"$unwind": "$items"},

        {"$addFields": {
            "service": {"$ifNull": ["$items.serviceName", "Unknown"]},
            "offer_label": {"$ifNull": ["$items.value_obj.label", None]},
            "offer_volume": {"$ifNull": ["$items.value_obj.volume", None]},
            "offer_id": {"$ifNull": ["$items.value_obj.id", None]},
            "offer_value": {"$ifNull": ["$items.value", None]},
            "offer_bundle": {"$ifNull": ["$items.shared_bundle", None]},
        }},

        {"$addFields": {
            "offer_raw": {
                "$ifNull": [
                    {"$cond": [{"$and": [{"$ne": ["$offer_label", None]}, {"$ne": ["$offer_label", ""]}]}, "$offer_label", None]},
                    {"$ifNull": [
                        {"$cond": [{"$and": [{"$ne": ["$offer_volume", None]}, {"$ne": ["$offer_volume", ""]}]}, "$offer_volume", None]},
                        {"$ifNull": [
                            {"$cond": [{"$and": [{"$ne": ["$offer_id", None]}, {"$ne": ["$offer_id", ""]}]}, "$offer_id", None]},
                            {"$ifNull": [
                                {"$cond": [{"$and": [{"$ne": ["$offer_value", None]}, {"$ne": ["$offer_value", ""]}]}, "$offer_value", None]},
                                {"$ifNull": ["$offer_bundle", "N/A"]}
                            ]}
                        ]}
                    ]}
                ]
            }
        }},

        {"$addFields": {"offer": {"$toString": "$offer_raw"}}},

        {"$group": {"_id": {"service": "$service", "offer": "$offer"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
    ]

    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    results: List[Dict[str, Any]] = []
    for doc in agg:
        _id = doc.get("_id") or {}
        results.append({
            "service": (_id.get("service") or "Unknown") or "Unknown",
            "offer": (_id.get("offer") or "N/A"),
            "count": int(doc.get("count", 0) or 0),
        })
    return results


def compute_totals() -> Dict[str, float]:
    pipeline = [{
        "$group": {
            "_id": None,
            "order_count": {"$sum": 1},
            "sum_total_amount": {"$sum": {"$convert": {"input": "$total_amount", "to": "double", "onError": 0, "onNull": 0}}},
            "sum_charged_amount": {"$sum": {"$convert": {"input": "$charged_amount", "to": "double", "onError": 0, "onNull": 0}}},
            "sum_profit_amount": {"$sum": {"$convert": {"input": {"$ifNull": ["$profit_amount_total", 0]}, "to": "double", "onError": 0, "onNull": 0}}},
        }
    }]
    try:
        doc = next(orders_col.aggregate(pipeline), None)
    except Exception:
        doc = None

    return {
        "order_count": int((doc or {}).get("order_count", 0) or 0),
        "sum_total_amount": float((doc or {}).get("sum_total_amount", 0) or 0),
        "sum_charged_amount": float((doc or {}).get("sum_charged_amount", 0) or 0),
        "sum_profit_amount": float((doc or {}).get("sum_profit_amount", 0) or 0),
    }


def compute_customer_counts() -> Dict[str, int]:
    try:
        doc = next(
            users_col.aggregate(
                [
                    {"$match": {"role": "customer"}},
                    {"$addFields": {"status_lc": {"$toLower": {"$toString": {"$ifNull": ["$status", ""]}}}}},
                    {
                        "$group": {
                            "_id": None,
                            "total_customers": {"$sum": 1},
                            "blocked_customers": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$status_lc", "blocked"]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "active_customers": {
                                "$sum": {
                                    "$cond": [
                                        {"$ne": ["$status_lc", "blocked"]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                        }
                    },
                ]
            ),
            None,
        )
    except Exception:
        doc = None
    return {
        "total_customers": int((doc or {}).get("total_customers", 0) or 0),
        "blocked_customers": int((doc or {}).get("blocked_customers", 0) or 0),
        "active_customers": int((doc or {}).get("active_customers", 0) or 0),
    }


def compute_balance_flow_totals() -> Dict[str, float]:
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)
    today_expr = {
        "$and": [
            {"$gte": ["$created_at", start]},
            {"$lt": ["$created_at", end]},
        ]
    }

    try:
        doc = next(
            balance_logs_col.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "deposits_overall": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$action", "deposit"]},
                                        {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}},
                                        0,
                                    ]
                                }
                            },
                            "withdrawals_overall": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$action", "withdraw"]},
                                        {"$abs": {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}}},
                                        0,
                                    ]
                                }
                            },
                            "deposits_today": {
                                "$sum": {
                                    "$cond": [
                                        {"$and": [{"$eq": ["$action", "deposit"]}, today_expr]},
                                        {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}},
                                        0,
                                    ]
                                }
                            },
                            "withdrawals_today": {
                                "$sum": {
                                    "$cond": [
                                        {"$and": [{"$eq": ["$action", "withdraw"]}, today_expr]},
                                        {"$abs": {"$convert": {"input": "$delta", "to": "double", "onError": 0, "onNull": 0}}},
                                        0,
                                    ]
                                }
                            },
                        }
                    }
                ]
            ),
            None,
        )
    except Exception:
        doc = None

    return {
        "deposits_overall": float((doc or {}).get("deposits_overall", 0) or 0),
        "withdrawals_overall": float((doc or {}).get("withdrawals_overall", 0) or 0),
        "deposits_today": float((doc or {}).get("deposits_today", 0) or 0),
        "withdrawals_today": float((doc or {}).get("withdrawals_today", 0) or 0),
    }


def compute_transaction_kpis() -> Dict[str, float]:
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)

    # Use orders (net charged_amount/total_amount) to exclude Paystack fees/overage.
    # Example: charged_amount=4.40, paystack_paid=4.49 -> KPI must show 4.40 (not 4.49).
    # We include BOTH store Paystack checkouts and customer dashboard wallet (from_account)
    # so the Transactions KPI reflects all paid-enough orders.
    paid_statuses = ["processing", "delivered", "success", "completed", "paid"]
    base_match = {
        "$or": [
            # Store-page Paystack orders (require paystack_reference)
            {
                "paid_from": "paystack_inline",
                "paystack_reference": {"$exists": True, "$ne": ""},
                "status": {"$in": paid_statuses},
            },
            # Customer dashboard wallet orders (require at least one successful line item)
            {
                "paid_from": "from_account",
                "status": {"$in": paid_statuses},
                "items": {
                    "$elemMatch": {
                        "$or": [
                            {"api_status": "success"},
                            {"line_status": {"$in": ["delivered", "success"]}},
                        ]
                    }
                },
            },
        ]
    }
    amt_expr = {"$ifNull": ["$charged_amount", "$total_amount"]}
    today_expr = {
        "$and": [
            {"$gte": ["$created_at", start]},
            {"$lt": ["$created_at", end]},
        ]
    }

    try:
        doc = next(
            orders_col.aggregate(
                [
                    {"$match": base_match},
                    {
                        "$group": {
                            "_id": None,
                            "txn_total_count": {"$sum": 1},
                            "txn_total_amount": {
                                "$sum": {
                                    "$convert": {"input": amt_expr, "to": "double", "onError": 0, "onNull": 0}
                                }
                            },
                            "txn_today_count": {
                                "$sum": {"$cond": [today_expr, 1, 0]}
                            },
                            "txn_today_amount": {
                                "$sum": {
                                    "$cond": [
                                        today_expr,
                                        {"$convert": {"input": amt_expr, "to": "double", "onError": 0, "onNull": 0}},
                                        0,
                                    ]
                                }
                            },
                        }
                    },
                ]
            ),
            None,
        )
    except Exception:
        doc = None

    return {
        "txn_total_count": int((doc or {}).get("txn_total_count", 0) or 0),
        "txn_today_count": int((doc or {}).get("txn_today_count", 0) or 0),
        "txn_total_amount": float((doc or {}).get("txn_total_amount", 0) or 0),
        "txn_today_amount": float((doc or {}).get("txn_today_amount", 0) or 0),
    }


@admin_dashboard_bp.route("/admin/api/codecraft/balance", methods=["GET"])
def admin_codecraft_balance():
    if session.get("role") not in ("admin", "superadmin"):
        return jsonify({"success": False, "message": "Not authorized"}), 403

    refresh = request.args.get("refresh") == "1"
    res = codecraft_get_wallet_balance(refresh=refresh)
    if not res.get("ok"):
        return jsonify({"success": False, "message": res.get("message") or "Failed"}), 500

    ts = res.get("ts")
    ts_str = ts.isoformat() + "Z" if isinstance(ts, datetime) else ""
    return jsonify(
        {
            "success": True,
            "wallet": res.get("wallet"),
            "currency": "GHS",
            "cached": bool(res.get("cached")),
            "ts": ts_str,
        }
    ), 200


@admin_dashboard_bp.route("/admin/api/datakazina/balance", methods=["GET"])
def admin_datakazina_balance():
    if session.get("role") not in ("admin", "superadmin"):
        return jsonify({"success": False, "message": "Not authorized"}), 403

    refresh = request.args.get("refresh") == "1"
    res = datakazina_get_console_balance(force_refresh=refresh)
    if not res.get("ok"):
        return jsonify({"success": False, "message": res.get("message") or "Failed"}), 500

    ts = res.get("ts")
    ts_str = ts.isoformat() + "Z" if isinstance(ts, datetime) else ""
    return jsonify(
        {
            "success": True,
            "wallet": res.get("wallet"),
            "currency": "GHS",
            "cached": bool(res.get("cached")),
            "ts": ts_str,
        }
    ), 200


@admin_dashboard_bp.route("/admin/api/bundleportal/balance", methods=["GET"])
def admin_bundleportal_balance():
    if not session.get("admin_logged_in") and session.get("role") not in ("admin", "superadmin"):
        return jsonify({"success": False, "message": "Not authorized"}), 403

    res = bundleportal_get_wallet_balance(force_refresh=request.args.get("refresh") == "1")
    if not res.get("ok"):
        return jsonify({"success": False, "message": res.get("message") or "Failed"}), 500
    ts = res.get("ts")
    return jsonify({
        "success": True,
        "wallet": res.get("wallet"),
        "currency": res.get("currency") or "GHS",
        "cached": bool(res.get("cached")),
        "ts": ts.isoformat() + "Z" if isinstance(ts, datetime) else "",
    }), 200


def compute_user_balances_summary() -> Dict[str, Union[float, int]]:
    try:
        doc = next(balances_col.aggregate([
            {"$group": {
                "_id": None,
                "total_balance_amount": {"$sum": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}},
                "doc_count": {"$sum": 1},
                "positive_count": {"$sum": {"$cond": [
                    {"$gt": [{"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}, 0]}, 1, 0
                ]}}
            }}
        ]), None)
    except Exception:
        doc = None
    return {
        "total_balance_amount": float((doc or {}).get("total_balance_amount", 0) or 0.0),
        "balance_doc_count": int((doc or {}).get("doc_count", 0) or 0),
        "positive_balance_count": int((doc or {}).get("positive_count", 0) or 0),
    }

def compute_store_accounts_outstanding() -> float:
    try:
        doc = next(store_accounts_col.aggregate([
            {"$group": {
                "_id": None,
                "total": {"$sum": {"$convert": {"input": "$total_profit_balance", "to": "double", "onError": 0, "onNull": 0}}}
            }}
        ]), None)
    except Exception:
        doc = None
    return float((doc or {}).get("total", 0) or 0.0)


def compute_afa_kpis() -> Dict[str, int]:
    today = datetime.utcnow().date()
    start = datetime.combine(today, datetime.min.time())
    end = start + timedelta(days=1)
    today_expr = {
        "$and": [
            {"$gte": ["$created_at", start]},
            {"$lt": ["$created_at", end]},
        ]
    }
    try:
        doc = next(
            afa_col.aggregate(
                [
                    {"$addFields": {"status_lc": {"$toLower": {"$toString": {"$ifNull": ["$status", ""]}}}}},
                    {
                        "$group": {
                            "_id": None,
                            "afa_total": {"$sum": 1},
                            "afa_pending": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$status_lc", "pending"]},
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "afa_today": {
                                "$sum": {"$cond": [today_expr, 1, 0]}
                            },
                        }
                    }
                ]
            ),
            None,
        )
    except Exception:
        doc = None
    return {
        "afa_total": int((doc or {}).get("afa_total", 0) or 0),
        "afa_pending": int((doc or {}).get("afa_pending", 0) or 0),
        "afa_today": int((doc or {}).get("afa_today", 0) or 0),
    }


def _day_range(d: datetime.date):
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end


def compute_daily_profits(days_back: int = 6) -> Dict[str, Any]:
    today = datetime.utcnow().date()
    days = [today - timedelta(days=i) for i in range(days_back)][::-1]
    if not days:
        return {
            "labels": [],
            "values": [],
            "today_profit": 0.0,
            "yesterday_profit": 0.0,
            "change_pct": 0.0,
            "trend": "flat",
            "statement": "No data."
        }

    window_start, _ = _day_range(days[0])
    _, window_end = _day_range(days[-1])

    pipeline = [
        {"$match": {"created_at": {"$gte": window_start, "$lt": window_end}}},
        {"$project": {
            "d": {"$dateTrunc": {"date": "$created_at", "unit": "day"}},
            "p": {"$ifNull": ["$profit_amount_total", 0]}
        }},
        {"$group": {"_id": "$d", "profit": {"$sum": {"$convert": {"input": "$p", "to": "double", "onError": 0, "onNull": 0}}}}}
    ]
    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    by_day: Dict[Any, float] = {}
    for row in agg:
        dt = row.get("_id")
        if isinstance(dt, datetime):
            by_day[dt.date()] = float(row.get("profit", 0) or 0)

    labels: List[str] = []
    values: List[float] = []
    for d in days:
        labels.append("Today" if d == today else d.strftime("%b %d"))
        values.append(round(by_day.get(d, 0.0), 2))

    today_profit = values[-1] if values else 0.0
    yesterday_profit = values[-2] if len(values) >= 2 else 0.0

    if yesterday_profit == 0:
        change_pct = 100.0 if today_profit > 0 else 0.0
    else:
        change_pct = ((today_profit - yesterday_profit) / abs(yesterday_profit)) * 100.0

    if abs(today_profit - yesterday_profit) < 1e-9:
        trend = "flat"
        statement = "Today’s profit is the same as yesterday."
    elif today_profit > yesterday_profit:
        trend = "up"
        diff = round(today_profit - yesterday_profit, 2)
        pct = round(change_pct, 2)
        statement = f"Today’s profit has risen by {pct}% compared to yesterday (up GHS {diff:,.2f})."
    else:
        trend = "down"
        diff = round(yesterday_profit - today_profit, 2)
        pct = round(abs(change_pct), 2)
        statement = f"Today’s profit has fallen by {pct}% compared to yesterday (down GHS {diff:,.2f})."

    return {
        "labels": labels,
        "values": values,
        "today_profit": round(today_profit, 2),
        "yesterday_profit": round(yesterday_profit, 2),
        "change_pct": round(change_pct, 2),
        "trend": trend,
        "statement": statement,
    }


def _display_for_actor(actor_id: str, users_map: Dict[str, str], source: str) -> str:
    label = None
    try:
        oid = ObjectId(actor_id)
        label = users_map.get(str(oid))
    except Exception:
        pass
    if not label:
        prefix = "Agent" if source == "agent" else "Customer"
        label = f"{prefix} {actor_id[:6].upper()}"
    return label


def agents_cumulative_sales(limit: int = 10) -> Tuple[List[str], List[float], List[Dict[str, Any]]]:
    pipeline: List[Dict[str, Any]] = [
        {"$unwind": "$items"},
        {"$addFields": {
            "amount_num": {"$convert": {"input": {"$ifNull": ["$items.amount", 0]}, "to": "double", "onError": 0, "onNull": 0}},
            "agent1": {"$ifNull": ["$items.agent_id", None]},
            "agent2": {"$ifNull": ["$items.agentId", None]},
            "agent3": {"$ifNull": ["$items.value_obj.agent_id", None]},
            "agent4": {"$ifNull": ["$items.value_obj.agentId", None]},
        }},
        {"$addFields": {
            "agent_coalesced": {
                "$let": {
                    "vars": {"a1": "$agent1", "a2": "$agent2", "a3": "$agent3", "a4": "$agent4"},
                    "in": {"$ifNull": [
                        {"$cond": [{"$ne": ["$$a1", ""]}, "$$a1", None]},
                        {"$ifNull": [
                            {"$cond": [{"$ne": ["$$a2", ""]}, "$$a2", None]},
                            {"$ifNull": [
                                {"$cond": [{"$ne": ["$$a3", ""]}, "$$a3", None]},
                                {"$cond": [{"$ne": ["$$a4", ""]}, "$$a4", None]}
                            ]}
                        ]}
                    ]}
                }
            }
        }},
        {"$addFields": {
            "actor_id": {"$toString": {"$ifNull": ["$agent_coalesced", "$user_id"]}},
            "actor_source": {"$cond": [{"$ne": ["$agent_coalesced", None]}, "agent", "customer"]}
        }},
        {"$match": {"amount_num": {"$gt": 0}}},
        {"$group": {
            "_id": {"actor_id": "$actor_id", "actor_source": "$actor_source"},
            "total_sales": {"$sum": "$amount_num"},
            "line_count": {"$sum": 1}
        }},
        {"$sort": {"total_sales": -1}},
        {"$limit": int(limit)},
    ]

    try:
        agg = list(orders_col.aggregate(pipeline))
    except Exception:
        agg = []

    to_resolve: List[ObjectId] = []
    for doc in agg:
        actor_id = (doc.get("_id") or {}).get("actor_id")
        try:
            to_resolve.append(ObjectId(actor_id))
        except Exception:
            pass
    users_map = _users_display_map(to_resolve)

    labels: List[str] = []
    values: List[float] = []
    table_rows: List[Dict[str, Any]] = []

    for doc in agg:
        _id = doc.get("_id") or {}
        actor_id = str(_id.get("actor_id"))
        actor_source = _id.get("actor_source")
        total_sales = float(doc.get("total_sales", 0) or 0)
        line_count = int(doc.get("line_count", 0) or 0)

        label = _display_for_actor(actor_id, users_map, actor_source)

        labels.append(label)
        values.append(round(total_sales, 2))
        table_rows.append({
            "agent_id": actor_id,
            "agent": label if actor_source == "agent" else f"{label} (Customer)",
            "sales": round(total_sales, 2),
            "lines": line_count
        })

    return labels, values, table_rows


# ✅ Withdrawal Requests KPI counters
def compute_withdraw_requests_pending() -> int:
    try:
        return int(store_withdraw_requests_col.count_documents({"status": "pending"}))
    except Exception:
        return 0


def compute_withdraw_requests_total_open() -> int:
    # “open” = pending or processing
    try:
        return int(store_withdraw_requests_col.count_documents({"status": {"$in": ["pending", "processing"]}}))
    except Exception:
        return 0


def compute_withdraw_request_counters() -> Dict[str, int]:
    try:
        doc = next(
            store_withdraw_requests_col.aggregate(
                [
                    {"$addFields": {"status_lc": {"$toLower": {"$toString": {"$ifNull": ["$status", ""]}}}}},
                    {
                        "$group": {
                            "_id": None,
                            "pending": {
                                "$sum": {
                                    "$cond": [{"$eq": ["$status_lc", "pending"]}, 1, 0]
                                }
                            },
                            "open": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$in": ["$status_lc", ["pending", "processing"]]
                                        },
                                        1,
                                        0,
                                    ]
                                }
                            },
                        }
                    }
                ]
            ),
            None,
        )
    except Exception:
        doc = None
    return {
        "pending": int((doc or {}).get("pending", 0) or 0),
        "open": int((doc or {}).get("open", 0) or 0),
    }


# ----------------------------
# API for modal (dashboard will call these)
# ----------------------------

@admin_dashboard_bp.route("/admin/withdrawals/list")
def admin_withdrawals_list():
    if not session.get("admin_logged_in"):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    # return latest 50
    try:
        status = (request.args.get("status") or "").strip().lower()
        q = (request.args.get("q") or "").strip()
        limit_raw = request.args.get("limit") or "50"
        offset_raw = request.args.get("offset") or "0"
        try:
            limit = max(1, min(200, int(limit_raw)))
        except Exception:
            limit = 50
        try:
            offset = max(0, int(offset_raw))
        except Exception:
            offset = 0

        query: Dict[str, Any] = {}
        if status == "unpaid":
            query["status"] = {"$in": ["pending", "processing"]}
        elif status:
            query["status"] = status
        if q:
            q_re = {"$regex": q, "$options": "i"}
            query["$or"] = [
                {"store_slug": q_re},
                {"store": q_re},
                {"account": q_re},
                {"msisdn": q_re},
                {"wallet": q_re},
                {"network": q_re},
                {"recipient_name": q_re},
                {"reference": q_re},
                {"method": q_re},
            ]

        docs = list(
            store_withdraw_requests_col.find(query, sort=[("created_at", -1)], limit=limit, skip=offset)
        )
    except Exception:
        docs = []

    def _safe_str(x):
        try:
            return str(x)
        except Exception:
            return ""

    out: List[Dict[str, Any]] = []
    for d in docs:
        out.append({
            "_id": _safe_str(d.get("_id")),
            "reference": d.get("reference") or d.get("ref") or d.get("request_ref") or "",
            "status": (d.get("status") or "pending"),
            "amount": d.get("amount", 0),
            "currency": d.get("currency", "GHS"),
            "owner_id": _safe_str(d.get("owner_id") or d.get("user_id") or ""),
            "store_slug": d.get("store_slug") or d.get("store") or "",
            "method": d.get("method") or d.get("payout_method") or d.get("type") or "",
            "account": d.get("account") or d.get("msisdn") or d.get("wallet") or "",
            "network": d.get("network") or "",
            "recipient_name": d.get("recipient_name") or "",
            "created_at": (d.get("created_at").isoformat() if isinstance(d.get("created_at"), datetime) else ""),
        })
    return jsonify({"ok": True, "items": out})


@admin_dashboard_bp.route("/admin/withdrawals/update", methods=["POST"])
def admin_withdrawals_update():
    if not session.get("admin_logged_in"):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    req_id = (data.get("id") or "").strip()
    new_status = (data.get("status") or "").strip().lower()
    note = (data.get("note") or "").strip()

    ok, payload, code = update_withdraw_request_status(
        req_id=req_id,
        new_status=new_status,
        actor_id=session.get("admin_id") or session.get("user_id") or "admin",
        note=note,
    )
    if ok:
        _cache_delete("admin_dashboard:snapshot:v2")
        return jsonify({"ok": True, **payload}), code
    return jsonify({"ok": False, "error": payload.get("message")}), code


# ----------------------------
# Dashboard Route
# ----------------------------

@admin_dashboard_bp.route("/admin/dashboard")
def admin_dashboard():
    if not session.get("admin_logged_in"):
        return redirect(url_for("login.login"))

    force_refresh = request.args.get("refresh") == "1"
    cache_key = "admin_dashboard:snapshot:v2"

    snapshot = None
    dashboard_cached = False
    if not force_refresh:
        snapshot = _cache_get_json(cache_key)
        dashboard_cached = isinstance(snapshot, dict)

    if not isinstance(snapshot, dict):
        started = time.perf_counter()

        totals = compute_totals()
        bal_summary = compute_user_balances_summary()
        outstanding_payouts = compute_store_accounts_outstanding()
        dp = compute_daily_profits(days_back=6)
        chart_labels, chart_values = top_customers_by_orders(limit=10)
        profit_chart_labels, profit_chart_values = top_customers_by_profit(limit=10)
        top_offers = top_offers_by_purchases(limit=10)
        agent_sales_labels, agent_sales_values, top_agents_rows = agents_cumulative_sales(limit=10)
        cust_counts = compute_customer_counts()
        flow = compute_balance_flow_totals()
        afa_stats = compute_afa_kpis()
        tx = compute_transaction_kpis()
        withdraw_stats = compute_withdraw_request_counters()

        generated_at = datetime.utcnow()
        snapshot = {
            "total_orders": int(totals["order_count"]),
            "sum_total_amount": totals["sum_total_amount"],
            "sum_charged_amount": totals["sum_charged_amount"],
            "sum_profit_amount": totals["sum_profit_amount"],
            "total_user_balance_amount": float(bal_summary["total_balance_amount"]),
            "balance_doc_count": int(bal_summary["balance_doc_count"]),
            "positive_balance_count": int(bal_summary["positive_balance_count"]),
            "outstanding_payouts": outstanding_payouts,
            "withdraw_requests_pending": withdraw_stats["pending"],
            "withdraw_requests_open": withdraw_stats["open"],
            "today_profit": dp["today_profit"],
            "yesterday_profit": dp["yesterday_profit"],
            "profit_change_pct": dp["change_pct"],
            "profit_trend": dp["trend"],
            "profit_statement": dp["statement"],
            "daily_profit_labels": dp["labels"],
            "daily_profit_values": dp["values"],
            "chart_labels": chart_labels,
            "chart_values": chart_values,
            "profit_chart_labels": profit_chart_labels,
            "profit_chart_values": profit_chart_values,
            "agent_sales_labels": agent_sales_labels,
            "agent_sales_values": agent_sales_values,
            "top_agents_rows": top_agents_rows,
            "top_offers": top_offers,
            "total_customers": cust_counts["total_customers"],
            "blocked_customers": cust_counts["blocked_customers"],
            "active_customers": cust_counts["active_customers"],
            "deposits_overall": flow["deposits_overall"],
            "withdrawals_overall": flow["withdrawals_overall"],
            "deposits_today": flow["deposits_today"],
            "withdrawals_today": flow["withdrawals_today"],
            "afa_total": afa_stats["afa_total"],
            "afa_pending": afa_stats["afa_pending"],
            "afa_today": afa_stats["afa_today"],
            "txn_total_count": tx["txn_total_count"],
            "txn_today_count": tx["txn_today_count"],
            "txn_total_amount": tx["txn_total_amount"],
            "txn_today_amount": tx["txn_today_amount"],
            "dashboard_generated_at_iso": generated_at.isoformat() + "Z",
            "dashboard_generated_at_display": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "dashboard_build_ms": round((time.perf_counter() - started) * 1000, 1),
        }
        _cache_set_json(cache_key, snapshot, DASHBOARD_CACHE_TTL_SECONDS)

    # Provider balances are cached separately and should not be frozen inside
    # the heavier dashboard snapshot.
    snapshot = dict(snapshot)
    bundleportal_balance = bundleportal_get_wallet_balance(force_refresh=force_refresh)
    snapshot["bundleportal_wallet"] = bundleportal_balance.get("wallet") if bundleportal_balance.get("ok") else None
    snapshot["bundleportal_balance_error"] = "" if bundleportal_balance.get("ok") else (bundleportal_balance.get("message") or "Unable to fetch")
    snapshot["dashboard_cached"] = dashboard_cached
    snapshot["dashboard_cache_ttl_seconds"] = DASHBOARD_CACHE_TTL_SECONDS
    snapshot["dashboard_refresh_requested"] = force_refresh

    return render_template("admin_dashboard.html", **snapshot)
