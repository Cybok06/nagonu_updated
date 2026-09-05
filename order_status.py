import json
import os
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Blueprint, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler

from db import db, campus_db
from checkout import (
    BUNDLEPORTAL_API_KEY,
    BUNDLEPORTAL_AUTH_HEADER,
    BUNDLEPORTAL_AUTH_PREFIX,
    BUNDLEPORTAL_BASE_URL,
    BUNDLEPORTAL_TIMEOUT,
    _clean_api_key,
)

order_status_bp = Blueprint("order_status", __name__)

# --- Collections ---
orders_col = db["orders"]
campus_orders_col = campus_db["orders"]
auto_update_settings_col = db["order_auto_update_settings"]

FINAL_STATUS = "delivered"
AUTO_UPDATE_SETTINGS_ID = "AUTO_UPDATE_SETTINGS"

# ===== CodeCraft Provider Config ============================================
CODECRAFT_BASE_URL = os.getenv("CODECRAFT_BASE_URL", "https://api.codecraftnetwork.com/api")
CODECRAFT_API_KEY = (os.getenv("CODECRAFT_API_KEY") or "260109122317-?cZT8C-1AE8bv-LiNnt5-6A8s6Q-4j8kO6").strip()

# ===== Auto status-sync switch ==============================================
# Enabled by default to keep provider-backed orders synchronized automatically.
STATUS_SYNC_ACTIVE = os.getenv("STATUS_SYNC_ACTIVE", "true").strip().lower() not in {
    "0", "false", "no", "off",
}


# ===== Tiny JSON logger ======================================================
def jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")


def _log_status_blocked(order: Dict[str, Any], attempted_status: str, reason: str, source: str):
    jlog(
        "order_status_blocked",
        order_id=order.get("order_id"),
        mongo_id=str(order.get("_id")),
        attempted_status=attempted_status,
        current_status=(order.get("status") or ""),
        reason=reason,
        source=source,
    )


def _log_line_status_blocked(order: Dict[str, Any], item: Dict[str, Any], attempted_status: str, reason: str, source: str):
    jlog(
        "order_line_status_blocked",
        order_id=order.get("order_id"),
        mongo_id=str(order.get("_id")),
        provider=item.get("provider"),
        attempted_status=attempted_status,
        current_status=(item.get("line_status") or ""),
        reason=reason,
        source=source,
    )


def _normalize_status(s: str | None) -> str:
    val = (s or "").strip().lower()
    if val == "completed":
        return "delivered"
    return val


def _service_name_key(name: Any) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _get_auto_update_settings() -> Dict[str, Any]:
    doc = auto_update_settings_col.find_one({"_id": AUTO_UPDATE_SETTINGS_ID}) or {}
    raw_services = doc.get("service_names") or []
    service_names = []
    for name in raw_services:
        key = _service_name_key(name)
        if key and key not in service_names:
            service_names.append(key)
    try:
        minutes = int(doc.get("minutes") or 0)
    except Exception:
        minutes = 0
    return {
        "active": bool(doc.get("active")),
        "minutes": max(0, minutes),
        "service_names": service_names,
    }


def _compute_order_status_from_items(items: List[Dict[str, Any]], current_status: str | None = None) -> str:
    statuses = [_normalize_status(i.get("line_status")) for i in items]
    if not statuses:
        return "processing"

    # Refunds are line-level. A delivered parent must be allowed to become
    # partially_refunded after an administrator refunds one of its lines.
    if all(s == "refunded" for s in statuses):
        return "refunded"

    if any(s == "refunded" for s in statuses):
        return "partially_refunded"

    if _normalize_status(current_status) == FINAL_STATUS:
        return FINAL_STATUS

    if all(s == "delivered" for s in statuses):
        return "delivered"

    if all(s == "pending" for s in statuses):
        return "pending"

    if any(s in {"processing", "queued"} for s in statuses):
        return "processing"

    if all(s == "failed" for s in statuses):
        return "failed"

    return "processing"


# ===== CodeCraft order-status caller (FIXED) =================================
def _fetch_codecraft_order_status(reference_id: str, mode: str, order_id: str | None = None) -> Tuple[bool, Dict[str, Any]]:
    """
    ✅ FIXED to match your working inline checker:

    1) Send GET with JSON body: {"reference_id": "..."}  (NOT query params)
    2) If no usable status, fallback to POST JSON.
    3) Only accept payload as "usable" if it contains data.order_status.
    """
    if not CODECRAFT_API_KEY:
        err = {"success": False, "message": "CODECRAFT API key not configured", "http_status": 500}
        jlog("codecraft_status_config_error", order_id=order_id, reference_id=reference_id)
        return False, err

    m = (mode or "").strip().lower()
    if m not in ("regular", "bigtime"):
        m = "regular"

    endpoint = "response_big_time.php" if m == "bigtime" else "response_regular.php"
    url = f"{CODECRAFT_BASE_URL.rstrip('/')}/{endpoint}"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": CODECRAFT_API_KEY,
    }

    jlog("codecraft_status_request", order_id=order_id, reference_id=reference_id, mode=m, url=url)

    def _parse(resp: requests.Response) -> Dict[str, Any]:
        text = resp.text or ""
        try:
            payload = resp.json() if text.strip() else {}
        except Exception:
            payload = {"raw": text} if text else {}
        if isinstance(payload, dict):
            payload.setdefault("http_status", resp.status_code)
        return payload

    def _has_status(payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        data = payload.get("data")
        return isinstance(data, dict) and data.get("order_status") is not None

    try:
        response = requests.request(
            "GET",
            url,
            headers=headers,
            data=json.dumps({"reference_id": str(reference_id)}),
            timeout=30,
        )
        payload = _parse(response)
        if not _has_status(payload):
            response = requests.post(
                url,
                headers=headers,
                json={"reference_id": str(reference_id)},
                timeout=30,
            )
            payload = _parse(response)
        ok = response.status_code == 200 and bool(payload.get("success")) and _has_status(payload)
        jlog("codecraft_status_response", order_id=order_id, reference_id=reference_id, mode=m, ok=ok, payload=payload)
        return ok, payload
    except requests.RequestException as exc:
        jlog("codecraft_status_network_error", order_id=order_id, reference_id=reference_id, mode=m, error=str(exc))
        return False, {"success": False, "message": str(exc), "http_status": 599}

def _extract_codecraft_status(payload: Dict[str, Any]) -> Optional[str]:
    data = payload.get("data") if isinstance(payload, dict) else None
    value = data.get("order_status") if isinstance(data, dict) else None
    return str(value) if value is not None else None


def _apply_codecraft_status_to_item(
    item: Dict[str, Any],
    status_raw: str,
    payload: Dict[str, Any],
    now: datetime,
    order: Optional[Dict[str, Any]] = None,
) -> None:
    status = (status_raw or "").strip().lower()
    if any(word in status for word in ("success", "completed", "delivered")):
        line_status, api_status = "delivered", "success"
    elif any(word in status for word in ("fail", "error", "reversed", "cancel")):
        line_status, api_status = "failed", "failed"
    elif "pending" in status:
        line_status, api_status = "pending", "pending"
    else:
        line_status, api_status = "processing", "processing"

    if _normalize_status(item.get("line_status")) == FINAL_STATUS and line_status != FINAL_STATUS:
        _log_line_status_blocked(order or {}, item, line_status, "final_line_status", "codecraft_apply")
        line_status, api_status = FINAL_STATUS, item.get("api_status") or "success"

    item.update({
        "line_status": line_status,
        "api_status": api_status,
        "provider_status_last": status_raw,
        "provider_status_checked_at": now,
        "provider_status_payload": payload,
    })


def _fetch_bundleportal_order_status(
    order_reference: str,
    order_id: str | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    token = _clean_api_key(BUNDLEPORTAL_API_KEY)
    if not token:
        error = {"success": False, "message": "BUNDLEPORTAL API key not configured", "http_status": 500}
        jlog("bundleportal_status_config_error", order_id=order_id, order_reference=order_reference)
        return False, error

    header_value = f"{BUNDLEPORTAL_AUTH_PREFIX.strip()} {token}".strip()
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    headers[BUNDLEPORTAL_AUTH_HEADER] = header_value
    body = {"action": "check_status", "order_reference": str(order_reference)}
    jlog("bundleportal_status_request", order_id=order_id, order_reference=order_reference)

    try:
        response = requests.post(
            BUNDLEPORTAL_BASE_URL.rstrip("/"),
            headers=headers,
            json=body,
            timeout=BUNDLEPORTAL_TIMEOUT,
        )
        text = response.text or ""
        try:
            payload = response.json() if text.strip() else {}
        except Exception:
            payload = {"raw": text} if text else {}
        if isinstance(payload, dict):
            payload.setdefault("http_status", response.status_code)
        ok = (
            200 <= response.status_code < 300
            and isinstance(payload, dict)
            and payload.get("success") is True
            and isinstance(payload.get("data"), dict)
        )
        jlog("bundleportal_status_response", order_id=order_id, order_reference=order_reference, ok=ok, payload=payload)
        return ok, payload
    except requests.RequestException as exc:
        jlog("bundleportal_status_network_error", order_id=order_id, order_reference=order_reference, error=str(exc))
        return False, {"success": False, "message": str(exc), "http_status": 599}


def _extract_bundleportal_status(payload: Dict[str, Any]) -> Optional[str]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict) and data.get("status") is not None:
        return str(data.get("status"))
    return None


def _map_bundleportal_status(status_raw: str) -> Tuple[str, str]:
    status = (status_raw or "").strip().lower()
    if status == "completed":
        return "delivered", "success"
    if status == "failed":
        return "failed", "failed"
    if status in {"processing", "cached"}:
        return "processing", status
    return "processing", "processing"


def _apply_bundleportal_status_to_item(
    item: Dict[str, Any],
    status_raw: str,
    payload: Dict[str, Any],
    now: datetime,
    order: Optional[Dict[str, Any]] = None,
) -> None:
    line_status, api_status = _map_bundleportal_status(status_raw)
    if _normalize_status(item.get("line_status")) == FINAL_STATUS and line_status != FINAL_STATUS:
        _log_line_status_blocked(order or {}, item, line_status, "final_line_status", "bundleportal_apply")
        line_status, api_status = FINAL_STATUS, item.get("api_status") or "success"

    data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else {}
    item.update({
        "line_status": line_status,
        "api_status": api_status,
        "provider_status": (status_raw or "").strip().lower(),
        "provider_status_last": status_raw,
        "provider_status_checked_at": now,
        "provider_status_payload": payload,
    })
    if data.get("reference"):
        item["provider_reference"] = data["reference"]
    if data.get("order_id"):
        item["provider_order_id"] = data["order_id"]
    if data.get("failure_reason") is not None:
        item["provider_failure_reason"] = data["failure_reason"]


def _run_order_status_sync_for_collection(collection, source_name: str, now: datetime) -> Dict[str, Any]:
    counters = {
        "checked_orders": 0,
        "codecraft_checked_orders": 0,
        "bundleportal_checked_orders": 0,
        "updated_orders": 0,
        "updated_lines": 0,
        "completed_lines": 0,
        "failed_lines": 0,
        "still_processing_lines": 0,
        "skipped_missing_reference_id": 0,
    }
    cursor = collection.find({
        "$or": [
            {"status": {"$in": ["pending", "processing"]}},
            {"items": {"$elemMatch": {
                "provider": {"$in": ["codecraft", "bundleportal"]},
                "line_status": {"$in": ["pending", "processing", "queued"]},
            }}},
        ]
    }).sort("created_at", -1).limit(50)

    for order in cursor:
        counters["checked_orders"] += 1
        oid = order.get("_id")
        order_id = order.get("order_id")
        current_status = _normalize_status(order.get("status"))
        if current_status == FINAL_STATUS:
            _log_status_blocked(order, "sync_update", "final_status", f"{source_name}_status_sync")
            continue

        items = order.get("items", []) or []
        changed = False
        for item in items:
            if _normalize_status(item.get("line_status")) not in {"pending", "processing", "queued"}:
                continue

            provider = (item.get("provider") or "").strip().lower()
            reference = None
            payload: Dict[str, Any] = {}
            status_raw: Optional[str] = None

            if provider == "codecraft":
                counters["codecraft_checked_orders"] += 1
                reference = item.get("provider_reference") or item.get("provider_order_id") or item.get("provider_request_order_id")
                if reference:
                    mode = (item.get("provider_mode") or "regular").strip().lower()
                    ok, payload = _fetch_codecraft_order_status(str(reference), mode, order_id)
                    status_raw = _extract_codecraft_status(payload) if ok else None
            elif provider == "bundleportal":
                counters["bundleportal_checked_orders"] += 1
                # BundlePortal checks our idempotency order ID, not its KT-* reference.
                reference = item.get("provider_order_id") or item.get("provider_request_order_id")
                if reference:
                    ok, payload = _fetch_bundleportal_order_status(str(reference), order_id)
                    status_raw = _extract_bundleportal_status(payload) if ok else None
            else:
                continue

            item["provider_status_checked_at"] = now
            changed = True
            if not reference:
                counters["skipped_missing_reference_id"] += 1
                counters["still_processing_lines"] += 1
                continue
            if status_raw is None:
                item["provider_status_payload"] = payload
                counters["still_processing_lines"] += 1
                continue

            if provider == "codecraft":
                _apply_codecraft_status_to_item(item, status_raw, payload, now, order=order)
            else:
                _apply_bundleportal_status_to_item(item, status_raw, payload, now, order=order)
            counters["updated_lines"] += 1

            line_status = _normalize_status(item.get("line_status"))
            if line_status == "delivered":
                counters["completed_lines"] += 1
            elif line_status == "failed":
                counters["failed_lines"] += 1
            else:
                counters["still_processing_lines"] += 1
            jlog(
                f"{provider}_line_checked",
                source=source_name,
                order_id=order_id,
                mongo_id=str(oid),
                order_reference=reference,
                status_raw=status_raw,
                mapped_line_status=item.get("line_status"),
            )

        if not changed:
            continue

        new_order_status = _compute_order_status_from_items(items, current_status=current_status)
        update_filter: Dict[str, Any] = {"_id": oid}
        if new_order_status != FINAL_STATUS:
            update_filter["status"] = {"$ne": FINAL_STATUS}
        result = collection.update_one(
            update_filter,
            {"$set": {"items": items, "status": new_order_status, "updated_at": now}},
        )
        if result.modified_count:
            counters["updated_orders"] += 1
        elif new_order_status != FINAL_STATUS:
            _log_status_blocked(order, new_order_status, "db_guard", f"{source_name}_status_sync")

    summary = {
        **counters,
        "timestamp": now.isoformat() + "Z",
        "interval_minutes": 3,
    }
    jlog("order_status_collection_sync_summary", source=source_name, **summary)
    return summary


def _run_order_status_sync() -> Dict[str, Any]:
    now = datetime.utcnow()
    main_summary = _run_order_status_sync_for_collection(orders_col, "main", now)
    campus_summary = _run_order_status_sync_for_collection(campus_orders_col, "campus", now)
    counter_names = (
        "checked_orders",
        "codecraft_checked_orders",
        "bundleportal_checked_orders",
        "updated_orders",
        "updated_lines",
        "completed_lines",
        "failed_lines",
        "still_processing_lines",
        "skipped_missing_reference_id",
    )
    summary = {
        name: int(main_summary.get(name, 0)) + int(campus_summary.get(name, 0))
        for name in counter_names
    }
    summary.update({
        "main_checked_orders": int(main_summary.get("checked_orders", 0)),
        "main_updated_orders": int(main_summary.get("updated_orders", 0)),
        "campus_checked_orders": int(campus_summary.get("checked_orders", 0)),
        "campus_updated_orders": int(campus_summary.get("updated_orders", 0)),
        "timestamp": now.isoformat() + "Z",
        "interval_minutes": 3,
    })
    jlog("order_status_sync_summary", **summary)
    return summary


def _run_auto_deliver_updates() -> Dict[str, Any]:
    now = datetime.utcnow()
    settings = _get_auto_update_settings()

    summary = {
        "active": settings["active"],
        "minutes": settings["minutes"],
        "selected_services": settings["service_names"],
        "checked_orders": 0,
        "updated_orders": 0,
        "updated_lines": 0,
        "main_updated_orders": 0,
        "campus_updated_orders": 0,
        "timestamp": now.isoformat() + "Z",
    }

    if not settings["active"] or settings["minutes"] <= 0 or not settings["service_names"]:
        jlog("auto_update_summary", **summary)
        return summary

    cutoff = now - timedelta(minutes=settings["minutes"])
    selected = set(settings["service_names"])

    def _apply_for_collection(collection, source_name: str) -> None:
        cursor = collection.find(
            {
                "created_at": {"$lte": cutoff},
                "$or": [
                    {"status": {"$in": ["pending", "processing"]}},
                    {
                        "items": {
                            "$elemMatch": {
                                "line_status": {"$in": ["pending", "processing", "queued"]},
                            }
                        }
                    },
                ],
            },
            {"items": 1, "status": 1, "order_id": 1, "created_at": 1},
        ).sort("created_at", 1)

        for order in cursor:
            summary["checked_orders"] += 1
            current_order_status = _normalize_status(order.get("status"))
            if current_order_status not in {"pending", "processing"}:
                continue
            items = order.get("items", []) or []
            changed = False
            changed_lines = 0

            for item in items:
                current_line = _normalize_status(item.get("line_status"))
                if current_line not in {"pending", "processing", "queued"}:
                    continue
                if _service_name_key(item.get("serviceName")) not in selected:
                    continue

                item["line_status"] = "delivered"
                if not item.get("api_status"):
                    item["api_status"] = "auto_delivered"
                item["auto_delivered_at"] = now
                item["auto_deliver_rule_minutes"] = settings["minutes"]
                changed = True
                changed_lines += 1

            if not changed:
                continue

            new_order_status = _compute_order_status_from_items(
                items,
                current_status=current_order_status,
            )
            res = collection.update_one(
                {"_id": order["_id"]},
                {"$set": {"items": items, "status": new_order_status, "updated_at": now}},
            )

            if res.modified_count:
                summary["updated_orders"] += 1
                summary["updated_lines"] += changed_lines
                if source_name == "campus":
                    summary["campus_updated_orders"] += 1
                else:
                    summary["main_updated_orders"] += 1
                jlog(
                    "auto_update_order_updated",
                    source=source_name,
                    order_id=order.get("order_id"),
                    mongo_id=str(order.get("_id")),
                    updated_lines=changed_lines,
                    new_status=new_order_status,
                    minutes=settings["minutes"],
                )

    _apply_for_collection(orders_col, "main")
    _apply_for_collection(campus_orders_col, "campus")

    jlog("auto_update_summary", **summary)
    return summary


def _scheduled_auto_update_job():
    try:
        jlog("auto_update_scheduled_run_start")
        summary = _run_auto_deliver_updates()
        jlog("auto_update_scheduled_run_done", **summary)
    except Exception:
        jlog("auto_update_scheduled_run_error", error=traceback.format_exc())


# ===== Route: manual sync ====================================================
@order_status_bp.route("/order-status-sync", methods=["GET"])
def sync_order_status():
    try:
        summary = _run_order_status_sync()
        return jsonify({"success": True, "summary": summary}), 200
    except Exception:
        jlog("order_status_sync_uncaught", error=traceback.format_exc())
        return jsonify({"success": False, "message": "Server error"}), 500


# ===== Background schedulers ================================================
def _scheduled_sync_job():
    try:
        jlog("order_status_scheduled_run_start")
        summary = _run_order_status_sync()
        jlog("order_status_scheduled_run_done", **summary)
    except Exception:
        jlog("order_status_scheduled_run_error", error=traceback.format_exc())


status_sync_scheduler = None
auto_update_scheduler = None

if STATUS_SYNC_ACTIVE:
    status_sync_scheduler = BackgroundScheduler(timezone="UTC")
    status_sync_scheduler.add_job(
        _scheduled_sync_job,
        "interval",
        minutes=3,
        max_instances=1,
        coalesce=True,
        id="order_status_sync",
    )

    try:
        status_sync_scheduler.start()
        jlog("order_status_scheduler_started", interval_minutes=3)
    except Exception:
        jlog("order_status_scheduler_start_failed", error=traceback.format_exc())

auto_update_scheduler = BackgroundScheduler(timezone="UTC")
auto_update_scheduler.add_job(
    _scheduled_auto_update_job,
    "interval",
    minutes=1,
    max_instances=1,
    coalesce=True,
    id="order_auto_update",
)

try:
    auto_update_scheduler.start()
    jlog("auto_update_scheduler_started", interval_minutes=1)
except Exception:
    jlog("auto_update_scheduler_start_failed", error=traceback.format_exc())
