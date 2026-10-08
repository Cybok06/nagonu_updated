from bundle_portal import PROVIDERS as BUNDLE_PORTAL_PROVIDERS
import json
import os
import traceback
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Blueprint, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler

from db import db, campus_db

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
# Set to False to completely stop the background status updater from starting.
STATUS_SYNC_ACTIVE = True


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
    if _normalize_status(current_status) == FINAL_STATUS:
        return FINAL_STATUS

    statuses = [_normalize_status(i.get("line_status")) for i in items]
    if not statuses:
        return "processing"

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
        response = requests.get(url, headers=headers, json={"reference_id": reference_id}, timeout=(5, 25))
        payload = _parse(response)
        if _has_status(payload):
            return response.ok, payload
        response = requests.post(url, headers=headers, json={"reference_id": reference_id}, timeout=(5, 25))
        payload = _parse(response)
        return response.ok and _has_status(payload), payload
    except requests.RequestException as exc:
        return False, {"success": False, "message": str(exc)}


def _extract_codecraft_status(payload):
    data = payload.get("data") if isinstance(payload, dict) else None
    return data.get("order_status") if isinstance(data, dict) else None


def _apply_codecraft_status_to_item(item, status_raw, payload, now, order=None):
    status = _normalize_status(status_raw)
    item.update(provider_status=status_raw, provider_status_payload=payload,
                provider_status_checked_at=now)
    if status in {"delivered", "failed", "refunded", "pending", "processing"}:
        item["line_status"] = status


def _run_bundle_portal_status_sync():
    """Reconcile authenticated local callbacks; Bundle Portal v2 cannot be polled."""
    from bundle_portal_orders import apply_status_event, refresh_order, invalidate_orders, line_reference, event_matches, settlement_time
    now = datetime.utcnow()
    summary = {"checked_lines": 0, "updated_lines": 0, "awaiting_webhook": 0}
    active = ["pending", "processing", "queued", "cached"]
    events_col = db["bundleportal_events"]
    for collection in (orders_col, campus_orders_col):
        query = {"items": {"$elemMatch": {
            "provider": {"$in": list(BUNDLE_PORTAL_PROVIDERS)},
        }}}
        for order in collection.find(query):
            for item in order.get("items") or []:
                if item.get("provider") not in BUNDLE_PORTAL_PROVIDERS or item.get("line_status") in {"refunded", "completed"}:
                    continue
                summary["checked_lines"] += 1
                reference = line_reference(item)
                candidates = list(events_col.find({"payload.order_id": reference}).sort("received_at", -1)) if reference else []
                # Invalid or delayed older receipts must not hide a valid settlement.
                candidates = [e for e in candidates
                              if event_matches(item, e.get("payload") or {})
                              and (e.get("payload") or {}).get("status") in {"completed", "failed", "cancelled", "refunded"}
                              and (e.get("payload") or {}).get("event") == "order." + (e.get("payload") or {}).get("status", "")]
                candidates.sort(key=lambda e: (
                    settlement_time(e.get("payload") or {}) or datetime.min.replace(tzinfo=timezone.utc),
                    int((e.get("payload") or {}).get("status") == "refunded")), reverse=True)
                event = candidates[0] if candidates else None
                if not event:
                    summary["awaiting_webhook"] += int(item.get("line_status") in active)
                    continue
                if apply_status_event(collection, order, item, event.get("payload") or {}):
                    summary["updated_lines"] += 1
            # Repair stale parent status even if a previous callback already
            # updated the line before a restart or cache failure.
            before = order.get("status")
            refresh_order(collection, order["order_id"])
            current = collection.find_one({"_id": order["_id"]}, {"status": 1})
            if current and current.get("status") != before:
                summary.setdefault("updated_orders", 0)
                summary["updated_orders"] += 1
    if summary["updated_lines"] or summary.get("updated_orders"):
        invalidate_orders()
    jlog("bundleportal_status_sync_summary", **summary)
    return summary


def _run_order_status_sync():
    now = datetime.utcnow()
    checked_orders = codecraft_checked_orders = updated_orders = updated_lines = 0
    completed_lines = failed_lines = still_processing_lines = skipped_missing_reference_id = 0
    bundleportal_summary = _run_bundle_portal_status_sync()
    cursor = orders_col.find({"items": {"$elemMatch": {
        "provider": "codecraft", "line_status": {"$in": ["pending", "processing", "queued"]},
    }}}).sort("created_at", 1)

    for order in cursor:
        checked_orders += 1

        oid = order.get("_id")
        order_id = order.get("order_id")
        current_status = _normalize_status(order.get("status"))

        if current_status == FINAL_STATUS:
            _log_status_blocked(order, "sync_update", "final_status", "status_sync")
            continue

        original_items = deepcopy(order.get("items", []) or [])
        items = order.get("items", []) or []
        changed = False

        for item in items:
            if _normalize_status(item.get("line_status")) not in {"pending", "processing", "queued"}:
                continue

            provider = item.get("provider")

            # --- CodeCraft (FIXED) ---
            if provider == "codecraft":
                codecraft_checked_orders += 1

                reference_id = item.get("provider_reference") or item.get("provider_order_id") or item.get("provider_request_order_id")
                if not reference_id:
                    skipped_missing_reference_id += 1
                    item["provider_status_checked_at"] = now
                    still_processing_lines += 1
                    changed = True
                    continue

                mode = (item.get("provider_mode") or "regular").strip().lower()
                if mode not in ("regular", "bigtime"):
                    mode = "regular"

                ok, payload = _fetch_codecraft_order_status(reference_id, mode, order_id)
                status_raw = _extract_codecraft_status(payload)

                if status_raw is None:
                    item["provider_status_checked_at"] = now
                    item["provider_status_payload"] = payload
                    still_processing_lines += 1
                    changed = True
                    continue

                _apply_codecraft_status_to_item(item, status_raw, payload, now, order=order)
                changed = True
                updated_lines += 1

                ls = _normalize_status(item.get("line_status"))
                if ls == "delivered":
                    completed_lines += 1
                elif ls == "failed":
                    failed_lines += 1
                else:
                    still_processing_lines += 1

                jlog(
                    "codecraft_line_checked",
                    order_id=order_id,
                    mongo_id=str(oid),
                    reference_id=reference_id,
                    mode=mode,
                    provider_ok=ok,
                    status_raw=status_raw,
                    mapped_line_status=item.get("line_status"),
                )
                continue

        if not changed:
            continue

        new_order_status = _compute_order_status_from_items(items, current_status=current_status)

        update_filter: Dict[str, Any] = {"_id": oid, "items": original_items}
        if new_order_status != FINAL_STATUS:
            update_filter["status"] = {"$ne": FINAL_STATUS}

        res = orders_col.update_one(update_filter, {"$set": {"items": items, "status": new_order_status, "updated_at": now}})

        if res.modified_count:
            updated_orders += 1
        elif new_order_status != FINAL_STATUS:
            _log_status_blocked(order, new_order_status, "db_guard", "status_sync")

        jlog("order_status_sync_updated_order", order_id=order_id, mongo_id=str(oid), new_status=new_order_status)

    summary = {
        "checked_orders": checked_orders,
        "codecraft_checked_orders": codecraft_checked_orders,
        "updated_orders": updated_orders,
        "updated_lines": updated_lines,
        "completed_lines": completed_lines,
        "failed_lines": failed_lines,
        "still_processing_lines": still_processing_lines,
        "skipped_missing_reference_id": skipped_missing_reference_id,
        "timestamp": now.isoformat() + "Z",
        "interval_minutes": 3,
        "bundleportal": bundleportal_summary,
    }

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
            original_items = deepcopy(items)
            changed = False
            changed_lines = 0

            for item in items:
                if item.get("provider") in BUNDLE_PORTAL_PROVIDERS:
                    continue
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
                {"_id": order["_id"], "items": original_items, "status": order.get("status")},
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
