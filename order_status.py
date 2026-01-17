from __future__ import annotations

import os
import json
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Blueprint, jsonify, request

from db import db

# Background scheduler
from apscheduler.schedulers.background import BackgroundScheduler

order_status_bp = Blueprint("order_status", __name__)

# --- Collections ---
orders_col = db["orders"]
FINAL_STATUS = "completed"

# ===== Portal-02 Provider Config ============================================
PORTAL02_BASE_URL = "https://www.portal-02.com/api/v1"
PORTAL02_API_KEY = os.getenv("PORTAL02_API_KEY", "dk_mJmQDFQWmDId4RT_c5HrEghcgwujPAFf")

# ===== DataConnect Provider Config ==========================================
DATACONNECT_BASE_URL = "https://dataconnectgh.com/api/v1"
DATACONNECT_API_KEY = os.getenv(
    "DATACONNECT_API_KEY",
    "90bcf2f236b8c95547b58b531f5c597df8a061a8",
)

# ===== Tiny JSON logger (same style as checkout) ============================
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


# ===== Portal-02 order-status caller ========================================
def _fetch_portal02_order_status(order_key: str, order_id: str | None = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Call Portal-02 order-status endpoint:
      GET {BASE}/order/status/<order_key>

    Returns: (ok, payload)
      ok = True when HTTP 200 and payload.success is True
    """
    if not PORTAL02_API_KEY or PORTAL02_API_KEY == "dk_your_api_key_here":
        err = {
            "success": False,
            "message": "PORTAL02 API key not configured",
            "http_status": 500,
        }
        jlog("portal02_status_config_error", order_id=order_id, order_key=order_key)
        return False, err

    url = f"{PORTAL02_BASE_URL.rstrip('/')}/order/status/{order_key}"
    headers = {"Accept": "application/json", "x-api-key": PORTAL02_API_KEY}

    jlog(
        "portal02_status_request",
        order_id=order_id,
        order_key=order_key,
        url=url,
    )

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        text = resp.text or ""
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": text} if text else {}

        ok = (
            resp.status_code == 200
            and isinstance(payload, dict)
            and bool(payload.get("success")) is True
        )
        if isinstance(payload, dict):
            payload.setdefault("http_status", resp.status_code)

        jlog(
            "portal02_status_response",
            order_id=order_id,
            order_key=order_key,
            ok=ok,
            payload=payload,
        )

        return ok, payload
    except requests.RequestException as e:
        jlog(
            "portal02_status_network_error",
            order_id=order_id,
            order_key=order_key,
            error=str(e),
        )
        return False, {"success": False, "message": str(e), "http_status": 599}


# ===== DataConnect order-status caller ======================================
def _fetch_dataconnect_order_status(
    transaction_id: str, order_id: str | None = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Call DataConnect transaction-status endpoint:
      POST {BASE}/fetch-other-network-transaction
      body: {"transaction_id": "<provider_reference>"}
    """
    if not DATACONNECT_API_KEY:
        err = {
            "success": False,
            "message": "DATACONNECT API key not configured",
            "http_status": 500,
        }
        jlog("dataconnect_status_config_error", order_id=order_id, transaction_id=transaction_id)
        return False, err

    url = f"{DATACONNECT_BASE_URL.rstrip('/')}/fetch-other-network-transaction"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": DATACONNECT_API_KEY,
    }
    payload = {"transaction_id": transaction_id}

    jlog(
        "dataconnect_status_request",
        order_id=order_id,
        transaction_id=transaction_id,
        url=url,
    )

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        text = resp.text or ""
        try:
            data = resp.json()
        except Exception:
            data = {"raw": text} if text else {}

        ok = (
            resp.status_code == 200
            and isinstance(data, dict)
            and bool(data.get("success")) is True
        )
        if isinstance(data, dict):
            data.setdefault("http_status", resp.status_code)

        jlog(
            "dataconnect_status_response",
            order_id=order_id,
            transaction_id=transaction_id,
            ok=ok,
            payload=data,
        )

        return ok, data
    except requests.RequestException as e:
        jlog(
            "dataconnect_status_network_error",
            order_id=order_id,
            transaction_id=transaction_id,
            error=str(e),
        )
        return False, {"success": False, "message": str(e), "http_status": 599}


# ===== Helper: decide item + order status ===================================
def _compute_order_status_from_items(items: List[Dict[str, Any]], current_status: str | None = None) -> str:
    if (current_status or "").lower() == FINAL_STATUS:
        return FINAL_STATUS
    statuses = [str(i.get("line_status") or "").lower() for i in items]
    if not statuses:
        return "processing"

    if all(s == "completed" for s in statuses):
        return "completed"

    if any(s == "processing" for s in statuses):
        return "processing"

    if all(s == "failed" for s in statuses):
        return "failed"

    # mixed / unknown -> keep as processing for safety
    return "processing"


def _map_portal02_status(status_raw: str) -> Tuple[str, str]:
    s = (status_raw or "").strip().lower()
    if s in {"delivered", "resolved"}:
        return "completed", "success"
    if s in {"failed", "cancelled", "canceled", "refunded"}:
        return "failed", "failed"
    if s in {"pending", "processing"}:
        return "processing", "processing"
    return "processing", "processing"


def _map_dataconnect_status(status_raw: str) -> Tuple[str, str]:
    s = (status_raw or "").strip().lower()
    if s in {"success", "successful", "completed", "delivered", "delivered_successfully"}:
        return "completed", "success"
    if s in {"failed", "fail", "error", "reversed", "cancelled", "canceled"}:
        return "failed", "failed"
    if s in {"pending", "processing", "queued", "initiated", "in_progress"}:
        return "processing", "processing"
    return "processing", "processing"


def _extract_portal02_status(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    order = payload.get("order") if isinstance(payload.get("order"), dict) else payload
    if isinstance(order, dict):
        v = order.get("status")
        if v is not None:
            return str(v)
    return None


def _extract_dataconnect_status(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("status", "transaction_status", "transactionStatus"):
        v = payload.get(key)
        if v is not None:
            return str(v)
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("status", "transaction_status", "transactionStatus"):
            v = data.get(key)
            if v is not None:
                return str(v)
    tx = payload.get("transaction")
    if isinstance(tx, dict):
        for key in ("status", "transaction_status", "transactionStatus"):
            v = tx.get(key)
            if v is not None:
                return str(v)
    return None


def _apply_status_to_item(
    item: Dict[str, Any],
    status_raw: str,
    payload: Dict[str, Any],
    now: datetime,
    order: Optional[Dict[str, Any]] = None,
) -> None:
    line_status, api_status = _map_portal02_status(status_raw)
    current_line = str(item.get("line_status") or "").lower()
    if current_line == FINAL_STATUS and line_status != FINAL_STATUS:
        _log_line_status_blocked(order or {}, item, line_status, "final_line_status", "portal02_apply")
        line_status = FINAL_STATUS
        api_status = item.get("api_status") or "success"
    item["line_status"] = line_status
    item["api_status"] = api_status
    item["provider_status_last"] = status_raw
    item["provider_status_checked_at"] = now
    item["provider_status_payload"] = payload


def _apply_dataconnect_status_to_item(
    item: Dict[str, Any],
    status_raw: str,
    payload: Dict[str, Any],
    now: datetime,
    order: Optional[Dict[str, Any]] = None,
) -> None:
    line_status, api_status = _map_dataconnect_status(status_raw)
    current_line = str(item.get("line_status") or "").lower()
    if current_line == FINAL_STATUS and line_status != FINAL_STATUS:
        _log_line_status_blocked(order or {}, item, line_status, "final_line_status", "dataconnect_apply")
        line_status = FINAL_STATUS
        api_status = item.get("api_status") or "success"
    item["line_status"] = line_status
    item["api_status"] = api_status
    item["provider_status_last"] = status_raw
    item["provider_status_checked_at"] = now
    item["provider_status_payload"] = payload


def _match_keys_for_item(item: Dict[str, Any], keys: List[str]) -> bool:
    if not keys:
        return False
    for k in keys:
        if k and (
            item.get("provider_order_id") == k
            or item.get("provider_reference") == k
            or item.get("provider_request_order_id") == k
        ):
            return True
    return False


def _is_mtn_express_item(item: Dict[str, Any]) -> bool:
    name = (item.get("serviceName") or "").strip().lower()
    return name == "mtn express"


# ===== Core sync logic (used by route + scheduler) ==========================
def _run_order_status_sync() -> Dict[str, Any]:
    """
    Internal function that:
      - finds all orders with Portal-02 items in 'processing'
      - calls Portal-02 order-status for each line
      - updates MongoDB
    Returns a summary dict.
    """
    now = datetime.utcnow()

    cursor = orders_col.find(
        {
            "items": {
                "$elemMatch": {
                    "provider": "portal02",
                    "line_status": "processing",
                }
            }
        }
    )

    checked_orders = 0
    dataconnect_checked_orders = 0
    updated_orders = 0
    updated_lines = 0
    completed_lines = 0
    failed_lines = 0
    still_processing_lines = 0

    for order in cursor:
        checked_orders += 1
        oid = order.get("_id")
        order_id = order.get("order_id")
        current_status = (order.get("status") or "").lower()
        if current_status == FINAL_STATUS:
            _log_status_blocked(order, "sync_update", "final_status", "portal02_sync")
            continue

        items = order.get("items", []) or []
        changed = False

        for item in items:
            if (
                item.get("provider") != "portal02"
                or str(item.get("line_status", "")).lower() != "processing"
            ):
                continue

            order_key = (
                item.get("provider_order_id")
                or item.get("provider_reference")
                or item.get("provider_request_order_id")
            )
            if not order_key:
                item["provider_status_checked_at"] = now
                still_processing_lines += 1
                continue

            ok, payload = _fetch_portal02_order_status(order_key, order_id)
            status_raw = _extract_portal02_status(payload)

            if status_raw is None:
                item["provider_status_checked_at"] = now
                item["provider_status_payload"] = payload
                still_processing_lines += 1
                changed = True
                continue

            _apply_status_to_item(item, status_raw, payload, now, order=order)
            changed = True

            line_status = item.get("line_status")
            if line_status == "completed":
                completed_lines += 1
            elif line_status == "failed":
                failed_lines += 1
            else:
                still_processing_lines += 1

        if not changed:
            continue

        new_order_status = _compute_order_status_from_items(items, current_status=current_status)
        update_filter = {"_id": oid}
        if new_order_status != FINAL_STATUS:
            update_filter["status"] = {"$ne": FINAL_STATUS}
        res = orders_col.update_one(
            update_filter,
            {"$set": {"items": items, "status": new_order_status, "updated_at": now}},
        )
        if res.modified_count:
            updated_orders += 1
            updated_lines += 1
        elif new_order_status != FINAL_STATUS:
            _log_status_blocked(order, new_order_status, "db_guard", "portal02_sync")

        jlog(
            "order_status_sync_updated_order",
            order_id=order_id,
            mongo_id=str(oid),
            new_status=new_order_status,
        )

    # DataConnect (MTN EXPRESS only): latest 50 orders
    dataconnect_cursor = (
        orders_col.find(
            {
                "items": {
                    "$elemMatch": {
                        "provider": "dataconnect",
                        "line_status": "processing",
                        "serviceName": {"$regex": r"^\s*mtn\s+express\s*$", "$options": "i"},
                    }
                }
            }
        )
        .sort("created_at", -1)
        .limit(50)
    )

    for order in dataconnect_cursor:
        dataconnect_checked_orders += 1
        oid = order.get("_id")
        order_id = order.get("order_id")
        current_status = (order.get("status") or "").lower()
        if current_status == FINAL_STATUS:
            _log_status_blocked(order, "sync_update", "final_status", "dataconnect_sync")
            continue

        items = order.get("items", []) or []
        changed = False

        for item in items:
            if (
                item.get("provider") != "dataconnect"
                or str(item.get("line_status", "")).lower() != "processing"
                or not _is_mtn_express_item(item)
            ):
                continue

            transaction_id = (
                item.get("provider_reference")
                or item.get("provider_order_id")
                or item.get("provider_request_order_id")
            )
            if not transaction_id:
                item["provider_status_checked_at"] = now
                still_processing_lines += 1
                continue

            ok, payload = _fetch_dataconnect_order_status(transaction_id, order_id)
            status_raw = _extract_dataconnect_status(payload)

            if status_raw is None:
                item["provider_status_checked_at"] = now
                item["provider_status_payload"] = payload
                still_processing_lines += 1
                changed = True
                continue

            _apply_dataconnect_status_to_item(item, status_raw, payload, now, order=order)
            changed = True

            line_status = item.get("line_status")
            if line_status == "completed":
                completed_lines += 1
            elif line_status == "failed":
                failed_lines += 1
            else:
                still_processing_lines += 1

        if not changed:
            continue

        new_order_status = _compute_order_status_from_items(items, current_status=current_status)
        update_filter = {"_id": oid}
        if new_order_status != FINAL_STATUS:
            update_filter["status"] = {"$ne": FINAL_STATUS}
        res = orders_col.update_one(
            update_filter,
            {"$set": {"items": items, "status": new_order_status, "updated_at": now}},
        )
        if res.modified_count:
            updated_orders += 1
            updated_lines += 1
        elif new_order_status != FINAL_STATUS:
            _log_status_blocked(order, new_order_status, "db_guard", "dataconnect_sync")

        jlog(
            "order_status_sync_updated_order_dataconnect",
            order_id=order_id,
            mongo_id=str(oid),
            new_status=new_order_status,
        )

    summary = {
        "checked_orders": checked_orders,
        "dataconnect_checked_orders": dataconnect_checked_orders,
        "updated_orders": updated_orders,
        "updated_lines_est": updated_lines,
        "completed_lines": completed_lines,
        "failed_lines": failed_lines,
        "still_processing_lines": still_processing_lines,
        "timestamp": now.isoformat() + "Z",
    }

    jlog("order_status_sync_summary", **summary)
    return summary


# ===== Route: manual sync (for testing / admin button) ======================
@order_status_bp.route("/order-status-sync", methods=["GET"])
def sync_order_status():
    """
    Manual trigger:
      - /order-status-sync
    """
    try:
        summary = _run_order_status_sync()
        return jsonify({"success": True, "summary": summary}), 200
    except Exception:
        jlog("order_status_sync_uncaught", error=traceback.format_exc())
        return jsonify({"success": False, "message": "Server error"}), 500


# ===== Webhook: Portal-02 order status updates ==============================
@order_status_bp.route("/webhooks/portal02", methods=["POST"])
def portal02_webhook():
    """
    Receives Portal-02 webhook notifications.
    """
    now = datetime.utcnow()
    payload = request.get_json(silent=True) or {}
    order_id = payload.get("orderId") or payload.get("order_id")
    reference = payload.get("reference")
    status_raw = payload.get("status")
    keys = [k for k in (order_id, reference) if k]

    if not keys or not status_raw:
        return jsonify({"success": False, "message": "orderId/reference and status required"}), 400

    q_or = []
    for k in keys:
        q_or.append({"provider_order_id": k})
        q_or.append({"provider_reference": k})
        q_or.append({"provider_request_order_id": k})

    cursor = orders_col.find(
        {"items": {"$elemMatch": {"provider": "portal02", "$or": q_or}}}
    )

    updated_orders = 0
    updated_lines = 0

    for order in cursor:
        current_status = (order.get("status") or "").lower()
        if current_status == FINAL_STATUS:
            _log_status_blocked(order, "webhook_update", "final_status", "portal02_webhook")
            continue
        items = order.get("items", []) or []
        changed = False
        for item in items:
            if item.get("provider") != "portal02":
                continue
            if not _match_keys_for_item(item, keys):
                continue
            _apply_status_to_item(item, status_raw, payload, now, order=order)
            if order_id and not item.get("provider_order_id"):
                item["provider_order_id"] = order_id
            if reference and not item.get("provider_reference"):
                item["provider_reference"] = reference
            changed = True
            updated_lines += 1

        if not changed:
            continue

        new_order_status = _compute_order_status_from_items(items, current_status=current_status)
        update_filter = {"_id": order["_id"]}
        if new_order_status != FINAL_STATUS:
            update_filter["status"] = {"$ne": FINAL_STATUS}
        res = orders_col.update_one(
            update_filter,
            {"$set": {"items": items, "status": new_order_status, "updated_at": now}},
        )
        if res.modified_count:
            updated_orders += 1
        elif new_order_status != FINAL_STATUS:
            _log_status_blocked(order, new_order_status, "db_guard", "portal02_webhook")

    jlog(
        "portal02_webhook_processed",
        order_id=order_id,
        reference=reference,
        status=status_raw,
        updated_orders=updated_orders,
        updated_lines=updated_lines,
    )

    return jsonify({"success": True}), 200


# ===== Manual status check (Portal-02) ======================================
@order_status_bp.route("/portal02/order-status/<order_key>", methods=["GET"])
def portal02_order_status(order_key: str):
    """
    Fetches latest Portal-02 status and updates matching line(s) in DB.
    """
    now = datetime.utcnow()
    order_key = (order_key or "").strip()
    if not order_key:
        return jsonify({"success": False, "message": "order_key required"}), 400

    ok, payload = _fetch_portal02_order_status(order_key, order_id=None)
    if not ok:
        return jsonify({"success": False, "payload": payload}), 502

    status_raw = _extract_portal02_status(payload)
    if not status_raw:
        return jsonify({"success": False, "message": "Missing status in response"}), 502

    q_or = [
        {"provider_order_id": order_key},
        {"provider_reference": order_key},
        {"provider_request_order_id": order_key},
    ]

    cursor = orders_col.find(
        {"items": {"$elemMatch": {"provider": "portal02", "$or": q_or}}}
    )

    updated_orders = 0
    updated_lines = 0

    for order in cursor:
        current_status = (order.get("status") or "").lower()
        if current_status == FINAL_STATUS:
            _log_status_blocked(order, "manual_status_update", "final_status", "portal02_manual")
            continue
        items = order.get("items", []) or []
        changed = False
        for item in items:
            if item.get("provider") != "portal02":
                continue
            if not _match_keys_for_item(item, [order_key]):
                continue
            _apply_status_to_item(item, status_raw, payload, now, order=order)
            changed = True
            updated_lines += 1

        if not changed:
            continue

        new_order_status = _compute_order_status_from_items(items, current_status=current_status)
        update_filter = {"_id": order["_id"]}
        if new_order_status != FINAL_STATUS:
            update_filter["status"] = {"$ne": FINAL_STATUS}
        res = orders_col.update_one(
            update_filter,
            {"$set": {"items": items, "status": new_order_status, "updated_at": now}},
        )
        if res.modified_count:
            updated_orders += 1
        elif new_order_status != FINAL_STATUS:
            _log_status_blocked(order, new_order_status, "db_guard", "portal02_manual")

    return jsonify(
        {
            "success": True,
            "status": status_raw,
            "updated_orders": updated_orders,
            "updated_lines": updated_lines,
            "payload": payload,
        }
    ), 200


# ===== Background scheduler: run every 1 minute ============================
def _scheduled_sync_job():
    try:
        jlog("order_status_scheduled_run_start")
        summary = _run_order_status_sync()
        jlog("order_status_scheduled_run_done", **summary)
    except Exception:
        jlog("order_status_scheduled_run_error", error=traceback.format_exc())


scheduler = BackgroundScheduler(timezone="UTC")

scheduler.add_job(
    _scheduled_sync_job,
    "interval",
    minutes=1,
    max_instances=1,
    coalesce=True,
    id="portal02_order_status_sync",
)

try:
    scheduler.start()
    jlog("order_status_scheduler_started", interval_minutes=1)
except Exception:
    jlog("order_status_scheduler_start_failed", error=traceback.format_exc())
