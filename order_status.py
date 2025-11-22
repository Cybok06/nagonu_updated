from flask import Blueprint, jsonify
from bson import ObjectId
from datetime import datetime, timedelta
import requests, json, traceback, os

from db import db

# Background scheduler
from apscheduler.schedulers.background import BackgroundScheduler

order_status_bp = Blueprint("order_status", __name__)

# --- Collections ---
orders_col = db["orders"]

# ===== DataVerse Provider Config (HARDCODED to match checkout) ==============
DATAVERSE_BASE_URL = "https://dataversegh.pro/wp-json/custom/v1"
DATAVERSE_USERNAME = "Nyebro"
DATAVERSE_PASSWORD = "EWzk0g0xetm7c5g2rDjB9opR"

# ===== Portal-02 Provider Config (MIRRORS checkout.py) ======================
PORTAL02_BASE_URL = "https://www.portal-02.com/api/v1"
PORTAL02_API_KEY = os.getenv("PORTAL02_API_KEY", "dk_mJmQDFQWmDId4RT_c5HrEghcgwujPAFf")

# ===== Tiny JSON logger (same style as checkout) ============================
def jlog(event: str, **kv):
    rec = {"evt": event, **kv}
    try:
        print(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        print(f"[LOG_FALLBACK] {event} {kv}")


# ===== DataVerse order-status caller ========================================
def _fetch_dataverse_order_status(order_reference: str, order_id: str | None = None):
    """
    Call DataVerse order-status endpoint:
      GET {BASE}/order-status?order_reference=<ref>

    Returns: (ok, payload)
      ok = True when HTTP 200 and payload.status == "success" or true
    """
    if not DATAVERSE_USERNAME or not DATAVERSE_PASSWORD:
        err = {
            "status": "error",
            "message": "DATAVERSE credentials not configured",
            "http_status": 500,
        }
        jlog(
            "dataverse_status_config_error",
            order_id=order_id,
            order_reference=order_reference,
        )
        return False, err

    url = f"{DATAVERSE_BASE_URL.rstrip('/')}/order-status"
    params = {"order_reference": order_reference}

    jlog(
        "dataverse_status_request",
        order_id=order_id,
        order_reference=order_reference,
        url=url,
        params=params,
    )

    try:
        resp = requests.get(
            url,
            params=params,
            auth=(DATAVERSE_USERNAME, DATAVERSE_PASSWORD),
            timeout=30,
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

        jlog(
            "dataverse_status_response",
            order_id=order_id,
            order_reference=order_reference,
            ok=ok,
            payload=payload,
        )

        return ok, payload

    except requests.RequestException as e:
        jlog(
            "dataverse_status_network_error",
            order_id=order_id,
            order_reference=order_reference,
            error=str(e),
        )
        return False, {"status": "error", "message": str(e), "http_status": 599}


# ===== Portal-02 order-status caller ========================================
def _fetch_portal02_order_status(identifier: str, order_id: str | None = None):
    """
    Call Portal-02 order-status endpoint:

      GET {BASE}/order/status/:identifier

    Where :identifier can be:
      - orderId (e.g. ORD-000067)
      - reference (e.g. ORD-IB22OQws)

    Returns: (ok, payload)
      ok = True when HTTP 200/201 and payload.success == True
    """
    if not PORTAL02_API_KEY or PORTAL02_API_KEY == "dk_your_api_key_here":
        err = {
            "success": False,
            "error": "PORTAL02 API key not configured",
            "type": "CONFIG_ERROR",
            "http_status": 500,
        }
        jlog(
            "portal02_status_config_error",
            order_id=order_id,
            identifier=identifier,
        )
        return False, err

    url = f"{PORTAL02_BASE_URL.rstrip('/')}/order/status/{identifier}"

    jlog(
        "portal02_status_request",
        order_id=order_id,
        identifier=identifier,
        url=url,
    )

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": PORTAL02_API_KEY,
    }

    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=30,
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

        jlog(
            "portal02_status_response",
            order_id=order_id,
            identifier=identifier,
            ok=ok,
            payload=payload,
        )

        return ok, payload

    except requests.RequestException as e:
        jlog(
            "portal02_status_network_error",
            order_id=order_id,
            identifier=identifier,
            error=str(e),
        )
        return False, {"success": False, "error": str(e), "http_status": 599}


# ===== Helper: decide item + order status ===================================
def _compute_order_status_from_items(items):
    statuses = [str(i.get("line_status") or "").lower() for i in items]
    if not statuses:
        return "processing"

    if all(s == "completed" for s in statuses):
        return "completed"

    if any(s == "processing" for s in statuses):
        return "processing"

    if all(s == "failed" for s in statuses):
        return "failed"

    # mixed / unknown → keep as processing for safety
    return "processing"


# ===== Core sync logic (used by route + scheduler) ==========================
def _run_order_status_sync():
    """
    Internal function that:
      - finds all orders with Dataverse *or* Portal-02 items in 'processing'
      - calls provider order-status for each such line
      - updates MongoDB
    Returns a summary dict.
    """
    now = datetime.utcnow()

    # Find orders where any provider line is still processing
    cursor = orders_col.find(
        {
            "items": {
                "$elemMatch": {
                    "provider": {"$in": ["dataverse", "portal02"]},
                    "line_status": "processing",
                }
            }
        }
    )

    checked_orders = 0
    updated_orders = 0
    updated_lines = 0
    completed_lines = 0
    failed_lines = 0
    still_processing_lines = 0

    for order in cursor:
        checked_orders += 1
        oid = order.get("_id")
        order_id = order.get("order_id")

        items = order.get("items", []) or []
        changed = False

        new_items = []
        for item in items:
            provider = item.get("provider")

            # ---- Dataverse processing lines ---------------------------------
            if (
                provider == "dataverse"
                and str(item.get("line_status", "")).lower() == "processing"
            ):
                # Prefer the exact ref we sent as "order_id" during place-order
                order_ref = (
                    item.get("provider_request_order_id")
                    or item.get("provider_reference")
                )

                if not order_ref:
                    # No reference: cannot sync status; just mark the check time
                    item["provider_status_checked_at"] = now
                    new_items.append(item)
                    still_processing_lines += 1
                    continue

                ok, payload = _fetch_dataverse_order_status(order_ref, order_id)

                provider_status = None
                if isinstance(payload, dict):
                    provider_status = (
                        payload.get("order_status") or payload.get("status")
                    )

                status_str = str(provider_status or "").lower()

                # Track raw info
                item["provider_status_last"] = provider_status
                item["provider_status_checked_at"] = now
                item["provider_status_payload"] = payload

                # Decide line_status + api_status based on provider status
                if ok and status_str in ("completed", "success"):
                    item["line_status"] = "completed"
                    item["api_status"] = "success"
                    completed_lines += 1
                elif ok and status_str in ("failed", "error", "cancelled", "canceled"):
                    item["line_status"] = "failed"
                    item["api_status"] = "failed"
                    failed_lines += 1
                else:
                    # pending / unknown → remain in processing
                    item["line_status"] = "processing"
                    item["api_status"] = "processing"
                    still_processing_lines += 1

                changed = True
                new_items.append(item)
                continue

            # ---- Portal-02 processing lines ---------------------------------
            if (
                provider == "portal02"
                and str(item.get("line_status", "")).lower() == "processing"
            ):
                # Prefer provider_order_id (ORD-xxxxx), else reference
                identifier = (
                    item.get("provider_order_id")
                    or item.get("provider_reference")
                )

                if not identifier:
                    # No identifier: cannot sync status; just mark the check time
                    item["provider_status_checked_at"] = now
                    new_items.append(item)
                    still_processing_lines += 1
                    continue

                ok, payload = _fetch_portal02_order_status(identifier, order_id)

                provider_status = None
                provider_order_data = {}
                if isinstance(payload, dict):
                    # Portal-02 wraps details under "order"
                    provider_order_data = payload.get("order") or {}
                    provider_status = provider_order_data.get("status")

                status_str = str(provider_status or "").lower()

                # Track raw info
                item["provider_status_last"] = provider_status
                item["provider_status_checked_at"] = now
                item["provider_status_payload"] = payload

                # Map Portal-02 statuses to our internal statuses:
                # pending, processing, delivered, failed, cancelled, refunded, resolved
                if ok and status_str in ("delivered", "resolved", "success", "completed"):
                    item["line_status"] = "completed"
                    item["api_status"] = "success"
                    completed_lines += 1
                elif ok and status_str in ("failed", "cancelled", "canceled", "refunded", "error"):
                    item["line_status"] = "failed"
                    item["api_status"] = "failed"
                    failed_lines += 1
                else:
                    # pending / processing / unknown → keep as processing
                    item["line_status"] = "processing"
                    item["api_status"] = "processing"
                    still_processing_lines += 1

                changed = True
                new_items.append(item)
                continue

            # ---- Non-provider or already-final lines ------------------------
            new_items.append(item)

        # If nothing changed in items, skip order update
        if not changed:
            continue

        # Recompute overall order.status from line statuses
        new_order_status = _compute_order_status_from_items(new_items)

        update_doc = {
            "items": new_items,
            "status": new_order_status,
            "updated_at": now,
        }

        orders_col.update_one({"_id": oid}, {"$set": update_doc})
        updated_orders += 1
        updated_lines += 1  # at least one line changed in this order

        jlog(
            "order_status_sync_updated_order",
            order_id=order_id,
            mongo_id=str(oid),
            new_status=new_order_status,
        )

    summary = {
        "checked_orders": checked_orders,
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

    Useful for testing or an admin "Sync Now" button.
    """
    try:
        summary = _run_order_status_sync()
        return jsonify({"success": True, "summary": summary}), 200
    except Exception:
        jlog("order_status_sync_uncaught", error=traceback.format_exc())
        return jsonify({"success": False, "message": "Server error"}), 500


# ===== Background scheduler: run every 15 minutes ===========================
def _scheduled_sync_job():
    """
    Job called by APScheduler every 15 minutes.
    """
    try:
        jlog("order_status_scheduled_run_start")
        summary = _run_order_status_sync()
        jlog("order_status_scheduled_run_done", **summary)
    except Exception:
        jlog("order_status_scheduled_run_error", error=traceback.format_exc())


# Create and start scheduler once when module is imported
scheduler = BackgroundScheduler(timezone="UTC")

# Run every 15 minutes, coalescing missed runs, ensuring only one at a time
scheduler.add_job(
    _scheduled_sync_job,
    "interval",
    minutes=15,
    max_instances=1,
    coalesce=True,
    id="provider_order_status_sync",  # updated name (covers Dataverse + Portal-02)
)

try:
    scheduler.start()
    jlog("order_status_scheduler_started", interval_minutes=15)
except Exception:
    # If APScheduler fails (e.g., in some environments), log but do not crash app
    jlog("order_status_scheduler_start_failed", error=traceback.format_exc())
