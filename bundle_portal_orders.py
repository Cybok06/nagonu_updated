"""Order persistence and authenticated Bundle Portal delivery callbacks."""
from datetime import datetime, timezone
import hashlib
import hmac
import os
import re
import logging
from threading import Thread

from flask import Blueprint, jsonify, request, session, flash, redirect, url_for
from bson import ObjectId

from bundle_portal import PROVIDERS, call, submit, canonical_network

bundle_portal_bp = Blueprint("bundle_portal", __name__)
FINAL_LINES = {"delivered", "completed", "refunded"}


def refresh_order(collection, order_id):
    # Compare the snapshot to avoid overwriting another line's concurrent result.
    for _ in range(5):
        order = collection.find_one({"order_id": order_id})
        if not order:
            return
        items = order.get("items") or []
        statuses = [i.get("line_status") for i in items if not str(i.get("line_status", "")).startswith("skipped")]
        if not statuses:
            return
        if all(s in {"delivered", "completed"} for s in statuses):
            status = "delivered"
        elif all(s == "refunded" for s in statuses):
            status = "refunded"
        elif all(s in {"failed", "refunded"} for s in statuses):
            status = "failed"
        else:
            status = "processing"
        if order.get("status") in {"completed", "refunded"} or order.get("status") == status:
            return
        fields = {"status": status, "updated_at": datetime.utcnow()}
        if status == "delivered" and not order.get("delivered_at"):
            fields["delivered_at"] = datetime.utcnow()
        result = collection.update_one(
            {"_id": order["_id"], "items": items, "status": order.get("status")},
            {"$set": fields},
        )
        if result.matched_count:
            return


def invalidate_orders():
    from admin_orders import bump_orders_cache_version
    bump_orders_cache_version()


def line_reference(item):
    return item.get("provider_request_order_id") or item.get("provider_order_id")


def phone_key(value):
    digits = re.sub(r"\D", "", str(value or ""))
    if digits.startswith("233") and len(digits) == 12:
        digits = "0" + digits[3:]
    return digits


def event_matches(item, payload):
    return (payload.get("order_id") == line_reference(item)
            and canonical_network(payload.get("network")) == PROVIDERS.get(item.get("provider"))
            and bool(phone_key(item.get("phone")))
            and phone_key(payload.get("recipient")) == phone_key(item.get("phone")))


def settlement_time(payload):
    try:
        value = datetime.fromisoformat(str(payload.get("settled_at") or "").replace("Z", "+00:00"))
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    except ValueError:
        return None


def apply_status_event(collection, order, item, payload):
    """Apply a verified stored event atomically, shared by webhook and reconciliation."""
    events = {"order.completed": "completed", "order.failed": "failed",
              "order.cancelled": "cancelled", "order.refunded": "refunded"}
    event = payload.get("event")
    if (event not in events or payload.get("status") != events[event]
            or not event_matches(item, payload)):
        return False
    previous = item.get("provider_status_payload") or {}
    previous_time, incoming_time = settlement_time(previous), settlement_time(payload)
    if previous_time and incoming_time and incoming_time < previous_time:
        return False
    if previous == payload:
        return False
    target = "delivered" if event == "order.completed" else "failed"
    blocked = list(FINAL_LINES | {"failed"})
    if event == "order.refunded":
        blocked = ["refunded", "completed"]
    result = collection.update_one({"_id": order["_id"], "items": {"$elemMatch": {
        "provider": item["provider"],
        ("provider_request_order_id" if item.get("provider_request_order_id") else "provider_order_id"): payload["order_id"],
        "provider_status_payload": item.get("provider_status_payload") if "provider_status_payload" in item else {"$exists": False},
        "line_status": {"$nin": blocked},

    }}}, {"$set": {
        "items.$.line_status": target,
        "items.$.api_status": "success" if target == "delivered" else "failed",
        "items.$.provider_status": payload["status"],
        "items.$.provider_reference": payload.get("reference"),
        "items.$.provider_status_payload": payload,
        "items.$.provider_status_checked_at": datetime.utcnow(),
        "items.$.refund_required": target == "failed",
        "updated_at": datetime.utcnow(),
    }})
    return bool(result.modified_count)


def process_job(collection, order_id, job):
    reference = job["provider_request_order_id"]
    provider = job["provider"]
    selector = {"order_id": order_id, "items": {"$elemMatch": {
        "provider_request_order_id": reference, "provider": provider,
        "api_status": {"$in": ["queued", "review_required"]}, "line_status": {"$in": ["pending", "processing"]},
    }}}
    claimed = collection.update_one(selector, {"$set": {
        "items.$.api_status": "submitting", "updated_at": datetime.utcnow(),
    }})
    if not claimed.modified_count:
        return
    payload = submit(provider, job.get("phone"), job.get("bundle_portal_gb_size"), reference,
                     retry_purchase=bool(job.get("retry_purchase")))
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        data = {}
    accepted = payload.get("success") is True
    # A timeout/5xx may follow a charged purchase. Keep it for webhook/manual review.
    uncertain = (payload.get("code") in {"unknown_outcome", "recipient_blocked", "not_allowlisted"}
                 or payload.get("http_status", 0) in {409, 429}
                 or payload.get("http_status", 0) >= 500)
    raw_status = data.get("status")
    line_status = "delivered" if accepted and raw_status == "completed" else "processing"
    if (not accepted and not uncertain) or (accepted and raw_status in {"failed", "cancelled", "refunded"}):
        line_status = "failed"
    fields = {
        "items.$.api_status": "success" if accepted else ("review_required" if uncertain else "failed"),
        "items.$.line_status": line_status,
        "items.$.api_response": payload,
        "items.$.provider_reference": data.get("reference"),
        "items.$.provider_order_id": reference,
        "items.$.provider_status": raw_status,
        "items.$.provider_submission_attempted": bool(payload.get("order_attempted")),
        "items.$.refund_required": line_status == "failed",
        "updated_at": datetime.utcnow(),
    }
    # A fast webhook must never be overwritten by the initial HTTP response.
    collection.update_one({"order_id": order_id, "items": {"$elemMatch": {
        "provider_request_order_id": reference, "provider": provider,
        "api_status": "submitting", "line_status": {"$nin": list(FINAL_LINES | {"failed"})},
    }}}, {"$set": fields})
    refresh_order(collection, order_id)
    invalidate_orders()


def finish_callback(collection, order, item, payload):
    try:
        apply_status_event(collection, order, item, payload)
        refresh_order(collection, order["order_id"])
        invalidate_orders()
    except Exception:
        # The authenticated inbox is already committed. The scheduled replay
        # recovers interruptions, including a process restart during this work.
        logging.getLogger(__name__).exception("BundlePortal callback queued for reconciliation")


@bundle_portal_bp.route("/webhooks/bundleportal", methods=["POST"])
def webhook():
    secret = os.getenv("BUNDLE_PORTAL_WEBHOOK_SECRET", "").strip()
    if not secret:
        return jsonify(success=False, error="Webhook is not configured"), 503
    raw = request.get_data()
    signature = request.headers.get("X-BundlePortal-Signature", "")
    expected = "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected.encode(), signature.encode()):
        return jsonify(success=False, error="Invalid signature"), 401
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify(success=False, error="Invalid payload"), 400
    events = {"order.completed": "completed", "order.failed": "failed",
              "order.cancelled": "cancelled", "order.refunded": "refunded"}
    event = payload.get("event")
    if not isinstance(event, str) or event not in events or payload.get("status") != events[event] or not isinstance(payload.get("order_id"), str):
        return jsonify(success=False, error="Invalid order event"), 400
    from db import db
    collection = db["orders"]
    reference = payload["order_id"]
    order = collection.find_one({"items": {"$elemMatch": {
        "$or": [{"provider_request_order_id": reference}, {"provider_order_id": reference}], "provider": {"$in": list(PROVIDERS)},
    }}})
    if not order:
        # One shared Bundle Portal account delivers both sites to this webhook.
        from db import campus_db
        collection = campus_db["orders"]
        order = collection.find_one({"items": {"$elemMatch": {
            "$or": [{"provider_request_order_id": reference}, {"provider_order_id": reference}], "provider": {"$in": list(PROVIDERS)},
        }}})
    item = next((i for i in (order or {}).get("items", [])
                 if line_reference(i) == reference and i.get("provider") in PROVIDERS), None)
    if item and not event_matches(item, payload):
        return jsonify(success=False, error="Order details do not match"), 400
    # A callback can arrive before checkout saves its order. Keep it durably so
    # the periodic reconciler can apply it once the matching line exists.
    db["bundleportal_events"].update_one(
        {"_id": hashlib.sha256(raw).hexdigest()},
        {"$setOnInsert": {"payload": payload, "received_at": datetime.utcnow(),
                          "delivery_id": request.headers.get("X-BundlePortal-Delivery")}}, upsert=True,
    )
    if not order:
        return jsonify(success=True, queued=True), 202
    Thread(target=finish_callback, args=(collection, order, item, payload), daemon=True).start()
    return jsonify(success=True)


@bundle_portal_bp.route("/admin/services/bundleportal/catalog", methods=["GET"])
def catalog():
    if session.get("role") != "admin":
        return jsonify(success=False, error="Unauthorized"), 401
    network = request.args.get("network", "mtn")
    if network not in PROVIDERS.values():
        return jsonify(success=False, error="Invalid Bundle Portal network"), 400
    payload = call("get_bundles", network=network)
    return jsonify(payload), 200 if payload.get("success") else 502


@bundle_portal_bp.route("/admin/orders/<order_id>/items/<int:item_index>/bundleportal-retry", methods=["POST"])
def retry_line(order_id, item_index):
    if session.get("role") != "admin":
        return jsonify(success=False, error="Unauthorized"), 401
    from db import db
    collection = db["orders"]
    try:
        order = collection.find_one({"_id": ObjectId(order_id)})
    except Exception:
        order = None
    items = (order or {}).get("items") or []
    if not order or item_index >= len(items):
        return jsonify(success=False, error="Line not found"), 404
    item = items[item_index]
    if item.get("provider") not in PROVIDERS or item.get("api_status") not in {"queued", "review_required"} or item.get("line_status") not in {"pending", "processing"}:
        return jsonify(success=False, error="Line is not eligible for retry"), 409
    process_job(collection, order["order_id"], {
        "provider": item["provider"], "phone": item.get("phone"),
        "provider_request_order_id": item["provider_request_order_id"],
        "bundle_portal_gb_size": item.get("provider_gb_size"),
        "retry_purchase": item.get("provider_submission_attempted", False),
    })
    flash("Bundle Portal retry processed. Check the line's current status.", "info")
    return redirect(url_for("admin_orders.admin_view_orders", view="lines"))
