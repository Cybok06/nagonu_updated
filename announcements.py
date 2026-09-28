from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from bson import ObjectId
from flask import Blueprint, flash, jsonify, redirect, render_template, request, session, url_for

from db import db


announcements_bp = Blueprint("announcements", __name__)
announcements_col = db["announcements"]
announcement_acks_col = db["announcement_acknowledgements"]
VALID_LOCATIONS = {"customer_dashboard", "store"}
_indexes_ready = False


def _ensure_indexes() -> None:
    global _indexes_ready
    if _indexes_ready:
        return
    try:
        announcements_col.create_index(
            [("active", 1), ("locations", 1), ("created_at", -1)],
            name="announcement_popup_lookup",
            background=True,
        )
        announcement_acks_col.create_index(
            [("announcement_id", 1), ("viewer_id", 1)],
            name="unique_announcement_viewer_ack",
            unique=True,
            background=True,
        )
        _indexes_ready = True
    except Exception:
        pass


def _oid(value: Any) -> Optional[ObjectId]:
    try:
        return ObjectId(str(value))
    except Exception:
        return None


def _admin_required() -> bool:
    return session.get("role") == "admin"


def _view(doc: Dict[str, Any]) -> Dict[str, Any]:
    created = doc.get("created_at")
    return {
        "_id_str": str(doc.get("_id")),
        "title": str(doc.get("title") or "Announcement"),
        "message": str(doc.get("message") or ""),
        "locations": list(doc.get("locations") or []),
        "active": bool(doc.get("active", True)),
        "created_at": created,
        "created_at_fmt": created.strftime("%d %b %Y, %I:%M %p") if isinstance(created, datetime) else "",
    }


def get_popup_announcement(location: str, viewer_id: Any = None) -> Optional[Dict[str, Any]]:
    if location not in VALID_LOCATIONS:
        return None
    _ensure_indexes()
    viewer_oid = _oid(viewer_id)
    try:
        doc = announcements_col.find_one(
            {"active": True, "locations": location},
            {"title": 1, "message": 1, "locations": 1, "active": 1, "created_at": 1},
            sort=[("created_at", -1)],
        )
        if not doc:
            return None
        if viewer_oid and announcement_acks_col.find_one(
            {"announcement_id": doc["_id"], "viewer_id": viewer_oid}, {"_id": 1}
        ):
            return None
        return _view(doc)
    except Exception:
        return None
    return None


@announcements_bp.route("/admin/announcements", methods=["GET"])
def manage_announcements():
    if not _admin_required():
        return redirect(url_for("login.login"))
    _ensure_indexes()
    docs = list(announcements_col.find({}).sort("created_at", -1).limit(100))
    return render_template("admin_announcements.html", announcements=[_view(doc) for doc in docs])


@announcements_bp.route("/admin/announcements/create", methods=["POST"])
def create_announcement():
    if not _admin_required():
        return redirect(url_for("login.login"))
    title = " ".join((request.form.get("title") or "").split()).strip()[:100]
    message = (request.form.get("message") or "").strip()[:2000]
    locations = [value for value in request.form.getlist("locations") if value in VALID_LOCATIONS]
    if not title or not message:
        flash("Enter an announcement title and message.", "danger")
    elif not locations:
        flash("Select Customer Dashboard, Store, or both.", "danger")
    else:
        announcements_col.insert_one({
            "title": title,
            "message": message,
            "locations": locations,
            "active": True,
            "created_at": datetime.utcnow(),
            "created_by": str(session.get("user_id") or ""),
        })
        flash("Announcement published successfully.", "success")
    return redirect(url_for("announcements.manage_announcements"))


@announcements_bp.route("/admin/announcements/<announcement_id>/toggle", methods=["POST"])
def toggle_announcement(announcement_id: str):
    if not _admin_required():
        return redirect(url_for("login.login"))
    oid = _oid(announcement_id)
    doc = announcements_col.find_one({"_id": oid}) if oid else None
    if doc:
        announcements_col.update_one(
            {"_id": oid},
            {"$set": {"active": not bool(doc.get("active", True)), "updated_at": datetime.utcnow()}},
        )
        flash("Announcement status updated.", "success")
    return redirect(url_for("announcements.manage_announcements"))


@announcements_bp.route("/admin/announcements/<announcement_id>/delete", methods=["POST"])
def delete_announcement(announcement_id: str):
    if not _admin_required():
        return redirect(url_for("login.login"))
    oid = _oid(announcement_id)
    if oid:
        announcements_col.delete_one({"_id": oid})
        announcement_acks_col.delete_many({"announcement_id": oid})
        flash("Announcement deleted.", "success")
    return redirect(url_for("announcements.manage_announcements"))


@announcements_bp.route("/api/announcements/<announcement_id>/acknowledge", methods=["POST"])
def acknowledge_announcement(announcement_id: str):
    viewer_oid = _oid(session.get("user_id"))
    announcement_oid = _oid(announcement_id)
    if not viewer_oid or not announcement_oid:
        return jsonify({"success": False}), 400
    if not announcements_col.find_one({"_id": announcement_oid, "active": True}, {"_id": 1}):
        return jsonify({"success": False}), 404
    announcement_acks_col.update_one(
        {"announcement_id": announcement_oid, "viewer_id": viewer_oid},
        {"$setOnInsert": {
            "announcement_id": announcement_oid,
            "viewer_id": viewer_oid,
            "acknowledged_at": datetime.utcnow(),
        }},
        upsert=True,
    )
    return jsonify({"success": True})
