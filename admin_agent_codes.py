from __future__ import annotations

import math
import re
from urllib.parse import urlencode

from bson import ObjectId
from datetime import datetime

from flask import Blueprint, redirect, render_template, request, session, url_for

from db import db


admin_agent_codes_bp = Blueprint("admin_agent_codes", __name__)

agent_codes_col = db["agent_codes"]
users_col = db["users"]


def _require_admin() -> bool:
    return session.get("role") == "admin"


def _object_id_from_any(value):
    if isinstance(value, ObjectId):
        return value
    try:
        return ObjectId(str(value))
    except Exception:
        return None


@admin_agent_codes_bp.route("/admin/agent-codes")
def admin_agent_codes():
    if not _require_admin():
        return redirect(url_for("login.login"))

    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = 50

    filters = []
    if status in {"active", "inactive"}:
        filters.append({"status": status})

    if q:
        rx = {"$regex": re.escape(q), "$options": "i"}
        matching_user_ids = [
            u["_id"]
            for u in users_col.find(
                {
                    "role": "customer",
                    "$or": [
                        {"first_name": rx},
                        {"last_name": rx},
                        {"username": rx},
                        {"phone": rx},
                        {"email": rx},
                        {"business_name": rx},
                    ],
                },
                {"_id": 1},
            ).limit(500)
        ]
        filters.append(
            {
                "$or": [
                    {"agent_code": rx},
                    {"id": rx},
                    {"user_id": {"$in": matching_user_ids}},
                ]
            }
        )

    query = {"$and": filters} if filters else {}
    total = agent_codes_col.count_documents(query)
    total_pages = max(math.ceil(total / per_page), 1)
    if page > total_pages:
        page = total_pages
    skip = (page - 1) * per_page

    rows = list(
        agent_codes_col.find(query)
        .sort([("created_at", -1), ("agent_code", 1)])
        .skip(skip)
        .limit(per_page)
    )

    user_ids = [_object_id_from_any(row.get("user_id")) for row in rows]
    user_ids = [uid for uid in user_ids if uid]
    users = {
        u["_id"]: u
        for u in users_col.find(
            {"_id": {"$in": user_ids}},
            {
                "first_name": 1,
                "last_name": 1,
                "username": 1,
                "phone": 1,
                "email": 1,
                "business_name": 1,
                "stage_label": 1,
                "status": 1,
            },
        )
    }

    for row in rows:
        row["_user"] = users.get(_object_id_from_any(row.get("user_id"))) or {}

    qs = request.args.to_dict(flat=True)
    qs.pop("page", None)
    base_qs = urlencode(qs)

    total_active = agent_codes_col.count_documents({"status": "active"})
    total_inactive = agent_codes_col.count_documents({"status": "inactive"})

    return render_template(
        "admin_agent_codes.html",
        rows=rows,
        q=q,
        status=status,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        total_active=total_active,
        total_inactive=total_inactive,
        base_qs=base_qs,
    )


@admin_agent_codes_bp.route("/admin/agent-codes/<code_id>/status", methods=["POST"])
def update_agent_code_status(code_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    code_oid = _object_id_from_any(code_id)
    new_status = (request.form.get("status") or "").strip().lower()
    if not code_oid or new_status not in {"active", "inactive"}:
        return redirect(url_for("admin_agent_codes.admin_agent_codes"))

    agent_codes_col.update_one(
        {"_id": code_oid},
        {
            "$set": {
                "status": new_status,
                "updated_at": datetime.utcnow(),
                "status_updated_by": session.get("admin_id") or session.get("user_id"),
            }
        },
    )

    return redirect(
        url_for(
            "admin_agent_codes.admin_agent_codes",
            q=(request.form.get("q") or "").strip(),
            status=(request.form.get("current_status") or "").strip(),
            page=(request.form.get("page") or "1").strip(),
        )
    )


@admin_agent_codes_bp.route("/admin/agent-codes/deactivate-all", methods=["POST"])
def deactivate_all_agent_codes():
    if not _require_admin():
        return redirect(url_for("login.login"))

    agent_codes_col.update_many(
        {},
        {
            "$set": {
                "status": "inactive",
                "updated_at": datetime.utcnow(),
                "status_updated_by": session.get("admin_id") or session.get("user_id"),
            }
        },
    )

    return redirect(url_for("admin_agent_codes.admin_agent_codes", status="inactive"))


@admin_agent_codes_bp.route("/admin/agent-codes/activate-all", methods=["POST"])
def activate_all_agent_codes():
    if not _require_admin():
        return redirect(url_for("login.login"))

    agent_codes_col.update_many(
        {},
        {
            "$set": {
                "status": "active",
                "updated_at": datetime.utcnow(),
                "status_updated_by": session.get("admin_id") or session.get("user_id"),
            }
        },
    )

    return redirect(url_for("admin_agent_codes.admin_agent_codes", status="active"))
