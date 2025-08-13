# admin_sidebar.py
from flask import Blueprint, session
from db import db

admin_sidebar_bp = Blueprint("admin_sidebar", __name__)

orders_col = db["orders"]
complaints_col = db["complaints"]   # change if your collection name differs

def _is_admin() -> bool:
    return session.get("role") == "admin"

@admin_sidebar_bp.app_context_processor
def inject_admin_counts():
    """
    Inject counts into all templates. If not an admin, keep zeros so templates are safe.
    """
    if not _is_admin():
        return {
            "pending_orders_count": 0,
            "pending_complaints_count": 0,
        }
    try:
        pending_orders = orders_col.count_documents({"status": "pending"})
    except Exception:
        pending_orders = 0

    try:
        pending_complaints = complaints_col.count_documents({"status": "pending"})
    except Exception:
        pending_complaints = 0

    return {
        "pending_orders_count": pending_orders,
        "pending_complaints_count": pending_complaints,
    }
