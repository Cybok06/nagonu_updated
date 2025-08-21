# admin_complaints.py
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file
from bson import ObjectId
from datetime import datetime
from io import BytesIO
import pandas as pd

# PDF export deps
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

from db import db

admin_complaints_bp = Blueprint("admin_complaints", __name__)
complaints_col = db["complaints"]
users_col = db["users"]

def _fmt_dt(dt):
    try:
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""

def _safe(c, *path, default=None):
    """Safely traverse nested keys/attrs in dict-like objects."""
    cur = c
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

@admin_complaints_bp.route("/admin/complaints", methods=["GET"])
def admin_view_complaints():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    status = (request.args.get("status") or "").strip()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    export_type = (request.args.get("export") or "").lower()

    query = {}

    if status:
        query["status"] = status

    # Date filtering: submitted_at
    date_cond = {}
    if start_date:
        try:
            date_cond["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
        except Exception:
            flash("Invalid start date format", "warning")
    if end_date:
        try:
            dt = datetime.strptime(end_date, "%Y-%m-%d")
            date_cond["$lte"] = dt
        except Exception:
            flash("Invalid end date format", "warning")
    if date_cond:
        query["submitted_at"] = date_cond

    complaints = list(complaints_col.find(query).sort("submitted_at", -1))

    # Fetch users in bulk
    user_ids = list({c.get("user_id") for c in complaints if c.get("user_id")})
    users = {u["_id"]: u for u in users_col.find({"_id": {"$in": user_ids}})} if user_ids else {}

    # Normalize fields for template & export
    for c in complaints:
        u = users.get(c.get("user_id"), {})
        c["user"] = u
        c["customer_name"] = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip() or u.get("username", "")
        c["customer_phone"] = u.get("phone", "")
        c["submitted_at_str"] = _fmt_dt(c.get("submitted_at"))
        # For legacy docs, order_date might be set; for new docs we keep it too
        c["order_date_str"] = _fmt_dt(c.get("order_date"))
        # New schema fields
        c["order_ref"] = c.get("order_ref", {}) or {}
        c["order_no_display"] = c.get("order_number_provided", "")
        # Screenshots (new) vs image_path (legacy)
        shots = c.get("screenshots") or {}
        c["screenshots"] = {
            "data_balance": shots.get("data_balance", ""),
            "phone_msisdn": shots.get("phone_msisdn", ""),
        }

    if export_type == "excel":
        return _export_complaints_to_excel(complaints)
    elif export_type == "pdf":
        return _export_complaints_to_pdf(complaints)

    return render_template(
        "admin_complaints.html",
        complaints=complaints,
        status_filter=status,
        start_date=start_date,
        end_date=end_date,
    )

def _export_complaints_to_excel(complaints):
    """
    Columns updated for the new schema; still includes legacy fields if present.
    """
    data = []
    for c in complaints:
        row = {
            "Customer": c.get("customer_name", ""),
            "Phone": c.get("customer_phone", ""),
            "Service": c.get("service_name", ""),
            "Offer": c.get("offer", ""),
            "Order Entered": c.get("order_no_display", ""),
            "Order _id": _safe(c, "order_ref", "_id", default=""),
            "Order order_no": _safe(c, "order_ref", "order_no", default=""),
            "Order order_id": _safe(c, "order_ref", "order_id", default=""),
            "Order Date": c.get("order_date_str", ""),
            # Proofs
            "Proof: Data Balance": _safe(c, "screenshots", "data_balance", default=c.get("image_path","")),
            "Proof: Phone MSISDN": _safe(c, "screenshots", "phone_msisdn", default=""),
            # Legacy fields (may be empty)
            "Description (legacy)": c.get("description", ""),
            "WhatsApp (legacy)": c.get("whatsapp", ""),
            # Meta
            "Status": c.get("status", ""),
            "Submitted At": c.get("submitted_at_str", ""),
        }
        data.append(row)

    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Complaints")
    output.seek(0)
    return send_file(output, download_name="complaints.xlsx", as_attachment=True)

def _export_complaints_to_pdf(complaints):
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=letter, leftMargin=36, rightMargin=36, topMargin=42, bottomMargin=36)
    elements = []
    styles = getSampleStyleSheet()
    elements.append(Paragraph("Customer Complaints Report", styles["Title"]))

    # Keep the PDF concise with key columns
    data = [["Customer", "Phone", "Service", "Offer", "Status", "Submitted"]]
    for c in complaints:
        data.append([
            c.get("customer_name", ""),
            c.get("customer_phone", ""),
            c.get("service_name", ""),
            c.get("offer", ""),
            (c.get("status","") or "").capitalize(),
            c.get("submitted_at_str",""),
        ])

    table = Table(data, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.darkblue),
        ("TEXTCOLOR", (0,0), (-1,0), colors.whitesmoke),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("ALIGN", (0,0), (-1,-1), "LEFT"),
        ("GRID", (0,0), (-1,-1), 0.25, colors.black),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.lightgrey]),
    ]))

    elements.append(table)
    doc.build(elements)
    output.seek(0)
    return send_file(output, download_name="complaints.pdf", as_attachment=True)

@admin_complaints_bp.route("/admin/complaints/<complaint_id>/update", methods=["POST"])
def update_complaint_status(complaint_id):
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    # Read new status and preserve current filters for redirect
    new_status = (request.form.get("status") or "").strip().lower()
    allowed = {"pending", "resolved", "refund"}
    if new_status not in allowed:
        flash("Invalid status selected.", "warning")
        return redirect(url_for(
            "admin_complaints.admin_view_complaints",
            status=request.args.get("status") or request.form.get("status_filter") or "",
            start_date=request.args.get("start_date") or request.form.get("start_date") or "",
            end_date=request.args.get("end_date") or request.form.get("end_date") or ""
        ))

    # Validate id
    try:
        _id = ObjectId(complaint_id)
    except Exception:
        flash("Invalid complaint id.", "danger")
        return redirect(url_for(
            "admin_complaints.admin_view_complaints",
            status=request.args.get("status") or request.form.get("status_filter") or "",
            start_date=request.args.get("start_date") or request.form.get("start_date") or "",
            end_date=request.args.get("end_date") or request.form.get("end_date") or ""
        ))

    c = complaints_col.find_one({"_id": _id})
    if not c:
        flash("Complaint not found.", "warning")
        return redirect(url_for(
            "admin_complaints.admin_view_complaints",
            status=request.args.get("status") or request.form.get("status_filter") or "",
            start_date=request.args.get("start_date") or request.form.get("start_date") or "",
            end_date=request.args.get("end_date") or request.form.get("end_date") or ""
        ))

    # Update document
    update_doc = {
        "status": new_status,
        "updated_at": datetime.utcnow(),
        "updated_by": {
            "user_id": session.get("user_id"),
            "username": session.get("username") or session.get("email") or "admin"
        }
    }
    complaints_col.update_one({"_id": _id}, {"$set": update_doc})

    flash(f"Complaint status updated to {new_status}.", "success")

    # Redirect back to the list (preserve current filters if present)
    return redirect(url_for(
        "admin_complaints.admin_view_complaints",
        status=request.args.get("status") or request.form.get("status_filter") or "",
        start_date=request.args.get("start_date") or request.form.get("start_date") or "",
        end_date=request.args.get("end_date") or request.form.get("end_date") or ""
    ))

