from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file
from bson import ObjectId
from datetime import datetime
from io import BytesIO
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from db import db

admin_complaints_bp = Blueprint("admin_complaints", __name__)
complaints_col = db["complaints"]
users_col = db["users"]

@admin_complaints_bp.route("/admin/complaints", methods=["GET"])
def admin_view_complaints():
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    status = request.args.get("status", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    export_type = request.args.get("export", "").lower()

    query = {}

    if status:
        query["status"] = status

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            query["submitted_at"] = {"$gte": start_dt}
        except:
            flash("Invalid start date format", "warning")

    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            if "submitted_at" in query:
                query["submitted_at"]["$lte"] = end_dt
            else:
                query["submitted_at"] = {"$lte": end_dt}
        except:
            flash("Invalid end date format", "warning")

    complaints = list(complaints_col.find(query).sort("submitted_at", -1))

    # Fetch user info
    user_ids = list(set(c["user_id"] for c in complaints))
    users = {u["_id"]: u for u in users_col.find({"_id": {"$in": user_ids}})}

    for c in complaints:
        user = users.get(c["user_id"], {})
        c["user"] = user
        c["submitted_at_str"] = c["submitted_at"].strftime("%Y-%m-%d %H:%M") if c.get("submitted_at") else ""
        c["customer_name"] = f"{user.get('first_name', '')} {user.get('last_name', '')}"
        c["customer_phone"] = user.get("phone", "")

    if export_type == "excel":
        return export_complaints_to_excel(complaints)
    elif export_type == "pdf":
        return export_complaints_to_pdf(complaints)

    return render_template("admin_complaints.html", complaints=complaints,
                           status_filter=status, start_date=start_date, end_date=end_date)

def export_complaints_to_excel(complaints):
    data = []
    for c in complaints:
        data.append({
            "Customer": c["customer_name"],
            "Phone": c["customer_phone"],
            "Service": c.get("service_name", ""),
            "Offer": c.get("offer", ""),
            "WhatsApp": c.get("whatsapp", ""),
            "Description": c.get("description", ""),
            "Status": c.get("status", ""),
            "Submitted At": c["submitted_at_str"]
        })
    df = pd.DataFrame(data)
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, download_name="complaints.xlsx", as_attachment=True)

def export_complaints_to_pdf(complaints):
    output = BytesIO()
    doc = SimpleDocTemplate(output, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
    elements.append(Paragraph("Customer Complaints Report", styles['Title']))

    data = [["Customer", "Phone", "Service", "Offer", "Status", "Date"]]
    for c in complaints:
        data.append([
            c["customer_name"],
            c["customer_phone"],
            c.get("service_name", ""),
            c.get("offer", ""),
            c.get("status", "").capitalize(),
            c["submitted_at_str"]
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))

    elements.append(table)
    doc.build(elements)
    output.seek(0)
    return send_file(output, download_name="complaints.pdf", as_attachment=True)

@admin_complaints_bp.route("/admin/complaints/<complaint_id>/update", methods=["POST"])
def update_complaint_status(complaint_id):
    if session.get("role") != "admin":
        return redirect(url_for("login.login"))

    new_status = request.form.get("status")
    if new_status not in ["resolved", "refund"]:
        flash("Invalid status selected", "danger")
        return redirect(url_for("admin_complaints.admin_view_complaints"))

    complaints_col.update_one({"_id": ObjectId(complaint_id)}, {"$set": {"status": new_status}})
    flash("Complaint status updated successfully!", "success")
    return redirect(url_for("admin_complaints.admin_view_complaints"))
