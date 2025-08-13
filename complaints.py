from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from bson.objectid import ObjectId
from datetime import datetime
from werkzeug.utils import secure_filename
import os

from db import db

complaints_bp = Blueprint("complaints", __name__)
orders_col = db["orders"]
complaints_col = db["complaints"]

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@complaints_bp.route("/complaints", methods=["GET", "POST"])
def submit_complaint():
    user_id = session.get("user_id")

    if not user_id:
        flash("You must be logged in to submit a complaint.", "danger")
        return redirect(url_for("login.login"))

    if request.method == "POST":
        order_id = request.form.get("order_id")
        description = request.form.get("description")
        whatsapp = request.form.get("whatsapp")
        image = request.files.get("image")

        if not all([order_id, description, whatsapp]):
            flash("All required fields must be filled.", "danger")
            return redirect(url_for("complaints.submit_complaint"))

        order = orders_col.find_one({"_id": ObjectId(order_id)})
        if not order or not order.get("items"):
            flash("Invalid order selected.", "danger")
            return redirect(url_for("complaints.submit_complaint"))

        item = order["items"][0]
        service_name = item.get("serviceName")
        offer = item.get("value")
        created_at = order.get("created_at")

        image_path = ""
        if image and image.filename != "":
            filename = secure_filename(image.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            image.save(filepath)
            image_path = f"/uploads/{filename}"

        complaint_data = {
            "user_id": ObjectId(user_id),
            "order_id": ObjectId(order_id),
            "service_name": service_name,
            "offer": offer,
            "order_date": created_at,
            "description": description,
            "whatsapp": whatsapp,
            "image_path": image_path,
            "submitted_at": datetime.utcnow(),
            "status": "pending"
        }

        complaints_col.insert_one(complaint_data)
        flash("✅ Complaint submitted successfully!", "success")
        return redirect(url_for("complaints.submit_complaint"))

    orders = list(orders_col.find({"user_id": ObjectId(user_id)}).sort("created_at", -1))
    for o in orders:
        o["created_at"] = o.get("created_at", datetime.utcnow())

    return render_template("complaints.html", orders=orders)
@complaints_bp.route("/view_complaints")
def view_complaints():
    user_id = session.get("user_id")
    if not user_id:
        flash("You must be logged in to view complaints.", "danger")
        return redirect(url_for("login.login"))

    status_filter = request.args.get("status", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()

    query = {"user_id": ObjectId(user_id)}

    if status_filter:
        query["status"] = status_filter

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

    return render_template("view_complaints.html", complaints=complaints, status_filter=status_filter, start_date=start_date, end_date=end_date)
