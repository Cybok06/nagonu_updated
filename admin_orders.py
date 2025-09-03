# admin_orders.py  — Admin Orders + SCHEDULER (per-order timed status changes)
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from bson import ObjectId, Regex
from db import db
from datetime import datetime, timedelta
from urllib.parse import urlencode

# === NEW: APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.base import ConflictingIdError, JobLookupError

admin_orders_bp = Blueprint("admin_orders", __name__)

orders_col        = db["orders"]
users_col         = db["users"]
balances_col      = db["balances"]         # for refunds
transactions_col  = db["transactions"]     # for refund ledger

# Keep legacy; primary set includes refunded
ALLOWED_STATUSES   = {"pending", "processing", "delivered", "failed", "completed", "refunded"}
ALLOWED_SORTS      = {"newest", "oldest", "amount_desc", "amount_asc"}
DEFAULT_PER_PAGE   = 10

# ---------- SCHEDULER SINGLETON ----------
_scheduler: BackgroundScheduler | None = None

def _ensure_scheduler_started():
    """
    Start a single background scheduler (idempotent). In production, prefer
    running this only on a single process/instance (e.g., admin worker),
    or use a distributed job store.
    """
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler
    _scheduler = BackgroundScheduler(daemon=True, job_defaults={"coalesce": True, "misfire_grace_time": 60})
    _scheduler.start()
    return _scheduler

# Call at import time (safe if multiple imports in same process)
_ensure_scheduler_started()

# ---------- HELPERS ----------
def _parse_date(dstr):
    if not dstr:
        return None
    try:
        # Expect UTC naive (YYYY-MM-DD or YYYY-MM-DD HH:MM)
        if len(dstr.strip()) <= 10:
            return datetime.strptime(dstr.strip(), "%Y-%m-%d")
        return datetime.strptime(dstr.strip(), "%Y-%m-%d %H:%M")
    except Exception:
        return None

def _build_preserved_query(args, exclude=("page",)):
    kept = {k: v for k, v in args.items() if k not in exclude and v not in (None, "", "None")}
    return urlencode(kept)

def _build_query_from_params(args):
    """Central builder so list + bulk share identical filters."""
    status_filter = (args.get("status") or "").strip().lower()
    order_id_q    = (args.get("order_id") or "").strip()
    customer_q    = (args.get("customer") or "").strip()
    paid_from     = (args.get("paid_from") or "").strip().lower()
    min_total     = (args.get("min_total") or "").strip()
    max_total     = (args.get("max_total") or "").strip()
    date_from     = _parse_date((args.get("date_from") or "").strip())
    date_to_raw   = _parse_date((args.get("date_to") or "").strip())
    date_to       = datetime(date_to_raw.year, date_to_raw.month, date_to_raw.day) + timedelta(days=1) if date_to_raw else None

    item_service  = (args.get("item_service") or "").strip()
    item_offer    = (args.get("item_offer") or "").strip()
    item_phone    = (args.get("item_phone") or "").strip()

    query = {}

    if status_filter and status_filter in ALLOWED_STATUSES:
        query["status"] = status_filter
    if paid_from:
        query["paid_from"] = paid_from
    if order_id_q:
        query["order_id"] = Regex(order_id_q, "i")

    if date_from or date_to:
        dt = {}
        if date_from: dt["$gte"] = date_from
        if date_to:   dt["$lt"]  = date_to
        query["created_at"] = dt

    amt = {}
    try:
        if min_total != "": amt["$gte"] = float(min_total)
    except Exception:
        pass
    try:
        if max_total != "": amt["$lte"] = float(max_total)
    except Exception:
        pass
    if amt:
        query["total_amount"] = amt

    if customer_q:
        rx = Regex(customer_q, "i")
        user_ids = [u["_id"] for u in users_col.find(
            {"$or": [
                {"first_name": rx}, {"last_name": rx}, {"email": rx},
                {"phone": rx}, {"username": rx},
            ]},
            {"_id": 1},
        )]
        query["user_id"] = {"$in": user_ids or []}

    item_and = []
    if item_service: item_and.append({"items.serviceName": Regex(item_service, "i")})
    if item_offer:   item_and.append({"items.value": Regex(item_offer, "i")})
    if item_phone:   item_and.append({"items.phone": Regex(item_phone, "i")})
    if item_and:
        query["$and"] = (query.get("$and") or []) + item_and

    return query

def _require_admin():
    return session.get("role") == "admin"

def _money(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

# ---------- CORE: apply status change (used by manual & scheduled) ----------
def _apply_status_change(order_ids: list[ObjectId], new_status: str, reason: str = "manual", actor_admin_id=None):
    """
    Idempotent per-order updates, including wallet credit for refunds.
    Returns (updated_count, errors)
    """
    updated = 0
    errors  = []

    now = datetime.utcnow()
    for oid in order_ids:
        try:
            order = orders_col.find_one({"_id": oid})
            if not order:
                errors.append(f"{oid}: not found")
                continue

            old_status = (order.get("status") or "").lower()
            update_doc = {"status": new_status, "updated_at": now}
            # Delivered → set delivered_at if missing
            if new_status == "delivered" and not order.get("delivered_at"):
                update_doc["delivered_at"] = now

            # Refunded → single wallet credit based on charged_amount
            if new_status == "refunded":
                charged_amount = _money(order.get("charged_amount"), 0.0)
                user_id = order.get("user_id")
                already_refunded = bool(order.get("refunded_at")) or (old_status == "refunded")

                if charged_amount > 0 and user_id and not already_refunded:
                    try:
                        balances_col.update_one(
                            {"user_id": user_id},
                            {"$inc": {"amount": charged_amount}, "$set": {"updated_at": now}},
                            upsert=True
                        )
                        transactions_col.insert_one({
                            "user_id": user_id,
                            "amount": charged_amount,
                            "reference": order.get("order_id"),
                            "status": "success",
                            "type": "refund",
                            "gateway": "Wallet",
                            "currency": "GHS",
                            "created_at": now,
                            "verified_at": now,
                            "meta": {
                                "note": f"{reason.capitalize()} refund",
                                "order_db_id": oid,
                                "actor_admin_id": actor_admin_id,
                            }
                        })
                    except Exception as e:
                        errors.append(f"{oid}: refund ledger err: {e}")
                update_doc["refunded_at"] = now

            res = orders_col.update_one({"_id": oid}, {"$set": update_doc})
            if res.modified_count:
                # Optionally flip line_status in items from processing -> delivered when marking delivered
                if new_status == "delivered":
                    try:
                        orders_col.update_one(
                            {"_id": oid},
                            {"$set": {"items.$[it].line_status": "delivered"}},
                            array_filters=[{"it.line_status": "processing"}]
                        )
                    except Exception:
                        pass
                updated += 1
            else:
                # No-op is okay; still count as updated? keep strict.
                pass

        except Exception as e:
            errors.append(f"{oid}: {e}")

    return updated, errors

# ---------- SCHEDULED JOB HANDLER ----------
def _scheduled_status_job(order_id_strs: list[str], new_status: str, actor_admin_id: str | None, note: str | None):
    """
    Executed by APScheduler in-process. Converts ids and calls _apply_status_change.
    """
    try:
        ids = []
        for s in order_id_strs:
            try:
                ids.append(ObjectId(s))
            except Exception:
                pass
        _apply_status_change(ids, new_status, reason="scheduled", actor_admin_id=actor_admin_id)
    except Exception as e:
        # In production, wire a structured logger
        print("[SCHEDULED_JOB_ERROR]", str(e))

# =========================================================
#                       ROUTES
# =========================================================
@admin_orders_bp.route("/admin/orders")
def admin_view_orders():
    if not _require_admin():
        return redirect(url_for("login.login"))

    sort = (request.args.get("sort") or "newest").strip().lower()
    if sort not in ALLOWED_SORTS:
        sort = "newest"

    try:
        per_page = int(request.args.get("per_page", DEFAULT_PER_PAGE))
        per_page = max(1, min(per_page, 100))
    except Exception:
        per_page = DEFAULT_PER_PAGE

    try:
        page = int(request.args.get("page", 1))
        page = max(1, page)
    except Exception:
        page = 1

    skip = (page - 1) * per_page
    query = _build_query_from_params(request.args)

    sort_spec = [("created_at", -1)]
    if sort == "oldest":
        sort_spec = [("created_at", 1)]
    elif sort == "amount_desc":
        sort_spec = [("total_amount", -1), ("created_at", -1)]
    elif sort == "amount_asc":
        sort_spec = [("total_amount", 1), ("created_at", -1)]

    try:
        total_orders = orders_col.count_documents(query)
        total_pages  = max(1, (total_orders + per_page - 1) // per_page)
        orders       = list(orders_col.find(query).sort(sort_spec).skip(skip).limit(per_page))

        for o in orders:
            uid = o.get("user_id")
            if isinstance(uid, str):
                try:
                    uid = ObjectId(uid)
                except Exception:
                    uid = None
            o["user"] = users_col.find_one({"_id": uid}) if uid else {}
    except Exception:
        flash("Error loading orders.", "danger")
        orders, total_pages, total_orders = [], 1, 0

    return render_template(
        "admin_orders.html",
        orders=orders,
        page=page, total_pages=total_pages, total_orders=total_orders,
        status_filter=(request.args.get("status") or "").strip().lower(),
        order_id_q=(request.args.get("order_id") or "").strip(),
        customer_q=(request.args.get("customer") or "").strip(),
        paid_from=(request.args.get("paid_from") or "").strip().lower(),
        min_total=(request.args.get("min_total") or "").strip(),
        max_total=(request.args.get("max_total") or "").strip(),
        date_from=(request.args.get("date_from") or "").strip(),
        date_to=(request.args.get("date_to") or "").strip(),
        sort=sort, per_page=per_page,
        item_service=(request.args.get("item_service") or "").strip(),
        item_offer=(request.args.get("item_offer") or "").strip(),
        item_phone=(request.args.get("item_phone") or "").strip(),
        filters_query=_build_preserved_query(request.args),
    )

@admin_orders_bp.route("/admin/orders/<order_id>/update", methods=["POST"])
def update_order_status(order_id):
    if not _require_admin():
        return redirect(url_for("login.login"))

    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ALLOWED_STATUSES:
        flash("Invalid status.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    try:
        oid = ObjectId(order_id)
    except Exception:
        flash("Invalid order id.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    updated, errors = _apply_status_change([oid], new_status, reason="manual", actor_admin_id=session.get("user_id"))
    if updated:
        msg = {
            "processing": "✅ Order marked as Processing.",
            "delivered": "✅ Order marked as Delivered.",
            "failed": "✅ Order marked as Failed.",
            "refunded": "✅ Order marked as Refunded (wallet credited if not already).",
            "pending": "✅ Order marked as Pending.",
            "completed": "✅ Order marked as Completed.",
        }.get(new_status, "✅ Order updated.")
        flash(msg, "success")
    else:
        flash("⚠️ No change to order.", "warning")
    if errors:
        flash(" | ".join(errors[:3]), "warning")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

@admin_orders_bp.route("/admin/orders/bulk-deliver", methods=["POST"])
def bulk_deliver_orders():
    if not _require_admin():
        return redirect(url_for("login.login"))
    args = request.args.to_dict(flat=True)
    args["status"] = "processing"
    query = _build_query_from_params(args)

    try:
        now = datetime.utcnow()
        res = orders_col.update_many(
            query,
            {"$set": {"status": "delivered", "delivered_at": now, "updated_at": now}}
        )
        modified = getattr(res, "modified_count", 0)

        try:
            orders_col.update_many(
                {"_id": {"$in": [o["_id"] for o in orders_col.find(query, {"_id": 1})]}},
                {"$set": {"items.$[it].line_status": "delivered"}},
                array_filters=[{"it.line_status": "processing"}]
            )
        except Exception:
            pass

        flash(f"✅ Marked {modified} processing order(s) as Delivered.", "success")
    except Exception:
        flash("❌ Bulk update failed.", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

# =========================================================
#            NEW: SCHEDULING ENDPOINTS (Admin)
# =========================================================

@admin_orders_bp.route("/admin/orders/schedule-status", methods=["POST"])
def schedule_status():
    """
    Form fields (either style):
      - order_ids: comma-separated string OR multiple order_ids[] fields
      - status: one of ALLOWED_STATUSES
      - delay_minutes: int (optional)
      - run_at: "YYYY-MM-DD HH:MM" (UTC, optional)
      - note: optional free text
    One of delay_minutes or run_at is required.
    """
    if not _require_admin():
        return redirect(url_for("login.login"))

    status = (request.form.get("status") or "").strip().lower()
    if status not in ALLOWED_STATUSES:
        flash("Invalid status for scheduling.", "danger")
        return redirect(url_for("admin_orders.admin_view_orders"))

    # collect order ids
    raw_list = []
    if "order_ids" in request.form:
        raw_list += [request.form.get("order_ids") or ""]
    raw_list += request.form.getlist("order_ids[]")  # supports multi-select UIs

    # Also support checkbox sets like order_id[]=... in your table if you add them later
    raw_list += request.form.getlist("order_id[]")
    raw_list = ",".join([s for s in raw_list if s]).split(",")

    order_id_strs = []
    bad_ids = []
    for s in raw_list:
        s2 = (s or "").strip()
        if not s2:
            continue
        try:
            ObjectId(s2)
            order_id_strs.append(s2)
        except Exception:
            bad_ids.append(s2)

    if not order_id_strs:
        flash("Please select at least one valid order.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    # compute run time
    delay_str  = (request.form.get("delay_minutes") or "").strip()
    run_at_str = (request.form.get("run_at") or "").strip()
    run_time   = None

    if delay_str:
        try:
            mins = int(delay_str)
            run_time = datetime.utcnow() + timedelta(minutes=max(0, mins))
        except Exception:
            flash("Invalid delay minutes.", "danger")
            return redirect(url_for("admin_orders.admin_view_orders"))
    elif run_at_str:
        dt = _parse_date(run_at_str)
        if not dt:
            flash("Invalid run_at datetime. Use 'YYYY-MM-DD HH:MM' (UTC).", "danger")
            return redirect(url_for("admin_orders.admin_view_orders"))
        run_time = dt
        if run_time < datetime.utcnow():
            flash("Run time must be in the future.", "warning")
            return redirect(url_for("admin_orders.admin_view_orders"))
    else:
        flash("Provide either delay_minutes or run_at.", "warning")
        return redirect(url_for("admin_orders.admin_view_orders"))

    note = (request.form.get("note") or "").strip()
    admin_id = (session.get("user_id") or None)

    # Create the job
    sched = _ensure_scheduler_started()
    # Use a stable job id if you want dedupe per exact tuple; here we let it create unique ids
    trigger = DateTrigger(run_date=run_time)

    try:
        job = sched.add_job(
            func=_scheduled_status_job,
            trigger=trigger,
            args=[order_id_strs, status, str(admin_id) if admin_id else None, note],
            replace_existing=False,
            id=None,  # auto id; you can construct one if you want deterministic dedupe
            max_instances=10,
            misfire_grace_time=120,
        )
        flash(f"⏱️ Scheduled {len(order_id_strs)} order(s) → {status} at {run_time.strftime('%Y-%m-%d %H:%M')} UTC.", "success")
    except ConflictingIdError:
        flash("A similar schedule already exists.", "warning")
    except Exception as e:
        flash(f"Failed to create schedule: {e}", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)

@admin_orders_bp.route("/admin/orders/schedules", methods=["GET"])
def list_schedules():
    """Lightweight JSON list for now; you can render a template if you prefer."""
    if not _require_admin():
        return redirect(url_for("login.login"))
    sched = _ensure_scheduler_started()
    jobs = []
    for j in sched.get_jobs():
        jobs.append({
            "id": j.id,
            "next_run_time": j.next_run_time.strftime("%Y-%m-%d %H:%M:%S UTC") if j.next_run_time else None,
            "func": str(j.func),
            "args": j.args,
        })
    # You can also render to a page; returning JSON is simple for start.
    return jsonify({"jobs": jobs})

@admin_orders_bp.route("/admin/orders/schedules/<job_id>/cancel", methods=["POST"])
def cancel_schedule(job_id):
    if not _require_admin():
        return redirect(url_for("login.login"))
    sched = _ensure_scheduler_started()
    try:
        sched.remove_job(job_id)
        flash("🗑️ Schedule cancelled.", "success")
    except JobLookupError:
        flash("Schedule not found.", "warning")
    except Exception as e:
        flash(f"Failed to cancel schedule: {e}", "danger")

    back_to = url_for("admin_orders.admin_view_orders")
    qs = _build_preserved_query(request.args)
    return redirect(f"{back_to}?{qs}" if qs else back_to)
