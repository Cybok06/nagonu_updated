from __future__ import annotations

import os
from flask import Flask, render_template, send_from_directory
from datetime import datetime

# Load .env (PAYSTACK keys, SECRET_KEY, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from db import db  # ⬅️ add this import if not already present

from customer_dashboard import customer_dashboard_bp
from admin_dashboard import admin_dashboard_bp
from login import login_bp
from signup import signup_bp
from admin_customers import admin_customers_bp
from admin_services import admin_services_bp
from deposit import deposit_bp
from checkout import checkout_bp
from orders import orders_bp
from transactions import transactions_bp
from customer_profile import customer_profile_bp
from complaints import complaints_bp
from referral import referral_bp
from admin_orders import admin_orders_bp
from admin_transactions import admin_transactions_bp
from admin_complaints import admin_complaints_bp
from admin_referrals import admin_referrals_bp
from admin_balance import admin_balance_bp
from admin_wassce_checker import admin_wassce_checker_bp
from purchases import purchases_bp
from purchase_checker import purchase_checker_bp
from admin_purchases import admin_purchases_bp
from settings import settings_bp
from admin_sidebar import admin_sidebar_bp
from login_logs import login_logs_bp
from reset import reset_bp
from afa_routes import afa_bp
from admin_afa import admin_afa_bp



# === NEW: visits collection ===
visits_col = db["visits"]

# Read upload folder from env (fallback to ./uploads)
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", os.path.join(os.getcwd(), "uploads"))

def create_app():
    app = Flask(__name__)
    # SECRET_KEY from env (fallback only for local dev)
    app.secret_key = os.getenv("SECRET_KEY", "change-me")

    # Ensure uploads dir exists
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

    # Register Blueprints (unchanged)
    app.register_blueprint(customer_dashboard_bp)
    app.register_blueprint(admin_dashboard_bp)
    app.register_blueprint(login_bp)
    app.register_blueprint(signup_bp)
    app.register_blueprint(admin_customers_bp)
    app.register_blueprint(admin_services_bp)
    app.register_blueprint(deposit_bp)
    app.register_blueprint(checkout_bp)
    app.register_blueprint(orders_bp)
    app.register_blueprint(transactions_bp)
    app.register_blueprint(customer_profile_bp)
    app.register_blueprint(complaints_bp)
    app.register_blueprint(referral_bp)
    app.register_blueprint(admin_orders_bp)
    app.register_blueprint(admin_transactions_bp)
    app.register_blueprint(admin_complaints_bp)
    app.register_blueprint(admin_referrals_bp)
    app.register_blueprint(admin_balance_bp)
    app.register_blueprint(admin_wassce_checker_bp)
    app.register_blueprint(purchase_checker_bp)
    app.register_blueprint(purchases_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(admin_purchases_bp)
    app.register_blueprint(admin_sidebar_bp)
    app.register_blueprint(login_logs_bp)
    app.register_blueprint(reset_bp)
    app.register_blueprint(afa_bp)
    app.register_blueprint(admin_afa_bp)




    # Make env values available in templates (Jinja)
    @app.context_processor
    def inject_env():
        return {
            "PAYSTACK_PUBLIC_KEY": os.getenv("PAYSTACK_PUBLIC_KEY", ""),  # e.g. pk_live_...
            "COMPANY_NAME": os.getenv("COMPANY_NAME", "Nagonu Data Services"),
            "SUPPORT_EMAIL": os.getenv("SUPPORT_EMAIL", "nagosenu4@gmail.com"),
            "SUPPORT_WHATSAPP": os.getenv("SUPPORT_WHATSAPP", "http://wa.me/233553226196"),
            "COMMUNITY_WHATSAPP": os.getenv(
                "COMMUNITY_WHATSAPP",
                "https://chat.whatsapp.com/ELrcPhAcUNGJGgxncgvbXW?mode=ac_t"
            ),
        }

    @app.route("/")
    def home():
        # === NEW: increment a global visit counter safely ===
        try:
            visits_col.update_one(
                {"_id": "global"},                       # single doc to hold totals
                {
                    "$inc": {"total": 1},
                    "$set": {"updated_at": datetime.utcnow()},
                    "$setOnInsert": {"created_at": datetime.utcnow()}
                },
                upsert=True
            )
        except Exception as e:
            # Non-fatal: don't block the homepage if DB hiccups
            print(f"[visits] increment failed: {e}")

        return render_template("index.html")

    @app.route("/uploads/<path:filename>")
    def uploaded_file(filename):
        return send_from_directory(UPLOAD_FOLDER, filename)

    # Simple health check for Render
    @app.route("/healthz")
    def healthz():
        return "ok", 200

    return app

# ✅ Expose a module-level app for Gunicorn (`gunicorn app:app`)
app = create_app()

if __name__ == "__main__":
    # Local dev only; Render uses Gunicorn
    app.run(debug=True)
