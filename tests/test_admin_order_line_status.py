import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

from bson import ObjectId
from flask import Flask


class AdminOrderLineStatusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Import the real handlers without opening the application's live DB.
        fake_db = types.ModuleType("db")
        fake_db.db = MagicMock()
        fake_db.campus_db = MagicMock()
        path = Path(__file__).resolve().parents[1] / "admin_orders.py"
        spec = importlib.util.spec_from_file_location("admin_orders_under_test", path)
        cls.orders = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"db": fake_db}), patch(
            "apscheduler.schedulers.background.BackgroundScheduler.start"
        ):
            spec.loader.exec_module(cls.orders)

    def setUp(self):
        app = Flask(__name__)
        app.secret_key = "test-only"
        app.register_blueprint(self.orders.admin_orders_bp)
        app.add_url_rule("/login", endpoint="login.login", view_func=lambda: "Login")
        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["role"] = "admin"
            session["user_id"] = "test-admin"
        self.oid = ObjectId()
        self.main = MagicMock()
        self.campus = MagicMock()
        for name, value in (("orders_col", self.main), ("campus_orders_col", self.campus)):
            patcher = patch.object(self.orders, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(self.orders, "bump_orders_cache_version")
        self.bump_cache = patcher.start()
        self.addCleanup(patcher.stop)

    def post_status(self, status, source="main", index=0):
        prefix = "campus:" if source == "campus" else ""
        return self.client.post(
            f"/admin/orders/{prefix}{self.oid}/items/{index}/status?view=lines&source={source}",
            data={"line_status": status},
        )

    def flashes(self):
        with self.client.session_transaction() as session:
            return session.get("_flashes", [])

    def test_updates_main_and_campus_lines_and_parent_status(self):
        for source in ("main", "campus"):
            for status in ("pending", "processing", "delivered", "failed"):
                with self.subTest(source=source, status=status):
                    self.main.reset_mock()
                    self.campus.reset_mock()
                    self.bump_cache.reset_mock()
                    collection = self.campus if source == "campus" else self.main
                    other = self.main if source == "campus" else self.campus
                    collection.find_one.return_value = {
                        "_id": self.oid, "status": "pending",
                        "items": [{"line_status": "pending"}],
                    }
                    collection.update_one.return_value.matched_count = 1
                    response = self.post_status(status, source)
                    self.assertEqual(response.status_code, 302)
                    self.assertIn(f"view=lines&source={source}", response.location)
                    collection.update_one.assert_called_once()
                    query, update = collection.update_one.call_args.args
                    self.assertEqual(query, {"_id": self.oid})
                    self.assertEqual(update["$set"]["items.0.line_status"], status)
                    if status != "pending":
                        self.assertEqual(update["$set"]["status"], status)
                    other.find_one.assert_not_called()
                    self.bump_cache.assert_called_once()
                    self.assertEqual(self.flashes()[-1], ("success", "Line status updated."))

    def test_delivered_line_cannot_be_reverted(self):
        self.main.find_one.return_value = {
            "_id": self.oid, "status": "delivered",
            "items": [{"line_status": "delivered"}],
        }
        self.post_status("processing")
        self.main.update_one.assert_not_called()
        self.assertIn("cannot be changed", self.flashes()[-1][1])

    def test_missing_line_does_not_report_success(self):
        self.main.find_one.return_value = {"_id": self.oid, "items": []}
        self.post_status("delivered")
        self.main.update_one.assert_not_called()
        self.assertIn("item not found", self.flashes()[-1][1])

    def test_database_failure_does_not_report_success(self):
        self.main.find_one.return_value = {
            "_id": self.oid, "items": [{"line_status": "pending"}],
        }
        self.main.update_one.side_effect = RuntimeError("Database unavailable")
        self.post_status("processing")
        self.assertEqual(self.flashes()[-1][0], "warning")
        self.bump_cache.assert_not_called()

    def test_non_admin_cannot_update_lines(self):
        with self.client.session_transaction() as session:
            session["role"] = "customer"
        response = self.post_status("delivered")
        self.assertEqual(response.location, "/login")
        self.main.find_one.assert_not_called()
        self.campus.find_one.assert_not_called()


if __name__ == "__main__":
    unittest.main()
