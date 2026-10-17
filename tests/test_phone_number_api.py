from __future__ import annotations

import unittest
from unittest.mock import patch

from flask import Flask

from phone_number_api import phone_number_api_bp


class PhoneNumberApiTests(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.register_blueprint(phone_number_api_bp)
        self.client = app.test_client()

    def test_rejects_wrong_reference(self):
        response = self.client.post(
            "/internal/phone-numbers/register",
            json={"phone_number": "0241235993", "agent_id": "agent", "reference": "wrong"},
        )
        self.assertEqual(response.status_code, 403)

    @patch("phone_number_api.register_phone_number")
    def test_accepts_exact_reference_without_exposing_insert_status(self, register):
        register.return_value = True
        response = self.client.post(
            "/internal/phone-numbers/register",
            json={"phone_number": "0241235993", "agent_id": "agent", "reference": "ussd_number_245"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"success": True})
        register.assert_called_once_with("0241235993", "agent")


if __name__ == "__main__":
    unittest.main()
