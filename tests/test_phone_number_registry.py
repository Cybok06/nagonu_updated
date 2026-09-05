from __future__ import annotations

import unittest

from phone_number_registry import normalize_registry_phone, order_agent_id, order_phone_numbers


class PhoneNumberRegistryTests(unittest.TestCase):
    def test_normalizes_supported_ghana_formats(self):
        self.assertEqual(normalize_registry_phone("0241235993"), "0241235993")
        self.assertEqual(normalize_registry_phone("233241235993"), "0241235993")
        self.assertEqual(normalize_registry_phone("+233 24 123 5993"), "0241235993")

    def test_collects_and_deduplicates_all_order_numbers(self):
        order = {
            "buyer_phone": "0241235993",
            "items": [
                {"phone": "233241235993"},
                {"phone": "0551234567"},
                {"phone": "invalid"},
            ],
        }
        self.assertEqual(order_phone_numbers(order), ["0241235993", "0551234567"])

    def test_uses_order_user_as_agent(self):
        self.assertEqual(order_agent_id({"user_id": "agent-1"}), "agent-1")


if __name__ == "__main__":
    unittest.main()
