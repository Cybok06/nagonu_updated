import ast
from pathlib import Path
import re
import unittest
from unittest.mock import Mock

from flask import Flask, Blueprint, request, jsonify
from pymongo import timeout
from pymongo.errors import ExecutionTimeout, PyMongoError


class StorePhoneVerificationTests(unittest.TestCase):
    def setUp(self):
        names = {'_existing_mtn_history_enforced', '_check_phone_history_requirement',
                 '_phone_has_existing_order', 'verify_existing_order_phone'}
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'checkout.py').read_text(encoding='utf-8'))
        self.settings = Mock()
        self.settings.find_one.return_value = {'require_existing_mtn_history': False}
        self.orders = Mock()
        self.alert = Mock()
        bp = Blueprint('checkout', __name__)
        self.env = dict(checkout_bp=bp, request=request, jsonify=jsonify, re=re,
                        mongo_timeout=timeout, PyMongoError=PyMongoError,
                        phone_verification_settings_col=self.settings, orders_col=self.orders,
                        PHONE_VERIFICATION_SETTINGS_ID='PHONE_HISTORY_SETTINGS',
                        _normalize_phone_for_blocking=lambda phone: phone,
                        _requires_existing_mtn_history=lambda *args: True,
                        _phone_history_match_keys=lambda phone: [phone],
                        _first_time_alert_for_phone=self.alert)
        module = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
        exec(compile(module, 'checkout.py', 'exec'), self.env)
        app = Flask(__name__)
        app.register_blueprint(bp)
        self.client = app.test_client()

    def test_off_skips_history_and_first_time_alert(self):
        response = self.client.post('/api/phone/verify-existing-order', json={'phone': '0241234567', 'serviceName': 'MTN Express'})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json['allow_order'])
        self.assertFalse(response.json['required'])
        self.assertFalse(response.json['warning'])
        self.orders.find_one.assert_not_called()
        self.alert.assert_not_called()

    def test_database_timeout_is_retryable_not_unknown_number(self):
        self.settings.find_one.return_value = {'require_existing_mtn_history': True}
        self.orders.find_one.side_effect = ExecutionTimeout('timed out')
        response = self.client.post('/api/phone/verify-existing-order', json={'phone': '0241234567'})
        self.assertEqual(response.status_code, 503)
        self.assertIn('try again', response.json['message'])

    def test_existing_number_uses_one_setting_read(self):
        self.settings.find_one.return_value = {'require_existing_mtn_history': True}
        self.orders.find_one.return_value = {'_id': 'order'}
        result = self.env['_check_phone_history_requirement']('0241234567', 'MTN Express')
        self.assertTrue(result['verified'])
        self.settings.find_one.assert_called_once()


if __name__ == '__main__':
    unittest.main()
