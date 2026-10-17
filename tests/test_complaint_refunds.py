import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

from bson import ObjectId
from flask import Flask
from test_campus_refunds import Database


class ComplaintRefundTests(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        fake = types.ModuleType('db')
        fake.db = self.db
        patcher = patch.dict(sys.modules, {'db': fake})
        patcher.start()
        self.addCleanup(patcher.stop)
        root = Path(__file__).resolve().parents[1]
        def load(name, filename):
            spec = importlib.util.spec_from_file_location(name, root / filename)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        self.refunds = load('complaint_refunds', 'complaint_refunds.py')
        patcher = patch.dict(sys.modules, {'complaint_refunds': self.refunds})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.admin = load('complaint_admin_test', 'admin_complaints.py')
        self.owner, self.order_id, self.cid = ObjectId(), ObjectId(), ObjectId()
        self.db['balances'].insert_one({'user_id': self.owner, 'amount': 50})
        self.db['orders'].insert_one({'_id': self.order_id, 'order_id': 'ORDER123',
            'user_id': self.owner, 'paid_from': 'wallet', 'charged_amount': 8,
            'status': 'delivered', 'items': [{'serviceName': 'MTN', 'value': '1GB',
                'phone': '0241234567', 'amount': 8, 'base_amount': 6, 'line_status': 'delivered'}]})
        self.db['complaints'].insert_one({'_id': self.cid, 'user_id': self.owner,
            'order_ref': {'_id': self.order_id}, 'service_name': 'MTN', 'offer': '1GB',
            'status': 'pending'})

    def refund(self):
        return self.refunds.refund_complaint(self.cid, {'user_id': 'admin'})

    def balance(self):
        return self.db['balances'].find_one({'user_id': self.owner})['amount']

    def store_order(self):
        self.db['stores'].insert_one({'slug': 'shop', 'owner_id': self.owner})
        self.db['orders'].update_one({}, {'$set': {'store_slug': 'shop',
            'paid_from': 'paystack_inline', 'paystack_reference': 'PS123',
            'user_id': ObjectId(), 'items.0.store_profit_amount': 2}})
        self.db['complaints'].update_one({}, {'$set': {'store_slug': 'shop',
            'paystack_reference': 'PS123'}, '$unset': {'user_id': ''}})

    def test_direct_purchase_returns_actual_deduction(self):
        self.assertEqual(self.refund(), (8, True))
        self.assertEqual(self.balance(), 58)
        self.assertEqual(self.db['transactions'].find_one({})['meta']['line_index'], 0)
        self.assertEqual(self.db['complaints'].find_one({})['status'], 'refund')

    def test_store_refunds_owner_base_price(self):
        self.store_order()
        self.assertEqual(self.refund(), (6, True))
        self.assertEqual(self.balance(), 56)
        self.assertEqual(self.db['balances'].count_documents({}), 1)

    def test_repeat_and_second_complaint_do_not_double_credit(self):
        self.refund()
        self.assertEqual(self.refund(), (8, False))
        complaint = self.db['complaints'].find_one({})
        complaint['_id'] = ObjectId()
        for key in ('refund_completed_at', 'refund_amount'):
            complaint.pop(key)
        self.db['complaints'].insert_one(complaint)
        self.refunds.refund_complaint(complaint['_id'], {})
        self.assertEqual(self.balance(), 58)
        self.assertEqual(self.db['transactions'].count_documents({}), 1)

    def test_only_selected_line_is_credited(self):
        self.db['orders'].update_one({}, {'$push': {'items': {'serviceName': 'MTN',
            'value': '2GB', 'amount': 14, 'line_status': 'processing'}}})
        self.refund()
        self.assertEqual(self.balance(), 58)
        order = self.db['orders'].find_one({})
        self.assertEqual(order['items'][1]['line_status'], 'processing')
        self.assertEqual(order['status'], 'delivered')

    def test_cart_matches_normalized_offer_and_phone(self):
        self.store_order()
        self.db['complaints'].update_one({}, {'$set': {'cart_snapshot': [{
            'serviceName': 'MTN', 'value': {'volume': 1000}, 'phone': '+233241234567',
            'amount': 999}]}})
        self.assertEqual(self.refund()[0], 6)

    def test_missing_order_or_ambiguous_item_does_not_credit(self):
        self.db['orders'].update_one({}, {'$push': {'items': {'serviceName': 'MTN',
            'value': '1GB', 'amount': 8}}})
        with self.assertRaises(ValueError):
            self.refund()
        self.assertEqual(self.balance(), 50)
        self.db['orders'].delete_many({})
        with self.assertRaises(ValueError):
            self.refund()
        self.assertEqual(self.db['complaints'].find_one({})['status'], 'pending')

    def test_missing_base_amount_does_not_credit_retail_price(self):
        self.store_order()
        self.db['orders'].update_one({}, {'$unset': {'items.0.base_amount': '',
            'items.0.store_profit_amount': ''}})
        with self.assertRaises(ValueError):
            self.refund()
        self.assertEqual(self.balance(), 50)

    def test_wallet_failure_rolls_back_and_can_retry(self):
        with patch.object(self.db['balances'], 'update_one', side_effect=RuntimeError('Unavailable')):
            with self.assertRaises(RuntimeError):
                self.refund()
        self.assertEqual(self.db['transactions'].count_documents({}), 0)
        self.assertEqual(self.db['complaints'].find_one({})['status'], 'pending')
        self.assertEqual(self.db['orders'].find_one({})['status'], 'delivered')
        self.assertEqual(self.refund(), (8, True))

    def test_prior_admin_order_refund_is_not_credited_again(self):
        self.db['orders'].update_one({}, {'$set': {'status': 'refunded'}})
        self.refund()
        self.assertEqual(self.balance(), 50)
        self.assertEqual(self.db['transactions'].count_documents({}), 0)

    def test_skipped_line_is_not_refundable(self):
        self.db['orders'].update_one({}, {'$set': {'items.0.line_status': 'skipped_duplicate_in_cart'}})
        with self.assertRaises(ValueError):
            self.refund()
        self.assertEqual(self.balance(), 50)

    def test_store_legacy_price_excludes_markup(self):
        self.store_order()
        self.db['orders'].update_one({}, {'$unset': {'items.0.base_amount': ''}})
        self.assertEqual(self.refund()[0], 6)

    def test_admin_endpoint_does_not_mark_refund_when_credit_fails(self):
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(self.admin.admin_complaints_bp)
        client = app.test_client()
        with client.session_transaction() as session:
            session['role'] = 'admin'
        with patch.object(self.admin, 'refund_complaint', side_effect=ValueError('Order missing')):
            response = client.post(f'/admin/complaints/{self.cid}/update', data={'status': 'refund'})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.db['complaints'].find_one({})['status'], 'pending')
        self.assertEqual(self.balance(), 50)

    def test_admin_endpoint_refunds_and_requires_admin(self):
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(self.admin.admin_complaints_bp)
        app.add_url_rule('/login', endpoint='login.login', view_func=lambda: 'Login')
        client = app.test_client()
        path = f'/admin/complaints/{self.cid}/update'
        client.post(path, data={'status': 'refund'})
        self.assertEqual(self.balance(), 50)
        with client.session_transaction() as session:
            session['role'] = 'admin'
        with patch.object(self.admin, '_send_sms', return_value='sent'):
            response = client.post(path, data={'status': 'refunded'})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.balance(), 58)
        client.post(path, data={'status': 'refund'})
        self.assertEqual(self.balance(), 58)
        client.post(path, data={'status': 'pending'})
        self.assertEqual(self.db['complaints'].find_one({})['status'], 'refund')


if __name__ == '__main__':
    unittest.main()
