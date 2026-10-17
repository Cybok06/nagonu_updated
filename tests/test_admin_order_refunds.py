import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

from bson import ObjectId
from test_campus_refunds import Database


class AdminRefundTests(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        fake = types.ModuleType('db')
        fake.db = self.db
        fake.campus_db = Database()
        patcher = patch.dict(sys.modules, {'db': fake})
        patcher.start()
        self.addCleanup(patcher.stop)
        root = Path(__file__).resolve().parents[1]
        def load(name, filename):
            spec = importlib.util.spec_from_file_location(name, root / filename)
            module = importlib.util.module_from_spec(spec)
            with patch('apscheduler.schedulers.background.BackgroundScheduler.start'):
                spec.loader.exec_module(module)
            return module
        self.refunds = load('admin_order_refunds', 'admin_order_refunds.py')
        patcher = patch.dict(sys.modules, {'admin_order_refunds': self.refunds})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.admin = load('admin_refund_handlers_test', 'admin_orders.py')
        self.admin.bump_orders_cache_version = lambda: None
        self.owner, self.oid = ObjectId(), ObjectId()
        self.db['users'].insert_one({'_id': self.owner})
        self.db['balances'].insert_one({'user_id': self.owner, 'amount': 50})
        self.db['orders'].insert_one({'_id': self.oid, 'order_id': 'TEST123',
            'user_id': self.owner, 'status': 'delivered', 'charged_amount': 12,
            'items': [{'amount': 12, 'base_amount': 10, 'line_status': 'delivered'}]})

    def balance(self):
        return self.db['balances'].find_one({'user_id': self.owner})['amount']

    def test_original_wallet_snapshot_overrides_wholesale_and_charge(self):
        self.db['orders'].update_one({}, {'$set': {'items.0.wallet_debit_amount': 14.5}})
        self.assertEqual(self.refunds.refund_preview(self.oid)['amount'], 14.5)
        self.assertEqual(self.refunds.refund_preview(self.oid, [0])['amount'], 14.5)
        self.assertEqual(self.refund(), (1, 14.5))
        self.assertEqual(self.balance(), 64.5)
        self.assertEqual(self.refunds.refund_preview(self.oid)['amount'], 0)

    def test_preview_uses_store_owner_and_excludes_markup(self):
        self.db['users'].update_one({}, {'$set': {'username': 'Cybok'}})
        self.db['orders'].update_one({}, {'$set': {'store_slug': 'shop', 'store_owner_id': self.owner}})
        self.assertEqual(self.refunds.refund_preview(self.oid), {'amount': 10, 'recipient': 'Cybok'})
        self.assertEqual(self.balance(), 50)

    def refund(self, indices=None):
        return self.refunds.refund_order(self.oid, indices, actor_admin_id='admin')

    def test_delivered_dashboard_credits_base_once(self):
        self.assertEqual(self.refund(), (1, 12))
        self.assertEqual(self.refund(), (0, 0))
        self.assertEqual(self.balance(), 62)
        self.assertEqual(self.db['transactions'].count_documents({}), 1)
        self.assertEqual(self.db['orders'].find_one({})['status'], 'refunded')

    def test_store_credits_owner_instead_of_logged_in_buyer(self):
        self.db['orders'].update_one({}, {'$set': {'store_slug': 'shop',
            'store_owner_id': str(self.owner), 'user_id': ObjectId(), 'items.0.store_profit_amount': 2}})
        self.assertEqual(self.refund(), (1, 10))
        self.assertEqual(self.balance(), 60)
        self.assertEqual(self.db['balances'].count_documents({}), 1)

    def test_store_owner_and_base_fallback(self):
        self.db['stores'].insert_one({'slug': 'shop', 'owner_id': self.owner})
        self.db['orders'].update_one({}, {'$set': {'store_slug': 'shop', 'items.0.store_profit_amount': 2},
                                         '$unset': {'items.0.base_amount': ''}})
        self.assertEqual(self.refund(), (1, 10))

    def test_saved_base_takes_precedence_over_markup(self):
        self.db['orders'].update_one({}, {'$set': {'store_slug': 'shop', 'store_owner_id': self.owner,
                                                 'items.0.store_profit_amount': 1}})
        self.assertEqual(self.refund(), (1, 10))

    def test_completed_handler_can_refund(self):
        self.db['orders'].update_one({}, {'$set': {'status': 'completed'}, '$unset': {'items.0.line_status': ''}})
        self.assertEqual(self.admin._apply_status_change([self.oid], 'refunded'), (1, []))
        self.assertEqual(self.balance(), 62)

    def test_delivered_line_handler_can_refund(self):
        self.assertEqual(self.admin._apply_line_status_change([f'{self.oid}:0'], 'refunded'), (1, []))
        self.assertEqual(self.balance(), 62)
        self.assertEqual(self.admin._apply_line_status_change([f'{self.oid}:0'], 'processing')[0], 0)

    def test_partial_then_whole_refunds_remaining_only(self):
        self.db['orders'].update_one({}, {'$push': {'items': {'amount': 24, 'base_amount': 20,
                                                          'line_status': 'delivered'}}})
        self.assertEqual(self.refund([0]), (1, 12))
        self.assertEqual(self.db['orders'].find_one({})['status'], 'delivered')
        self.assertEqual(self.refund(), (1, 24))
        self.assertEqual(self.balance(), 86)

    def test_complaint_refund_marker_prevents_credit(self):
        self.db['orders'].update_one({}, {'$set': {'items.0.line_status': 'refunded',
                                                 'items.0.refunded_at': 'previous'}})
        self.assertEqual(self.refund(), (0, 0))
        self.assertEqual(self.balance(), 50)

    def test_existing_line_ledger_prevents_credit(self):
        self.db['transactions'].insert_one({'type': 'refund', 'reference': 'TEST123:LINE:0:REFUND', 'amount': 10})
        self.assertEqual(self.refund(), (1, 0))
        self.assertEqual(self.balance(), 50)

    def test_missing_base_or_owner_fails_without_status_change(self):
        for fields in ({'items.0.amount': None, 'charged_amount': None}, {'user_id': None}):
            with self.subTest(fields=fields):
                self.db['orders'].update_one({}, {'$set': fields})
                with self.assertRaises(ValueError):
                    self.refund()
                self.assertEqual(self.balance(), 50)
                self.assertEqual(self.db['orders'].find_one({})['status'], 'delivered')
                self.assertEqual(self.db['transactions'].count_documents({}), 0)

    def test_skipped_items_never_credited(self):
        self.db['orders'].update_one({}, {'$push': {'items': {'amount': 20, 'base_amount': 15,
                                                          'line_status': 'skipped_duplicate_in_cart'}}})
        with self.assertRaises(ValueError):
            self.refund([1])
        self.assertEqual(self.refund(), (1, 12))

    def test_status_failure_rolls_back_wallet_and_ledger(self):
        with patch.object(self.db['orders'], 'update_one', side_effect=RuntimeError('write failed')):
            with self.assertRaises(RuntimeError):
                self.refund()
        self.assertEqual(self.balance(), 50)
        self.assertEqual(self.db['transactions'].count_documents({}), 0)
        self.assertEqual(self.db['orders'].find_one({})['status'], 'delivered')

    def test_admin_http_refund_and_missing_price_error(self):
        from flask import Flask
        app = Flask(__name__)
        app.secret_key = 'test-only'
        app.register_blueprint(self.admin.admin_orders_bp)
        client = app.test_client()
        with client.session_transaction() as session:
            session['role'] = 'admin'
            session['user_id'] = 'admin'
        preview = client.get(f'/admin/orders/{self.oid}/refund-preview?item_index=0')
        self.assertEqual(preview.status_code, 200)
        self.assertEqual(preview.get_json()['amount'], 12)
        self.assertEqual(self.balance(), 50)
        self.assertEqual(client.get(f'/admin/orders/{self.oid}/refund-preview?item_index=99').status_code, 400)
        response = client.post(f'/admin/orders/{self.oid}/items/0/status', data={'line_status': 'refunded'})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.balance(), 62)
        with client.session_transaction() as session:
            self.assertEqual(session['_flashes'][-1][0], 'success')

    def test_preview_requires_admin(self):
        from flask import Flask
        app = Flask(__name__)
        app.secret_key = 'test-only'
        app.register_blueprint(self.admin.admin_orders_bp)
        self.assertEqual(app.test_client().get(f'/admin/orders/{self.oid}/refund-preview').status_code, 403)

    def test_late_missing_price_rolls_back_earlier_item(self):
        self.db['orders'].update_one({}, {'$push': {'items': {'line_status': 'delivered'}}})
        with self.assertRaises(ValueError):
            self.refund()
        self.assertEqual(self.balance(), 50)
        self.assertEqual(self.db['transactions'].count_documents({}), 0)

    def test_wallet_failure_rolls_back_ledger(self):
        with patch.object(self.db['balances'], 'update_one', side_effect=RuntimeError('wallet failed')):
            with self.assertRaises(RuntimeError):
                self.refund()
        self.assertEqual(self.db['transactions'].count_documents({}), 0)
        self.assertEqual(self.db['orders'].find_one({})['status'], 'delivered')


if __name__ == '__main__':
    unittest.main()
