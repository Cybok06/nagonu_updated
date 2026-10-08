import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from copy import deepcopy

import mongomock
import pandas  # Load native dependencies before the isolated module patch.
from bson import ObjectId


class Session:
    def __init__(self, database):
        self.database = database

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def with_transaction(self, callback):
        # Model transaction rollback in memory; production uses MongoDB sessions.
        snapshot = {name: deepcopy(list(self.database.raw[name].find({})))
                    for name in self.database.raw.list_collection_names()}
        try:
            return callback(self)
        except Exception:
            for name in self.database.raw.list_collection_names():
                self.database.raw[name].delete_many({})
                if snapshot.get(name):
                    self.database.raw[name].insert_many(snapshot[name])
            raise


class Collection:
    def __init__(self, raw):
        self.raw = raw

    def __getattr__(self, name):
        def call(*args, **kwargs):
            kwargs.pop('session', None)
            return getattr(self.raw, name)(*args, **kwargs)
        return call


class Database:
    def __init__(self):
        self.raw = mongomock.MongoClient().campus
        self.client = types.SimpleNamespace(start_session=lambda: Session(self))
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, Collection(self.raw[name]))


class CampusRefundTests(unittest.TestCase):
    def setUp(self):
        self.campus = Database()
        fake = types.ModuleType('db')
        fake.db = mongomock.MongoClient().main
        fake.campus_db = self.campus
        self.main = fake.db
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
        self.refunds = load('campus_refunds', 'campus_refunds.py')
        patcher = patch.dict(sys.modules, {'campus_refunds': self.refunds})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.admin = load('campus_admin_under_test', 'admin_orders.py')
        self.admin.bump_orders_cache_version = lambda: None
        self.oid = ObjectId()
        self.user_id = ObjectId()
        self.campus['balances'].insert_one({'user_id': self.user_id, 'amount': 50})
        self.campus['provider_accounts'].insert_one({'provider': 'provider_wallet', 'balance': 100})
        self.campus['orders'].insert_one({'_id': self.oid, 'order_id': 'CAMPUS123',
            'user_id': self.user_id, 'paid_from': 'wallet', 'status': 'processing', 'items': [{
                'serviceName': 'MTN NORMAL', 'value': '5GB', 'amount': 20,
                'base_amount': 15, 'phone': '0241234567', 'line_status': 'processing'}]})

    def refund(self, index=0):
        return self.admin._apply_line_status_change(
            [f'campus:{self.oid}:{index}'], 'refunded', orders_collection=self.campus['orders'],
            target_source='campus', actor_admin_id='admin')

    def test_credit_base_cost_and_history_not_main_wallet(self):
        count, errors = self.refund()
        self.assertEqual((count, errors), (1, []))
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 115)
        log = self.campus['provider_transactions'].find_one({})
        self.assertEqual((log['direction'], log['reason'], log['amount']), ('CREDIT', 'REFUNDED', 15))
        self.assertEqual(log['meta']['actor_admin_id'], 'admin')
        self.assertEqual(self.campus['orders'].find_one({})['status'], 'refunded')
        self.assertEqual(self.main.balances.count_documents({}), 0)
        self.assertEqual(self.main.transactions.count_documents({}), 0)
        self.assertEqual(self.campus['balances'].find_one({'user_id': self.user_id})['amount'], 70)
        wallet_log = self.campus['transactions'].find_one({'type': 'refund'})
        self.assertEqual((wallet_log['user_id'], wallet_log['amount']), (self.user_id, 20))
        self.assertEqual(wallet_log['gateway'], 'Wallet')
        self.assertEqual(self.campus['orders'].find_one({})['items'][0]['wallet_refund_amount'], 20)

    def test_repeat_refund_does_not_credit_again(self):
        self.refund()
        self.refund()
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 115)
        self.assertEqual(self.campus['provider_transactions'].count_documents({}), 1)
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 70)
        self.assertEqual(self.campus['transactions'].count_documents({'type': 'refund'}), 1)

    def test_refund_is_visible_in_campus_balance_history(self):
        from flask import Flask
        self.refund()
        spec = importlib.util.spec_from_file_location('campus_balance_under_test',
            Path(__file__).resolve().parents[1] / 'routes/admin_campus_balance.py')
        balance = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(balance)
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(balance.admin_campus_balance_bp)
        client = app.test_client()
        with client.session_transaction() as session:
            session['role'] = 'admin'
        with patch.object(balance, 'render_template', return_value='History') as render:
            response = client.get('/admin/campus-balance?direction=CREDIT&reason=REFUNDED')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(render.call_args.kwargs['balance'], 115)
        self.assertEqual(render.call_args.kwargs['transactions'][0]['reason'], 'REFUNDED')

    def test_original_debit_overrides_changed_base_price(self):
        self.campus['provider_transactions'].insert_one({'provider': 'provider_wallet',
            'direction': 'DEBIT', 'amount': 12, 'order_id': 'CAMPUS123', 'line_index': 0})
        self.refund()
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 112)

    def test_legacy_offer_lookup(self):
        self.campus['orders'].update_one({}, {'$unset': {'items.0.base_amount': ''}})
        self.campus['services'].insert_one({'name': 'MTN NORMAL', 'offers': [
            {'value': "{'id': 5, 'volume': 5000}", 'amount': 13}]})
        self.assertEqual(self.refund(), (1, []))
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 113)

    def test_missing_base_price_does_not_mark_refunded(self):
        self.campus['orders'].update_one({}, {'$unset': {'items.0.base_amount': ''}})
        count, errors = self.refund()
        self.assertEqual(count, 0)
        self.assertTrue(errors)
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 100)
        self.assertEqual(self.campus['orders'].find_one({})['status'], 'processing')

    def test_delivered_and_failed_items_can_be_refunded(self):
        for status in ('failed', 'delivered'):
            with self.subTest(status=status):
                self.campus['orders'].update_one({}, {'$set': {'items.0.line_status': status,
                    'status': status}, '$unset': {'items.0.refunded_at': ''}})
                self.campus['provider_transactions'].delete_many({})
                self.campus['transactions'].delete_many({})
                self.assertEqual(self.refund(), (1, []))

    def test_whole_order_after_partial_refund_credits_only_remaining_line(self):
        self.campus['orders'].update_one({}, {'$push': {'items': {
            'line_status': 'failed', 'base_amount': 7, 'amount': 10}}})
        self.refund()
        count, errors = self.admin._apply_status_change([self.oid], 'refunded',
            orders_collection=self.campus['orders'], source='campus')
        self.assertEqual((count, errors), (1, []))
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 122)
        self.assertEqual(self.campus['provider_transactions'].count_documents({}), 2)
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 80)
        self.assertEqual(self.campus['transactions'].count_documents({'type': 'refund'}), 2)

    def test_failed_credit_rolls_back_ledger_and_status(self):
        with patch.object(self.campus['provider_accounts'], 'update_one', side_effect=RuntimeError('Unavailable')):
            count, errors = self.refund()
        self.assertEqual(count, 0)
        self.assertTrue(errors)
        self.assertEqual(self.campus['provider_transactions'].count_documents({}), 0)
        self.assertEqual(self.campus['orders'].find_one({})['status'], 'processing')
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 50)
        self.assertEqual(self.campus['transactions'].count_documents({}), 0)
        self.assertEqual(self.refund(), (1, []))

    def test_wallet_failure_rolls_back_both_refunds(self):
        with patch.object(self.campus['balances'], 'update_one', side_effect=RuntimeError('Unavailable')):
            count, errors = self.refund()
        self.assertEqual(count, 0)
        self.assertTrue(errors)
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 100)
        self.assertEqual(self.campus['provider_transactions'].count_documents({}), 0)
        self.assertEqual(self.campus['transactions'].count_documents({}), 0)
        self.assertEqual(self.campus['orders'].find_one({})['status'], 'processing')
        self.assertEqual(self.refund(), (1, []))

    def test_split_order_uses_its_charge_not_entire_batch(self):
        self.campus['orders'].update_one({}, {'$set': {'batch_id': 'BATCH',
            'batch_position': 2, 'charged_amount': 18}})
        self.campus['transactions'].insert_one({'user_id': self.user_id,
            'reference': 'BATCH', 'amount': 48, 'type': 'purchase',
            'status': 'success', 'gateway': 'Wallet'})
        self.campus['provider_transactions'].insert_one({'provider': 'provider_wallet',
            'direction': 'DEBIT', 'amount': 11, 'order_id': 'CAMPUS123', 'line_index': 2})
        self.assertEqual(self.refund(), (1, []))
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 68)
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 111)

    def test_unrelinked_batch_provider_debit(self):
        self.campus['orders'].update_one({}, {'$set': {'batch_id': 'BATCH', 'batch_position': 3}})
        self.campus['provider_transactions'].insert_one({'provider': 'provider_wallet',
            'direction': 'DEBIT', 'amount': 9, 'order_id': 'BATCH', 'line_index': 3})
        self.assertEqual(self.refund(), (1, []))
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 109)

    def test_one_based_legacy_provider_debit(self):
        self.campus['provider_transactions'].insert_one({'provider': 'provider_wallet',
            'direction': 'DEBIT', 'amount': 13, 'order_id': 'CAMPUS123', 'line_index': 1})
        self.refund()
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 113)

    def test_wallet_snapshot_survives_changed_item_price(self):
        self.campus['orders'].update_one({}, {'$set': {'wallet_user_id': self.user_id,
            'items.0.wallet_debit_amount': 17, 'items.0.amount': 99}})
        self.refund()
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 67)

    def test_zero_charge_and_missing_payer_do_not_refund(self):
        for fields in ({'charged_amount': 0}, {'user_id': None}):
            with self.subTest(fields=fields):
                self.campus['orders'].update_one({}, {'$set': fields})
                count, errors = self.refund()
                self.assertEqual(count, 0)
                self.assertTrue(errors)
                self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 100)
                self.assertEqual(self.campus['balances'].find_one({})['amount'], 50)

    def test_paystack_store_order_has_no_wallet_deduction_to_return(self):
        self.campus['orders'].update_one({}, {'$set': {
            'store_slug': 'shop', 'paid_from': 'paystack_inline'}})
        self.assertEqual(self.refund(), (1, []))
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 50)
        self.assertEqual(self.campus['transactions'].count_documents({}), 0)
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 115)

    def test_historical_provider_only_refund_is_not_recredited(self):
        self.campus['orders'].update_one({}, {'$set': {'items.0.line_status': 'refunded'}})
        self.refund()
        self.assertEqual(self.campus['balances'].find_one({})['amount'], 50)
        self.assertEqual(self.campus['provider_accounts'].find_one({})['balance'], 100)

    def test_customer_history_in_separate_campus_app_shows_wallet_refund(self):
        from flask import Flask
        self.refund()
        # Load the other app's actual history endpoint against the Campus DB.
        campus_root = Path(__file__).resolve().parents[2] / 'campus_data-main'
        spec = importlib.util.spec_from_file_location('campus_customer_history_test',
                                                     campus_root / 'transactions.py')
        history = importlib.util.module_from_spec(spec)
        fake = types.ModuleType('db')
        fake.db = self.campus
        with patch.dict(sys.modules, {'db': fake}):
            spec.loader.exec_module(history)
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(history.transactions_bp)
        client = app.test_client()
        with client.session_transaction() as session:
            session['role'] = 'customer'
            session['user_id'] = str(self.user_id)
        # mongomock does not implement the production $toDouble aggregate.
        with patch.object(history, '_sum_amount', return_value=0), patch.object(
            history, 'render_template', return_value='History') as render:
            response = client.get('/customer/transactions')
        self.assertEqual(response.status_code, 200)
        log = render.call_args.kwargs['transactions'][0]
        self.assertEqual((log['type'], log['amount'], log['user_id']), ('refund', 20, self.user_id))


if __name__ == '__main__':
    unittest.main()
