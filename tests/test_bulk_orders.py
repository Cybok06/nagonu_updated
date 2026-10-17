import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from bson import ObjectId
import mongomock
from bulk_orders import split_documents, persist_bulk, load_batch


class BulkOrderTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient().db
        self.user = ObjectId()
        self.db.balances.insert_one({'user_id': self.user, 'amount': 100})
        self.order = {'user_id': self.user, 'order_id': 'BATCH', 'status': 'pending',
                      'charged_amount': 30, 'total_amount': 30, 'profit_amount_total': 3,
                      'items': [{'phone': '0241111111', 'amount': 10, 'profit_amount': 1,
                                 'line_status': 'pending', 'provider_request_order_id': 'BP_one'},
                                {'phone': '0242222222', 'amount': 20, 'profit_amount': 2,
                                 'line_status': 'processing', 'provider_request_order_id': 'BP_two'}]}
        self.transaction = {'reference': 'BATCH', 'amount': 30, 'type': 'purchase'}

    def run_transaction(self, callback):
        # Test double rolls back persisted data, matching Mongo transactions.
        snapshots = {name: list(self.db[name].find({})) for name in ('orders', 'balances', 'transactions')}
        try:
            return callback(None)
        except Exception:
            for name, documents in snapshots.items():
                self.db[name].delete_many({})
                if documents:
                    self.db[name].insert_many(deepcopy(documents))
            raise

    def persist(self):
        with patch.object(self.db.client, 'start_session') as start:
            start.return_value.__enter__.return_value.with_transaction.side_effect = self.run_transaction
            return persist_bulk(self.db, self.order, self.transaction)

    def test_split_preserves_provider_and_pricing_snapshots(self):
        original = deepcopy(self.order)
        children = split_documents(self.order)
        self.assertEqual(self.order, original)
        self.assertEqual(len({c['order_id'] for c in children}), 2)
        self.assertTrue(all(c['order_id'].startswith('NAN') and '-' not in c['order_id'] for c in children))
        self.assertEqual([c['order_id'] for c in children], [c['order_id'] for c in split_documents(self.order)])
        self.assertTrue(all('batch_order_id' not in c and len(c['items']) == 1 for c in children))
        self.assertEqual([c['charged_amount'] for c in children], [10, 20])
        self.assertEqual([c['profit_amount_total'] for c in children], [1, 2])
        self.assertEqual([c['items'][0]['provider_request_order_id'] for c in children], ['BP_one', 'BP_two'])

    def test_one_debit_and_one_payment_for_separate_orders(self):
        self.persist()
        self.assertEqual(self.db.orders.count_documents({}), 2)
        self.assertEqual(self.db.transactions.count_documents({}), 1)
        self.assertEqual(self.db.balances.find_one({})['amount'], 70)

    def test_insertion_failure_rolls_back_debit_and_all_children(self):
        real_insert = self.db.orders.insert_many
        def interrupted(documents, **kwargs):
            real_insert(documents[:1], **kwargs)
            raise RuntimeError('interrupted')
        with patch.object(self.db.orders, 'insert_many', side_effect=interrupted):
            with self.assertRaises(RuntimeError):
                self.persist()
        self.assertEqual(self.db.orders.count_documents({}), 0)
        self.assertEqual(self.db.transactions.count_documents({}), 0)
        self.assertEqual(self.db.balances.find_one({})['amount'], 100)

    def test_insufficient_balance_creates_no_orders_or_payment(self):
        self.db.balances.update_one({}, {'$set': {'amount': 5}})
        with self.assertRaisesRegex(ValueError, 'Insufficient'):
            self.persist()
        self.assertEqual(self.db.orders.count_documents({}), 0)
        self.assertEqual(self.db.transactions.count_documents({}), 0)
        self.assertEqual(self.db.balances.find_one({})['amount'], 5)

    def test_skipped_item_has_no_charge_or_profit(self):
        self.order['items'][1]['line_status'] = 'skipped_duplicate_processing'
        self.order['charged_amount'] = 10
        self.persist()
        skipped = self.db.orders.find_one({'receipt_position': 2})
        self.assertEqual(skipped['charged_amount'], 0)
        self.assertEqual(skipped['profit_amount_total'], 0)
        self.assertEqual(skipped['status'], 'skipped')
        self.assertEqual(self.db.balances.find_one({})['amount'], 90)

    def test_batch_invoice_totals_and_customer_isolation(self):
        self.persist()
        order = load_batch(self.db.orders, 'BATCH', self.user)
        self.assertEqual(order['charged_amount'], 30)
        self.assertEqual(order['total_amount'], 30)
        self.assertEqual(len(order['items']), 2)
        self.assertEqual(order['order_ids'], [d['order_id'] for d in split_documents(self.order)])
        self.assertEqual([i['order_id'] for i in order['items']], order['order_ids'])
        self.assertIsNone(load_batch(self.db.orders, 'BATCH', ObjectId()))

    def test_receipt_renders_each_independent_order_id(self):
        from datetime import datetime
        from jinja2 import Environment, FileSystemLoader
        self.order['created_at'] = datetime(2026, 10, 11)
        self.order['paid_from'] = 'wallet'
        self.persist()
        receipt = load_batch(self.db.orders, 'BATCH', self.user)
        env = Environment(loader=FileSystemLoader(Path(__file__).resolve().parents[1] / 'templates'), autoescape=True)
        html = env.get_template('invoice.html').render(order=receipt, customer='Customer',
                url_for=lambda *args, **kwargs: '/invoice/BATCH')
        for item in receipt['items']:
            self.assertIn('Order ID: ' + item['order_id'], html)
            self.assertIn(item['phone'], html)
        self.assertIn('Receipt Reference:', html)
        self.assertIn('GHS 30.00', html)
        self.assertEqual(self.db.orders.count_documents({'order_id': 'BATCH'}), 0)

    def test_legacy_receipt_remains_readable(self):
        for position, item in enumerate(self.order['items'], 1):
            self.db.orders.insert_one({'order_id': f'OLD-{position}', 'batch_order_id': 'OLD',
                'batch_position': position, 'user_id': self.user, 'items': [item],
                'charged_amount': item['amount'], 'total_amount': item['amount']})
        receipt = load_batch(self.db.orders, 'OLD', self.user)
        self.assertEqual(receipt['order_ids'], ['OLD-1', 'OLD-2'])
        self.assertEqual([i['order_id'] for i in receipt['items']], receipt['order_ids'])

    def test_mismatched_payment_rejected_before_any_write(self):
        self.order['charged_amount'] = 40
        with self.assertRaisesRegex(ValueError, 'do not match'):
            self.persist()
        self.assertEqual(self.db.balances.find_one({})['amount'], 100)
        self.assertEqual(self.db.orders.count_documents({}), 0)

    def test_store_bulk_preserves_payment_and_never_debits_agent_wallet(self):
        from bulk_orders import persist_store_bulk
        self.order.update(store_slug='shop', paid_from='paystack_inline', paystack_reference='PS-paid')
        with patch.object(self.db.client, 'start_session') as start:
            start.return_value.__enter__.return_value.with_transaction.side_effect = self.run_transaction
            documents = persist_store_bulk(self.db, self.order)
        self.assertEqual(self.db.balances.find_one({})['amount'], 100)
        self.assertEqual(self.db.transactions.count_documents({}), 0)
        self.assertEqual(self.db.orders.count_documents({}), 2)
        self.assertEqual([len(doc['items']) for doc in documents], [1, 1])
        self.assertEqual([doc['charged_amount'] for doc in documents], [10, 20])
        self.assertEqual([doc['paystack_reference'] for doc in documents], ['PS-paid', 'PS-paid'])
        self.assertEqual(load_batch(self.db.orders, 'BATCH', store_slug='shop')['charged_amount'], 30)
        self.assertIsNone(load_batch(self.db.orders, 'BATCH', store_slug='another-shop'))

    def test_store_bulk_failure_rolls_back_all_order_records(self):
        from bulk_orders import persist_store_bulk
        real_insert = self.db.orders.insert_many
        def interrupted(documents, **kwargs):
            real_insert(documents[:1], **kwargs)
            raise RuntimeError('interrupted')
        with patch.object(self.db.client, 'start_session') as start, patch.object(self.db.orders, 'insert_many', side_effect=interrupted):
            start.return_value.__enter__.return_value.with_transaction.side_effect = self.run_transaction
            with self.assertRaises(RuntimeError):
                persist_store_bulk(self.db, self.order)
        self.assertEqual(self.db.orders.count_documents({}), 0)
        self.assertEqual(self.db.balances.find_one({})['amount'], 100)
