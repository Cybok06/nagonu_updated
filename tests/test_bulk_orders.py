import unittest
from copy import deepcopy
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
        self.assertEqual([c['order_id'] for c in children], ['BATCH-1', 'BATCH-2'])
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
        skipped = self.db.orders.find_one({'order_id': 'BATCH-2'})
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
        self.assertEqual(order['order_ids'], ['BATCH-1', 'BATCH-2'])
        self.assertIsNone(load_batch(self.db.orders, 'BATCH', ObjectId()))

    def test_mismatched_payment_rejected_before_any_write(self):
        self.order['charged_amount'] = 40
        with self.assertRaisesRegex(ValueError, 'do not match'):
            self.persist()
        self.assertEqual(self.db.balances.find_one({})['amount'], 100)
        self.assertEqual(self.db.orders.count_documents({}), 0)
