"""Exercise store tracking handlers with isolated persisted child orders."""
import ast
from datetime import datetime
from pathlib import Path
import unittest
import mongomock
from flask import Flask, Blueprint, jsonify, request
from bulk_orders import split_documents, load_batch

class StoreBulkTrackingTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient().db
        self.app = Flask(__name__)
        bp = Blueprint('stores', __name__)
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'routes/store_page.py').read_text(encoding='utf-8'))
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in {'api_store_order', 'api_store_order_by_ref'}]
        env = dict(stores_bp=bp, orders_col=self.db.orders, load_batch=load_batch, jsonify=jsonify, request=request, datetime=datetime)
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'store_page.py', 'exec'), env)
        self.app.register_blueprint(bp)
        self.client = self.app.test_client()
        self.db.orders.insert_many(split_documents({
            'order_id': 'STORE-BATCH', 'user_id': 'private-user', 'store_owner_id': 'private-owner',
            'store_slug': 'shop', 'paystack_reference': 'PS-paid', 'charged_amount': 30, 'total_amount': 30,
            'status': 'processing', 'created_at': datetime(2026, 10, 9), 'debug': {'private': True},
            'items': [{'phone': '0241111111', 'amount': 10, 'line_status': 'processing'},
                      {'phone': '0242222222', 'amount': 20, 'line_status': 'processing'}],
        }))

    def test_original_checkout_id_returns_whole_batch(self):
        result = self.client.get('/api/store-order/STORE-BATCH')
        self.assertEqual(result.status_code, 200)
        order = result.get_json()['order']
        self.assertEqual(order['total_amount'], 30)
        self.assertEqual(order['order_ids'], ['STORE-BATCH-1', 'STORE-BATCH-2'])
        self.assertEqual(len(order['items']), 2)
        for private in ['_id', 'user_id', 'store_owner_id', 'debug']:
            self.assertNotIn(private, order)

    def test_child_id_returns_only_that_order(self):
        order = self.client.get('/api/store-order/STORE-BATCH-2').get_json()['order']
        self.assertEqual(order['total_amount'], 20)
        self.assertEqual(len(order['items']), 1)
        self.assertEqual(order['items'][0]['phone'], '0242222222')

    def test_payment_reference_returns_original_id_and_is_store_scoped(self):
        result = self.client.get('/api/store-order-by-ref/shop?ref=PS-paid').get_json()
        self.assertEqual(result['order_id'], 'STORE-BATCH')
        self.assertFalse(self.client.get('/api/store-order-by-ref/other?ref=PS-paid').get_json()['exists'])

    def test_missing_order_still_returns_not_found(self):
        self.assertEqual(self.client.get('/api/store-order/unknown').status_code, 404)
