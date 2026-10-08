import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from datetime import datetime

import mongomock
from bson import ObjectId
from flask import Flask


class CustomerOrderLinesTests(unittest.TestCase):
    def setUp(self):
        fake_db = types.ModuleType('db')
        fake_db.db = mongomock.MongoClient().db
        spec = importlib.util.spec_from_file_location(
            'customer_orders_under_test', Path(__file__).resolve().parents[1] / 'orders.py')
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'db': fake_db}):
            spec.loader.exec_module(self.module)
        self.collection = self.module.orders_col
        self.user_id = ObjectId()
        self.app = Flask(__name__)
        self.app.secret_key = 'test'
        self.app.register_blueprint(self.module.orders_bp)
        self.app.add_url_rule('/login', endpoint='login.login', view_func=lambda: 'Login')
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session.update(role='customer', user_id=str(self.user_id))
        self.collection.insert_one({
            'user_id': self.user_id, 'order_id': 'BULK123', 'status': 'processing',
            'created_at': datetime(2026, 10, 5), 'total_amount': 30, 'paid_from': 'wallet',
            'items': [
                {'phone': '0241234567', 'amount': 10, 'serviceName': 'MTN', 'line_status': 'delivered'},
                {'phone': '0551234567', 'amount': 20, 'serviceName': 'Telecel', 'line_status': 'processing'},
            ],
        })

    def get_context(self, query=''):
        with patch.object(self.module, 'render_template', return_value='Rendered') as render:
            response = self.client.get('/customer/orders' + query)
        self.assertEqual(response.status_code, 200)
        return render.call_args.kwargs

    def test_bulk_lines_have_individual_amount_phone_and_status(self):
        context = self.get_context()
        self.assertEqual(context['total_count'], 2)
        self.assertEqual([o['total_amount'] for o in context['orders']], [10, 20])
        self.assertEqual([o['status'] for o in context['orders']], ['delivered', 'processing'])
        self.assertEqual([o['line_number'] for o in context['orders']], [1, 2])
        self.assertTrue(all(len(o['items']) == 1 for o in context['orders']))

    def test_filters_match_the_same_line(self):
        context = self.get_context('?status=delivered&phone=055')
        self.assertEqual(context['total_count'], 0)
        context = self.get_context('?status=delivered&phone=024&order_id=BULK')
        self.assertEqual(context['total_count'], 1)
        self.assertEqual(context['orders'][0]['items'][0]['phone'], '0241234567')

    def test_customer_isolation_and_line_pagination(self):
        self.collection.insert_one({'user_id': ObjectId(), 'items': [{'phone': 'PRIVATE'}]})
        self.collection.update_one({'order_id': 'BULK123'}, {'$set': {
            'items': [{'phone': str(i), 'amount': i, 'line_status': 'processing'} for i in range(12)]}})
        first = self.get_context()
        last = self.get_context('?page=99')
        self.assertEqual(first['total_count'], 12)
        self.assertEqual(len(first['orders']), 10)
        self.assertEqual(last['page'], 2)
        self.assertEqual([o['line_number'] for o in last['orders']], [11, 12])

    def test_single_legacy_order_and_completed_status(self):
        self.collection.delete_many({})
        self.collection.insert_one({'user_id': self.user_id, 'order_id': 'OLD',
                                    'status': 'completed', 'total_amount': 15,
                                    'items': [{'phone': '0241234567'}]})
        context = self.get_context('?status=delivered')
        self.assertEqual(context['total_count'], 1)
        self.assertEqual(context['orders'][0]['total_amount'], 15)
        self.assertEqual(context['orders'][0]['status'], 'delivered')

    def test_customer_sees_admin_line_status_change(self):
        self.collection.update_one({'order_id': 'BULK123'}, {'$set': {'items.1.line_status': 'failed'}})
        context = self.get_context('?status=failed')
        self.assertEqual(context['total_count'], 1)
        self.assertEqual(context['orders'][0]['line_number'], 2)

    def test_desktop_and_mobile_render_each_split_item(self):
        from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
        from flask import request
        context = self.get_context()
        env = Environment(loader=ChoiceLoader([
            DictLoader({'customer_sidebar.html': ''}),
            FileSystemLoader(Path(__file__).resolve().parents[1] / 'templates'),
        ]), autoescape=True)
        with self.app.test_request_context('/customer/orders'):
            html = env.get_template('orders.html').render(
                **context, request=request, url_for=lambda *args, **kwargs: '#')
        for value in ('Item 1 of 2', 'Item 2 of 2', '0241234567', '0551234567'):
            self.assertEqual(html.count(value), 2)
        self.assertIn('data-amt="10.0"', html)
        self.assertIn('data-amt="20.0"', html)

    def test_saved_bulk_children_display_separate_ids_and_live_status(self):
        from bulk_orders import split_documents
        parent = self.collection.find_one({'order_id': 'BULK123'})
        parent['charged_amount'] = 30
        self.collection.delete_many({})
        self.collection.insert_many(split_documents(parent))
        context = self.get_context()
        self.assertEqual([o['order_id'] for o in context['orders']], ['BULK123-2', 'BULK123-1'])
        self.assertEqual(context['total_count'], 2)
        self.assertEqual(sorted(o['line_number'] for o in context['orders']), [1, 2])
        self.assertTrue(all(o['line_count'] == 2 for o in context['orders']))
        self.collection.update_one({'order_id': 'BULK123-2'}, {'$set': {
            'status': 'refunded', 'items.0.line_status': 'refunded'}})
        refunded = self.get_context('?status=refunded')
        self.assertEqual(refunded['total_count'], 1)
        self.assertEqual(refunded['orders'][0]['order_id'], 'BULK123-2')

    def test_login_required(self):
        with self.client.session_transaction() as session:
            session.clear()
        self.assertEqual(self.client.get('/customer/orders').location, '/login')


if __name__ == '__main__':
    unittest.main()
