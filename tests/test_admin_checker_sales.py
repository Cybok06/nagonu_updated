import ast
from pathlib import Path
import unittest
from unittest.mock import Mock
import re

from bson import ObjectId
from jinja2 import Environment, FileSystemLoader


ROOT = Path(__file__).resolve().parents[1]


class AdminCheckerSalesTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse((ROOT / 'admin_wassce_checker.py').read_text(encoding='utf-8'))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                        and n.name == '_attach_sale_details')
        self.collections = {name: Mock() for name in ('users', 'stores', 'store_checker_purchases', 'public_checker_purchases')}
        for collection in self.collections.values():
            collection.find.return_value = []
        env = {'db': self.collections, 'ObjectId': ObjectId, 're': re}
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'admin_wassce_checker.py', 'exec'), env)
        self.attach = env['_attach_sale_details']

    def test_legacy_dashboard_resolves_user_phone(self):
        user_id = ObjectId()
        self.collections['users'].find.return_value = [{'_id': user_id, 'phone': '0241234567'}]
        rows = [{'_id': ObjectId(), 'status': 'sold', 'sold_to': str(user_id)}]
        self.attach(rows)
        self.assertEqual(rows[0]['buyer_phone'], '0241234567')
        self.assertEqual(rows[0]['sale_channel_label'], 'Customer Dashboard')
        self.assertIn(user_id, self.collections['users'].find.call_args.args[0]['_id']['$in'])

    def test_legacy_store_resolves_buyer_and_owner_separately(self):
        checker_id, owner_id = ObjectId(), ObjectId()
        self.collections['store_checker_purchases'].find.return_value = [
            {'checker_id': str(checker_id), 'phone': '0551234567', 'store_slug': 'shop', 'store_owner_id': owner_id}]
        self.collections['stores'].find.return_value = [
            {'slug': 'shop', 'name': 'My Shop', 'owner_id': owner_id}]
        self.collections['users'].find.return_value = [{'_id': owner_id, 'phone': '0201234567'}]
        rows = [{'_id': checker_id, 'status': 'sold', 'sold_to_store': 'shop'}]
        self.attach(rows)
        self.assertEqual(rows[0]['buyer_phone'], '0551234567')
        self.assertEqual(rows[0]['sale_store_phone'], '0201234567')
        self.assertEqual(rows[0]['sale_store_name'], 'My Shop')
        self.assertEqual(rows[0]['sale_channel_label'], 'Store Page')

    def test_snapshot_survives_missing_customer_and_public_phone_is_preserved(self):
        rows = [
            {'_id': ObjectId(), 'status': 'sold', 'sold_channel': 'customer_dashboard', 'sold_phone': '0241234567'},
            {'_id': ObjectId(), 'status': 'sold', 'sold_channel': 'public_results_checker', 'sold_to': '0551234567'},
            {'_id': ObjectId(), 'status': 'sold'},
        ]
        self.attach(rows)
        self.assertEqual(rows[0]['buyer_phone'], '0241234567')
        self.assertEqual(rows[1]['buyer_phone'], '0551234567')
        self.assertEqual(rows[1]['sale_channel_label'], 'Results Checker Page')
        self.assertEqual(rows[2]['buyer_phone'], 'Unavailable')
        self.assertEqual(rows[2]['sale_channel_label'], 'Unknown')

    def test_unsold_inventory_does_not_query_purchase_records(self):
        rows = [{'_id': ObjectId(), 'status': 'not_sold'}]
        self.attach(rows)
        self.assertEqual(rows[0]['buyer_phone'], '')
        for collection in self.collections.values():
            collection.find.assert_not_called()

    def test_template_displays_buyer_and_store_owner(self):
        env = Environment(loader=FileSystemLoader(ROOT / 'templates'), autoescape=True)
        env.globals.update(url_for=lambda *args, **kwargs: '#', get_flashed_messages=lambda **kwargs: [],
                           session={'role': 'admin'}, request=Mock(endpoint='admin_wassce_checker.admin_wassce_checker'))
        from datetime import datetime
        row = dict(_id=ObjectId(), status='sold', type='wassce', amount=20, profit=2,
                   message='Checker', created_at=datetime.now(), sale_channel_class='store',
                   sale_channel_label='Store Page', buyer_phone='0551234567',
                   sale_store_name='My Shop', sale_store_phone='0201234567')
        html = env.get_template('admin_wassce_checker.html').render(
            messages=[row], checker_prices={'wassce': 20, 'bece': 15}, undelivered_orders_count=0,
            pending_manual_deposits_count=0, pending_complaints_count=0)
        for value in ('Delivered to:', '0551234567', 'tel:0551234567', 'Store owner phone:', '0201234567', 'Sold via Store Page'):
            self.assertIn(value, html)

    def test_store_recipient_snapshot_is_used_instead_of_owner_phone(self):
        owner_id = ObjectId()
        self.collections['stores'].find.return_value = [{'slug': 'shop', 'owner_id': owner_id}]
        self.collections['users'].find.return_value = [{'_id': owner_id, 'phone': '0201234567'}]
        row = {'_id': ObjectId(), 'status': 'sold', 'sold_channel': 'store_page',
               'sold_to_store': 'shop', 'sold_phone': '+233 50 057 2478'}
        self.attach([row])
        self.assertEqual(row['buyer_phone'], '0500572478')
        self.assertEqual(row['sale_store_phone'], '0201234567')

    def test_missing_store_recipient_is_not_replaced_by_owner_phone(self):
        owner_id = ObjectId()
        self.collections['stores'].find.return_value = [{'slug': 'shop', 'owner_id': owner_id}]
        self.collections['users'].find.return_value = [{'_id': owner_id, 'phone': '0201234567'}]
        row = {'_id': ObjectId(), 'status': 'sold', 'sold_to_store': 'shop'}
        self.attach([row])
        self.assertEqual(row['buyer_phone'], 'Unavailable')

    def test_public_purchase_record_recovers_older_recipient(self):
        checker_id = ObjectId()
        self.collections['public_checker_purchases'].find.return_value = [
            {'checker_id': checker_id, 'phone': '0593568977'}]
        row = {'_id': checker_id, 'status': 'sold', 'sold_channel': 'public_results_checker'}
        self.attach([row])
        self.assertEqual(row['buyer_phone'], '0593568977')

    def test_identifier_is_not_displayed_as_phone(self):
        row = {'_id': ObjectId(), 'status': 'sold', 'sold_channel': 'public_results_checker',
               'sold_to': str(ObjectId())}
        self.attach([row])
        self.assertEqual(row['buyer_phone'], 'Unavailable')


if __name__ == '__main__':
    unittest.main()
