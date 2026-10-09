"""Isolated deposit integration checks; no live database or payment calls."""
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import mongomock
from bson import ObjectId
from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
fake_db = types.ModuleType('db')
fake_db.db = mongomock.MongoClient().test
fake_admin = types.ModuleType('admin_balance')
fake_admin.ARKESEL_API_KEY = ''
fake_admin.SENDER_ID = ''
fake_admin._normalize_phone = lambda value: value
fake_admin._send_sms = Mock()
with patch.dict(sys.modules, {'db': fake_db, 'admin_balance': fake_admin}):
    spec = importlib.util.spec_from_file_location('deposit_under_test', ROOT / 'deposit.py')
    deposit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(deposit)

class DepositPageTests(unittest.TestCase):
    def setUp(self):
        for collection in fake_db.db.list_collection_names():
            fake_db.db[collection].delete_many({})
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        self.app.secret_key = 'test'
        self.app.register_blueprint(deposit.deposit_bp)
        self.app.add_url_rule('/login', endpoint='login.login', view_func=lambda: 'login')
        self.client = self.app.test_client()
        self.owner, self.other = ObjectId(), ObjectId()
        deposit.users_col.insert_many([
            {'_id': self.owner, 'role': 'customer', 'first_name': 'Owner', 'email': 'owner@example.com', 'deposit_page_token': 'owner-token'},
            {'_id': self.other, 'role': 'customer', 'email': 'other@example.com'},
            {'role': 'admin', 'manual_topup': {'active': True, 'name': 'Recipient', 'number': '', 'network': 'MTN'}},
        ])

    def login(self, oid):
        with self.client.session_transaction() as session:
            session['user_id'] = str(oid)
            session['role'] = 'customer'

    def initialize(self):
        response = Mock()
        response.json.return_value = {'status': True, 'data': {'authorization_url': 'https://checkout.paystack.com/test'}}
        with patch.object(deposit.requests, 'post', return_value=response) as call:
            result = self.client.post('/deposit/agent/owner-token/initialize', json={'amount': '100'})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(call.call_args.kwargs['json']['amount'], 10050)
        return deposit.transactions_col.find_one({'source': 'agent_deposit_page'})

    def test_generation_requires_login_and_is_stable(self):
        self.assertEqual(self.client.post('/agent/api/deposit-page').status_code, 401)
        self.login(self.other)
        first = self.client.post('/agent/api/deposit-page').get_json()['deposit_url']
        self.assertEqual(first, self.client.post('/agent/api/deposit-page').get_json()['deposit_url'])
        self.assertNotIn('owner-token', first)

    def test_public_page_history_is_scoped_even_with_other_login(self):
        self.login(self.other)
        deposit.transactions_col.insert_many([
            {'user_id': self.owner, 'type': 'deposit', 'amount': 20, 'reference': 'OWNER-HISTORY'},
            {'user_id': self.other, 'type': 'deposit', 'amount': 20, 'reference': 'OTHER-HISTORY'},
        ])
        result = self.client.get('/deposit/agent/owner-token')
        self.assertEqual(result.status_code, 200)
        html = result.get_data(as_text=True)
        self.assertIn('Agent: Owner', html)
        self.assertIn('OWNER-HISTORY', html)
        self.assertNotIn('OTHER-HISTORY', html)
        self.assertIn('/deposit/agent/owner-token/manual', html)
        self.assertEqual(result.headers['Cache-Control'], 'no-store')
        self.assertEqual(self.client.get('/deposit/agent/unknown').status_code, 404)

    def test_manual_submission_uses_link_owner_and_stays_on_page(self):
        self.login(self.other)
        result = self.client.post('/deposit/agent/owner-token/manual', data={'amount': '50', 'payer_name': 'Payer'})
        self.assertIn('/deposit/agent/owner-token', result.location)
        txn = deposit.transactions_col.find_one({'source': 'manual_topup'})
        self.assertEqual(txn['user_id'], self.owner)
        self.client.post('/deposit/agent/owner-token/manual', data={'amount': '50', 'payer_name': 'Payer'})
        self.assertEqual(deposit.transactions_col.count_documents({'source': 'manual_topup'}), 1)

    def test_rendered_javascript_and_api_controls(self):
        import re
        import subprocess
        from flask import render_template
        from jinja2 import ChoiceLoader, DictLoader
        self.app.jinja_loader = ChoiceLoader([DictLoader({'customer_sidebar.html': ''}), self.app.jinja_loader])
        self.app.add_url_rule('/agent/api/docs', endpoint='agent_api.agent_api_docs', view_func=lambda: '')
        self.app.add_url_rule('/agent/api/generate', endpoint='agent_api.agent_api_generate', view_func=lambda: '')
        with self.app.test_request_context('/'):
            api_html = render_template('agent_api_access.html', api_key='', deposit_url='https://example.com/deposit/agent/owner-token')
        self.assertIn('Generate Deposit Page', api_html)
        self.assertIn('Copy Link', api_html)
        public_html = self.client.get('/deposit/agent/owner-token').get_data(as_text=True)
        for html in [api_html, public_html]:
            scripts = re.findall(r'<script(?:\s[^>]*)?>(.*?)</script>', html, re.S)
            checked = subprocess.run(['node', '--check'], input='\n'.join(scripts), capture_output=True, text=True, encoding="utf-8")
            self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_payment_service_failure_can_be_retried(self):
        with patch.object(deposit.requests, 'post', side_effect=TimeoutError):
            result = self.client.post('/deposit/agent/owner-token/initialize', json={'amount': 100})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(deposit.transactions_col.find_one({})['status'], 'failed')
        txn = self.initialize()
        with patch.object(deposit.requests, 'get', side_effect=TimeoutError):
            self.client.get('/deposit/agent/owner-token/verify?reference=' + txn['reference'])
        self.assertEqual(deposit.balances_col.count_documents({}), 0)
        html = self.client.get('/deposit/agent/owner-token').get_data(as_text=True)
        self.assertIn('Check payment', html)

    def test_invalid_amounts_and_disabled_method(self):
        for amount in ['NaN', 'Infinity', '-1', '19', '1000001']:
            self.assertEqual(self.client.post('/deposit/agent/owner-token/initialize', json={'amount': amount}).status_code, 400)
        deposit.users_col.update_one({'role': 'admin'}, {'$set': {'deposit_methods.paystack_active': False}})
        self.assertEqual(self.client.post('/deposit/agent/owner-token/initialize', json={'amount': 100}).status_code, 400)

    def test_verification_credits_owner_once_and_rejects_legacy_route(self):
        self.login(self.other)
        txn = self.initialize()
        response = Mock()
        response.json.return_value = {'status': True, 'data': {'status': 'success', 'reference': txn['reference'], 'currency': 'GHS', 'amount': 10050}}
        url = '/deposit/agent/owner-token/verify?reference=' + txn['reference']
        with patch.object(deposit.requests, 'get', return_value=response):
            self.client.get(url)
            self.client.get(url)
            # Simulate retry after credit succeeded but transaction finalization failed.
            deposit.transactions_col.update_one({'_id': txn['_id']}, {'$set': {'status': 'processing'}})
            self.client.get(url)
        self.assertEqual(deposit.balances_col.find_one({'user_id': self.owner})['amount'], 100)
        self.assertIsNone(deposit.balances_col.find_one({'user_id': self.other}))
        self.assertEqual(self.client.get('/verify_transaction?reference=' + txn['reference']).status_code, 400)

    def test_verification_rejects_mismatched_amount_and_reference_owner(self):
        txn = self.initialize()
        response = Mock()
        response.json.return_value = {'status': True, 'data': {'status': 'success', 'reference': txn['reference'], 'currency': 'GHS', 'amount': 1}}
        with patch.object(deposit.requests, 'get', return_value=response):
            self.client.get('/deposit/agent/owner-token/verify?reference=' + txn['reference'])
        self.assertEqual(deposit.balances_col.count_documents({}), 0)
        self.client.get('/deposit/agent/owner-token/verify?reference=unknown')
        self.assertEqual(deposit.balances_col.count_documents({}), 0)

    def test_history_paginates_all_deposits_newest_first(self):
        from datetime import datetime, timedelta
        for index in range(27):
            deposit.transactions_col.insert_one({'user_id': self.owner, 'type': 'deposit', 'amount': 20,
                'reference': f'PAGE-REF-{index:02d}', 'created_at': datetime(2026, 10, 1) + timedelta(minutes=index)})
        deposit.transactions_col.insert_one({'user_id': self.other, 'type': 'deposit', 'amount': 99, 'reference': 'PRIVATE-OTHER'})
        with self.app.test_request_context('/'):
            first = deposit._deposit_history_context({'_id': self.owner}, 'owner-token', 1)
            last = deposit._deposit_history_context({'_id': self.owner}, 'owner-token', 3)
        self.assertEqual(first['history_total'], 27)
        self.assertEqual(first['history_pages'], 3)
        self.assertEqual([row['reference'] for row in first['deposit_history']], [f'PAGE-REF-{index:02d}' for index in range(26, 16, -1)])
        self.assertEqual(len(last['deposit_history']), 7)
        self.login(self.other)
        html = self.client.get('/deposit/agent/owner-token/history?page=2').get_data(as_text=True)
        self.assertIn('Page 2 of 3', html)
        self.assertIn('PAGE-REF-16', html)
        self.assertNotIn('PAGE-REF-26', html)
        self.assertNotIn('PRIVATE-OTHER', html)
        self.assertLess(html.index('PAGE-REF-16'), html.index('PAGE-REF-07'))

    def test_history_authentication_and_invalid_pages(self):
        self.assertEqual(self.client.get('/deposit/history').status_code, 401)
        self.assertEqual(self.client.get('/deposit/agent/invalid/history').status_code, 404)
        for page in ['bad', '-1', '999999']:
            result = self.client.get('/deposit/agent/owner-token/history?page=' + page)
            self.assertEqual(result.status_code, 200)
            self.assertIn('No deposit history yet.', result.get_data(as_text=True))
            self.assertEqual(result.headers['Cache-Control'], 'no-store')


if __name__ == '__main__':
    unittest.main()

