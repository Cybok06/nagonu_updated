import hashlib
import hmac
import json
import os
import sys
import types
import unittest
from datetime import datetime, timedelta
import importlib.util
from pathlib import Path
from unittest.mock import patch

import mongomock
import requests
from flask import Flask
from bson import ObjectId

import bundle_portal as api
import bundle_portal_orders as orders


class BundlePortalClientTests(unittest.TestCase):
    def test_sizes_preserve_fractional_gb_and_convert_mb(self):
        for value, item, expected in [({'volume': 1500}, {}, 1.5),
                                      ({'size_gb': 0.5}, {}, 0.5),
                                      ({'volume': 5}, {}, 5),
                                      ({}, {'value': '500MB'}, 0.5),
                                      ({}, {'value': '5 GB'}, 5),
                                      ({'size_gb': 'NaN'}, {}, None), ({}, {}, None)]:
            with self.subTest(value=value, item=item):
                self.assertEqual(api.package_size(value, item), expected)

    @patch.object(api, 'call')
    def test_all_routes_verify_catalog_and_recipient_before_purchase(self, call):
        for provider, network in api.PROVIDERS.items():
            with self.subTest(provider=provider):
                call.reset_mock()
                call.side_effect = [
                    {'success': True, 'data': {'bundles': [{'size_gb': 1.5, 'network': network}]}},
                    {'success': True, 'data': {'can_order': True}},
                    {'success': True, 'data': {'status': 'cached'}},
                ]
                result = api.submit(provider, '0241234567', 1.5, 'BP_reference')
                self.assertTrue(result['success'])
                self.assertEqual([c.args[0] for c in call.call_args_list], ['get_bundles', 'verify_number', 'place_order'])
                self.assertTrue(all(c.kwargs['network'] == network for c in call.call_args_list))
                self.assertEqual(call.call_args.kwargs['order_id'], 'BP_reference')

    @patch.object(api, 'call')
    def test_unavailable_size_and_blocked_number_do_not_purchase(self, call):
        call.return_value = {'success': True, 'data': {'bundles': []}}
        self.assertEqual(api.submit('bundleportal_mtn', '0241234567', 5, 'ref')['code'], 'bundle_unavailable')
        self.assertEqual(call.call_count, 1)
        call.reset_mock()
        call.side_effect = [
            {'success': True, 'data': {'bundles': [{'size_gb': 5}]}},
            {'success': True, 'data': {'can_order': False}},
        ]
        self.assertEqual(api.submit('bundleportal_mtn', '0241234567', 5, 'ref')['code'], 'recipient_blocked')
        self.assertEqual(call.call_count, 2)

    @patch.object(api, 'call', return_value={'success': True})
    def test_uncertain_purchase_retry_reuses_reference_without_blocking_on_own_order(self, call):
        api.submit('bundleportal_mtn2', '0241234567', 5, 'original_ref', retry_purchase=True)
        call.assert_called_once_with('place_order', network='mtn_2', recipient='0241234567', package_size=5, order_id='original_ref')

    @patch.dict(os.environ, {'BUNDLE_PORTAL_KEY': ''})
    @patch.object(api.requests, 'post')
    def test_missing_credentials_never_calls_network(self, post):
        self.assertEqual(api.call('get_bundles')['code'], 'not_configured')
        post.assert_not_called()

    @patch.dict(os.environ, {'BUNDLE_PORTAL_KEY': 'test-key'})
    @patch.object(api.requests, 'post', side_effect=requests.Timeout())
    def test_timeout_is_uncertain_and_not_automatically_retried(self, post):
        self.assertEqual(api.call('place_order', order_id='same-ref')['code'], 'unknown_outcome')
        self.assertEqual(post.call_count, 1)


class BundlePortalOrderTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient().test
        fake = types.ModuleType('db')
        fake.db = self.db
        fake.campus_db = mongomock.MongoClient().campus
        for p in (patch.dict(sys.modules, {'db': fake}),
                  patch.dict(os.environ, {'BUNDLE_PORTAL_WEBHOOK_SECRET': 'test-secret'}),
                  patch.object(orders, 'invalidate_orders'),
                  patch.object(orders, 'Thread', side_effect=self.immediate_worker)):
            p.start()
            self.addCleanup(p.stop)
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(orders.bundle_portal_bp)
        self.client = app.test_client()
        self.db.orders.insert_one({'order_id': 'ORDER1', 'status': 'processing', 'items': [{
            'provider': 'bundleportal_mtn2', 'provider_request_order_id': 'BP_ref',
            'phone': '0241234567', 'provider_network': 'mtn_2', 'provider_gb_size': 5,
            'line_status': 'pending', 'api_status': 'queued',
        }]})
        self.job = {'provider': 'bundleportal_mtn2', 'provider_request_order_id': 'BP_ref',
                    'phone': '0241234567', 'bundle_portal_gb_size': 5}

    @staticmethod
    def immediate_worker(target, args, **kwargs):
        return types.SimpleNamespace(start=lambda: target(*args))

    def test_early_unknown_callback_is_saved_for_later_reconciliation(self):
        self.assertEqual(self.callback(order_id='BP_not_saved_yet').status_code, 202)
        self.assertEqual(self.db.bundleportal_events.count_documents({}), 1)
        self.assertEqual(self.document()['status'], 'processing')

    def test_campus_reference_does_not_need_a_prefix(self):
        campus = sys.modules['db'].campus_db
        document = self.document()
        document.pop('_id')
        document['items'][0]['provider_request_order_id'] = 'legacy_campus_ref'
        campus.orders.insert_one(document)
        self.assertEqual(self.callback(order_id='legacy_campus_ref').status_code, 200)
        self.assertEqual(campus.orders.find_one({})['status'], 'delivered')

    def test_callback_is_acknowledged_when_worker_fails(self):
        with patch.object(orders, 'apply_status_event', side_effect=RuntimeError('interrupted')), patch.object(orders.logging, 'getLogger'):
            self.assertEqual(self.callback().status_code, 200)
        self.assertEqual(self.db.bundleportal_events.count_documents({}), 1)

    def document(self):
        return self.db.orders.find_one({'order_id': 'ORDER1'})

    def callback(self, status='completed', **overrides):
        data = {'event': f'order.{status}', 'status': status, 'order_id': 'BP_ref',
                'network': 'mtn_2', 'recipient': '0241234567', 'reference': 'provider-ref', **overrides}
        raw = json.dumps(data).encode()
        signature = 'sha256=' + hmac.new(b'test-secret', raw, hashlib.sha256).hexdigest()
        return self.client.post('/webhooks/bundleportal', data=raw, content_type='application/json',
                                headers={'X-BundlePortal-Signature': signature})

    def test_invalid_signature_rejected_without_writes(self):
        response = self.client.post('/webhooks/bundleportal', json={'status': 'completed'})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(self.document()['items'][0]['line_status'], 'pending')

    def test_completed_callback_updates_line_and_parent_and_is_idempotent(self):
        self.assertEqual(self.callback().status_code, 200)
        self.assertEqual(self.callback().status_code, 200)
        self.assertEqual(self.document()['status'], 'delivered')
        self.assertEqual(self.document()['items'][0]['line_status'], 'delivered')
        self.assertEqual(self.db.bundleportal_events.count_documents({}), 1)

    def test_wrong_route_or_recipient_rejected(self):
        self.assertEqual(self.callback(network='mtn').status_code, 400)
        self.assertEqual(self.callback(recipient='0240000000').status_code, 400)
        self.assertEqual(self.document()['status'], 'processing')

    def test_shared_webhook_routes_campus_reference_to_campus_database(self):
        campus = sys.modules['db'].campus_db
        document = self.document()
        document.pop('_id')
        document['items'][0]['provider_request_order_id'] = 'BPC_test'
        campus.orders.insert_one(document)
        self.assertEqual(self.callback(order_id='BPC_test').status_code, 200)
        self.assertEqual(campus.orders.find_one({})['status'], 'delivered')
        self.assertEqual(self.document()['status'], 'processing')

    def test_ishare_webhook_alias_updates_the_correct_line(self):
        self.db.orders.update_one({'order_id': 'ORDER1'}, {'$set': {
            'items.0.provider': 'bundleportal_ishare', 'items.0.provider_network': 'airteltigo',
            'items.0.phone': '0271234567',
        }})
        self.assertEqual(self.callback(network='ishare', recipient='0271234567').status_code, 200)
        self.assertEqual(self.document()['status'], 'delivered')

    def test_telecel_webhook_updates_the_correct_line(self):
        self.db.orders.update_one({'order_id': 'ORDER1'}, {'$set': {
            'items.0.provider': 'bundleportal_telecel', 'items.0.provider_network': 'telecel',
            'items.0.phone': '0201234567',
        }})
        self.assertEqual(self.callback(network='telecel', recipient='0201234567').status_code, 200)
        self.assertEqual(self.document()['status'], 'delivered')

    def test_provider_refund_after_delivery_does_not_claim_local_wallet_refund(self):
        self.callback()
        self.assertEqual(self.callback('refunded').status_code, 200)
        self.assertEqual(self.document()['items'][0]['line_status'], 'failed')
        self.assertEqual(self.document()['items'][0]['provider_status'], 'refunded')

    def test_local_refund_is_not_reverted_by_late_callback(self):
        self.db.orders.update_one({'order_id': 'ORDER1'}, {'$set': {'items.0.line_status': 'refunded'}})
        self.callback()
        self.assertEqual(self.document()['items'][0]['line_status'], 'refunded')

    def test_failure_flags_local_refund_without_claiming_wallet_was_credited(self):
        self.assertEqual(self.callback('failed').status_code, 200)
        self.assertEqual(self.document()['status'], 'failed')
        self.assertTrue(self.document()['items'][0]['refund_required'])
        self.assertEqual(self.db.transactions.count_documents({}), 0)

    @patch.object(orders, 'submit', return_value={'success': True, 'data': {'status': 'cached'}})
    def test_cached_is_processing_and_duplicate_worker_does_not_respend(self, submit):
        orders.process_job(self.db.orders, 'ORDER1', self.job)
        orders.process_job(self.db.orders, 'ORDER1', self.job)
        self.assertEqual(self.document()['items'][0]['line_status'], 'processing')
        self.assertEqual(submit.call_count, 1)

    def test_callback_arriving_before_submit_response_is_not_overwritten(self):
        def early_callback(*args, **kwargs):
            self.assertEqual(self.callback().status_code, 200)
            return {'success': True, 'data': {'status': 'processing'}}
        with patch.object(orders, 'submit', side_effect=early_callback):
            orders.process_job(self.db.orders, 'ORDER1', self.job)
        self.assertEqual(self.document()['status'], 'delivered')
        self.assertEqual(self.document()['items'][0]['line_status'], 'delivered')

    def test_transient_errors_stay_processing_for_review(self):
        for payload in ({'success': False, 'http_status': 429},
                        {'success': False, 'http_status': 409},
                        {'success': False, 'code': 'unknown_outcome'}):
            self.db.orders.update_one({'order_id': 'ORDER1'}, {'$set': {'items.0.api_status': 'queued'}})
            with patch.object(orders, 'submit', return_value=payload):
                orders.process_job(self.db.orders, 'ORDER1', self.job)
            item = self.document()['items'][0]
            self.assertEqual(item['api_status'], 'review_required')
            self.assertEqual(item['line_status'], 'processing')
            self.assertFalse(item['refund_required'])

    def test_catalog_and_retry_require_admin(self):
        self.assertEqual(self.client.get('/admin/services/bundleportal/catalog').status_code, 401)
        self.assertEqual(self.client.post('/admin/orders/abc/items/0/bundleportal-retry').status_code, 401)


class BundlePortalCheckoutRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = mongomock.MongoClient().routing
        fake = types.ModuleType('db')
        fake.db = cls.db
        fake.campus_db = mongomock.MongoClient().campus
        root = Path(__file__).resolve().parents[1]
        with patch.dict(sys.modules, {'db': fake}), patch('apscheduler.schedulers.background.BackgroundScheduler.start'):
            for name, path in [('checkout', 'checkout.py'), ('store_under_test', 'routes/store_page.py'), ('services_under_test', 'admin_services.py'), ('status_under_test', 'order_status.py')]:
                spec = importlib.util.spec_from_file_location(name, root / path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                sys.modules[name] = module
                setattr(cls, name, module)

    def setUp(self):
        for name in self.db.list_collection_names():
            self.db[name].delete_many({})
        self.user_id = ObjectId()
        self.db.balances.insert_one({'user_id': self.user_id, 'amount': 1000})
        self.db.stores.insert_one({'slug': 'test-store', 'owner_id': self.user_id})
        app = Flask(__name__)
        app.secret_key = 'test'
        app.register_blueprint(self.checkout.checkout_bp)
        app.register_blueprint(self.store_under_test.stores_bp)
        app.register_blueprint(self.services_under_test.admin_services_bp)
        self.client = app.test_client()
        for module in (self.checkout, self.store_under_test):
            for name in ('register_order_phone_numbers_async', '_send_mashup_order_sms_async'):
                p = patch.object(module, name)
                p.start()
                self.addCleanup(p.stop)
            p = patch.object(module, '_check_phone_history_requirement', return_value={'required': False, 'allow_order': True})
            p.start()
            self.addCleanup(p.stop)
        p = patch('threading.Thread')
        self.thread = p.start()
        self.addCleanup(p.stop)

    def test_customer_and_store_use_selected_route_for_supported_services(self):
        for service_name in ('MTN NORMAL', 'MTN EXPRESS', 'AT iShare', 'Telecel', 'Vodafone'):
            for provider, network in api.PROVIDERS.items():
                if not api.supports_service(provider, {'name': service_name}):
                    continue
                for channel in ('customer', 'store'):
                    with self.subTest(service=service_name, provider=provider, channel=channel):
                        self.db.orders.delete_many({})
                        sid = self.db.services.insert_one({'name': service_name, 'provider': provider, 'type': 'API', 'status': 'OPEN', 'offers': []}).inserted_id
                        cart = [{'serviceId': str(sid), 'serviceName': service_name, 'phone': '0271234567' if service_name == 'AT iShare' else ('0201234567' if service_name in ('Telecel', 'Vodafone') else '0241234567'),
                                 'amount': 20, 'base_amount': 18, 'value_obj': {'volume': 5000}, 'value': '5GB'}]
                        with self.client.session_transaction() as sess:
                            sess['user_id'] = str(self.user_id)
                            sess['role'] = 'customer' if channel == 'customer' else 'admin'
                        if channel == 'customer':
                            response = self.client.post('/checkout', json={'cart': cart})
                        else:
                            with patch.object(self.store_under_test, '_server_reprice_store_cart', return_value=(cart, 20)), patch.object(
                                self.store_under_test, '_verify_paystack',
                                return_value=(True, {'amount': 2000, 'currency': 'GHS', 'channel': 'mobile_money'}, 'Verified', {}),
                            ):
                                response = self.client.post('/store-checkout/test-store', json={
                                    'cart': cart, 'method': 'paystack_inline', 'paystack': {'reference': str(sid)},
                                })
                        self.assertEqual(response.status_code, 200, response.get_json())
                        document = self.db.orders.find_one({})
                        self.assertIsNotNone(document, response.get_json())
                        item = document['items'][0]
                        self.assertEqual(item['provider'], provider)
                        self.assertEqual(item['provider_network'], network)
                        self.assertEqual(item['provider_gb_size'], 5)
                        self.assertEqual(item['api_status'], 'queued')
                        jobs = self.thread.call_args.kwargs['args'][1]
                        self.assertEqual(jobs[0]['provider'], provider)
                        self.assertEqual(jobs[0]['provider_request_order_id'], item['provider_request_order_id'])

    def test_bulk_checkout_saves_children_and_routes_each_job_once(self):
        provider = 'bundleportal_mtn2'
        sid = self.db.services.insert_one({'name': 'MTN NORMAL', 'provider': provider,
                    'type': 'API', 'status': 'OPEN', 'offers': []}).inserted_id
        cart = [{'serviceId': str(sid), 'serviceName': 'MTN NORMAL', 'phone': phone,
                 'amount': 20, 'base_amount': 18, 'value_obj': {'volume': 5000}, 'value': '5GB'}
                for phone in ('0241234567', '0241234568')]
        with self.client.session_transaction() as sess:
            sess.update(user_id=str(self.user_id), role='customer')
        with patch.object(self.db.client, 'start_session') as start:
            start.return_value.__enter__.return_value.with_transaction.side_effect = lambda callback: callback(None)
            response = self.client.post('/checkout', json={'cart': cart, 'client_request_id': 'bulk-test'})
        self.assertEqual(response.status_code, 200, response.get_json())
        payload = response.get_json()
        children = list(self.db.orders.find({}).sort('batch_position', 1))
        self.assertEqual(len(children), 2)
        self.assertEqual([c['order_id'] for c in children], payload['order_ids'])
        self.assertTrue(all(len(c['items']) == 1 for c in children))
        self.assertEqual([c['charged_amount'] for c in children], [20, 20])
        self.assertEqual(self.db.balances.find_one({})['amount'], 960)
        self.assertEqual(self.db.transactions.count_documents({'type': 'purchase'}), 1)
        jobs = self.thread.call_args.kwargs['args'][1]
        self.assertEqual([j['order_id'] for j in jobs], payload['order_ids'])
        repeated = self.client.post('/checkout', json={'cart': cart, 'client_request_id': 'bulk-test'})
        self.assertEqual(repeated.get_json()['charged_amount'], 40)
        self.assertEqual(repeated.get_json()['order_id'], payload['order_id'])
        self.assertEqual(repeated.get_json()['order_ids'], payload['order_ids'])
        self.assertEqual(self.db.balances.find_one({})['amount'], 960)
        with patch.object(self.checkout, 'render_template', return_value='invoice') as render:
            self.assertEqual(self.client.get(payload['redirect_url']).status_code, 200)
        self.assertEqual(len(render.call_args.kwargs['order']['items']), 2)
        self.assertEqual(render.call_args.kwargs['order']['charged_amount'], 40)

    @patch.dict(os.environ, {'BUNDLE_PORTAL_KEY': 'test-key', 'BUNDLE_PORTAL_WEBHOOK_SECRET': 'test-secret'})
    def test_admin_switches_each_mtn_service_independently(self):
        with self.client.session_transaction() as sess:
            sess['role'] = 'admin'
        normal = self.db.services.insert_one({'name': 'MTN NORMAL', 'type': 'API'}).inserted_id
        express = self.db.services.insert_one({'name': 'MTN EXPRESS', 'type': 'API'}).inserted_id
        for sid in (normal, express):
            for provider in api.PROVIDERS:
                if not api.supports_service(provider, {'name': 'MTN NORMAL'}):
                    continue
                response = self.client.post(f'/admin/services/{sid}/provider', json={'provider': provider})
                self.assertEqual(response.status_code, 200, response.get_json())
                self.assertEqual(self.db.services.find_one({'_id': sid})['provider'], provider)

    @patch.dict(os.environ, {'BUNDLE_PORTAL_KEY': 'test-key', 'BUNDLE_PORTAL_WEBHOOK_SECRET': 'test-secret'})
    def test_ishare_switch_and_wrong_product_rejection(self):
        with self.client.session_transaction() as sess:
            sess['role'] = 'admin'
        for name, provider, expected in (
            ('AT iShare', 'bundleportal_ishare', 200),
            ('AT iShare', 'bundleportal_mtn', 400),
            ('MTN NORMAL', 'bundleportal_ishare', 400),
            ('AT Bigtime', 'bundleportal_ishare', 400),
            ('AT Bigtime', 'codecraft', 200),
            ('Telecel', 'bundleportal_telecel', 200),
            ('Vodafone', 'bundleportal_telecel', 200),
            ('Telecel', 'bundleportal_mtn', 400),
            ('Telecel', 'bundleportal_ishare', 400),
            ('MTN NORMAL', 'bundleportal_telecel', 400),
            ('AT iShare', 'bundleportal_telecel', 400),
        ):
            with self.subTest(name=name, provider=provider):
                sid = self.db.services.insert_one({'name': name, 'type': 'API'}).inserted_id
                response = self.client.post(f'/admin/services/{sid}/provider', json={'provider': provider})
                self.assertEqual(response.status_code, expected, response.get_json())

    @patch.dict(os.environ, {'BUNDLE_PORTAL_KEY': 'test-key', 'BUNDLE_PORTAL_WEBHOOK_SECRET': 'test-secret'})
    def test_telecel_off_service_cannot_enable_provider(self):
        with self.client.session_transaction() as sess:
            sess['role'] = 'admin'
        sid = self.db.services.insert_one({'name': 'Telecel', 'type': 'OFF'}).inserted_id
        response = self.client.post(f'/admin/services/{sid}/provider', json={'provider': 'bundleportal_telecel'})
        self.assertEqual(response.status_code, 400)
        self.assertNotIn('provider', self.db.services.find_one({'_id': sid}))

    def test_timed_auto_delivery_does_not_deliver_bundle_portal_lines(self):
        self.db.order_auto_update_settings.insert_one({
            '_id': 'AUTO_UPDATE_SETTINGS', 'active': True, 'minutes': 1, 'service_names': ['mtn normal'],
        })
        self.db.orders.insert_one({
            'order_id': 'OLD', 'status': 'processing', 'created_at': datetime.utcnow() - timedelta(hours=1),
            'items': [{'serviceName': 'MTN NORMAL', 'provider': 'bundleportal_mtn', 'line_status': 'processing'}],
        })
        self.status_under_test._run_auto_deliver_updates()
        self.assertEqual(self.db.orders.find_one({'order_id': 'OLD'})['items'][0]['line_status'], 'processing')

    @patch.dict(os.environ, {'BUNDLE_PORTAL_KEY': '', 'BUNDLE_PORTAL_WEBHOOK_SECRET': ''})
    def test_unconfigured_provider_cannot_be_enabled(self):
        with self.client.session_transaction() as sess:
            sess['role'] = 'admin'
        sid = self.db.services.insert_one({'name': 'MTN EXPRESS', 'type': 'API'}).inserted_id
        response = self.client.post(f'/admin/services/{sid}/provider', json={'provider': 'bundleportal_mtn'})
        self.assertEqual(response.status_code, 409)
        self.assertNotIn('provider', self.db.services.find_one({'_id': sid}))


if __name__ == '__main__':
    unittest.main()
