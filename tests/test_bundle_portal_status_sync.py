import importlib.util
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import patch

import mongomock
import bundle_portal_orders


class BundlePortalStatusSyncTests(unittest.TestCase):
    def setUp(self):
        fake_db = types.ModuleType('db')
        fake_db.db = mongomock.MongoClient().main
        fake_db.campus_db = mongomock.MongoClient().campus
        spec = importlib.util.spec_from_file_location(
            'bundleportal_status_under_test', Path(__file__).resolve().parents[1] / 'order_status.py')
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'db': fake_db}), patch(
                'apscheduler.schedulers.background.BackgroundScheduler.start'):
            spec.loader.exec_module(self.module)
        self.db = fake_db.db
        self.campus = fake_db.campus_db
        patcher = patch.object(bundle_portal_orders, 'invalidate_orders')
        self.invalidate = patcher.start()
        self.addCleanup(patcher.stop)
        self.module.jlog = lambda *args, **kwargs: None

    def add_order(self, collection, reference='BP_ref', status='processing'):
        collection.insert_one({'order_id': reference, 'status': 'processing', 'items': [{
            'provider': 'bundleportal_mtn', 'provider_request_order_id': reference,
            'phone': '0241234567', 'line_status': status,
        }, {'provider': 'manual', 'line_status': 'delivered'}]})

    def add_event(self, reference='BP_ref', status='completed', **overrides):
        self.db.bundleportal_events.insert_one({'received_at': datetime.now(), 'payload': {
            'event': 'order.' + status, 'status': status, 'order_id': reference,
            'network': 'mtn', 'recipient': '0241234567', **overrides,
        }})

    def test_reconciliation_updates_main_and_campus_without_polling(self):
        for collection, reference in [(self.db.orders, 'BP_ref'), (self.campus.orders, 'BPC_ref')]:
            self.add_order(collection, reference)
            self.add_event(reference)
        with patch.object(self.module.requests, 'get') as get, patch.object(self.module.requests, 'post') as post:
            summary = self.module._run_order_status_sync()
        self.assertEqual(summary['bundleportal']['updated_lines'], 2)
        self.assertEqual(self.db.orders.find_one({})['status'], 'delivered')
        self.assertEqual(self.campus.orders.find_one({})['status'], 'delivered')
        get.assert_not_called()
        post.assert_not_called()
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 0)

    def test_cached_without_webhook_remains_processing(self):
        self.add_order(self.db.orders, status='cached')
        summary = self.module._run_bundle_portal_status_sync()
        self.assertEqual(summary['awaiting_webhook'], 1)
        self.assertEqual(self.db.orders.find_one({})['items'][0]['line_status'], 'cached')
        self.invalidate.assert_not_called()

    def test_failure_cancel_and_refund_require_local_refund_review(self):
        for status in ('failed', 'cancelled', 'refunded'):
            self.add_order(self.db.orders, reference=status)
            self.add_event(status, status)
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 3)
        for order in self.db.orders.find({}):
            self.assertEqual(order['items'][0]['line_status'], 'failed')
            self.assertTrue(order['items'][0]['refund_required'])
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 0)

    def test_mismatched_event_cannot_change_order(self):
        self.add_order(self.db.orders)
        self.add_event(recipient='0551234567')
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 0)
        self.assertEqual(self.db.orders.find_one({})['items'][0]['line_status'], 'processing')

    def test_final_wallet_items_are_protected(self):
        for status in ('refunded', 'completed'):
            self.add_order(self.db.orders, status, status)
            self.add_event(status, 'refunded')
        self.assertEqual(self.module._run_bundle_portal_status_sync()['checked_lines'], 0)
        for order in self.db.orders.find({}):
            self.assertEqual(order['items'][0]['line_status'], order['order_id'])

    def test_provider_refund_after_delivery_updates_line_and_parent(self):
        self.add_order(self.db.orders, status='delivered')
        self.db.orders.update_one({}, {'$set': {'status': 'delivered'}})
        self.add_event(status='refunded')
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 1)
        order = self.db.orders.find_one({})
        self.assertEqual(order['items'][0]['provider_status'], 'refunded')
        self.assertEqual(order['items'][0]['line_status'], 'failed')
        self.assertEqual(order['status'], 'processing')  # other line delivered
        self.assertTrue(order['items'][0]['refund_required'])

    def test_existing_payload_repairs_stale_parent(self):
        self.add_order(self.db.orders, status='delivered')
        self.add_event()
        payload = self.db.bundleportal_events.find_one({})['payload']
        self.db.orders.update_one({}, {'$set': {'items.0.provider_status_payload': payload}})
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_orders'], 1)
        self.assertEqual(self.db.orders.find_one({})['status'], 'delivered')

    def test_all_five_routes_and_legacy_reference(self):
        for provider, network in bundle_portal_orders.PROVIDERS.items():
            self.add_order(self.db.orders, provider)
            self.db.orders.update_one({'order_id': provider}, {'$set': {
                'items.0.provider': provider, 'items.0.provider_order_id': provider},
                '$unset': {'items.0.provider_request_order_id': ''}})
            self.add_event(provider, network=network, recipient='+233241234567')
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 5)

    def test_older_and_invalid_receipts_cannot_hide_new_refund(self):
        self.add_order(self.db.orders, status='delivered')
        self.add_event(status='refunded', settled_at='2026-10-08T10:00:00Z')
        self.add_event(status='completed', settled_at='2026-10-08T09:00:00Z')
        self.add_event(status='completed', recipient='0551234567', settled_at='2026-10-08T11:00:00Z')
        self.module._run_bundle_portal_status_sync()
        self.assertEqual(self.db.orders.find_one({})['items'][0]['provider_status'], 'refunded')

    def test_early_receipt_applies_after_local_order_is_saved(self):
        self.add_event()
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 0)
        self.add_order(self.db.orders)
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 1)

    def test_delayed_old_refund_does_not_replace_newer_delivery(self):
        self.add_order(self.db.orders, status='delivered')
        self.db.orders.update_one({}, {'$set': {'items.0.provider_status_payload': {
            'status': 'completed', 'settled_at': '2026-10-08T11:00:00Z'}}})
        self.add_event(status='refunded', settled_at='2026-10-08T10:00:00Z')
        self.assertEqual(self.module._run_bundle_portal_status_sync()['updated_lines'], 0)
        self.assertEqual(self.db.orders.find_one({})['items'][0]['line_status'], 'delivered')

    def test_scheduler_runs_every_three_minutes(self):
        job = self.module.status_sync_scheduler.get_job('order_status_sync')
        self.assertEqual(job.trigger.interval.total_seconds(), 180)
        self.assertEqual(job.max_instances, 1)

    def test_codecraft_sync_function_is_restored(self):
        self.db.orders.insert_one({'order_id': 'CC', 'status': 'processing', 'items': [{
            'provider': 'codecraft', 'provider_reference': 'ref', 'line_status': 'processing',
        }]})
        with patch.object(self.module, '_fetch_codecraft_order_status', return_value=(
                True, {'data': {'order_status': 'completed'}})):
            summary = self.module._run_order_status_sync()
        self.assertEqual(summary['updated_orders'], 1)
        self.assertEqual(self.db.orders.find_one({})['status'], 'delivered')

    def test_diagnostic_read_only_shows_response_without_updating(self):
        self.add_order(self.db.orders)
        self.add_event()
        before = self.db.orders.find_one({})
        report = self.module.inspect_bundle_portal_order(self.db.orders, before, apply=False)
        self.assertEqual(report['lines'][0]['reason'], 'matching_settlement_available')
        self.assertEqual(report['lines'][0]['selected_status_response']['status'], 'completed')
        self.assertEqual(report['summary']['updated_lines'], 0)
        self.assertEqual(before, self.db.orders.find_one({}))
        self.invalidate.assert_not_called()

    def test_diagnostic_explains_missing_reference_and_mismatch(self):
        self.add_order(self.db.orders)
        self.add_event(recipient='0551234567')
        report = self.module.inspect_bundle_portal_order(self.db.orders, self.db.orders.find_one({}))
        self.assertEqual(report['lines'][0]['reason'], 'no_matching_settlement')
        self.assertEqual(report['lines'][0]['stored_receipts'], 1)
        self.db.orders.update_one({}, {'$unset': {'items.0.provider_request_order_id': ''}})
        report = self.module.inspect_bundle_portal_order(self.db.orders, self.db.orders.find_one({}))
        self.assertEqual(report['lines'][0]['reason'], 'missing_reference')

    def test_diagnostic_selects_latest_order_across_databases(self):
        from test_bundle_portal_status import run_diagnostics
        self.add_order(self.db.orders, 'older')
        self.add_order(self.campus.orders, 'newer')
        self.db.orders.update_one({}, {'$set': {'created_at': datetime(2026, 10, 1)}})
        self.campus.orders.update_one({}, {'$set': {'created_at': datetime(2026, 10, 9)}})
        reports = run_diagnostics([('main', self.db.orders), ('campus', self.campus.orders)],
                                  self.db.bundleportal_events, self.module.inspect_bundle_portal_order)
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]['order_id'], 'newer')
        self.assertEqual(reports[0]['source'], 'campus')
        self.assertEqual(reports[0]['lines'][0]['reason'], 'awaiting_webhook')
        reports = run_diagnostics([('main', self.db.orders)], self.db.bundleportal_events,
                                  self.module.inspect_bundle_portal_order, order_id='older')
        self.assertEqual(reports[0]['order_id'], 'older')

    def test_diagnostic_apply_uses_real_update_logic(self):
        from test_bundle_portal_status import run_diagnostics
        self.add_order(self.db.orders)
        self.add_event()
        reports = run_diagnostics([('main', self.db.orders)], self.db.bundleportal_events,
                                  self.module.inspect_bundle_portal_order, apply=True)
        self.assertEqual(reports[0]['summary']['updated_lines'], 1)
        self.assertEqual(reports[0]['status_after'], 'delivered')
        self.assertEqual(reports[0]['lines'][0]['line_status_after'], 'delivered')


if __name__ == '__main__':
    unittest.main()
