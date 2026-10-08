import ast
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from unittest.mock import Mock

from bson import Regex
import mongomock


class AdminOrderTimeFilterTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'admin_orders.py').read_text(encoding='utf-8'))
        names = {'_parse_date', '_parse_filter_time', '_build_query_from_params', '_build_orders_cache_key'}
        import json
        self.env = dict(datetime=datetime, timedelta=timedelta, Regex=Regex, users_col=Mock(),
                        ALLOWED_STATUSES=['processing'], json=json,
                        _normalize_source_filter=lambda value: value or 'all',
                        _get_orders_cache_version=lambda: 1)
        functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'admin_orders.py', 'exec'), self.env)
        self.query = self.env['_build_query_from_params']

    def test_date_and_time_bounds_include_end_minute(self):
        query = self.query(dict(date_from='2026-10-05', date_to='2026-10-05',
                                time_from='09:30', time_to='10:15'))
        self.assertEqual(query['created_at'], {'$gte': datetime(2026, 10, 5, 9, 30),
                                               '$lt': datetime(2026, 10, 5, 10, 16)})

    def test_date_only_keeps_whole_day(self):
        self.assertEqual(self.query({'date_to': '2026-10-05'})['created_at'],
                         {'$lt': datetime(2026, 10, 6)})

    def test_time_only_and_overnight_match_expected_orders(self):
        collection = mongomock.MongoClient().db.orders
        for hour, minute in [(9, 29), (9, 30), (10, 15), (10, 16), (23, 30), (0, 30)]:
            collection.insert_one({'created_at': datetime(2026, 10, 5, hour, minute)})
        self.assertEqual(collection.count_documents(self.query({'time_from': '09:30', 'time_to': '10:15'})), 2)
        self.assertEqual(collection.count_documents(self.query({'time_from': '23:00', 'time_to': '01:00'})), 2)
        self.assertEqual(collection.count_documents(self.query({'time_from': '23:00'})), 1)

    def test_invalid_time_does_not_break_date_filter(self):
        query = self.query(dict(date_from='2026-10-05', time_from='25:99', time_to='bad'))
        self.assertEqual(query, {'created_at': {'$gte': datetime(2026, 10, 5)}})

    def test_live_refresh_cache_distinguishes_times(self):
        cache_key = self.env['_build_orders_cache_key']
        self.assertNotEqual(cache_key({'time_from': '09:00'}), cache_key({'time_from': '10:00'}))
        self.assertNotEqual(cache_key({'time_to': '09:00'}), cache_key({'time_to': '10:00'}))


if __name__ == '__main__':
    unittest.main()
