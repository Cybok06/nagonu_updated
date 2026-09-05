import ast
import re
from pathlib import Path

from bson.objectid import ObjectId


class Purchases:
    def __init__(self, rows=()):
        self.rows = rows
        self.queries = []

    def find(self, query, projection):
        self.queries.append(query)
        return [row for row in self.rows if row['checker_id'] in query['checker_id']['$in']]


def load_badges(store=(), public=()):
    tree = ast.parse(Path('admin_wassce_checker.py').read_text(encoding='utf-8-sig'))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
             and node.name in {'_delivery_phone', '_add_delivery_badges'}]
    database = {'store_checker_purchases': Purchases(store),
                'public_checker_purchases': Purchases(public)}
    namespace = {'re': re, 'ObjectId': ObjectId, 'db': database}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'admin_wassce_checker.py', 'exec'), namespace)
    return namespace['_add_delivery_badges'], database


def test_existing_sales_use_exact_checker_recipient_for_both_types():
    first, second = ObjectId(), ObjectId()
    add_badges, database = load_badges(store=[
        {'checker_id': first, 'phone': '+233530393625'},
        {'checker_id': str(second), 'phone': '0241234567'},
        {'checker_id': ObjectId(), 'phone': '0201234567'},
    ])
    messages = [
        {'_id': first, 'status': 'sold', 'type': 'wassce', 'sold_to_store': 'shop'},
        {'_id': second, 'status': 'sold', 'type': 'bece', 'sold_channel': 'store_page'},
    ]
    add_badges(messages)
    assert [m['delivery_label'] for m in messages] == ['0530393625', '0241234567']
    assert len(database['store_checker_purchases'].queries) == 1


def test_new_and_legacy_public_sales_and_unsold_inventory():
    add_badges, database = load_badges()
    messages = [
        {'_id': ObjectId(), 'status': 'sold', 'delivery_phone': '0530393625'},
        {'_id': ObjectId(), 'status': 'sold', 'sold_to': '233241234567'},
        {'_id': ObjectId(), 'status': 'not_sold', 'delivery_phone': '0530393625'},
    ]
    add_badges(messages)
    assert messages[0]['delivery_label'] == '0530393625'
    assert messages[1]['delivery_label'] == '0241234567'
    assert 'delivery_label' not in messages[2]
    assert not database['store_checker_purchases'].queries


def test_missing_numbers_and_dashboard_ids_are_not_displayed_as_phone_numbers():
    checker_id = ObjectId()
    add_badges, _ = load_badges(public=[{'checker_id': checker_id, 'phone': '0530393625'}])
    messages = [
        {'_id': checker_id, 'status': 'sold', 'sold_channel': 'public_results_checker'},
        {'_id': ObjectId(), 'status': 'sold', 'sold_to': str(ObjectId())},
        {'_id': ObjectId(), 'status': 'sold', 'sold_to_store': 'shop'},
    ]
    add_badges(messages)
    assert [m['delivery_label'] for m in messages] == [
        '0530393625', 'Customer Dashboard', 'Number unavailable']
