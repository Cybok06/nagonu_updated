import ast
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _functions(*names):
    tree = ast.parse(Path("order_status.py").read_text(encoding="utf-8-sig"))
    wanted = set(names)
    return [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]


def test_bundleportal_status_request_uses_check_status_contract():
    class Response:
        status_code = 200
        text = "{}"

        @staticmethod
        def json():
            return {"success": True, "data": {"status": "processing"}}

    class Requests:
        class RequestException(Exception):
            pass

        def __init__(self):
            self.call = None

        def post(self, url, **kwargs):
            self.call = (url, kwargs)
            return Response()

    fake_requests = Requests()
    namespace = {
        "Any": Any,
        "Dict": Dict,
        "Tuple": Tuple,
        "requests": fake_requests,
        "jlog": lambda *args, **kwargs: None,
        "_clean_api_key": lambda value: value,
        "BUNDLEPORTAL_API_KEY": "test-key",
        "BUNDLEPORTAL_AUTH_HEADER": "x-api-key",
        "BUNDLEPORTAL_AUTH_PREFIX": "",
        "BUNDLEPORTAL_BASE_URL": "https://api.bundleportal.test/v1",
        "BUNDLEPORTAL_TIMEOUT": 45,
    }
    nodes = _functions("_fetch_bundleportal_order_status")
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "order_status.py", "exec"), namespace)

    ok, payload = namespace["_fetch_bundleportal_order_status"]("ORDER-LINE-1", "ORDER-1")

    assert ok is True
    assert payload["data"]["status"] == "processing"
    assert fake_requests.call[1]["json"] == {
        "action": "check_status",
        "order_reference": "ORDER-LINE-1",
    }
    assert fake_requests.call[1]["headers"]["x-api-key"] == "test-key"


def test_bundleportal_status_mapping_matches_provider_contract():
    namespace = {"Tuple": Tuple}
    nodes = _functions("_map_bundleportal_status")
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "order_status.py", "exec"), namespace)
    status_map = namespace["_map_bundleportal_status"]

    assert status_map("completed") == ("delivered", "success")
    assert status_map("failed") == ("failed", "failed")
    assert status_map("cached") == ("processing", "cached")
    assert status_map("processing") == ("processing", "processing")


def test_bundleportal_sync_updates_line_and_parent_order():
    class Cursor(list):
        def sort(self, *args):
            return self

        def limit(self, *args):
            return self

    class Result:
        modified_count = 1

    class Orders:
        def __init__(self, source, reference):
            self.source = source
            self.reference = reference
            self.updated = None

        def find(self, query):
            return Cursor([{
                "_id": f"{self.source}-mongo-1",
                "order_id": f"{self.source.upper()}-ORDER-1",
                "status": "processing",
                "items": [{
                    "provider": "bundleportal",
                    "provider_reference": "KT-PROVIDER-REF",
                    "provider_order_id": self.reference,
                    "line_status": "pending",
                }],
            }])

        def update_one(self, query, update):
            self.updated = (query, update)
            return Result()

    checked_references = []
    orders = Orders("main", "MAIN-ORDER-LINE-1")
    campus_orders = Orders("campus", "CAMPUS-ORDER-LINE-1")
    namespace = {
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "Tuple": Tuple,
        "datetime": datetime,
        "FINAL_STATUS": "delivered",
        "orders_col": orders,
        "campus_orders_col": campus_orders,
        "jlog": lambda *args, **kwargs: None,
        "_log_status_blocked": lambda *args, **kwargs: None,
        "_log_line_status_blocked": lambda *args, **kwargs: None,
        "_fetch_codecraft_order_status": lambda *args: (False, {}),
        "_fetch_bundleportal_order_status": lambda reference, order_id: (
            checked_references.append(reference) or True,
            {"success": True, "data": {"status": "completed", "order_id": reference}},
        ),
    }
    nodes = _functions(
        "_normalize_status",
        "_compute_order_status_from_items",
        "_extract_codecraft_status",
        "_extract_bundleportal_status",
        "_map_bundleportal_status",
        "_apply_codecraft_status_to_item",
        "_apply_bundleportal_status_to_item",
        "_run_order_status_sync_for_collection",
        "_run_order_status_sync",
    )
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "order_status.py", "exec"), namespace)

    summary = namespace["_run_order_status_sync"]()

    main_saved = orders.updated[1]["$set"]
    campus_saved = campus_orders.updated[1]["$set"]
    assert checked_references == ["MAIN-ORDER-LINE-1", "CAMPUS-ORDER-LINE-1"]
    assert main_saved["items"][0]["line_status"] == "delivered"
    assert main_saved["status"] == "delivered"
    assert campus_saved["items"][0]["line_status"] == "delivered"
    assert campus_saved["status"] == "delivered"
    assert summary["bundleportal_checked_orders"] == 2
    assert summary["completed_lines"] == 2
    assert summary["main_updated_orders"] == 1
    assert summary["campus_updated_orders"] == 1
