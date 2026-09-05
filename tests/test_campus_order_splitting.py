import ast
from pathlib import Path


def _load_split_helper():
    tree = ast.parse(Path("campus_data-main/checkout.py").read_text(encoding="utf-8-sig"))
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in {"_money", "_split_order_documents"}
    ]
    ids = iter(("CAMP100001", "CAMP100002"))
    namespace = {"generate_order_id": ids.__next__}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "campus_data-main/checkout.py", "exec"), namespace)
    return namespace["_split_order_documents"]


def test_campus_bulk_checkout_creates_independent_orders():
    split = _load_split_helper()
    docs, order_ids = split(
        {"order_id": "CAMP-BATCH", "status": "pending", "items": []},
        [
            {"phone": "0240000001", "amount": 10, "profit_amount": 1},
            {"phone": "0240000002", "amount": 20, "profit_amount": 2},
        ],
        "CAMP-BATCH",
    )
    assert order_ids == ["CAMP100001", "CAMP100002"]
    assert len(docs) == 2
    assert all(len(doc["items"]) == 1 for doc in docs)
    assert [doc["order_id"] for doc in docs] == order_ids
    assert [doc["items"][0]["order_id"] for doc in docs] == order_ids
    assert [doc["charged_amount"] for doc in docs] == [10, 20]
    assert all(doc["batch_id"] == "CAMP-BATCH" for doc in docs)


def test_campus_skipped_line_is_not_charged():
    split = _load_split_helper()
    docs, _ = split(
        {"status": "skipped"},
        [{"amount": 10, "line_status": "skipped_duplicate_in_cart"}],
        "CAMP-BATCH",
    )
    assert docs[0]["charged_amount"] == 0
