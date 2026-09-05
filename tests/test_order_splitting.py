import ast
from pathlib import Path


def _load_split_helper():
    source = Path("checkout.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == "_split_order_documents"
    )
    module = ast.Module(body=[node], type_ignores=[])
    namespace = {
        "_money": lambda value: round(float(value or 0), 2),
        "generate_order_id": iter(("NAN-LINE-1", "NAN-LINE-2")).__next__,
    }
    exec(compile(module, "checkout.py", "exec"), namespace)
    return namespace["_split_order_documents"]


def _load_transaction_order_links():
    source = Path("checkout.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == "_transaction_order_links"
    )
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "checkout.py", "exec"), namespace)
    return namespace["_transaction_order_links"]


def test_bulk_checkout_becomes_independent_one_item_orders():
    split = _load_split_helper()
    base = {
        "order_id": "NAN-BATCH",
        "items": [],
        "total_amount": 30,
        "charged_amount": 30,
        "profit_amount_total": 3,
        "status": "pending",
    }
    results = [
        {"phone": "0240000001", "amount": 10, "profit_amount": 1},
        {"phone": "0240000002", "amount": 20, "profit_amount": 2},
    ]

    docs, order_ids = split(base, results, "NAN-BATCH")

    assert order_ids == ["NAN-LINE-1", "NAN-LINE-2"]
    assert len(docs) == 2
    assert all(len(doc["items"]) == 1 for doc in docs)
    assert [doc["order_id"] for doc in docs] == order_ids
    assert [doc["items"][0]["order_id"] for doc in docs] == order_ids
    assert [doc["total_amount"] for doc in docs] == [10, 20]
    assert [doc["charged_amount"] for doc in docs] == [10, 20]
    assert all(doc["batch_id"] == "NAN-BATCH" for doc in docs)
    assert [doc["batch_position"] for doc in docs] == [1, 2]


def test_skipped_line_is_saved_without_a_charge():
    split = _load_split_helper()
    docs, _ = split(
        {"status": "skipped"},
        [{"amount": 12, "profit_amount": 0, "line_status": "skipped_duplicate_processing"}],
        "NAN-BATCH",
    )
    assert docs[0]["charged_amount"] == 0


def test_bulk_store_checkout_keeps_store_and_platform_profit_separate():
    split = _load_split_helper()
    docs, _ = split(
        {"store_slug": "cytech", "debug": {"store_checkout": True}},
        [
            {"amount": 4.84, "base_amount": 4.40, "profit_amount": 0.30, "store_profit_amount": 0.44},
            {"amount": 11.00, "base_amount": 10.00, "profit_amount": 0.50, "store_profit_amount": 1.00},
        ],
        "NAN-BATCH",
    )

    assert [doc["profit_amount_total"] for doc in docs] == [0.44, 1.0]
    assert [doc["platform_profit_amount_total"] for doc in docs] == [0.3, 0.5]


def test_store_order_never_falls_back_to_platform_profit():
    split = _load_split_helper()
    docs, _ = split(
        {"store_slug": "cytech", "debug": {"store_checkout": True}},
        [{"amount": 4.84, "base_amount": 4.40, "profit_amount": 0.30}],
        "NAN-BATCH",
    )

    assert docs[0]["profit_amount_total"] == 0.0
    assert docs[0]["platform_profit_amount_total"] == 0.3


def test_single_order_transaction_reference_uses_visible_order_id():
    links = _load_transaction_order_links()("NAN-BATCH", ["NAN-LINE-1"])

    assert links == {
        "reference": "NAN-LINE-1",
        "order_id": "NAN-LINE-1",
        "batch_id": "NAN-BATCH",
        "order_ids": ["NAN-LINE-1"],
    }


def test_multi_order_transaction_reference_keeps_wallet_debit_batch_id():
    links = _load_transaction_order_links()(
        "NAN-BATCH", ["NAN-LINE-1", "NAN-LINE-2"]
    )

    assert links == {
        "reference": "NAN-BATCH",
        "order_id": None,
        "batch_id": "NAN-BATCH",
        "order_ids": ["NAN-LINE-1", "NAN-LINE-2"],
    }
