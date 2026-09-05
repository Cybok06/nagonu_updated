from order_status import _compute_order_status_from_items


def test_mixed_delivered_and_refunded_is_partially_refunded():
    items = [
        {"line_status": "delivered"},
        {"line_status": "refunded"},
    ]
    assert _compute_order_status_from_items(items, "delivered") == "partially_refunded"


def test_all_refunded_lines_make_parent_refunded():
    items = [
        {"line_status": "refunded"},
        {"line_status": "refunded"},
    ]
    assert _compute_order_status_from_items(items, "completed") == "refunded"

