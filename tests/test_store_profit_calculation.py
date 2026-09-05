import ast
from pathlib import Path


def _load_profit_helper():
    source = Path("routes/store_page.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == "_finalize_store_profit_lines"
    )
    module = ast.Module(body=[node], type_ignores=[])
    namespace = {
        "List": list,
        "Dict": dict,
        "Any": object,
        "_money": lambda value: round(float(value or 0), 2),
    }
    exec(compile(module, "routes/store_page.py", "exec"), namespace)
    return namespace["_finalize_store_profit_lines"]


def test_single_store_line_profit_is_selling_price_minus_store_cost():
    finalize = _load_profit_helper()
    lines = [{"amount": 4.84, "base_amount": 4.40, "profit_amount": 0.30}]

    assert finalize(lines) == 0.44
    assert lines[0]["store_profit_amount"] == 0.44


def test_bulk_store_profit_is_sum_of_each_line_margin():
    finalize = _load_profit_helper()
    lines = [
        {"amount": 4.84, "base_amount": 4.40},
        {"amount": 11.00, "base_amount": 10.00},
        {
            "amount": 20.00,
            "base_amount": 18.00,
            "line_status": "skipped_duplicate_processing",
        },
    ]

    assert finalize(lines) == 1.44
    assert [line["store_profit_amount"] for line in lines] == [0.44, 1.0, 0.0]
