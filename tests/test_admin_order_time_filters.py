import ast
from datetime import datetime, timedelta
from pathlib import Path


def _load_filter_helpers():
    tree = ast.parse(Path("admin_orders.py").read_text(encoding="utf-8-sig"))
    names = {"_parse_date", "_parse_time", "_filter_datetime_bounds"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {"datetime": datetime, "timedelta": timedelta}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "admin_orders.py", "exec"), namespace)
    return namespace["_filter_datetime_bounds"]


def test_same_day_time_bounds_include_the_selected_end_minute():
    bounds = _load_filter_helpers()
    start, end = bounds({
        "date_from": "2026-08-18",
        "time_from": "09:15",
        "date_to": "2026-08-18",
        "time_to": "14:30",
    })
    assert start == datetime(2026, 8, 18, 9, 15)
    assert end == datetime(2026, 8, 18, 14, 31)


def test_dates_without_times_keep_full_day_behavior():
    bounds = _load_filter_helpers()
    start, end = bounds({"date_from": "2026-08-18", "date_to": "2026-08-19"})
    assert start == datetime(2026, 8, 18)
    assert end == datetime(2026, 8, 20)


def test_time_without_matching_date_is_ignored():
    bounds = _load_filter_helpers()
    assert bounds({"time_from": "09:00", "time_to": "17:00"}) == (None, None)
