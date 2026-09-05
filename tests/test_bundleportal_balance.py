import ast
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


class _Response:
    ok = True
    def json(self):
        return {"success": True, "data": {"wallet_balance": 180.0, "currency": "GHS"}}


class _Requests:
    class RequestException(Exception):
        pass
    def __init__(self):
        self.call = None
    def post(self, url, **kwargs):
        self.call = (url, kwargs)
        return _Response()


def test_bundleportal_balance_uses_check_balance_action_and_parses_wallet():
    tree = ast.parse(Path("admin_dashboard.py").read_text(encoding="utf-8-sig"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "bundleportal_get_wallet_balance")
    fake_requests = _Requests()
    cache = {"wallet": None, "currency": "GHS", "ts": None, "raw": None}
    namespace = {
        "Dict": Dict, "Any": Any, "datetime": datetime,
        "requests": fake_requests,
        "_clean_api_key": lambda value: value,
        "BUNDLEPORTAL_API_KEY": "secret",
        "BUNDLEPORTAL_AUTH_HEADER": "x-api-key",
        "BUNDLEPORTAL_AUTH_PREFIX": "",
        "BUNDLEPORTAL_BASE_URL": "https://api.bundleportal.test/v1",
        "BUNDLEPORTAL_TIMEOUT": 45,
        "BUNDLEPORTAL_WALLET_TTL_SECONDS": 60,
        "_BUNDLEPORTAL_WALLET_CACHE": cache,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), "admin_dashboard.py", "exec"), namespace)

    result = namespace["bundleportal_get_wallet_balance"](True)

    assert result["ok"] is True
    assert result["wallet"] == 180.0
    assert fake_requests.call[1]["json"] == {"action": "check_balance"}
    assert fake_requests.call[1]["headers"]["x-api-key"] == "secret"
