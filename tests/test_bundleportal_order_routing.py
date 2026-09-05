import ast
from pathlib import Path


def _checkout_function(name):
    tree = ast.parse(Path("checkout.py").read_text(encoding="utf-8-sig"))
    return next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)


def test_bundleportal_network_maps_ishare_and_bigtime_to_airtel_tigo():
    node = _checkout_function("_resolve_bundleportal_network")
    namespace = {"_resolve_provider_network": lambda service, item: None}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "checkout.py", "exec"), namespace)

    resolve = namespace["_resolve_bundleportal_network"]

    assert resolve({"name": "AT - iShare"}, {}) == "airteltigo"
    assert resolve({"name": "AT - BigTime"}, {}) == "airteltigo"


def test_bundleportal_submit_normalizes_legacy_ishare_job():
    class Response:
        ok = True
        text = ""

        @staticmethod
        def json():
            return {"success": True, "data": {"order_id": "BP-123"}}

    class Requests:
        def __init__(self):
            self.body = None

        def post(self, url, **kwargs):
            self.body = kwargs["json"]
            return Response()

    node = _checkout_function("_bundleportal_submit_single")
    fake_requests = Requests()
    namespace = {
        "requests": fake_requests,
        "_clean_api_key": lambda value: value,
        "BUNDLEPORTAL_API_KEY": "test-key",
        "BUNDLEPORTAL_AUTH_PREFIX": "",
        "BUNDLEPORTAL_AUTH_HEADER": "x-api-key",
        "BUNDLEPORTAL_BASE_URL": "https://api.bundleportal.test/v1",
        "BUNDLEPORTAL_TIMEOUT": 45,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), "checkout.py", "exec"), namespace)

    result = namespace["_bundleportal_submit_single"]("0240000000", 2, "ishare", "ORDER-1")

    assert result["ok"] is True
    assert fake_requests.body["network"] == "airteltigo"


def test_campus_admin_exposes_bundleportal_as_a_provider():
    route_tree = ast.parse(Path("routes/admin_campus_services.py").read_text(encoding="utf-8-sig"))
    assignment = next(
        node for node in route_tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "ALLOWED_PROVIDERS" for target in node.targets)
    )
    allowed = ast.literal_eval(assignment.value)
    template = Path("templates/campus_services.html").read_text(encoding="utf-8-sig")

    assert "bundleportal" in allowed
    assert '<option value="bundleportal">BundlePortal</option>' in template
    assert '<option value="bundleportal" {{' in template
