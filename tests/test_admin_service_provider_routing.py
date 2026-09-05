import ast
from pathlib import Path


def _admin_service_function(name):
    tree = ast.parse(Path("admin_services.py").read_text(encoding="utf-8-sig"))
    return next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)


def test_at_services_are_eligible_for_provider_switching():
    node = _admin_service_function("_is_at_provider_service")
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "admin_services.py", "exec"), namespace)
    is_at_service = namespace["_is_at_provider_service"]

    assert is_at_service({"name": "AT - BigTime"}) is True
    assert is_at_service({"name": "AT - iSHare"}) is True
    assert is_at_service({"name": "AT-iShare"}) is True
    assert is_at_service({"name": "Telecel"}) is False


def test_at_admin_selectors_offer_codecraft_and_bundleportal():
    template = Path("templates/admin_services.html").read_text(encoding="utf-8-sig")

    assert template.count("service-provider-select") >= 5
    assert "supports_provider_routing = is_mtn_normal or is_mtn_express or is_telecel or is_at_provider_service" in template
    assert template.count("{% if not is_at_provider_service %}") == 3
    assert template.count('value="codecraft"') >= 3
    assert template.count('value="bundleportal"') >= 3


def test_at_backend_restricts_provider_choices_to_supported_apis():
    source = Path("admin_services.py").read_text(encoding="utf-8-sig")

    assert 'provider not in {"codecraft", "bundleportal"}' in source
    assert "AT - BigTime and AT - iShare support CodeCraft or BundlePortal only" in source
