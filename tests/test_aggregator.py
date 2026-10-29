from aggregator_service.app import app, normalize_temperature


def test_normalization() -> None:
    assert normalize_temperature(37.0) == 0.0
    assert normalize_temperature(38.0) == 1.0
    assert normalize_temperature(39.5) == 2.5


def test_obsolete_in_memory_result_endpoints_are_removed() -> None:
    paths = {route.path for route in app.routes}
    assert "/results" not in paths
    assert "/reset" not in paths
    detector_route = next(route for route in app.routes if route.path == "/admin/detector")
    assert detector_route.methods == {"POST"}
