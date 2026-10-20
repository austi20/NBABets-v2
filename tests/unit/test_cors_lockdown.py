from fastapi.testclient import TestClient

from app.server.main import create_app


def test_cors_rejects_arbitrary_origin():
    c = TestClient(create_app(app_token="t"))
    r = c.options(
        "/api/health",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    origin_header = r.headers.get("access-control-allow-origin", "")
    assert origin_header != "https://evil.example"
    assert origin_header != "*"


def test_cors_allows_tauri_origin():
    c = TestClient(create_app(app_token="t"))
    r = c.options(
        "/api/health",
        headers={
            "Origin": "tauri://localhost",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert r.headers.get("access-control-allow-origin") == "tauri://localhost"


def test_cors_allows_tauri_http_origin():
    c = TestClient(create_app(app_token="t"))
    r = c.options(
        "/api/health",
        headers={
            "Origin": "http://tauri.localhost",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert r.headers.get("access-control-allow-origin") == "http://tauri.localhost"


def test_cors_allows_protected_startup_preflight_from_vite_dev():
    c = TestClient(create_app(app_token="t"))
    r = c.options(
        "/api/startup/run",
        headers={
            "Origin": "http://localhost:1420",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "X-App-Token, Content-Type",
        },
    )
    assert r.status_code == 200
    assert r.headers.get("access-control-allow-origin") == "http://localhost:1420"
