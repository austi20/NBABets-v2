from fastapi.testclient import TestClient


def test_api_settings_redacts_keys(monkeypatch):
    secret_bdl = "super-secret-bdl-value"
    secret_kalshi = "super-secret-kalshi-id"
    monkeypatch.setenv("BALLDONTLIE_API_KEY", secret_bdl)
    monkeypatch.setenv("KALSHI_API_KEY_ID", secret_kalshi)

    from app.config import settings as settings_module

    settings_module.get_settings.cache_clear()

    from app.server.main import create_app

    app = create_app(app_token="test-token")
    with TestClient(app) as client:
        response = client.get("/api/settings")

    assert response.status_code == 200
    body = response.json()
    assert body["balldontlie_api_key"] == "***"
    assert body["kalshi_api_key_id"] == "***"
    assert secret_bdl not in response.text
    assert secret_kalshi not in response.text

    settings_module.get_settings.cache_clear()
