from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient


def test_server_repo_root_prefers_explicit_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("APP_ENV=dev\n", encoding="utf-8")
    monkeypatch.setenv("NBA_PROP_ENV_FILE", str(env_file))

    from app.server.main import _resolve_repo_root

    assert _resolve_repo_root() == tmp_path


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


def test_api_health_masks_database_url_credentials(monkeypatch):
    """/api/health is outside the auth prefix; it must not expose DB credentials.

    The app is built against the default sqlite URL (so create_app/_upgrade_database
    don't need a postgres driver); the health handler's get_settings() is then
    patched to surface a credentialed postgres URL, mirroring a prod deployment.
    """
    from app.config import settings as settings_module

    settings_module.get_settings.cache_clear()

    from app.server import main as main_module
    from app.server.main import create_app

    app = create_app(app_token="test-token")

    secret_user = "dbuser"
    secret_pass = "dbpassword123"
    pg_settings = SimpleNamespace(
        database_url=f"postgresql://{secret_user}:{secret_pass}@db.internal:5432/nba"
    )

    # No `with` block: skip lifespan startup (no real connection attempted).
    client = TestClient(app)
    with patch.object(main_module, "get_settings", return_value=pg_settings):
        response = client.get("/api/health")

    assert response.status_code == 200
    body = response.json()
    assert secret_pass not in response.text
    assert secret_user not in response.text
    # Credentials replaced by the mask sentinel (URL-encoded as %2A by SQLAlchemy).
    assert "***" in body["db_path"] or "%2A%2A%2A" in body["db_path"]
    # Non-credential parts of the URL are preserved.
    assert "db.internal" in body["db_path"]

    settings_module.get_settings.cache_clear()


def test_api_health_passes_through_sqlite_url(monkeypatch):
    """sqlite URLs have no credentials and must pass through unchanged."""
    monkeypatch.setenv("DATABASE_URL", "sqlite:///./local.sqlite")

    from app.config import settings as settings_module

    settings_module.get_settings.cache_clear()

    from app.server.main import create_app

    app = create_app(app_token="test-token")
    with TestClient(app) as client:
        response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["db_path"] == "sqlite:///./local.sqlite"

    settings_module.get_settings.cache_clear()
