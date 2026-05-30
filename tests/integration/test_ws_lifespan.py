from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient


def _write_key(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "kalshi.pem"
    path.write_bytes(pem)
    return path


@pytest.fixture
def reset_settings():
    from app.config.settings import get_settings

    yield
    get_settings.cache_clear()


def _reload_create_app():
    # No module reload: create_app reads get_settings() at call time, so a
    # cleared settings cache is enough to pick up the test's monkeypatched env.
    # Reloading app.server.main here forked get_settings/create_app and leaked
    # divergent objects into other test modules that import those symbols.
    from app.config.settings import get_settings
    from app.server.main import create_app

    get_settings.cache_clear()
    return create_app


def test_lifespan_does_not_start_service_when_ws_disabled(tmp_path, reset_settings, monkeypatch):
    monkeypatch.delenv("KALSHI_WS_ENABLED", raising=False)
    monkeypatch.setenv("KALSHI_API_KEY_ID", "key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(_write_key(tmp_path)))
    monkeypatch.setenv("KALSHI_SYMBOLS_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setenv("KALSHI_DECISION_BRAIN_ENABLED", "false")
    monkeypatch.setenv("AUTO_INIT_BUDGET_FROM_WALLET", "false")
    create_app = _reload_create_app()

    app = create_app()
    with TestClient(app) as client:
        client.get("/api/health")
        service = getattr(app.state, "market_service", None)
        assert service is not None
        assert service.is_connected is False
        assert service.tickers == ()


def test_lifespan_starts_service_when_ws_enabled_with_no_tickers(tmp_path, reset_settings, monkeypatch):
    monkeypatch.setenv("KALSHI_WS_ENABLED", "true")
    monkeypatch.setenv("KALSHI_API_KEY_ID", "key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(_write_key(tmp_path)))
    # symbols file does not exist; service should idle, not crash
    monkeypatch.setenv("KALSHI_SYMBOLS_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setenv("KALSHI_DECISION_BRAIN_ENABLED", "false")
    monkeypatch.setenv("AUTO_INIT_BUDGET_FROM_WALLET", "false")
    create_app = _reload_create_app()

    app = create_app()
    with TestClient(app) as client:
        client.get("/api/health")
        service = app.state.market_service
        assert service.tickers == ()
        assert service.is_connected is False
