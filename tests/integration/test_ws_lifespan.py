from __future__ import annotations

import importlib
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
    import app.config.settings as settings_mod
    yield
    importlib.reload(settings_mod)


def _reload_create_app():
    import app.config.settings as settings_mod
    import app.server.main as main_mod

    importlib.reload(settings_mod)
    importlib.reload(main_mod)
    return main_mod.create_app


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
