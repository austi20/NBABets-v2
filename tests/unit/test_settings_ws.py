import importlib

import pytest


@pytest.fixture
def reload_settings(monkeypatch):
    def _reload():
        import app.config.settings as settings_mod
        importlib.reload(settings_mod)
        return settings_mod.get_settings()
    return _reload


def test_ws_settings_have_expected_defaults(monkeypatch, reload_settings):
    for var in (
        "KALSHI_WS_ENABLED",
        "KALSHI_WS_BASE_URL",
        "KALSHI_WS_MAX_BACKOFF_SECONDS",
        "KALSHI_WS_PING_INTERVAL_SECONDS",
        "KALSHI_WS_MAX_CONSECUTIVE_AUTH_FAILURES",
    ):
        monkeypatch.delenv(var, raising=False)
    settings = reload_settings()
    assert settings.kalshi_ws_enabled is False
    assert settings.kalshi_ws_base_url == "wss://api.elections.kalshi.com/trade-api/ws/v2"
    assert settings.kalshi_ws_max_backoff_seconds == 30
    assert settings.kalshi_ws_ping_interval_seconds == 10
    assert settings.kalshi_ws_max_consecutive_auth_failures == 5


def test_ws_enabled_overridable_via_env(monkeypatch, reload_settings):
    monkeypatch.setenv("KALSHI_WS_ENABLED", "true")
    monkeypatch.setenv("KALSHI_WS_BASE_URL", "wss://demo-api.kalshi.co/trade-api/ws/v2")
    settings = reload_settings()
    assert settings.kalshi_ws_enabled is True
    assert settings.kalshi_ws_base_url == "wss://demo-api.kalshi.co/trade-api/ws/v2"
