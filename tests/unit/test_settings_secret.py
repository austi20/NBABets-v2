from pydantic import SecretStr


def test_balldontlie_key_has_no_hardcoded_default(monkeypatch):
    monkeypatch.delenv("BALLDONTLIE_API_KEY", raising=False)
    from app.config import settings as settings_module
    settings_module.get_settings.cache_clear()
    cfg = settings_module.get_settings()
    assert cfg.balldontlie_api_key is None or (
        isinstance(cfg.balldontlie_api_key, SecretStr)
        and cfg.balldontlie_api_key.get_secret_value() == ""
    )


def test_balldontlie_key_is_secretstr_when_set(monkeypatch):
    monkeypatch.setenv("BALLDONTLIE_API_KEY", "fake-key")
    from app.config import settings as settings_module
    settings_module.get_settings.cache_clear()
    cfg = settings_module.get_settings()
    assert isinstance(cfg.balldontlie_api_key, SecretStr)
    assert "fake-key" not in repr(cfg)


def test_kalshi_api_key_id_is_secretstr(monkeypatch):
    monkeypatch.setenv("KALSHI_API_KEY_ID", "k1")
    from app.config import settings as settings_module
    settings_module.get_settings.cache_clear()
    cfg = settings_module.get_settings()
    assert isinstance(cfg.kalshi_api_key_id, SecretStr)
    assert "k1" not in repr(cfg)
