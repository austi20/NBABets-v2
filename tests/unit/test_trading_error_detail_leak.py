from __future__ import annotations

import pytest
from fastapi import HTTPException

import app.server.routers.trading as trading


def test_brain_status_error_detail_does_not_leak_exception(monkeypatch) -> None:
    secret = "boom-secret-path /home/user/private.key"

    def _raise(_settings):  # noqa: ANN001
        raise RuntimeError(secret)

    monkeypatch.setattr(trading, "load_last_brain_status", _raise)

    with pytest.raises(HTTPException) as excinfo:
        trading.trading_brain_status()

    assert excinfo.value.status_code == 500
    assert secret not in str(excinfo.value.detail)
    assert excinfo.value.detail == "Decision brain status unavailable"


def test_wallet_balance_error_detail_does_not_leak_exception(monkeypatch) -> None:
    secret = "kalshi-internal-error 0xDEADBEEF"

    class _Settings:
        kalshi_api_key_id = type("K", (), {"get_secret_value": lambda self: "id"})()
        kalshi_private_key_path = "key.pem"
        kalshi_base_url = "https://example.test"

    class _Client:
        def __init__(self, **_kwargs) -> None:  # noqa: ANN003
            pass

        def get_balance(self):  # noqa: ANN201
            raise RuntimeError(secret)

        def close(self) -> None:
            pass

    monkeypatch.setattr(trading, "get_settings", lambda: _Settings())
    monkeypatch.setattr(trading, "KalshiClient", _Client)

    with pytest.raises(HTTPException) as excinfo:
        trading.trading_wallet_balance(request=None)  # type: ignore[arg-type]

    assert excinfo.value.status_code == 502
    assert secret not in str(excinfo.value.detail)
    assert excinfo.value.detail == "Kalshi balance fetch failed"
