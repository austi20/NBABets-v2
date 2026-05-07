from __future__ import annotations

import pytest
from app.providers.exchanges.kalshi_errors import (
    KalshiApiError,
    KalshiAuthError,
    KalshiInsufficientFunds,
    KalshiMarketError,
    KalshiRateLimited,
    KalshiServerError,
    classify_response,
)


def test_classify_401_returns_auth_error() -> None:
    with pytest.raises(KalshiAuthError):
        classify_response(401, b"{}", {})


def test_classify_404_returns_market_error() -> None:
    with pytest.raises(KalshiMarketError):
        classify_response(404, b"{}", {})


def test_classify_insufficient_funds_body() -> None:
    body = b'{"error":{"code":"insufficient_funds","message":"need more"}}'
    with pytest.raises(KalshiInsufficientFunds):
        classify_response(400, body, {})


def test_classify_429_surfaces_retry_after() -> None:
    with pytest.raises(KalshiRateLimited) as exc_info:
        classify_response(429, b"{}", {"retry-after": "7"})
    assert exc_info.value.retry_after == 7


def test_classify_500_returns_server_error() -> None:
    with pytest.raises(KalshiServerError):
        classify_response(500, b"{}", {})


def test_classify_2xx_no_raise() -> None:
    classify_response(200, b"{}", {})


def test_classify_unknown_4xx_returns_generic() -> None:
    with pytest.raises(KalshiApiError):
        classify_response(418, b"{}", {})
