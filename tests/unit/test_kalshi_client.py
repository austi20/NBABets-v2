from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from app.providers.exchanges.kalshi_client import KalshiClient
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.providers.exchanges.kalshi_errors import KalshiAuthError, KalshiMarketError


@pytest.fixture()
def private_key_pem(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "key.pem"
    path.write_bytes(pem)
    return path


def _client_with(private_key_pem: Path, transport: httpx.MockTransport) -> KalshiClient:
    return KalshiClient(
        api_key_id="test-key",
        private_key_path=private_key_pem,
        base_url="https://api.example",
        transport=transport,
    )


def test_get_balance_includes_signing_headers(private_key_pem: Path) -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        return httpx.Response(200, json={"balance": 5000})

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    result = client.get_balance()
    assert result["balance"] == 5000
    assert captured["headers"]["kalshi-access-key"] == "test-key"
    assert "kalshi-access-timestamp" in captured["headers"]
    assert "kalshi-access-signature" in captured["headers"]


def test_get_market_404_raises_market_error(private_key_pem: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "market_not_found"}})

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    with pytest.raises(KalshiMarketError):
        client.get_market("FAKE-TICKER")


def test_create_order_sends_payload(private_key_pem: Path) -> None:
    captured_body: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body["json"] = json.loads(request.content)
        return httpx.Response(
            200, json={"order": {"order_id": "ord123", "status": "executed"}}
        )

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    result = client.create_order(
        ticker="X-TICKER",
        side="yes",
        count=1,
        order_type="market",
        client_order_id="intent-1",
    )
    assert result["order"]["order_id"] == "ord123"
    assert captured_body["json"]["ticker"] == "X-TICKER"
    assert captured_body["json"]["count"] == 1
    assert captured_body["json"]["client_order_id"] == "intent-1"


def test_get_balance_401_raises_auth(private_key_pem: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad sig"})

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    with pytest.raises(KalshiAuthError):
        client.get_balance()
