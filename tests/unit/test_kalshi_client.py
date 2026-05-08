from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.providers.exchanges.kalshi_client import KalshiClient
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

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        result = client.get_balance()
    assert result["balance"] == 5000
    assert captured["headers"]["kalshi-access-key"] == "test-key"
    assert "kalshi-access-timestamp" in captured["headers"]
    assert "kalshi-access-signature" in captured["headers"]


def test_get_market_404_raises_market_error(private_key_pem: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "market_not_found"}})

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        with pytest.raises(KalshiMarketError):
            client.get_market("FAKE-TICKER")


def test_create_order_sends_payload(private_key_pem: Path) -> None:
    captured_body: dict = {}
    captured_path: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_path["path"] = request.url.path
        captured_body["json"] = json.loads(request.content)
        return httpx.Response(
            201, json={"order_id": "ord123", "fill_count": "1.00", "remaining_count": "0.00"}
        )

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        result = client.create_order(
            ticker="X-TICKER",
            side="bid",
            count=1,
            price_dollars=0.56,
            client_order_id="intent-1",
            post_only=True,
            cancel_order_on_pause=True,
        )
    assert result["order_id"] == "ord123"
    assert captured_path["path"] == "/trade-api/v2/portfolio/events/orders"
    assert captured_body["json"]["ticker"] == "X-TICKER"
    assert captured_body["json"]["side"] == "bid"
    assert captured_body["json"]["count"] == "1.00"
    assert captured_body["json"]["price"] == "0.5600"
    assert captured_body["json"]["time_in_force"] == "fill_or_kill"
    assert captured_body["json"]["client_order_id"] == "intent-1"
    assert captured_body["json"]["post_only"] is True
    assert captured_body["json"]["cancel_order_on_pause"] is True


def test_get_balance_401_raises_auth(private_key_pem: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad sig"})

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        with pytest.raises(KalshiAuthError):
            client.get_balance()


def test_get_order_sends_correct_path(private_key_pem: Path) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(200, json={"order": {"order_id": "ord1", "status": "executed", "fills": []}})

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        result = client.get_order("ord1")
    assert "ord1" in captured["path"]
    assert result["order"]["order_id"] == "ord1"


def test_base_url_with_api_root_is_not_doubled(private_key_pem: Path) -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(200, json={"balance": 5000})

    client = KalshiClient(
        api_key_id="test-key",
        private_key_path=private_key_pem,
        base_url="https://external-api.kalshi.com/trade-api/v2",
        transport=httpx.MockTransport(handler),
    )
    try:
        client.get_balance()
    finally:
        client.close()
    assert captured["path"] == "/trade-api/v2/portfolio/balance"


def test_get_fills_filters_by_order_id_without_signing_query(private_key_pem: Path) -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["query"] = request.url.query.decode()
        return httpx.Response(200, json={"fills": [{"fill_id": "f1"}], "cursor": ""})

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        result = client.get_fills(order_id="ord1", ticker="T1")
    assert captured["path"] == "/trade-api/v2/portfolio/fills"
    assert "order_id=ord1" in captured["query"]
    assert "ticker=T1" in captured["query"]
    assert result["fills"][0]["fill_id"] == "f1"


def test_get_orderbook_positions_orders_and_cancel_paths(private_key_pem: Path) -> None:
    seen: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.url.query.decode()))
        if request.url.path.endswith("/orderbook"):
            return httpx.Response(200, json={"orderbook": {"yes": [[50, 1]], "no": [[48, 1]]}})
        if request.url.path.endswith("/portfolio/positions"):
            return httpx.Response(200, json={"market_positions": [{"ticker": "T1", "position": 1}]})
        if request.url.path.endswith("/portfolio/orders") and request.method == "GET":
            return httpx.Response(200, json={"orders": [{"order_id": "ord1"}]})
        if request.url.path.endswith("/portfolio/events/orders/ord1"):
            return httpx.Response(200, json={"order": {"order_id": "ord1", "status": "canceled"}})
        return httpx.Response(404, json={})

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        assert client.get_orderbook("T1", depth=5)["orderbook"]["yes"]
        assert client.get_positions(ticker="T1")["market_positions"][0]["ticker"] == "T1"
        assert client.get_orders(status="resting")["orders"][0]["order_id"] == "ord1"
        assert client.cancel_order("ord1")["order"]["status"] == "canceled"

    assert ("GET", "/trade-api/v2/markets/T1/orderbook", "depth=5") in seen
    assert any(method == "GET" and path == "/trade-api/v2/portfolio/positions" for method, path, _query in seen)
    assert any(method == "GET" and path == "/trade-api/v2/portfolio/orders" for method, path, _query in seen)
    assert ("DELETE", "/trade-api/v2/portfolio/events/orders/ord1", "") in seen


def test_create_exit_order_for_no_converts_to_yes_book_reduce_only(private_key_pem: Path) -> None:
    captured_body: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.update(json.loads(request.content))
        return httpx.Response(201, json={"order_id": "exit1"})

    with _client_with(private_key_pem, httpx.MockTransport(handler)) as client:
        result = client.create_exit_order(
            ticker="T1",
            held_side="no",
            count=1,
            exit_price_dollars=0.65,
            client_order_id="exit-no-1",
        )

    assert result["order_id"] == "exit1"
    assert captured_body["side"] == "bid"
    assert captured_body["price"] == "0.3500"
    assert captured_body["reduce_only"] is True
