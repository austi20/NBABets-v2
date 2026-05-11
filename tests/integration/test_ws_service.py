from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.trading.market_book import MarketBook
from app.trading.ws_consumer import KalshiWsCredentials
from app.trading.ws_service import KalshiMarketService


@pytest.fixture
def rsa_key_file(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "kalshi.pem"
    path.write_bytes(pem)
    return path


def _write_symbols(path: Path, tickers: list[str]) -> None:
    payload = {
        "symbols": [
            {
                "kalshi_ticker": t,
                "recommendation": "buy_yes",
                "line_value": 24.5,
                "market_key": "points",
            }
            for t in tickers
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


async def test_service_starts_with_empty_tickers_and_does_not_connect(tmp_path, rsa_key_file):
    symbols = tmp_path / "kalshi_symbols.json"
    _write_symbols(symbols, [])
    creds = KalshiWsCredentials(api_key_id="key", private_key_path=rsa_key_file)
    book = MarketBook()
    service = KalshiMarketService(
        symbols_path=symbols,
        credentials=creds,
        book=book,
        base_url="ws://127.0.0.1:1",  # would fail to connect if attempted
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=5,
    )
    await service.start()
    await asyncio.sleep(0.05)
    assert service.is_connected is False
    assert service.tickers == ()
    await service.stop()


async def test_service_subscribes_to_tickers_from_symbols_file(tmp_path, rsa_key_file):
    from tests.integration.test_ws_consumer import FakeKalshiServer  # reuse

    server = FakeKalshiServer()
    await server.start()
    try:
        symbols = tmp_path / "kalshi_symbols.json"
        _write_symbols(symbols, ["KXA", "KXB"])
        creds = KalshiWsCredentials(api_key_id="key", private_key_path=rsa_key_file)
        book = MarketBook()
        service = KalshiMarketService(
            symbols_path=symbols,
            credentials=creds,
            book=book,
            base_url=f"ws://127.0.0.1:{server.port}",
            ping_interval_seconds=60,
            max_backoff_seconds=1,
            max_consecutive_auth_failures=5,
        )
        await service.start()
        for _ in range(100):
            if server.received_subscribes:
                break
            await asyncio.sleep(0.02)
        assert server.received_subscribes
        assert set(server.received_subscribes[0]["params"]["market_tickers"]) == {"KXA", "KXB"}
        await service.stop()
    finally:
        await server.stop()
