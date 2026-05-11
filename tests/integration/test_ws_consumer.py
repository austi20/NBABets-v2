from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import websockets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.trading.market_book import MarketBook
from app.trading.ws_consumer import KalshiWebSocketConsumer, KalshiWsCredentials


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


class FakeKalshiServer:
    """Minimal in-process Kalshi WS server for tests."""

    def __init__(self) -> None:
        self.received_subscribes: list[dict] = []
        self.received_headers: list[dict] = []
        self._server = None
        self.port: int = 0
        self._connections: list = []
        self._frames_to_send: list[dict] = []
        self._auth_reject = False

    def queue_frame(self, frame: dict) -> None:
        self._frames_to_send.append(frame)

    def set_auth_reject(self, value: bool) -> None:
        self._auth_reject = value

    async def _handler(self, ws) -> None:
        if self._auth_reject:
            await ws.close(code=4401, reason="unauthorized")
            return
        # websockets v16: ServerConnection.request.headers exposes handshake headers.
        request = getattr(ws, "request", None)
        if request is not None and getattr(request, "headers", None) is not None:
            headers = dict(request.headers)
        else:
            headers = dict(getattr(ws, "request_headers", {}))
        self.received_headers.append(headers)
        self._connections.append(ws)
        try:
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("cmd") == "subscribe":
                    self.received_subscribes.append(msg)
                    for frame in self._frames_to_send:
                        await ws.send(json.dumps(frame))
        except websockets.ConnectionClosed:
            pass
        finally:
            if ws in self._connections:
                self._connections.remove(ws)

    async def start(self) -> None:
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        for ws in list(self._connections):
            try:
                await ws.close()
            except Exception:
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


@pytest.fixture
async def fake_server():
    server = FakeKalshiServer()
    await server.start()
    yield server
    await server.stop()


@pytest.mark.asyncio
async def test_consumer_connects_signs_and_subscribes(fake_server, rsa_key_file):
    book = MarketBook()
    creds = KalshiWsCredentials(api_key_id="key-abc", private_key_path=rsa_key_file)
    fake_server.queue_frame(
        {
            "type": "ticker",
            "msg": {
                "market_ticker": "KXNBA-LAL-W",
                "yes_bid_dollars": 0.52,
                "yes_ask_dollars": 0.55,
            },
        }
    )
    consumer = KalshiWebSocketConsumer(
        base_url=f"ws://127.0.0.1:{fake_server.port}",
        credentials=creds,
        book=book,
        tickers=["KXNBA-LAL-W"],
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=2,
    )
    task = asyncio.create_task(consumer.run())
    # wait for first book update
    deadline = datetime.now(UTC).timestamp() + 2.0
    while datetime.now(UTC).timestamp() < deadline:
        if book.get("KXNBA-LAL-W") is not None:
            break
        await asyncio.sleep(0.02)
    await consumer.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert book.get("KXNBA-LAL-W") is not None
    assert book.get("KXNBA-LAL-W").yes_bid == 0.52
    assert len(fake_server.received_subscribes) == 1
    sub = fake_server.received_subscribes[0]
    assert sub["cmd"] == "subscribe"
    assert "ticker" in sub["params"]["channels"]
    assert sub["params"]["market_tickers"] == ["KXNBA-LAL-W"]
    headers = fake_server.received_headers[0]
    assert "KALSHI-ACCESS-KEY" in headers or "kalshi-access-key" in headers
    if "KALSHI-ACCESS-KEY" in headers:
        assert headers["KALSHI-ACCESS-KEY"] == "key-abc"
    else:
        assert headers["kalshi-access-key"] == "key-abc"
    assert (
        "KALSHI-ACCESS-SIGNATURE" in headers or "kalshi-access-signature" in headers
    )
    assert (
        "KALSHI-ACCESS-TIMESTAMP" in headers or "kalshi-access-timestamp" in headers
    )


async def test_consumer_reconnects_after_server_close(fake_server, rsa_key_file):
    book = MarketBook()
    creds = KalshiWsCredentials(api_key_id="key-abc", private_key_path=rsa_key_file)
    fake_server.queue_frame({
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.50,
            "yes_ask_dollars": 0.55,
        },
    })
    consumer = KalshiWebSocketConsumer(
        base_url=f"ws://127.0.0.1:{fake_server.port}",
        credentials=creds,
        book=book,
        tickers=["KXNBA-LAL-W"],
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=5,
    )
    task = asyncio.create_task(consumer.run())
    # wait for first connect + frame
    for _ in range(100):
        if consumer.is_connected and book.get("KXNBA-LAL-W") is not None:
            break
        await asyncio.sleep(0.02)
    assert consumer.is_connected

    # force-close server side, expect reconnect
    for ws in list(fake_server._connections):
        await ws.close()
    for _ in range(200):
        if consumer.reconnect_count >= 1 and consumer.is_connected:
            break
        await asyncio.sleep(0.02)
    assert consumer.reconnect_count >= 1
    assert consumer.is_connected

    await consumer.stop()
    await asyncio.wait_for(task, timeout=2.0)
