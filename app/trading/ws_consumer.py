"""Kalshi WebSocket consumer that pumps frames into a MarketBook.

Connection lifecycle:
  1. Sign handshake with RSA-PSS over (timestamp + "GET" + path).
  2. Open ws connection with KALSHI-ACCESS-* headers.
  3. Send subscribe command for ("ticker",) on the configured tickers.
  4. Receive frames, parse, MarketBook.update().
  5. On disconnect: backoff and reconnect (Task 8). On 401: increment auth
     failure counter; stop after threshold (Task 9). Ping in Task 10.

This task implements steps 1-4. Reconnect/auth/ping arrive in Tasks 8-10.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from app.providers.exchanges.kalshi_signing import sign_request
from app.trading.market_book import MarketBook
from app.trading.ws_frames import parse_frame

_LOG = logging.getLogger(__name__)
_DEFAULT_CHANNELS: tuple[str, ...] = ("ticker",)


def _is_auth_status(exc: InvalidStatus) -> bool:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in (401, 403)


@dataclass(frozen=True)
class KalshiWsCredentials:
    api_key_id: str
    private_key_path: Path


@dataclass
class _ConsumerState:
    is_connected: bool = False
    last_message_at: float = 0.0
    reconnect_count: int = 0
    consecutive_auth_failures: int = 0
    stopped: bool = False
    fatal_auth_failure: bool = False


class KalshiWebSocketConsumer:
    def __init__(
        self,
        *,
        base_url: str,
        credentials: KalshiWsCredentials,
        book: MarketBook,
        tickers: list[str],
        ping_interval_seconds: int = 10,
        max_backoff_seconds: int = 30,
        max_consecutive_auth_failures: int = 5,
        channels: tuple[str, ...] = _DEFAULT_CHANNELS,
    ) -> None:
        self._base_url = base_url
        self._creds = credentials
        self._book = book
        self._tickers = list(tickers)
        self._ping_interval = ping_interval_seconds
        self._max_backoff = max_backoff_seconds
        self._max_auth_failures = max_consecutive_auth_failures
        self._channels = tuple(channels) or _DEFAULT_CHANNELS
        self._state = _ConsumerState()
        self._stop_event = asyncio.Event()
        self._next_subscribe_id = 1
        self._ws: ClientConnection | None = None

    @property
    def is_connected(self) -> bool:
        return self._state.is_connected

    @property
    def reconnect_count(self) -> int:
        return self._state.reconnect_count

    @property
    def consecutive_auth_failures(self) -> int:
        return self._state.consecutive_auth_failures

    @property
    def last_message_at(self) -> float:
        return self._state.last_message_at

    async def stop(self) -> None:
        self._state.stopped = True
        self._stop_event.set()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def run(self) -> None:
        if not self._tickers:
            _LOG.info("Kalshi WS consumer started with no tickers; idling")
            await self._stop_event.wait()
            return
        backoff = 1.0
        while not self._state.stopped and not self._state.fatal_auth_failure:
            try:
                await self._run_once()
                backoff = 1.0  # reset on clean exit
            except InvalidStatus as exc:
                if _is_auth_status(exc):
                    self._state.consecutive_auth_failures += 1
                    _LOG.error(
                        "Kalshi WS auth rejected (%d/%d)",
                        self._state.consecutive_auth_failures,
                        self._max_auth_failures,
                    )
                    if self._state.consecutive_auth_failures >= self._max_auth_failures:
                        self._state.fatal_auth_failure = True
                        break
                else:
                    _LOG.warning("Kalshi WS handshake failed: %s", exc)
            except (ConnectionClosed, OSError) as exc:
                _LOG.info("Kalshi WS disconnected: %s", exc)
            except Exception:
                _LOG.exception("Unexpected Kalshi WS error")
            finally:
                self._state.is_connected = False
            if self._state.stopped or self._state.fatal_auth_failure:
                break
            self._state.reconnect_count += 1
            sleep_for = min(backoff, float(self._max_backoff))
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_for)
                break  # stopped while sleeping
            except TimeoutError:
                pass
            backoff = min(backoff * 2.0, float(self._max_backoff))

    async def _run_once(self) -> None:
        headers = self._signed_headers()
        async with connect(self._base_url, additional_headers=headers) as ws:
            self._ws = ws
            self._state.is_connected = True
            self._state.consecutive_auth_failures = 0
            try:
                await self._send_subscribe(ws)
                await self._receive_loop(ws)
            finally:
                self._ws = None

    def _signed_headers(self) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        path = urlparse(self._base_url).path or "/trade-api/ws/v2"
        signature = sign_request(self._creds.private_key_path, ts, "GET", path)
        return {
            "KALSHI-ACCESS-KEY": self._creds.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def _send_subscribe(self, ws: ClientConnection) -> None:
        msg = {
            "id": self._next_subscribe_id,
            "cmd": "subscribe",
            "params": {
                "channels": list(self._channels),
                "market_tickers": list(self._tickers),
            },
        }
        self._next_subscribe_id += 1
        await ws.send(json.dumps(msg))

    async def _receive_loop(self, ws: ClientConnection) -> None:
        try:
            async for raw in ws:
                if self._state.stopped:
                    break
                self._state.last_message_at = time.time()
                try:
                    frame = json.loads(raw)
                except json.JSONDecodeError:
                    _LOG.warning("Malformed JSON frame; skipping")
                    continue
                entry = parse_frame(frame)
                if entry is None:
                    _LOG.debug(
                        "Frame skipped (non-ticker or invalid): %s",
                        frame.get("type") if isinstance(frame, dict) else type(frame).__name__,
                    )
                    continue
                await self._book.update(entry)
        except ConnectionClosed:
            if not self._state.stopped:
                raise
