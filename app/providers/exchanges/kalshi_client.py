from __future__ import annotations

import json
import time
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

from app.providers.exchanges.kalshi_errors import classify_response
from app.providers.exchanges.kalshi_signing import sign_request

_API_ROOT = "/trade-api/v2"


def _normalise_base_url(base_url: str) -> tuple[str, str]:
    url = httpx.URL(base_url.rstrip("/"))
    root_path = url.path.rstrip("/")
    if root_path.endswith(_API_ROOT):
        api_path = root_path
    else:
        api_path = f"{root_path}{_API_ROOT}" if root_path else _API_ROOT
    origin = str(url.copy_with(path="/")).rstrip("/")
    return origin, api_path


def _fixed(value: int | float | Decimal | str, places: str) -> str:
    try:
        decimal = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid fixed-point value: {value!r}") from exc
    return format(decimal.quantize(Decimal(places)), "f")


class KalshiClient:
    def __init__(
        self,
        *,
        api_key_id: str,
        private_key_path: Path | str,
        base_url: str = "https://external-api.kalshi.com/trade-api/v2",
        timeout_seconds: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key_id:
            raise ValueError("KalshiClient requires a non-empty api_key_id")
        self._api_key_id = api_key_id
        self._private_key_path = Path(private_key_path)
        self._base_url, self._api_root_path = _normalise_base_url(base_url)
        self._client = httpx.Client(
            base_url=self._base_url,
            timeout=timeout_seconds,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> KalshiClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def _signed_headers(self, method: str, path: str) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        signature = sign_request(self._private_key_path, ts, method, path)
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "accept": "application/json",
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: Mapping[str, str | int] | None = None,
    ) -> dict[str, Any]:
        path = f"{self._api_root_path}{endpoint}"
        headers = self._signed_headers(method, path)
        if json_body is not None:
            headers["content-type"] = "application/json"
        response = self._client.request(
            method,
            path,
            headers=headers,
            params=params,
            content=json.dumps(json_body).encode("utf-8") if json_body is not None else None,
        )
        classify_response(response.status_code, response.content, dict(response.headers))
        if not response.content:
            return {}
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Kalshi API response must be a JSON object")
        return payload

    def get_balance(self) -> dict[str, Any]:
        return self._request("GET", "/portfolio/balance")

    def get_market(self, ticker: str) -> dict[str, Any]:
        return self._request("GET", f"/markets/{ticker}")

    def create_order(
        self,
        *,
        ticker: str,
        side: str,
        count: int | float | Decimal | str,
        price_dollars: int | float | Decimal | str,
        client_order_id: str,
        time_in_force: str = "fill_or_kill",
        self_trade_prevention_type: str = "taker_at_cross",
        post_only: bool | None = None,
        cancel_order_on_pause: bool | None = None,
        reduce_only: bool | None = None,
    ) -> dict[str, Any]:
        normalized_side = side.strip().lower()
        if normalized_side not in {"bid", "ask"}:
            raise ValueError("Kalshi V2 order side must be 'bid' or 'ask'")
        body: dict[str, Any] = {
            "ticker": ticker,
            "side": normalized_side,
            "count": _fixed(count, "0.01"),
            "price": _fixed(price_dollars, "0.0001"),
            "client_order_id": client_order_id,
            "time_in_force": time_in_force,
            "self_trade_prevention_type": self_trade_prevention_type,
        }
        if post_only is not None:
            body["post_only"] = bool(post_only)
        if cancel_order_on_pause is not None:
            body["cancel_order_on_pause"] = bool(cancel_order_on_pause)
        if reduce_only is not None:
            body["reduce_only"] = bool(reduce_only)
        return self._request("POST", "/portfolio/events/orders", json_body=body)

    def create_exit_order(
        self,
        *,
        ticker: str,
        held_side: str,
        count: int | float | Decimal | str,
        exit_price_dollars: int | float | Decimal | str,
        client_order_id: str,
        time_in_force: str = "immediate_or_cancel",
        self_trade_prevention_type: str = "taker_at_cross",
        cancel_order_on_pause: bool | None = True,
    ) -> dict[str, Any]:
        normalized = held_side.strip().lower()
        if normalized in {"yes", "over", "buy_yes"}:
            order_side = "ask"
            yes_book_price = Decimal(str(exit_price_dollars))
        elif normalized in {"no", "under", "buy_no"}:
            order_side = "bid"
            yes_book_price = Decimal("1") - Decimal(str(exit_price_dollars))
        else:
            raise ValueError("held_side must be yes/over or no/under")
        if yes_book_price < 0 or yes_book_price > 1:
            raise ValueError("exit_price_dollars must convert to a YES-book price between 0 and 1")
        return self.create_order(
            ticker=ticker,
            side=order_side,
            count=count,
            price_dollars=yes_book_price,
            client_order_id=client_order_id,
            time_in_force=time_in_force,
            self_trade_prevention_type=self_trade_prevention_type,
            cancel_order_on_pause=cancel_order_on_pause,
            reduce_only=True,
        )

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._request("GET", f"/portfolio/orders/{order_id}")

    def get_orders(
        self,
        *,
        ticker: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": max(1, min(int(limit), 1000))}
        if ticker:
            params["ticker"] = ticker
        if status:
            params["status"] = status
        return self._request("GET", "/portfolio/orders", params=params)

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"/portfolio/events/orders/{order_id}")

    def get_fills(
        self,
        *,
        order_id: str | None = None,
        ticker: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": max(1, min(int(limit), 1000))}
        if order_id:
            params["order_id"] = order_id
        if ticker:
            params["ticker"] = ticker
        return self._request("GET", "/portfolio/fills", params=params)

    def get_positions(
        self,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        count_filter: str | None = "position,total_traded",
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": max(1, min(int(limit), 1000))}
        if ticker:
            params["ticker"] = ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if count_filter:
            params["count_filter"] = count_filter
        return self._request("GET", "/portfolio/positions", params=params)

    def get_orderbook(self, ticker: str, *, depth: int = 0) -> dict[str, Any]:
        params: dict[str, str | int] = {"depth": max(0, min(int(depth), 100))}
        return self._request("GET", f"/markets/{ticker}/orderbook", params=params)
