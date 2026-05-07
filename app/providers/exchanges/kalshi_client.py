from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx

from app.providers.exchanges.kalshi_errors import classify_response
from app.providers.exchanges.kalshi_signing import sign_request


class KalshiClient:
    def __init__(
        self,
        *,
        api_key_id: str,
        private_key_path: Path | str,
        base_url: str = "https://api.elections.kalshi.com",
        timeout_seconds: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key_id:
            raise ValueError("KalshiClient requires a non-empty api_key_id")
        self._api_key_id = api_key_id
        self._private_key_path = Path(private_key_path)
        self._base_url = base_url.rstrip("/")
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
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = self._signed_headers(method, path)
        if json_body is not None:
            headers["content-type"] = "application/json"
        response = self._client.request(
            method,
            path,
            headers=headers,
            content=json.dumps(json_body).encode("utf-8") if json_body is not None else None,
        )
        classify_response(response.status_code, response.content, dict(response.headers))
        if not response.content:
            return {}
        return response.json()

    def get_balance(self) -> dict[str, Any]:
        return self._request("GET", "/trade-api/v2/portfolio/balance")

    def get_market(self, ticker: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/markets/{ticker}")

    def create_order(
        self,
        *,
        ticker: str,
        side: str,
        count: int,
        order_type: str,
        client_order_id: str,
        max_price_cents: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "ticker": ticker,
            "side": side,
            "count": int(count),
            "type": order_type,
            "client_order_id": client_order_id,
            "action": "buy",
        }
        if max_price_cents is not None:
            body["yes_price"] = int(max_price_cents)
        return self._request("POST", "/trade-api/v2/portfolio/orders", json_body=body)

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/portfolio/orders/{order_id}")
