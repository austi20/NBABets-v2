from __future__ import annotations

import json
from collections.abc import Mapping


class KalshiApiError(RuntimeError):
    def __init__(self, status: int, body: bytes, message: str = "") -> None:
        self.status = status
        self.body = body
        super().__init__(message or f"Kalshi API error (status={status})")


class KalshiAuthError(KalshiApiError):
    pass


class KalshiMarketError(KalshiApiError):
    pass


class KalshiInsufficientFunds(KalshiApiError):
    pass


class KalshiRateLimited(KalshiApiError):
    def __init__(self, status: int, body: bytes, retry_after: int) -> None:
        super().__init__(status, body, message=f"Kalshi rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


class KalshiServerError(KalshiApiError):
    pass


def _looks_like_insufficient_funds(body: bytes) -> bool:
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return False
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return False
    code = str(error.get("code", "")).lower()
    return "insufficient" in code or "funds" in code


def _parse_retry_after(headers: Mapping[str, str]) -> int:
    lower = {k.lower(): v for k, v in headers.items()}
    raw = lower.get("retry-after") or "1"
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def classify_response(status: int, body: bytes, headers: Mapping[str, str]) -> None:
    if 200 <= status < 300:
        return
    if status in (401, 403):
        raise KalshiAuthError(status, body)
    if status == 404:
        raise KalshiMarketError(status, body)
    if status == 429:
        raise KalshiRateLimited(status, body, _parse_retry_after(headers))
    if _looks_like_insufficient_funds(body):
        raise KalshiInsufficientFunds(status, body)
    if status >= 500:
        raise KalshiServerError(status, body)
    raise KalshiApiError(status, body)
