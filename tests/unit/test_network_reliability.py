from __future__ import annotations

import httpx

from app.providers.http import _network_error_category


def test_network_error_category_timeout() -> None:
    request = httpx.Request("GET", "https://example.com")
    exc = httpx.ReadTimeout("timed out", request=request)
    assert _network_error_category(exc) == "timeout"


def test_network_error_category_status() -> None:
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(status_code=503, request=request)
    exc = httpx.HTTPStatusError("server error", request=request, response=response)
    assert _network_error_category(exc) == "http_503"
