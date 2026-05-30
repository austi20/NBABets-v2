"""Tests for pull_historical_props auth/utc/except hygiene."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import httpx
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scripts.pull_historical_props as mod  # noqa: E402


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("boom", request=None, response=None)  # type: ignore[arg-type]

    def json(self) -> dict:
        return self._payload


class _RecordingClient:
    """Records the params of each GET and returns canned payloads."""

    def __init__(self, payloads: list[dict] | None = None) -> None:
        self.calls: list[dict] = []
        self._payloads = payloads or [{"data": []}]
        self._i = 0

    async def get(self, url: str, params: dict, timeout: int) -> _FakeResponse:
        self.calls.append(dict(params))
        payload = self._payloads[min(self._i, len(self._payloads) - 1)]
        self._i += 1
        return _FakeResponse(payload)


@pytest.mark.asyncio
async def test_pull_games_params_have_no_authorization() -> None:
    client = _RecordingClient([{"data": []}])
    await mod.pull_games(client, None, date(2026, 3, 15), date(2026, 3, 15))
    assert client.calls, "expected at least one request"
    for params in client.calls:
        assert "Authorization" not in params


@pytest.mark.asyncio
async def test_pull_props_params_have_no_placeholder() -> None:
    client = _RecordingClient([{"data": [], "meta": {}}])
    await mod.pull_props_for_game(client, None, 123)
    assert client.calls
    for params in client.calls:
        assert "_h" not in params


@pytest.mark.asyncio
async def test_get_json_does_not_swallow_non_http_errors() -> None:
    class _Boom:
        async def get(self, *a, **k):  # noqa: ANN002, ANN003
            raise ValueError("not an http error")

    with pytest.raises(ValueError):
        await mod.get_json(_Boom(), "http://x", {})


@pytest.mark.asyncio
async def test_pull_props_propagates_fetch_failure_not_empty() -> None:
    # A fetch that fails after retries must NOT be conflated with 0 results.
    async def _always_fail(*a, **k):  # noqa: ANN002, ANN003
        raise RuntimeError("giving up")

    client = _RecordingClient()
    # Patch get_json to simulate exhausted retries.
    orig = mod.get_json
    mod.get_json = _always_fail  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError):
            await mod.pull_props_for_game(client, None, 999)
    finally:
        mod.get_json = orig  # type: ignore[assignment]


def test_module_uses_timezone_aware_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    # The fetched_at stamp must be timezone-aware (datetime.now(UTC)),
    # not the deprecated naive datetime.utcnow().
    src = (mod.__file__ and Path(mod.__file__).read_text(encoding="utf-8")) or ""
    assert "utcnow()" not in src
    assert "datetime.now(UTC)" in src
