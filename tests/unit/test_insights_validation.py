from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.server.routers import insights


class _StubEntry:
    injury_status_by_player_id: dict[int, object] = {}


class _StubCache:
    def get_cached(self) -> _StubEntry:
        return _StubEntry()


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(insights.router)
    app.state.board_cache = _StubCache()
    return TestClient(app, raise_server_exceptions=False)


def test_insights_malformed_player_ids_returns_422() -> None:
    resp = _client().get("/api/insights/injuries", params={"player_ids": "abc"})
    assert resp.status_code == 422


def test_insights_too_many_player_ids_returns_422() -> None:
    ids = ",".join("1" for _ in range(10_000))
    resp = _client().get("/api/insights/injuries", params={"player_ids": ids})
    assert resp.status_code == 422
