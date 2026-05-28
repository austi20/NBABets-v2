from __future__ import annotations

import hmac

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.server.auth import AppTokenMiddleware


def _make_app(token: str = "tok-correct") -> FastAPI:
    app = FastAPI()

    @app.get("/api/trading/snapshot")
    @app.head("/api/trading/snapshot")
    async def snapshot():
        return {"ok": True}

    @app.post("/api/trading/intent")
    async def intent():
        return {"ok": True}

    @app.post("/api/startup/run")
    async def startup_run():
        return {"ok": True}

    @app.get("/healthz")
    async def health():
        return {"ok": True}

    app.state.app_token = token
    app.add_middleware(
        AppTokenMiddleware,
        protected_prefixes=("/api/trading", "/api/local-agent", "/api/startup/run"),
    )
    return app


def test_get_requires_token() -> None:
    c = TestClient(_make_app(), raise_server_exceptions=False)
    assert c.get("/api/trading/snapshot").status_code == 401
    assert (
        c.get("/api/trading/snapshot", headers={"X-App-Token": "tok-correct"}).status_code
        == 200
    )


def test_head_requires_token() -> None:
    c = TestClient(_make_app(), raise_server_exceptions=False)
    assert c.head("/api/trading/snapshot").status_code == 401
    assert (
        c.head("/api/trading/snapshot", headers={"X-App-Token": "tok-correct"}).status_code
        == 200
    )


def test_post_requires_token() -> None:
    c = TestClient(_make_app(), raise_server_exceptions=False)
    assert c.post("/api/trading/intent").status_code == 401
    assert (
        c.post("/api/trading/intent", headers={"X-App-Token": "tok-correct"}).status_code
        == 200
    )


def test_startup_run_is_protected() -> None:
    c = TestClient(_make_app(), raise_server_exceptions=False)
    assert c.post("/api/startup/run").status_code == 401
    assert (
        c.post("/api/startup/run", headers={"X-App-Token": "tok-correct"}).status_code
        == 200
    )


def test_unprotected_path_is_open() -> None:
    c = TestClient(_make_app(), raise_server_exceptions=False)
    assert c.get("/healthz").status_code == 200


def test_wrong_token_rejected() -> None:
    c = TestClient(_make_app(), raise_server_exceptions=False)
    assert (
        c.get("/api/trading/snapshot", headers={"X-App-Token": "wrong"}).status_code
        == 401
    )


def test_unconfigured_token_returns_503() -> None:
    app = FastAPI()

    @app.get("/api/trading/snapshot")
    async def s():
        return {"ok": True}

    # No app_token set on state -- should 503, not pass-through
    app.add_middleware(
        AppTokenMiddleware,
        protected_prefixes=("/api/trading",),
    )
    c = TestClient(app, raise_server_exceptions=False)
    assert (
        c.get("/api/trading/snapshot", headers={"X-App-Token": "anything"}).status_code
        == 503
    )


def test_compare_is_constant_time(monkeypatch) -> None:
    called: dict[str, int] = {"n": 0}
    real = hmac.compare_digest

    def spy(a: str | bytes, b: str | bytes) -> bool:
        called["n"] += 1
        return real(a, b)

    monkeypatch.setattr(hmac, "compare_digest", spy)
    c = TestClient(_make_app(), raise_server_exceptions=False)
    c.get("/api/trading/snapshot", headers={"X-App-Token": "wrong"})
    assert called["n"] >= 1
