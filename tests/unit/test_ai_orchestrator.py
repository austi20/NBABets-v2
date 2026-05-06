from __future__ import annotations

import uuid
from pathlib import Path

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.models.all import AIProviderEvent
from app.services.ai_orchestrator import AIOrchestrator


class _FakeResponse:
    def __init__(self, body: dict[str, object]) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._body


class _FakeHealthResponse:
    """Minimal response for _verify_local_connection GET /health."""

    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code


class _FakeClient:
    def __init__(
        self,
        *,
        recorder: dict[str, object],
        response: _FakeResponse | None = None,
        exc: Exception | None = None,
        timeout: int | None = None,
    ) -> None:
        recorder["timeout"] = timeout
        self._recorder = recorder
        self._response = response
        self._exc = exc

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def get(self, url: str) -> _FakeHealthResponse:
        # Always return 200 for the preflight health check so existing tests keep working.
        return _FakeHealthResponse(200)

    def post(self, url: str, *, headers: dict[str, str] | None = None, json: dict[str, object] | None = None):
        self._recorder["url"] = url
        self._recorder["headers"] = headers
        self._recorder["json"] = json
        if self._exc is not None:
            raise self._exc
        return self._response


class _FailingHealthClient:
    """httpx.Client stand-in where GET /health raises a connection error."""

    def __init__(self, *, timeout: int | None = None) -> None:
        pass

    def __enter__(self) -> _FailingHealthClient:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def get(self, url: str) -> None:
        raise httpx.ConnectError("health check failed")

    def post(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("post() must not be called when preflight fails")


def _make_session(monkeypatch):
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_ai_orchestrator_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite:///{(root / 'ai.sqlite').resolve().as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    get_settings.cache_clear()
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_ai_orchestrator_uses_chat_completions_and_api_key(monkeypatch) -> None:
    from app.config.settings import get_settings

    recorder: dict[str, object] = {}
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:8080/v1/chat/completions")
    monkeypatch.setenv("AI_LOCAL_MODEL", "qwen35-9b-q8")
    monkeypatch.setenv("AI_LOCAL_API_KEY", "test-key")
    monkeypatch.setenv("AI_TIMEOUT_SECONDS", "12")
    session = _make_session(monkeypatch)

    def _client_factory(*, timeout: int):
        return _FakeClient(
            recorder=recorder,
            response=_FakeResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": "summary ok",
                            }
                        }
                    ]
                }
            ),
            timeout=timeout,
        )

    monkeypatch.setattr("app.services.ai_orchestrator.httpx.Client", _client_factory)

    result = AIOrchestrator(session).summarize(task_name="model_health", prompt="Summarize today's metrics.")

    assert result.provider == "local"
    assert result.model == "qwen35-9b-q8"
    assert result.text == "summary ok"
    assert recorder["url"] == "http://127.0.0.1:8080/v1/chat/completions"
    assert recorder["timeout"] == 12
    assert recorder["headers"] == {
        "Authorization": "Bearer test-key",
        "Content-Type": "application/json",
    }
    assert recorder["json"] == {
        "model": "qwen35-9b-q8",
        "messages": [{"role": "user", "content": "Summarize today's metrics."}],
        "stream": False,
        "temperature": 0.2,
    }
    events = session.query(AIProviderEvent).all()
    assert len(events) == 1
    assert events[0].status == "ok"
    assert events[0].detail == "Local llama.cpp request succeeded."
    get_settings.cache_clear()


def test_ai_orchestrator_fallback_mentions_llamacpp(monkeypatch) -> None:
    from app.config.settings import get_settings

    recorder: dict[str, object] = {}
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:8080/v1/chat/completions")
    monkeypatch.setenv("AI_LOCAL_MODEL", "qwen35-9b-q8")
    monkeypatch.setenv("AI_LOCAL_API_KEY", "test-key")
    session = _make_session(monkeypatch)

    def _client_factory(*, timeout: int):
        return _FakeClient(
            recorder=recorder,
            exc=httpx.ConnectError("dial failed"),
            timeout=timeout,
        )

    monkeypatch.setattr("app.services.ai_orchestrator.httpx.Client", _client_factory)

    result = AIOrchestrator(session).summarize(task_name="provider_health", prompt="Summarize provider health.")

    assert result.provider == "fallback"
    assert result.model == "deterministic"
    assert "llama.cpp server" in result.text
    assert "Ollama" not in result.text
    events = session.query(AIProviderEvent).order_by(AIProviderEvent.event_id.asc()).all()
    assert len(events) == 2
    assert events[0].status == "error"
    assert "dial failed" in (events[0].detail or "")
    assert events[1].status == "degraded"
    assert events[1].detail == "Local llama.cpp request failed or returned empty content."
    get_settings.cache_clear()


def test_verify_local_connection_returns_true_on_200(monkeypatch) -> None:
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    monkeypatch.setattr("app.services.ai_orchestrator.httpx.Client", lambda *, timeout: _FakeClient(recorder={}, response=None, timeout=timeout))
    assert AIOrchestrator(session)._verify_local_connection() is True
    get_settings.cache_clear()


def test_verify_local_connection_returns_false_on_connect_error(monkeypatch) -> None:
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    monkeypatch.setattr("app.services.ai_orchestrator.httpx.Client", _FailingHealthClient)
    assert AIOrchestrator(session)._verify_local_connection() is False
    get_settings.cache_clear()


def test_summarize_skips_try_local_when_preflight_fails(monkeypatch) -> None:
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    monkeypatch.setattr("app.services.ai_orchestrator.httpx.Client", _FailingHealthClient)

    result = AIOrchestrator(session).summarize(task_name="model_health", prompt="test")

    assert result.provider == "fallback"
    assert result.model == "deterministic"
    events = session.query(AIProviderEvent).order_by(AIProviderEvent.event_id.asc()).all()
    # preflight error + fallback degraded
    assert len(events) == 2
    assert events[0].status == "error"
    assert "Pre-flight" in (events[0].detail or "")
    assert events[1].status == "degraded"
    get_settings.cache_clear()
