from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime
from time import perf_counter, time
from typing import Any

import httpx
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt, wait_exponential

from app.config.settings import get_settings
from app.db.session import session_scope
from app.schemas.domain import ProviderFetchResult
from app.services.agents.run_service import AgentRunService

logger = logging.getLogger(__name__)

_CIRCUIT_FAILURES: dict[str, int] = defaultdict(int)
_CIRCUIT_OPEN_UNTIL: dict[str, float] = {}


class HttpProviderMixin:
    base_url: str

    def __init__(self) -> None:
        settings = get_settings()
        self._timeout = settings.request_timeout_seconds
        self._settings = settings

    async def _get(
        self,
        endpoint: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> ProviderFetchResult:
        circuit_key = f"{self.base_url}{endpoint}"
        if _CIRCUIT_OPEN_UNTIL.get(circuit_key, 0.0) > time():
            self._record_network_observation(
                status="error",
                latency_ms=0,
                endpoint=endpoint,
                error_category="circuit_open",
                detail="Circuit breaker is open for this endpoint.",
            )
            raise RuntimeError(f"Circuit open for endpoint={endpoint}")
        started = perf_counter()

        def _log_before_retry(retry_state: RetryCallState) -> None:
            provider = str(getattr(self, "provider_name", "unknown"))
            wait_s = float(retry_state.next_action.sleep) if retry_state.next_action else float(
                retry_state.upcoming_sleep
            )
            err = retry_state.outcome.exception() if retry_state.outcome and retry_state.outcome.failed else None
            logger.info(
                "http_get retry backoff: provider=%s endpoint=%s attempt=%s wait_seconds=%.2f error=%r",
                provider,
                endpoint,
                retry_state.attempt_number,
                wait_s,
                err,
            )

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._settings.network_retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception(_should_retry_http_error),
            before_sleep=_log_before_retry,
            reraise=True,
        ):
            with attempt:
                try:
                    async with httpx.AsyncClient(timeout=self._timeout, base_url=self.base_url) as client:
                        response = await client.get(endpoint, headers=headers, params=params)
                        response.raise_for_status()
                        payload = response.json()
                    _CIRCUIT_FAILURES[circuit_key] = 0
                    latency_ms = int((perf_counter() - started) * 1000)
                    self._record_network_observation(
                        status="ok",
                        latency_ms=latency_ms,
                        endpoint=endpoint,
                        payload={"http_status": response.status_code},
                    )
                    return ProviderFetchResult(
                        endpoint=f"{self.base_url}{endpoint}",
                        fetched_at=datetime.now(UTC),
                        payload=payload,
                    )
                except Exception as exc:
                    _CIRCUIT_FAILURES[circuit_key] += 1
                    failure_count = _CIRCUIT_FAILURES[circuit_key]
                    if failure_count >= self._settings.network_circuit_breaker_failures:
                        _CIRCUIT_OPEN_UNTIL[circuit_key] = time() + self._settings.network_circuit_breaker_open_seconds
                    latency_ms = int((perf_counter() - started) * 1000)
                    self._record_network_observation(
                        status="error",
                        latency_ms=latency_ms,
                        endpoint=endpoint,
                        error_category=_network_error_category(exc),
                        detail=str(exc),
                        payload={"consecutive_failures": failure_count},
                    )
                    raise
        raise RuntimeError("Unreachable retry loop")

    def _record_network_observation(
        self,
        *,
        status: str,
        latency_ms: int | None,
        endpoint: str,
        error_category: str | None = None,
        detail: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        provider_name = str(getattr(self, "provider_name", "unknown"))
        try:
            with session_scope() as session:
                AgentRunService(session).record(
                    task_id=f"{provider_name}:{endpoint}",
                    agent_role="network_observer",
                    event_type="http_get",
                    status=status,
                    latency_ms=latency_ms,
                    detail=detail,
                    error_category=error_category,
                    payload={"provider_name": provider_name, "endpoint": endpoint, **(payload or {})},
                )
        except Exception:
            # Network telemetry must never break ingestion.
            return


def _should_retry_http_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 429 or status_code >= 500
    return isinstance(exc, httpx.HTTPError)


def _network_error_category(exc: BaseException) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.HTTPStatusError):
        return f"http_{exc.response.status_code}"
    if isinstance(exc, httpx.ConnectError):
        return "connect_error"
    if isinstance(exc, RuntimeError) and "Circuit open" in str(exc):
        return "circuit_open"
    return "network_error"
