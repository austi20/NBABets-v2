from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from urllib.parse import urlparse

import httpx
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import AIProviderEvent


@dataclass(frozen=True)
class AIResult:
    provider: str
    model: str
    text: str


class AIOrchestrator:
    """Local llama.cpp chat completions only. No cloud models."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()

    def _append_local_ai_training_log(
        self,
        *,
        task_name: str,
        prompt: str,
        response_text: str,
        status: str,
    ) -> None:
        flag = os.environ.get("LOCAL_AI_LOG_TRAINING", "").strip().lower()
        if flag not in {"1", "true", "yes", "on"}:
            return
        path = self._settings.logs_dir / "local_ai_training_pairs.jsonl"
        self._settings.logs_dir.mkdir(parents=True, exist_ok=True)
        max_prompt = 12_000
        max_resp = 12_000
        clipped_prompt = prompt if len(prompt) <= max_prompt else f"{prompt[:max_prompt]}…"
        clipped_resp = response_text if len(response_text) <= max_resp else f"{response_text[:max_resp]}…"
        record = {
            "ts": datetime.now(UTC).isoformat(),
            "task_name": task_name,
            "prompt": clipped_prompt,
            "response": clipped_resp,
            "status": status,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _verify_local_connection(self) -> bool:
        """GET /health on the llama.cpp server with a 5 s hard timeout."""
        parsed = urlparse(self._settings.ai_local_endpoint)
        health_url = f"{parsed.scheme}://{parsed.netloc}/health"
        try:
            with httpx.Client(timeout=5) as client:
                resp = client.get(health_url)
                return resp.status_code < 500
        except Exception:
            return False

    def summarize(self, *, task_name: str, prompt: str) -> AIResult:
        if self._verify_local_connection():
            local = self._try_local(task_name=task_name, prompt=prompt)
            if local is not None:
                return local
        else:
            self._record_event(
                provider_name="local",
                model_name=self._settings.ai_local_model,
                event_type=task_name,
                status="error",
                latency_ms=None,
                detail="Pre-flight health check failed; skipping local AI call.",
                payload={"endpoint": self._settings.ai_local_endpoint},
            )
        fallback_text = (
            "AI local provider unavailable. Returning deterministic fallback summary.\n\n"
            f"Task: {task_name}\n"
            "Action: ensure the local llama.cpp server is running and `AI_LOCAL_ENDPOINT`, "
            "`AI_LOCAL_MODEL`, and `AI_LOCAL_API_KEY` match the external runtime configuration, then rerun."
        )
        self._record_event(
            provider_name="fallback",
            model_name="deterministic",
            event_type=task_name,
            status="degraded",
            latency_ms=None,
            detail="Local llama.cpp request failed or returned empty content.",
            payload={},
        )
        return AIResult(provider="fallback", model="deterministic", text=fallback_text)

    def _try_local(self, *, task_name: str, prompt: str) -> AIResult | None:
        start = perf_counter()
        payload = {
            "model": self._settings.ai_local_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0.2,
        }
        try:
            headers = {
                "Authorization": f"Bearer {self._settings.ai_local_api_key}",
                "Content-Type": "application/json",
            }
            with httpx.Client(timeout=self._settings.ai_timeout_seconds) as client:
                response = client.post(self._settings.ai_local_endpoint, headers=headers, json=payload)
                response.raise_for_status()
                body = response.json()
            content = _extract_message_content(body)
            latency_ms = int((perf_counter() - start) * 1000)
            if not content:
                self._append_local_ai_training_log(
                    task_name=task_name,
                    prompt=prompt,
                    response_text="",
                    status="empty",
                )
                self._record_event(
                    provider_name="local",
                    model_name=self._settings.ai_local_model,
                    event_type=task_name,
                    status="error",
                    latency_ms=latency_ms,
                    detail="Local model returned empty response",
                    payload={"endpoint": self._settings.ai_local_endpoint},
                )
                return None
            self._append_local_ai_training_log(
                task_name=task_name,
                prompt=prompt,
                response_text=content,
                status="ok",
            )
            self._record_event(
                provider_name="local",
                model_name=self._settings.ai_local_model,
                event_type=task_name,
                status="ok",
                latency_ms=latency_ms,
                detail="Local llama.cpp request succeeded.",
                payload={"endpoint": self._settings.ai_local_endpoint},
            )
            return AIResult(provider="local", model=self._settings.ai_local_model, text=content)
        except Exception as exc:
            latency_ms = int((perf_counter() - start) * 1000)
            self._append_local_ai_training_log(
                task_name=task_name,
                prompt=prompt,
                response_text=str(exc),
                status="error",
            )
            self._record_event(
                provider_name="local",
                model_name=self._settings.ai_local_model,
                event_type=task_name,
                status="error",
                latency_ms=latency_ms,
                detail=str(exc),
                payload={"endpoint": self._settings.ai_local_endpoint},
            )
            return None

    def _record_event(
        self,
        *,
        provider_name: str,
        model_name: str | None,
        event_type: str,
        status: str,
        latency_ms: int | None,
        detail: str | None,
        payload: dict[str, Any],
    ) -> None:
        event = AIProviderEvent(
            provider_name=provider_name,
            model_name=model_name,
            event_type=event_type,
            status=status,
            latency_ms=latency_ms,
            detail=detail,
            payload=payload,
            created_at=datetime.now(UTC),
        )
        # Commit-per-call is intentional: audit trail must survive even if the caller crashes.
        self._session.add(event)
        self._session.commit()


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        return ""
    message = first_choice.get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                text_parts.append(text.strip())
        return "\n".join(text_parts).strip()
    return ""
