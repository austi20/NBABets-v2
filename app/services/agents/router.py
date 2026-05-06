from __future__ import annotations

from time import perf_counter

from app.services.agents.contracts import AgentHandler, AgentResult, AgentTask
from app.services.agents.run_service import AgentRunService


class AgentRouter:
    def __init__(self, handlers: list[AgentHandler], run_service: AgentRunService) -> None:
        self._handlers = {handler.role: handler for handler in handlers}
        self._run_service = run_service

    def dispatch(self, task: AgentTask) -> AgentResult:
        handler = self._handlers.get(task.role)
        if handler is None:
            missing = AgentResult(
                task_id=task.task_id,
                role=task.role,
                status="skipped",
                summary=f"No handler registered for role={task.role}",
                error_category="missing_handler",
            )
            self._run_service.record(
                task_id=task.task_id,
                agent_role=task.role,
                event_type=task.task_type,
                status=missing.status,
                detail=missing.summary,
                error_category=missing.error_category,
                payload=task.input_payload,
            )
            return missing

        start = perf_counter()
        self._run_service.record(
            task_id=task.task_id,
            agent_role=task.role,
            event_type=task.task_type,
            status="started",
            payload=task.input_payload,
        )
        try:
            result = handler.handle(task)
            latency_ms = int((perf_counter() - start) * 1000)
            action_summary = ", ".join(action.action_type for action in result.actions) or "none"
            self._run_service.record(
                task_id=task.task_id,
                agent_role=task.role,
                event_type=task.task_type,
                status=result.status,
                latency_ms=latency_ms,
                confidence=result.confidence,
                action_summary=action_summary,
                error_category=result.error_category,
                detail=result.summary,
                payload=result.details,
            )
            return result
        except Exception as exc:
            latency_ms = int((perf_counter() - start) * 1000)
            summary = f"Agent execution failed for role={task.role}: {exc}"
            self._run_service.record(
                task_id=task.task_id,
                agent_role=task.role,
                event_type=task.task_type,
                status="error",
                latency_ms=latency_ms,
                error_category="execution_error",
                detail=summary,
                payload=task.input_payload,
            )
            return AgentResult(
                task_id=task.task_id,
                role=task.role,
                status="error",
                summary=summary,
                error_category="execution_error",
            )
