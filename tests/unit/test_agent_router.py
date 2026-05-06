from __future__ import annotations

from dataclasses import dataclass, field

from app.services.agents.contracts import AgentAction, AgentResult, AgentTask
from app.services.agents.router import AgentRouter


@dataclass
class _Recorder:
    calls: list[dict[str, object]] = field(default_factory=list)

    def record(self, **kwargs: object) -> None:
        self.calls.append(kwargs)


class _Handler:
    role = "workflow"

    def handle(self, task: AgentTask) -> AgentResult:
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status="recommendation",
            summary="ok",
            actions=[AgentAction(action_type="run_refresh_all", reason="test")],
        )


def test_agent_router_dispatch_records_start_and_result() -> None:
    recorder = _Recorder()
    router = AgentRouter([_Handler()], recorder)  # type: ignore[arg-type]
    result = router.dispatch(AgentTask(role="workflow", task_type="workflow_assessment"))

    assert result.status == "recommendation"
    statuses = [str(call.get("status")) for call in recorder.calls]
    assert "started" in statuses
    assert "recommendation" in statuses


def test_agent_router_returns_missing_handler_result() -> None:
    recorder = _Recorder()
    router = AgentRouter([], recorder)  # type: ignore[arg-type]
    result = router.dispatch(AgentTask(role="missing", task_type="any"))

    assert result.status == "skipped"
    assert result.error_category == "missing_handler"
