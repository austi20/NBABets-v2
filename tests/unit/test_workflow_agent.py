from __future__ import annotations

from app.services.agents.contracts import AgentTask
from app.services.agents.workflow import WorkflowAgent


class _Session:
    def __init__(self, values: list[int]) -> None:
        self._values = values

    def scalar(self, _statement: object) -> int:
        return self._values.pop(0)


def test_workflow_agent_recommends_actions_for_missing_work() -> None:
    session = _Session([0, 0, 10, 0])
    result = WorkflowAgent(session).handle(AgentTask(role="workflow", task_type="workflow_assessment"))

    assert result.status == "recommendation"
    action_types = [item.action_type for item in result.actions]
    assert "trigger_retrain" in action_types
    assert "run_refresh_all" in action_types
    assert "run_backtest" in action_types
