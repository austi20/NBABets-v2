from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import AIProviderEvent, BacktestResult, ModelRun, Prediction
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask


class WorkflowAgent:
    role = "workflow"

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()

    def handle(self, task: AgentTask) -> AgentResult:
        today = date.today()
        day_start = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
        prediction_count = self._session.scalar(
            select(func.count(Prediction.prediction_id)).where(Prediction.predicted_at >= day_start)
        ) or 0
        model_run_count = self._session.scalar(
            select(func.count(ModelRun.model_run_id)).where(ModelRun.completed_at >= day_start)
        ) or 0
        provider_error_count = self._session.scalar(
            select(func.count(AIProviderEvent.event_id)).where(
                AIProviderEvent.status == "error",
                AIProviderEvent.created_at >= day_start,
            )
        ) or 0
        backtest_today = self._session.scalar(
            select(func.count(BacktestResult.backtest_result_id)).where(BacktestResult.computed_at >= day_start)
        ) or 0

        actions: list[AgentAction] = []
        if model_run_count == 0:
            actions.append(
                AgentAction(
                    action_type="trigger_retrain",
                    reason="No model runs completed today.",
                    payload={"target_date": today.isoformat()},
                    safe_to_auto_execute=False,
                )
            )
        if prediction_count == 0:
            actions.append(
                AgentAction(
                    action_type="run_refresh_all",
                    reason="No predictions generated today.",
                    payload={"target_date": today.isoformat()},
                    safe_to_auto_execute=True,
                )
            )
        if provider_error_count >= self._settings.workflow_agent_error_threshold:
            actions.append(
                AgentAction(
                    action_type="run_refresh_all",
                    reason="Provider errors exceeded threshold.",
                    payload={"target_date": today.isoformat(), "error_count": int(provider_error_count)},
                    safe_to_auto_execute=True,
                )
            )
        if backtest_today == 0:
            actions.append(
                AgentAction(
                    action_type="run_backtest",
                    reason="No same-day backtest found.",
                    payload={"target_date": today.isoformat()},
                    safe_to_auto_execute=True,
                )
            )

        summary = "Workflow health stable."
        status = "ok"
        if actions:
            status = "recommendation"
            summary = f"Generated {len(actions)} workflow action recommendation(s)."
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status=status,
            summary=summary,
            actions=actions,
            confidence=0.75 if actions else 0.9,
            details={
                "predictions_today": int(prediction_count),
                "model_runs_today": int(model_run_count),
                "provider_errors_today": int(provider_error_count),
                "backtests_today": int(backtest_today),
                "dry_run": task.dry_run,
            },
        )
