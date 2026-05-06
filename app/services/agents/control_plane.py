from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.evaluation.backtest import RollingOriginBacktester
from app.services.agents.accuracy_examiner import AccuracyExaminerAgent
from app.services.agents.api_monitor import ApiMonitorAgent
from app.services.agents.contracts import AgentResult, AgentTask
from app.services.agents.data_quality import DataQualityAgent
from app.services.agents.network_reliability import NetworkReliabilityAgent
from app.services.agents.parlay_advisor import ParlayAdvisorAgent
from app.services.agents.prediction_validator import PredictionValidatorAgent
from app.services.agents.router import AgentRouter
from app.services.agents.run_service import AgentRunService
from app.services.agents.workflow import WorkflowAgent
from app.tasks.ingestion import refresh_all


class AgentControlPlane:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._router = AgentRouter(
            handlers=[
                WorkflowAgent(session),
                ApiMonitorAgent(session),
                DataQualityAgent(session),
                NetworkReliabilityAgent(session),
                PredictionValidatorAgent(session),
                ParlayAdvisorAgent(session),
                AccuracyExaminerAgent(session),
            ],
            run_service=AgentRunService(session),
        )

    def run(
        self,
        *,
        mode: str = "recommend",
        dry_run: bool = True,
        include_roles: tuple[str, ...] = (
            "workflow",
            "api_monitor",
            "data_quality",
            "network",
            "prediction_validator",
            "accuracy_examiner",
        ),
        parlays: list[dict] | None = None,
        report_date: date | None = None,
        examiner_context: dict[str, Any] | None = None,
    ) -> dict[str, AgentResult]:
        dry_run_effective = dry_run if mode == "auto" else True
        rd = report_date or date.today()
        results: dict[str, AgentResult] = {}
        role_flags = {
            "workflow": self._settings.workflow_agent_enabled,
            "api_monitor": self._settings.api_monitor_agent_enabled,
            "data_quality": self._settings.data_quality_agent_enabled,
            "network": self._settings.network_reliability_agent_enabled,
            "prediction_validator": self._settings.prediction_validator_enabled,
            "parlay_advisor": self._settings.parlay_advisor_enabled,
            "accuracy_examiner": self._settings.examiner_enabled,
        }
        for role in include_roles:
            if not role_flags.get(role, False):
                continue
            input_payload: dict = {"mode": mode}
            if role == "data_quality":
                input_payload["weekly_maintenance"] = date.today().weekday() == 6
            if role == "prediction_validator":
                input_payload["limit"] = 50
            if role == "parlay_advisor":
                input_payload["parlays"] = parlays or []
            if role == "accuracy_examiner":
                xctx = examiner_context or {}
                input_payload["report_date"] = rd.isoformat()
                input_payload["markets"] = xctx.get("markets") or (
                    "points",
                    "rebounds",
                    "assists",
                    "threes",
                    "turnovers",
                    "pra",
                )
                input_payload["latest_model_metrics"] = xctx.get("latest_model_metrics") or {}
                input_payload["latest_backtest_metrics"] = xctx.get("latest_backtest_metrics") or {}
                input_payload["trend_alerts"] = xctx.get("trend_alerts") or ()
            task = AgentTask(
                role=role,
                task_type=f"{role}_assessment",
                input_payload=input_payload,
                dry_run=dry_run_effective,
            )
            results[role] = self._router.dispatch(task)

        if mode == "auto":
            self._execute_workflow_actions(results.get("workflow"), dry_run=dry_run_effective)
        return results

    def _execute_workflow_actions(self, result: AgentResult | None, *, dry_run: bool) -> None:
        if result is None or dry_run or not self._settings.workflow_agent_allow_auto_actions:
            return
        for action in result.actions:
            if action.action_type == "run_refresh_all" and action.safe_to_auto_execute:
                asyncio.run(refresh_all(target_date=date.today()))
            elif action.action_type == "run_backtest" and action.safe_to_auto_execute:
                RollingOriginBacktester(self._session).run()
                self._session.commit()


def render_agent_markdown(results: dict[str, AgentResult]) -> str:
    if not results:
        return "Agent mode disabled."
    lines: list[str] = []
    for role, result in results.items():
        lines.append(f"### {role}")
        lines.append(f"- status: {result.status}")
        lines.append(f"- summary: {result.summary}")
        if result.actions:
            for action in result.actions:
                lines.append(
                    f"- action: `{action.action_type}` | safe_auto={action.safe_to_auto_execute} | reason: {action.reason}"
                )
        else:
            lines.append("- action: none")
        lines.append("")
    return "\n".join(lines).strip()
