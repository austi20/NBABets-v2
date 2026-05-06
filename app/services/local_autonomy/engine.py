from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.evaluation.backtest import RollingOriginBacktester
from app.services.agents.run_service import AgentRunService
from app.services.ai_orchestrator import AIOrchestrator
from app.services.local_autonomy.contracts import (
    LocalAgentAction,
    LocalAgentDecision,
    action_from_payload,
    build_snapshot_hash,
    clamp_confidence,
    extract_json_object,
)
from app.services.local_autonomy.debug_copilot import build_debug_hints
from app.services.local_autonomy.overfit_intel import build_feature_attribution_review, build_overfit_intel_snapshot
from app.services.local_autonomy.policy_state import load_local_agent_policy_state
from app.tasks.ingestion import refresh_all


@dataclass(frozen=True)
class LocalAutonomyResult:
    decision: LocalAgentDecision
    executed_actions: tuple[str, ...]
    blocked_actions: tuple[str, ...]
    policy_state_enabled: bool
    policy_auto_execute_safe: bool
    model: str
    provider: str
    feature_attribution_review: dict[str, Any] = field(default_factory=dict)


class LocalAutonomyEngine:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._orchestrator = AIOrchestrator(session)
        self._run_service = AgentRunService(session)

    def run(
        self,
        *,
        report_date: date,
        mode: str,
        dry_run: bool,
        latest_model_metrics: dict[str, Any],
        latest_backtest_metrics: dict[str, Any],
        release_status: str,
        trend_alerts: list[str],
    ) -> LocalAutonomyResult:
        policy_state = load_local_agent_policy_state()
        overfit = build_overfit_intel_snapshot(
            latest_model_metrics=latest_model_metrics,
            latest_backtest_metrics=latest_backtest_metrics,
            trend_alerts=trend_alerts,
        )
        feature_review = build_feature_attribution_review(self._session, self._orchestrator, limit=50)
        debug_hints = build_debug_hints(self._session, report_date=report_date)
        snapshot_payload = {
            "report_date": report_date.isoformat(),
            "mode": mode,
            "dry_run": dry_run,
            "release_status": release_status,
            "overfit_risk_score": overfit.risk_score,
            "overfit_signals": [signal.__dict__ for signal in overfit.signals[:6]],
            "mitigations": list(overfit.mitigations[:6]),
            "debug_hints": [
                {
                    "category": hint.category,
                    "summary": hint.summary,
                    "next_steps": list(hint.next_steps),
                }
                for hint in debug_hints[:6]
            ],
            "feature_attribution_review": feature_review,
        }
        snapshot_hash = build_snapshot_hash(snapshot_payload)
        default_decision = LocalAgentDecision(
            run_id=f"local_autonomy_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}",
            status="advisory",
            confidence=0.0,
            summary="Local autonomy baseline generated without structured actions.",
            overfit_risk_score=overfit.risk_score,
            overfit_signals=overfit.signals,
            debug_hints=debug_hints,
            actions=tuple(),
            deterministic_blockers=tuple(),
            snapshot_hash=snapshot_hash,
        )
        if not self._settings.local_autonomy_enabled or not policy_state.enabled:
            disabled = LocalAgentDecision(
                **{**default_decision.__dict__, "status": "disabled", "summary": "Local autonomy disabled by policy."}
            )
            return LocalAutonomyResult(
                decision=disabled,
                executed_actions=tuple(),
                blocked_actions=tuple(),
                policy_state_enabled=policy_state.enabled,
                policy_auto_execute_safe=policy_state.auto_execute_safe,
                model=self._settings.ai_local_model,
                provider="local",
            )

        prompt = _build_autonomy_prompt(snapshot_payload)
        ai_result = self._orchestrator.summarize(task_name="local_autonomy", prompt=prompt)
        decision = self._decision_from_response(
            ai_text=ai_result.text,
            fallback=default_decision,
            overfit=overfit,
            debug_hints=debug_hints,
            snapshot_hash=snapshot_hash,
            release_status=release_status,
        )
        executed_actions, blocked_actions = self._apply_actions(
            decision=decision,
            mode=mode,
            dry_run=dry_run,
            policy_auto_execute_safe=policy_state.auto_execute_safe,
            release_status=release_status,
        )
        self._record_decision_event(decision, executed_actions=executed_actions, blocked_actions=blocked_actions)
        return LocalAutonomyResult(
            decision=decision,
            executed_actions=executed_actions,
            blocked_actions=blocked_actions,
            policy_state_enabled=policy_state.enabled,
            policy_auto_execute_safe=policy_state.auto_execute_safe,
            model=ai_result.model,
            provider=ai_result.provider,
            feature_attribution_review=feature_review,
        )

    def _decision_from_response(
        self,
        *,
        ai_text: str,
        fallback: LocalAgentDecision,
        overfit: Any,
        debug_hints: tuple[Any, ...],
        snapshot_hash: str,
        release_status: str,
    ) -> LocalAgentDecision:
        parsed = extract_json_object(ai_text, required_keys=frozenset({"status", "confidence"}))
        if parsed is None:
            return LocalAgentDecision(
                **{
                    **fallback.__dict__,
                    "status": "hold",
                    "summary": "Local model returned non-JSON output; using deterministic hold.",
                    "raw_response": ai_text[:4000],
                }
            )
        status_raw = str(parsed.get("status", "advisory")).lower()
        if status_raw not in {"advisory", "hold", "execute"}:
            status_raw = "advisory"
        status: Literal["advisory", "hold", "execute", "disabled", "error"]
        if status_raw == "hold":
            status = "hold"
        elif status_raw == "execute":
            status = "execute"
        else:
            status = "advisory"
        confidence = clamp_confidence(parsed.get("confidence"), default=0.0)
        summary = str(parsed.get("summary", "No summary provided.")).strip() or "No summary provided."
        actions: list[LocalAgentAction] = []
        for item in parsed.get("actions", []):
            if not isinstance(item, dict):
                continue
            action = action_from_payload(item)
            if action is None:
                continue
            actions.append(action)
        blockers = _deterministic_blockers(
            release_status=release_status,
            risk_score=overfit.risk_score,
            overfit_block_threshold=self._settings.local_autonomy_overfit_block_threshold,
        )
        if blockers and status == "execute":
            status = "hold"
            summary = f"Execution downgraded by deterministic blockers: {', '.join(blockers)}. {summary}"
        return LocalAgentDecision(
            run_id=fallback.run_id,
            status=status,
            confidence=confidence,
            summary=summary,
            overfit_risk_score=overfit.risk_score,
            overfit_signals=overfit.signals,
            debug_hints=debug_hints,
            actions=tuple(actions),
            deterministic_blockers=tuple(blockers),
            raw_response=ai_text[:4000],
            snapshot_hash=snapshot_hash,
        )

    def _apply_actions(
        self,
        *,
        decision: LocalAgentDecision,
        mode: str,
        dry_run: bool,
        policy_auto_execute_safe: bool,
        release_status: str,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        executed: list[str] = []
        blocked: list[str] = []
        execute_allowed = mode == "auto" and not dry_run and policy_auto_execute_safe and decision.status == "execute"
        for action in decision.actions:
            if action.action_class != "safe_auto":
                blocked.append(f"{action.action_type}:requires_{action.action_class}")
                continue
            if not execute_allowed:
                blocked.append(f"{action.action_type}:auto_disabled")
                continue
            if release_status == "BLOCKED" and action.action_type != "run_refresh_all":
                blocked.append(f"{action.action_type}:release_blocked")
                continue
            try:
                if action.action_type == "run_refresh_all":
                    asyncio.run(refresh_all(target_date=date.today()))
                    executed.append(action.action_type)
                elif action.action_type == "run_backtest":
                    RollingOriginBacktester(self._session).run()
                    self._session.commit()
                    executed.append(action.action_type)
                else:
                    blocked.append(f"{action.action_type}:unsupported_safe_action")
            except Exception as exc:  # pragma: no cover - runtime guard
                blocked.append(f"{action.action_type}:error:{exc}")
        return tuple(executed), tuple(blocked)

    def _record_decision_event(
        self,
        decision: LocalAgentDecision,
        *,
        executed_actions: tuple[str, ...],
        blocked_actions: tuple[str, ...],
    ) -> None:
        self._run_service.record(
            task_id=decision.run_id,
            agent_role="local_autonomy",
            event_type="local_autonomy_decision",
            status=decision.status,
            confidence=decision.confidence,
            action_summary=", ".join(action.action_type for action in decision.actions) or "none",
            error_category="quality_gate" if decision.status == "error" else None,
            detail=decision.summary,
            payload={
                "snapshot_hash": decision.snapshot_hash,
                "overfit_risk_score": decision.overfit_risk_score,
                "deterministic_blockers": list(decision.deterministic_blockers),
                "executed_actions": list(executed_actions),
                "blocked_actions": list(blocked_actions),
            },
        )


def render_local_autonomy_markdown(result: LocalAutonomyResult) -> str:
    decision = result.decision
    lines: list[str] = [
        f"- status: {decision.status}",
        f"- confidence: {decision.confidence:.2f}",
        f"- policy_enabled: {result.policy_state_enabled}",
        f"- policy_auto_execute_safe: {result.policy_auto_execute_safe}",
        f"- snapshot_hash: {decision.snapshot_hash[:12]}...",
        f"- summary: {decision.summary}",
        "",
        "### Overfit Signals",
    ]
    if not decision.overfit_signals:
        lines.append("- none")
    else:
        for signal in decision.overfit_signals[:8]:
            lines.append(f"- {signal.market}: score={signal.score:.2f} ({signal.note})")
    lines.extend(["", "### Debug Copilot"])
    for hint in decision.debug_hints[:5]:
        lines.append(f"- {hint.category}: {hint.summary}")
        for step in hint.next_steps[:2]:
            lines.append(f"  - {step}")
    lines.extend(["", "### Actions"])
    if not decision.actions:
        lines.append("- none")
    else:
        for action in decision.actions:
            lines.append(
                f"- `{action.action_type}` ({action.action_class}) conf={action.confidence:.2f} reason={action.reason}"
            )
    if result.executed_actions:
        lines.append("")
        lines.append("### Executed")
        for executed_action in result.executed_actions:
            lines.append(f"- {executed_action}")
    if result.blocked_actions:
        lines.append("")
        lines.append("### Blocked")
        for blocked_action in result.blocked_actions:
            lines.append(f"- {blocked_action}")
    return "\n".join(lines)


def _build_autonomy_prompt(snapshot_payload: dict[str, Any]) -> str:
    return (
        "You are the local autonomy copilot for NBA prop modeling. "
        "Return STRICT JSON only with keys: status, confidence, summary, actions. "
        "status must be advisory|hold|execute. confidence is 0..1.\n"
        "Each action object keys: action_type, reason, confidence, payload.\n"
        "Prefer safe actions first. Do not recommend destructive or schema-changing actions.\n"
        f"Snapshot:\n{json.dumps(snapshot_payload, indent=2, sort_keys=True)}"
    )


def _deterministic_blockers(
    *, release_status: str, risk_score: float, overfit_block_threshold: float
) -> list[str]:
    blockers: list[str] = []
    if release_status in {"BLOCKED", "HOLD"}:
        blockers.append(f"release_status_{release_status.lower()}")
    if risk_score >= overfit_block_threshold:
        blockers.append("overfit_risk_extreme")
    return blockers
