from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask
from app.services.ai_orchestrator import AIOrchestrator
from app.services.local_autonomy.contracts import extract_json_object


class ParlayAdvisorAgent:
    role = "parlay_advisor"

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._orchestrator = AIOrchestrator(session)

    def handle(self, task: AgentTask) -> AgentResult:
        parlays_raw: list[dict[str, Any]] = task.input_payload.get("parlays", [])
        if not parlays_raw:
            return AgentResult(
                task_id=task.task_id,
                role=self.role,
                status="ok",
                summary="No parlays provided for advisor review.",
                actions=[],
                confidence=1.0,
                details={"parlay_count": 0},
            )

        actions: list[AgentAction] = []
        assessments: list[dict[str, Any]] = []
        for parlay in parlays_raw:
            assessment, action = self._assess_parlay(parlay, task)
            assessments.append(assessment)
            if action is not None:
                actions.append(action)

        flagged_count = len(actions)
        status = "recommendation" if actions else "ok"
        summary = (
            f"Reviewed {len(parlays_raw)} parlay(s); flagged {flagged_count} as high-correlation."
            if actions
            else f"Reviewed {len(parlays_raw)} parlay(s); all within correlation thresholds."
        )
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status=status,
            summary=summary,
            actions=actions,
            confidence=0.78 if actions else 0.91,
            details={
                "parlay_count": len(parlays_raw),
                "flagged_count": flagged_count,
                "assessments": assessments,
                "dry_run": task.dry_run,
            },
        )

    def _assess_parlay(
        self, parlay: dict[str, Any], task: AgentTask
    ) -> tuple[dict[str, Any], AgentAction | None]:
        correlation_penalty = float(parlay.get("correlation_penalty", 0.0))
        weakest = float(parlay.get("weakest_leg_hit_probability", 0.0))
        joint = float(parlay.get("joint_probability", 0.0))
        legs: list[dict[str, Any]] = parlay.get("legs", [])
        leg_count = len(legs)
        legs_json = json.dumps(
            [
                {
                    "player_name": leg.get("player_name", ""),
                    "market_key": leg.get("market_key", ""),
                    "hit_probability": leg.get("hit_probability", 0.0),
                    "recommended_side": leg.get("recommended_side", ""),
                }
                for leg in legs
            ],
            indent=2,
        )
        prompt = (
            f"Review this {leg_count}-leg NBA parlay. "
            f"Correlation penalty: {correlation_penalty:.2%}. "
            f"Weakest leg: {weakest:.2%}. "
            f"Joint probability: {joint:.2%}. "
            f"Legs:\n{legs_json}\n"
            "Return STRICT JSON only: "
            '{"fragility_assessment": "", "risky_legs": [], '
            '"suggested_alternative": "", "confidence": 0.0}'
        )
        ai_result = self._orchestrator.summarize(task_name="parlay_advisor", prompt=prompt)
        parsed = extract_json_object(
            ai_result.text,
            required_keys=frozenset({"fragility_assessment", "confidence"}),
        )

        threshold = self._settings.parlay_advisor_correlation_threshold
        is_high_correlation = correlation_penalty > threshold

        assessment: dict[str, Any] = {
            "correlation_penalty": correlation_penalty,
            "joint_probability": joint,
            "weakest_leg": weakest,
            "leg_count": leg_count,
            "is_high_correlation": is_high_correlation,
            "fragility_assessment": parsed.get("fragility_assessment", "") if parsed else "",
            "risky_legs": parsed.get("risky_legs", []) if parsed else [],
            "suggested_alternative": parsed.get("suggested_alternative", "") if parsed else "",
            "ai_confidence": parsed.get("confidence", 0.0) if parsed else 0.0,
        }

        action: AgentAction | None = None
        if is_high_correlation:
            fragility = assessment["fragility_assessment"]
            reason = (
                f"Correlation penalty {correlation_penalty:.2%} exceeds threshold "
                f"{threshold:.2%}."
                + (f" {fragility}" if fragility else "")
            )
            action = AgentAction(
                action_type="flag_high_correlation_parlay",
                reason=reason,
                payload={
                    "correlation_penalty": correlation_penalty,
                    "risky_legs": assessment["risky_legs"],
                    "suggested_alternative": assessment["suggested_alternative"],
                },
                safe_to_auto_execute=False,
            )
        return assessment, action
