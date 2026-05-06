from __future__ import annotations

import json
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import Player, Prediction, PropMarket
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask
from app.services.ai_orchestrator import AIOrchestrator
from app.services.local_autonomy.contracts import extract_json_object


class PredictionValidatorAgent:
    role = "prediction_validator"

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._orchestrator = AIOrchestrator(session)

    def handle(self, task: AgentTask) -> AgentResult:
        threshold = self._settings.prediction_validator_extreme_prob_threshold
        batch_size = self._settings.prediction_validator_batch_size
        limit = int(task.input_payload.get("limit", 50))

        rows = self._session.execute(
            select(
                Prediction.prediction_id,
                Prediction.over_probability,
                Player.full_name,
                PropMarket.key,
            )
            .join(Player, Prediction.player_id == Player.player_id)
            .join(PropMarket, Prediction.market_id == PropMarket.market_id)
            .where(
                or_(
                    Prediction.over_probability > threshold,
                    Prediction.over_probability < (1.0 - threshold),
                )
            )
            .order_by(Prediction.predicted_at.desc())
            .limit(limit)
        ).all()

        if not rows:
            return AgentResult(
                task_id=task.task_id,
                role=self.role,
                status="ok",
                summary="No extreme-probability predictions found.",
                actions=[],
                confidence=0.95,
                details={"extreme_count": 0, "threshold": threshold},
            )

        actions: list[AgentAction] = []
        flagged_count = 0

        # Deterministic pre-pass: flag extreme probabilities as critical without AI.
        # These are always wrong for NBA player props.
        critical_hi = min(1.0 - (1.0 - threshold) * 0.2, 0.99)  # e.g. 0.95 → 0.99
        critical_lo = max((1.0 - threshold) * 0.2, 0.01)         # e.g. 0.05 → 0.01
        for row in rows:
            prob = float(row.over_probability)
            if prob > critical_hi or prob < critical_lo:
                actions.append(
                    AgentAction(
                        action_type="flag_unrealistic_prediction",
                        reason=(
                            f"[critical] {row.full_name} {row.key}: probability {prob:.4f} "
                            "is beyond 99/1% -- likely DNP contamination or data error"
                        ),
                        payload={
                            "prediction_id": row.prediction_id,
                            "severity": "critical",
                            "over_probability": round(prob, 6),
                        },
                        safe_to_auto_execute=False,
                    )
                )
                flagged_count += 1

        for batch_start in range(0, len(rows), batch_size):
            batch = rows[batch_start : batch_start + batch_size]
            batch_actions, batch_flagged = self._validate_batch(batch, task)
            actions.extend(batch_actions)
            flagged_count += batch_flagged

        status = "recommendation" if actions else "ok"
        summary = (
            f"Validated {len(rows)} extreme predictions; flagged {flagged_count} as unrealistic."
            if actions
            else f"Validated {len(rows)} extreme predictions; all appear realistic."
        )
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status=status,
            summary=summary,
            actions=actions,
            confidence=0.75 if actions else 0.88,
            details={
                "extreme_count": len(rows),
                "flagged_count": flagged_count,
                "threshold": threshold,
                "dry_run": task.dry_run,
            },
        )

    def _validate_batch(
        self, batch: list[Any], task: AgentTask
    ) -> tuple[list[AgentAction], int]:
        predictions_json = [
            {
                "prediction_id": row.prediction_id,
                "player_name": row.full_name,
                "market_key": row.key,
                "over_probability": round(row.over_probability, 4),
            }
            for row in batch
        ]
        prompt = (
            "You are a sports statistics validator. Review these NBA prop predictions for "
            "unrealistic probabilities. Historical range for most NBA props is 25-75%. "
            "CRITICAL: Probabilities above 95% or below 5% almost always indicate data "
            "contamination such as DNP games (0 minutes played) polluting rolling averages, "
            "causing projected_mean to collapse far below the sportsbook line. When you see "
            "these extreme values, flag them as unrealistic with severity='high' or 'critical' "
            "and mention possible DNP contamination in the reason. "
            "Return STRICT JSON only: "
            '{"validations": [{"prediction_id": 0, "is_realistic": true, "reason": "", '
            '"severity": "low"}]}\n'
            f"Predictions:\n{json.dumps(predictions_json, indent=2)}"
        )
        ai_result = self._orchestrator.summarize(
            task_name="prediction_validation", prompt=prompt
        )
        parsed = extract_json_object(
            ai_result.text, required_keys=frozenset({"validations"})
        )
        actions: list[AgentAction] = []
        flagged = 0
        if parsed is None:
            return actions, flagged
        for item in parsed.get("validations", []):
            if not isinstance(item, dict):
                continue
            if item.get("is_realistic", True):
                continue
            severity = str(item.get("severity", "low"))
            prediction_id = item.get("prediction_id")
            reason = str(item.get("reason", ""))
            actions.append(
                AgentAction(
                    action_type="flag_unrealistic_prediction",
                    reason=f"[{severity}] {reason}",
                    payload={"prediction_id": prediction_id, "severity": severity},
                    safe_to_auto_execute=False,
                )
            )
            flagged += 1
        return actions, flagged
