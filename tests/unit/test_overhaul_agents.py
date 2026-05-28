"""Tests for overhaul agent validators: PredictionValidator deterministic pass
and ParlayAdvisor correlation threshold logic."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from app.services.agents.contracts import AgentTask

# ---------------------------------------------------------------------------
# PredictionValidator: deterministic critical pass
# ---------------------------------------------------------------------------


class TestPredictionValidatorDeterministic:
    """The deterministic pre-pass should flag extreme probabilities without AI."""

    @staticmethod
    def _make_validator() -> Any:
        from app.services.agents.prediction_validator import PredictionValidatorAgent

        mock_session = MagicMock()
        # Make the DB query return an empty list by default
        mock_session.execute.return_value.all.return_value = []
        return PredictionValidatorAgent(mock_session)

    @staticmethod
    def _row(prob: float, name: str = "Player", key: str = "points") -> SimpleNamespace:
        return SimpleNamespace(
            prediction_id=1,
            over_probability=prob,
            full_name=name,
            key=key,
        )

    def test_flags_above_99_percent(self) -> None:
        validator = self._make_validator()
        rows = [self._row(0.995)]
        # Patch the DB query and AI batch call to isolate deterministic logic
        with patch.object(validator, "_validate_batch", return_value=([], 0)):
            validator._session.execute.return_value.all.return_value = rows
            task = AgentTask(role="prediction_validator", task_type="validate")
            result = validator.handle(task)
        critical_actions = [a for a in result.actions if "critical" in a.reason]
        assert len(critical_actions) == 1
        assert "0.9950" in critical_actions[0].reason

    def test_flags_below_1_percent(self) -> None:
        validator = self._make_validator()
        rows = [self._row(0.005)]
        with patch.object(validator, "_validate_batch", return_value=([], 0)):
            validator._session.execute.return_value.all.return_value = rows
            task = AgentTask(role="prediction_validator", task_type="validate")
            result = validator.handle(task)
        critical_actions = [a for a in result.actions if "critical" in a.reason]
        assert len(critical_actions) == 1

    def test_does_not_flag_moderate_extreme(self) -> None:
        validator = self._make_validator()
        # 0.96 is above 0.95 threshold but below 0.99 critical
        rows = [self._row(0.96)]
        with patch.object(validator, "_validate_batch", return_value=([], 0)):
            validator._session.execute.return_value.all.return_value = rows
            task = AgentTask(role="prediction_validator", task_type="validate")
            result = validator.handle(task)
        critical_actions = [a for a in result.actions if "critical" in a.reason]
        assert len(critical_actions) == 0

    def test_no_rows_returns_ok(self) -> None:
        validator = self._make_validator()
        task = AgentTask(role="prediction_validator", task_type="validate")
        result = validator.handle(task)
        assert result.status == "ok"
        assert result.details["extreme_count"] == 0


# ---------------------------------------------------------------------------
# ParlayAdvisor: correlation threshold and empty input
# ---------------------------------------------------------------------------


class TestParlayAdvisorThreshold:
    """ParlayAdvisor should flag parlays exceeding correlation threshold."""

    @staticmethod
    def _make_advisor() -> Any:
        from app.services.agents.parlay_advisor import ParlayAdvisorAgent

        mock_session = MagicMock()
        return ParlayAdvisorAgent(mock_session)

    def test_empty_parlays_returns_ok(self) -> None:
        advisor = self._make_advisor()
        task = AgentTask(
            role="parlay_advisor",
            task_type="review",
            input_payload={"parlays": []},
        )
        result = advisor.handle(task)
        assert result.status == "ok"
        assert result.details["parlay_count"] == 0

    def test_no_parlays_key_returns_ok(self) -> None:
        advisor = self._make_advisor()
        task = AgentTask(
            role="parlay_advisor",
            task_type="review",
            input_payload={},
        )
        result = advisor.handle(task)
        assert result.status == "ok"

    def test_high_correlation_flagged(self) -> None:
        advisor = self._make_advisor()
        # Mock AI call to return valid assessment
        mock_ai_result = SimpleNamespace(
            text='{"fragility_assessment": "high risk", "confidence": 0.9, '
            '"risky_legs": ["leg1"], "suggested_alternative": "swap leg1"}'
        )
        advisor._orchestrator.summarize = MagicMock(return_value=mock_ai_result)

        parlay = {
            "correlation_penalty": 0.80,  # above default 0.60 threshold
            "weakest_leg_hit_probability": 0.55,
            "joint_probability": 0.10,
            "legs": [
                {"player_name": "A", "market_key": "points", "hit_probability": 0.6, "recommended_side": "over"},
                {"player_name": "B", "market_key": "points", "hit_probability": 0.55, "recommended_side": "over"},
            ],
        }
        task = AgentTask(
            role="parlay_advisor",
            task_type="review",
            input_payload={"parlays": [parlay]},
        )
        result = advisor.handle(task)
        assert result.status == "recommendation"
        assert len(result.actions) == 1
        assert result.actions[0].action_type == "flag_high_correlation_parlay"

    def test_low_correlation_not_flagged(self) -> None:
        advisor = self._make_advisor()
        mock_ai_result = SimpleNamespace(
            text='{"fragility_assessment": "ok", "confidence": 0.3}'
        )
        advisor._orchestrator.summarize = MagicMock(return_value=mock_ai_result)

        parlay = {
            "correlation_penalty": 0.30,  # below 0.60 threshold
            "weakest_leg_hit_probability": 0.60,
            "joint_probability": 0.15,
            "legs": [
                {"player_name": "A", "market_key": "rebounds", "hit_probability": 0.65, "recommended_side": "over"},
            ],
        }
        task = AgentTask(
            role="parlay_advisor",
            task_type="review",
            input_payload={"parlays": [parlay]},
        )
        result = advisor.handle(task)
        assert result.status == "ok"
        assert len(result.actions) == 0
