from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from pathlib import Path

from app.services.daily_workflow import DailyWorkflowService, WorkflowGateSummary
from app.services.parlays import ParlayLeg, ParlayRecommendation
from app.services.startup import StartupRunner, StartupRunResult


class _SessionStub:
    def expire_all(self) -> None:
        return None


def test_startup_runner_returns_structured_result(monkeypatch) -> None:
    board_date = date(2026, 4, 9)
    runner = StartupRunner(preferred_board_date=board_date)

    monkeypatch.setattr(runner, "_step_discover_db", lambda **_: None)
    monkeypatch.setattr(runner, "_step_initialize_db", lambda **_: None)
    monkeypatch.setattr(runner, "_step_start_local_ai", lambda **_: {"early_complete": True})
    monkeypatch.setattr(runner, "_step_refresh_data", lambda **_: {"board_date": board_date})
    monkeypatch.setattr(runner, "_step_train_model", lambda **_: None)
    monkeypatch.setattr(runner, "_step_predict", lambda **_: {"board_date": board_date})
    monkeypatch.setattr(runner, "_step_backtest", lambda **_: None)
    monkeypatch.setattr(runner, "_step_automation_report", lambda **_: None)
    monkeypatch.setattr(runner, "_step_analyze_props", lambda **_: None)
    monkeypatch.setattr(runner, "_record_quote_coverage_metric", lambda: None)

    result = runner.run()

    assert result.status == "completed"
    assert result.failed is False
    assert [step.key for step in result.steps] == [
        "discover_db",
        "initialize_db",
        "start_local_ai",
        "refresh_data",
        "train_model",
        "predict",
        "backtest",
        "automation_report",
        "analyze_props",
    ]


def test_daily_workflow_retries_once_then_succeeds(monkeypatch) -> None:
    service = DailyWorkflowService(session=_SessionStub())  # type: ignore[arg-type]
    board_date = date(2026, 4, 9)
    parlay = _sample_parlay(game_count=4)
    report_root = _temp_dir("workflow")
    report_path = report_root / "report.md"
    report_root.mkdir(parents=True, exist_ok=True)
    report_path.write_text("# report", encoding="utf-8")
    reset_modes: list[str] = []
    gate_calls = {"count": 0}

    monkeypatch.setattr(service, "_resolve_board_date", lambda: board_date)
    monkeypatch.setattr(
        service,
        "_reset_board",
        lambda *, board_date, reset_mode: reset_modes.append(reset_mode),
    )
    monkeypatch.setattr(
        StartupRunner,
        "run",
        lambda self: StartupRunResult(
            status="completed",
            failed=False,
            error_message=None,
            board_date=board_date,
            metrics={},
            report_path=None,
            opportunity_count=0,
            steps=[],
            database_message="",
            board_date_message=f"Board date: {board_date.isoformat()}",
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            log_lines=[],
        ),
    )
    monkeypatch.setattr(service, "_generate_fresh_report", lambda *, board_date, agent_mode: report_path)

    def fake_gate_summary(*, board_date, startup_result, report_path):
        del board_date, startup_result, report_path
        gate_calls["count"] += 1
        if gate_calls["count"] == 1:
            return _gate_summary(recoverable=("Scheduled board exists but predictions are missing",))
        return _gate_summary()

    monkeypatch.setattr(service, "_evaluate_gates", fake_gate_summary)
    monkeypatch.setattr(service, "_extract_strict_parlays", lambda *, board_date, top_parlays: (parlay,))

    result = service.run(max_attempts=2)

    assert reset_modes == ["soft_reset", "hard_reset"]
    assert result.attempt_count == 2
    assert result.final_status == "success"
    assert result.parlays == (parlay,)


def test_daily_workflow_extracts_only_strict_four_game_parlays(monkeypatch) -> None:
    service = DailyWorkflowService(session=_SessionStub())  # type: ignore[arg-type]
    good = _sample_parlay(game_count=4)
    bad = _sample_parlay(game_count=3)

    class _StubParlayService:
        def suggest_by_sportsbook_and_leg_count(self, *args, **kwargs):
            del args, kwargs
            return {"book": {4: [bad, good]}}

    monkeypatch.setattr("app.services.daily_workflow.MultiGameParlayService", lambda session: _StubParlayService())
    parlays = service._extract_strict_parlays(board_date=date(2026, 4, 9), top_parlays=5)

    assert parlays == (good,)


def _gate_summary(
    *,
    recoverable: tuple[str, ...] = (),
    terminal: tuple[str, ...] = (),
) -> WorkflowGateSummary:
    return WorkflowGateSummary(
        board_date="2026-04-09",
        scheduled_games=4,
        live_games=0,
        final_games=0,
        predictions_for_board=12,
        expected_prediction_rows=12,
        line_snapshots_for_board=12,
        scheduled_games_with_verified_lines=4,
        raw_payload_counts={"stats": 1, "odds": 1, "injuries": 1},
        sentinel_status="CLEAN",
        extreme_predictions_today=0,
        projection_line_divergences=0,
        release_status="GO",
        quality_guardrail_status="GO",
        report_flags=(),
        recoverable_reasons=recoverable,
        terminal_reasons=terminal,
    )


def _sample_parlay(*, game_count: int) -> ParlayRecommendation:
    legs = [
        ParlayLeg(
            game_id=index + 1 if index < game_count else game_count,
            matchup=f"G{index + 1}",
            player_name=f"Player {index + 1}",
            market_key="points",
            recommended_side="OVER",
            line_value=20.5 + index,
            american_odds=-110,
            hit_probability=0.6,
            likelihood_score=60,
            is_live_quote=True,
            verification_status="provider_live",
            odds_source_provider="balldontlie",
        )
        for index in range(4)
    ]
    game_ids = tuple(dict.fromkeys(leg.game_id for leg in legs))
    game_labels = tuple(dict.fromkeys(leg.matchup for leg in legs))
    return ParlayRecommendation(
        rank=1,
        game_id=game_ids[0],
        matchup=" / ".join(game_labels),
        sportsbook_key="book",
        sportsbook_name="Book",
        sportsbook_icon="B",
        leg_count=4,
        game_count=len(game_ids),
        game_ids=game_ids,
        game_labels=game_labels,
        joint_probability=0.15,
        combined_decimal_odds=10.0,
        combined_american_odds=900,
        expected_profit_per_unit=0.10 if len(game_ids) == 4 else 0.05,
        implied_probability=0.10,
        edge=0.05 if len(game_ids) == 4 else 0.01,
        all_legs_live=True,
        verification_status="provider_live",
        odds_source_provider="balldontlie",
        correlation_penalty=1.0,
        average_leg_hit_probability=0.6,
        weakest_leg_hit_probability=0.55,
        legs=legs,
    )


def _temp_dir(prefix: str) -> Path:
    return Path("temp") / f"pytest_{prefix}_{uuid.uuid4().hex}"
