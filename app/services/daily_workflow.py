from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import Game, Prediction, RawPayload
from app.services.automation import generate_daily_automation_report
from app.services.board_date import matches_board_date
from app.services.parlays import MultiGameParlayService, ParlayRecommendation
from app.services.query import QueryService
from app.services.startup import StartupRunner, StartupRunResult
from app.services.startup_cache import (
    StartupCacheResetService,
    StartupComputationCacheService,
    StartupRefreshCacheService,
    StartupResetResult,
)

REPORT_FLAG_PATTERNS = (
    (r"dnp_contamination_warning", "DataQualityAgent flagged DNP contamination"),
    (r"extreme_probability_detected", "DataQualityAgent flagged extreme probabilities"),
    (r"projection_line_divergence", "DataQualityAgent flagged projection-line divergence"),
    (r"flag_unrealistic_prediction.*?critical", "PredictionValidator flagged unrealistic predictions"),
)


@dataclass(frozen=True)
class WorkflowGateSummary:
    board_date: str
    scheduled_games: int
    live_games: int
    final_games: int
    predictions_for_board: int
    expected_prediction_rows: int
    line_snapshots_for_board: int
    scheduled_games_with_verified_lines: int
    raw_payload_counts: dict[str, int]
    sentinel_status: str
    extreme_predictions_today: int
    projection_line_divergences: int
    release_status: str
    quality_guardrail_status: str
    report_flags: tuple[str, ...]
    recoverable_reasons: tuple[str, ...]
    terminal_reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.recoverable_reasons and not self.terminal_reasons


@dataclass(frozen=True)
class WorkflowResult:
    attempt_count: int
    final_status: str
    board_date: str
    report_path: str | None
    retry_reason: str | None
    parlays: tuple[ParlayRecommendation, ...]
    gate_summary: WorkflowGateSummary


class DailyWorkflowService:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()

    def run(
        self,
        *,
        agent_mode: str = "recommend",
        top_parlays: int = 5,
        max_attempts: int = 2,
    ) -> WorkflowResult:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        board_date = self._resolve_board_date()
        retry_reason: str | None = None
        last_gate_summary: WorkflowGateSummary | None = None
        last_report_path: Path | None = None
        last_parlays: tuple[ParlayRecommendation, ...] = ()
        attempt_count = 0

        for attempt_index in range(max_attempts):
            attempt_count = attempt_index + 1
            reset_mode = "soft_reset" if attempt_index == 0 else "hard_reset"
            self._reset_board(board_date=board_date, reset_mode=reset_mode)
            startup_result = StartupRunner(preferred_board_date=board_date).run()
            self._session.expire_all()
            fresh_report_path = self._generate_fresh_report(board_date=board_date, agent_mode=agent_mode)
            self._session.expire_all()

            last_report_path = fresh_report_path
            last_gate_summary = self._evaluate_gates(
                board_date=board_date,
                startup_result=startup_result,
                report_path=fresh_report_path,
            )
            if last_gate_summary.passed:
                last_parlays = self._extract_strict_parlays(board_date=board_date, top_parlays=top_parlays)
                if last_parlays:
                    return WorkflowResult(
                        attempt_count=attempt_count,
                        final_status="success",
                        board_date=board_date.isoformat(),
                        report_path=str(fresh_report_path),
                        retry_reason=retry_reason,
                        parlays=last_parlays,
                        gate_summary=last_gate_summary,
                    )
                last_gate_summary = WorkflowGateSummary(
                    **{**last_gate_summary.__dict__, "terminal_reasons": ("No strict 4-leg, 4-game parlays exist on the clean board",)}
                )
                break

            if last_gate_summary.recoverable_reasons and attempt_index + 1 < max_attempts:
                retry_reason = last_gate_summary.recoverable_reasons[0]
                continue
            break

        assert last_gate_summary is not None
        final_status = "retry_failed" if last_gate_summary.recoverable_reasons else "blocked"
        if any("No scheduled pregame board" in reason for reason in last_gate_summary.terminal_reasons):
            final_status = "no_board"
        elif any("No strict 4-leg, 4-game parlays" in reason for reason in last_gate_summary.terminal_reasons):
            final_status = "no_strict_parlays"
        elif any("Release recommendation remains" in reason for reason in last_gate_summary.terminal_reasons):
            final_status = "quality_blocked"
        elif last_gate_summary.recoverable_reasons:
            final_status = "retry_failed"
        return WorkflowResult(
            attempt_count=attempt_count,
            final_status=final_status,
            board_date=board_date.isoformat(),
            report_path=str(last_report_path) if last_report_path else None,
            retry_reason=retry_reason,
            parlays=last_parlays,
            gate_summary=last_gate_summary,
        )

    def _resolve_board_date(self) -> date:
        self._session.expire_all()
        return QueryService(self._session).resolve_board_date(date.today()) or date.today()

    def _reset_board(self, *, board_date: date, reset_mode: str) -> StartupResetResult:
        result = StartupCacheResetService(self._session).reset(
            mode=reset_mode,
            target_date=date.today(),
            board_date=board_date,
        )
        self._session.expire_all()
        return result

    def _generate_fresh_report(self, *, board_date: date, agent_mode: str) -> Path:
        return generate_daily_automation_report(
            self._session,
            target_date=board_date,
            agent_mode=agent_mode,
            dry_run=agent_mode != "auto",
        )

    def _evaluate_gates(
        self,
        *,
        board_date: date,
        startup_result: StartupRunResult,
        report_path: Path,
    ) -> WorkflowGateSummary:
        availability = QueryService(self._session).board_availability(board_date)
        refresh_metrics = StartupRefreshCacheService(self._session).decide(board_date).metrics
        expected_prediction_rows = StartupComputationCacheService(self._session)._expected_prediction_rows(board_date)
        predictions_for_board = self._prediction_count_for_board(board_date)
        payload_counts = self._same_day_payload_counts()
        extreme_predictions_today, projection_line_divergences = self._sentinel_counts()
        sentinel_status = "ALERT" if (extreme_predictions_today > 0 or projection_line_divergences > 0) else "CLEAN"

        report_text = report_path.read_text(encoding="utf-8")
        release_section = _extract_markdown_bullets(report_text, "Release Recommendation")
        report_flags = tuple(
            description
            for pattern, description in REPORT_FLAG_PATTERNS
            if re.search(pattern, report_text, re.IGNORECASE)
        )
        release_status = release_section.get("status", "UNKNOWN")
        quality_guardrail_status = release_section.get("quality_guardrail_status", "UNKNOWN")

        recoverable_reasons: list[str] = []
        terminal_reasons: list[str] = []

        if startup_result.failed:
            recoverable_reasons.append(f"Startup step failure: {startup_result.error_message or 'unknown error'}")

        if availability.scheduled_games <= 0:
            terminal_reasons.append(f"No scheduled pregame board for {board_date.isoformat()}")
        else:
            if predictions_for_board <= 0:
                recoverable_reasons.append("Scheduled board exists but predictions are missing")
            if int(refresh_metrics.get("line_snapshots_for_target_date", 0)) <= 0:
                recoverable_reasons.append("Scheduled games exist but verified live odds are missing")
            elif int(refresh_metrics.get("scheduled_games_with_verified_lines", 0)) < availability.scheduled_games:
                recoverable_reasons.append("Scheduled games exist but verified live odds are incomplete")

        if payload_counts.get("stats", 0) <= 0:
            recoverable_reasons.append("Same-day stats payload coverage is missing")
        if payload_counts.get("odds", 0) <= 0:
            recoverable_reasons.append("Same-day odds payload coverage is missing")
        if self._injuries_required() and payload_counts.get("injuries", 0) <= 0:
            recoverable_reasons.append("Same-day injuries payload coverage is missing")

        if sentinel_status == "ALERT":
            recoverable_reasons.append("Data quality sentinel is ALERT")
        if report_flags:
            recoverable_reasons.append("Report agents flagged critical unrealistic output")

        operationally_healthy = not recoverable_reasons and availability.scheduled_games > 0
        if operationally_healthy and expected_prediction_rows <= 0:
            terminal_reasons.append("Clean run completed but no valid board outputs exist")
        if operationally_healthy and release_status in {"HOLD", "BLOCKED"}:
            terminal_reasons.append(f"Release recommendation remains {release_status} after an operationally healthy run")

        return WorkflowGateSummary(
            board_date=board_date.isoformat(),
            scheduled_games=availability.scheduled_games,
            live_games=availability.live_games,
            final_games=availability.final_games,
            predictions_for_board=predictions_for_board,
            expected_prediction_rows=expected_prediction_rows,
            line_snapshots_for_board=int(refresh_metrics.get("line_snapshots_for_target_date", 0)),
            scheduled_games_with_verified_lines=int(refresh_metrics.get("scheduled_games_with_verified_lines", 0)),
            raw_payload_counts=payload_counts,
            sentinel_status=sentinel_status,
            extreme_predictions_today=extreme_predictions_today,
            projection_line_divergences=projection_line_divergences,
            release_status=release_status,
            quality_guardrail_status=quality_guardrail_status,
            report_flags=report_flags,
            recoverable_reasons=tuple(dict.fromkeys(recoverable_reasons)),
            terminal_reasons=tuple(dict.fromkeys(terminal_reasons)),
        )

    def _extract_strict_parlays(self, *, board_date: date, top_parlays: int) -> tuple[ParlayRecommendation, ...]:
        sections = MultiGameParlayService(self._session).suggest_by_sportsbook_and_leg_count(
            board_date,
            min_legs=4,
            max_legs=4,
            top_per_leg_count=max(top_parlays, 5),
            minimum_distinct_games=4,
        )
        parlays: list[ParlayRecommendation] = []
        for leg_sections in sections.values():
            for candidates in leg_sections.values():
                parlays.extend(
                    parlay
                    for parlay in candidates
                    if parlay.leg_count == 4 and parlay.game_count == 4
                )
        parlays.sort(key=lambda item: (item.expected_profit_per_unit, item.edge, item.joint_probability), reverse=True)
        return tuple(parlays[:top_parlays])

    def _prediction_count_for_board(self, board_date: date) -> int:
        rows = self._session.execute(
            select(
                Prediction.prediction_id,
                Prediction.predicted_at,
                Game.game_date,
                Game.start_time,
            )
            .join(Game, Prediction.game_id == Game.game_id)
            .where(Game.status == "scheduled")
        ).all()
        return sum(
            1
            for prediction_id, predicted_at, game_date, start_time in rows
            if prediction_id
            and matches_board_date(game_date, start_time, board_date)
            and _coerce_datetime_to_utc_date(predicted_at) == date.today()
        )

    def _same_day_payload_counts(self) -> dict[str, int]:
        day_start = datetime.combine(date.today(), datetime.min.time(), tzinfo=UTC)
        next_day = day_start + timedelta(days=1)
        rows = self._session.execute(
            select(RawPayload.provider_type, func.count(RawPayload.payload_id))
            .where(RawPayload.fetched_at >= day_start, RawPayload.fetched_at < next_day)
            .group_by(RawPayload.provider_type)
        ).all()
        return {str(provider_type): int(count or 0) for provider_type, count in rows}

    def _sentinel_counts(self) -> tuple[int, int]:
        day_start = datetime.combine(date.today(), datetime.min.time(), tzinfo=UTC)
        next_day = day_start + timedelta(days=1)
        extreme_prediction_count = int(
            self._session.scalar(
                select(func.count(Prediction.prediction_id)).where(
                    Prediction.predicted_at >= day_start,
                    Prediction.predicted_at < next_day,
                    (Prediction.over_probability > 0.97) | (Prediction.over_probability < 0.03),
                )
            )
            or 0
        )
        divergence_count = int(
            self._session.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM predictions p
                    JOIN line_snapshots ls ON p.line_snapshot_id = ls.snapshot_id
                    WHERE p.predicted_at >= :day_start
                      AND p.predicted_at < :day_end
                      AND ls.line_value > 0
                      AND ABS(p.projected_mean - ls.line_value) / ls.line_value > 0.40
                    """
                ),
                {"day_start": day_start, "day_end": next_day},
            ).scalar()
            or 0
        )
        return extreme_prediction_count, divergence_count

    def _injuries_required(self) -> bool:
        provider = self._settings.injury_provider.lower()
        return provider == "balldontlie"


def _extract_markdown_bullets(report_text: str, section_title: str) -> dict[str, str]:
    match = re.search(
        rf"^## {re.escape(section_title)}\s*$\n(?P<body>.*?)(?=^## |\Z)",
        report_text,
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        return {}
    bullets: dict[str, str] = {}
    for line in match.group("body").splitlines():
        if not line.startswith("- "):
            continue
        key, _, value = line[2:].partition(":")
        bullets[key.strip()] = value.strip()
    return bullets


def _coerce_datetime_to_utc_date(value: object) -> date | None:
    if not hasattr(value, "astimezone"):
        return None
    coerced = value
    if coerced.tzinfo is None:
        coerced = coerced.replace(tzinfo=UTC)
    return coerced.astimezone(UTC).date()
