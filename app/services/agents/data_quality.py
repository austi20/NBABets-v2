from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask
from app.services.db_maintenance import DbMaintenanceService
from app.services.provider_cache import LocalProviderCache


class DataQualityAgent:
    role = "data_quality"

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._maintenance = DbMaintenanceService(session)
        self._provider_cache = LocalProviderCache()

    def handle(self, task: AgentTask) -> AgentResult:
        day_start = datetime.combine(date.today(), datetime.min.time(), tzinfo=UTC)

        duplicate_predictions = int(
            self._session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT model_run_id, game_id, player_id, market_id, COUNT(*) AS c
                        FROM predictions
                        GROUP BY model_run_id, game_id, player_id, market_id
                        HAVING COUNT(*) > 1
                    ) t
                    """
                )
            ).scalar()
            or 0
        )
        orphan_predictions = int(
            self._session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM predictions p
                    LEFT JOIN games g ON p.game_id = g.game_id
                    WHERE g.game_id IS NULL
                    """
                )
            ).scalar()
            or 0
        )
        stale_raw_payloads = int(
            self._session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM raw_payloads
                    WHERE fetched_at < datetime('now', '-30 day')
                    """
                )
            ).scalar()
            or 0
        )
        recent_days = [date.today() - timedelta(days=offset) for offset in range(0, 7)]
        latest_cache_day = self._provider_cache.latest_cached_log_day()
        missing_cache_days = self._provider_cache.get_missing_log_days(requested_days=recent_days)

        # --- DNP contamination: zero-minute and low-minute games in recent game logs ---
        dnp_counts = self._session.execute(
            text(
                """
                SELECT
                    SUM(CASE WHEN pgl.minutes = 0 THEN 1 ELSE 0 END) AS zero_minute,
                    SUM(CASE WHEN pgl.minutes > 0 AND pgl.minutes < 5 THEN 1 ELSE 0 END) AS low_minute
                FROM player_game_logs pgl
                JOIN games g ON pgl.game_id = g.game_id
                WHERE g.status != 'superseded'
                  AND g.game_date >= date('now', '-30 day')
                """
            )
        ).fetchone()
        zero_minute_games = int(dnp_counts[0] or 0)
        low_minute_games = int(dnp_counts[1] or 0)

        # --- Extreme probability predictions today ---
        extreme_predictions = int(
            self._session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM predictions
                    WHERE predicted_at >= :day_start
                      AND (over_probability > 0.97 OR over_probability < 0.03)
                    """
                ),
                {"day_start": day_start},
            ).scalar()
            or 0
        )

        # --- Mean-vs-line divergence (model > 40% off the sportsbook line) ---
        projection_line_divergences = int(
            self._session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM predictions p
                    JOIN line_snapshots ls ON p.line_snapshot_id = ls.snapshot_id
                    WHERE p.predicted_at >= :day_start
                      AND ls.line_value > 0
                      AND ABS(p.projected_mean - ls.line_value) / ls.line_value > 0.40
                    """
                ),
                {"day_start": day_start},
            ).scalar()
            or 0
        )

        actions: list[AgentAction] = []
        if duplicate_predictions > 0:
            actions.append(
                AgentAction(
                    action_type="manual_dedupe_review",
                    reason="Duplicate prediction keys detected.",
                    payload={"duplicate_predictions": duplicate_predictions},
                    safe_to_auto_execute=False,
                )
            )
        if stale_raw_payloads > 0:
            actions.append(
                AgentAction(
                    action_type="cache_prune",
                    reason="Stale raw payload records exceed retention window.",
                    payload={
                        "stale_raw_payloads": stale_raw_payloads,
                        "retention_days": self._settings.data_quality_raw_payload_retention_days,
                    },
                    safe_to_auto_execute=True,
                )
            )
        if orphan_predictions > 0:
            actions.append(
                AgentAction(
                    action_type="integrity_repair",
                    reason="Prediction rows reference missing games.",
                    payload={"orphan_predictions": orphan_predictions},
                    safe_to_auto_execute=False,
                )
            )
        if missing_cache_days:
            actions.append(
                AgentAction(
                    action_type="cache_backfill",
                    reason="Provider cache has missing recent log days.",
                    payload={
                        "latest_cached_day": latest_cache_day.isoformat() if latest_cache_day else None,
                        "missing_days": [item.isoformat() for item in missing_cache_days],
                    },
                    safe_to_auto_execute=False,
                )
            )
        if zero_minute_games > 0 or low_minute_games > 0:
            actions.append(
                AgentAction(
                    action_type="dnp_contamination_warning",
                    reason=(
                        f"{zero_minute_games} zero-minute (DNP) and {low_minute_games} low-minute "
                        f"(<5 min) game logs in the last 30 days. Both are excluded from the "
                        f"training pipeline (minutes >= 5 filter) but remain in the database."
                    ),
                    payload={
                        "zero_minute_games": zero_minute_games,
                        "low_minute_games": low_minute_games,
                    },
                    safe_to_auto_execute=False,
                )
            )
        if extreme_predictions > 0:
            actions.append(
                AgentAction(
                    action_type="extreme_probability_detected",
                    reason=(
                        f"{extreme_predictions} predictions today have over_probability > 97% or < 3%. "
                        "This typically indicates DNP contamination, stale features, or calibration failure."
                    ),
                    payload={"extreme_predictions": extreme_predictions},
                    safe_to_auto_execute=False,
                )
            )
        if projection_line_divergences > 0:
            actions.append(
                AgentAction(
                    action_type="projection_line_divergence",
                    reason=(
                        f"{projection_line_divergences} predictions today have projected_mean > 40% "
                        "away from the sportsbook line. The model's projections are significantly "
                        "diverged from market consensus."
                    ),
                    payload={"projection_line_divergences": projection_line_divergences},
                    safe_to_auto_execute=False,
                )
            )

        executed: list[str] = []
        if not task.dry_run:
            for action in actions:
                if action.action_type != "cache_prune":
                    continue
                deleted = self._maintenance.prune_old_raw_payloads(self._settings.data_quality_raw_payload_retention_days)
                executed.append(f"cache_prune:{deleted}")
            if task.input_payload.get("weekly_maintenance", False):
                if self._maintenance.vacuum_and_analyze():
                    executed.append("vacuum_analyze")

        status = "ok" if not actions else "recommendation"
        summary = "Data quality checks passed." if not actions else f"Detected {len(actions)} data quality issue(s)."
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status=status,
            summary=summary,
            actions=actions,
            confidence=0.8 if actions else 0.95,
            details={
                "duplicate_predictions": duplicate_predictions,
                "orphan_predictions": orphan_predictions,
                "stale_raw_payloads": stale_raw_payloads,
                "latest_cached_log_day": latest_cache_day.isoformat() if latest_cache_day else None,
                "missing_cached_log_days": [item.isoformat() for item in missing_cache_days],
                "zero_minute_games": zero_minute_games,
                "extreme_predictions": extreme_predictions,
                "projection_line_divergences": projection_line_divergences,
                "executed_actions": executed,
                "dry_run": task.dry_run,
            },
        )
