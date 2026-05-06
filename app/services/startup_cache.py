from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import (
    BacktestResult,
    Game,
    GamePlayerAvailability,
    InjuryReport,
    LineSnapshot,
    ModelRun,
    PlayerGameLog,
    Prediction,
    RawPayload,
)
from app.services.board_date import matches_board_date
from app.services.provider_cache import LocalProviderCache
from app.training.artifacts import artifact_exists, artifact_paths, load_artifact, resolve_artifact_namespace

LOCAL_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class StartupRefreshDecision:
    use_cached_data: bool
    reason: str
    metrics: dict[str, int | str]


@dataclass(frozen=True)
class StartupReuseDecision:
    use_cached_result: bool
    reason: str
    metrics: dict[str, int | str]


@dataclass(frozen=True)
class StartupResetResult:
    mode: str
    target_date: str
    board_date: str | None
    deleted_predictions: int = 0
    deleted_backtests: int = 0
    deleted_model_runs: int = 0
    deleted_reports: int = 0
    deleted_line_snapshots: int = 0
    deleted_raw_payloads: int = 0
    deleted_injury_reports: int = 0
    deleted_game_availability: int = 0
    deleted_provider_cached_fetches: int = 0
    deleted_provider_cached_log_days: int = 0
    deleted_provider_cached_logs: int = 0
    deleted_artifacts: tuple[str, ...] = ()


class StartupRefreshCacheService:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()

    def decide(self, target_date: date) -> StartupRefreshDecision:
        today = date.today()
        stats_payloads = self._raw_payload_count("stats", today)
        odds_payloads = self._raw_payload_count("odds", today)
        injuries_payloads = self._raw_payload_count("injuries", today)
        game_count = self._game_count(target_date)
        scheduled_game_count = self._scheduled_game_count(target_date)
        line_snapshot_count = self._line_snapshot_count(target_date)
        scheduled_games_with_verified_lines = self._scheduled_games_with_verified_lines(target_date)

        metrics = {
            "stats_payloads_today": stats_payloads,
            "odds_payloads_today": odds_payloads,
            "injuries_payloads_today": injuries_payloads,
            "games_for_target_date": game_count,
            "scheduled_games_for_target_date": scheduled_game_count,
            "line_snapshots_for_target_date": line_snapshot_count,
            "scheduled_games_with_verified_lines": scheduled_games_with_verified_lines,
        }

        if stats_payloads <= 0:
            return StartupRefreshDecision(False, "No same-day stats cache available", metrics)
        if odds_payloads <= 0:
            return StartupRefreshDecision(False, "No same-day odds cache available", metrics)
        if self._requires_live_injuries() and injuries_payloads <= 0:
            return StartupRefreshDecision(False, "No same-day injuries cache available", metrics)
        if scheduled_game_count > 0 and line_snapshot_count <= 0:
            return StartupRefreshDecision(False, "Scheduled games exist but cached lines are missing", metrics)
        if scheduled_game_count > scheduled_games_with_verified_lines:
            return StartupRefreshDecision(
                False,
                "One or more scheduled games are missing fully verified live odds",
                metrics,
            )

        return StartupRefreshDecision(
            True,
            "Reused same-day cached provider pulls",
            metrics,
        )

    def _raw_payload_count(self, provider_type: str, target_day: date) -> int:
        rows = self._session.scalars(
            select(RawPayload).where(RawPayload.provider_type == provider_type)
        ).all()
        count = 0
        for row in rows:
            fetched_at = row.fetched_at
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=UTC)
            if fetched_at.astimezone(LOCAL_TIMEZONE).date() == target_day:
                count += 1
        return count

    def _game_count(self, target_date: date) -> int:
        return len(_board_game_ids(self._session, target_date, statuses=None, include_superseded=False))

    def _scheduled_game_count(self, target_date: date) -> int:
        return len(_board_game_ids(self._session, target_date, statuses=("scheduled",)))

    def _line_snapshot_count(self, target_date: date) -> int:
        board_game_ids = _board_game_ids(self._session, target_date, statuses=("scheduled",))
        if not board_game_ids:
            return 0
        query = select(func.count(LineSnapshot.snapshot_id)).where(LineSnapshot.game_id.in_(board_game_ids))
        return int(self._session.scalar(query) or 0)

    def _scheduled_games_with_verified_lines(self, target_date: date) -> int:
        scheduled_game_ids = _board_game_ids(self._session, target_date, statuses=("scheduled",))
        if not scheduled_game_ids:
            return 0
        rows = self._session.execute(
            select(LineSnapshot.game_id, LineSnapshot.meta, LineSnapshot.over_odds, LineSnapshot.under_odds)
            .where(LineSnapshot.game_id.in_(scheduled_game_ids))
        ).all()
        verified_game_ids: set[int] = set()
        for game_id, meta, over_odds, under_odds in rows:
            snapshot_meta = _coerce_snapshot_meta(meta)
            if (
                bool(snapshot_meta.get("is_live_quote", False))
                and str(snapshot_meta.get("odds_verification_status", "")).lower() == "provider_live"
                and over_odds is not None
                and under_odds is not None
            ):
                verified_game_ids.add(int(game_id))
        return len(verified_game_ids)

    def _requires_live_injuries(self) -> bool:
        provider = self._settings.injury_provider.lower()
        return provider == "balldontlie" and bool(self._settings.balldontlie_api_key)


class StartupComputationCacheService:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._artifact_namespace = resolve_artifact_namespace(
            getattr(getattr(session.bind, "url", None), "render_as_string", lambda **_: self._settings.database_url)(
                hide_password=False
            )
            if getattr(session.bind, "url", None) is not None
            else self._settings.database_url,
            self._settings.app_env,
        )

    def training_decide(self) -> StartupReuseDecision:
        paths = artifact_paths(self._settings.model_version, self._artifact_namespace)
        artifact_map = {
            "minutes_model": paths.minutes_model,
            "stat_models": paths.stat_models,
            "calibrators": paths.calibrators,
            "metadata": paths.metadata,
        }
        found_count = sum(1 for path in artifact_map.values() if artifact_exists(path))
        if found_count < len(artifact_map):
            missing = sorted(name for name, path in artifact_map.items() if not artifact_exists(path))
            return StartupReuseDecision(
                False,
                f"Incomplete cached model artifacts: missing {', '.join(missing)}",
                {"artifacts_found": found_count, "artifacts_expected": len(artifact_map)},
            )
        try:
            metadata = load_artifact(paths.metadata)
            # Probe-load core artifacts so we never reuse a partial/corrupt bundle.
            load_artifact(paths.minutes_model)
            load_artifact(paths.stat_models)
            load_artifact(paths.calibrators)
        except Exception:
            return StartupReuseDecision(
                False,
                "Cached model artifacts could not be loaded",
                {"artifacts_found": found_count, "artifacts_expected": len(artifact_map)},
            )
        trained_at_text = str(metadata.get("trained_at") or "")
        trained_today = _local_date_from_iso(trained_at_text) == date.today()
        metrics = {
            "artifacts_found": found_count,
            "artifacts_expected": len(artifact_map),
            "trained_at": trained_at_text,
            "artifact_namespace": self._artifact_namespace,
        }
        if not trained_today:
            return StartupReuseDecision(False, "Model artifacts are stale", metrics)
        return StartupReuseDecision(True, "Reused same-day trained model artifacts", metrics)

    def predictions_decide(self, target_date: date) -> StartupReuseDecision:
        paths = artifact_paths(self._settings.model_version, self._artifact_namespace)
        try:
            metadata = load_artifact(paths.metadata)
            model_run_id = int(metadata.get("model_run_id"))
        except Exception:
            return StartupReuseDecision(
                False,
                "No valid model metadata found for prediction cache reuse",
                {"metadata_available": 0},
            )
        prediction_rows = self._session.execute(
            select(
                Prediction.prediction_id,
                Prediction.predicted_at,
                Game.game_id,
                Game.game_date,
                Game.start_time,
            )
            .join(Game, Prediction.game_id == Game.game_id)
            .where(Game.status == "scheduled")
            .where(Prediction.model_run_id == model_run_id)
        ).all()
        predictions_today = sum(
            1
            for _, predicted_at, _, game_date, start_time in prediction_rows
            if matches_board_date(game_date, start_time, target_date) and _coerce_datetime_to_local_date(predicted_at) == date.today()
        )
        expected_rows = self._expected_prediction_rows(target_date)
        metrics = {
            "predictions_today": predictions_today,
            "expected_prediction_rows": expected_rows,
            "model_run_id": model_run_id,
        }
        if predictions_today <= 0 and expected_rows > 0:
            return StartupReuseDecision(False, "No same-day cached predictions found", metrics)
        if predictions_today < expected_rows:
            return StartupReuseDecision(False, "Cached predictions do not cover the full same-day board", metrics)
        return StartupReuseDecision(True, "Reused same-day prediction cache", metrics)

    def backtest_decide(self) -> StartupReuseDecision:
        rows = self._session.execute(select(BacktestResult.backtest_result_id, BacktestResult.computed_at)).all()
        backtests_today = sum(1 for _, computed_at in rows if _coerce_datetime_to_local_date(computed_at) == date.today())
        metrics = {"backtests_today": backtests_today}
        if backtests_today <= 0:
            return StartupReuseDecision(False, "No same-day cached backtest found", metrics)
        return StartupReuseDecision(True, "Reused same-day backtest cache", metrics)

    def _expected_prediction_rows(self, target_date: date) -> int:
        board_game_ids = _board_game_ids(self._session, target_date, statuses=("scheduled",))
        if not board_game_ids:
            return 0
        player_history = {
            int(player_id): (int(game_count or 0), float(minutes_total or 0.0))
            for player_id, game_count, minutes_total in self._session.execute(
                select(
                    PlayerGameLog.player_id,
                    func.count(func.distinct(PlayerGameLog.game_id)),
                    func.coalesce(func.sum(PlayerGameLog.minutes), 0.0),
                ).group_by(PlayerGameLog.player_id)
            ).all()
        }
        rows = self._session.execute(
            select(
                LineSnapshot.game_id,
                LineSnapshot.player_id,
                LineSnapshot.market_id,
                LineSnapshot.sportsbook_id,
                LineSnapshot.meta,
            )
            .where(LineSnapshot.game_id.in_(board_game_ids))
        ).all()
        expected_keys: set[tuple[int, int, int, int]] = set()
        for game_id, player_id, market_id, sportsbook_id, meta in rows:
            snapshot_meta = _coerce_snapshot_meta(meta)
            historical_games, historical_minutes = player_history.get(int(player_id), (0, 0.0))
            if (
                bool(snapshot_meta.get("is_live_quote", False))
                and str(snapshot_meta.get("odds_verification_status", "")).lower() == "provider_live"
                and historical_games >= self._settings.minimum_prediction_history_games
                and historical_minutes >= self._settings.minimum_prediction_history_minutes
            ):
                expected_keys.add((int(game_id), int(player_id), int(market_id), int(sportsbook_id)))
        return len(expected_keys)


def _coerce_snapshot_meta(meta: object) -> dict[str, object]:
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        try:
            parsed = json.loads(meta)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_datetime_to_local_date(value: object) -> date | None:
    if not hasattr(value, "astimezone"):
        return None
    coerced = value
    if coerced.tzinfo is None:
        coerced = coerced.replace(tzinfo=UTC)
    return coerced.astimezone(LOCAL_TIMEZONE).date()


def _local_date_from_iso(value: str) -> date | None:
    if not value:
        return None
    try:
        return _coerce_datetime_to_local_date(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except Exception:
        return None


def _board_game_ids(
    session: Session,
    target_date: date,
    *,
    statuses: tuple[str, ...] | None,
    include_superseded: bool = False,
) -> set[int]:
    query = select(Game.game_id, Game.game_date, Game.start_time, Game.status)
    if statuses is not None:
        query = query.where(Game.status.in_(statuses))
    elif not include_superseded:
        query = query.where(Game.status != "superseded")
    rows = session.execute(query).all()
    return {
        int(game_id)
        for game_id, game_date, start_time, _ in rows
        if matches_board_date(game_date, start_time, target_date)
    }


class StartupCacheResetService:
    """Invalidate startup outputs and same-day operational inputs in reset tiers."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()

    def reset(
        self,
        *,
        mode: str,
        target_date: date | None = None,
        board_date: date | None = None,
    ) -> StartupResetResult:
        if mode not in {"soft_reset", "hard_reset"}:
            raise ValueError(f"Unsupported startup reset mode: {mode}")
        effective_date = target_date or date.today()
        effective_board_date = board_date or effective_date
        day_start, day_end = _utc_day_bounds(effective_date)

        deleted_predictions = self._delete_predictions(day_start, day_end)
        deleted_backtests = self._delete_backtests(day_start, day_end)
        deleted_model_runs = self._delete_model_runs(day_start, day_end)
        deleted_reports = self._delete_report_files(effective_date)

        deleted_line_snapshots = 0
        deleted_raw_payloads = 0
        deleted_injury_reports = 0
        deleted_game_availability = 0
        deleted_provider_cached_fetches = 0
        deleted_provider_cached_log_days = 0
        deleted_provider_cached_logs = 0
        deleted_artifacts: tuple[str, ...] = ()

        if mode == "hard_reset":
            board_game_ids = _board_game_ids(
                self._session,
                effective_board_date,
                statuses=None,
                include_superseded=False,
            )
            deleted_line_snapshots = self._delete_line_snapshots(board_game_ids)
            deleted_injury_reports = self._delete_injury_reports(day_start, day_end, board_game_ids)
            deleted_game_availability = self._delete_game_availability(board_game_ids)
            deleted_raw_payloads = self._delete_raw_payloads(day_start, day_end)
            provider_cache_summary = LocalProviderCache().delete_day_scoped_entries(target_date=effective_date)
            deleted_provider_cached_fetches = provider_cache_summary["deleted_provider_cached_fetches"]
            deleted_provider_cached_log_days = provider_cache_summary["deleted_provider_cached_log_days"]
            deleted_provider_cached_logs = provider_cache_summary["deleted_provider_cached_logs"]
            deleted_artifacts = self._delete_artifacts()

        self._session.commit()
        return StartupResetResult(
            mode=mode,
            target_date=effective_date.isoformat(),
            board_date=effective_board_date.isoformat() if effective_board_date else None,
            deleted_predictions=deleted_predictions,
            deleted_backtests=deleted_backtests,
            deleted_model_runs=deleted_model_runs,
            deleted_reports=deleted_reports,
            deleted_line_snapshots=deleted_line_snapshots,
            deleted_raw_payloads=deleted_raw_payloads,
            deleted_injury_reports=deleted_injury_reports,
            deleted_game_availability=deleted_game_availability,
            deleted_provider_cached_fetches=deleted_provider_cached_fetches,
            deleted_provider_cached_log_days=deleted_provider_cached_log_days,
            deleted_provider_cached_logs=deleted_provider_cached_logs,
            deleted_artifacts=deleted_artifacts,
        )

    def soft_reset(
        self,
        *,
        target_date: date | None = None,
        board_date: date | None = None,
    ) -> StartupResetResult:
        return self.reset(mode="soft_reset", target_date=target_date, board_date=board_date)

    def hard_reset(
        self,
        *,
        target_date: date | None = None,
        board_date: date | None = None,
    ) -> StartupResetResult:
        return self.reset(mode="hard_reset", target_date=target_date, board_date=board_date)

    def invalidate_todays_caches(self, target_date: date | None = None) -> dict[str, int | str]:
        result = self.soft_reset(target_date=target_date)
        return {
            "deleted_predictions": result.deleted_predictions,
            "target_date": result.target_date,
        }

    def _delete_predictions(self, day_start: datetime, day_end: datetime) -> int:
        statement = delete(Prediction).where(Prediction.predicted_at >= day_start, Prediction.predicted_at < day_end)
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_backtests(self, day_start: datetime, day_end: datetime) -> int:
        statement = delete(BacktestResult).where(BacktestResult.computed_at >= day_start, BacktestResult.computed_at < day_end)
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_model_runs(self, day_start: datetime, day_end: datetime) -> int:
        statement = delete(ModelRun).where(
            or_(
                and_(ModelRun.completed_at.is_not(None), ModelRun.completed_at >= day_start, ModelRun.completed_at < day_end),
                and_(ModelRun.completed_at.is_(None), ModelRun.started_at >= day_start, ModelRun.started_at < day_end),
            )
        )
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_report_files(self, target_date: date) -> int:
        pattern = f"automation_daily_{target_date.strftime('%Y%m%d')}T*.md"
        deleted = 0
        for path in self._settings.reports_dir.glob(pattern):
            try:
                path.unlink()
                deleted += 1
            except OSError:
                continue
        return deleted

    def _delete_line_snapshots(self, board_game_ids: set[int]) -> int:
        if not board_game_ids:
            return 0
        statement = delete(LineSnapshot).where(LineSnapshot.game_id.in_(board_game_ids))
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_injury_reports(self, day_start: datetime, day_end: datetime, board_game_ids: set[int]) -> int:
        predicates = [InjuryReport.report_timestamp >= day_start, InjuryReport.report_timestamp < day_end]
        if board_game_ids:
            predicates = [or_(and_(*predicates), InjuryReport.game_id.in_(board_game_ids))]
        statement = delete(InjuryReport).where(*predicates)
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_game_availability(self, board_game_ids: set[int]) -> int:
        if not board_game_ids:
            return 0
        statement = delete(GamePlayerAvailability).where(GamePlayerAvailability.game_id.in_(board_game_ids))
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_raw_payloads(self, day_start: datetime, day_end: datetime) -> int:
        statement = delete(RawPayload).where(
            RawPayload.provider_type.in_(("stats", "odds", "injuries")),
            RawPayload.fetched_at >= day_start,
            RawPayload.fetched_at < day_end,
        )
        return int(self._session.execute(statement).rowcount or 0)

    def _delete_artifacts(self) -> tuple[str, ...]:
        artifact_namespace = resolve_artifact_namespace(
            getattr(getattr(self._session.bind, "url", None), "render_as_string", lambda **_: self._settings.database_url)(
                hide_password=False
            )
            if getattr(self._session.bind, "url", None) is not None
            else self._settings.database_url,
            self._settings.app_env,
        )
        paths = artifact_paths(self._settings.model_version, artifact_namespace)
        deleted: list[str] = []
        for artifact_path in (paths.minutes_model, paths.stat_models, paths.calibrators, paths.metadata):
            if not artifact_path.exists():
                continue
            try:
                artifact_path.unlink()
            except OSError:
                continue
            deleted.append(str(artifact_path))
        return tuple(deleted)


def _utc_day_bounds(target_date: date) -> tuple[datetime, datetime]:
    day_start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC)
    next_day = day_start + timedelta(days=1)
    return day_start, next_day
