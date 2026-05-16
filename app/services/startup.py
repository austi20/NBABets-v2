from __future__ import annotations

import asyncio
import logging as _startup_logging
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.config.settings import get_settings
from app.db.bootstrap import create_all
from app.db.session import configure_engine, get_database_selection, session_scope
from app.evaluation.backtest import RollingOriginBacktester
from app.services.automation import generate_daily_automation_report
from app.services.local_ai_server import local_ai_server
from app.services.prop_analysis import PropAnalysisService, PropOpportunity
from app.services.query import QueryService
from app.services.startup_cache import (
    StartupCacheResetService,
    StartupComputationCacheService,
    StartupRefreshCacheService,
)
from app.services.startup_eta_history import load_step_estimates, record_step_duration
from app.tasks.ingestion import refresh_all
from app.training.data import DatasetLoader
from app.training.pipeline import TrainingPipeline

_startup_log = _startup_logging.getLogger(__name__)


def _emit_volatility_distribution(board_date: date | None) -> None:
    """Log a one-line summary of the predicted volatility tier distribution.

    Pulls every prediction whose `predicted_at` falls on `board_date` (or any
    if board_date is None), computes a `VolatilityScore` for each, and emits a
    single info-level line with low/medium/high counts plus p50/p90 of the
    coefficient and the count of insufficient_features predictions.
    """
    from collections import Counter
    from datetime import datetime as _datetime
    from datetime import time as _time

    from sqlalchemy import select

    from app.models.all import Prediction, PropMarket
    from app.services.volatility import build_feature_snapshot, compute_volatility

    with session_scope() as session:
        stmt = select(Prediction)
        if board_date is not None:
            start = _datetime.combine(board_date, _time.min, tzinfo=UTC)
            stmt = stmt.where(Prediction.predicted_at >= start)
        predictions = session.scalars(stmt).all()

        markets: dict[int, str] = {}
        tiers: Counter[str] = Counter()
        coefficients: list[float] = []
        insufficient = 0
        for prediction in predictions:
            market_key = markets.get(prediction.market_id) if prediction.market_id else None
            if market_key is None and prediction.market_id is not None:
                market = session.get(PropMarket, prediction.market_id)
                if market is not None:
                    market_key = market.key
                    markets[prediction.market_id] = market_key
            if market_key is None:
                market_key = "points"
            score = compute_volatility(
                raw_probability=prediction.over_probability,
                features=build_feature_snapshot(
                    session=session,
                    player_id=prediction.player_id,
                    market_key=market_key,
                    as_of_date=prediction.predicted_at.date(),
                    predicted_minutes_std=None,
                ),
            )
            tiers[score.tier] += 1
            coefficients.append(score.coefficient)
            if score.reason == "insufficient_features":
                insufficient += 1

        if not coefficients:
            return
        sorted_c = sorted(coefficients)
        n = len(sorted_c)
        p50 = sorted_c[n // 2]
        p90 = sorted_c[min(n - 1, int(n * 0.9))]
        _startup_log.info(
            "volatility: tier_distribution low=%d medium=%d high=%d coef_p50=%.2f p90=%.2f insufficient_features=%d",
            tiers.get("low", 0),
            tiers.get("medium", 0),
            tiers.get("high", 0),
            p50,
            p90,
            insufficient,
        )


_STEP_RESULT_KEYS = frozenset({"refresh_mode", "reused_training", "board_date", "cached_historical", "early_complete"})
_OPTIONAL_STEP_KEYS = frozenset({"start_local_ai", "backtest", "automation_report", "analyze_props"})
_STEP_SPECS = (
    ("discover_db", "Find freshest database", 5, 2.0),
    ("initialize_db", "Initialize schema", 10, 3.0),
    ("start_local_ai", "Start local AI server", 5, 8.0),
    ("refresh_data", "Refresh data feeds", 35, 15.0),
    ("train_model", "Train models", 20, 12.0),
    ("predict", "Generate predictions", 10, 6.0),
    ("backtest", "Run backtests", 15, 10.0),
    ("automation_report", "Generate daily automation report", 5, 2.0),
    ("analyze_props", "Rank best prop lines", 5, 3.0),
)


@dataclass
class StartupStep:
    key: str
    label: str
    weight: int
    estimated_seconds: float
    status: str = "pending"
    message: str = ""
    progress_fraction: float = 0.0
    started_at: float | None = None
    ended_at: float | None = None


@dataclass
class StartupSnapshot:
    progress_percent: float
    eta_seconds: float | None
    current_step: str
    current_detail: str
    database_message: str
    board_date_message: str
    started_at: datetime
    completed: bool = False
    failed: bool = False
    error_message: str | None = None
    steps: list[StartupStep] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    opportunities: list[PropOpportunity] = field(default_factory=list)
    log_lines: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StartupRunResult:
    status: str
    failed: bool
    error_message: str | None
    board_date: date | None
    metrics: dict[str, Any]
    report_path: str | None
    opportunity_count: int
    steps: list[StartupStep]
    database_message: str
    board_date_message: str
    started_at: datetime
    completed_at: datetime
    log_lines: list[str]


def _build_startup_steps(eta_history_path: Path) -> list[StartupStep]:
    steps = [StartupStep(*spec) for spec in _STEP_SPECS]
    defaults = {step.key: step.estimated_seconds for step in steps}
    estimates = load_step_estimates(eta_history_path, defaults)
    for step in steps:
        step.estimated_seconds = estimates[step.key]
    return steps


class StartupRunner:
    def __init__(self, *, preferred_board_date: date | None = None, log_path: Path | None = None) -> None:
        settings = get_settings()
        self._lock = threading.Lock()
        self._started_at = datetime.now(UTC)
        self._completed_at: datetime | None = None
        self._preferred_board_date = preferred_board_date or datetime.now().date()
        self._resolved_board_date: date | None = None
        self._report_path: str | None = None
        self._eta_history_path = settings.startup_eta_history_path
        self._log_path = log_path or settings.logs_dir / "startup_latest.log"
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_path.write_text("", encoding="utf-8")
        self._progress_log_buckets: dict[str, int] = {}
        self._last_progress_messages: dict[str, str] = {}
        self._steps = _build_startup_steps(self._eta_history_path)
        self._optional_steps = set(_OPTIONAL_STEP_KEYS)
        self._snapshot = StartupSnapshot(
            progress_percent=0.0,
            eta_seconds=None,
            current_step="Waiting to start",
            current_detail="Waiting to start",
            database_message="",
            board_date_message="Board date: pending",
            started_at=self._started_at,
            steps=deepcopy(self._steps),
        )
        self._append_log_line("Startup runner initialized")

    def run(self) -> StartupRunResult:
        refresh_mode = "live_fetch"
        reused_training = False
        board_date = self._preferred_board_date
        cached_historical: pd.DataFrame | None = None
        try:
            for step in self._steps:
                step_key = step.key
                self._set_step_status(step_key, "running", f"{step.label}...")
                try:
                    result = self._execute_step(
                        step_key=step_key,
                        refresh_mode=refresh_mode,
                        reused_training=reused_training,
                        board_date=board_date,
                        cached_historical=cached_historical,
                    )
                except Exception as step_exc:
                    if step_key in self._optional_steps:
                        self._set_step_status(step_key, "failed", f"Non-critical: {step_exc}")
                        continue
                    raise
                if result is not None:
                    refresh_mode = result.get("refresh_mode", refresh_mode)
                    reused_training = result.get("reused_training", reused_training)
                    board_date = result.get("board_date", board_date)
                    cached_historical = result.get("cached_historical", cached_historical)
                    if result.get("early_complete"):
                        continue
                self._set_step_status(step_key, "completed", "Completed")
            self._mark_finished()
        except Exception as exc:
            self._mark_failed(str(exc), traceback.format_exc())
        return self.result()

    def snapshot(self) -> StartupSnapshot:
        with self._lock:
            snapshot = deepcopy(self._snapshot)
        snapshot.progress_percent = self._calculate_progress(snapshot.steps)
        snapshot.eta_seconds = self._calculate_eta(snapshot.steps, snapshot.progress_percent)
        return snapshot

    def result(self) -> StartupRunResult:
        snapshot = self.snapshot()
        completed_at = self._completed_at or datetime.now(UTC)
        status = "failed" if snapshot.failed else "completed" if snapshot.completed else "running"
        return StartupRunResult(
            status=status,
            failed=snapshot.failed,
            error_message=snapshot.error_message,
            board_date=self._resolved_board_date,
            metrics=deepcopy(snapshot.metrics),
            report_path=self._report_path,
            opportunity_count=int(snapshot.metrics.get("opportunity_count") or len(snapshot.opportunities)),
            steps=deepcopy(snapshot.steps),
            database_message=snapshot.database_message,
            board_date_message=snapshot.board_date_message,
            started_at=snapshot.started_at,
            completed_at=completed_at,
            log_lines=list(snapshot.log_lines),
        )

    def _execute_step(
        self,
        *,
        step_key: str,
        refresh_mode: str,
        reused_training: bool,
        board_date: Any,
        cached_historical: pd.DataFrame | None,
    ) -> dict[str, Any] | None:
        handlers: dict[str, Callable[..., dict[str, Any] | None]] = {
            "discover_db": self._step_discover_db,
            "initialize_db": self._step_initialize_db,
            "start_local_ai": self._step_start_local_ai,
            "refresh_data": self._step_refresh_data,
            "train_model": self._step_train_model,
            "predict": self._step_predict,
            "backtest": self._step_backtest,
            "automation_report": self._step_automation_report,
            "analyze_props": self._step_analyze_props,
        }
        handler = handlers.get(step_key)
        if handler is None:
            return None
        result = handler(
            step_key=step_key,
            refresh_mode=refresh_mode,
            reused_training=reused_training,
            board_date=board_date,
            cached_historical=cached_historical,
        )
        if result is not None:
            unknown_keys = sorted(set(result.keys()) - _STEP_RESULT_KEYS)
            if unknown_keys:
                raise ValueError(f"Startup step '{step_key}' returned unsupported keys: {unknown_keys}")
        return result

    def _step_discover_db(self, **_: Any) -> None:
        selection = configure_engine()
        self._set_database_message(selection.message)
        self._set_metric("database_url", selection.url)
        return None

    def _step_initialize_db(self, **_: Any) -> None:
        create_all()
        return None

    def _step_start_local_ai(self, *, step_key: str, **_: Any) -> dict[str, Any]:
        outcome = local_ai_server.ensure_running()
        self._set_metric("local_ai_server", outcome)
        self._set_step_status(step_key, "completed", outcome["message"])
        return {"early_complete": True}

    def _step_refresh_data(
        self,
        *,
        step_key: str,
        board_date: Any,
        **_: Any,
    ) -> dict[str, Any]:
        target_date = self._preferred_board_date
        with session_scope() as session:
            refresh_decision = StartupRefreshCacheService(session).decide(target_date)
        if refresh_decision.use_cached_data:
            self._set_metric("refresh_data", {**refresh_decision.metrics, "refresh_mode": "same_day_cache"})
            self._set_step_status(step_key, "completed", refresh_decision.reason)
            return {"refresh_mode": "same_day_cache", "early_complete": True}
        metrics = asyncio.run(
            refresh_all(
                target_date,
                progress_callback=lambda current, total, message, _k=step_key: self._set_step_progress(
                    _k, current, total, message,
                ),
            )
        )
        metrics["refresh_mode"] = "live_fetch"
        self._set_metric("refresh_data", metrics)
        with session_scope() as session:
            resolved_board_date = QueryService(session).resolve_board_date(board_date)
        if resolved_board_date is not None:
            board_date = resolved_board_date
            self._resolved_board_date = resolved_board_date
            self._set_metric("board_date", resolved_board_date.isoformat())
            self._set_board_date_message(f"Board date: {board_date.isoformat()}")
        return {"refresh_mode": "live_fetch", "board_date": board_date}

    def _step_train_model(
        self,
        *,
        step_key: str,
        refresh_mode: str,
        cached_historical: pd.DataFrame | None,
        **_: Any,
    ) -> dict[str, Any]:
        if refresh_mode == "same_day_cache":
            with session_scope() as session:
                reuse = StartupComputationCacheService(session).training_decide()
            self._set_metric("train_model_cache", reuse.metrics)
            if reuse.use_cached_result:
                self._set_step_status(step_key, "completed", reuse.reason)
                return {"reused_training": True, "early_complete": True}
        with session_scope() as session:
            if cached_historical is None:
                cached_historical = DatasetLoader(session).load_historical_player_games()
            metrics = TrainingPipeline(session).train(
                progress_callback=lambda current, total, message, _k=step_key: self._set_step_progress(
                    _k, current, total, message,
                ),
                historical=cached_historical,
            )
        self._set_metric("train_model", metrics)
        self._record_quote_coverage_metric()
        return {"cached_historical": cached_historical}

    def _step_predict(
        self,
        *,
        step_key: str,
        refresh_mode: str,
        reused_training: bool,
        board_date: Any,
        cached_historical: pd.DataFrame | None,
        **_: Any,
    ) -> dict[str, Any]:
        if refresh_mode == "same_day_cache" and reused_training:
            with session_scope() as session:
                resolved_board_date = QueryService(session).resolve_board_date(board_date)
                if resolved_board_date is not None:
                    board_date = resolved_board_date
                    self._resolved_board_date = resolved_board_date
                    self._set_metric("board_date", resolved_board_date.isoformat())
                    self._set_board_date_message(f"Board date: {board_date.isoformat()}")
                reuse = StartupComputationCacheService(session).predictions_decide(board_date)
            self._set_metric("predict_cache", reuse.metrics)
            if reuse.use_cached_result:
                self._set_step_status(step_key, "completed", reuse.reason)
                return {"board_date": board_date, "early_complete": True}
        with session_scope() as session:
            if cached_historical is None:
                cached_historical = DatasetLoader(session).load_historical_player_games()
            predictions = TrainingPipeline(session).predict_upcoming(
                target_date=board_date,
                progress_callback=lambda current, total, message, _k=step_key: self._set_step_progress(
                    _k, current, total, message,
                ),
                historical=cached_historical,
            )
        self._set_metric("predictions_generated", len(predictions))
        if get_settings().volatility_tier_enabled:
            try:
                _emit_volatility_distribution(board_date)
            except Exception:  # noqa: BLE001
                _startup_log.exception("volatility distribution log failed")
        return {"board_date": board_date, "cached_historical": cached_historical}

    def _step_backtest(
        self,
        *,
        step_key: str,
        refresh_mode: str,
        reused_training: bool,
        cached_historical: pd.DataFrame | None,
        **_: Any,
    ) -> dict[str, Any]:
        if refresh_mode == "same_day_cache" and reused_training:
            with session_scope() as session:
                reuse = StartupComputationCacheService(session).backtest_decide()
            self._set_metric("backtest_cache", reuse.metrics)
            if reuse.use_cached_result:
                self._set_step_status(step_key, "completed", reuse.reason)
                return {"early_complete": True}
        with session_scope() as session:
            if cached_historical is None:
                cached_historical = DatasetLoader(session).load_historical_player_games()
            backtest = RollingOriginBacktester(session).run(
                progress_callback=lambda current, total, message, _k=step_key: self._set_step_progress(
                    _k, current, total, message,
                ),
                historical=cached_historical,
            )
        self._set_metric("backtest", backtest)
        return {"cached_historical": cached_historical}

    def _step_automation_report(self, *, board_date: Any, **_: Any) -> None:
        with session_scope() as session:
            report_path = generate_daily_automation_report(session, target_date=board_date)
        self._report_path = str(report_path)
        self._set_metric("automation_report_path", str(report_path))
        self._append_log_line(f"Daily automation report generated at {report_path}")

        # Brain self-correction cycle: read the report, diagnose, plan corrections
        try:
            from app.services.brain.self_correct import run_self_correction_cycle

            with session_scope() as session:
                result = run_self_correction_cycle(
                    report_path=report_path,
                    session=session,
                )
            level = result.get("autonomy_level", "observe")
            n_signals = len(result.get("signals", []))
            n_corrections = len(result.get("correction_ids", []))
            dry_run = result.get("dry_run", True)
            mode_label = "dry-run" if dry_run else "live"
            self._set_metric("brain_autonomy_level", level)
            self._set_metric("brain_signals", n_signals)
            self._set_metric("brain_corrections", n_corrections)
            self._append_log_line(
                f"Brain self-correction ({level}, {mode_label}): "
                f"{n_signals} signals, {n_corrections} corrections"
            )
        except Exception as brain_err:
            _startup_log.warning("Brain self-correction skipped: %s", brain_err)

        return None

    def _step_analyze_props(self, *, board_date: Any, **_: Any) -> None:
        with session_scope() as session:
            opportunities = PropAnalysisService(session).top_opportunities(target_date=board_date)
        self._set_opportunities(opportunities)
        self._set_metric("opportunity_count", len(opportunities))
        return None

    def _record_quote_coverage_metric(self) -> None:
        try:
            from sqlalchemy import text as _sql_text

            with session_scope() as _qsession:
                _result = _qsession.execute(
                    _sql_text(
                        "SELECT COUNT(*) FROM line_snapshots "
                        "WHERE json_extract(meta, '$.is_historical_training_quote') = 1"
                    )
                )
                _quote_count = _result.scalar() or 0
            self._set_metric("historical_training_quotes", _quote_count)
            _startup_log.info(
                "[v1.2.2] Historical training quotes available: %d "
                "(target: grows daily as games finish ingestion)",
                _quote_count,
            )
            if _quote_count == 0:
                _startup_log.warning(
                    "[v1.2.2] No historical training quotes found - "
                    "mark_historical_training_quotes() may not be running. "
                    "Betting-market calibration metrics will be unavailable until quotes accumulate."
                )
        except Exception as _qe:
            _startup_log.debug("[v1.2.2] Quote coverage check skipped: %s", _qe)

    def _set_step_status(self, key: str, status: str, message: str) -> None:
        recorded_duration: float | None = None
        with self._lock:
            for step in self._snapshot.steps:
                if step.key != key:
                    continue
                previous_status = step.status
                previous_message = step.message
                step.status = status
                step.message = message
                if status == "running" and step.started_at is None:
                    step.started_at = time.perf_counter()
                    step.progress_fraction = 0.0
                if status in {"completed", "failed"}:
                    step.ended_at = time.perf_counter()
                    step.progress_fraction = 1.0 if status == "completed" else step.progress_fraction
                    if step.started_at is not None:
                        recorded_duration = max(0.0, step.ended_at - step.started_at)
                self._snapshot.current_step = step.label if status == "running" else self._snapshot.current_step
                self._snapshot.current_detail = message
                if status == "running" and previous_status != "running":
                    self._append_log_line_locked(f"{step.label}: started")
                elif status == "completed":
                    self._append_log_line_locked(f"{step.label}: {message}")
                elif status == "failed":
                    self._append_log_line_locked(f"{step.label}: failed - {message}")
                elif status == "running" and message and message != previous_message:
                    self._append_log_line_locked(f"{step.label}: {message}")
                break
        if recorded_duration is not None:
            record_step_duration(self._eta_history_path, key, recorded_duration)

    def _set_step_progress(self, key: str, current: int | None, total: int | None, message: str) -> None:
        with self._lock:
            for step in self._snapshot.steps:
                if step.key != key:
                    continue
                if step.status != "running":
                    step.status = "running"
                step.message = message
                if step.started_at is None:
                    step.started_at = time.perf_counter()
                if current is not None and total and total > 0:
                    step.progress_fraction = min(max(current / total, 0.0), 0.99 if current < total else 1.0)
                self._snapshot.current_step = step.label
                self._snapshot.current_detail = message
                progress_bucket = int(step.progress_fraction * 10) if total and total > 0 else -1
                should_log = message != self._last_progress_messages.get(key, "")
                if progress_bucket > self._progress_log_buckets.get(key, -1):
                    should_log = True
                if should_log and message:
                    self._progress_log_buckets[key] = progress_bucket
                    self._last_progress_messages[key] = message
                    progress_suffix = ""
                    if current is not None and total and total > 0:
                        progress_suffix = f" ({current}/{total}, {int(step.progress_fraction * 100)}%)"
                    self._append_log_line_locked(f"{step.label}: {message}{progress_suffix}")
                break

    def _set_database_message(self, message: str) -> None:
        with self._lock:
            self._snapshot.database_message = message

    def _set_board_date_message(self, message: str) -> None:
        with self._lock:
            self._snapshot.board_date_message = message

    def _set_metric(self, key: str, value: Any) -> None:
        with self._lock:
            self._snapshot.metrics[key] = value

    def _set_opportunities(self, opportunities: list[PropOpportunity]) -> None:
        with self._lock:
            self._snapshot.opportunities = opportunities

    def _mark_finished(self) -> None:
        with self._lock:
            self._completed_at = datetime.now(UTC)
            self._snapshot.completed = True
            self._snapshot.current_step = "Startup complete"
            self._snapshot.current_detail = "All startup steps completed"
            self._snapshot.database_message = get_database_selection().message
            predictions_generated = self._snapshot.metrics.get("predictions_generated")
            opportunity_count = self._snapshot.metrics.get("opportunity_count")
            summary_parts = []
            if predictions_generated is not None:
                summary_parts.append(f"{predictions_generated} predictions")
            if opportunity_count is not None:
                summary_parts.append(f"{opportunity_count} ranked props")
            if summary_parts:
                self._append_log_line_locked(f"Startup summary: {', '.join(summary_parts)}")
            self._append_log_line_locked("Startup complete")

    def _mark_failed(self, error_message: str, traceback_text: str | None = None) -> None:
        with self._lock:
            self._completed_at = datetime.now(UTC)
            self._snapshot.failed = True
            self._snapshot.error_message = error_message
            self._snapshot.current_step = "Startup failed"
            self._snapshot.current_detail = error_message
            self._append_log_line_locked(f"Startup failed: {error_message}")
            if traceback_text:
                for line in traceback_text.rstrip().splitlines():
                    self._append_log_line_locked(line)

    def _calculate_progress(self, steps: list[StartupStep]) -> float:
        completed_weight = sum(step.weight for step in steps if step.status == "completed")
        running_weight = 0.0
        for step in steps:
            if step.status != "running" or step.started_at is None:
                continue
            if step.progress_fraction > 0:
                step_progress = step.progress_fraction
            else:
                elapsed = max(time.perf_counter() - step.started_at, 0.0)
                step_progress = min(elapsed / step.estimated_seconds, 0.95)
            running_weight += step.weight * step_progress
        return min(completed_weight + running_weight, 100.0)

    def _calculate_eta(self, steps: list[StartupStep], progress_percent: float) -> float | None:
        if progress_percent <= 0:
            return None
        elapsed = max((datetime.now(UTC) - self._started_at).total_seconds(), 0.0)
        return elapsed * (100.0 - progress_percent) / progress_percent

    def _append_log_line(self, message: str) -> None:
        with self._lock:
            self._append_log_line_locked(message)

    def _append_log_line_locked(self, message: str) -> None:
        timestamp = datetime.now().astimezone().strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        if self._snapshot.log_lines and self._snapshot.log_lines[-1] == line:
            return
        self._snapshot.log_lines.append(line)
        self._snapshot.log_lines = self._snapshot.log_lines[-250:]
        try:
            with self._log_path.open("a", encoding="utf-8") as handle:
                handle.write(f"{line}\n")
        except OSError:
            pass


class StartupCoordinator:
    # Lineup poll window: 3:00 PM - 11:00 PM Eastern Time (game-day active window)
    _POLL_INTERVAL_SECONDS: int = 900
    _POLL_WINDOW_START_HOUR: int = 15
    _POLL_WINDOW_END_HOUR: int = 23
    _FULL_REFRESH_INTERVAL_SECONDS: int = 7200

    def __init__(
        self,
        *,
        on_start: Callable[[], None] | None = None,
        on_success: Callable[[StartupRunResult], None] | None = None,
    ) -> None:
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._poll_thread: threading.Thread | None = None
        self._poll_stop_event = threading.Event()
        self._runner = StartupRunner()
        self._steps = self._runner._steps
        self._optional_steps = self._runner._optional_steps
        self._last_result: StartupRunResult | None = None
        self._active_run_id: str | None = None
        self._pending_full_refresh = False
        self._on_start = on_start
        self._on_success = on_success

    def start(self, *, full_refresh: bool = False) -> None:
        if self._thread and self._thread.is_alive():
            if full_refresh:
                with self._state_lock:
                    self._pending_full_refresh = True
                self._runner._append_log_line(
                    "Full refresh requested while startup is running; scheduling a hard refresh rerun."
                )
            return
        self._stop_lineup_poll()
        self._poll_stop_event = threading.Event()
        self._runner = StartupRunner()
        self._steps = self._runner._steps
        self._optional_steps = self._runner._optional_steps
        self._last_result = None
        if full_refresh:
            today = date.today()
            with session_scope() as session:
                reset = StartupCacheResetService(session).hard_reset(target_date=today, board_date=today)
            self._runner._append_log_line(
                "Manual full refresh: cleared same-day provider caches, lines, and model artifacts "
                f"(raw_payloads={reset.deleted_raw_payloads}, line_snapshots={reset.deleted_line_snapshots}, "
                f"artifact_files={len(reset.deleted_artifacts)})"
            )
        if self._on_start is not None:
            try:
                self._on_start()
            except Exception as exc:  # pragma: no cover - defensive logging path
                _startup_log.warning("startup on_start hook failed: %s", exc)
        self._active_run_id = uuid.uuid4().hex
        self._runner._append_log_line("Launching startup worker")
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def run_async(self, *, full_refresh: bool = False) -> str:
        self.start(full_refresh=full_refresh)
        return self._active_run_id or ""

    def snapshot(self) -> StartupSnapshot:
        return self._runner.snapshot()

    def active_run_id(self) -> str | None:
        return self._active_run_id

    def result(self) -> StartupRunResult | None:
        return self._last_result

    def _run(self) -> None:
        self._last_result = self._runner.run()
        pending_full_refresh = False
        with self._state_lock:
            pending_full_refresh = self._pending_full_refresh
            self._pending_full_refresh = False
        if pending_full_refresh:
            self._runner._append_log_line("Running queued hard refresh request.")
            self._thread = None
            self.start(full_refresh=True)
            return
        if self._last_result.failed:
            return
        if self._on_success is not None:
            try:
                self._on_success(self._last_result)
            except Exception as exc:  # pragma: no cover - defensive logging path
                self._runner._append_log_line(f"Board cache warmup failed: {exc}")
                _startup_log.warning("Board cache warmup failed: %s", exc, exc_info=True)
        self._start_lineup_poll()

    def stop(self) -> None:
        self._stop_lineup_poll()

    def _calculate_progress(self, steps: list[StartupStep]) -> float:
        return self._runner._calculate_progress(steps)

    def _calculate_eta(self, steps: list[StartupStep], progress_percent: float) -> float | None:
        return self._runner._calculate_eta(steps, progress_percent)

    def _start_lineup_poll(self) -> None:
        import asyncio as _asyncio
        try:
            from zoneinfo import ZoneInfo as _ZoneInfo
        except ImportError:
            _ZoneInfo = None  # type: ignore[assignment,misc]

        from app.tasks.ingestion import poll_lineup_changes

        _interval = self._POLL_INTERVAL_SECONDS
        _start_h = self._POLL_WINDOW_START_HOUR
        _end_h = self._POLL_WINDOW_END_HOUR
        _full_refresh_interval = self._FULL_REFRESH_INTERVAL_SECONDS
        _stop = self._poll_stop_event

        def _poll_loop() -> None:
            last_full_refresh_at = 0.0
            while not _stop.wait(timeout=_interval):
                try:
                    if _ZoneInfo is not None:
                        _et = _ZoneInfo("America/New_York")
                        _now_h = datetime.now(_et).hour
                    else:
                        _now_h = datetime.now().hour
                    if not (_start_h <= _now_h < _end_h):
                        continue
                    if _ZoneInfo is not None:
                        _target_date = datetime.now(_ZoneInfo("America/New_York")).date()
                    else:
                        _target_date = datetime.now().date()
                    result = _asyncio.run(poll_lineup_changes(target_date=_target_date))
                    repredicted = result.get("repredicted_games", 0)
                    if repredicted:
                        self._runner._append_log_line(
                            f"[lineup-poll] {repredicted} game(s) re-predicted after inactive list update"
                        )
                        self._runner._set_metric("lineup_poll_repredictions", repredicted)
                        self._runner._set_metric("analysis_refresh_requested_at", datetime.now(UTC).isoformat())

                    now_ts = time.time()
                    if now_ts - last_full_refresh_at >= _full_refresh_interval:
                        refresh_metrics = _asyncio.run(refresh_all(target_date=_target_date))
                        self._runner._set_metric("scheduled_full_refresh", refresh_metrics)
                        with session_scope() as _session:
                            report_path = generate_daily_automation_report(_session, target_date=_target_date)
                        self._runner._set_metric("automation_report_path", str(report_path))
                        self._runner._set_metric("analysis_refresh_requested_at", datetime.now(UTC).isoformat())
                        last_full_refresh_at = now_ts
                        self._runner._append_log_line("[lineup-poll] Scheduled full refresh completed")
                except Exception as _poll_exc:
                    _startup_log.debug("[lineup-poll] poll cycle error: %s", _poll_exc)

        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._poll_thread = threading.Thread(target=_poll_loop, daemon=True, name="lineup-poll")
        self._poll_thread.start()
        _startup_log.info(
            "[v1.2.3] Lineup polling thread started - interval %ds, window %02d:00-%02d:00 ET",
            _interval,
            _start_h,
            _end_h,
        )

    def _stop_lineup_poll(self) -> None:
        self._poll_stop_event.set()
        if (
            self._poll_thread
            and self._poll_thread.is_alive()
            and self._poll_thread is not threading.current_thread()
        ):
            self._poll_thread.join(timeout=1.0)
        self._poll_thread = None
