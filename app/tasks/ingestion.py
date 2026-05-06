from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date, timedelta

from app.config.settings import get_settings
from app.db.session import session_scope
from app.providers.factory import get_injuries_provider, get_odds_provider, get_stats_provider
from app.services.ingestion import IngestionOrchestrator
from app.services.provider_cache import LocalProviderCache

ProgressCallback = Callable[[int | None, int | None, str], None]


async def refresh_all(
    target_date: date | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, int]:
    settings = get_settings()
    effective_date = target_date or date.today()
    history_days = settings.startup_history_days
    stats_provider = get_stats_provider()
    odds_provider = get_odds_provider()
    injuries_provider = get_injuries_provider()
    cold_start_date = effective_date - timedelta(days=history_days)
    history_start_date = cold_start_date
    cache_anchor_day: date | None = None
    missing_log_days: list[date] = []
    odds_cache_pruned = 0
    if settings.enable_provider_cache:
        provider_cache = LocalProviderCache()
        cache_anchor_day = provider_cache.latest_cached_log_day(
            provider_names=_cache_provider_names(stats_provider),
        )
        if cache_anchor_day is not None:
            overlap_start_date = max(
                cold_start_date,
                cache_anchor_day - timedelta(days=settings.provider_cache_log_overlap_days),
            )
            history_start_date = overlap_start_date
        requested_history_days = _date_range(cold_start_date, effective_date)
        missing_log_days = provider_cache.get_missing_log_days(
            provider_names=_cache_provider_names(stats_provider),
            requested_days=requested_history_days,
        )
        if missing_log_days:
            history_start_date = min(history_start_date, min(missing_log_days))
        odds_cache_pruned = provider_cache.prune_odds_cache(
            keep_date=effective_date,
            provider_name=_cache_provider_name(odds_provider),
        )
    total_steps = 9
    current_step = 0

    def emit(message: str) -> None:
        if progress_callback is not None:
            progress_callback(current_step, total_steps, message)

    if cache_anchor_day is not None:
        emit(
            _cache_window_message(
                cache_anchor_day=cache_anchor_day,
                history_start_date=history_start_date,
                missing_log_days=missing_log_days,
                odds_cache_pruned=odds_cache_pruned,
            )
        )
    elif odds_cache_pruned:
        emit(f"Pruned {odds_cache_pruned} stale odds cache entries before refresh")
    emit("Preparing provider clients")
    if hasattr(stats_provider, "verify_required_access"):
        current_step += 1
        emit(f"Verifying {stats_provider.provider_name} access")
        await stats_provider.verify_required_access()
    prefetched_odds_result = None
    prefetched_odds_lines = None
    if hasattr(stats_provider, "set_team_scope"):
        current_step += 1
        emit(f"Prefetching odds from {odds_provider.provider_name}")
        prefetched_odds_result, prefetched_odds_lines = await odds_provider.fetch_upcoming_player_props(effective_date)
        team_scope = {
            value.upper()
            for line in prefetched_odds_lines
            for value in (
                str(line.meta.get("home_team_abbreviation", "")).strip(),
                str(line.meta.get("away_team_abbreviation", "")).strip(),
            )
            if value
        }
        if team_scope:
            stats_provider.set_team_scope(team_scope)
    with session_scope() as session:
        orchestrator = IngestionOrchestrator(session)
        current_step += 1
        emit("Refreshing schedule, teams, and rosters")
        metrics = await orchestrator.refresh_reference_data(stats_provider, effective_date)
        current_step += 1
        emit(
            _history_progress_message(
                prefix="Refreshing historical schedules",
                start_date=history_start_date,
                end_date=effective_date,
                cache_anchor_day=cache_anchor_day,
                fallback_days=history_days,
                missing_log_days=missing_log_days,
            )
        )
        metrics.update(await orchestrator.refresh_reference_history(stats_provider, history_start_date, effective_date))
        current_step += 1
        emit(
            _history_progress_message(
                prefix="Ingesting player game logs",
                start_date=history_start_date,
                end_date=effective_date,
                cache_anchor_day=cache_anchor_day,
                fallback_days=history_days,
                missing_log_days=missing_log_days,
            )
        )
        metrics.update(await orchestrator.ingest_game_logs(stats_provider, history_start_date, effective_date))
        current_step += 1
        emit("Refreshing game-day player availability")
        changed_game_ids: set[int] = set()
        if hasattr(stats_provider, "fetch_game_availability"):
            avail_result = await orchestrator.ingest_game_availability(stats_provider, effective_date)
            changed_game_ids = avail_result.pop("_changed_game_ids", set())  # type: ignore[assignment]
            metrics.update({k: v for k, v in avail_result.items() if isinstance(v, int)})
        current_step += 1
        emit(f"Refreshing injuries from {injuries_provider.provider_name}")
        metrics.update(await orchestrator.ingest_injuries(injuries_provider, effective_date))
        current_step += 1
        emit(f"Ingesting verified odds from {odds_provider.provider_name}")
        metrics.update(await orchestrator.ingest_odds(odds_provider, effective_date, prefetched_odds_result, prefetched_odds_lines))
        current_step += 1
        emit("Capturing closing lines and finalizing refresh")
        metrics.update(orchestrator.mark_closing_lines(effective_date))

        # If the official inactive list changed since the last run, re-generate
        # predictions for the affected games so downstream consumers always see
        # projections that account for confirmed absences and role changes.
        if changed_game_ids:
            current_step += 1
            emit(
                f"Re-predicting {len(changed_game_ids)} game(s) after inactive list update"
            )
            from app.training.pipeline import TrainingPipeline
            pipeline = TrainingPipeline(session)
            pipeline.predict_upcoming(target_date=effective_date, game_ids=changed_game_ids)
            metrics["repredicted_games"] = len(changed_game_ids)

        if progress_callback is not None:
            progress_callback(total_steps, total_steps, "Refresh complete")
        return metrics


def run_refresh_all() -> None:
    asyncio.run(refresh_all())


async def poll_lineup_changes(
    target_date: date | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, int]:
    """Lightweight availability-only poll for intra-day lineup monitoring.

    Called by the background polling thread in StartupCoordinator every
    N minutes during the game-day window.  Only fetches the NBA inactive list
    and triggers targeted re-prediction for games whose lineup changed since
    the last fetch.  Does NOT refetch game logs, odds, or injuries — those are
    handled by the full refresh_all() at startup.

    Returns metrics dict with keys: player_availability, repredicted_games.
    Returns empty dict if the stats provider has no fetch_game_availability.
    """
    effective_date = target_date or date.today()
    stats_provider = get_stats_provider()
    metrics: dict[str, int] = {}

    if not hasattr(stats_provider, "fetch_game_availability"):
        return metrics

    with session_scope() as session:
        orchestrator = IngestionOrchestrator(session)
        avail_result = await orchestrator.ingest_game_availability(stats_provider, effective_date)
        changed_game_ids: set[int] = avail_result.pop("_changed_game_ids", set())  # type: ignore[assignment]
        metrics.update({k: v for k, v in avail_result.items() if isinstance(v, int)})
        session.commit()

        if changed_game_ids:
            if progress_callback is not None:
                progress_callback(None, None, f"Re-predicting {len(changed_game_ids)} game(s) after lineup change")
            from app.training.pipeline import TrainingPipeline
            pipeline = TrainingPipeline(session)
            pipeline.predict_upcoming(target_date=effective_date, game_ids=changed_game_ids)
            metrics["repredicted_games"] = len(changed_game_ids)
            session.commit()

    if progress_callback is not None:
        n = metrics.get("repredicted_games", 0)
        progress_callback(1, 1, f"Lineup poll complete — {n} game(s) updated")
    return metrics


def _cache_provider_name(provider: object) -> str | None:
    aliases = list(getattr(provider, "_cache_provider_aliases", []) or [])
    if aliases:
        return aliases[0]
    return getattr(provider, "_cache_provider_name", getattr(provider, "provider_name", None))


def _cache_provider_names(provider: object) -> list[str]:
    provider_names = list(getattr(provider, "_cache_provider_aliases", []) or [])
    primary_name = _cache_provider_name(provider)
    if primary_name:
        provider_names.append(primary_name)
    current_name = getattr(provider, "provider_name", None)
    if current_name:
        provider_names.append(str(current_name))
    return [item for item in dict.fromkeys(provider_names) if item]


def _history_progress_message(
    *,
    prefix: str,
    start_date: date,
    end_date: date,
    cache_anchor_day: date | None,
    fallback_days: int,
    missing_log_days: list[date],
) -> str:
    if cache_anchor_day is None:
        return f"{prefix} for last {fallback_days} days"
    if missing_log_days:
        earliest_missing = min(missing_log_days)
        if earliest_missing < start_date:
            return (
                f"{prefix} from cached overlap {start_date.isoformat()} through {end_date.isoformat()} "
                f"with earlier cached gaps starting {earliest_missing.isoformat()}"
            )
        return (
            f"{prefix} from cached overlap {start_date.isoformat()} through {end_date.isoformat()} "
            f"with {len(missing_log_days)} cached gap day(s) to backfill"
        )
    if start_date == end_date:
        return f"{prefix} from cached boundary day {start_date.isoformat()}"
    return f"{prefix} from cached boundary {start_date.isoformat()} through {end_date.isoformat()}"


def _cache_window_message(
    *,
    cache_anchor_day: date,
    history_start_date: date,
    missing_log_days: list[date],
    odds_cache_pruned: int,
) -> str:
    message = (
        f"Using cached history anchor {cache_anchor_day.isoformat()} with revalidation from "
        f"{history_start_date.isoformat()}"
    )
    if missing_log_days:
        message += f" and {len(missing_log_days)} cached gap day(s) to recover"
    if odds_cache_pruned:
        message += f"; pruned {odds_cache_pruned} stale odds cache entries"
    return message


def _date_range(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current_day = start_date
    while current_day <= end_date:
        days.append(current_day)
        current_day += timedelta(days=1)
    return days
