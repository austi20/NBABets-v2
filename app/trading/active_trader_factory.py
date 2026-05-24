"""Factory that assembles a production-ready ActiveTrader from live components.

Dependency injection pattern: callers supply I/O callables (live-stats fetch,
session factory) so unit tests can swap them without touching application logic.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.trading.active_trader import ActiveCandidate, ActiveTrader, MergingCandidateProvider
from app.trading.live_limits import ActiveTraderConfig
from app.trading.live_stats_poller import LiveStatsPoller
from app.trading.loop import make_kill_switch_callable
from app.trading.market_book import MarketBook
from app.trading.portfolio_manager import PortfolioManager
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.sql_ledger import SqlPortfolioLedger


class StaticCandidateProvider:
    """CandidateProvider backed by an in-memory list.

    Suitable for loading a decision pack once at startup and feeding it
    to the MergingCandidateProvider as the base provider.
    """

    def __init__(self, candidates: list[ActiveCandidate]) -> None:
        self._candidates = list(candidates)

    def update(self, candidates: list[ActiveCandidate]) -> None:
        self._candidates = list(candidates)

    async def candidates(self) -> list[ActiveCandidate]:
        return list(self._candidates)


def build_active_trader(
    *,
    at_config: ActiveTraderConfig,
    risk_limits: RiskLimits,
    router: Any,
    session_factory: Callable[[], Session],
    live_stats_fetch: Callable[[list[int]], Awaitable[dict[str, Any]]],
    base_candidates: list[ActiveCandidate],
    preemptive_source: Callable[[], Iterable[ActiveCandidate]] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[ActiveTrader, LiveStatsPoller, StaticCandidateProvider]:
    """Assemble a fully wired ActiveTrader for production use.

    Returns the trader, the LiveStatsPoller (caller must start its ``run()``
    coroutine), and the StaticCandidateProvider so the caller can refresh
    the base candidate list without rebuilding the whole assembly.

    The ``router`` must implement the OrderRouter protocol (e.g.
    KalshiOrderRouter). Pass ``live_stats_fetch`` as an async function
    accepting a list of BDL player IDs and returning the raw API payload.
    """
    book = MarketBook()

    poller = LiveStatsPoller(
        config=at_config,
        player_ids_source=_noop_player_ids,
        fetch=live_stats_fetch,
        sleep=asyncio.sleep,
    )

    from app.trading.active_trader import make_on_pace_callable

    portfolio = PortfolioManager(
        config=at_config,
        router=router,
        clock=clock,
        on_pace_callable=make_on_pace_callable(poller),
    )

    base_provider = StaticCandidateProvider(base_candidates)

    if preemptive_source is not None:
        candidates_source = MergingCandidateProvider(
            base=base_provider,
            poller=poller,
            preemptive_source=preemptive_source,
        )
    else:
        candidates_source = base_provider  # type: ignore[assignment]

    ledger = SqlPortfolioLedger(session_factory)
    risk_engine = ExposureRiskEngine(risk_limits)
    kill_switch = make_kill_switch_callable(session_factory)

    trader = ActiveTrader(
        config=at_config,
        risk_limits=risk_limits,
        book=book,
        portfolio=portfolio,
        candidates_source=candidates_source,
        kill_switch=kill_switch,
        clock=clock,
        risk_engine=risk_engine,
        ledger=ledger,
    )

    return trader, poller, base_provider


def _noop_player_ids() -> list[int]:
    return []
