from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from datetime import date as _date
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from sse_starlette.sse import EventSourceResponse

from app.config.settings import get_settings
from app.db.models.trading import TradingKillSwitch
from app.providers.exchanges.kalshi_client import KalshiClient
from app.server.schemas.trading import (
    ActiveLimitsModel,
    ExchangePositionModel,
    FillModel,
    LimitsResponseModel,
    LimitsUpdateRequestModel,
    LivePositionModel,
    PickBulkRequestModel,
    PickToggleRequestModel,
    PositionModel,
    RestingOrderModel,
    ThresholdsUpdateRequestModel,
    TradingBrainCheckModel,
    TradingBrainSyncModel,
    TradingBrainSyncRequestModel,
    TradingIntentRequestModel,
    TradingIntentResponseModel,
    TradingLiveSnapshotModel,
    TradingLoopStartRequestModel,
    TradingLoopStatusModel,
    TradingPnlModel,
    TradingQuoteModel,
    TradingReadinessCheckModel,
    TradingReadinessModel,
    TradingSnapshotModel,
    WalletBalanceResponseModel,
)
from app.server.services.board_cache import BoardCache
from app.server.sse import SSE_RECONNECT_MS
from app.trading.decision_brain import default_brain_status, load_last_brain_status, sync_decision_brain
from app.trading.loop import set_kill_switch
from app.trading.mapper import signal_to_market_ref
from app.trading.monitoring import (
    ExchangePositionSnapshot,
    KalshiPublicMarketDataClient,
    LivePositionSnapshot,
    QuoteSnapshot,
    RestingOrderSnapshot,
    build_monitor_snapshot,
    load_monitored_symbols,
)
from app.trading.protocols import PortfolioLedger
from app.trading.readiness import build_trading_readiness
from app.trading.risk import RiskLimits
from app.trading.selections import SelectionStore
from app.trading.snapshot_service import TradingSnapshotService
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.stream_publisher import TradingStreamPublisher
from app.trading.types import ExecutionIntent, OrderEvent, Signal
from app.trading.wallet_init import extract_balance

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/trading", tags=["trading"])


def _snapshot_service(request: Request) -> TradingSnapshotService:
    return cast(TradingSnapshotService, request.app.state.trading_snapshot_service)


def _board_date_today() -> _date:
    return _date.today()


# mtime-keyed cache for the decision-pack mode so the SSE stream does not
# re-read and re-parse the decisions file on every tick. Keyed by resolved
# path; value is (mtime_ns, mode).
_DECISION_PACK_MODE_CACHE: dict[str, tuple[int, str]] = {}


def _decision_pack_mode(settings) -> str:  # noqa: ANN001
    path = Path(str(settings.kalshi_decisions_path))
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        return "observe"
    cache_key = str(path)
    cached = _DECISION_PACK_MODE_CACHE.get(cache_key)
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]
    mode = _read_decision_pack_mode(path)
    _DECISION_PACK_MODE_CACHE[cache_key] = (mtime_ns, mode)
    return mode


def _read_decision_pack_mode(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload if isinstance(payload, list) else payload.get("decisions", [])
        any_live = isinstance(rows, list) and any(
            isinstance(row, dict) and row.get("mode") == "live" for row in rows
        )
        return "supervised-live" if any_live else "observe"
    except (OSError, json.JSONDecodeError):
        return "observe"


def _build_live_snapshot(request: Request) -> TradingLiveSnapshotModel:
    settings = get_settings()
    service = _snapshot_service(request)
    controller = getattr(request.app.state, "trading_loop_controller", None)
    loop_state = "idle"
    if controller is not None:
        try:
            loop_state = controller.status().state
        except Exception:  # noqa: BLE001
            loop_state = "idle"
    risk_engine = request.app.state.trading_risk
    ledger = _ledger(request)
    limits = getattr(risk_engine, "limits", RiskLimits())
    monitored_symbols = load_monitored_symbols(settings.kalshi_symbols_path)
    # Resolve the kill-switch once per snapshot build so a single SSE tick does
    # not re-query the DB for each monitor-snapshot branch.
    kill_switch_active = _kill_switch_active(request, bool(risk_engine.killed))
    account_error: str | None = None
    with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as market_client:
        private_key_path = Path(settings.kalshi_private_key_path) if settings.kalshi_private_key_path else None
        if settings.kalshi_api_key_id and private_key_path is not None and private_key_path.exists():
            with KalshiClient(
                api_key_id=settings.kalshi_api_key_id.get_secret_value() if settings.kalshi_api_key_id else "",
                private_key_path=private_key_path,
                base_url=settings.kalshi_base_url,
            ) as account_client:
                ledger_snapshot = build_monitor_snapshot(
                    ledger=ledger,
                    limits=limits,
                    kill_switch_active=kill_switch_active,
                    monitored_symbols=monitored_symbols,
                    market_client=market_client,
                    account_client=account_client,
                )
        else:
            if settings.kalshi_api_key_id or private_key_path is not None:
                account_error = "Kalshi account sync disabled: missing API key or private key file."
            ledger_snapshot = build_monitor_snapshot(
                ledger=ledger,
                limits=limits,
                kill_switch_active=kill_switch_active,
                monitored_symbols=monitored_symbols,
                market_client=market_client,
            )
        readiness = build_trading_readiness(settings=settings, market_client=market_client)
    errors = [*ledger_snapshot.errors]
    if account_error is not None:
        errors.append(account_error)
    brain_status: TradingBrainSyncModel | None = None
    try:
        brain_status = _brain_sync_model(load_last_brain_status(settings) or default_brain_status(settings))
    except Exception:  # noqa: BLE001
        logger.exception("Decision brain status unavailable")
        errors.append("Decision brain status unavailable")
    return service.build(
        board_date=_board_date_today(),
        ledger_state=ledger_snapshot,
        positions=[_live_position_model(position) for position in ledger_snapshot.positions],
        fills=[FillModel.from_dataclass(fill) for fill in ledger.recent_fills(limit=50)],
        resting_orders=[_resting_order_model(order) for order in ledger_snapshot.resting_orders],
        loop_state=loop_state,
        mode=_decision_pack_mode(settings),
        kill_switch_active=ledger_snapshot.kill_switch_active,
        readiness=_readiness_model(readiness),
        brain_status=brain_status,
        errors=errors,
    )


@router.get("/snapshot-live", response_model=TradingLiveSnapshotModel)
def trading_snapshot_live(request: Request) -> TradingLiveSnapshotModel:
    """Return a one-shot live trading snapshot for the trading tab."""
    return _build_live_snapshot(request)


@router.get("/stream")
async def trading_stream(request: Request) -> EventSourceResponse:
    """Stream live trading snapshots as server-sent events with a reconnect hint."""
    service = _snapshot_service(request)

    async def event_generator() -> Any:
        last_yielded = ""
        while True:
            if await request.is_disconnected():
                break
            snapshot = _build_live_snapshot(request)
            payload = snapshot.model_dump_json()
            if payload != last_yielded:
                last_yielded = payload
                # retry tells the EventSource client how long to wait before
                # auto-reconnecting if the stream drops.
                yield {"event": "snapshot", "data": payload, "retry": SSE_RECONNECT_MS}
            await service.publisher.wait_for_update(timeout=2.0)

    return EventSourceResponse(event_generator())


def _ledger(request: Request) -> PortfolioLedger:
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is not None:
        return SqlPortfolioLedger(factory)
    return cast(PortfolioLedger, request.app.state.trading_ledger)


def _active_limits(risk_engine) -> ActiveLimitsModel | None:  # noqa: ANN001
    limits = getattr(risk_engine, "limits", None)
    if limits is None:
        return None
    return ActiveLimitsModel(
        per_order_cap=float(limits.per_order_cap),
        per_market_cap=float(limits.per_market_cap),
        max_open_notional=float(limits.max_open_notional),
        daily_loss_cap=float(limits.daily_loss_cap),
        reject_cooldown_seconds=int(limits.reject_cooldown_seconds),
    )


def _kill_switch_active(request: Request, fallback: bool) -> bool:
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is None:
        return fallback
    with factory() as session:
        row = session.get(TradingKillSwitch, 1)
        return bool(row.killed) if row is not None else fallback


def _pnl_model(request: Request) -> TradingPnlModel:
    ledger = _ledger(request)
    risk_engine = request.app.state.trading_risk
    return TradingPnlModel(
        daily_realized_pnl=ledger.daily_realized_pnl(),
        kill_switch_active=_kill_switch_active(request, bool(risk_engine.killed)),
        active_limits=_active_limits(risk_engine),
    )


def _quote_model(quote: QuoteSnapshot) -> TradingQuoteModel:
    return TradingQuoteModel(**quote.__dict__)


def _live_position_model(position: LivePositionSnapshot) -> LivePositionModel:
    return LivePositionModel(
        market_symbol=position.market_symbol,
        market_key=position.market_key,
        side=position.side,
        ticker=position.ticker,
        open_stake=position.open_stake,
        contract_count=position.contract_count,
        avg_price=position.avg_price,
        current_exit_price=position.current_exit_price,
        current_value=position.current_value,
        unrealized_pnl=position.unrealized_pnl,
        unrealized_pnl_pct=position.unrealized_pnl_pct,
        realized_pnl=position.realized_pnl,
        updated_at=position.updated_at,
        quote=_quote_model(position.quote) if position.quote is not None else None,
    )


def _exchange_position_model(position: ExchangePositionSnapshot) -> ExchangePositionModel:
    return ExchangePositionModel(
        ticker=position.ticker,
        side=position.side,
        contract_count=position.contract_count,
        net_position=position.net_position,
        market_exposure=position.market_exposure,
        fees_paid=position.fees_paid,
        realized_pnl=position.realized_pnl,
        current_exit_price=position.current_exit_price,
        current_value=position.current_value,
        updated_at=position.updated_at,
        quote=_quote_model(position.quote) if position.quote is not None else None,
    )


def _resting_order_model(order: RestingOrderSnapshot) -> RestingOrderModel:
    return RestingOrderModel(**order.__dict__)


def _readiness_model(readiness) -> TradingReadinessModel:  # noqa: ANN001
    return TradingReadinessModel(
        observed_at=readiness.observed_at,
        state=readiness.state,
        summary=readiness.summary,
        live_trading_enabled=readiness.live_trading_enabled,
        credentials_configured=readiness.credentials_configured,
        account_sync_enabled=readiness.account_sync_enabled,
        decisions_path=readiness.decisions_path,
        symbols_path=readiness.symbols_path,
        decision_id=readiness.decision_id,
        ticker=readiness.ticker,
        game_date=readiness.game_date,
        market_status=readiness.market_status,
        executable_symbol_count=readiness.executable_symbol_count,
        unresolved_symbol_count=readiness.unresolved_symbol_count,
        brain_state=readiness.brain_state,
        brain_policy_version=readiness.brain_policy_version,
        brain_selected_candidate_id=readiness.brain_selected_candidate_id,
        brain_last_sync_at=readiness.brain_last_sync_at,
        brain_snapshot_dir=readiness.brain_snapshot_dir,
        checks=[TradingReadinessCheckModel(**check.__dict__) for check in readiness.checks],
    )


def _brain_sync_model(result) -> TradingBrainSyncModel:  # noqa: ANN001
    return TradingBrainSyncModel(
        state=result.state,
        policy_version=result.policy_version,
        policy_hash=result.policy_hash,
        board_date=result.board_date,
        mode=result.mode,
        generated_candidate_count=result.generated_candidate_count,
        manual_candidate_count=result.manual_candidate_count,
        exported_target_count=result.exported_target_count,
        resolved_symbol_count=result.resolved_symbol_count,
        unresolved_symbol_count=result.unresolved_symbol_count,
        selected_candidate_id=result.selected_candidate_id,
        selected_ticker=result.selected_ticker,
        selected_candidate_ids=getattr(result, "selected_candidate_ids", []),
        selected_tickers=getattr(result, "selected_tickers", []),
        live_candidate_count=getattr(result, "live_candidate_count", 0),
        targets_path=result.targets_path,
        symbols_path=result.symbols_path,
        decisions_path=result.decisions_path,
        snapshot_dir=result.snapshot_dir,
        checks=[TradingBrainCheckModel(**check.__dict__) for check in result.checks],
        synced_at=result.synced_at,
    )


def _loop_status_model(status) -> TradingLoopStatusModel:  # noqa: ANN001
    return TradingLoopStatusModel(
        state=status.state,
        message=status.message,
        pid=status.pid,
        started_at=status.started_at,
        ended_at=status.ended_at,
        return_code=status.return_code,
        command=status.command,
        log_path=status.log_path,
        preflight_output=status.preflight_output,
        brain_state=status.brain_state,
        selected_candidate_id=status.selected_candidate_id,
        selected_ticker=status.selected_ticker,
    )


@router.get("/positions", response_model=list[PositionModel])
def trading_positions(request: Request) -> list[PositionModel]:
    """Return the current open trading positions from the ledger."""
    ledger = _ledger(request)
    return [PositionModel.from_dataclass(position) for position in ledger.open_positions()]


@router.get("/pnl", response_model=TradingPnlModel)
def trading_pnl(request: Request) -> TradingPnlModel:
    """Return daily realized PnL plus kill-switch and active-limit state."""
    return _pnl_model(request)


@router.get("/snapshot", response_model=TradingSnapshotModel)
def trading_snapshot(request: Request) -> TradingSnapshotModel:
    """Return the full monitor snapshot (positions, quotes, PnL, orders)."""
    settings = get_settings()
    risk_engine = request.app.state.trading_risk
    ledger = _ledger(request)
    limits = getattr(risk_engine, "limits", RiskLimits())
    monitored_symbols = load_monitored_symbols(settings.kalshi_symbols_path)
    account_error: str | None = None
    with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as market_client:
        private_key_path = Path(settings.kalshi_private_key_path) if settings.kalshi_private_key_path else None
        if settings.kalshi_api_key_id and private_key_path is not None and private_key_path.exists():
            with KalshiClient(
                api_key_id=settings.kalshi_api_key_id.get_secret_value() if settings.kalshi_api_key_id else "",
                private_key_path=private_key_path,
                base_url=settings.kalshi_base_url,
            ) as account_client:
                snapshot = build_monitor_snapshot(
                    ledger=ledger,
                    limits=limits,
                    kill_switch_active=_kill_switch_active(request, bool(risk_engine.killed)),
                    monitored_symbols=monitored_symbols,
                    market_client=market_client,
                    account_client=account_client,
                )
        else:
            if settings.kalshi_api_key_id or private_key_path is not None:
                account_error = "Kalshi account sync disabled: missing API key or private key file."
            snapshot = build_monitor_snapshot(
                ledger=ledger,
                limits=limits,
                kill_switch_active=_kill_switch_active(request, bool(risk_engine.killed)),
                monitored_symbols=monitored_symbols,
                market_client=market_client,
            )
    errors = [*snapshot.errors]
    if account_error is not None:
        errors.append(account_error)
    return TradingSnapshotModel(
        observed_at=snapshot.observed_at,
        daily_realized_pnl=snapshot.daily_realized_pnl,
        daily_unrealized_pnl=snapshot.daily_unrealized_pnl,
        total_daily_pnl=snapshot.total_daily_pnl,
        open_notional=snapshot.open_notional,
        budget_used=snapshot.budget_used,
        budget_remaining=snapshot.budget_remaining,
        max_open_notional=snapshot.max_open_notional,
        daily_loss_cap=snapshot.daily_loss_cap,
        loss_progress=snapshot.loss_progress,
        kill_switch_active=snapshot.kill_switch_active,
        positions=[_live_position_model(position) for position in snapshot.positions],
        quotes=[_quote_model(quote) for quote in snapshot.quotes],
        account_positions=[_exchange_position_model(position) for position in snapshot.account_positions],
        resting_orders=[_resting_order_model(order) for order in snapshot.resting_orders],
        errors=errors,
    )


@router.get("/readiness", response_model=TradingReadinessModel)
def trading_readiness() -> TradingReadinessModel:
    """Return the live-trading readiness assessment (creds, pack, market state)."""
    settings = get_settings()
    with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as market_client:
        readiness = build_trading_readiness(settings=settings, market_client=market_client)
    return _readiness_model(readiness)


@router.get("/brain/status", response_model=TradingBrainSyncModel)
def trading_brain_status() -> TradingBrainSyncModel:
    """Return the last persisted decision-brain status for the trading UI."""
    settings = get_settings()
    try:
        result = load_last_brain_status(settings) or default_brain_status(settings)
    except Exception:  # noqa: BLE001 - status should degrade into a visible API error
        logger.exception("Decision brain status unavailable")
        raise HTTPException(status_code=500, detail="Decision brain status unavailable") from None
    return _brain_sync_model(result)


@router.post("/brain/sync", response_model=TradingBrainSyncModel)
def trading_brain_sync(request: Request, payload: TradingBrainSyncRequestModel) -> TradingBrainSyncModel:
    """Rebuild the prop board and re-sync the decision brain for the requested date."""
    settings = get_settings()
    board_cache = getattr(request.app.state, "board_cache", None) or BoardCache()
    try:
        board_entry = board_cache.populate(payload.board_date)
    except Exception:  # noqa: BLE001 - caller needs a clear setup failure
        logger.exception("Could not build prop board for decision brain")
        raise HTTPException(status_code=500, detail="Could not build prop board for decision brain") from None
    result = sync_decision_brain(
        settings=settings,
        board_entry=board_entry,
        board_date=payload.board_date or board_entry.board_date,
        mode=payload.mode,
        candidate_limit=payload.candidate_limit,
        resolve_markets=payload.resolve_markets,
        build_pack=payload.build_pack,
    )
    return _brain_sync_model(result)


@router.get("/fills/recent", response_model=list[FillModel])
def trading_recent_fills(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[FillModel]:
    """Return the most recent trade fills, newest first, up to ``limit``."""
    ledger = _ledger(request)
    return [FillModel.from_dataclass(fill) for fill in ledger.recent_fills(limit=limit)]


@router.post("/kill-switch", response_model=TradingPnlModel)
def trading_kill_switch(request: Request) -> TradingPnlModel:
    """Engage the trading kill-switch and halt the loop, returning PnL state."""
    risk_engine = request.app.state.trading_risk
    risk_engine.set_killed(True)
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is not None:
        set_kill_switch(factory, killed=True, set_by="api")
    controller = getattr(request.app.state, "trading_loop_controller", None)
    if controller is not None and factory is not None:
        controller.kill(session_factory=factory)
    return _pnl_model(request)


@router.get("/loop/status", response_model=TradingLoopStatusModel)
def trading_loop_status(request: Request) -> TradingLoopStatusModel:
    """Return the current state of the background trading loop process."""
    controller = request.app.state.trading_loop_controller
    return _loop_status_model(controller.status())


@router.post("/loop/start", response_model=TradingLoopStatusModel)
def trading_loop_start(request: Request, payload: TradingLoopStartRequestModel | None = None) -> TradingLoopStatusModel:
    """Build the board and start the background trading loop for the date."""
    settings = get_settings()
    body = payload or TradingLoopStartRequestModel()
    board_cache = getattr(request.app.state, "board_cache", None) or BoardCache()
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is None:
        raise HTTPException(status_code=500, detail="Trading session factory is not configured.")
    try:
        board_entry = board_cache.populate(body.board_date)
    except Exception:  # noqa: BLE001
        logger.exception("Could not build prop board for loop start")
        raise HTTPException(status_code=500, detail="Could not build prop board for loop start") from None
    controller = request.app.state.trading_loop_controller
    status = controller.start(
        settings=settings,
        board_entry=board_entry,
        board_date=body.board_date or board_entry.board_date,
        session_factory=factory,
    )
    return _loop_status_model(status)


_SELECTIONS_FILENAME = "trading_selections.json"


def _resolve_under_app_data(base: str | Path, candidate: str | Path) -> Path:
    """Resolve ``candidate`` and ensure it does not escape ``base``.

    Rejects ``..`` traversal in the candidate and any resolved path that lands
    outside ``base`` (the base itself is allowed). Use this for any path derived
    from configuration or request input so it cannot point outside the intended
    directory.
    """
    if ".." in Path(candidate).parts:
        raise ValueError(f"path traversal not allowed: {candidate}")
    base_resolved = Path(base).resolve()
    cand = Path(candidate)
    resolved = cand.resolve() if cand.is_absolute() else (base_resolved / cand).resolve()
    if base_resolved != resolved and base_resolved not in resolved.parents:
        raise ValueError(f"path escapes base dir: {resolved}")
    return resolved


def _selections_path(request: Request) -> Path:
    return _resolve_under_app_data(get_settings().app_data_dir, _SELECTIONS_FILENAME)


def _limits_path() -> Path:
    # trading_limits_path is configured/resolved as an absolute path that may
    # legitimately live outside app_data_dir; anchor the traversal check to its
    # own parent directory so ``..``-escapes are rejected without breaking the
    # default location.
    configured = Path(get_settings().trading_limits_path)
    if ".." in configured.parts:
        raise ValueError(f"path traversal not allowed: {configured}")
    return _resolve_under_app_data(configured.parent, configured.name)


@router.post("/picks/{candidate_id}/toggle", response_model=TradingLiveSnapshotModel)
def trading_picks_toggle(
    request: Request, candidate_id: str, body: PickToggleRequestModel
) -> TradingLiveSnapshotModel:
    """Include or exclude a single candidate pick and return the live snapshot."""
    store = SelectionStore.load(_selections_path(request))
    store.set_selection(_board_date_today(), candidate_id, body.included)
    store.save(today=_board_date_today())
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(
        level="info",
        message=f"pick {candidate_id} {'included' if body.included else 'excluded'}",
    )
    publisher.notify()
    return _build_live_snapshot(request)


@router.post("/picks/bulk", response_model=TradingLiveSnapshotModel)
def trading_picks_bulk(
    request: Request, body: PickBulkRequestModel
) -> TradingLiveSnapshotModel:
    """Apply a bulk pick action (select/deselect/top-n) and return the snapshot."""
    snapshot = _build_live_snapshot(request)
    store = SelectionStore.load(_selections_path(request))
    today = _board_date_today()
    if body.action == "deselect_all":
        store.bulk_set(today, {row.candidate_id: False for row in snapshot.picks})
    elif body.action == "select_all_hittable":
        store.bulk_set(
            today,
            {row.candidate_id: True for row in snapshot.picks if row.state != "blocked"},
        )
    elif body.action == "top_n":
        n = body.n or 5
        ranked = sorted(
            (row for row in snapshot.picks if row.state != "blocked"),
            key=lambda r: r.edge_bps,
            reverse=True,
        )
        keep_ids = {row.candidate_id for row in ranked[:n]}
        store.bulk_set(
            today,
            {
                row.candidate_id: (row.candidate_id in keep_ids)
                for row in snapshot.picks
                if row.state != "blocked"
            },
        )
    store.save(today=today)
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(level="info", message=f"bulk pick action: {body.action}")
    publisher.notify()
    return _build_live_snapshot(request)


@router.post("/thresholds", response_model=TradingLiveSnapshotModel)
def trading_thresholds_update(
    request: Request, body: ThresholdsUpdateRequestModel
) -> TradingLiveSnapshotModel:
    """Update the hit/edge selection thresholds and return the live snapshot."""
    store = SelectionStore.load(_selections_path(request))
    store.update_thresholds(min_hit_pct=body.min_hit_pct, min_edge_bps=body.min_edge_bps)
    store.save(today=_board_date_today())
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(
        level="info",
        message=f"thresholds set: min_hit={body.min_hit_pct:.2f} min_edge={body.min_edge_bps}bp",
    )
    publisher.notify()
    return _build_live_snapshot(request)


@router.post("/limits", response_model=LimitsResponseModel)
def trading_limits_update(
    request: Request, body: LimitsUpdateRequestModel
) -> LimitsResponseModel:
    """Persist updated risk limits from the UI and return the new values."""
    path = _limits_path()
    existing: dict[str, Any] = (
        json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    )
    if body.max_open_notional is not None:
        existing["max_open_notional"] = body.max_open_notional
        existing["per_market_cap"] = round(body.max_open_notional / 2, 2)
    if body.daily_loss_cap is not None:
        existing["daily_loss_cap"] = body.daily_loss_cap
    if body.reject_cooldown_seconds is not None:
        existing["reject_cooldown_seconds"] = body.reject_cooldown_seconds
    if body.per_order_cap_override is not None:
        existing["per_order_cap_override"] = body.per_order_cap_override
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8")
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(level="info", message="limits updated via UI")
    publisher.notify()
    return LimitsResponseModel(
        max_open_notional=float(existing.get("max_open_notional", 0)),
        per_market_cap=float(existing.get("per_market_cap", 0)),
        daily_loss_cap=float(existing.get("daily_loss_cap", 0)),
        reject_cooldown_seconds=int(existing.get("reject_cooldown_seconds", 300)),
        per_order_cap_override=existing.get("per_order_cap_override"),
        wallet_init_done_at=existing.get("wallet_init_done_at"),
    )


@router.get("/limits", response_model=LimitsResponseModel)
def trading_limits_read(request: Request) -> LimitsResponseModel:
    """Return the currently configured risk limits."""
    path = _limits_path()
    existing: dict[str, Any] = (
        json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    )
    return LimitsResponseModel(
        max_open_notional=float(existing.get("max_open_notional", 0)),
        per_market_cap=float(existing.get("per_market_cap", 0)),
        daily_loss_cap=float(existing.get("daily_loss_cap", 0)),
        reject_cooldown_seconds=int(existing.get("reject_cooldown_seconds", 300)),
        per_order_cap_override=existing.get("per_order_cap_override"),
        wallet_init_done_at=existing.get("wallet_init_done_at"),
    )


@router.get("/wallet", response_model=WalletBalanceResponseModel)
def trading_wallet_balance(request: Request) -> WalletBalanceResponseModel:
    """Fetch the live Kalshi wallet balance for the trading UI."""
    settings = get_settings()
    if not (settings.kalshi_api_key_id and settings.kalshi_private_key_path):
        raise HTTPException(status_code=400, detail="Kalshi credentials not configured")
    private_key_path = Path(str(settings.kalshi_private_key_path))
    client = KalshiClient(
        api_key_id=settings.kalshi_api_key_id.get_secret_value() if settings.kalshi_api_key_id else "",
        private_key_path=private_key_path,
        base_url=settings.kalshi_base_url,
    )
    try:
        raw = client.get_balance()
        balance = extract_balance(raw)
    except Exception:
        logger.exception("Kalshi balance fetch failed")
        raise HTTPException(status_code=502, detail="Kalshi balance fetch failed") from None
    finally:
        client.close()
    return WalletBalanceResponseModel(balance=balance, fetched_at=datetime.now(UTC))


@router.post("/intent", response_model=TradingIntentResponseModel)
def trading_intent(request: Request, payload: TradingIntentRequestModel) -> TradingIntentResponseModel:
    """Evaluate and (if accepted) execute a manual trading intent."""
    ledger = _ledger(request)
    risk_engine = request.app.state.trading_risk
    adapter = request.app.state.trading_adapter
    exchange = str(getattr(request.app.state, "trading_exchange", "paper"))

    if exchange != "paper" and payload.confirm_live != "LIVE":
        raise HTTPException(status_code=400, detail='confirm_live="LIVE" required for live submissions')

    side = payload.side.strip().upper()
    if side not in {"OVER", "UNDER"}:
        raise HTTPException(status_code=422, detail="side must be 'over' or 'under'")
    signal_id = f"api-{uuid4().hex[:12]}"
    signal = Signal(
        signal_id=signal_id,
        created_at=datetime.now(UTC),
        market_key=payload.market,
        side=side,
        confidence="manual",
        edge=0.0,
        model_probability=0.5,
        line_value=float(payload.line),
        metadata={
            "game_id": payload.game_id if payload.game_id is not None else "na",
            "player_id": payload.player_id,
            "sportsbook_key": payload.sportsbook_key,
            "source": "api_trading_intent",
        },
    )
    intent = ExecutionIntent(
        intent_id=f"{signal_id}-intent",
        signal=signal,
        market=signal_to_market_ref(signal, exchange),
        side="buy",
        stake=float(payload.stake),
    )
    ok, reason = risk_engine.evaluate(intent, ledger)
    if not ok:
        ledger.record_order_event(
            OrderEvent(
                intent_id=intent.intent_id,
                event_type="rejected",
                status="blocked",
                message=reason,
            )
        )
        return TradingIntentResponseModel(
            accepted=False,
            intent_id=intent.intent_id,
            message=f"Trading intent blocked: {reason}.",
        )

    events, fills = adapter.place_order(intent)
    for event in events:
        ledger.record_order_event(event)
    for fill in fills:
        ledger.record_fill(fill)
    blocked = any(
        event.event_type == "rejected" or event.status in {"blocked", "failed"}
        for event in events
    )
    accepted = not blocked and bool(fills)
    if accepted:
        message = f"Trading intent accepted on {exchange}: {len(events)} event(s), {len(fills)} fill(s)."
    else:
        message = events[-1].message if events else f"No execution events returned by {exchange} adapter."
    return TradingIntentResponseModel(
        accepted=accepted,
        intent_id=intent.intent_id,
        message=message,
    )

