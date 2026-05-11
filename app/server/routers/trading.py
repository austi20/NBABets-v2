from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request

from app.config.settings import get_settings
from app.db.models.trading import TradingKillSwitch
from app.providers.exchanges.kalshi_client import KalshiClient
from app.server.schemas.trading import (
    ActiveLimitsModel,
    ExchangePositionModel,
    FillModel,
    LivePositionModel,
    PositionModel,
    RestingOrderModel,
    TradingBrainCheckModel,
    TradingBrainSyncModel,
    TradingBrainSyncRequestModel,
    TradingIntentRequestModel,
    TradingIntentResponseModel,
    TradingLoopStartRequestModel,
    TradingLoopStatusModel,
    TradingPnlModel,
    TradingQuoteModel,
    TradingReadinessCheckModel,
    TradingReadinessModel,
    TradingSnapshotModel,
)
from app.server.services.board_cache import BoardCache
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
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.types import ExecutionIntent, OrderEvent, Signal

router = APIRouter(prefix="/api/trading", tags=["trading"])


def _ledger(request: Request) -> PortfolioLedger:
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is not None:
        return SqlPortfolioLedger(factory)
    return request.app.state.trading_ledger


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
    ledger = _ledger(request)
    return [PositionModel.from_dataclass(position) for position in ledger.open_positions()]


@router.get("/pnl", response_model=TradingPnlModel)
def trading_pnl(request: Request) -> TradingPnlModel:
    return _pnl_model(request)


@router.get("/snapshot", response_model=TradingSnapshotModel)
def trading_snapshot(request: Request) -> TradingSnapshotModel:
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
                api_key_id=settings.kalshi_api_key_id,
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
    settings = get_settings()
    with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as market_client:
        readiness = build_trading_readiness(settings=settings, market_client=market_client)
    return _readiness_model(readiness)


@router.get("/brain/status", response_model=TradingBrainSyncModel)
def trading_brain_status() -> TradingBrainSyncModel:
    settings = get_settings()
    try:
        result = load_last_brain_status(settings) or default_brain_status(settings)
    except Exception as exc:  # noqa: BLE001 - status should degrade into a visible API error
        raise HTTPException(status_code=500, detail=f"Decision brain status unavailable: {exc}") from exc
    return _brain_sync_model(result)


@router.post("/brain/sync", response_model=TradingBrainSyncModel)
def trading_brain_sync(request: Request, payload: TradingBrainSyncRequestModel) -> TradingBrainSyncModel:
    settings = get_settings()
    board_cache = getattr(request.app.state, "board_cache", None) or BoardCache()
    try:
        board_entry = board_cache.populate(payload.board_date)
    except Exception as exc:  # noqa: BLE001 - caller needs a clear setup failure
        raise HTTPException(status_code=500, detail=f"Could not build prop board for decision brain: {exc}") from exc
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
    ledger = _ledger(request)
    return [FillModel.from_dataclass(fill) for fill in ledger.recent_fills(limit=limit)]


@router.post("/kill-switch", response_model=TradingPnlModel)
def trading_kill_switch(request: Request) -> TradingPnlModel:
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
    controller = request.app.state.trading_loop_controller
    return _loop_status_model(controller.status())


@router.post("/loop/start", response_model=TradingLoopStatusModel)
def trading_loop_start(request: Request, payload: TradingLoopStartRequestModel | None = None) -> TradingLoopStatusModel:
    settings = get_settings()
    body = payload or TradingLoopStartRequestModel()
    board_cache = getattr(request.app.state, "board_cache", None) or BoardCache()
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is None:
        raise HTTPException(status_code=500, detail="Trading session factory is not configured.")
    try:
        board_entry = board_cache.populate(body.board_date)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Could not build prop board for loop start: {exc}") from exc
    controller = request.app.state.trading_loop_controller
    status = controller.start(
        settings=settings,
        board_entry=board_entry,
        board_date=body.board_date or board_entry.board_date,
        session_factory=factory,
    )
    return _loop_status_model(status)


@router.post("/intent", response_model=TradingIntentResponseModel)
def trading_intent(request: Request, payload: TradingIntentRequestModel) -> TradingIntentResponseModel:
    ledger = _ledger(request)
    risk_engine = request.app.state.trading_risk
    adapter = request.app.state.trading_adapter
    exchange = str(getattr(request.app.state, "trading_exchange", "paper"))

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

