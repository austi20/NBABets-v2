from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request

from app.db.models.trading import TradingKillSwitch
from app.server.schemas.trading import (
    ActiveLimitsModel,
    FillModel,
    PositionModel,
    TradingIntentRequestModel,
    TradingIntentResponseModel,
    TradingPnlModel,
)
from app.trading.loop import set_kill_switch
from app.trading.mapper import signal_to_market_ref
from app.trading.types import ExecutionIntent, OrderEvent, Signal

router = APIRouter(prefix="/api/trading", tags=["trading"])


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
    ledger = request.app.state.trading_ledger
    risk_engine = request.app.state.trading_risk
    return TradingPnlModel(
        daily_realized_pnl=ledger.daily_realized_pnl(),
        kill_switch_active=_kill_switch_active(request, bool(risk_engine.killed)),
        active_limits=_active_limits(risk_engine),
    )


@router.get("/positions", response_model=list[PositionModel])
def trading_positions(request: Request) -> list[PositionModel]:
    ledger = request.app.state.trading_ledger
    return [PositionModel.from_dataclass(position) for position in ledger.open_positions()]


@router.get("/pnl", response_model=TradingPnlModel)
def trading_pnl(request: Request) -> TradingPnlModel:
    return _pnl_model(request)


@router.get("/fills/recent", response_model=list[FillModel])
def trading_recent_fills(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[FillModel]:
    ledger = request.app.state.trading_ledger
    return [FillModel.from_dataclass(fill) for fill in ledger.recent_fills(limit=limit)]


@router.post("/kill-switch", response_model=TradingPnlModel)
def trading_kill_switch(request: Request) -> TradingPnlModel:
    risk_engine = request.app.state.trading_risk
    risk_engine.set_killed(True)
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is not None:
        set_kill_switch(factory, killed=True, set_by="api")
    return _pnl_model(request)


@router.post("/intent", response_model=TradingIntentResponseModel)
def trading_intent(request: Request, payload: TradingIntentRequestModel) -> TradingIntentResponseModel:
    ledger = request.app.state.trading_ledger
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

