from __future__ import annotations

from fastapi import APIRouter, Query, Request

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
def trading_intent_stub(payload: TradingIntentRequestModel) -> TradingIntentResponseModel:
    return TradingIntentResponseModel(
        accepted=False,
        intent_id=None,
        message=(
            "Trading intent stub received for "
            f"{payload.market} {payload.line:.1f} ({payload.side}). "
            "Execution wiring lands in T5."
        ),
    )

