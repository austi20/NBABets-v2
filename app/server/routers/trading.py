from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.server.schemas.trading import (
    FillModel,
    PositionModel,
    TradingIntentRequestModel,
    TradingIntentResponseModel,
    TradingPnlModel,
)

router = APIRouter(prefix="/api/trading", tags=["trading"])


@router.get("/positions", response_model=list[PositionModel])
def trading_positions(request: Request) -> list[PositionModel]:
    ledger = request.app.state.trading_ledger
    return [PositionModel.from_dataclass(position) for position in ledger.open_positions()]


@router.get("/pnl", response_model=TradingPnlModel)
def trading_pnl(request: Request) -> TradingPnlModel:
    ledger = request.app.state.trading_ledger
    risk_engine = request.app.state.trading_risk
    return TradingPnlModel(
        daily_realized_pnl=ledger.daily_realized_pnl(),
        kill_switch_active=risk_engine.killed,
    )


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
    ledger = request.app.state.trading_ledger
    return TradingPnlModel(
        daily_realized_pnl=ledger.daily_realized_pnl(),
        kill_switch_active=risk_engine.killed,
    )


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

