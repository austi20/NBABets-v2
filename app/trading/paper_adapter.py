from __future__ import annotations

import random
import time
from datetime import UTC, datetime

from app.trading.protocols import ExchangeAdapter
from app.trading.types import ExecutionIntent, Fill, OrderEvent


class FakePaperAdapter(ExchangeAdapter):
    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        events = [
            OrderEvent(intent_id=intent.intent_id, event_type="accepted", status="ok", message="paper order accepted"),
        ]
        fill = Fill(
            fill_id=f"fill-{intent.intent_id}-1",
            intent_id=intent.intent_id,
            market=intent.market,
            side=intent.side,
            stake=float(intent.stake),
            price=0.5,
            fee=0.0,
            realized_pnl=0.0,
            timestamp=datetime.now(UTC),
        )
        events.append(
            OrderEvent(
                intent_id=intent.intent_id,
                event_type="filled",
                status="ok",
                message=f"filled stake={fill.stake:.2f}",
            )
        )
        return events, [fill]


class RealisticPaperAdapter(ExchangeAdapter):
    def __init__(
        self,
        *,
        seed: int = 7,
        min_latency_ms: int = 20,
        max_latency_ms: int = 90,
        slippage_bps: int = 40,
    ) -> None:
        self._rng = random.Random(seed)
        self._min_latency_ms = max(0, int(min_latency_ms))
        self._max_latency_ms = max(self._min_latency_ms, int(max_latency_ms))
        self._slippage_bps = max(0, int(slippage_bps))

    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        events: list[OrderEvent] = [
            OrderEvent(intent_id=intent.intent_id, event_type="accepted", status="ok", message="paper order accepted"),
        ]
        latency = self._rng.randint(self._min_latency_ms, self._max_latency_ms)
        if latency > 0:
            time.sleep(latency / 1000.0)

        first_chunk = round(float(intent.stake) * self._rng.uniform(0.45, 0.8), 4)
        second_chunk = round(max(float(intent.stake) - first_chunk, 0.0), 4)
        fills: list[Fill] = []
        for index, chunk in enumerate([first_chunk, second_chunk], start=1):
            if chunk <= 0:
                continue
            slip = self._rng.uniform(0.0, self._slippage_bps / 10_000.0)
            price = 0.5 + slip
            fills.append(
                Fill(
                    fill_id=f"fill-{intent.intent_id}-{index}",
                    intent_id=intent.intent_id,
                    market=intent.market,
                    side=intent.side,
                    stake=chunk,
                    price=price,
                    fee=round(chunk * 0.001, 4),
                    realized_pnl=0.0,
                    timestamp=datetime.now(UTC),
                )
            )
        events.append(
            OrderEvent(
                intent_id=intent.intent_id,
                event_type="filled",
                status="ok",
                message=f"partial fills={len(fills)}",
            )
        )
        return events, fills
