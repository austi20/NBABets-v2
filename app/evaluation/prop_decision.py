from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PropDecision:
    model_prob: float
    market_prob: float
    no_vig_market_prob: float
    ev: float
    recommendation: str
    confidence: str
    driver: str
    market_key: str
    line_value: float
    over_odds: int | None
    under_odds: int | None
