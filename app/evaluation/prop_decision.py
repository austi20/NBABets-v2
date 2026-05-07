from __future__ import annotations

from dataclasses import dataclass
from datetime import date


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
    player_id: int | None = None
    game_id: int | str | None = None
    game_date: date | str | None = None
