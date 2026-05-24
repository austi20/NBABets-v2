from __future__ import annotations

MARKET_TARGETS: dict[str, str] = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "turnovers": "turnovers",
    "pra": "pra",
}

ROLLING_WINDOWS = (3, 5, 10, 20)

ROLLING_WINDOW_RECENCY_WEIGHTS: dict[int, float] = {
    3: 0.25,
    5: 0.35,
    10: 0.30,
    20: 0.10,
}

