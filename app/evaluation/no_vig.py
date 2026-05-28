from __future__ import annotations


def multiplicative_no_vig(over_odds: int, under_odds: int) -> tuple[float, float]:
    over_implied = _american_to_probability(over_odds)
    under_implied = _american_to_probability(under_odds)
    total = over_implied + under_implied
    if total <= 0.0:
        raise ValueError("implied probability total must be positive")
    return over_implied / total, under_implied / total


def _american_to_probability(odds: int) -> float:
    value = float(odds)
    if value > 0:
        return 100.0 / (value + 100.0)
    return abs(value) / (abs(value) + 100.0)
