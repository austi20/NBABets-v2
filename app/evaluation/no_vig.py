from __future__ import annotations


def multiplicative_no_vig(over_odds: int, under_odds: int) -> tuple[float, float]:
    over_implied = _american_to_probability(over_odds)
    under_implied = _american_to_probability(under_odds)
    total = over_implied + under_implied
    if total <= 0.0:
        raise ValueError("implied probability total must be positive")
    return over_implied / total, under_implied / total


def additive_no_vig(over_odds: int, under_odds: int) -> tuple[float, float]:
    over_implied = _american_to_probability(over_odds)
    under_implied = _american_to_probability(under_odds)
    vig = max(0.0, over_implied + under_implied - 1.0)
    if vig == 0.0:
        return over_implied, under_implied

    adjusted_over = max(0.0, over_implied - vig / 2.0)
    adjusted_under = max(0.0, under_implied - vig / 2.0)
    total = adjusted_over + adjusted_under
    if total <= 0.0:
        return 0.5, 0.5
    return adjusted_over / total, adjusted_under / total


def _american_to_probability(odds: int) -> float:
    value = float(odds)
    if value > 0:
        return 100.0 / (value + 100.0)
    return abs(value) / (abs(value) + 100.0)
