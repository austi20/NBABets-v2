from __future__ import annotations

from app.evaluation.no_vig import additive_no_vig, multiplicative_no_vig


def american_to_prob(american_odds: int | None) -> float:
    if american_odds is None or american_odds == 0:
        return 0.0
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    return abs(float(american_odds)) / (abs(float(american_odds)) + 100.0)


def prob_to_decimal(probability: float) -> float:
    clipped = min(max(float(probability), 1e-6), 1.0 - 1e-6)
    return 1.0 / clipped


def prob_to_clob_price(probability: float) -> int:
    clipped = min(max(float(probability), 0.01), 0.99)
    return int(round(clipped * 100.0))


def no_vig_over_probability(over_odds: int | None, under_odds: int | None) -> float:
    if over_odds is None and under_odds is None:
        return 0.5
    if over_odds is not None and under_odds is not None:
        try:
            over_prob, _ = multiplicative_no_vig(over_odds, under_odds)
            return float(over_prob)
        except (ValueError, ZeroDivisionError):
            over_prob, _ = additive_no_vig(over_odds, under_odds)
            return float(over_prob)
    return american_to_prob(over_odds if over_odds is not None else under_odds)
