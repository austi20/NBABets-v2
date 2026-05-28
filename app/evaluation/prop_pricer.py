from __future__ import annotations

from typing import Any

from app.evaluation.no_vig import multiplicative_no_vig
from app.evaluation.prop_decision import PropDecision


def compute_ev(*, hit_prob: float, push_prob: float, decimal_odds: float) -> float:
    """EV = hit * profit - loss * stake. Push mass is excluded from loss."""
    loss_prob = max(0.0, 1.0 - hit_prob - push_prob)
    return hit_prob * (decimal_odds - 1.0) - loss_prob * 1.0


def price_prop(prediction: Any, line_snapshot: Any) -> PropDecision:
    over_odds = _read_value(line_snapshot, "over_odds")
    under_odds = _read_value(line_snapshot, "under_odds")
    market_key = str(_read_value(line_snapshot, "market_key") or "")
    line_value = float(_read_value(line_snapshot, "line_value") or 0.0)

    model_over = float(_read_value(prediction, "calibration_adjusted_probability") or 0.0)
    raw_under = _read_value(prediction, "under_probability")
    model_under = float(raw_under) if raw_under is not None else max(0.0, 1.0 - model_over)
    push_prob = max(0.0, 1.0 - model_over - model_under)

    # Short-circuit: no odds on either side => observe only, no edge
    if over_odds is None and under_odds is None:
        return PropDecision(
            model_prob=model_over,
            market_prob=None,
            no_vig_market_prob=None,
            ev=None,
            recommendation="observe_only",
            confidence=_confidence(model_over),
            driver="no_odds",
            market_key=market_key,
            line_value=line_value,
            over_odds=None,
            under_odds=None,
        )

    recommendation, hit_prob = _recommend_side(
        model_over=model_over,
        model_under=model_under,
        over_odds=over_odds,
        under_odds=under_odds,
    )
    market_prob, no_vig_market_prob = _market_probabilities(
        recommendation=recommendation,
        over_odds=over_odds,
        under_odds=under_odds,
    )
    decimal = _american_to_decimal(over_odds if recommendation == "OVER" else under_odds)
    ev = compute_ev(hit_prob=hit_prob, push_prob=push_prob, decimal_odds=decimal)
    edge = hit_prob - no_vig_market_prob

    # Downgrade to observe_only when EV is non-positive
    final_rec = recommendation if ev > 0.0 else "observe_only"

    return PropDecision(
        model_prob=hit_prob,
        market_prob=market_prob,
        no_vig_market_prob=no_vig_market_prob,
        ev=ev,
        recommendation=final_rec,
        confidence=_confidence(hit_prob),
        driver=f"edge_vs_no_vig={edge:.3f}",
        market_key=market_key,
        line_value=line_value,
        over_odds=_to_int_or_none(over_odds),
        under_odds=_to_int_or_none(under_odds),
    )


def _recommend_side(*, model_over: float, model_under: float, over_odds: Any, under_odds: Any) -> tuple[str, float]:
    candidates: list[tuple[str, float]] = []
    if over_odds is not None:
        candidates.append(("OVER", model_over))
    if under_odds is not None:
        candidates.append(("UNDER", model_under))
    if not candidates:
        return "OVER", model_over
    return max(candidates, key=lambda item: (item[1], item[0] == "OVER"))


def _market_probabilities(*, recommendation: str, over_odds: Any, under_odds: Any) -> tuple[float, float]:
    over_implied = _american_to_probability(over_odds)
    under_implied = _american_to_probability(under_odds)

    if over_odds is not None and under_odds is not None:
        over_no_vig, under_no_vig = multiplicative_no_vig(int(over_odds), int(under_odds))
    else:
        over_no_vig = over_implied
        under_no_vig = under_implied

    if recommendation == "OVER":
        return over_implied, over_no_vig
    return under_implied, under_no_vig


def _american_to_probability(odds: Any) -> float:
    if odds is None:
        return 0.5
    value = float(odds)
    if value > 0:
        return 100.0 / (value + 100.0)
    return abs(value) / (abs(value) + 100.0)


def _american_to_decimal(odds: Any) -> float:
    if odds is None:
        return 2.0
    value = float(odds)
    if value > 0:
        return 1.0 + value / 100.0
    return 1.0 + 100.0 / abs(value)


def _confidence(probability: float) -> str:
    if probability >= 0.67:
        return "high"
    if probability >= 0.60:
        return "medium"
    return "low"


def _read_value(payload: Any, key: str) -> Any:
    if isinstance(payload, dict):
        return payload.get(key)
    return getattr(payload, key, None)


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
