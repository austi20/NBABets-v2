from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.evaluation.prop_pricer import price_prop
from app.training.distributions import DistFamily, DistributionSummary, summarize_line_probability


@dataclass(frozen=True)
class LineEvaluationResult:
    line_value: float
    threshold_value: float | None
    over_probability: float
    under_probability: float
    push_probability: float
    calibration_adjusted_probability: float
    calibrated_under_probability: float
    fair_over_price: float
    fair_under_price: float
    market_probability: float
    no_vig_market_probability: float
    ev: float
    edge_bps: int
    recommendation: str
    hit_probability: float
    summary: DistributionSummary


def evaluate_projection_line(
    *,
    projected_mean: float,
    projected_variance: float,
    line_value: float,
    market_key: str,
    over_odds: int | None = None,
    under_odds: int | None = None,
    threshold_value: float | None = None,
    summary: DistributionSummary | None = None,
    summary_line_value: float | None = None,
    minutes_mean: float | None = None,
    minutes_std: float | None = None,
    context: dict[str, float] | None = None,
    simulations: int = 5000,
    dist_family: DistFamily = "legacy",
) -> LineEvaluationResult:
    """Price one tradeable line from a canonical stat projection.

    The model-owned output is the projected stat distribution. This helper keeps
    sportsbook odds, Kalshi prices, and arbitrary thresholds in the evaluation
    layer by deriving hit probabilities from the distribution at the supplied
    line.
    """

    line = float(line_value)
    can_reuse_summary = (
        summary is not None
        and summary_line_value is not None
        and abs(float(summary_line_value) - line) < 1e-9
    )
    dist_summary = summary if can_reuse_summary else summarize_line_probability(
        mean=float(projected_mean),
        variance=float(projected_variance),
        line=line,
        simulations=simulations,
        minutes_mean=minutes_mean,
        minutes_std=minutes_std,
        market_key=market_key,
        context=context,
        dist_family=dist_family,
    )
    over_probability = _clip_probability(dist_summary.over_probability)
    under_probability = _clip_probability(dist_summary.under_probability)
    push_probability = _clip_probability(1.0 - over_probability - under_probability)

    assert abs(over_probability + under_probability + push_probability - 1.0) < 1e-6, (
        f"probability partition does not sum to 1: "
        f"over={over_probability} under={under_probability} push={push_probability}"
    )

    # Calibration can still be layered on later, but the primary architecture is
    # projection CDF -> line probability.
    calibrated_over = over_probability
    calibrated_under = under_probability
    decision = price_prop(
        prediction={
            "calibration_adjusted_probability": calibrated_over,
            "under_probability": calibrated_under,
        },
        line_snapshot={
            "market_key": market_key,
            "line_value": line,
            "over_odds": over_odds,
            "under_odds": under_odds,
        },
    )
    edge_bps = int(round((decision.model_prob - (decision.no_vig_market_prob or 0.0)) * 10000))
    return LineEvaluationResult(
        line_value=line,
        threshold_value=float(threshold_value) if threshold_value is not None else None,
        over_probability=over_probability,
        under_probability=under_probability,
        push_probability=push_probability,
        calibration_adjusted_probability=calibrated_over,
        calibrated_under_probability=calibrated_under,
        fair_over_price=over_probability,
        fair_under_price=under_probability,
        market_probability=float(decision.market_prob or 0.0),
        no_vig_market_probability=float(decision.no_vig_market_prob or 0.0),
        ev=float(decision.ev or 0.0),
        edge_bps=edge_bps,
        recommendation=str(decision.recommendation),
        hit_probability=float(decision.model_prob),
        summary=dist_summary,
    )


def line_evaluation_meta(
    *,
    probability_source: str = "projection_distribution",
    samples_used: int | None = None,
    margin_of_error: float | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"probability_source": probability_source}
    if samples_used is not None:
        payload["simulation_samples"] = int(samples_used)
    if margin_of_error is not None:
        payload["simulation_margin_of_error"] = float(margin_of_error)
    if extra:
        payload.update(extra)
    return payload


def _clip_probability(value: float) -> float:
    return float(max(0.0, min(1.0, value)))
