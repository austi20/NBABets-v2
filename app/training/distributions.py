from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
from scipy.stats import nbinom

from app.config.settings import get_settings
from app.core.resources import get_runtime_budget

DistFamily = Literal["legacy", "count_aware", "decomposed"]


@dataclass(frozen=True)
class DistributionSummary:
    mean: float
    variance: float
    median: float
    p10: float
    p90: float
    over_probability: float
    under_probability: float
    ci_low: float
    ci_high: float
    p25: float = 0.0
    p75: float = 0.0
    boom_probability: float = 0.0
    bust_probability: float = 0.0


@dataclass(frozen=True)
class SimulationResult:
    summary: DistributionSummary
    samples_used: int
    margin_of_error: float


def fit_count_distribution(mean: float, variance: float) -> tuple[str, dict[str, float]]:
    bounded_mean = max(mean, 1e-6)
    bounded_variance = max(variance, bounded_mean + 1e-6)
    if bounded_variance <= bounded_mean * 1.05:
        return "poisson", {"mu": bounded_mean}
    size = bounded_mean**2 / (bounded_variance - bounded_mean)
    probability = size / (size + bounded_mean)
    return "negative_binomial", {"n": size, "p": probability}


def summarize_line_probability(
    mean: float,
    variance: float,
    line: float,
    simulations: int = 5000,
    *,
    minutes_mean: float | None = None,
    minutes_std: float | None = None,
    market_key: str | None = None,
    context: dict[str, float] | None = None,
    dist_family: DistFamily = "legacy",
) -> DistributionSummary:
    rng = np.random.default_rng(42)
    samples = _sample_market_distribution(
        mean=mean,
        variance=variance,
        sample_size=simulations,
        rng=rng,
        minutes_mean=minutes_mean,
        minutes_std=minutes_std,
        market_key=market_key,
        context=context,
        dist_family=dist_family,
    )
    return _summarize_samples(samples, line)


def sample_market_outcomes(
    *,
    mean: float,
    variance: float,
    sample_size: int,
    rng: np.random.Generator,
    minutes_mean: float | None = None,
    minutes_std: float | None = None,
    market_key: str | None = None,
    context: dict[str, float] | None = None,
    dist_family: DistFamily = "legacy",
) -> np.ndarray:
    """Draw ``sample_size`` i.i.d. outcome samples aligned with Monte Carlo inference."""
    return _sample_market_distribution(
        mean=mean,
        variance=variance,
        sample_size=sample_size,
        rng=rng,
        minutes_mean=minutes_mean,
        minutes_std=minutes_std,
        market_key=market_key,
        context=context,
        dist_family=dist_family,
    )


def distribution_summary_from_samples(samples: np.ndarray, line: float) -> DistributionSummary:
    return _summarize_samples(np.asarray(samples, dtype=float), line)


def empirical_pit_cdf(samples: np.ndarray, actual: float) -> float:
    """Mid-rank empirical CDF of ``actual`` under simulation ``samples`` (PIT uniforms if model is calibrated)."""
    vals = np.asarray(samples, dtype=float)
    total = vals.size
    if total <= 0:
        return 0.5
    lt = np.sum(vals < actual)
    eq = np.sum(vals == actual)
    pit = (lt + 0.5 * eq) / total
    return float(np.clip(pit, 1e-6, 1.0 - 1e-6))


def simulate_line_probability(
    mean: float,
    variance: float,
    line: float,
    *,
    minutes_mean: float | None,
    minutes_std: float | None,
    target_margin: float | None = None,
    min_samples: int | None = None,
    max_samples: int | None = None,
    batch_size: int | None = None,
    seed: int = 42,
    market_key: str | None = None,
    context: dict[str, float] | None = None,
    dist_family: DistFamily = "legacy",
) -> SimulationResult:
    settings = get_settings()
    budget = get_runtime_budget()
    min_samples = min_samples or settings.simulation_min_samples
    max_samples = min(max_samples or settings.simulation_max_samples, budget.simulation_max_samples)
    batch_size = min(batch_size or budget.simulation_batch_size, budget.simulation_batch_size)
    target_margin = target_margin or settings.simulation_target_margin

    rng = np.random.default_rng(seed)
    collected: list[np.ndarray] = []
    hits = 0
    total = 0
    margin = 1.0
    while total < max_samples:
        draw_count = min(batch_size, max_samples - total)
        samples = _sample_market_distribution(
            mean=mean,
            variance=variance,
            sample_size=draw_count,
            rng=rng,
            minutes_mean=minutes_mean,
            minutes_std=minutes_std,
            market_key=market_key,
            context=context,
            dist_family=dist_family,
        )
        collected.append(samples)
        hits += int(np.sum(samples > line))
        total += len(samples)
        if total < min_samples:
            continue
        probability = hits / total
        margin = 1.96 * np.sqrt(max(probability * (1.0 - probability), 1e-6) / total)
        if margin <= target_margin:
            break
    all_samples = np.concatenate(collected) if collected else np.zeros(1, dtype=float)
    return SimulationResult(
        summary=_summarize_samples(all_samples, line),
        samples_used=total,
        margin_of_error=margin,
    )


def simulate_joint_combo_probability(
    *,
    line: float,
    minutes_mean: float | None,
    minutes_std: float | None,
    component_inputs: dict[str, dict[str, Any]],
    combo_key: str = "pra",
    target_margin: float | None = None,
    min_samples: int | None = None,
    max_samples: int | None = None,
    batch_size: int | None = None,
    seed: int = 42,
    dist_family: DistFamily = "legacy",
) -> SimulationResult:
    settings = get_settings()
    budget = get_runtime_budget()
    min_samples = min_samples or settings.simulation_min_samples
    max_samples = min(max_samples or settings.simulation_max_samples, budget.simulation_max_samples)
    batch_size = min(batch_size or budget.simulation_batch_size, budget.simulation_batch_size)
    target_margin = target_margin or settings.simulation_target_margin

    rng = np.random.default_rng(seed)
    collected: list[np.ndarray] = []
    hits = 0
    total = 0
    margin = 1.0
    while total < max_samples:
        draw_count = min(batch_size, max_samples - total)
        samples = _sample_joint_combo_distribution(
            sample_size=draw_count,
            rng=rng,
            minutes_mean=minutes_mean,
            minutes_std=minutes_std,
            component_inputs=component_inputs,
            combo_key=combo_key,
            dist_family=dist_family,
        )
        collected.append(samples)
        hits += int(np.sum(samples > line))
        total += len(samples)
        if total < min_samples:
            continue
        probability = hits / total
        margin = 1.96 * np.sqrt(max(probability * (1.0 - probability), 1e-6) / total)
        if margin <= target_margin:
            break
    all_samples = np.concatenate(collected) if collected else np.zeros(1, dtype=float)
    return SimulationResult(
        summary=_summarize_samples(all_samples, line),
        samples_used=total,
        margin_of_error=margin,
    )


def _sample_joint_combo_distribution(
    *,
    sample_size: int,
    rng: np.random.Generator,
    minutes_mean: float | None,
    minutes_std: float | None,
    component_inputs: dict[str, dict[str, Any]],
    combo_key: str,
    dist_family: DistFamily,
) -> np.ndarray:
    safe_minutes_mean = max(minutes_mean or 32.0, 8.0)
    safe_minutes_std = max(minutes_std or np.sqrt(safe_minutes_mean) * 0.35, 1.0)
    minutes = np.clip(rng.normal(loc=safe_minutes_mean, scale=safe_minutes_std, size=sample_size), 6.0, 48.0)
    shared_game_latent = rng.lognormal(mean=0.0, sigma=0.12, size=sample_size)
    samples_by_component: dict[str, np.ndarray] = {}
    for index, (market_key, payload) in enumerate(component_inputs.items()):
        market_latent = np.exp(0.65 * np.log(shared_game_latent) + 0.35 * rng.normal(0.0, 0.10 + 0.02 * index, size=sample_size))
        context = dict(payload.get("context") or {})
        context["shared_latent"] = market_latent
        samples_by_component[market_key] = _sample_market_distribution(
            mean=float(payload["mean"]),
            variance=float(payload["variance"]),
            sample_size=sample_size,
            rng=rng,
            minutes_mean=safe_minutes_mean,
            minutes_std=safe_minutes_std,
            market_key=market_key,
            context=context,
            sampled_minutes=minutes,
            dist_family=dist_family,
        )
    if combo_key == "pra":
        return (
            samples_by_component.get("points", 0.0)
            + samples_by_component.get("rebounds", 0.0)
            + samples_by_component.get("assists", 0.0)
        ).astype(float)
    total = np.zeros(sample_size, dtype=float)
    for values in samples_by_component.values():
        total = total + values
    return total


def _sample_market_distribution(
    *,
    mean: float,
    variance: float,
    sample_size: int,
    rng: np.random.Generator,
    minutes_mean: float | None,
    minutes_std: float | None,
    market_key: str | None,
    context: dict[str, float] | None,
    sampled_minutes: np.ndarray | None = None,
    dist_family: DistFamily = "legacy",
) -> np.ndarray:
    bounded_mean = max(mean, 1e-6)
    bounded_variance = max(variance, bounded_mean + 1e-6)
    if sampled_minutes is None:
        sampled_minutes = _sample_minutes(sample_size, rng, minutes_mean, minutes_std)
    if sampled_minutes.size and float(np.nanmax(sampled_minutes)) <= 0.0:
        return np.zeros(sample_size, dtype=float)
    if dist_family == "count_aware":
        return _sample_count_aware_distribution(
            bounded_mean,
            bounded_variance,
            sampled_minutes,
            sample_size,
            rng,
            context,
        )
    # "decomposed" currently shares the same decomposition pathway as legacy.
    if market_key == "threes":
        return _sample_threes(bounded_mean, sampled_minutes, sample_size, rng, context)
    if market_key == "rebounds":
        return _sample_rebounds(bounded_mean, bounded_variance, sampled_minutes, sample_size, rng, context)
    if market_key == "assists":
        return _sample_assists(bounded_mean, bounded_variance, sampled_minutes, sample_size, rng, context)
    if market_key == "turnovers":
        return _sample_turnovers(bounded_mean, bounded_variance, sampled_minutes, sample_size, rng, context)
    if market_key == "points":
        return _sample_points(bounded_mean, bounded_variance, sampled_minutes, sample_size, rng, context)
    return _sample_generic_count(
        bounded_mean,
        bounded_variance,
        sampled_minutes,
        sample_size,
        rng,
        context,
    )


def _sample_minutes(
    sample_size: int,
    rng: np.random.Generator,
    minutes_mean: float | None,
    minutes_std: float | None,
) -> np.ndarray:
    if minutes_mean is None:
        return np.full(sample_size, 30.0, dtype=float)
    if minutes_mean <= 0:
        return np.zeros(sample_size, dtype=float)
    safe_minutes_mean = max(minutes_mean, 1.0)
    safe_minutes_std = max(minutes_std or np.sqrt(safe_minutes_mean) * 0.5, 1.0)
    return np.clip(rng.normal(loc=safe_minutes_mean, scale=safe_minutes_std, size=sample_size), 6.0, 48.0)


def _sample_generic_count(
    mean: float,
    variance: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    base_minutes = max(float(np.mean(minutes)), 1.0)
    shared_latent = _context_array(context, "shared_latent", 1.0, sample_size)
    adjusted_mean = np.clip(mean * (minutes / base_minutes) * shared_latent, 1e-6, None)
    family, params = fit_count_distribution(mean, variance)
    if family == "poisson" or mean < 0.5:
        return rng.poisson(lam=adjusted_mean).astype(float)
    dispersion = max((variance - mean) / max(mean**2, 1e-6), 0.0)
    if dispersion <= 1e-6:
        return rng.poisson(lam=adjusted_mean).astype(float)
    size = max(1.0 / dispersion, 1e-3)
    probability = np.clip(size / (size + adjusted_mean), 1e-6, 1.0 - 1e-6)
    return nbinom(n=size, p=probability).rvs(size=sample_size, random_state=rng).astype(float)


def _sample_count_aware_distribution(
    mean: float,
    variance: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    base_minutes = max(float(np.mean(minutes)), 1.0)
    shared_latent = _context_array(context, "shared_latent", 1.0, sample_size)
    adjusted_mean = np.clip(mean * (minutes / base_minutes) * shared_latent, 1e-6, None)
    family, params = fit_count_distribution(mean, variance)
    if family == "poisson":
        return rng.poisson(lam=adjusted_mean).astype(float)
    size = max(float(params.get("n", 1.0)), 1e-3)
    probability = np.clip(size / (size + adjusted_mean), 1e-6, 1.0 - 1e-6)
    return nbinom(n=size, p=probability).rvs(size=sample_size, random_state=rng).astype(float)


def _sample_threes(
    mean: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    raw_attempt_rate = _context_value(context, "estimated_three_point_attempts_per_minute", np.nan)
    raw_make_rate = _context_value(context, "three_point_make_rate", np.nan)
    if not _is_positive_finite(raw_attempt_rate) or not _is_positive_finite(raw_make_rate):
        return _sample_generic_count(mean, mean + max(mean * 0.75, 1.0), minutes, sample_size, rng, context)
    attempt_rate = max(raw_attempt_rate, 0.02)
    three_share = np.clip(_context_value(context, "percentage_field_goals_attempted_3pt", 0.35), 0.05, 0.85)
    make_rate = np.clip(raw_make_rate, 0.12, 0.65)
    shared_latent = np.maximum(_context_array(context, "shared_latent", 1.0, sample_size), 0.6)
    base_attempt_mean = np.clip(minutes * attempt_rate * shared_latent * (0.75 + 0.5 * three_share), 0.0, None)
    expected_makes = base_attempt_mean * make_rate
    scaling = _mean_anchor_multiplier(mean, expected_makes)
    attempts_mean = np.clip(base_attempt_mean * scaling, 0.0, None)
    attempts = rng.poisson(lam=attempts_mean).astype(int)
    return rng.binomial(attempts, make_rate).astype(float)


def _sample_rebounds(
    mean: float,
    variance: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    raw_chances_rate = _context_value(context, "rebound_chances_total_per_minute", np.nan)
    raw_conversion = _context_value(context, "rebound_conversion_rate", np.nan)
    if not _is_positive_finite(raw_chances_rate) or not _is_positive_finite(raw_conversion):
        return _sample_generic_count(mean, variance, minutes, sample_size, rng, context)
    chances_rate = max(raw_chances_rate, 0.05)
    conversion = np.clip(raw_conversion, 0.05, 0.95)
    shared_latent = np.maximum(_context_array(context, "shared_latent", 1.0, sample_size), 0.5)
    volatility = np.clip(_context_value(context, "rebounds_std_10", 0.0), 0.0, 8.0)
    rebound_noise = rng.lognormal(mean=0.0, sigma=min(0.30, 0.08 + 0.02 * volatility), size=sample_size)
    base_chance_mean = np.clip(minutes * chances_rate * shared_latent, 0.0, None)
    expected_rebounds = base_chance_mean * conversion
    scaling = _mean_anchor_multiplier(mean, expected_rebounds)
    chance_mean = np.clip(base_chance_mean * scaling * rebound_noise, 0.0, None)
    chances = _draw_count(chance_mean, variance, sample_size, rng)
    return rng.binomial(chances.astype(int), conversion).astype(float)


def _sample_assists(
    mean: float,
    variance: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    raw_opp_rate = _context_value(context, "assist_creation_proxy_per_minute", np.nan)
    raw_conversion = _context_value(context, "assist_conversion_rate", np.nan)
    if not _is_positive_finite(raw_opp_rate) or not _is_positive_finite(raw_conversion):
        return _sample_generic_count(mean, variance, minutes, sample_size, rng, context)
    opp_rate = max(raw_opp_rate, 0.03)
    conversion = np.clip(raw_conversion, 0.08, 0.65)
    passes_rate = max(_context_value(context, "passes_per_minute", opp_rate * 6.0), 0.5)
    shared_latent = np.maximum(_context_array(context, "shared_latent", 1.0, sample_size), 0.55)
    base_opportunity_mean = np.clip(minutes * (0.55 * opp_rate + 0.45 * passes_rate / 8.0) * shared_latent, 0.0, None)
    expected_assists = base_opportunity_mean * conversion
    scaling = _mean_anchor_multiplier(mean, expected_assists)
    opportunity_mean = np.clip(base_opportunity_mean * scaling, 0.0, None)
    opportunities = _draw_count(opportunity_mean, variance, sample_size, rng)
    return rng.binomial(opportunities.astype(int), conversion).astype(float)


def _sample_turnovers(
    mean: float,
    variance: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    # v1.2.2 Step 2: Zero-inflation hurdle model.
    # Turnovers are zero-inflated (many zero-TO games exist even for high-usage
    # players). The old pure-binomial model over-predicted non-zero counts,
    # driving ECE to ~0.282. We now apply a Poisson-derived zero-mass spike on
    # top of the base draw so the 0-turnover bin is correctly weighted.
    # p_zero = P(X=0 | Poisson(lambda=mean)), which equals exp(-mean).
    # Each simulated game is independently zeroed with that probability before
    # the binomial draw, reducing systematic over-confidence in over lines.
    raw_touches_rate = _context_value(context, "touches_per_minute", np.nan)
    raw_turnover_ratio = _context_value(context, "turnover_ratio", np.nan)
    if not _is_positive_finite(raw_touches_rate) or not _is_positive_finite(raw_turnover_ratio):
        base_samples = _sample_generic_count(mean, variance, minutes, sample_size, rng, context)
    else:
        touches_rate = max(raw_touches_rate, 0.5)
        turnover_ratio = np.clip(raw_turnover_ratio, 0.02, 0.35)
        usage_rate = np.clip(_context_value(context, "usage_rate", 0.22), 0.08, 0.45)
        shared_latent = np.maximum(_context_array(context, "shared_latent", 1.0, sample_size), 0.6)
        base_event_mean = np.clip(minutes * touches_rate * shared_latent * (0.45 + usage_rate), 0.0, None)
        expected_turnovers = base_event_mean * turnover_ratio
        scaling = _mean_anchor_multiplier(mean, expected_turnovers)
        event_mean = np.clip(base_event_mean * scaling, 0.0, None)
        events = _draw_count(event_mean, variance * 1.25, sample_size, rng)
        base_samples = rng.binomial(events.astype(int), turnover_ratio).astype(float)

    # Apply zero-inflation: Poisson zero-mass for a player with this mean TO rate.
    # Clamp mean to [0.1, 5.0] so p_zero stays in a sensible range (0.007–0.905).
    p_zero = float(np.exp(-max(min(mean, 5.0), 0.1)))
    zero_mask = rng.random(sample_size) < p_zero
    return np.where(zero_mask, 0.0, base_samples)


def _sample_points(
    mean: float,
    variance: float,
    minutes: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
    context: dict[str, float] | None,
) -> np.ndarray:
    raw_shot_rate = _context_value(context, "field_goal_attempts_per_minute", np.nan)
    raw_three_rate = _context_value(context, "estimated_three_point_attempts_per_minute", np.nan)
    raw_free_throw_rate = _context_value(context, "free_throw_attempts_per_minute", np.nan)
    if (
        not _is_positive_finite(raw_shot_rate)
        or not _is_positive_finite(raw_three_rate)
        or not _is_positive_finite(raw_free_throw_rate)
    ):
        return _sample_generic_count(mean, variance, minutes, sample_size, rng, context)
    shot_rate = max(raw_shot_rate, 0.08)
    three_rate = max(raw_three_rate, 0.01)
    free_throw_rate = max(raw_free_throw_rate, 0.02)
    two_point_rate = np.maximum(shot_rate - three_rate, 0.02)
    shared_latent = np.maximum(_context_value(context, "shared_latent", 1.0), 0.6)
    usage_rate = np.clip(_context_value(context, "usage_rate", 0.22), 0.08, 0.45)
    true_shooting = np.clip(_context_value(context, "true_shooting_percentage", 0.58), 0.35, 0.80)
    three_make = np.clip(_context_value(context, "three_point_make_rate", 0.36), 0.18, 0.60)
    two_make = np.clip((true_shooting * 2.0) - 0.45, 0.35, 0.72)
    ft_make = np.clip(_context_value(context, "free_throw_make_rate", 0.78), 0.55, 0.95)
    pace_exposure = np.clip(_context_value(context, "points_pace_exposure", 0.0), 0.0, 120.0)
    threes_variance = np.clip(_context_value(context, "points_3pt_variance", 0.0), 0.0, 16.0)
    pace_factor = np.clip(0.92 + (pace_exposure / 220.0), 0.85, 1.25)
    volatility_sigma = float(np.clip(0.08 + 0.015 * threes_variance, 0.08, 0.35))

    shared_latent = np.maximum(_context_array(context, "shared_latent", 1.0, sample_size), 0.6)
    pace_noise = rng.lognormal(mean=0.0, sigma=volatility_sigma, size=sample_size)
    multiplier = shared_latent * (0.70 + usage_rate) * pace_factor * pace_noise
    two_attempts = rng.poisson(lam=np.clip(minutes * two_point_rate * multiplier, 0.0, None))
    three_attempts = rng.poisson(lam=np.clip(minutes * three_rate * multiplier, 0.0, None))
    free_throws = rng.poisson(lam=np.clip(minutes * free_throw_rate * multiplier, 0.0, None))
    made_twos = rng.binomial(two_attempts, two_make)
    made_threes = rng.binomial(three_attempts, three_make)
    made_free_throws = rng.binomial(free_throws, ft_make)
    points = (2 * made_twos) + (3 * made_threes) + made_free_throws
    sim_mean = np.mean(points)
    if sim_mean <= 0:
        return _sample_generic_count(mean, variance, minutes, sample_size, rng, context)
    # Apply only a very small correction (±5%) to anchor the simulation near the model mean
    # without forcing an exact match. Forcing scale=mean/sim_mean was inflating raw probabilities
    # and collapsing the calibrator by training it to map 80-90% raw → 50% calibrated.
    scale = np.clip(mean / max(sim_mean, 1e-6), 0.95, 1.05)
    return np.round(points.astype(float) * scale, 0)


def _draw_count(mean: np.ndarray, variance: float, sample_size: int, rng: np.random.Generator) -> np.ndarray:
    scalar_mean = max(float(np.mean(mean)), 1e-6)
    scalar_variance = max(variance, scalar_mean + 1e-6)
    family, params = fit_count_distribution(scalar_mean, scalar_variance)
    if family == "poisson":
        return rng.poisson(lam=np.clip(mean, 1e-6, None)).astype(float)
    dispersion = max((scalar_variance - scalar_mean) / max(scalar_mean**2, 1e-6), 0.0)
    if dispersion <= 1e-6:
        return rng.poisson(lam=np.clip(mean, 1e-6, None)).astype(float)
    size = max(1.0 / dispersion, 1e-3)
    probability = np.clip(size / (size + np.clip(mean, 1e-6, None)), 1e-6, 1.0 - 1e-6)
    return nbinom(n=size, p=probability).rvs(size=sample_size, random_state=rng).astype(float)


def _context_value(context: dict[str, float] | None, key: str, fallback: float) -> float:
    if context is None:
        return float(fallback)
    value = context.get(key, fallback)
    try:
        return float(value)
    except Exception:
        return float(fallback)


def _context_array(
    context: dict[str, Any] | None,
    key: str,
    fallback: float,
    sample_size: int,
) -> np.ndarray:
    if context is None or key not in context:
        return np.full(sample_size, float(fallback), dtype=float)
    value = context[key]
    if isinstance(value, np.ndarray):
        return value.astype(float)
    try:
        return np.full(sample_size, float(value), dtype=float)
    except Exception:
        return np.full(sample_size, float(fallback), dtype=float)


def _is_positive_finite(value: float) -> bool:
    return bool(np.isfinite(value) and value > 0.0)


def _summarize_samples(samples: np.ndarray, line: float) -> DistributionSummary:
    samples = np.asarray(samples, dtype=float)
    total = max(len(samples), 1)
    over_hits = int(np.sum(samples > line))
    under_hits = int(np.sum(samples < line))
    boom_threshold = line * 1.10 if line > 0 else float("inf")
    bust_threshold = line * 0.70 if line > 0 else 0.0
    boom_hits = int(np.sum(samples >= boom_threshold)) if np.isfinite(boom_threshold) else 0
    bust_hits = int(np.sum(samples <= bust_threshold))
    return DistributionSummary(
        mean=float(np.mean(samples)),
        variance=float(np.var(samples)),
        median=float(np.quantile(samples, 0.5)),
        p10=float(np.quantile(samples, 0.1)),
        p25=float(np.quantile(samples, 0.25)),
        p90=float(np.quantile(samples, 0.9)),
        p75=float(np.quantile(samples, 0.75)),
        over_probability=_posterior_probability(over_hits, total),
        under_probability=_posterior_probability(under_hits, total),
        ci_low=float(np.quantile(samples, 0.1)),
        ci_high=float(np.quantile(samples, 0.9)),
        boom_probability=float(boom_hits / total),
        bust_probability=float(bust_hits / total),
    )


def _posterior_probability(successes: int, total: int) -> float:
    return float((successes + 0.5) / (total + 1.0))


def _mean_anchor_multiplier(target_mean: float, expected_values: np.ndarray) -> float:
    observed_mean = max(float(np.mean(expected_values)), 1e-6)
    anchored = max(float(target_mean), 1e-6) / observed_mean
    return float(np.clip(anchored, 0.25, 4.0))
