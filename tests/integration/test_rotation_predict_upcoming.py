from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from app.schemas.domain import PropPrediction
from app.training.distributions import DistributionSummary, _summarize_samples
from app.training.pipeline import (
    TrainingPipeline,
    _apply_dnp_correction,
    _availability_branch_context,
    _lookup_availability_context,
)

# ---------------------------------------------------------------------------
# Schema compatibility
# ---------------------------------------------------------------------------

def test_prop_prediction_accepts_phase6_additive_fields() -> None:
    prediction = PropPrediction(
        player_id=7,
        player_name="Player Seven",
        game_id=1007,
        market_key="points",
        sportsbook_line=24.5,
        projected_mean=26.1,
        projected_variance=18.0,
        projected_median=25.8,
        over_probability=0.56,
        under_probability=0.44,
        calibrated_over_probability=0.55,
        percentile_10=18.2,
        percentile_25=21.4,
        percentile_50=25.8,
        percentile_75=30.1,
        percentile_90=33.0,
        dnp_risk=0.15,
        boom_probability=0.33,
        bust_probability=0.18,
        availability_branches=4,
        confidence_interval_low=18.2,
        confidence_interval_high=33.0,
        top_features=["signal-1"],
        model_version="v1",
        feature_version="v1",
        data_freshness={"predicted_at": datetime.now(UTC)},
    )

    assert prediction.dnp_risk == 0.15
    assert prediction.availability_branches == 4
    assert prediction.boom_probability == 0.33
    assert prediction.bust_probability == 0.18
    assert prediction.percentile_25 == 21.4
    assert prediction.percentile_75 == 30.1


# ---------------------------------------------------------------------------
# DistributionSummary: real quartiles from samples
# ---------------------------------------------------------------------------

def test_summarize_samples_computes_real_p25_p75() -> None:
    rng = np.random.default_rng(0)
    samples = rng.normal(loc=20.0, scale=3.0, size=50_000)
    summary = _summarize_samples(samples, line=20.0)

    # For N(20,3): p25 ≈ 17.97, p75 ≈ 22.02
    assert 17.0 < summary.p25 < 19.5
    assert 20.5 < summary.p75 < 23.0
    assert summary.p25 < summary.median < summary.p75


def test_summarize_samples_boom_bust_from_samples() -> None:
    rng = np.random.default_rng(1)
    # Nearly all samples well above the line → high boom, low bust
    samples = rng.normal(loc=30.0, scale=2.0, size=20_000)
    summary = _summarize_samples(samples, line=20.0)

    # boom threshold = 22.0, almost all samples exceed it
    assert summary.boom_probability > 0.95
    # bust threshold = 14.0, almost no samples below it
    assert summary.bust_probability < 0.01


def test_summarize_samples_zero_line_no_boom() -> None:
    samples = np.array([5.0, 10.0, 15.0], dtype=float)
    summary = _summarize_samples(samples, line=0.0)
    # line=0 → boom threshold=inf, raw count = 0 so probability is exactly 0
    assert summary.boom_probability == 0.0


# ---------------------------------------------------------------------------
# DNP correction: mixture model correctness
# ---------------------------------------------------------------------------

def test_dnp_correction_zero_risk_is_identity() -> None:
    summary = DistributionSummary(
        mean=20.0, variance=9.0, median=20.0,
        p10=14.0, p25=17.0, p75=23.0, p90=26.0,
        over_probability=0.5, under_probability=0.5,
        ci_low=14.0, ci_high=26.0,
        boom_probability=0.4, bust_probability=0.05,
    )
    boom, bust, p25, p75 = _apply_dnp_correction(summary, dnp_risk=0.0, line=20.0)

    assert boom == pytest.approx(summary.boom_probability)
    assert bust == pytest.approx(summary.bust_probability)
    assert p25 == pytest.approx(summary.p25)
    assert p75 == pytest.approx(summary.p75)


def test_dnp_correction_bust_respects_dnp_floor() -> None:
    # With dnp_risk=0.30 and bust_threshold=line*0.70, a DNP (stat=0) is always a bust.
    # So bust_mixture >= dnp_risk regardless of the conditional distribution shape.
    rng = np.random.default_rng(42)
    samples = rng.normal(loc=20.0, scale=3.0, size=10_000)
    summary = _summarize_samples(samples, line=20.0)
    dnp_risk = 0.30

    _, bust, _, _ = _apply_dnp_correction(summary, dnp_risk=dnp_risk, line=20.0)

    assert bust >= dnp_risk - 1e-6
    assert 0.0 <= bust <= 1.0


def test_dnp_correction_boom_bounded_by_play_probability() -> None:
    # boom_mixture = p_play * P(boom | plays), so it can never exceed p_play.
    rng = np.random.default_rng(42)
    samples = rng.normal(loc=20.0, scale=3.0, size=10_000)
    summary = _summarize_samples(samples, line=20.0)
    dnp_risk = 0.30

    boom, _, _, _ = _apply_dnp_correction(summary, dnp_risk=dnp_risk, line=20.0)

    assert boom <= (1.0 - dnp_risk) + 1e-6
    assert 0.0 <= boom


def test_dnp_correction_high_risk_zeroes_p25() -> None:
    summary = DistributionSummary(
        mean=10.0, variance=4.0, median=10.0,
        p10=8.0, p25=9.0, p75=11.0, p90=12.0,
        over_probability=0.5, under_probability=0.5,
        ci_low=8.0, ci_high=12.0,
        boom_probability=0.2, bust_probability=0.1,
    )
    _, _, p25, _ = _apply_dnp_correction(summary, dnp_risk=0.40, line=10.0)
    # 40% DNP mass at 0 → combined p25 should be 0 (DNP mass covers the first quartile)
    assert p25 == 0.0


# ---------------------------------------------------------------------------
# Availability branch context: DNP risk monotone with play probability
# ---------------------------------------------------------------------------

def test_dnp_risk_increases_with_lower_play_probability() -> None:

    frame = pd.DataFrame([{"game_id": 1, "player_team_id": 10, "player_id": 101}])

    absences_high_risk = pd.DataFrame(
        [{"game_id": 1, "team_id": 10, "player_id": 101, "play_probability": 0.15}]
    )
    absences_low_risk = pd.DataFrame(
        [{"game_id": 1, "team_id": 10, "player_id": 101, "play_probability": 0.85}]
    )

    ctx_high = _availability_branch_context(frame, absences_high_risk, max_exact_players=8, sampled_branch_count=1000)
    ctx_low = _availability_branch_context(frame, absences_low_risk, max_exact_players=8, sampled_branch_count=1000)

    dnp_high = _lookup_availability_context(ctx_high, {"game_id": 1, "player_team_id": 10, "player_id": 101})["dnp_risk"]
    dnp_low = _lookup_availability_context(ctx_low, {"game_id": 1, "player_team_id": 10, "player_id": 101})["dnp_risk"]

    assert dnp_high > dnp_low


def test_branch_simulation_frames_disabled_when_rotation_off(monkeypatch) -> None:
    monkeypatch.setenv("ROTATION_SHOCK_ENABLED", "false")
    monkeypatch.setenv("ROTATION_SHOCK_ABLATION_MODE", "off")
    pipe = TrainingPipeline.__new__(TrainingPipeline)
    frame = pd.DataFrame({"game_id": [1], "player_team_id": [10]})

    assert pipe._build_branch_simulation_frames(
        frame=frame,
        absence_profiles=pd.DataFrame(),
        max_exact_players=8,
        sampled_branch_count=100,
    ) == {}


# ---------------------------------------------------------------------------
# Clean-roster parity: no uncertain players → defaults preserved
# ---------------------------------------------------------------------------

def test_clean_roster_dnp_correction_is_identity() -> None:
    rng = np.random.default_rng(7)
    samples = rng.normal(loc=25.0, scale=4.0, size=10_000)
    summary = _summarize_samples(samples, line=24.5)

    boom, bust, p25, p75 = _apply_dnp_correction(summary, dnp_risk=0.0, line=24.5)

    assert boom == pytest.approx(summary.boom_probability, abs=1e-9)
    assert bust == pytest.approx(summary.bust_probability, abs=1e-9)
    assert p25 == pytest.approx(summary.p25, abs=1e-9)
    assert p75 == pytest.approx(summary.p75, abs=1e-9)
