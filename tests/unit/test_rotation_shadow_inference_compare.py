from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.schemas.domain import PropPrediction
from app.services.rotation_shadow_compare import build_comparison_frame


def _pred(
    *,
    game_id: int = 1,
    player_id: int = 2,
    market_key: str = "points",
    line: float = 20.5,
    mean: float = 20.0,
    cal: float = 0.5,
) -> PropPrediction:
    return PropPrediction(
        player_id=player_id,
        player_name="Tester",
        game_id=game_id,
        market_key=market_key,
        sportsbook_line=line,
        projected_mean=mean,
        projected_variance=4.0,
        projected_median=mean,
        over_probability=cal,
        under_probability=1.0 - cal,
        calibrated_over_probability=cal,
        percentile_10=14.0,
        percentile_50=mean,
        percentile_90=26.0,
        confidence_interval_low=15.0,
        confidence_interval_high=25.0,
        top_features=["a"],
        model_version="t",
        feature_version="v",
        data_freshness={"predicted_at": datetime.now(UTC)},
        dnp_risk=0.1,
    )


def test_comparison_frame_empty_when_no_overlap() -> None:
    legacy = [_pred(mean=18.0, cal=0.4, market_key="points")]
    shadow = [_pred(mean=21.0, cal=0.55, market_key="rebounds")]
    frame = build_comparison_frame(legacy, shadow)
    assert frame.empty


def test_comparison_frame_does_not_pair_different_lines() -> None:
    legacy = [_pred(mean=18.0, cal=0.4, line=20.5)]
    shadow = [_pred(mean=21.0, cal=0.55, line=21.5)]
    frame = build_comparison_frame(legacy, shadow)
    assert frame.empty


def test_comparison_frame_pairs_matching_triples() -> None:
    legacy = [_pred(mean=18.0, cal=0.4)]
    shadow = [_pred(mean=21.0, cal=0.55)]
    frame = build_comparison_frame(legacy, shadow)
    assert len(frame) == 1
    assert frame.iloc[0]["delta_proj_mean"] == pytest.approx(3.0)
    assert frame.iloc[0]["delta_calibrated_over"] == pytest.approx(0.15)
    assert float(frame["delta_calibrated_over"].abs().mean()) == pytest.approx(0.15)
    assert float(frame["delta_proj_mean"].abs().mean()) == pytest.approx(3.0)
