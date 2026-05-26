from __future__ import annotations

import pytest

from app.training.distributions import DistributionSummary
from app.training.pipeline import _projection_boom_bust


def _summary(*, p10: float, p25: float, median: float, p75: float, p90: float) -> DistributionSummary:
    return DistributionSummary(
        mean=20.0,
        variance=25.0,
        median=median,
        p10=p10,
        p25=p25,
        p90=p90,
        p75=p75,
        over_probability=0.5,
        under_probability=0.5,
        ci_low=p10,
        ci_high=p90,
    )


def test_projection_boom_bust_uses_material_projection_thresholds_not_one_sigma_tails() -> None:
    boom, bust = _projection_boom_bust(
        _summary(p10=14.0, p25=18.0, median=20.0, p75=22.0, p90=35.0)
    )

    assert boom == pytest.approx(0.25)
    assert bust == pytest.approx(0.10)
    assert boom != pytest.approx(0.158655, abs=0.001)
    assert bust != pytest.approx(0.158655, abs=0.001)


def test_projection_boom_bust_changes_with_distribution_shape_for_same_mean() -> None:
    tight = _summary(p10=17.0, p25=19.0, median=20.0, p75=21.0, p90=23.0)
    volatile = _summary(p10=8.0, p25=15.0, median=20.0, p75=25.0, p90=34.0)

    tight_boom, tight_bust = _projection_boom_bust(tight)
    volatile_boom, volatile_bust = _projection_boom_bust(volatile)

    assert volatile_boom > tight_boom
    assert volatile_bust > tight_bust
