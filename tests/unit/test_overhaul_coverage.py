"""Tests for engine overhaul phases 2-5: population priors, tier-aware priors,
confidence propagation, trade/injury detection, and role bucket classification."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app.training.data_sufficiency import (
    _downgrade_tier,
    annotate_tiers,
    classify_data_sufficiency_tier,
)
from app.training.features import FeatureEngineer, role_bucket_label, role_prior_flag_series

# ---------------------------------------------------------------------------
# Phase 1 boundary tests: data sufficiency tiers
# ---------------------------------------------------------------------------


class TestDataSufficiencyBoundaries:
    """Exact boundary conditions for tier classification thresholds."""

    def test_tier_a_exact_boundary(self) -> None:
        # 10 games, 100 min, 12 avg = minimum A
        assert classify_data_sufficiency_tier(10, 100.0, 12.0) == "A"

    def test_tier_a_below_games_boundary(self) -> None:
        assert classify_data_sufficiency_tier(9, 100.0, 12.0) == "B"

    def test_tier_a_below_minutes_boundary(self) -> None:
        assert classify_data_sufficiency_tier(10, 99.9, 12.0) == "B"

    def test_tier_a_below_recent_avg_boundary(self) -> None:
        # Games and total minutes qualify for A, but recent avg too low → B
        assert classify_data_sufficiency_tier(10, 100.0, 11.9) == "B"

    def test_tier_b_exact_boundary(self) -> None:
        assert classify_data_sufficiency_tier(5, 50.0, 20.0) == "B"

    def test_tier_b_below_games_boundary(self) -> None:
        assert classify_data_sufficiency_tier(4, 50.0, 20.0) == "C"

    def test_tier_b_below_minutes_boundary(self) -> None:
        # 5 games but only 49 total minutes → C
        assert classify_data_sufficiency_tier(5, 49.9, 20.0) == "C"

    def test_tier_d_zero_games(self) -> None:
        assert classify_data_sufficiency_tier(0, 0.0, 0.0) == "D"

    def test_tier_d_negative_games(self) -> None:
        assert classify_data_sufficiency_tier(-1, 0.0, 0.0) == "D"


class TestTeamChangeDowngrade:
    """Trade/team-change detection downgrades tiers by one level."""

    def test_a_downgrades_to_b(self) -> None:
        assert classify_data_sufficiency_tier(15, 420.0, 31.5, team_changed=True) == "B"

    def test_b_downgrades_to_c(self) -> None:
        assert classify_data_sufficiency_tier(7, 160.0, 24.0, team_changed=True) == "C"

    def test_c_downgrades_to_d(self) -> None:
        assert classify_data_sufficiency_tier(3, 54.0, 18.0, team_changed=True) == "D"

    def test_d_stays_d(self) -> None:
        # Can't downgrade below D
        assert classify_data_sufficiency_tier(0, 0.0, 0.0, team_changed=True) == "D"

    def test_downgrade_helper(self) -> None:
        assert _downgrade_tier("A") == "B"
        assert _downgrade_tier("B") == "C"
        assert _downgrade_tier("C") == "D"
        assert _downgrade_tier("D") == "D"


class TestAnnotateTiersPreservesRows:
    """annotate_tiers must never drop rows (never-drop-players principle)."""

    def test_empty_upcoming(self) -> None:
        upcoming = pd.DataFrame(columns=["player_id", "game_id"])
        historical = pd.DataFrame(
            [{"player_id": 1, "game_id": 1, "minutes": 30.0, "game_date": "2026-01-01"}]
        )
        result = annotate_tiers(upcoming, historical)
        assert len(result) == 0
        assert "_data_sufficiency_tier" in result.columns

    def test_empty_historical_gives_all_d(self) -> None:
        upcoming = pd.DataFrame(
            [{"player_id": 1, "game_id": 100}, {"player_id": 2, "game_id": 100}]
        )
        historical = pd.DataFrame(columns=["player_id", "game_id", "minutes", "game_date"])
        result = annotate_tiers(upcoming, historical)
        assert len(result) == 2
        assert list(result["_data_sufficiency_tier"]) == ["D", "D"]

    def test_player_with_no_history_gets_d(self) -> None:
        upcoming = pd.DataFrame(
            [{"player_id": 1, "game_id": 100}, {"player_id": 999, "game_id": 100}]
        )
        historical = pd.DataFrame(
            [{"player_id": 1, "game_id": i, "minutes": 30.0, "game_date": f"2026-03-{i:02d}"}
             for i in range(1, 12)]
        )
        historical["game_date"] = pd.to_datetime(historical["game_date"])
        result = annotate_tiers(upcoming, historical)
        tiers = dict(zip(result["player_id"], result["_data_sufficiency_tier"], strict=True))
        assert tiers[1] == "A"
        assert tiers[999] == "D"


# ---------------------------------------------------------------------------
# Phase 2: role bucket labels + population priors
# ---------------------------------------------------------------------------


class TestRoleBucketLabel:
    def test_guard_starter(self) -> None:
        assert role_bucket_label("G", 1.0) == "G_starter"

    def test_forward_bench(self) -> None:
        assert role_bucket_label("F", 0.0) == "F_bench"

    def test_center_starter(self) -> None:
        assert role_bucket_label("C", 1.0) == "C_starter"

    def test_none_position_defaults_to_unk(self) -> None:
        assert role_bucket_label(None, 0.0) == "U_bench"

    def test_empty_position_defaults_to_unk(self) -> None:
        assert role_bucket_label("", 0.0) == "U_bench"

    def test_threshold_at_half(self) -> None:
        # 0.5 is the boundary: >= 0.5 is starter
        assert role_bucket_label("G", 0.5) == "G_starter"
        assert role_bucket_label("G", 0.49) == "G_bench"


class TestRolePriorFlagSeries:
    def test_uses_starter_flag_when_available(self) -> None:
        frame = pd.DataFrame({"starter_flag": [1.0, 0.0, 1.0], "minutes": [30, 30, 30]})
        result = role_prior_flag_series(frame)
        assert list(result) == [1.0, 0.0, 1.0]

    def test_falls_back_to_minutes_when_no_starter_data(self) -> None:
        frame = pd.DataFrame({"starter_flag": [0.0, 0.0, 0.0], "minutes": [30, 15, 25]})
        result = role_prior_flag_series(frame)
        # 30 >= 24 → 1.0, 15 < 24 → 0.0, 25 >= 24 → 1.0
        assert list(result) == [1.0, 0.0, 1.0]


class TestBuildPopulationPriors:
    def test_returns_structured_priors(self) -> None:
        engineer = FeatureEngineer()
        frame = pd.DataFrame({
            "position_group": ["G", "G", "F", "F"],
            "starter_flag": [1.0, 0.0, 1.0, 0.0],
            "minutes": [32.0, 18.0, 30.0, 15.0],
            "points_avg_10": [22.0, 8.0, 20.0, 6.0],
            "rebounds_avg_10": [3.0, 2.0, 8.0, 5.0],
        })
        priors = engineer.build_population_priors(frame, ["points_avg_10", "rebounds_avg_10"])

        assert "global_feature_priors" in priors
        assert "position_feature_priors" in priors
        assert "role_feature_priors" in priors
        assert "role_bucket_thresholds" in priors

        # Global priors are means across all rows
        assert priors["global_feature_priors"]["points_avg_10"] == pytest.approx(14.0)
        assert priors["global_feature_priors"]["rebounds_avg_10"] == pytest.approx(4.5)

        # Position priors are grouped by position
        assert "G" in priors["position_feature_priors"]
        assert "F" in priors["position_feature_priors"]
        assert priors["position_feature_priors"]["G"]["points_avg_10"] == pytest.approx(15.0)
        assert priors["position_feature_priors"]["F"]["points_avg_10"] == pytest.approx(13.0)

        # Role priors are grouped by role bucket
        assert "G_starter" in priors["role_feature_priors"]
        assert "F_bench" in priors["role_feature_priors"]

    def test_empty_columns_returns_empty_priors(self) -> None:
        engineer = FeatureEngineer()
        frame = pd.DataFrame({"position_group": ["G"], "starter_flag": [1.0], "minutes": [30.0]})
        priors = engineer.build_population_priors(frame, ["nonexistent_col"])
        assert priors["global_feature_priors"] == {}


class TestFillWithPopulationPriors:
    def test_tier_d_gets_all_columns_filled(self) -> None:
        engineer = FeatureEngineer()
        frame = pd.DataFrame({
            "position_group": ["G"],
            "_data_sufficiency_tier": ["D"],
            "points_avg_10": [np.nan],
            "rebounds_avg_10": [np.nan],
        })
        priors = {
            "global_feature_priors": {"points_avg_10": 15.0, "rebounds_avg_10": 5.0},
            "position_feature_priors": {"G": {"points_avg_10": 18.0, "rebounds_avg_10": 3.5}},
            "role_feature_priors": {},
            "role_bucket_thresholds": {},
        }
        result = engineer._fill_with_population_priors(
            frame, priors, "_data_sufficiency_tier", ["points_avg_10", "rebounds_avg_10"]
        )
        # Tier D: uses position priors (G) since no role priors available
        assert result.at[0, "points_avg_10"] == pytest.approx(18.0)
        assert result.at[0, "rebounds_avg_10"] == pytest.approx(3.5)

    def test_tier_c_fills_only_nan_columns(self) -> None:
        engineer = FeatureEngineer()
        frame = pd.DataFrame({
            "position_group": ["F"],
            "_data_sufficiency_tier": ["C"],
            "points_avg_10": [12.0],  # has value — should NOT be overwritten
            "rebounds_avg_10": [np.nan],  # NaN — should be filled
        })
        priors = {
            "global_feature_priors": {"points_avg_10": 15.0, "rebounds_avg_10": 5.0},
            "position_feature_priors": {"F": {"points_avg_10": 13.0, "rebounds_avg_10": 7.0}},
            "role_feature_priors": {},
            "role_bucket_thresholds": {},
        }
        result = engineer._fill_with_population_priors(
            frame, priors, "_data_sufficiency_tier", ["points_avg_10", "rebounds_avg_10"]
        )
        assert result.at[0, "points_avg_10"] == pytest.approx(12.0)  # preserved
        assert result.at[0, "rebounds_avg_10"] == pytest.approx(7.0)  # filled from position prior

    def test_tier_a_not_filled(self) -> None:
        engineer = FeatureEngineer()
        frame = pd.DataFrame({
            "position_group": ["G"],
            "_data_sufficiency_tier": ["A"],
            "points_avg_10": [np.nan],
        })
        priors = {
            "global_feature_priors": {"points_avg_10": 15.0},
            "position_feature_priors": {"G": {"points_avg_10": 18.0}},
            "role_feature_priors": {},
            "role_bucket_thresholds": {},
        }
        result = engineer._fill_with_population_priors(
            frame, priors, "_data_sufficiency_tier", ["points_avg_10"]
        )
        # Tier A: NOT filled by population priors
        assert pd.isna(result.at[0, "points_avg_10"])

    def test_none_priors_returns_unchanged(self) -> None:
        engineer = FeatureEngineer()
        frame = pd.DataFrame({
            "position_group": ["G"],
            "_data_sufficiency_tier": ["D"],
            "points_avg_10": [np.nan],
        })
        result = engineer._fill_with_population_priors(
            frame, None, "_data_sufficiency_tier", ["points_avg_10"]
        )
        assert pd.isna(result.at[0, "points_avg_10"])


# ---------------------------------------------------------------------------
# Phase 3: confidence score propagation through PropOpportunity
# ---------------------------------------------------------------------------


class TestConfidenceScorePropagation:
    """Verify tier and confidence flow from prediction to PropOpportunity."""

    @staticmethod
    def _make_prediction(**overrides: object) -> object:
        from app.schemas.domain import PropPrediction

        defaults = dict(
            player_id=1,
            player_name="Test Player",
            game_id=1,
            market_key="points",
            sportsbook_line=20.5,
            projected_mean=22.0,
            projected_variance=4.0,
            projected_median=21.5,
            over_probability=0.55,
            under_probability=0.45,
            calibrated_over_probability=0.54,
            percentile_10=16.0,
            percentile_50=22.0,
            percentile_90=28.0,
            confidence_interval_low=18.0,
            confidence_interval_high=26.0,
            top_features=["points_avg_10"],
            model_version="test",
            feature_version="test",
            data_freshness={},
        )
        defaults.update(overrides)
        return PropPrediction(**defaults)

    def test_prop_prediction_defaults(self) -> None:
        pred = self._make_prediction()
        assert pred.data_sufficiency_tier == "A"
        assert pred.data_confidence_score == 1.0

    def test_prop_prediction_custom_tier(self) -> None:
        pred = self._make_prediction(
            data_sufficiency_tier="D",
            data_confidence_score=0.25,
        )
        assert pred.data_sufficiency_tier == "D"
        assert pred.data_confidence_score == 0.25

    def test_prop_opportunity_carries_tier_fields(self) -> None:
        from app.services.prop_analysis import PropOpportunity

        opp = PropOpportunity(
            rank=1,
            game_id=1,
            player_id=1,
            player_name="Test Player",
            player_icon="",
            market_key="points",
            consensus_line=20.5,
            projected_mean=22.0,
            recommended_side="over",
            hit_probability=0.65,
            likelihood_score=0.7,
            calibrated_over_probability=0.63,
            sportsbooks_summary="DK: -110",
            top_features=["points_avg_10"],
            quotes=[],
            data_sufficiency_tier="C",
            data_confidence_score=0.45,
        )
        assert opp.data_sufficiency_tier == "C"
        assert opp.data_confidence_score == 0.45
