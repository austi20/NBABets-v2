from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit

from app.core.resources import get_runtime_budget
from app.training.constants import MARKET_TARGETS
from app.training.features import clip_non_negative, role_bucket_label, role_prior_flag_series

try:
    from xgboost import XGBRegressor
except Exception:  # pragma: no cover
    XGBRegressor = None  # type: ignore[assignment]

GENERIC_MODEL_FEATURES = {
    "predicted_minutes",
    "predicted_minutes_std",
    "baseline_projected_minutes",
    "adjusted_projected_minutes",
    "baseline_usage_share",
    "baseline_usage_rate",
    "adjusted_usage_share",
    "adjusted_usage_rate",
    "team_efficiency_delta",
    "pace_delta",
    "rotation_shock_magnitude",
    "rotation_shock_confidence",
    "minutes_blended",
    "expected_possessions",
    "blended_game_pace",
    "minutes_volatility",
    "minutes_uncertainty_ratio",
    "role_stability",
    "recent_role_delta",
    "history_games_played",
    "history_minutes_played",
    "days_rest",
    "player_days_since_last_game",
    "player_games_since_return",
    "days_since_extended_absence",
    "player_injury_return_flag",
    "team_changed_recently",
    "back_to_back",
    "is_home",
    "spread",
    "total",
    "team_injuries",
    "team_out_count",
    "team_doubtful_count",
    "team_questionable_count",
    "same_position_out_count",
    "projected_starter_count",
    "missing_starter_count",
    "projected_rotation_players",
    "projected_rotation_minutes",
    "projected_minutes_share",
    "projected_starter_flag",
    "lineup_report_count",
    "lineup_instability_score",
    "teammate_absence_pressure",
    "starter_flag",
    "starter_consistency_10",
    "is_guard",
    "is_forward",
    "is_center",
    "consistency_score",
}
_SHARED_PACE_PREFIXES: tuple[str, ...] = (
    "pace_proxy",
    "blended_game_pace",
    "team_pace_",
    "opponent_pace_",
)

# Explicit prefix allowlists per market (AG-TECH-004). Prefer startswith; use
# ``MARKET_FEATURE_EMBEDDED_TOKENS`` only where legacy substring hints were not
# true prefixes (e.g. ``assist_`` inside ``assist_ratio``).
MARKET_FEATURE_PREFIX_ALLOWLISTS: dict[str, tuple[str, ...]] = {
    "points": (
        "points_",
        "usage_",
        "usage_rate",
        "adjusted_usage",
        "touches",
        "adjusted_touches",
        "passes",
        "adjusted_passes",
        "scoring_opportunities",
        "field_goal_attempts",
        "adjusted_field_goal_attempts",
        "free_throw_attempts",
        "adjusted_free_throw_attempts",
        "estimated_three_point",
        "adjusted_estimated_three_point",
        "three_point",
        "true_shooting",
        "effective_field_goal",
        *_SHARED_PACE_PREFIXES,
    ),
    "rebounds": (
        "rebounds_",
        "rebound_",
        "offensive_rebound",
        "defensive_rebound",
        "rebound_chances",
        "adjusted_rebound_chances",
        *_SHARED_PACE_PREFIXES,
    ),
    "assists": (
        "assists_",
        "secondary_assists",
        "free_throw_assists",
        "usage_",
        "usage_rate",
        "adjusted_usage",
        "touches",
        "adjusted_touches",
        "passes",
        "adjusted_passes",
        "adjusted_assist_creation",
        *_SHARED_PACE_PREFIXES,
    ),
    "threes": (
        "threes_",
        "estimated_three_point",
        "three_point",
        "percentage_field_goals_attempted_3pt",
        "percentage_assisted_3pt",
        "usage_",
        "usage_rate",
        "adjusted_usage",
        "touches",
        "adjusted_touches",
        "adjusted_estimated_three_point",
        *_SHARED_PACE_PREFIXES,
    ),
    "turnovers": (
        "turnovers_",
        "usage_",
        "usage_rate",
        "adjusted_usage",
        "touches",
        "adjusted_touches",
        "passes",
        "adjusted_passes",
        *_SHARED_PACE_PREFIXES,
    ),
    "pra": (
        "pra_",
        "points_",
        "rebounds_",
        "rebound_",
        "assists_",
        "scoring_opportunities",
        "usage_",
        "usage_rate",
        "adjusted_usage",
        "touches",
        "adjusted_touches",
        "passes",
        "adjusted_passes",
        "adjusted_field_goal_attempts",
        "adjusted_free_throw_attempts",
        "adjusted_estimated_three_point",
        "adjusted_rebound_chances",
        "adjusted_assist_creation",
        *_SHARED_PACE_PREFIXES,
    ),
}

MARKET_FEATURE_EMBEDDED_TOKENS: dict[str, tuple[str, ...]] = {
    "points": (),
    "rebounds": (),
    "assists": ("assist_",),
    "threes": (),
    "turnovers": ("turnover_",),
    "pra": ("assist_", "turnover_"),
}
RAW_MODEL_EXCLUDE_TOKENS = ("consensus_prob", "line_movement_", "book_count", "market_count", "group_market_consensus")


def _make_regressor(random_state: int, *, reg_alpha: float = 0.0) -> Any:
    worker_count = get_runtime_budget().worker_count
    if XGBRegressor is not None:
        return XGBRegressor(
            n_estimators=220,
            max_depth=5,
            learning_rate=0.04,
            subsample=0.9,
            colsample_bytree=0.9,
            min_child_weight=2,
            reg_alpha=max(float(reg_alpha), 0.0),
            reg_lambda=1.0,
            n_jobs=worker_count,
            tree_method="hist",
            objective="reg:squarederror",
            random_state=random_state,
        )
    return HistGradientBoostingRegressor(max_depth=5, learning_rate=0.04, random_state=random_state)


@dataclass
class FittedMarketModel:
    market_key: str
    mean_model: Any
    residual_std: float
    feature_columns: list[str]
    rate_model: Any | None = None
    rate_feature_columns: list[str] = field(default_factory=list)
    stacker: Ridge | None = None
    stacker_columns: list[str] = field(default_factory=list)
    variance_model: Any | None = None
    variance_feature_columns: list[str] = field(default_factory=list)
    learned_weights: dict[str, float] = field(default_factory=dict)
    position_total_priors: dict[str, float] = field(default_factory=dict)
    position_rate_priors: dict[str, float] = field(default_factory=dict)
    role_total_priors: dict[str, float] = field(default_factory=dict)
    role_rate_priors: dict[str, float] = field(default_factory=dict)
    role_variance_priors: dict[str, float] = field(default_factory=dict)
    global_total_prior: float = 0.0
    global_rate_prior: float = 0.0
    prior_strength: float = 10.0

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        model_mean = clip_non_negative(np.asarray(self.mean_model.predict(frame[self.feature_columns]), dtype=float))
        baseline_column = f"{self.market_key}_baseline_mean"
        if baseline_column in frame.columns:
            baseline_mean = clip_non_negative(
                np.nan_to_num(np.asarray(frame[baseline_column], dtype=float), nan=0.0)
            )
        else:
            baseline_mean = np.zeros(len(frame), dtype=float)
        if self.rate_model is not None and self.rate_feature_columns:
            predicted_rate = clip_non_negative(
                np.asarray(self.rate_model.predict(frame[self.rate_feature_columns]), dtype=float)
            )
            exposure = _numeric_column(
                frame,
                "predicted_minutes",
                _numeric_column(frame, "minutes_blended", _numeric_column(frame, "minutes_avg_10", 0.0)),
            )
            rate_mean = clip_non_negative(predicted_rate * np.maximum(exposure, 0.0))
        else:
            rate_mean = model_mean

        if self.stacker is not None and self.stacker_columns:
            stack_frame = self._stack_frame(frame, model_mean, rate_mean, baseline_mean)
            mean = clip_non_negative(np.asarray(self.stacker.predict(stack_frame[self.stacker_columns]), dtype=float))
        elif self.rate_model is not None and self.rate_feature_columns:
            mean = clip_non_negative(0.50 * model_mean + 0.30 * rate_mean + 0.20 * baseline_mean)
        else:
            mean = clip_non_negative(0.75 * model_mean + 0.25 * baseline_mean)
        mean = self._apply_partial_pooling(frame, mean, baseline_mean)
        mean = self._stabilize_mean(frame, mean, baseline_mean)

        baseline_variance_column = f"{self.market_key}_baseline_variance"
        baseline_variance = (
            np.nan_to_num(np.asarray(frame[baseline_variance_column], dtype=float), nan=1.0)
            if baseline_variance_column in frame.columns
            else np.full(len(frame), 1.0, dtype=float)
        )
        if self.variance_model is not None and self.variance_feature_columns:
            variance_frame = self._stack_frame(frame, model_mean, rate_mean, baseline_mean)
            variance = clip_non_negative(
                np.asarray(self.variance_model.predict(variance_frame[self.variance_feature_columns]), dtype=float)
            )
        else:
            variance = np.full(len(frame), max(self.residual_std**2, 1.0), dtype=float)
        variance = np.maximum(variance, baseline_variance)
        variance = np.maximum(variance, np.maximum(mean, 0.25))
        variance = self._inflate_variance(frame, variance, mean)
        return mean, variance

    def _stack_frame(
        self,
        frame: pd.DataFrame,
        direct_mean: np.ndarray,
        rate_mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> pd.DataFrame:
        stack_frame = frame.copy()
        stack_frame["stack_direct_mean"] = direct_mean
        stack_frame["stack_rate_mean"] = rate_mean
        stack_frame["stack_baseline_mean"] = baseline_mean
        return stack_frame

    def _stabilize_mean(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        if self.market_key == "points":
            return self._stabilize_points_mean(frame, mean, baseline_mean)
        if self.market_key == "rebounds":
            return self._stabilize_rebounds_mean(frame, mean, baseline_mean)
        if self.market_key == "assists":
            return self._stabilize_assists_mean(frame, mean, baseline_mean)
        if self.market_key == "turnovers":
            return self._stabilize_turnovers_mean(frame, mean, baseline_mean)
        if self.market_key == "pra":
            return self._stabilize_pra_mean(frame, mean, baseline_mean)
        if self.market_key != "threes":
            return mean

        recent_5 = _numeric_column(frame, "threes_avg_5", baseline_mean)
        recent_10 = _numeric_column(frame, "threes_avg_10", recent_5)
        recent_20 = _numeric_column(frame, "threes_avg_20", recent_10)
        season = _numeric_column(frame, "threes_season_avg", recent_20)
        recent_std = _numeric_column(
            frame,
            "threes_std_10",
            np.sqrt(np.maximum(recent_10, 0.25)),
        )
        predicted_minutes = _numeric_column(frame, "predicted_minutes", _numeric_column(frame, "minutes_avg_10", 0.0))
        attempt_rate = _numeric_column(
            frame,
            "estimated_three_point_attempts_per_minute_blended",
            _numeric_column(frame, "estimated_three_point_attempts_per_minute_avg_10", 0.0),
        )

        anchor = clip_non_negative(0.40 * recent_5 + 0.25 * recent_10 + 0.20 * recent_20 + 0.15 * season)
        ceiling_anchor = np.maximum.reduce([anchor, baseline_mean, recent_10, season])
        stabilized = clip_non_negative(0.50 * mean + 0.50 * ceiling_anchor)
        attempt_ceiling = np.maximum(predicted_minutes * np.maximum(attempt_rate, 0.06) * 0.44, 0.70)
        raw_ceiling = np.maximum.reduce(
            [
                ceiling_anchor + 1.15 * np.maximum(recent_std, 0.55),
                recent_5 + 0.90,
                recent_10 + 1.10,
                season + 1.35,
                attempt_ceiling,
            ]
        )
        return self._cap_stabilized_mean(
            stabilized=stabilized,
            ceiling_anchor=ceiling_anchor,
            raw_ceiling=raw_ceiling,
            ratio_cap=1.28,
            minimum_headroom=0.35,
        )

    def _market_history_anchor(
        self,
        frame: pd.DataFrame,
        market_key: str,
        baseline_mean: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        recent_5 = _numeric_column(frame, f"{market_key}_avg_5", baseline_mean)
        recent_10 = _numeric_column(frame, f"{market_key}_avg_10", recent_5)
        recent_20 = _numeric_column(frame, f"{market_key}_avg_20", recent_10)
        season = _numeric_column(frame, f"{market_key}_season_avg", recent_20)
        recent_std = _numeric_column(
            frame,
            f"{market_key}_std_10",
            np.sqrt(np.maximum(recent_10, 0.25)),
        )
        weighted_anchor = clip_non_negative(0.40 * recent_5 + 0.25 * recent_10 + 0.20 * recent_20 + 0.15 * season)
        # Use a true blended mean as the history anchor — not a ceiling based on the max of averages.
        history_anchor = clip_non_negative(0.45 * weighted_anchor + 0.35 * recent_10 + 0.20 * season)
        history_anchor = np.maximum(history_anchor, baseline_mean * 0.50)
        return weighted_anchor, history_anchor, recent_std, recent_5, recent_10, season

    def _cap_stabilized_mean(
        self,
        *,
        stabilized: np.ndarray,
        ceiling_anchor: np.ndarray,
        raw_ceiling: np.ndarray,
        ratio_cap: float,
        minimum_headroom: float,
    ) -> np.ndarray:
        ratio_ceiling = ceiling_anchor * ratio_cap
        final_ceiling = np.maximum(ceiling_anchor + minimum_headroom, np.minimum(raw_ceiling, ratio_ceiling))
        return clip_non_negative(np.minimum(stabilized, final_ceiling))

    def _stabilize_points_mean(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        _, ceiling_anchor, recent_std, recent_5, recent_10, season = self._market_history_anchor(
            frame,
            "points",
            baseline_mean,
        )
        predicted_minutes = _numeric_column(frame, "predicted_minutes", _numeric_column(frame, "minutes_avg_10", 0.0))
        point_rate = _numeric_column(
            frame,
            "points_per_minute_blended",
            _numeric_column(frame, "points_per_minute_avg_10", 0.0),
        )
        shot_rate = _numeric_column(
            frame,
            "field_goal_attempts_per_minute_blended",
            _numeric_column(frame, "field_goal_attempts_per_minute_avg_10", point_rate / 1.9),
        )
        free_throw_rate = _numeric_column(
            frame,
            "free_throw_attempts_per_minute_blended",
            _numeric_column(frame, "free_throw_attempts_per_minute_avg_10", 0.0),
        )
        three_rate = _numeric_column(
            frame,
            "estimated_three_point_attempts_per_minute_blended",
            _numeric_column(frame, "estimated_three_point_attempts_per_minute_avg_10", 0.0),
        )
        opportunity_rate = np.maximum(point_rate, (shot_rate * 1.45) + (free_throw_rate * 0.75) + (three_rate * 0.35))
        stabilized = clip_non_negative(0.50 * mean + 0.50 * ceiling_anchor)
        raw_ceiling = np.maximum.reduce(
            [
                ceiling_anchor + 1.15 * np.maximum(recent_std, 1.0),
                recent_5 + 3.5,
                recent_10 + 4.0,
                season + 4.5,
                predicted_minutes * np.maximum(opportunity_rate, 0.22) * 1.05,
            ]
        )
        return self._cap_stabilized_mean(
            stabilized=stabilized,
            ceiling_anchor=ceiling_anchor,
            raw_ceiling=raw_ceiling,
            ratio_cap=1.35,
            minimum_headroom=1.0,
        )

    def _stabilize_rebounds_mean(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        _, ceiling_anchor, recent_std, recent_5, recent_10, season = self._market_history_anchor(
            frame,
            "rebounds",
            baseline_mean,
        )
        predicted_minutes = _numeric_column(frame, "predicted_minutes", _numeric_column(frame, "minutes_avg_10", 0.0))
        rebound_rate = _numeric_column(
            frame,
            "rebounds_per_minute_blended",
            _numeric_column(frame, "rebounds_per_minute_avg_10", 0.0),
        )
        chances_rate = _numeric_column(
            frame,
            "rebound_chances_total_per_minute_blended",
            _numeric_column(frame, "rebound_chances_total_per_minute_avg_10", rebound_rate),
        )
        conversion = _numeric_column(
            frame,
            "rebound_conversion_rate_blended",
            _numeric_column(frame, "rebound_conversion_rate_avg_10", 0.35),
        )
        opportunity_rate = np.maximum(rebound_rate, chances_rate * np.clip(conversion, 0.10, 0.90))
        stabilized = clip_non_negative(0.50 * mean + 0.50 * ceiling_anchor)
        raw_ceiling = np.maximum.reduce(
            [
                ceiling_anchor + 1.00 * np.maximum(recent_std, 0.75),
                recent_5 + 1.5,
                recent_10 + 1.75,
                season + 2.0,
                predicted_minutes * np.maximum(opportunity_rate, 0.12) * 1.08,
            ]
        )
        return self._cap_stabilized_mean(
            stabilized=stabilized,
            ceiling_anchor=ceiling_anchor,
            raw_ceiling=raw_ceiling,
            ratio_cap=1.32,
            minimum_headroom=0.5,
        )

    def _stabilize_assists_mean(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        _, ceiling_anchor, recent_std, recent_5, recent_10, season = self._market_history_anchor(
            frame,
            "assists",
            baseline_mean,
        )
        predicted_minutes = _numeric_column(frame, "predicted_minutes", _numeric_column(frame, "minutes_avg_10", 0.0))
        assist_rate = _numeric_column(
            frame,
            "assists_per_minute_blended",
            _numeric_column(frame, "assists_per_minute_avg_10", 0.0),
        )
        creation_rate = _numeric_column(
            frame,
            "assist_creation_proxy_per_minute_blended",
            _numeric_column(frame, "assist_creation_proxy_per_minute_avg_10", assist_rate),
        )
        stabilized = clip_non_negative(0.50 * mean + 0.50 * ceiling_anchor)
        raw_ceiling = np.maximum.reduce(
            [
                ceiling_anchor + 0.85 * np.maximum(recent_std, 0.70),
                recent_5 + 1.00,
                recent_10 + 1.20,
                season + 1.45,
                np.maximum(
                    predicted_minutes * np.maximum(np.maximum(assist_rate, creation_rate), 0.06) * 1.04,
                    1.0,
                ),
            ]
        )
        return self._cap_stabilized_mean(
            stabilized=stabilized,
            ceiling_anchor=ceiling_anchor,
            raw_ceiling=raw_ceiling,
            ratio_cap=1.30,
            minimum_headroom=0.4,
        )

    def _stabilize_turnovers_mean(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        _, ceiling_anchor, recent_std, recent_5, recent_10, season = self._market_history_anchor(
            frame,
            "turnovers",
            baseline_mean,
        )
        predicted_minutes = _numeric_column(frame, "predicted_minutes", _numeric_column(frame, "minutes_avg_10", 0.0))
        turnover_rate = _numeric_column(
            frame,
            "turnovers_per_minute_blended",
            _numeric_column(frame, "turnovers_per_minute_avg_10", 0.0),
        )
        touches_rate = _numeric_column(
            frame,
            "touches_per_minute_blended",
            _numeric_column(frame, "touches_per_minute_avg_10", turnover_rate * 10.0),
        )
        turnover_ratio = _numeric_column(
            frame,
            "turnover_ratio_blended",
            _numeric_column(frame, "turnover_ratio_avg_10", 0.12),
        )
        opportunity_rate = np.maximum(turnover_rate, touches_rate * np.clip(turnover_ratio, 0.03, 0.30) * 0.75)
        stabilized = clip_non_negative(0.50 * mean + 0.50 * ceiling_anchor)
        raw_ceiling = np.maximum.reduce(
            [
                ceiling_anchor + 0.85 * np.maximum(recent_std, 0.50),
                recent_5 + 0.75,
                recent_10 + 0.90,
                season + 1.00,
                predicted_minutes * np.maximum(opportunity_rate, 0.045) * 1.05,
            ]
        )
        return self._cap_stabilized_mean(
            stabilized=stabilized,
            ceiling_anchor=ceiling_anchor,
            raw_ceiling=raw_ceiling,
            ratio_cap=1.30,
            minimum_headroom=0.35,
        )

    def _stabilize_pra_mean(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        _, ceiling_anchor, recent_std, recent_5, recent_10, season = self._market_history_anchor(
            frame,
            "pra",
            baseline_mean,
        )
        predicted_minutes = _numeric_column(frame, "predicted_minutes", _numeric_column(frame, "minutes_avg_10", 0.0))
        # Use blended component means — not the max of each component's averages, which compounded the inflation.
        component_anchor = (
            clip_non_negative(
                0.50 * _numeric_column(frame, "points_avg_10", 0.0)
                + 0.30 * _numeric_column(frame, "points_season_avg", 0.0)
                + 0.20 * _numeric_column(frame, "points_baseline_mean", 0.0)
            )
            + clip_non_negative(
                0.50 * _numeric_column(frame, "rebounds_avg_10", 0.0)
                + 0.30 * _numeric_column(frame, "rebounds_season_avg", 0.0)
                + 0.20 * _numeric_column(frame, "rebounds_baseline_mean", 0.0)
            )
            + clip_non_negative(
                0.50 * _numeric_column(frame, "assists_avg_10", 0.0)
                + 0.30 * _numeric_column(frame, "assists_season_avg", 0.0)
                + 0.20 * _numeric_column(frame, "assists_baseline_mean", 0.0)
            )
        )
        # Soft floor — don't let anchor go below component sum, but don't force it above either.
        ceiling_anchor = np.maximum(ceiling_anchor, component_anchor * 0.80)
        point_rate = _numeric_column(
            frame,
            "points_per_minute_blended",
            _numeric_column(frame, "points_per_minute_avg_10", 0.0),
        )
        rebound_rate = _numeric_column(
            frame,
            "rebounds_per_minute_blended",
            _numeric_column(frame, "rebounds_per_minute_avg_10", 0.0),
        )
        assist_rate = _numeric_column(
            frame,
            "assists_per_minute_blended",
            _numeric_column(frame, "assists_per_minute_avg_10", 0.0),
        )
        opportunity_rate = np.maximum(point_rate + rebound_rate + assist_rate, 0.75)
        stabilized = clip_non_negative(0.50 * mean + 0.50 * ceiling_anchor)
        raw_ceiling = np.maximum.reduce(
            [
                ceiling_anchor + 1.15 * np.maximum(recent_std, 1.25),
                recent_5 + 4.0,
                recent_10 + 4.5,
                season + 5.0,
                predicted_minutes * opportunity_rate * 1.06,
            ]
        )
        return self._cap_stabilized_mean(
            stabilized=stabilized,
            ceiling_anchor=ceiling_anchor,
            raw_ceiling=raw_ceiling,
            ratio_cap=1.35,
            minimum_headroom=1.5,
        )

    def _apply_partial_pooling(
        self,
        frame: pd.DataFrame,
        mean: np.ndarray,
        baseline_mean: np.ndarray,
    ) -> np.ndarray:
        if (
            not self.position_total_priors
            and not self.position_rate_priors
            and "history_games_played" not in frame.columns
            and f"{self.market_key}_avg_10" not in frame.columns
        ):
            return mean
        history_games = _numeric_column(frame, "history_games_played", 0.0)
        history_minutes = _numeric_column(frame, "history_minutes_played", 0.0)
        role_stability = _numeric_column(frame, "role_stability", 0.5)
        lineup_instability = _numeric_column(frame, "lineup_instability_score", 0.0)
        minutes_uncertainty = _numeric_column(frame, "minutes_uncertainty_ratio", 0.0)
        exposure = _numeric_column(
            frame,
            "predicted_minutes",
            _numeric_column(frame, "minutes_blended", _numeric_column(frame, "minutes_avg_10", 0.0)),
        )
        position_series = frame.get("position_group", pd.Series("UNK", index=frame.index)).fillna("UNK").astype(str)
        starter_series = pd.to_numeric(frame.get("starter_flag", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
        role_bucket_series = [
            role_bucket_label(position_group, starter_flag)
            for position_group, starter_flag in zip(position_series, starter_series, strict=False)
        ]
        position_total_prior = np.asarray(
            [self.position_total_priors.get(position, self.global_total_prior) for position in position_series],
            dtype=float,
        )
        position_rate_prior = np.asarray(
            [self.position_rate_priors.get(position, self.global_rate_prior) for position in position_series],
            dtype=float,
        )
        role_total_prior = np.asarray(
            [
                self.role_total_priors.get(role_bucket, self.position_total_priors.get(position, self.global_total_prior))
                for role_bucket, position in zip(role_bucket_series, position_series, strict=False)
            ],
            dtype=float,
        )
        role_rate_prior = np.asarray(
            [
                self.role_rate_priors.get(role_bucket, self.position_rate_priors.get(position, self.global_rate_prior))
                for role_bucket, position in zip(role_bucket_series, position_series, strict=False)
            ],
            dtype=float,
        )
        position_exposure_prior = clip_non_negative(position_rate_prior * np.maximum(exposure, 0.0))
        role_exposure_prior = clip_non_negative(role_rate_prior * np.maximum(exposure, 0.0))
        history_anchor = _numeric_column(frame, f"{self.market_key}_avg_10", baseline_mean)
        season_anchor = _numeric_column(frame, f"{self.market_key}_season_avg", history_anchor)
        pooled_prior = clip_non_negative(
            0.30 * baseline_mean
            + 0.25 * history_anchor
            + 0.20 * season_anchor
            + 0.15 * np.maximum(position_total_prior, position_exposure_prior)
            + 0.10 * np.maximum(role_total_prior, role_exposure_prior)
        )
        tier_series = frame.get("_data_sufficiency_tier", pd.Series("", index=frame.index)).fillna("").astype(str)
        tier_prior_strength = np.asarray(
            [
                {"A": 10.0, "B": 15.0, "C": 25.0, "D": 50.0}.get(tier, self.prior_strength)
                for tier in tier_series
            ],
            dtype=float,
        )
        sample_weight = history_games / (history_games + tier_prior_strength)
        minute_weight = np.minimum(history_minutes / 320.0, 1.0)
        stability_weight = np.clip(0.55 + 0.45 * role_stability, 0.35, 1.0)
        disruption_penalty = np.clip(1.0 - 0.35 * lineup_instability - 0.25 * minutes_uncertainty, 0.25, 1.0)
        reliability = np.clip(0.5 * sample_weight + 0.5 * minute_weight, 0.05, 0.98) * stability_weight * disruption_penalty
        return clip_non_negative(reliability * mean + (1.0 - reliability) * pooled_prior)

    def _inflate_variance(self, frame: pd.DataFrame, variance: np.ndarray, mean: np.ndarray) -> np.ndarray:
        lineup_instability = _numeric_column(frame, "lineup_instability_score", 0.0)
        minutes_uncertainty = _numeric_column(frame, "minutes_uncertainty_ratio", 0.0)
        recent_role_delta = _numeric_column(frame, "recent_role_delta", 0.0)
        same_position_out = _numeric_column(frame, "same_position_out_count", 0.0)
        position_series = frame.get("position_group", pd.Series("UNK", index=frame.index)).fillna("UNK").astype(str)
        starter_series = pd.to_numeric(frame.get("starter_flag", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
        role_bucket_series = [
            role_bucket_label(position_group, starter_flag)
            for position_group, starter_flag in zip(position_series, starter_series, strict=False)
        ]
        inflation = (
            1.0
            + 0.35 * np.clip(lineup_instability, 0.0, 1.0)
            + 0.28 * np.clip(minutes_uncertainty, 0.0, 1.5)
            + 0.20 * np.clip(recent_role_delta, 0.0, 1.5)
            + 0.06 * np.clip(same_position_out, 0.0, 4.0)
        )
        role_variance_prior = np.asarray(
            [self.role_variance_priors.get(role_bucket, 0.0) for role_bucket in role_bucket_series],
            dtype=float,
        )
        variance = np.maximum(variance, role_variance_prior)
        variance = variance * inflation
        tier_series = frame.get("_data_sufficiency_tier", pd.Series("", index=frame.index)).fillna("").astype(str)
        tier_multipliers = np.asarray(
            [{"A": 1.0, "B": 1.3, "C": 2.0, "D": 3.0}.get(tier, 1.0) for tier in tier_series],
            dtype=float,
        )
        variance = variance * tier_multipliers
        return np.maximum(variance, np.maximum(mean, 0.25))


class MinutesModel:
    def __init__(self, random_state: int) -> None:
        self._model = _make_regressor(random_state)
        self._variance_model = _make_regressor(random_state + 19)
        self.feature_columns: list[str] = []
        self.residual_floor: float = 1.0

    def fit(self, frame: pd.DataFrame, feature_columns: list[str]) -> None:
        self.feature_columns = feature_columns
        self._model.fit(frame[feature_columns], frame["minutes"])
        fitted = clip_non_negative(np.asarray(self._model.predict(frame[feature_columns]), dtype=float))
        squared_residuals = np.square(frame["minutes"].to_numpy(dtype=float) - fitted)
        self._variance_model.fit(frame[feature_columns], squared_residuals)
        self.residual_floor = float(max(np.std(frame["minutes"].to_numpy(dtype=float) - fitted), 1.0))

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        return clip_non_negative(np.asarray(self._model.predict(frame[self.feature_columns]), dtype=float))

    def predict_uncertainty(self, frame: pd.DataFrame) -> np.ndarray:
        raw_variance = clip_non_negative(np.asarray(self._variance_model.predict(frame[self.feature_columns]), dtype=float))
        return np.maximum(np.sqrt(np.maximum(raw_variance, 0.0)), self.residual_floor * 0.35)


class StatModelSuite:
    def __init__(self, random_state: int, *, l1_alpha: float = 0.0) -> None:
        self._random_state = random_state
        self._l1_alpha = max(float(l1_alpha), 0.0)
        self.models: dict[str, FittedMarketModel] = {}

    def fit(self, frame: pd.DataFrame, feature_columns: list[str], l1_alpha: float | None = None) -> dict[str, float]:
        active_l1 = self._l1_alpha if l1_alpha is None else max(float(l1_alpha), 0.0)
        metrics: dict[str, float] = {}
        for market_key, target_column in MARKET_TARGETS.items():
            market_features = _market_feature_columns(frame, market_key, feature_columns)
            rate_target = _safe_training_rate(frame[target_column], frame["minutes"])
            direct_model = _make_regressor(self._random_state, reg_alpha=active_l1)
            direct_model.fit(frame[market_features], frame[target_column])
            rate_model = _make_regressor(self._random_state + 7, reg_alpha=active_l1)
            rate_model.fit(frame[market_features], rate_target)

            direct_fitted = clip_non_negative(np.asarray(direct_model.predict(frame[market_features]), dtype=float))
            rate_fitted = clip_non_negative(
                np.asarray(rate_model.predict(frame[market_features]), dtype=float) * np.maximum(frame["predicted_minutes"].to_numpy(dtype=float), 0.0)
            )
            stacker_columns, stack_train = _build_stacker_frame(
                frame,
                market_key,
                direct_fitted,
                rate_fitted,
            )
            stacker = _fit_time_series_stacker(stack_train, frame[target_column].to_numpy(dtype=float))
            blended_fitted = clip_non_negative(np.asarray(stacker.predict(stack_train[stacker_columns]), dtype=float))
            residual_std = float(np.std(frame[target_column].to_numpy(dtype=float) - blended_fitted))
            learned_weights = {
                column: float(weight)
                for column, weight in zip(
                    stacker_columns,
                    np.ravel(getattr(stacker, "coef_", np.zeros(len(stacker_columns)))),
                    strict=False,
                )
            }

            variance_model = _make_regressor(self._random_state + 13, reg_alpha=active_l1)
            squared_residuals = np.square(frame[target_column].to_numpy(dtype=float) - blended_fitted)
            variance_model.fit(stack_train[stacker_columns], squared_residuals)

            self.models[market_key] = FittedMarketModel(
                market_key=market_key,
                mean_model=direct_model,
                residual_std=residual_std,
                feature_columns=market_features,
                rate_model=rate_model,
                rate_feature_columns=market_features,
                stacker=stacker,
                stacker_columns=stacker_columns,
                variance_model=variance_model,
                variance_feature_columns=stacker_columns,
                learned_weights=learned_weights,
                position_total_priors=_build_position_priors(frame, "position_group", target_column),
                position_rate_priors=_build_position_priors(frame, "position_group", target_column, per_minute=True),
                role_total_priors=_build_role_priors(frame, "position_group", "starter_flag", target_column),
                role_rate_priors=_build_role_priors(
                    frame,
                    "position_group",
                    "starter_flag",
                    target_column,
                    per_minute=True,
                ),
                role_variance_priors=_build_role_priors(
                    frame,
                    "position_group",
                    "starter_flag",
                    target_column,
                    statistic="var",
                ),
                global_total_prior=float(pd.to_numeric(frame[target_column], errors="coerce").fillna(0.0).mean()),
                global_rate_prior=float(np.mean(rate_target)),
                prior_strength=10.0,
            )
            fitted, _ = self.models[market_key].predict(frame)
            metrics[f"{market_key}_rmse"] = float(mean_squared_error(frame[target_column], fitted) ** 0.5)
            metrics[f"{market_key}_mae"] = float(np.mean(np.abs(frame[target_column].to_numpy(dtype=float) - fitted)))
            metrics[f"{market_key}_rate_rmse"] = float(
                mean_squared_error(rate_target, _safe_training_rate(fitted, frame["predicted_minutes"])) ** 0.5
            )
        return metrics


def _market_feature_column_match(column: str, market_key: str) -> bool:
    if column.startswith(f"{market_key}_group_"):
        return True
    if any(column.startswith(prefix) for prefix in MARKET_FEATURE_PREFIX_ALLOWLISTS[market_key]):
        return True
    return any(token in column for token in MARKET_FEATURE_EMBEDDED_TOKENS[market_key])


def _market_feature_columns(frame: pd.DataFrame, market_key: str, feature_columns: list[str]) -> list[str]:
    selected = []
    for column in feature_columns:
        if any(token in column for token in RAW_MODEL_EXCLUDE_TOKENS):
            continue
        if column in GENERIC_MODEL_FEATURES or _market_feature_column_match(column, market_key):
            selected.append(column)
    if "predicted_minutes" not in selected:
        selected.append("predicted_minutes")
    return sorted({column for column in selected if column in frame.columns})


def _build_stacker_frame(
    frame: pd.DataFrame,
    market_key: str,
    direct_mean: np.ndarray,
    rate_mean: np.ndarray,
) -> tuple[list[str], pd.DataFrame]:
    stack_frame = pd.DataFrame(index=frame.index)
    stack_frame["stack_direct_mean"] = direct_mean
    stack_frame["stack_rate_mean"] = rate_mean
    stack_frame["stack_baseline_mean"] = _numeric_column(frame, f"{market_key}_baseline_mean", 0.0)
    group_columns = sorted(
        column
        for column in frame.columns
        if column.startswith(f"{market_key}_group_") and "market_consensus" not in column
    )
    for column in group_columns:
        stack_frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    stacker_columns = ["stack_direct_mean", "stack_rate_mean", "stack_baseline_mean", *group_columns]
    return stacker_columns, stack_frame


def _fit_time_series_stacker(frame: pd.DataFrame, labels: np.ndarray) -> Ridge:
    candidate_alphas = [0.1, 1.0, 5.0]
    if len(frame) < 60:
        model = Ridge(alpha=1.0)
        model.fit(frame, labels)
        return model
    splitter = TimeSeriesSplit(n_splits=min(5, max(2, len(frame) // 60)))
    best_alpha = 1.0
    best_score = float("inf")
    for alpha in candidate_alphas:
        fold_errors: list[float] = []
        for train_idx, valid_idx in splitter.split(frame):
            if len(train_idx) < 30 or len(valid_idx) < 10:
                continue
            fold_model = Ridge(alpha=alpha)
            fold_model.fit(frame.iloc[train_idx], labels[train_idx])
            prediction = fold_model.predict(frame.iloc[valid_idx])
            fold_errors.append(float(np.mean(np.square(labels[valid_idx] - prediction))))
        if fold_errors and np.mean(fold_errors) < best_score:
            best_score = float(np.mean(fold_errors))
            best_alpha = alpha
    model = Ridge(alpha=best_alpha)
    model.fit(frame, labels)
    return model


def _safe_training_rate(numerator: pd.Series | np.ndarray, denominator: pd.Series | np.ndarray) -> np.ndarray:
    numerator_array = np.asarray(numerator, dtype=float)
    denominator_array = np.maximum(np.asarray(denominator, dtype=float), 1.0)
    return clip_non_negative(numerator_array / denominator_array)


def _build_position_priors(
    frame: pd.DataFrame,
    position_column: str,
    target_column: str,
    *,
    per_minute: bool = False,
) -> dict[str, float]:
    if position_column not in frame.columns or target_column not in frame.columns:
        return {}
    grouped = frame[[position_column, target_column, "minutes"]].copy()
    grouped[position_column] = grouped[position_column].fillna("UNK").astype(str)
    if per_minute:
        grouped["target_value"] = _safe_training_rate(grouped[target_column], grouped["minutes"])
    else:
        grouped["target_value"] = pd.to_numeric(grouped[target_column], errors="coerce").fillna(0.0)
    return (
        grouped.groupby(position_column)["target_value"]
        .mean()
        .fillna(0.0)
        .astype(float)
        .to_dict()
    )


def _build_role_priors(
    frame: pd.DataFrame,
    position_column: str,
    starter_column: str,
    target_column: str,
    *,
    per_minute: bool = False,
    statistic: str = "mean",
) -> dict[str, float]:
    if (
        position_column not in frame.columns
        or starter_column not in frame.columns
        or target_column not in frame.columns
        or "minutes" not in frame.columns
    ):
        return {}
    grouped = frame[[position_column, starter_column, target_column, "minutes"]].copy()
    grouped[position_column] = grouped[position_column].fillna("UNK").astype(str).str.upper().str[:1]
    grouped[starter_column] = role_prior_flag_series(grouped, starter_column=starter_column, minutes_column="minutes")
    grouped["role_bucket"] = [
        role_bucket_label(position_group, starter_flag)
        for position_group, starter_flag in zip(
            grouped[position_column],
            grouped[starter_column],
            strict=False,
        )
    ]
    if per_minute:
        grouped["target_value"] = _safe_training_rate(grouped[target_column], grouped["minutes"])
    else:
        grouped["target_value"] = pd.to_numeric(grouped[target_column], errors="coerce").fillna(0.0)
    if statistic == "var":
        aggregated = grouped.groupby("role_bucket")["target_value"].var(ddof=0)
    else:
        aggregated = grouped.groupby("role_bucket")["target_value"].mean()
    return aggregated.fillna(0.0).astype(float).to_dict()


def _numeric_column(
    frame: pd.DataFrame,
    column: str,
    fallback: float | np.ndarray,
) -> np.ndarray:
    if column not in frame.columns:
        if isinstance(fallback, np.ndarray):
            return np.asarray(fallback, dtype=float)
        return np.full(len(frame), float(fallback), dtype=float)
    return np.nan_to_num(np.asarray(frame[column], dtype=float), nan=0.0)
