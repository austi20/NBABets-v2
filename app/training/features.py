from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from app.providers.canonical_schema import ODDS_CANONICAL_FIELDS
from app.training.constants import MARKET_TARGETS
from app.training.feature_builders import travel_geography
from app.training.feature_builders.market_group import add_market_group_features
from app.training.feature_builders.opponent_lineup import add_opponent_lineup_disruption
from app.training.feature_builders.travel_geography import _arena_coords_series, _haversine_km_series

TEAM_HOME_ARENA_COORDS = travel_geography.TEAM_HOME_ARENA_COORDS

PLAYER_SIGNAL_COLUMNS = [
    "pace",
    "estimated_pace",
    "possessions",
    "usage_percentage",
    "estimated_usage_percentage",
    "assist_percentage",
    "assist_ratio",
    "turnover_ratio",
    "rebound_percentage",
    "offensive_rebound_percentage",
    "defensive_rebound_percentage",
    "touches",
    "passes",
    "secondary_assists",
    "free_throw_assists",
    "rebound_chances_offensive",
    "rebound_chances_defensive",
    "rebound_chances_total",
    "contested_field_goals_attempted",
    "uncontested_field_goals_attempted",
    "true_shooting_percentage",
    "effective_field_goal_percentage",
    "percentage_field_goals_attempted_2pt",
    "percentage_field_goals_attempted_3pt",
    "percentage_points_2pt",
    "percentage_points_3pt",
    "percentage_points_free_throw",
    "percentage_points_paint",
    "percentage_assisted_2pt",
    "percentage_assisted_3pt",
    "percentage_assisted_fgm",
]
DERIVED_SIGNAL_COLUMNS = [
    "pace_proxy",
    "usage_rate",
    "assist_creation_proxy",
    "estimated_three_point_attempts",
    "estimated_two_point_attempts",
    "scoring_opportunities",
    "rebound_conversion_rate",
    "points_per_minute",
    "rebounds_per_minute",
    "assists_per_minute",
    "threes_per_minute",
    "turnovers_per_minute",
    "pra_per_minute",
    "field_goal_attempts_per_minute",
    "free_throw_attempts_per_minute",
    "estimated_three_point_attempts_per_minute",
    "touches_per_minute",
    "passes_per_minute",
    "assist_creation_proxy_per_minute",
    "rebound_chances_total_per_minute",
    "points_per_possession",
    "rebounds_per_possession",
    "assists_per_possession",
    "threes_per_possession",
    "turnovers_per_possession",
]
AVAILABILITY_SIGNAL_COLUMNS = [
    "team_injuries",
    "team_out_count",
    "team_doubtful_count",
    "team_questionable_count",
    "same_position_out_count",
    "same_position_doubtful_count",
    "projected_starter_count",
    "missing_starter_count",
    "projected_rotation_players",
    "projected_rotation_minutes",
    "projected_minutes_share",
    "projected_starter_flag",
    "lineup_report_count",
    "lineup_instability_score",
    "teammate_absence_pressure",
]
ODDS_MARKET_COLUMNS = [column for column in ODDS_CANONICAL_FIELDS if column != "line_value"]
ROLE_BUCKET_LINE_THRESHOLDS: dict[str, float] = {
    "points": 18.0,
    "rebounds": 6.5,
    "assists": 4.5,
    "threes": 1.8,
    "turnovers": 1.5,
    "pra": 28.0,
}


def role_bucket_label(position_group: object, starter_flag: object) -> str:
    safe_position = str(position_group or "UNK").upper()[:1] or "UNK"
    try:
        starter = float(starter_flag) >= 0.5
    except Exception:
        starter = False
    return f"{safe_position}_{'starter' if starter else 'bench'}"


def role_prior_flag_series(
    frame: pd.DataFrame,
    *,
    starter_column: str = "starter_flag",
    minutes_column: str = "minutes",
    starter_minutes_threshold: float = 24.0,
) -> pd.Series:
    raw_starter = pd.to_numeric(
        frame.get(starter_column, pd.Series(0.0, index=frame.index)),
        errors="coerce",
    ).fillna(0.0)
    if raw_starter.max() > 0:
        return (raw_starter >= 0.5).astype(float)
    minutes = pd.to_numeric(
        frame.get(minutes_column, pd.Series(0.0, index=frame.index)),
        errors="coerce",
    ).fillna(0.0)
    return (minutes >= starter_minutes_threshold).astype(float)


def _deduplicate_columns(frame: pd.DataFrame, *, keep: str = "last") -> pd.DataFrame:
    if frame.columns.is_unique:
        return frame.copy()
    return frame.loc[:, ~frame.columns.duplicated(keep=keep)].copy()


def _with_output_columns(frame: pd.DataFrame, outputs: dict[str, object] | pd.DataFrame) -> pd.DataFrame:
    output_frame = outputs if isinstance(outputs, pd.DataFrame) else pd.DataFrame(outputs, index=frame.index)
    if output_frame.empty:
        return _deduplicate_columns(frame, keep="last")
    output_frame = _deduplicate_columns(output_frame, keep="last")
    result = _deduplicate_columns(frame, keep="last").drop(columns=list(output_frame.columns), errors="ignore").copy()
    return pd.concat([result, output_frame], axis=1).copy()


@dataclass(frozen=True)
class FeatureSet:
    frame: pd.DataFrame
    feature_columns: list[str]


class FeatureEngineer:
    def __init__(self, *, k_seasons: int = 4) -> None:
        from app.training.feature_builders.rolling_windows import RollingWindowBuilder

        self._rolling_windows = RollingWindowBuilder(k_seasons=k_seasons)

    def build_training_frame(
        self,
        historical: pd.DataFrame,
        *,
        fill_terminal_nan: float | None = 0.0,
    ) -> FeatureSet:
        frame = self._prepare_base_frame(historical)
        if frame.empty:
            return FeatureSet(frame=frame, feature_columns=[])

        frame = self._attach_team_travel_schedule_distance(frame)

        rolling_result = self._rolling_windows.build_player_history_features(
            frame,
            fill_terminal_nan=fill_terminal_nan,
        )
        feature_map, anomaly_mask = rolling_result
        days_rest = pd.to_numeric(
            frame.get("player_days_since_last_game", self._days_rest(frame)),
            errors="coerce",
        ).fillna(self._days_rest(frame))
        feature_map["days_rest"] = days_rest.clip(lower=0, upper=14)
        feature_map["back_to_back"] = (feature_map["days_rest"] <= 1).astype(int)
        feature_map["player_days_since_last_game"] = feature_map["days_rest"]
        feature_map["days_since_extended_absence"] = pd.to_numeric(
            frame.get("days_since_extended_absence", feature_map["days_rest"] - 10.0),
            errors="coerce",
        ).fillna((feature_map["days_rest"] - 10.0).clip(lower=0.0)).clip(lower=0.0)
        feature_map["player_games_since_return"] = pd.to_numeric(
            frame.get("player_games_since_return", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        feature_map["player_injury_return_flag"] = pd.to_numeric(
            frame.get("player_injury_return_flag", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        feature_map["team_changed_recently"] = pd.to_numeric(
            frame.get("team_changed_recently", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        travel_km = pd.to_numeric(frame.get("travel_distance_km", 0.0), errors="coerce").fillna(0.0)
        feature_map["travel_distance_km"] = travel_km
        b2b_f = feature_map["back_to_back"].astype(float)
        feature_map["travel_fatigue_score"] = b2b_f * np.log1p(travel_km / 1000.0)
        feature_map["long_haul_travel_leg"] = b2b_f * (travel_km > 2000.0).astype(float)
        feature_map["starter_flag"] = frame["starter_flag"].fillna(False).astype(int)
        feature_map["is_guard"] = (frame["position_group"] == "G").astype(int)
        feature_map["is_forward"] = (frame["position_group"] == "F").astype(int)
        feature_map["is_center"] = (frame["position_group"] == "C").astype(int)
        feature_map["minutes_volatility"] = feature_map.get("minutes_std_10", pd.Series(0.0, index=frame.index))
        feature_map["history_games_played"] = frame.groupby("player_id").cumcount()
        feature_map["history_minutes_played"] = (
            frame.groupby("player_id")["minutes"].transform(lambda series: series.shift(1).cumsum()).fillna(0.0)
        )
        feature_map["usage_proxy"] = (
            feature_map.get("field_goal_attempts_avg_10", pd.Series(0.0, index=frame.index))
            + 0.44 * feature_map.get("free_throw_attempts_avg_10", pd.Series(0.0, index=frame.index))
        )
        feature_map["consistency_score"] = 1.0 / (
            1.0
            + feature_map.get("points_std_10", pd.Series(0.0, index=frame.index))
            + feature_map.get("assists_std_10", pd.Series(0.0, index=frame.index))
            + feature_map.get("rebounds_std_10", pd.Series(0.0, index=frame.index))
        )
        feature_map["starter_consistency_10"] = (
            frame.groupby("player_id")["starter_flag"]
            .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
            .fillna(0.5)
        )
        min_floor = frame.groupby("player_id")["minutes"].transform(
            lambda series: series.shift(1).rolling(10, min_periods=3).quantile(0.2)
        )
        feature_map["min_minutes_floor_10"] = pd.to_numeric(min_floor, errors="coerce").fillna(0.0)
        feature_map["minutes_floor_reliability"] = feature_map["min_minutes_floor_10"] * feature_map["starter_consistency_10"]

        shift_fouls = frame.groupby("player_id")["fouls"].transform(lambda series: series.shift(1))
        shift_minutes = frame.groupby("player_id")["minutes"].transform(lambda series: series.shift(1)).clip(lower=1.0)
        fouls_per_minute = shift_fouls / shift_minutes
        feature_map["foul_rate_10"] = fouls_per_minute.groupby(frame["player_id"]).transform(
            lambda series: series.rolling(10, min_periods=3).mean()
        ).fillna(0.0)
        feature_map["high_foul_risk"] = (feature_map["foul_rate_10"] > 0.12).astype(int)

        temporary = _with_output_columns(frame.drop(columns=["starter_flag"]), feature_map)
        feature_map["recent_role_delta"] = (
            np.abs(
                pd.to_numeric(temporary.get("minutes_avg_5", 0.0), errors="coerce").fillna(0.0)
                - pd.to_numeric(temporary.get("minutes_avg_20", temporary.get("minutes_avg_10", 0.0)), errors="coerce").fillna(0.0)
            )
            / np.maximum(pd.to_numeric(temporary.get("minutes_avg_20", 0.0), errors="coerce").fillna(0.0), 1.0)
        ).clip(0.0, 2.0)
        feature_map["role_stability"] = self._role_stability(temporary, predicted_minutes_column=None)

        # v1.2.2 Step 3: Points-calibration helper features.
        # "points_pace_exposure" — rolling pace × usage proxy, captures how many
        # possessions a player consumes in fast vs slow games.  The product scales
        # naturally with scoring opportunity and anchors the regression to pace.
        # "points_3pt_variance" — rolling 3PT attempt variance. Players with high
        # variance here have boom/bust distributions that a Gaussian model
        # underestimates. Exposing this to the regression lets XGBoost learn wider
        # residual bands for streaky shooters, directly attacking the high RMSE for
        # points (5.70) and poor coverage (84 %) seen in the v1.2.1 backtest.
        blended_pace = pd.to_numeric(
            feature_map.get("blended_game_pace", pd.Series(95.0, index=frame.index)),
            errors="coerce",
        ).fillna(95.0)
        usage_proxy_vals = pd.to_numeric(feature_map["usage_proxy"], errors="coerce").fillna(0.0)
        feature_map["points_pace_exposure"] = (blended_pace * usage_proxy_vals).clip(lower=0.0)

        # threes_std_10 is already computed by _build_player_history_features above;
        # squaring it gives variance units and amplifies signal for boom/bust shooters.
        threes_std = pd.to_numeric(
            feature_map.get("threes_std_10", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        feature_map["points_3pt_variance"] = threes_std ** 2

        # ── Team composition / role expansion features ──────────────────────────
        # Captures how much a player's statistical opportunity grows when key
        # teammates are absent.  Existing columns (teammate_absence_pressure,
        # same_position_out_count) count absences but don't translate them into
        # per-player opportunity lift.  These features do.
        #
        # All inputs come from AVAILABILITY_CONTEXT_FIELDS (attached at data-load
        # time), so they are valid at both training time and inference time.
        #
        # role_expansion_score
        #   = usage_proxy × (1 + vacancy_rate + position_bonus)
        #   Intuition: a high-usage guard absorbs more vacated touches than a
        #   low-usage center when two guards are ruled out.
        #   vacancy_rate  — fraction of team's absence pressure ceiling consumed
        #                   (teammate_absence_pressure is 0-8, cap at 0.6 = 60%)
        #   position_bonus — extra lift when absent teammates share the player's
        #                    position group (direct competition for possessions)
        #
        # positional_opportunity_index
        #   Ratio of same-position absences to total absences.  A value of 1.0
        #   means all missing teammates are in this player's position — maximum
        #   direct opportunity transfer.  0.0 means absences are entirely in other
        #   positions — indirect effect only.  Used as an interaction term by XGB.
        teammate_absence_pressure = pd.to_numeric(
            frame.get("teammate_absence_pressure", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        same_position_out = pd.to_numeric(
            frame.get("same_position_out_count", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        team_out_total = pd.to_numeric(
            frame.get("team_out_count", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)

        # v1.2.3 A2: prefer production-weighted vacancy when available.
        # missing_teammate_usage_sum is populated when game-log FGA/FTA columns
        # exist (training path).  For inference (upcoming games), it is 0.0 and
        # we fall back to the count-based teammate_absence_pressure signal.
        # Normalisation: ~20 = FGA+0.44*FTA for a heavy-usage star (~18 FGA+5 FTA)
        missing_usage = pd.to_numeric(
            frame.get("missing_teammate_usage_sum", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        star_absent = pd.to_numeric(
            frame.get("star_absent_flag", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        production_vacancy = (missing_usage / 20.0).clip(0.0, 0.6)
        count_vacancy = (teammate_absence_pressure / 8.0).clip(0.0, 0.6)
        vacancy_rate = pd.Series(
            np.where(missing_usage > 0, production_vacancy, count_vacancy),
            index=frame.index,
        )
        # Star-absent bonus: extra lift when a high-usage teammate is confirmed out.
        # A star absence opens disproportionate offensive opportunities for remaining players.
        star_bonus = (star_absent * 0.15).clip(0.0, 0.15)
        position_bonus = (same_position_out * 0.2).clip(0.0, 0.4)
        feature_map["role_expansion_score"] = (
            usage_proxy_vals * (1.0 + vacancy_rate + position_bonus + star_bonus)
        ).clip(lower=0.0)

        # Positional opportunity index: fraction of confirmed-out teammates that
        # share this player's position. Safe-div returns 0.0 when nobody is out.
        feature_map["positional_opportunity_index"] = np.where(
            team_out_total > 0,
            (same_position_out / team_out_total.clip(lower=1.0)).clip(0.0, 1.0),
            0.0,
        )

        profile_bucket = self._profile_bucket(frame["position_group"], feature_map["usage_proxy"])
        feature_map["likely_matchup_player_id"] = self._historical_matchup_players(temporary.assign(profile_bucket=profile_bucket))
        frame = _with_output_columns(frame.drop(columns=["starter_flag"]), feature_map)
        frame["profile_bucket"] = profile_bucket

        matchup_features = self._build_matchup_features(frame)
        frame = _with_output_columns(frame, matchup_features)

        # v1.2.3 B3: Opponent defensive disruption.
        # opponent_allowed_{market}_10 reflects the opponent's recent defensive
        # performance, but ignores whether *their* key defenders are injured.
        # A team missing its best perimeter defender will allow more 3s; a team
        # missing its rim protector will concede more points in the paint.
        # We capture this as opponent_lineup_disruption: the opponent team's own
        # lineup_instability_score in this specific game.  XGBoost can interact
        # this with the per-market opponent_allowed features to widen predictions
        # when the opponent is disrupted.
        #
        # Implementation: for each (game_id, team_id) compute the team's
        # lineup_instability_score (constant across all players on that team in
        # that game), then look up each row's *opponent* team_id in that mapping.
        # Falls back to 0.0 if the opponent has no availability data.
        frame = add_opponent_lineup_disruption(frame)

        frame = self._add_environment_features(frame)
        frame = self._add_blended_features(frame)
        frame = self.apply_post_minutes_features(frame, minutes_column="minutes_blended")
        frame = add_market_group_features(frame)
        frame = _deduplicate_columns(frame, keep="last")
        # Attach anomaly mask so pipeline.train() can drop these rows as training targets.
        # Reindex to match frame after deduplication.
        frame["_is_anomalous"] = anomaly_mask.reindex(frame.index).fillna(False).astype(bool)
        feature_columns = self._feature_columns(frame)
        return FeatureSet(frame=frame.reset_index(drop=True), feature_columns=feature_columns)

    def build_population_priors(
        self,
        frame: pd.DataFrame,
        feature_columns: list[str],
    ) -> dict[str, Any]:
        numeric_columns = [column for column in feature_columns if column in frame.columns]
        if not numeric_columns:
            return {
                "global_feature_priors": {},
                "position_feature_priors": {},
                "role_feature_priors": {},
                "role_bucket_thresholds": dict(ROLE_BUCKET_LINE_THRESHOLDS),
            }

        selected_columns = list(dict.fromkeys(["position_group", "starter_flag", "minutes", *numeric_columns]))
        priors_frame = frame[selected_columns].copy()
        priors_frame["position_group"] = priors_frame["position_group"].fillna("UNK").astype(str).str.upper().str[:1]
        priors_frame["starter_flag"] = role_prior_flag_series(priors_frame)
        priors_frame["role_bucket"] = [
            role_bucket_label(position_group, starter_flag)
            for position_group, starter_flag in zip(
                priors_frame["position_group"],
                priors_frame["starter_flag"],
                strict=False,
            )
        ]
        for column in numeric_columns:
            priors_frame[column] = pd.to_numeric(priors_frame[column], errors="coerce")

        global_feature_priors = priors_frame[numeric_columns].mean().fillna(0.0).astype(float).to_dict()
        position_feature_priors = {
            str(position_group): group[numeric_columns].mean().fillna(0.0).astype(float).to_dict()
            for position_group, group in priors_frame.groupby("position_group", dropna=False)
        }
        role_feature_priors = {
            str(role_bucket): group[numeric_columns].mean().fillna(0.0).astype(float).to_dict()
            for role_bucket, group in priors_frame.groupby("role_bucket", dropna=False)
        }
        return {
            "global_feature_priors": global_feature_priors,
            "position_feature_priors": position_feature_priors,
            "role_feature_priors": role_feature_priors,
            "role_bucket_thresholds": dict(ROLE_BUCKET_LINE_THRESHOLDS),
        }

    def build_inference_frame(
        self,
        historical: pd.DataFrame,
        upcoming: pd.DataFrame,
        population_priors: dict[str, Any] | None = None,
    ) -> FeatureSet:
        if upcoming.empty:
            return FeatureSet(frame=upcoming.copy(), feature_columns=[])
        # fill_terminal_nan=None: preserve NaNs through cascading fill so that
        # _fill_with_population_priors() (below) can replace them with role-bucketed
        # group averages instead of 0.0.  Training uses 0.0 as terminal fill, so
        # this creates a deliberate train-inference asymmetry for tier-C/D players
        # where population priors are strictly more informative than zeros.
        training_frame = self.build_training_frame(historical, fill_terminal_nan=None).frame
        if training_frame.empty:
            return FeatureSet(frame=upcoming.copy(), feature_columns=[])
        sort_columns = ["game_date", "start_time", "game_id", "player_id"]
        ordered = training_frame.sort_values(sort_columns)
        # Use last non-anomalous game per player for inference context so that a
        # load-management or early-exit game doesn't poison _prev features for
        # the upcoming prediction.
        clean_ordered = ordered[~ordered.get("_is_anomalous", pd.Series(False, index=ordered.index)).astype(bool)]
        latest_per_player = (clean_ordered if not clean_ordered.empty else ordered).groupby("player_id").tail(1).copy()
        player_feature_columns = [
            column
            for column in latest_per_player.columns
            if (
                column.endswith("_prev")
                or "_avg_" in column
                or "_std_" in column
                or "_ewm_" in column
                or "_season_avg" in column
                or column.endswith("_blended")
                or column.startswith("recent_hit_rate_")
                or column.startswith("opponent_allowed_")
                or column.startswith("opponent_position_allowed_")
                or column.startswith("opponent_similar_allowed_")
                or column.startswith("vs_opponent_")
                or column.startswith("vs_matchup_player_")
                or column.endswith("_line_delta_5")
                or "_group_" in column
                or "home_away_" in column
                or column.startswith("travel_")
                or column == "travel_distance_km"
                or column == "long_haul_travel_leg"
                or "minutes_floor" in column
                or "foul_rate" in column
                or column == "high_foul_risk"
            )
            and column not in {"game_id", "game_date", "start_time"}
            or column in {
                "usage_proxy",
                "consistency_score",
                "minutes_volatility",
                "team_injuries",
                "starter_flag",
                "starter_consistency_10",
                "role_stability",
                "history_games_played",
                "history_minutes_played",
                "recent_role_delta",
                "is_guard",
                "is_forward",
                "is_center",
                "position_group",
                "profile_bucket",
                "team_pace_avg_10",
                "opponent_pace_avg_10",
                "blended_game_pace",
                "expected_possessions",
                "home_team_abbreviation",
                *AVAILABILITY_SIGNAL_COLUMNS,
            }
        ]
        _ctx_feats = sorted(set(player_feature_columns))
        if "home_team_abbreviation" in latest_per_player.columns:
            _ctx_feats = ["home_team_abbreviation", *[c for c in _ctx_feats if c != "home_team_abbreviation"]]
        _ctx_rename: dict[str, str] = {"game_date": "last_game_date"}
        if "home_team_abbreviation" in latest_per_player.columns:
            _ctx_rename["home_team_abbreviation"] = "last_game_venue_abbr"
        player_context = latest_per_player[["player_id", "game_date", *_ctx_feats]].rename(columns=_ctx_rename)
        merged = upcoming.merge(player_context, on="player_id", how="left")
        merged["position_group"] = merged["position"].fillna("").astype(str).str.upper().str[:1].replace("", "UNK")
        merged["last_game_date"] = pd.to_datetime(merged["last_game_date"])
        fallback_days_rest = (merged["game_date"] - merged["last_game_date"]).dt.days.fillna(3).clip(lower=0)
        merged["player_days_since_last_game"] = pd.to_numeric(
            merged.get("player_days_since_last_game", fallback_days_rest),
            errors="coerce",
        ).fillna(fallback_days_rest).clip(lower=0, upper=14)
        merged["days_rest"] = merged["player_days_since_last_game"]
        merged["days_since_extended_absence"] = pd.to_numeric(
            merged.get("days_since_extended_absence", merged["days_rest"] - 10.0),
            errors="coerce",
        ).fillna((merged["days_rest"] - 10.0).clip(lower=0.0)).clip(lower=0.0)
        merged["player_games_since_return"] = pd.to_numeric(
            merged.get("player_games_since_return", pd.Series(0.0, index=merged.index)),
            errors="coerce",
        ).fillna(0.0)
        merged["player_injury_return_flag"] = pd.to_numeric(
            merged.get("player_injury_return_flag", pd.Series(0.0, index=merged.index)),
            errors="coerce",
        ).fillna(0.0)
        merged["team_changed_recently"] = pd.to_numeric(
            merged.get(
                "team_changed_recently",
                merged.get("_team_changed", pd.Series(0.0, index=merged.index)),
            ),
            errors="coerce",
        ).fillna(0.0)
        merged["back_to_back"] = (merged["days_rest"] <= 1).astype(int)
        merged["travel_distance_km"] = self._inference_travel_distance_km(merged)
        _tkm = pd.to_numeric(merged["travel_distance_km"], errors="coerce").fillna(0.0)
        merged["travel_fatigue_score"] = merged["back_to_back"].astype(float) * np.log1p(_tkm / 1000.0)
        merged["long_haul_travel_leg"] = merged["back_to_back"].astype(float) * (_tkm > 2000.0).astype(float)
        merged["starter_flag"] = merged["starter_flag"].fillna(0).astype(int)
        merged["is_guard"] = (merged["position_group"] == "G").astype(int)
        merged["is_forward"] = (merged["position_group"] == "F").astype(int)
        merged["is_center"] = (merged["position_group"] == "C").astype(int)
        merged["profile_bucket"] = merged["profile_bucket"].fillna(
            self._profile_bucket(merged["position_group"], merged["usage_proxy"].fillna(0.0))
        )

        merged = self._merge_lookup_features(merged, ordered)
        merged = self._merge_matchup_history(merged, ordered)
        merged = self._attach_current_lines(merged, upcoming)
        merged = self._attach_current_market_odds_context(merged)
        merged = self._finalize_inference_frame(merged)
        feature_columns = self._feature_columns(training_frame)
        missing_columns = [column for column in feature_columns if column not in merged.columns]
        if missing_columns:
            merged = pd.concat(
                [merged, pd.DataFrame(0.0, index=merged.index, columns=missing_columns)],
                axis=1,
            )
        merged = _deduplicate_columns(merged, keep="last")
        merged = self._fill_with_population_priors(
            merged,
            population_priors,
            "_data_sufficiency_tier",
            feature_columns,
        )
        merged = self._attach_current_market_odds_context(merged)
        merged[feature_columns] = merged[feature_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        return FeatureSet(frame=merged.reset_index(drop=True), feature_columns=feature_columns)

    def apply_post_minutes_features(self, frame: pd.DataFrame, minutes_column: str = "predicted_minutes") -> pd.DataFrame:
        result = _deduplicate_columns(frame, keep="last")
        if "blended_game_pace" not in result.columns:
            result = self._add_environment_features(result)
        minutes_source = result.get(minutes_column, result.get("minutes_avg_10", pd.Series(0.0, index=result.index)))
        result["expected_possessions"] = clip_non_negative(
            np.asarray(result["blended_game_pace"].fillna(96.0) * np.maximum(minutes_source, 0.0) / 48.0, dtype=float)
        )
        predicted_minutes_std = pd.to_numeric(
            result.get("predicted_minutes_std", pd.Series(0.0, index=result.index)),
            errors="coerce",
        ).fillna(0.0)
        result["minutes_uncertainty_ratio"] = predicted_minutes_std / np.maximum(np.maximum(minutes_source, 0.0), 1.0)
        result["role_stability"] = self._role_stability(result, predicted_minutes_column=minutes_column)
        result = self._add_blended_features(result)
        result = add_market_group_features(result)
        return result

    def _prepare_base_frame(self, historical: pd.DataFrame) -> pd.DataFrame:
        frame = _deduplicate_columns(historical, keep="last")
        if frame.empty:
            return frame
        frame = frame.sort_values(["player_id", "game_date", "start_time", "game_id"]).reset_index(drop=True)
        frame["position_group"] = frame["position"].fillna("").astype(str).str.upper().str[:1].replace("", "UNK")
        frame["starter_flag"] = frame["starter_flag"].fillna(False).astype(int)
        percentage_columns = [
            column
            for column in frame.columns
            if column.startswith("percentage_") or column.endswith("_percentage")
        ]
        for column in percentage_columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").apply(_normalize_percentage)
        frame["pace_proxy"] = _coalesce_columns(frame, ["estimated_pace", "pace"], default=np.nan)
        pace_from_possessions = np.where(
            frame["minutes"].fillna(0.0) > 0.0,
            frame["possessions"].fillna(0.0) * 48.0 / frame["minutes"].replace(0, np.nan),
            np.nan,
        )
        frame["pace_proxy"] = pd.Series(frame["pace_proxy"]).fillna(pd.Series(pace_from_possessions)).fillna(96.0)
        frame["usage_rate"] = _coalesce_columns(frame, ["estimated_usage_percentage", "usage_percentage"], default=0.20)
        frame["rebound_chances_total"] = _coalesce_columns(
            frame,
            ["rebound_chances_total", "rebound_chances_offensive", "rebound_chances_defensive"],
            default=0.0,
        )
        frame["assist_creation_proxy"] = (
            frame["assists"].fillna(0.0)
            + frame["secondary_assists"].fillna(0.0)
            + frame["free_throw_assists"].fillna(0.0)
        )
        frame["estimated_three_point_attempts"] = (
            frame["field_goal_attempts"].fillna(0.0) * frame["percentage_field_goals_attempted_3pt"].fillna(0.0)
        )
        frame["estimated_two_point_attempts"] = (
            frame["field_goal_attempts"].fillna(0.0) * frame["percentage_field_goals_attempted_2pt"].fillna(0.0)
        )
        frame["scoring_opportunities"] = frame["field_goal_attempts"].fillna(0.0) + 0.44 * frame["free_throw_attempts"].fillna(0.0)
        frame["rebound_conversion_rate"] = _safe_rate(frame["rebounds"], frame["rebound_chances_total"])
        frame["pra"] = frame["points"] + frame["rebounds"] + frame["assists"]
        # P2 CHANGE 5: Define physical maximums for per-minute rates to prevent feature explosion from low minutes
        _PER_MINUTE_CAPS: dict[str, float] = {
            "points_per_minute": 1.5,
            "rebounds_per_minute": 0.8,
            "assists_per_minute": 0.6,
            "threes_per_minute": 0.4,
            "turnovers_per_minute": 0.4,
            "pra_per_minute": 2.5,
            "field_goal_attempts_per_minute": 1.0,
            "free_throw_attempts_per_minute": 0.8,
            "estimated_three_point_attempts_per_minute": 0.6,
            "touches_per_minute": 5.0,
            "passes_per_minute": 4.0,
            "assist_creation_proxy_per_minute": 0.8,
            "rebound_chances_total_per_minute": 1.0,
        }

        frame["points_per_minute"] = _safe_rate(frame["points"], frame["minutes"])
        frame["rebounds_per_minute"] = _safe_rate(frame["rebounds"], frame["minutes"])
        frame["assists_per_minute"] = _safe_rate(frame["assists"], frame["minutes"])
        frame["threes_per_minute"] = _safe_rate(frame["threes"], frame["minutes"])
        frame["turnovers_per_minute"] = _safe_rate(frame["turnovers"], frame["minutes"])
        frame["pra_per_minute"] = _safe_rate(frame["pra"], frame["minutes"])
        frame["field_goal_attempts_per_minute"] = _safe_rate(frame["field_goal_attempts"], frame["minutes"])
        frame["free_throw_attempts_per_minute"] = _safe_rate(frame["free_throw_attempts"], frame["minutes"])
        frame["estimated_three_point_attempts_per_minute"] = _safe_rate(frame["estimated_three_point_attempts"], frame["minutes"])
        frame["touches_per_minute"] = _safe_rate(frame["touches"], frame["minutes"])
        frame["passes_per_minute"] = _safe_rate(frame["passes"], frame["minutes"])
        frame["assist_creation_proxy_per_minute"] = _safe_rate(frame["assist_creation_proxy"], frame["minutes"])
        frame["rebound_chances_total_per_minute"] = _safe_rate(frame["rebound_chances_total"], frame["minutes"])

        # Cap per-minute rates at physical maximums. A player who played 1 minute and scored 2 points gets
        # points_per_minute=2.0 (normal is ~0.6). These outliers create feature values the model has never seen,
        # producing unpredictable outputs.
        for col, cap in _PER_MINUTE_CAPS.items():
            if col in frame.columns:
                frame[col] = frame[col].clip(upper=cap)
        possessions = frame["possessions"].replace(0, np.nan)
        frame["points_per_possession"] = _safe_rate(frame["points"], possessions)
        frame["rebounds_per_possession"] = _safe_rate(frame["rebounds"], possessions)
        frame["assists_per_possession"] = _safe_rate(frame["assists"], possessions)
        frame["threes_per_possession"] = _safe_rate(frame["threes"], possessions)
        frame["turnovers_per_possession"] = _safe_rate(frame["turnovers"], possessions)
        return frame

    def _build_matchup_features(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        features: dict[str, pd.Series] = {}
        for market_key, target_column in MARKET_TARGETS.items():
            player_vs_opponent = frame.groupby(["player_id", "opponent_team_id"])[target_column]
            features[f"vs_opponent_{market_key}_avg_3"] = (
                player_vs_opponent.transform(lambda series: series.shift(1).rolling(3, min_periods=1).mean()).fillna(0.0)
            )
            opponent_allowed = frame.groupby("opponent_team_id")[target_column]
            features[f"opponent_allowed_{market_key}_10"] = (
                opponent_allowed.transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean()).fillna(frame[target_column].mean())
            )
            rate_column = f"{target_column}_per_possession"
            if rate_column in frame.columns:
                opponent_allowed_rate = frame.groupby("opponent_team_id")[rate_column]
                features[f"opponent_allowed_{market_key}_per_possession_10"] = (
                    opponent_allowed_rate
                    .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
                    .fillna(frame[rate_column].mean())
                )
            opponent_position_allowed = frame.groupby(["opponent_team_id", "position_group"])[target_column]
            features[f"opponent_position_allowed_{market_key}_10"] = (
                opponent_position_allowed
                .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
                .fillna(features[f"opponent_allowed_{market_key}_10"])
            )
            opponent_similar_allowed = frame.groupby(["opponent_team_id", "profile_bucket"])[target_column]
            features[f"opponent_similar_allowed_{market_key}_10"] = (
                opponent_similar_allowed
                .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
                .fillna(features[f"opponent_position_allowed_{market_key}_10"])
            )
            player_vs_matchup = frame.groupby(["player_id", "likely_matchup_player_id"])[target_column]
            features[f"vs_matchup_player_{market_key}_avg_3"] = (
                player_vs_matchup.transform(lambda series: series.shift(1).rolling(3, min_periods=1).mean())
                .fillna(features[f"vs_opponent_{market_key}_avg_3"])
            )
            line_column = f"line_{market_key}"
            line_values = frame.get(line_column, pd.Series(0.0, index=frame.index)).fillna(0.0)
            features[f"{market_key}_line_delta_5"] = line_values - frame[f"{target_column}_avg_5"]
            hit_rate = (frame[target_column] > line_values).astype(float)
            hit_rate = hit_rate.where(line_values > 0, np.nan)
            features[f"recent_hit_rate_{market_key}_10"] = (
                hit_rate.groupby(frame["player_id"])
                .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
                .fillna(0.5)
            )
        return features

    def _add_environment_features(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        if "pace_proxy_avg_10" in result.columns:
            pace_source = result["pace_proxy_avg_10"]
        else:
            pace_source = result.get("pace_proxy", pd.Series(96.0, index=result.index))
        if pace_source.name in result.columns:
            result["team_pace_avg_10"] = (
                result.groupby("player_team_id")[pace_source.name]
                .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
                .fillna(pace_source)
            )
            result["opponent_pace_avg_10"] = (
                result.groupby("opponent_team_id")[pace_source.name]
                .transform(lambda series: series.shift(1).rolling(10, min_periods=1).mean())
                .fillna(float(pace_source.mean()) if len(pace_source) else 96.0)
            )
        else:
            result["team_pace_avg_10"] = pace_source
            result["opponent_pace_avg_10"] = pd.Series(96.0, index=result.index)
        market_pace_proxy = result.get("total", pd.Series(230.0, index=result.index)).fillna(230.0) / 4.8
        result["blended_game_pace"] = (
            0.45 * pd.to_numeric(result["team_pace_avg_10"], errors="coerce").fillna(96.0)
            + 0.45 * pd.to_numeric(result["opponent_pace_avg_10"], errors="coerce").fillna(96.0)
            + 0.10 * market_pace_proxy
        )
        if "expected_possessions" not in result.columns:
            result["expected_possessions"] = result["blended_game_pace"] * result.get("minutes_avg_10", 0.0) / 48.0
        return result

    def _add_blended_features(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        role = result.get("role_stability", pd.Series(0.5, index=result.index)).fillna(0.5).clip(0.0, 1.0)
        candidate_metrics = {
            column.rsplit("_avg_", 1)[0]
            for column in result.columns
            if "_avg_5" in column
        }
        for metric in sorted(candidate_metrics):
            avg_5 = result.get(f"{metric}_avg_5", pd.Series(0.0, index=result.index))
            avg_10 = result.get(f"{metric}_avg_10", avg_5)
            avg_20 = result.get(f"{metric}_avg_20", avg_10)
            ewm_10 = result.get(f"{metric}_ewm_10", avg_10)
            season = result.get(f"{metric}_season_avg", avg_20)
            short_horizon = 0.55 * avg_5 + 0.45 * ewm_10
            long_horizon = 0.65 * avg_20 + 0.35 * season
            result[f"{metric}_blended"] = (1.0 - role) * short_horizon + role * long_horizon
        return result

    def _attach_team_travel_schedule_distance(self, frame: pd.DataFrame) -> pd.DataFrame:
        """B2: km between this game's arena and the team's previous game arena."""
        if frame.empty:
            return frame.assign(travel_distance_km=0.0)
        if (
            "home_team_abbreviation" not in frame.columns
            or "player_team_id" not in frame.columns
            or "game_id" not in frame.columns
        ):
            return frame.assign(travel_distance_km=0.0)
        sched = frame[["player_team_id", "game_id", "game_date", "start_time", "home_team_abbreviation"]].drop_duplicates()
        sched = sched.sort_values(["player_team_id", "game_date", "start_time", "game_id"], kind="mergesort")
        lat, lon = _arena_coords_series(sched["home_team_abbreviation"])
        sched = sched.assign(_lat=lat.to_numpy(dtype=float), _lon=lon.to_numpy(dtype=float))
        prev_lat = sched.groupby("player_team_id", sort=False)["_lat"].shift(1)
        prev_lon = sched.groupby("player_team_id", sort=False)["_lon"].shift(1)
        sched["travel_distance_km"] = _haversine_km_series(prev_lat, prev_lon, sched["_lat"], sched["_lon"])
        merged = frame.merge(
            sched[["player_team_id", "game_id", "travel_distance_km"]],
            on=["player_team_id", "game_id"],
            how="left",
        )
        merged["travel_distance_km"] = pd.to_numeric(merged["travel_distance_km"], errors="coerce").fillna(0.0)
        return merged

    def _inference_travel_distance_km(self, frame: pd.DataFrame) -> pd.Series:
        last_abbr = frame.get("last_game_venue_abbr", pd.Series("", index=frame.index))
        cur_abbr = frame.get("home_team_abbreviation", pd.Series("", index=frame.index))
        la1, lo1 = _arena_coords_series(last_abbr)
        la2, lo2 = _arena_coords_series(cur_abbr)
        return _haversine_km_series(la1, lo1, la2, lo2)

    def _days_rest(self, frame: pd.DataFrame) -> pd.Series:
        previous = frame.groupby("player_id")["game_date"].shift(1)
        return (frame["game_date"] - previous).dt.days.fillna(4).clip(lower=0)

    def _role_stability(self, frame: pd.DataFrame, predicted_minutes_column: str | None) -> pd.Series:
        minutes_avg_10 = pd.to_numeric(frame.get("minutes_avg_10", 0.0), errors="coerce").fillna(0.0)
        minutes_avg_20 = pd.to_numeric(frame.get("minutes_avg_20", minutes_avg_10), errors="coerce").fillna(0.0)
        minutes_std_10 = pd.to_numeric(frame.get("minutes_std_10", 0.0), errors="coerce").fillna(0.0)
        starter_consistency = pd.to_numeric(frame.get("starter_consistency_10", 0.5), errors="coerce").fillna(0.5)
        team_injuries_avg_10 = pd.to_numeric(frame.get("team_injuries_avg_10", frame.get("team_injuries", 0.0)), errors="coerce").fillna(0.0)
        lineup_instability = pd.to_numeric(
            frame.get("lineup_instability_score", frame.get("lineup_instability_score_avg_10", 0.0)),
            errors="coerce",
        ).fillna(0.0)
        missing_starters = pd.to_numeric(
            frame.get("missing_starter_count", pd.Series(0.0, index=frame.index)),
            errors="coerce",
        ).fillna(0.0)
        if predicted_minutes_column and predicted_minutes_column in frame.columns:
            projected_minutes = pd.to_numeric(frame[predicted_minutes_column], errors="coerce").fillna(minutes_avg_10)
        else:
            projected_minutes = minutes_avg_10
        volatility = (minutes_std_10 / np.maximum(minutes_avg_10, 1.0)).clip(0.0, 2.0)
        starter_certainty = (2.0 * np.abs(starter_consistency - 0.5)).clip(0.0, 1.0)
        projected_delta = (np.abs(projected_minutes - minutes_avg_20) / np.maximum(minutes_avg_20, 1.0)).clip(0.0, 2.0)
        injury_disruption = (team_injuries_avg_10 / 5.0).clip(0.0, 1.0)
        lineup_disruption = (0.6 * lineup_instability + 0.4 * (missing_starters / 5.0).clip(0.0, 1.0)).clip(0.0, 1.0)
        stability = (
            0.30 * (1.0 - volatility.clip(0.0, 1.0))
            + 0.30 * starter_certainty
            + 0.20 * (1.0 - projected_delta.clip(0.0, 1.0))
            + 0.10 * (1.0 - injury_disruption)
            + 0.10 * (1.0 - lineup_disruption)
        )
        return stability.clip(0.0, 1.0)

    def _profile_bucket(self, position_group: pd.Series, usage_proxy: pd.Series) -> pd.Series:
        safe_position = position_group.fillna("UNK").astype(str)
        usage_rank = usage_proxy.groupby(safe_position).rank(method="first", pct=True).fillna(0.0)
        bucket = np.clip((usage_rank * 4).astype(int), 0, 3)
        return safe_position + "_" + bucket.astype(str)

    def _historical_matchup_players(self, frame: pd.DataFrame) -> pd.Series:
        rows = frame.reset_index().rename(columns={"index": "row_id"})
        candidates = rows[
            ["row_id", "game_id", "player_id", "team_id", "position_group", "starter_flag", "minutes"]
        ].rename(
            columns={
                "row_id": "candidate_row_id",
                "player_id": "candidate_player_id",
                "team_id": "candidate_team_id",
                "starter_flag": "candidate_starter_flag",
                "minutes": "candidate_minutes",
            }
        )
        position_matches = rows[["row_id", "game_id", "opponent_team_id", "position_group"]].merge(
            candidates,
            on=["game_id", "position_group"],
            how="left",
        )
        position_matches = position_matches[
            position_matches["opponent_team_id"] == position_matches["candidate_team_id"]
        ]
        position_matches = position_matches.sort_values(
            ["row_id", "candidate_starter_flag", "candidate_minutes", "candidate_player_id"],
            ascending=[True, False, False, True],
        )
        best_position = position_matches.drop_duplicates(subset=["row_id"], keep="first")

        fallback_matches = rows[["row_id", "game_id", "opponent_team_id"]].merge(
            candidates.drop(columns=["position_group"]),
            on=["game_id"],
            how="left",
        )
        fallback_matches = fallback_matches[
            fallback_matches["opponent_team_id"] == fallback_matches["candidate_team_id"]
        ]
        fallback_matches = fallback_matches.sort_values(
            ["row_id", "candidate_starter_flag", "candidate_minutes", "candidate_player_id"],
            ascending=[True, False, False, True],
        )
        best_fallback = fallback_matches.drop_duplicates(subset=["row_id"], keep="first")

        merged = rows[["row_id"]].merge(
            best_position[["row_id", "candidate_player_id"]],
            on="row_id",
            how="left",
        ).merge(
            best_fallback[["row_id", "candidate_player_id"]].rename(
                columns={"candidate_player_id": "fallback_player_id"}
            ),
            on="row_id",
            how="left",
        )
        result = merged["candidate_player_id"].fillna(merged["fallback_player_id"])
        return result.astype("Int64")

    def _merge_lookup_features(self, merged: pd.DataFrame, ordered: pd.DataFrame) -> pd.DataFrame:
        opponent_columns = [column for column in ordered.columns if column.startswith("opponent_allowed_")]
        if opponent_columns:
            opponent_lookup = ordered.groupby("opponent_team_id").tail(1)[["opponent_team_id", *opponent_columns]]
            merged = merged.merge(opponent_lookup, on="opponent_team_id", how="left")

        opponent_position_columns = [
            column for column in ordered.columns if column.startswith("opponent_position_allowed_")
        ]
        if opponent_position_columns:
            position_lookup = ordered.groupby(["opponent_team_id", "position_group"]).tail(1)[
                ["opponent_team_id", "position_group", *opponent_position_columns]
            ]
            merged = merged.merge(position_lookup, on=["opponent_team_id", "position_group"], how="left")

        opponent_similar_columns = [
            column for column in ordered.columns if column.startswith("opponent_similar_allowed_")
        ]
        if opponent_similar_columns:
            similar_lookup = ordered.groupby(["opponent_team_id", "profile_bucket"]).tail(1)[
                ["opponent_team_id", "profile_bucket", *opponent_similar_columns]
            ]
            merged = merged.merge(similar_lookup, on=["opponent_team_id", "profile_bucket"], how="left")

        player_opponent_columns = [column for column in ordered.columns if column.startswith("vs_opponent_")]
        if player_opponent_columns:
            player_opponent_lookup = ordered.groupby(["player_id", "opponent_team_id"]).tail(1)[
                ["player_id", "opponent_team_id", *player_opponent_columns]
            ]
            merged = merged.merge(player_opponent_lookup, on=["player_id", "opponent_team_id"], how="left")
        return merged

    def _merge_matchup_history(self, merged: pd.DataFrame, ordered: pd.DataFrame) -> pd.DataFrame:
        player_rows = merged[
            ["game_id", "player_id", "team_id", "opponent_team_id", "position_group", "starter_flag", "minutes_avg_10"]
        ].drop_duplicates()
        candidates = player_rows[
            ["game_id", "player_id", "team_id", "position_group", "starter_flag", "minutes_avg_10"]
        ].rename(
            columns={
                "player_id": "candidate_player_id",
                "team_id": "candidate_team_id",
                "starter_flag": "candidate_starter_flag",
                "minutes_avg_10": "candidate_minutes_avg_10",
            }
        )
        position_matches = player_rows.merge(candidates, on=["game_id", "position_group"], how="left")
        position_matches = position_matches[
            position_matches["opponent_team_id"] == position_matches["candidate_team_id"]
        ]
        position_matches = position_matches.sort_values(
            ["player_id", "candidate_starter_flag", "candidate_minutes_avg_10", "candidate_player_id"],
            ascending=[True, False, False, True],
        )
        best_matchup = position_matches.drop_duplicates(subset=["player_id"], keep="first")[
            ["player_id", "candidate_player_id"]
        ].rename(columns={"candidate_player_id": "likely_matchup_player_id"})
        merged = merged.merge(best_matchup, on="player_id", how="left")

        player_matchup_columns = [column for column in ordered.columns if column.startswith("vs_matchup_player_")]
        if player_matchup_columns:
            player_matchup_lookup = ordered.groupby(["player_id", "likely_matchup_player_id"]).tail(1)[
                ["player_id", "likely_matchup_player_id", *player_matchup_columns]
            ]
            merged = merged.merge(
                player_matchup_lookup,
                on=["player_id", "likely_matchup_player_id"],
                how="left",
            )
        return merged

    def _attach_current_lines(self, merged: pd.DataFrame, upcoming: pd.DataFrame) -> pd.DataFrame:
        line_context = (
            upcoming[["game_id", "player_id", "market_key", "line_value"]]
            .drop_duplicates(subset=["game_id", "player_id", "market_key"], keep="last")
            .pivot_table(
                index=["game_id", "player_id"],
                columns="market_key",
                values="line_value",
                aggfunc="median",
            )
            .rename(columns=lambda market_key: f"line_{market_key}")
            .reset_index()
        )
        line_context.columns.name = None
        return merged.merge(line_context, on=["game_id", "player_id"], how="left")

    def _attach_current_market_odds_context(self, merged: pd.DataFrame) -> pd.DataFrame:
        result = merged.copy()
        updates: dict[str, pd.Series] = {}
        for market_key in MARKET_TARGETS:
            mask = result["market_key"] == market_key
            if not mask.any():
                continue
            line_series = result.get(f"line_{market_key}", pd.Series(np.nan, index=result.index, dtype=float)).copy()
            line_series.loc[mask] = result.loc[mask, "line_value"]
            updates[f"line_{market_key}"] = line_series
            for field_name in ODDS_MARKET_COLUMNS:
                column_name = f"{market_key}_{field_name}"
                field_series = result.get(column_name, pd.Series(0.0, index=result.index, dtype=float)).copy()
                if field_name in result.columns:
                    field_series.loc[mask] = result.loc[mask, field_name]
                updates[column_name] = field_series
        if updates:
            result = result.drop(columns=list(updates.keys()), errors="ignore")
            result = pd.concat([result, pd.DataFrame(updates, index=result.index)], axis=1)
        return _deduplicate_columns(result, keep="last")

    def _finalize_inference_frame(self, merged: pd.DataFrame) -> pd.DataFrame:
        result = merged.copy()
        updates: dict[str, pd.Series] = {}
        for market_key, target_column in MARKET_TARGETS.items():
            avg_5 = result.get(f"{target_column}_avg_5", pd.Series(0.0, index=result.index, dtype=float))
            avg_10 = result.get(f"{target_column}_avg_10", avg_5)
            opponent_column = f"opponent_allowed_{market_key}_10"
            position_column = f"opponent_position_allowed_{market_key}_10"
            similar_column = f"opponent_similar_allowed_{market_key}_10"
            opponent_possession_column = f"opponent_allowed_{market_key}_per_possession_10"
            vs_opponent_column = f"vs_opponent_{market_key}_avg_3"
            vs_matchup_column = f"vs_matchup_player_{market_key}_avg_3"
            recent_hit_column = f"recent_hit_rate_{market_key}_10"
            line_column = f"line_{market_key}"

            opponent_values = result.get(opponent_column, pd.Series(np.nan, index=result.index, dtype=float)).fillna(avg_10)
            updates[opponent_column] = opponent_values
            if opponent_possession_column not in result.columns:
                updates[opponent_possession_column] = _safe_rate(opponent_values, result.get("blended_game_pace", 96.0))
            updates[position_column] = (
                result.get(position_column, pd.Series(np.nan, index=result.index, dtype=float))
                .fillna(opponent_values)
            )
            updates[similar_column] = (
                result.get(similar_column, pd.Series(np.nan, index=result.index, dtype=float))
                .fillna(updates[position_column])
            )
            updates[vs_opponent_column] = (
                result.get(vs_opponent_column, pd.Series(np.nan, index=result.index, dtype=float)).fillna(avg_10)
            )
            updates[vs_matchup_column] = (
                result.get(vs_matchup_column, pd.Series(np.nan, index=result.index, dtype=float))
                .fillna(updates[vs_opponent_column])
            )
            updates[recent_hit_column] = (
                result.get(recent_hit_column, pd.Series(np.nan, index=result.index, dtype=float)).fillna(0.5)
            )
            average_column = f"{target_column}_avg_5"
            if average_column not in result.columns:
                updates[average_column] = pd.Series(0.0, index=result.index, dtype=float)
            line_values = result.get(line_column, pd.Series(np.nan, index=result.index, dtype=float))
            updates[line_column] = line_values
            average_values = updates.get(average_column, result.get(average_column, pd.Series(0.0, index=result.index, dtype=float)))
            updates[f"{market_key}_line_delta_5"] = (line_values - average_values).fillna(0.0)
            for odds_field in ODDS_MARKET_COLUMNS:
                column_name = f"{market_key}_{odds_field}"
                if column_name not in result.columns:
                    updates[column_name] = pd.Series(0.0, index=result.index, dtype=float)
        if updates:
            result = result.drop(columns=list(updates.keys()), errors="ignore")
            result = pd.concat([result, pd.DataFrame(updates, index=result.index)], axis=1)
        result = _deduplicate_columns(result, keep="last")
        numeric_columns = result.select_dtypes(include=["number", "bool"]).columns
        result[numeric_columns] = result[numeric_columns].fillna(0.0)
        result = self.apply_post_minutes_features(result, minutes_column="minutes_blended")
        return result

    def _fill_with_population_priors(
        self,
        frame: pd.DataFrame,
        priors: dict[str, Any] | None,
        tier_col: str,
        feature_columns: list[str],
    ) -> pd.DataFrame:
        if frame.empty or not priors:
            return frame
        result = frame.copy()
        available_columns = [column for column in feature_columns if column in result.columns]
        if not available_columns:
            return result

        position_feature_priors = priors.get("position_feature_priors", {})
        role_feature_priors = priors.get("role_feature_priors", {})
        global_feature_priors = priors.get("global_feature_priors", {})

        for idx, row in result.iterrows():
            tier = str(row.get(tier_col, "A"))
            if tier not in {"C", "D"}:
                continue

            position_group = str(row.get("position_group", "UNK") or "UNK").upper()[:1] or "UNK"
            position_values = position_feature_priors.get(position_group, {})
            role_values = (
                role_feature_priors.get(self._population_role_bucket_for_row(row, priors), {})
                if tier == "D"
                else {}
            )

            for column in available_columns:
                if tier == "D":
                    prior_value = role_values.get(column, position_values.get(column, global_feature_priors.get(column)))
                    if prior_value is not None and pd.notna(prior_value):
                        result.at[idx, column] = float(prior_value)
                elif pd.isna(result.at[idx, column]):
                    prior_value = position_values.get(column, global_feature_priors.get(column))
                    if prior_value is not None and pd.notna(prior_value):
                        result.at[idx, column] = float(prior_value)
        return result

    def _population_role_bucket_for_row(self, row: pd.Series, priors: dict[str, Any]) -> str:
        position_group = str(row.get("position_group", "UNK") or "UNK").upper()[:1] or "UNK"
        projected_starter = pd.to_numeric(pd.Series([row.get("projected_starter_flag")]), errors="coerce").iloc[0]
        if pd.notna(projected_starter):
            return role_bucket_label(position_group, projected_starter)

        line_value = pd.to_numeric(pd.Series([row.get("line_value")]), errors="coerce").iloc[0]
        market_key = str(row.get("market_key", "") or "").lower()
        threshold = float(priors.get("role_bucket_thresholds", {}).get(market_key, np.inf))
        if pd.notna(line_value) and line_value >= threshold:
            return role_bucket_label(position_group, 1.0)

        starter_flag = pd.to_numeric(pd.Series([row.get("starter_flag")]), errors="coerce").iloc[0]
        return role_bucket_label(position_group, 0.0 if pd.isna(starter_flag) else starter_flag)

    def _feature_columns(self, frame: pd.DataFrame) -> list[str]:
        feature_columns = [
            column
            for column in frame.columns
            if any(
                token in column
                for token in (
                    "_avg_",
                    "_std_",
                    "_ewm_",
                    "_season_avg",
                    "_prev",
                    "_blended",
                    "_group_",
                    "days_rest",
                    "back_to_back",
                    "is_home",
                    "spread",
                    "total",
                    "team_injuries",
                    "history_games_played",
                    "history_minutes_played",
                    "recent_role_delta",
                    "lineup_instability",
                    "teammate_absence_pressure",
                    "same_position_out",
                    "player_injury_return_flag",
                    "player_days_since_last_game",
                    "player_games_since_return",
                    "days_since_extended_absence",
                    "team_changed_recently",
                    "projected_",
                    "missing_starter",
                    "minutes_uncertainty",
                    "opponent_allowed_",
                    "opponent_position_allowed_",
                    "opponent_similar_allowed_",
                    "vs_opponent_",
                    "vs_matchup_player_",
                    "line_delta",
                    "usage_proxy",
                    "consistency_score",
                    "minutes_volatility",
                    "starter_flag",
                    "starter_consistency_10",
                    "role_stability",
                    "recent_hit_rate_",
                    "sportsbook_count",
                    "book_count",
                    "market_count",
                    "consensus_prob",
                    "line_movement_",
                    "expected_possessions",
                    "blended_game_pace",
                    "team_pace_avg_10",
                    "opponent_pace_avg_10",
                    "is_guard",
                    "is_forward",
                    "is_center",
                    "home_away_",
                    "travel_",
                    "long_haul",
                    "minutes_floor",
                    "foul_rate",
                    "high_foul",
                )
            )
        ]
        return sorted(set(feature_columns))


def clip_non_negative(values: np.ndarray) -> np.ndarray:
    return np.clip(values, 0.0, None)


def _safe_rate(numerator: pd.Series | np.ndarray | float, denominator: pd.Series | np.ndarray | float) -> pd.Series:
    numerator_series = pd.Series(numerator)
    denominator_series = pd.Series(denominator).replace(0, np.nan)
    return numerator_series.divide(denominator_series).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _normalize_percentage(value: Any) -> float:
    if value in (None, "") or pd.isna(value):
        return 0.0
    numeric = float(value)
    if numeric > 1.0:
        return numeric / 100.0
    return max(numeric, 0.0)


def _coalesce_columns(frame: pd.DataFrame, columns: list[str], default: float) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        result = result.fillna(values)
    return result.fillna(default)
