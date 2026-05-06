from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from app.training.constants import ROLLING_WINDOWS

logger = logging.getLogger(__name__)

# DNP anomaly guard: minimum minutes to consider a game valid for rolling windows.
# Games below this threshold get NaN'd out so they don't poison rolling averages.
_DNP_MIN_MINUTES = 5.0
# Per-player anomaly ratio: games where minutes < this fraction of the player's
# expanding median are treated as DNP-like (load management, injury exits, blowouts).
_DNP_ANOMALY_RATIO = 0.40
# Only apply anomaly detection for players whose median minutes exceed this floor
# (avoids false positives on bench players with legitimately low minutes).
_DNP_ANOMALY_MEDIAN_FLOOR = 12.0


class RollingFeatureResult(tuple):
    """Tuple-compatible rolling feature result with dict-like feature access."""

    def __new__(cls, features: dict[str, pd.Series], dnp_mask: pd.Series) -> RollingFeatureResult:
        return super().__new__(cls, (features, dnp_mask))

    @property
    def features(self) -> dict[str, pd.Series]:
        return super().__getitem__(0)

    @property
    def dnp_mask(self) -> pd.Series:
        return super().__getitem__(1)

    def __getitem__(self, item: object) -> object:
        if isinstance(item, str):
            return self.features[item]
        return super().__getitem__(item)

    def keys(self) -> object:
        return self.features.keys()

    def items(self) -> object:
        return self.features.items()

    def values(self) -> object:
        return self.features.values()

    def get(self, key: str, default: object = None) -> object:
        return self.features.get(key, default)

    def __contains__(self, item: object) -> bool:
        return item in self.features


class RollingWindowBuilder:
    """Shifted rolling / EWM / expanding aggregates per player (training frame).

    Extracted from FeatureEngineer for testability; behavior must stay aligned with
    historical characterization tests in ``tests/unit/test_rolling_window_builder.py``.
    """

    def __init__(self, *, k_seasons: int = 4) -> None:
        # k_seasons tunes effective rolling lookback while preserving feature names.
        self._k_seasons = max(1, int(k_seasons))

    def build_player_history_features(
        self,
        frame: pd.DataFrame,
        *,
        fill_terminal_nan: float | None = 0.0,
    ) -> RollingFeatureResult:
        from app.training.features import (
            AVAILABILITY_SIGNAL_COLUMNS,
            DERIVED_SIGNAL_COLUMNS,
            PLAYER_SIGNAL_COLUMNS,
        )

        features: dict[str, pd.Series] = {}
        metrics = [
            "minutes",
            "points",
            "rebounds",
            "assists",
            "threes",
            "turnovers",
            "pra",
            "field_goal_attempts",
            "field_goals_made",
            "free_throw_attempts",
            "free_throws_made",
            "offensive_rebounds",
            "defensive_rebounds",
            "plus_minus",
            "fouls",
            *[column for column in AVAILABILITY_SIGNAL_COLUMNS if column in frame.columns],
            *[column for column in PLAYER_SIGNAL_COLUMNS if column in frame.columns],
            *[column for column in DERIVED_SIGNAL_COLUMNS if column in frame.columns],
        ]
        # DNP anomaly guard: mask games where the player played < _DNP_MIN_MINUTES.
        # These rows get NaN'd in the rolling source so they don't drag averages
        # toward zero (the root cause of extreme <3% probabilities).
        dnp_mask = pd.Series(False, index=frame.index)
        if "minutes" in frame.columns:
            mins = frame["minutes"].fillna(0)
            dnp_mask = mins < _DNP_MIN_MINUTES

            # Per-player anomaly detection: flag games where minutes are far below
            # the player's typical playing time (load management, injury exits,
            # blowout benchings). Uses expanding median of PRIOR games (shifted)
            # to avoid leakage.
            player_median_minutes = frame.groupby("player_id")["minutes"].transform(
                lambda s: s.shift(1).expanding(min_periods=3).median()
            )
            anomaly_threshold = _DNP_ANOMALY_RATIO * player_median_minutes.fillna(0)
            anomaly_mask = (
                (mins < anomaly_threshold)
                & (player_median_minutes.fillna(0) >= _DNP_ANOMALY_MEDIAN_FLOOR)
                & ~dnp_mask  # don't double-count absolute DNPs
            )
            anomaly_count = int(anomaly_mask.sum())
            if anomaly_count > 0:
                logger.info(
                    "DNP guard: flagged %d anomalous low-minute games "
                    "(minutes < %.0f%% of player median)",
                    anomaly_count, _DNP_ANOMALY_RATIO * 100,
                )
            dnp_mask = dnp_mask | anomaly_mask

            flagged = int(dnp_mask.sum())
            if flagged > 0:
                logger.debug(
                    "DNP guard: masking %d/%d rows with minutes < %.0f or anomalous from rolling windows",
                    flagged, len(frame), _DNP_MIN_MINUTES,
                )

        # Build a DNP-cleaned copy of the frame for rolling source data.
        # DNP rows get NaN'd so rolling/ewm/expanding naturally skip them.
        has_dnp = dnp_mask.any()
        clean_frame = frame.copy() if has_dnp else frame
        if has_dnp:
            for col in metrics:
                if col in clean_frame.columns:
                    clean_frame.loc[dnp_mask, col] = np.nan

        for metric in metrics:
            grouped = frame.groupby("player_id")[metric]
            # Use cleaned values for rolling calculations
            clean_grouped = clean_frame.groupby("player_id")[metric] if has_dnp else grouped
            features[f"{metric}_prev"] = grouped.shift(1).fillna(0.0)
            for window in ROLLING_WINDOWS:
                effective_window = self._effective_window(window)
                # P0 CHANGE 1A: Raise min_periods to avoid phantom zeros from fillna(0.0)
                # Window sizes need at least 3 real data points, not just 1
                avg_min_periods = min(max(3, effective_window // 2), effective_window)
                features[f"{metric}_avg_{window}"] = (
                    clean_grouped.transform(
                        lambda series, w=effective_window, mp=avg_min_periods: series.shift(1).rolling(w, min_periods=mp).mean()
                    )
                )
                # Std stays available after three prior games so volatility
                # features do not flatline to zero for otherwise healthy rows.
                std_min_periods = min(3, effective_window)
                features[f"{metric}_std_{window}"] = (
                    clean_grouped.transform(
                        lambda series, w=effective_window, mp=std_min_periods: series.shift(1).rolling(w, min_periods=mp).std()
                    )
                )
            # EWM and expanding also use cleaned data
            features[f"{metric}_ewm_10"] = (
                clean_grouped.transform(lambda series: series.shift(1).ewm(span=10, adjust=False).mean())
            )
            features[f"{metric}_season_avg"] = (
                clean_grouped.transform(lambda series: series.shift(1).expanding(min_periods=1).mean())
            )

        # P0 CHANGE 1B: Cascading NaN fill - cascade shorter windows from longer windows, never from zero.
        # Order: avg_3 → avg_5 → avg_10 → avg_20 → ewm_10 → season_avg → 0.0
        for metric in metrics:
            season_key = f"{metric}_season_avg"
            ewm_key = f"{metric}_ewm_10"
            for window in ROLLING_WINDOWS:
                avg_key = f"{metric}_avg_{window}"
                if avg_key not in features:
                    continue
                current = features[avg_key]
                # Fill from next-longer windows first (longer windows have more data, so they're more reliable)
                for longer_window in ROLLING_WINDOWS:
                    if longer_window > window:
                        longer_key = f"{metric}_avg_{longer_window}"
                        if longer_key in features:
                            current = current.fillna(features[longer_key])
                # Then from EWM, then season, then 0.0 (last resort)
                current = current.fillna(features.get(ewm_key, pd.Series(dtype=float)))
                current = current.fillna(features.get(season_key, pd.Series(dtype=float)))
                if fill_terminal_nan is not None:
                    current = current.fillna(fill_terminal_nan)
                features[avg_key] = current

            # Similarly cascade std features
            for window in ROLLING_WINDOWS:
                std_key = f"{metric}_std_{window}"
                if std_key not in features:
                    continue
                current = features[std_key]
                for longer_window in ROLLING_WINDOWS:
                    if longer_window > window:
                        longer_std_key = f"{metric}_std_{longer_window}"
                        if longer_std_key in features:
                            current = current.fillna(features[longer_std_key])
                if fill_terminal_nan is not None:
                    current = current.fillna(fill_terminal_nan)
                features[std_key] = current

            # EWM and season_avg: fill with each other, then 0.0
            if ewm_key in features and season_key in features:
                ewm = features[ewm_key].fillna(features[season_key])
                season = features[season_key].fillna(ewm)
                if fill_terminal_nan is not None:
                    ewm = ewm.fillna(fill_terminal_nan)
                    season = season.fillna(fill_terminal_nan)
                features[ewm_key] = ewm
                features[season_key] = season
            elif ewm_key in features:
                if fill_terminal_nan is not None:
                    features[ewm_key] = features[ewm_key].fillna(fill_terminal_nan)
            elif season_key in features:
                if fill_terminal_nan is not None:
                    features[season_key] = features[season_key].fillna(fill_terminal_nan)

        team_injuries = frame.groupby("player_id")["team_injuries"]
        # Keep _prev unchanged — fillna(0.0) is correct for single-game lag (first game should be 0)
        features["team_injuries_avg_10"] = (
            team_injuries.transform(lambda series: series.shift(1).rolling(10, min_periods=5).mean())
        )
        if "lineup_instability_score" in frame.columns:
            lineup_instability = frame.groupby("player_id")["lineup_instability_score"]
            features["lineup_instability_score_avg_10"] = (
                lineup_instability.transform(lambda series: series.shift(1).rolling(10, min_periods=5).mean())
            )
        # B1: home / away conditional 10-game rolling means (shifted; train/inference-safe).
        if "is_home" in frame.columns:
            pid = frame["player_id"]
            for stat in ("points", "rebounds", "assists", "minutes"):
                if stat not in frame.columns or f"{stat}_avg_10" not in features:
                    continue
                values = frame.groupby("player_id")[stat].transform(lambda series: series.shift(1))
                loc_home = frame.groupby("player_id")["is_home"].transform(lambda series: series.shift(1))
                overall = features[f"{stat}_avg_10"]
                for suffix, flag in (("home", 1), ("away", 0)):
                    mask = (loc_home == flag).astype(float)
                    num = (values * mask).groupby(pid).transform(lambda s: s.rolling(10, min_periods=3).sum())
                    den = mask.groupby(pid).transform(lambda s: s.rolling(10, min_periods=3).sum())
                    col = f"{stat}_avg_10_{suffix}"
                    features[col] = (num / den.replace(0, np.nan)).fillna(overall)
            if (
                "points_avg_10_home" in features
                and "points_avg_10_away" in features
            ):
                features["home_away_points_delta"] = features["points_avg_10_home"] - features["points_avg_10_away"]
        return RollingFeatureResult(features, dnp_mask)

    def _effective_window(self, base_window: int) -> int:
        # Baseline (k=4) preserves current behavior.
        scaled = int(round((base_window * self._k_seasons) / 4.0))
        return max(2, scaled)
