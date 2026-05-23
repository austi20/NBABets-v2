from __future__ import annotations

import pandas as pd

from app.training.constants import MARKET_TARGETS, ROLLING_WINDOW_RECENCY_WEIGHTS


class RecencyBaseline:
    def predict(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        return self._compute(frame)

    def fit_predict(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        return self._compute(frame)

    def _compute(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        predictions: dict[str, pd.Series] = {}
        for market_key, target in MARKET_TARGETS.items():
            season = frame[f"{target}_season_avg"].fillna(0.0)
            recency_weighted = self._recency_weighted(frame, target, season)
            opponent = frame[f"opponent_allowed_{market_key}_10"].fillna(season)
            mean = 0.80 * recency_weighted + 0.10 * season + 0.10 * opponent
            variance_source = frame.get(f"{target}_std_recency_weighted", frame[f"{target}_std_10"])
            variance = variance_source.fillna(frame[f"{target}_std_10"]).fillna(1.0).pow(2).clip(lower=1.0)
            predictions[f"{market_key}_baseline_mean"] = mean
            predictions[f"{market_key}_baseline_variance"] = variance
        return predictions

    def _recency_weighted(self, frame: pd.DataFrame, target: str, season: pd.Series) -> pd.Series:
        column = f"{target}_recency_weighted"
        if column in frame.columns:
            return frame[column].fillna(season)

        weighted = pd.Series(0.0, index=frame.index, dtype=float)
        for window, weight in ROLLING_WINDOW_RECENCY_WEIGHTS.items():
            fallback = frame.get(f"{target}_avg_20", season) if window != 20 else season
            values = frame.get(f"{target}_avg_{window}", fallback)
            weighted = weighted + values.fillna(season) * weight
        return weighted.fillna(season)
