from __future__ import annotations

import pandas as pd

from app.training.constants import MARKET_TARGETS


class RecencyBaseline:
    def predict(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        return self._compute(frame)

    def fit_predict(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        return self._compute(frame)

    def _compute(self, frame: pd.DataFrame) -> dict[str, pd.Series]:
        predictions: dict[str, pd.Series] = {}
        for market_key, target in MARKET_TARGETS.items():
            season = frame[f"{target}_season_avg"].fillna(0.0)
            recent_20 = frame[f"{target}_avg_20"].fillna(season)
            recent_10 = frame[f"{target}_avg_10"].fillna(recent_20)
            recent_5 = frame[f"{target}_avg_5"].fillna(recent_10)
            opponent = frame[f"opponent_allowed_{market_key}_10"].fillna(season)
            mean = 0.35 * recent_5 + 0.30 * recent_10 + 0.20 * recent_20 + 0.10 * season + 0.05 * opponent
            variance = frame[f"{target}_std_10"].fillna(1.0).pow(2).clip(lower=1.0)
            predictions[f"{market_key}_baseline_mean"] = mean
            predictions[f"{market_key}_baseline_variance"] = variance
        return predictions
