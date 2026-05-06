from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd
from sqlalchemy.orm import Session

from app.evaluation.backtest import RollingOriginBacktester
from app.training.data import DatasetLoader

DistFamily = Literal["legacy", "count_aware", "decomposed"]


@dataclass(frozen=True)
class AblationConfig:
    k_seasons: int
    l1_alpha: float
    dist_family: DistFamily
    feature_flags: frozenset[str] = field(default_factory=frozenset)

    @property
    def config_id(self) -> str:
        flags = ",".join(sorted(self.feature_flags)) if self.feature_flags else "none"
        return f"k={self.k_seasons}|l1={self.l1_alpha:g}|dist={self.dist_family}|flags={flags}"


def run_ablation(
    configs: list[AblationConfig],
    holdout_seasons: list[str],
    session: Session,
    *,
    historical: pd.DataFrame | None = None,
    backtester_cls: type[RollingOriginBacktester] = RollingOriginBacktester,
    train_days: int = 120,
    validation_days: int = 14,
    step_days: int = 14,
) -> pd.DataFrame:
    if not configs:
        return pd.DataFrame()
    if historical is None:
        historical = DatasetLoader(session).load_historical_player_games()
    holdout = _filter_holdout_seasons(historical, holdout_seasons)
    if holdout.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for config in configs:
        backtester = backtester_cls(
            session,
            k_seasons=int(config.k_seasons),
            l1_alpha=float(config.l1_alpha),
            dist_family=config.dist_family,
        )
        result = backtester.run(
            train_days=train_days,
            validation_days=validation_days,
            step_days=step_days,
            historical=holdout,
        )
        summary = pd.DataFrame(result.get("summary", []))
        if summary.empty:
            continue
        market_rows = (
            summary[summary["segment"] == "market"].copy()
            if "segment" in summary.columns
            else summary.copy()
        )
        for entry in market_rows.to_dict("records"):
            rows.append(
                {
                    "config_id": config.config_id,
                    "k_seasons": config.k_seasons,
                    "l1_alpha": config.l1_alpha,
                    "dist_family": config.dist_family,
                    "feature_flags": ",".join(sorted(config.feature_flags)),
                    "market_key": str(entry.get("market_key", "unknown")),
                    "log_loss": float(entry.get("log_loss", float("nan"))),
                    "brier_score": float(entry.get("brier_score", float("nan"))),
                    "expected_calibration_error": float(entry.get("expected_calibration_error", float("nan"))),
                    "mae": float(entry.get("mae", float("nan"))),
                    "rmse": float(entry.get("rmse", float("nan"))),
                }
            )
    return pd.DataFrame(rows)


def _filter_holdout_seasons(frame: pd.DataFrame, seasons: list[str]) -> pd.DataFrame:
    if frame.empty or "game_date" not in frame.columns:
        return pd.DataFrame()
    years = {int(season) for season in seasons if str(season).strip()}
    if not years:
        return frame.copy()
    game_dates = pd.to_datetime(frame["game_date"], errors="coerce")
    return frame[game_dates.dt.year.isin(years)].reset_index(drop=True)
