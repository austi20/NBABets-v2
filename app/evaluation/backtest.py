from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.evaluation.metrics import compute_betting_metrics, compute_probability_metrics, compute_regression_metrics
from app.models.all import BacktestResult, ModelRun
from app.training.baseline import RecencyBaseline
from app.training.calibration import ProbabilityCalibrator
from app.training.constants import MARKET_TARGETS
from app.training.data import DEFAULT_PREGAME_BUFFER_MINUTES, DatasetLoader
from app.training.distributions import summarize_line_probability
from app.training.features import FeatureEngineer
from app.training.locked_defaults import (
    DEFAULT_DIST_FAMILY,
    DEFAULT_K_SEASONS,
    DEFAULT_L1_ALPHA,
    MARKET_DIST_FAMILY_DEFAULTS,
    DistFamily,
)
from app.training.models import MinutesModel, StatModelSuite
from app.training.pipeline import _with_output_columns

MIN_TRAIN_QUOTES_PER_MARKET = 50
MIN_VALIDATION_QUOTES_PER_MARKET = 20


@dataclass(frozen=True)
class BacktestArtifacts:
    summary_csv: Path
    detail_csv: Path
    fold_csv: Path
    markdown_report: Path


class RollingOriginBacktester:
    def __init__(
        self,
        session: Session,
        *,
        k_seasons: int | None = None,
        l1_alpha: float | None = None,
        dist_family: str | None = None,
    ) -> None:
        self._session = session
        self._settings = get_settings()
        self._loader = DatasetLoader(session)
        self._k_seasons = max(1, int(DEFAULT_K_SEASONS if k_seasons is None else k_seasons))
        self._l1_alpha = max(0.0, float(DEFAULT_L1_ALPHA if l1_alpha is None else l1_alpha))
        self._dist_family_override: DistFamily | None = cast(DistFamily | None, dist_family)
        self._features = FeatureEngineer(k_seasons=self._k_seasons)

    def run(
        self,
        train_days: int = 120,
        validation_days: int = 14,
        step_days: int = 14,
        prediction_buffer_minutes: int = DEFAULT_PREGAME_BUFFER_MINUTES,
        progress_callback: Callable[[int | None, int | None, str], None] | None = None,
        historical: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        if historical is None:
            historical = self._loader.load_historical_player_games(prediction_buffer_minutes=prediction_buffer_minutes)
        quote_inventory = self._loader.load_historical_bet_quotes(prediction_buffer_minutes=prediction_buffer_minutes)
        feature_set = self._features.build_training_frame(historical)
        frame = feature_set.frame.copy()
        if frame.empty:
            raise ValueError("No historical data available for backtesting")
        frame = frame.sort_values(["game_date", "start_time", "game_id", "player_id"]).reset_index(drop=True)
        if quote_inventory.empty:
            raise ValueError("No historical quotes available for backtesting")

        requested_train_days = int(train_days)
        history_start = pd.Timestamp(frame["game_date"].min()).date()
        end_date = pd.Timestamp(frame["game_date"].max()).date()
        history_span_days = max((end_date - history_start).days + 1, 1)
        if history_span_days <= train_days + max(validation_days, step_days):
            train_days = max(30, min(train_days, int(history_span_days * 0.70)))
            if train_days >= history_span_days:
                train_days = max(1, history_span_days - 1)
        start_date = history_start + timedelta(days=train_days)
        cursor = start_date
        fold_rows: list[dict[str, object]] = []
        detailed_bets: list[pd.DataFrame] = []
        total_windows = max(((end_date - start_date).days // max(step_days, 1)) + 1, 1)
        current_window = 0

        while cursor <= end_date:
            current_window += 1
            train_start = cursor - timedelta(days=train_days)
            train_end = cursor - timedelta(days=1)
            validation_end = min(cursor + timedelta(days=validation_days - 1), end_date)
            train_frame = frame[(frame["game_date"].dt.date >= train_start) & (frame["game_date"].dt.date <= train_end)].copy()
            validation_frame = frame[
                (frame["game_date"].dt.date >= cursor) & (frame["game_date"].dt.date <= validation_end)
            ].copy()
            train_quotes = quote_inventory[
                (quote_inventory["game_date"].dt.date >= train_start) & (quote_inventory["game_date"].dt.date <= train_end)
            ].copy()
            validation_quotes = quote_inventory[
                (quote_inventory["game_date"].dt.date >= cursor) & (quote_inventory["game_date"].dt.date <= validation_end)
            ].copy()
            if len(train_frame) < 50 or validation_frame.empty:
                cursor += timedelta(days=step_days)
                continue

            train_frame, validation_frame, feature_columns = self._prepare_frames(train_frame, validation_frame, feature_set.feature_columns)
            stat_models = StatModelSuite(self._settings.training_seed, l1_alpha=self._l1_alpha)
            stat_models.fit(train_frame, feature_columns, l1_alpha=self._l1_alpha)

            for market_index, (market_key, target_column) in enumerate(MARKET_TARGETS.items(), start=1):
                if progress_callback is not None:
                    progress_callback(
                        min(((current_window - 1) * len(MARKET_TARGETS)) + market_index, total_windows * len(MARKET_TARGETS)),
                        total_windows * len(MARKET_TARGETS),
                        f"Backtesting fold {current_window}/{total_windows}: {market_key.upper()}",
                    )

                # Feature frame is wide (one row per player-game, not per market).
                # For regression metrics, use the full frame; line_value is set
                # to 0.0 as a placeholder since p10/p90 quantiles are independent
                # of the betting line.
                validation_market = validation_frame.copy()
                train_market = train_frame.copy()
                line_col = f"line_{market_key}"
                validation_market["line_value"] = (
                    pd.to_numeric(validation_market[line_col], errors="coerce").fillna(0.0)
                    if line_col in validation_market.columns else 0.0
                )
                train_market["line_value"] = (
                    pd.to_numeric(train_market[line_col], errors="coerce").fillna(0.0)
                    if line_col in train_market.columns else 0.0
                )
                if validation_market.empty or train_market.empty:
                    continue
                validation_quote_market = validation_quotes[validation_quotes["market_key"] == market_key].copy()
                train_quote_market = train_quotes[train_quotes["market_key"] == market_key].copy()
                train_quote_count_raw = int(len(train_quote_market))
                validation_quote_count_raw = int(len(validation_quote_market))
                train_row_count = max(int(len(train_market)), 1)
                validation_row_count = max(int(len(validation_market)), 1)
                quote_density_train = train_quote_count_raw / train_row_count
                quote_density_validation = validation_quote_count_raw / validation_row_count
                sample_sufficient = (
                    train_quote_count_raw >= MIN_TRAIN_QUOTES_PER_MARKET
                    and validation_quote_count_raw >= MIN_VALIDATION_QUOTES_PER_MARKET
                )
                sufficiency_flag = "sufficient"
                if not sample_sufficient:
                    sufficiency_flag = "insufficient_quote_density"

                # --- Regression metrics: lightweight path, no MC simulation ---
                validation_reg = self._predict_means_variances(
                    validation_market, stat_models, market_key, target_column
                )
                if validation_reg.empty:
                    continue

                regression = compute_regression_metrics(
                    validation_reg["actual_total"].to_numpy(dtype=float),
                    validation_reg["projected_mean"].to_numpy(dtype=float),
                )
                interval_coverage = float(
                    np.mean(
                        (
                            validation_reg["summary_p10"].to_numpy(dtype=float)
                            <= validation_reg["actual_total"].to_numpy(dtype=float)
                        )
                        & (
                            validation_reg["actual_total"].to_numpy(dtype=float)
                            <= validation_reg["summary_p90"].to_numpy(dtype=float)
                        )
                    )
                )

                # --- Calibration / betting metrics: require matched quote rows ---
                nan = float("nan")
                log_loss_val = nan
                brier_val = nan
                ece_val = nan
                quote_count = 0
                bet_count = 0
                realized_profit = nan
                realized_roi = nan
                avg_ev = nan
                total_ev = nan
                avg_edge_implied = nan
                avg_edge_no_vig = nan
                avg_clv_line = nan
                avg_clv_prob = nan
                win_rate = nan
                push_rate = nan

                if not validation_quote_market.empty:
                    # Build quote-aligned rows whenever possible so deep-eval slices
                    # stay populated, even when calibration samples are sparse.
                    val_with_lines = validation_market[validation_market["line_value"] > 0].copy()
                    train_with_lines = train_market[train_market["line_value"] > 0].copy()
                    train_predictions = (
                        self._build_prediction_frame(train_with_lines, stat_models, market_key, target_column)
                        if not train_with_lines.empty
                        else pd.DataFrame()
                    )
                    validation_predictions = (
                        self._build_prediction_frame(val_with_lines, stat_models, market_key, target_column)
                        if not val_with_lines.empty
                        else pd.DataFrame()
                    )
                    train_quote_rows = self._score_quote_rows(train_predictions, train_quote_market, market_key)
                    validation_quote_rows = self._score_quote_rows(validation_predictions, validation_quote_market, market_key)
                    if not validation_quote_rows.empty:
                        calibration_mode = "raw_probability_fallback"
                        calibrator = ProbabilityCalibrator()
                        if sample_sufficient and not train_quote_rows.empty:
                            calibration_mode = "isotonic_with_market_prior"
                            calibrator.fit(
                                train_quote_rows["raw_over_probability"].to_numpy(dtype=float),
                                train_quote_rows["label"].to_numpy(dtype=int),
                                market_priors=train_quote_rows["no_vig_over_probability"].to_numpy(dtype=float),
                            )
                            validation_quote_rows["calibrated_over_probability"] = calibrator.transform(
                                validation_quote_rows["raw_over_probability"].to_numpy(dtype=float),
                                market_priors=validation_quote_rows["no_vig_over_probability"].to_numpy(dtype=float),
                            )
                        else:
                            validation_quote_rows["calibrated_over_probability"] = validation_quote_rows[
                                "raw_over_probability"
                            ].to_numpy(dtype=float)
                            if sufficiency_flag == "sufficient":
                                sufficiency_flag = "insufficient_for_calibration_fallback_raw"
                        validation_quote_rows["calibration_mode"] = calibration_mode
                        validation_quote_rows = _finalize_bet_rows(validation_quote_rows)
                        detailed_bets.append(validation_quote_rows)
                        probability = compute_probability_metrics(
                            validation_quote_rows["label"].to_numpy(dtype=int),
                            validation_quote_rows["calibrated_over_probability"].to_numpy(dtype=float),
                        )
                        betting = compute_betting_metrics(validation_quote_rows)
                        log_loss_val = probability.log_loss
                        brier_val = probability.brier_score
                        ece_val = probability.expected_calibration_error
                        quote_count = betting.quote_count
                        bet_count = betting.bet_count
                        realized_profit = betting.realized_profit
                        realized_roi = betting.realized_roi
                        avg_ev = betting.average_expected_value
                        total_ev = betting.total_expected_value
                        avg_edge_implied = betting.average_edge_implied
                        avg_edge_no_vig = betting.average_edge_no_vig
                        avg_clv_line = betting.average_clv_line
                        avg_clv_prob = betting.average_clv_probability
                        win_rate = betting.win_rate
                        push_rate = betting.push_rate
                    else:
                        sufficiency_flag = "insufficient_matched_quote_rows"

                fold_rows.append(
                    {
                        "fold_start": cursor.isoformat(),
                        "fold_end": validation_end.isoformat(),
                        "market_key": market_key,
                        "quote_count": quote_count,
                        "bet_count": bet_count,
                        "mae": regression.mae,
                        "rmse": regression.rmse,
                        "log_loss": log_loss_val,
                        "brier_score": brier_val,
                        "expected_calibration_error": ece_val,
                        "interval_80_coverage": interval_coverage,
                        "realized_profit": realized_profit,
                        "realized_roi": realized_roi,
                        "average_expected_value": avg_ev,
                        "total_expected_value": total_ev,
                        "average_edge_implied": avg_edge_implied,
                        "average_edge_no_vig": avg_edge_no_vig,
                        "average_clv_line": avg_clv_line,
                        "average_clv_probability": avg_clv_prob,
                        "win_rate": win_rate,
                        "push_rate": push_rate,
                        "train_quote_count_raw": train_quote_count_raw,
                        "validation_quote_count_raw": validation_quote_count_raw,
                        "quote_density_train": quote_density_train,
                        "quote_density_validation": quote_density_validation,
                        "sample_sufficient": int(sample_sufficient),
                        "sufficiency_flag": sufficiency_flag,
                    }
                )
            cursor += timedelta(days=step_days)

        results = pd.DataFrame(fold_rows)
        bet_detail = pd.concat(detailed_bets, ignore_index=True) if detailed_bets else pd.DataFrame()
        summary_rows = _build_segmented_summary(results, bet_detail)
        artifacts = self._write_artifacts(summary_rows, bet_detail, results)
        market_summary = (
            summary_rows[summary_rows["segment"] == "market"]
            .drop(columns=["segment", "segment_value"])
            .rename(columns={"market_key": "market_key"})
            .to_dict("records")
            if not summary_rows.empty
            else []
        )

        model_run = ModelRun(
            model_version=f"{self._settings.model_version}_backtest",
            feature_version=self._settings.feature_version,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            notes=f"Rolling-origin backtest at T-{prediction_buffer_minutes} minutes",
            metrics={
                "summary_rows": market_summary,
                "backtest_window": {
                    "requested_train_days": requested_train_days,
                    "effective_train_days": int(train_days),
                    "history_span_days": int(history_span_days),
                    "validation_days": int(validation_days),
                    "step_days": int(step_days),
                    "fold_rows": int(len(fold_rows)),
                },
            },
        )
        self._session.add(model_run)
        self._session.flush()
        self._session.add(
            BacktestResult(
                model_run_id=model_run.model_run_id,
                computed_at=datetime.now(UTC),
                metrics={
                    "summary_rows": market_summary,
                    "backtest_window": {
                        "requested_train_days": requested_train_days,
                        "effective_train_days": int(train_days),
                        "history_span_days": int(history_span_days),
                        "validation_days": int(validation_days),
                        "step_days": int(step_days),
                        "fold_rows": int(len(fold_rows)),
                    },
                },
                artifact_path=str(artifacts.markdown_report),
            )
        )
        self._session.commit()
        if progress_callback is not None:
            progress_callback(total_windows * len(MARKET_TARGETS), total_windows * len(MARKET_TARGETS), "Backtest complete")
        return {
            "summary": market_summary,
            "backtest_window": {
                "requested_train_days": requested_train_days,
                "effective_train_days": int(train_days),
                "history_span_days": int(history_span_days),
                "validation_days": int(validation_days),
                "step_days": int(step_days),
                "fold_rows": int(len(fold_rows)),
            },
            "artifacts": {
                "csv": str(artifacts.summary_csv),
                "detail_csv": str(artifacts.detail_csv),
                "fold_csv": str(artifacts.fold_csv),
                "report": str(artifacts.markdown_report),
            },
        }

    def _prepare_frames(
        self,
        train_frame: pd.DataFrame,
        validation_frame: pd.DataFrame,
        base_feature_columns: list[str],
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
        baseline_outputs = RecencyBaseline().fit_predict(train_frame)
        train_frame = _with_output_columns(train_frame, baseline_outputs)
        validation_baseline = RecencyBaseline().fit_predict(validation_frame)
        validation_frame = _with_output_columns(validation_frame, validation_baseline)
        feature_columns = sorted(
            set(
                base_feature_columns
                + [
                    column
                    for column in train_frame.columns
                    if column.endswith("_baseline_mean") or column.endswith("_baseline_variance")
                ]
            )
        )
        train_frame = _apply_feature_defaults(train_frame, feature_columns)
        validation_frame = _apply_feature_defaults(validation_frame, feature_columns)

        minutes_model = MinutesModel(self._settings.training_seed)
        minutes_model.fit(train_frame, feature_columns)
        train_frame = _with_output_columns(
            train_frame,
            {
                "predicted_minutes": minutes_model.predict(train_frame),
                "predicted_minutes_std": minutes_model.predict_uncertainty(train_frame),
            },
        )
        validation_frame = _with_output_columns(
            validation_frame,
            {
                "predicted_minutes": minutes_model.predict(validation_frame),
                "predicted_minutes_std": minutes_model.predict_uncertainty(validation_frame),
            },
        )
        train_frame = self._features.apply_post_minutes_features(train_frame, minutes_column="predicted_minutes")
        validation_frame = self._features.apply_post_minutes_features(validation_frame, minutes_column="predicted_minutes")
        return train_frame, validation_frame, feature_columns

    def _predict_means_variances(
        self,
        market_frame: pd.DataFrame,
        stat_models: StatModelSuite,
        market_key: str,
        target_column: str,
    ) -> pd.DataFrame:
        """Lightweight regression-only prediction: no Monte Carlo simulation.

        Returns a frame with projected_mean, projected_variance, actual_total,
        and approximate p10/p90 via normal quantile (1.28σ).  Used for MAE,
        RMSE, and 80%-interval coverage without the cost of full distribution
        simulation.
        """
        means, variances = stat_models.models[market_key].predict(market_frame)
        stds = np.sqrt(np.maximum(variances, 0.0))
        return pd.DataFrame(
            {
                "game_id": market_frame["game_id"].to_numpy(dtype=int),
                "player_id": market_frame["player_id"].to_numpy(dtype=int),
                "game_date": market_frame["game_date"].to_numpy(),
                "projected_mean": means,
                "projected_variance": variances,
                "actual_total": market_frame[target_column].to_numpy(dtype=float),
                "summary_p10": np.maximum(means - 1.28 * stds, 0.0),
                "summary_p90": means + 1.28 * stds,
            }
        )

    def _build_prediction_frame(
        self,
        market_frame: pd.DataFrame,
        stat_models: StatModelSuite,
        market_key: str,
        target_column: str,
    ) -> pd.DataFrame:
        means, variances = stat_models.models[market_key].predict(market_frame)
        summary_rows = [
            summarize_line_probability(
                mean,
                variance,
                float(line),
                market_key=market_key,
                context=_simulation_context(row),
                dist_family=self._dist_family_for_market(market_key),
            )
            for row, mean, variance, line in zip(
                market_frame.to_dict("records"),
                means,
                variances,
                market_frame["line_value"].to_numpy(dtype=float),
                strict=False,
            )
        ]
        return pd.DataFrame(
            {
                "game_id": market_frame["game_id"].to_numpy(dtype=int),
                "player_id": market_frame["player_id"].to_numpy(dtype=int),
                "player_name": market_frame["player_name"].astype(str).to_numpy(),
                "game_date": market_frame["game_date"].to_numpy(),
                "projected_mean": means,
                "projected_variance": variances,
                "actual_total": market_frame[target_column].to_numpy(dtype=float),
                "summary_p10": [summary.p10 for summary in summary_rows],
                "summary_p90": [summary.p90 for summary in summary_rows],
            }
        )

    def _score_quote_rows(
        self,
        predictions: pd.DataFrame,
        quote_rows: pd.DataFrame,
        market_key: str,
    ) -> pd.DataFrame:
        required_merge_keys = {"game_id", "player_id", "game_date"}
        if quote_rows.empty or not required_merge_keys.issubset(quote_rows.columns):
            return pd.DataFrame()
        if predictions.empty or not required_merge_keys.issubset(predictions.columns):
            return quote_rows.iloc[0:0].copy()
        merged = quote_rows.merge(predictions, on=["game_id", "player_id", "game_date"], how="inner")
        if merged.empty:
            return merged
        summaries = [
            summarize_line_probability(
                float(row["projected_mean"]),
                float(row["projected_variance"]),
                float(row["line_value"]),
                market_key=market_key,
                context=_simulation_context(row),
                dist_family=self._dist_family_for_market(market_key),
            )
            for row in merged.to_dict("records")
        ]
        merged["raw_over_probability"] = [summary.over_probability for summary in summaries]
        merged["raw_under_probability"] = [summary.under_probability for summary in summaries]
        merged["push_probability"] = [max(0.0, 1.0 - summary.over_probability - summary.under_probability) for summary in summaries]
        merged["label"] = (merged["actual_total"].to_numpy(dtype=float) > merged["line_value"].to_numpy(dtype=float)).astype(int)
        return merged

    def _dist_family_for_market(self, market_key: str) -> DistFamily:
        if self._dist_family_override is not None:
            return self._dist_family_override
        return MARKET_DIST_FAMILY_DEFAULTS.get(market_key, DEFAULT_DIST_FAMILY)

    def _write_artifacts(
        self,
        summary_rows: pd.DataFrame,
        bet_detail: pd.DataFrame,
        fold_results: pd.DataFrame,
    ) -> BacktestArtifacts:
        report_root = get_settings().reports_dir
        report_root.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        summary_csv = report_root / f"backtest_summary_{timestamp}.csv"
        detail_csv = report_root / f"backtest_detail_{timestamp}.csv"
        fold_csv = report_root / f"backtest_folds_{timestamp}.csv"
        markdown_report = report_root / f"backtest_report_{timestamp}.md"
        summary_rows.to_csv(summary_csv, index=False)
        bet_detail.to_csv(detail_csv, index=False)
        fold_results.to_csv(fold_csv, index=False)

        lines = [
            "# Backtest Summary",
            "",
            "## Overview",
            "",
        ]
        overview = summary_rows[summary_rows["segment"] == "overall"]
        if not overview.empty:
            row = overview.iloc[0]
            lines.extend(
                [
                    f"- Quotes evaluated: {int(row['quote_count'])}",
                    f"- Bets placed: {int(row['bet_count'])}",
                    f"- Realized ROI: {row['realized_roi']:.3f}",
                    f"- Avg EV: {row['average_expected_value']:.3f}",
                    f"- Avg edge vs implied: {row['average_edge_implied']:.3f}",
                    f"- Avg edge vs no-vig: {row['average_edge_no_vig']:.3f}",
                    f"- Avg CLV line delta: {row['average_clv_line']:.3f}",
                    f"- Avg CLV probability delta: {row['average_clv_probability']:.3f}",
                    "",
                ]
            )

        lines.extend(
            [
                "## Data Sufficiency",
                "",
            ]
        )
        if fold_results.empty:
            lines.extend(
                [
                    "- No fold results available.",
                    "",
                ]
            )
        else:
            sufficient_rate = float(fold_results["sample_sufficient"].mean()) if "sample_sufficient" in fold_results.columns else 0.0
            lines.extend(
                [
                    f"- Sufficient fold ratio: {sufficient_rate:.1%}",
                    f"- Minimum train quotes per market: {MIN_TRAIN_QUOTES_PER_MARKET}",
                    f"- Minimum validation quotes per market: {MIN_VALIDATION_QUOTES_PER_MARKET}",
                    "",
                    "| Market | Avg train quotes | Avg validation quotes | Avg train density | Avg validation density | Sufficient ratio |",
                    "|---|---:|---:|---:|---:|---:|",
                ]
            )
            grouped = (
                fold_results.groupby("market_key", dropna=False)
                .agg(
                    train_quote_count_raw=("train_quote_count_raw", "mean"),
                    validation_quote_count_raw=("validation_quote_count_raw", "mean"),
                    quote_density_train=("quote_density_train", "mean"),
                    quote_density_validation=("quote_density_validation", "mean"),
                    sample_sufficient=("sample_sufficient", "mean"),
                )
                .reset_index()
            )
            for row in grouped.to_dict("records"):
                lines.append(
                    f"| {row['market_key']} | {row['train_quote_count_raw']:.1f} | "
                    f"{row['validation_quote_count_raw']:.1f} | {row['quote_density_train']:.3f} | "
                    f"{row['quote_density_validation']:.3f} | {row['sample_sufficient']:.1%} |"
                )
            lines.append("")
            if "sufficiency_flag" in fold_results.columns:
                flag_rows = (
                    fold_results.groupby(["market_key", "sufficiency_flag"], dropna=False)
                    .size()
                    .reset_index(name="count")
                    .sort_values(["market_key", "count"], ascending=[True, False])
                )
                lines.extend(
                    [
                        "| Market | Sufficiency flag | Folds |",
                        "|---|---|---:|",
                    ]
                )
                for row in flag_rows.to_dict("records"):
                    lines.append(f"| {row['market_key']} | {row['sufficiency_flag']} | {int(row['count'])} |")
                lines.append("")

        for segment, title in (
            ("market", "By Market"),
            ("odds_provider", "By Odds Provider"),
            ("sportsbook", "By Sportsbook"),
            ("confidence_tier", "By Confidence Tier"),
            ("date_range", "By Date Range"),
        ):
            section = summary_rows[summary_rows["segment"] == segment].copy()
            if section.empty:
                continue
            lines.extend(
                [
                    f"## {title}",
                    "",
                    "| Segment | Quotes | Bets | ROI | Avg EV | Edge vs Implied | Edge vs No-Vig | Avg CLV Line | Avg CLV Prob |",
                    "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
                ]
            )
            for row in section.to_dict("records"):
                lines.append(
                    f"| {row['segment_value']} | {int(row['quote_count'])} | {int(row['bet_count'])} | "
                    f"{row['realized_roi']:.3f} | {row['average_expected_value']:.3f} | "
                    f"{row['average_edge_implied']:.3f} | {row['average_edge_no_vig']:.3f} | "
                    f"{row['average_clv_line']:.3f} | {row['average_clv_probability']:.3f} |"
                )
            lines.append("")

        markdown_report.write_text("\n".join(lines), encoding="utf-8")
        return BacktestArtifacts(
            summary_csv=summary_csv,
            detail_csv=detail_csv,
            fold_csv=fold_csv,
            markdown_report=markdown_report,
        )


def _build_segmented_summary(results: pd.DataFrame, bet_detail: pd.DataFrame) -> pd.DataFrame:
    if results.empty and bet_detail.empty:
        return pd.DataFrame(
            [
                {
                    "segment": "overall",
                    "segment_value": "all",
                    "market_key": "all",
                    "quote_count": 0,
                    "bet_count": 0,
                    "realized_roi": 0.0,
                    "average_expected_value": 0.0,
                    "average_edge_implied": 0.0,
                    "average_edge_no_vig": 0.0,
                    "average_clv_line": 0.0,
                    "average_clv_probability": 0.0,
                }
            ]
        )

    summary_rows: list[dict[str, object]] = []
    if not bet_detail.empty:
        overall = compute_betting_metrics(bet_detail)
        summary_rows.append(
            {
                "segment": "overall",
                "segment_value": "all",
                "market_key": "all",
                **asdict(overall),
            }
        )
        group_specs = [
            ("market", ["market_key"]),
            ("odds_provider", ["odds_source_provider"]),
            ("sportsbook", ["sportsbook_key"]),
            ("confidence_tier", ["confidence_tier"]),
            ("date_range", ["evaluation_date"]),
        ]
        for segment_name, columns in group_specs:
            grouped = bet_detail.groupby(columns, dropna=False)
            for group_values, group in grouped:
                if not isinstance(group_values, tuple):
                    group_values = (group_values,)
                metrics = compute_betting_metrics(group)
                summary_rows.append(
                    {
                        "segment": segment_name,
                        "segment_value": " | ".join(str(value) for value in group_values),
                        "market_key": group["market_key"].iloc[0] if "market_key" in group.columns else "all",
                        **asdict(metrics),
                    }
                )
    if not results.empty:
        market_rows = results.groupby("market_key").mean(numeric_only=True).reset_index()
        for row in market_rows.to_dict("records"):
            row.setdefault("segment", "market")
            row.setdefault("segment_value", row["market_key"])
            summary_rows.append(row)
    summary = pd.DataFrame(summary_rows).drop_duplicates(subset=["segment", "segment_value", "market_key"], keep="first")
    numeric_columns = summary.select_dtypes(include=["number", "bool"]).columns
    summary[numeric_columns] = summary[numeric_columns].fillna(0.0)
    return summary.sort_values(["segment", "segment_value", "market_key"]).reset_index(drop=True)


def _finalize_bet_rows(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["calibrated_under_probability"] = np.maximum(
        0.0,
        1.0 - result["calibrated_over_probability"].to_numpy(dtype=float) - result["push_probability"].to_numpy(dtype=float),
    )
    result["recommended_side"] = np.where(
        result["calibrated_over_probability"].to_numpy(dtype=float) >= result["calibrated_under_probability"].to_numpy(dtype=float),
        "OVER",
        "UNDER",
    )
    result["hit_probability"] = np.where(
        result["recommended_side"] == "OVER",
        result["calibrated_over_probability"],
        result["calibrated_under_probability"],
    )
    result["implied_side_probability"] = np.where(
        result["recommended_side"] == "OVER",
        result["raw_implied_over_probability"],
        result["raw_implied_under_probability"],
    )
    result["no_vig_side_probability"] = np.where(
        result["recommended_side"] == "OVER",
        result["no_vig_over_probability"],
        result["no_vig_under_probability"],
    )
    result["edge_vs_implied"] = result["hit_probability"] - result["implied_side_probability"].fillna(0.0)
    result["edge_vs_no_vig"] = result["hit_probability"] - result["no_vig_side_probability"].fillna(0.0)
    result["decimal_odds"] = result["recommended_side"].where(result["recommended_side"] == "OVER", "UNDER")
    result["decimal_odds"] = np.where(
        result["recommended_side"] == "OVER",
        result["over_odds"].apply(_american_to_decimal),
        result["under_odds"].apply(_american_to_decimal),
    )
    result["win_profit"] = result["decimal_odds"] - 1.0
    lose_probability = np.maximum(0.0, 1.0 - result["hit_probability"].to_numpy(dtype=float) - result["push_probability"].to_numpy(dtype=float))
    result["expected_value"] = result["hit_probability"] * result["win_profit"] - lose_probability
    result["bet_placed"] = (result["expected_value"] > 0.0) & (result["edge_vs_implied"] > 0.0)
    over_push = np.isclose(result["actual_total"].to_numpy(dtype=float), result["line_value"].to_numpy(dtype=float))
    over_hit = result["actual_total"].to_numpy(dtype=float) > result["line_value"].to_numpy(dtype=float)
    under_hit = result["actual_total"].to_numpy(dtype=float) < result["line_value"].to_numpy(dtype=float)
    result["bet_result"] = np.where(
        ~result["bet_placed"],
        "skip",
        np.where(
            over_push,
            "push",
            np.where(
                ((result["recommended_side"] == "OVER") & over_hit) | ((result["recommended_side"] == "UNDER") & under_hit),
                "win",
                "loss",
            ),
        ),
    )
    result["realized_profit"] = np.where(
        result["bet_result"] == "win",
        result["win_profit"],
        np.where(result["bet_result"] == "push", 0.0, np.where(result["bet_placed"], -1.0, 0.0)),
    )
    result["clv_line_delta"] = np.where(
        result["recommended_side"] == "OVER",
        result["closing_line_value"].fillna(result["line_value"]) - result["line_value"],
        result["line_value"] - result["closing_line_value"].fillna(result["line_value"]),
    )
    result["clv_probability_delta"] = np.where(
        result["recommended_side"] == "OVER",
        result["closing_no_vig_over_probability"].fillna(result["no_vig_over_probability"]) - result["no_vig_over_probability"].fillna(0.0),
        result["closing_no_vig_under_probability"].fillna(result["no_vig_under_probability"]) - result["no_vig_under_probability"].fillna(0.0),
    )
    result["confidence_tier"] = result["hit_probability"].apply(_confidence_tier)
    result["evaluation_date"] = pd.to_datetime(result["game_date"]).dt.date.astype(str)
    return result


def _apply_feature_defaults(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in feature_columns:
        if column not in result.columns:
            result[column] = 0.0
    result[feature_columns] = result[feature_columns].fillna(0.0)
    return result


def _market_prior(frame: pd.DataFrame, market_key: str) -> np.ndarray | None:
    for column in (f"{market_key}_consensus_prob_mean", "consensus_prob_mean", "no_vig_over_probability"):
        if column in frame.columns:
            return pd.to_numeric(frame[column], errors="coerce").fillna(0.5).to_numpy(dtype=float)
    return None


def _simulation_context(row: dict[str, object] | pd.Series) -> dict[str, float]:
    getter = row.get if isinstance(row, dict) else row.get
    context = {
        "usage_rate": float(getter("usage_rate_blended", getter("usage_rate_avg_10", getter("usage_rate", 0.20)))),
        "touches_per_minute": float(getter("touches_per_minute_blended", getter("touches_per_minute_avg_10", getter("touches_per_minute", 0.0)))),
        "passes_per_minute": float(getter("passes_per_minute_blended", getter("passes_per_minute_avg_10", getter("passes_per_minute", 0.0)))),
        "assist_creation_proxy_per_minute": float(
            getter(
                "assist_creation_proxy_per_minute_blended",
                getter("assist_creation_proxy_per_minute_avg_10", getter("assist_creation_proxy_per_minute", 0.0)),
            )
        ),
        "rebound_chances_total_per_minute": float(
            getter(
                "rebound_chances_total_per_minute_blended",
                getter("rebound_chances_total_per_minute_avg_10", getter("rebound_chances_total_per_minute", 0.0)),
            )
        ),
        "rebound_conversion_rate": float(
            getter("rebound_conversion_rate_blended", getter("rebound_conversion_rate_avg_10", getter("rebound_conversion_rate", 0.0)))
        ),
        "field_goal_attempts_per_minute": float(
            getter(
                "field_goal_attempts_per_minute_blended",
                getter("field_goal_attempts_per_minute_avg_10", getter("field_goal_attempts_per_minute", 0.0)),
            )
        ),
        "free_throw_attempts_per_minute": float(
            getter(
                "free_throw_attempts_per_minute_blended",
                getter("free_throw_attempts_per_minute_avg_10", getter("free_throw_attempts_per_minute", 0.0)),
            )
        ),
        "estimated_three_point_attempts_per_minute": float(
            getter(
                "estimated_three_point_attempts_per_minute_blended",
                getter("estimated_three_point_attempts_per_minute_avg_10", getter("estimated_three_point_attempts_per_minute", 0.0)),
            )
        ),
        "percentage_field_goals_attempted_3pt": float(
            getter(
                "percentage_field_goals_attempted_3pt_blended",
                getter("percentage_field_goals_attempted_3pt_avg_10", getter("percentage_field_goals_attempted_3pt", 0.35)),
            )
        ),
        "true_shooting_percentage": float(
            getter("true_shooting_percentage_blended", getter("true_shooting_percentage_avg_10", getter("true_shooting_percentage", 0.58)))
        ),
        "turnover_ratio": float(getter("turnover_ratio_blended", getter("turnover_ratio_avg_10", getter("turnover_ratio", 0.12)))),
    }
    context["three_point_make_rate"] = float(getter("threes_per_minute_blended", getter("threes_avg_10", 0.0))) / max(
        float(
            getter(
                "estimated_three_point_attempts_per_minute_blended",
                getter("estimated_three_point_attempts_per_minute_avg_10", 0.0),
            )
        ),
        1e-6,
    )
    assists_rate = float(getter("assists_per_minute_blended", getter("assists_per_minute_avg_10", 0.0)))
    context["assist_conversion_rate"] = assists_rate / max(context["assist_creation_proxy_per_minute"], 1e-6)
    return context


def _confidence_tier(probability: float) -> str:
    if probability >= 0.67:
        return "very_high"
    if probability >= 0.60:
        return "high"
    if probability >= 0.55:
        return "medium"
    return "low"


def _american_to_decimal(value: object) -> float:
    if value in (None, "", 0):
        return 1.0
    odds = float(value)
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)
