from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.core.resources import get_runtime_budget
from app.models.all import ModelRun, Prediction, PropMarket, RawPayload
from app.schemas.domain import PropPrediction
from app.training.artifacts import (
    artifact_exists,
    artifact_paths,
    dump_artifact,
    load_artifact,
    resolve_artifact_namespace,
)
from app.training.baseline import RecencyBaseline
from app.training.calibration import ProbabilityCalibrator
from app.training.constants import MARKET_TARGETS
from app.training.data import DatasetLoader
from app.training.data_sufficiency import annotate_tiers
from app.training.distributions import (
    simulate_joint_combo_probability,
    simulate_line_probability,
    summarize_line_probability,
)
from app.training.features import FeatureEngineer
from app.training.locked_defaults import (
    DEFAULT_DIST_FAMILY,
    DEFAULT_K_SEASONS,
    DEFAULT_L1_ALPHA,
    MARKET_DIST_FAMILY_DEFAULTS,
    DistFamily,
)
from app.training.models import MinutesModel, StatModelSuite

_log = __import__("logging").getLogger(__name__)

ProgressCallback = Callable[[int | None, int | None, str], None]


@dataclass
class TrainedBundle:
    minutes_model: MinutesModel
    stat_models: StatModelSuite
    calibrators: dict[str, ProbabilityCalibrator]
    metadata: dict[str, Any]


_TIER_BASE_CONFIDENCE = {"A": 0.90, "B": 0.70, "C": 0.45, "D": 0.25}


def _compute_data_confidence(
    tier: str,
    variance: float,
    mean: float,
    historical_games: int,
    days_since_last_game: float | None = None,
    injury_return: bool = False,
    team_changed: bool = False,
) -> float:
    """Compute a 0-1 data confidence score reflecting data quality."""
    base = _TIER_BASE_CONFIDENCE.get(tier, 0.25)
    uncertainty_penalty = min(variance / (mean + 1.0), 0.30)
    history_bonus = min(historical_games / 30.0, 0.10)
    recency_bonus = 0.05 if days_since_last_game is not None and days_since_last_game <= 3 else 0.0
    injury_penalty = 0.15 if injury_return else 0.0
    trade_penalty = 0.10 if team_changed else 0.0
    score = base - uncertainty_penalty + history_bonus + recency_bonus - injury_penalty - trade_penalty
    return float(np.clip(score, 0.05, 0.95))


class TrainingPipeline:
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
        self._artifact_namespace = resolve_artifact_namespace(
            getattr(getattr(session.bind, "url", None), "render_as_string", lambda **_: self._settings.database_url)(
                hide_password=False
            )
            if getattr(session.bind, "url", None) is not None
            else self._settings.database_url,
            self._settings.app_env,
        )

    def train(
        self,
        progress_callback: ProgressCallback | None = None,
        historical: pd.DataFrame | None = None,
        skip_calibration: bool = False,
    ) -> dict[str, Any]:
        total_steps = 4 + (2 * len(MARKET_TARGETS)) + 1
        current_step = 0

        def emit(message: str) -> None:
            if progress_callback is not None:
                progress_callback(current_step, total_steps, message)

        if historical is None:
            emit("Loading historical training data")
            historical = self._loader.load_historical_player_games()
        current_step += 1
        emit("Building leakage-safe training features")
        feature_set = self._features.build_training_frame(historical)
        frame = feature_set.frame.copy()
        if frame.empty:
            raise ValueError("No historical data available for training")

        # Drop anomalous rows (load management, injury exits, blowouts) from training
        # targets. These games have valid rolling features for subsequent games but
        # their own labels (low stats due to minimal play) would confuse the model.
        if "_is_anomalous" in frame.columns:
            n_anomalous = int(frame["_is_anomalous"].sum())
            if n_anomalous:
                _log.info("Dropping %d anomalous training rows (load management/injury/blowout)", n_anomalous)
            frame = frame[~frame["_is_anomalous"].astype(bool)].reset_index(drop=True)
            frame = frame.drop(columns=["_is_anomalous"])

        baseline_outputs = RecencyBaseline().fit_predict(frame)
        frame = _with_output_columns(frame, baseline_outputs)

        feature_columns = sorted(
            set(
                feature_set.feature_columns
                + [
                    column
                    for column in frame.columns
                    if column.endswith("_baseline_mean") or column.endswith("_baseline_variance")
                ]
            )
        )
        frame = _apply_feature_defaults(frame, feature_columns)
        population_priors = self._features.build_population_priors(frame, feature_columns)
        training_data_quality = _training_data_quality_checks(frame, feature_columns)
        current_step += 1
        emit("Fitting minutes model")
        minutes_model = MinutesModel(self._settings.training_seed)
        minutes_model.fit(frame, feature_columns)
        predicted_minutes = minutes_model.predict(frame)
        predicted_minutes_std = minutes_model.predict_uncertainty(frame)
        frame = _with_minutes_predictions(frame, predicted_minutes, predicted_minutes_std)
        frame = self._features.apply_post_minutes_features(frame, minutes_column="predicted_minutes")

        current_step += 1
        emit("Fitting market models")
        stat_models = StatModelSuite(self._settings.training_seed, l1_alpha=self._l1_alpha)
        training_metrics = stat_models.fit(frame, feature_columns, l1_alpha=self._l1_alpha)
        if skip_calibration:
            if progress_callback is not None:
                progress_callback(current_step + 1, total_steps, "Smoke mode: fitting direct calibrators")
            calibrators = {
                market_key: self._fit_direct_calibrator(
                    frame,
                    market_key,
                    target_column,
                    stat_models,
                    progress_callback=progress_callback,
                    progress_state=(current_step + 1, total_steps),
                )
                for market_key, target_column in MARKET_TARGETS.items()
            }
            calibration_diagnostics: dict[str, Any] = {"smoke_mode": True, "oof_skipped": True}
            current_step += len(MARKET_TARGETS)
            if progress_callback is not None:
                progress_callback(current_step, total_steps, "Direct calibration complete")
        else:
            if progress_callback is not None:
                progress_callback(current_step + 1, total_steps, "Starting calibration pass (OOF folds)")
            calibrators, calibration_diagnostics = self._fit_calibrators(
                frame,
                feature_columns,
                stat_models,
                progress_callback=progress_callback,
                progress_state=(current_step + 1, total_steps),
            )
            current_step += len(MARKET_TARGETS)
            if progress_callback is not None:
                progress_callback(current_step, total_steps, "Calibration pass complete")
        training_metrics["calibration_diagnostics"] = calibration_diagnostics
        training_metrics["provider_context"] = self._provider_context_counts()
        training_metrics["training_data_quality"] = training_data_quality

        model_run = ModelRun(
            model_version=self._settings.model_version,
            feature_version=self._settings.feature_version,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            training_window_start=pd.Timestamp(frame["game_date"].min()).to_pydatetime(),
            training_window_end=pd.Timestamp(frame["game_date"].max()).to_pydatetime(),
            notes=f"Tier 1 minutes+stat pipeline | feature_version={self._settings.feature_version} | calibration_purge_days={self._settings.calibration_purge_days} | simulation_min_samples={self._settings.simulation_min_samples}",
            metrics=training_metrics,
        )
        self._session.add(model_run)
        self._session.flush()

        paths = artifact_paths(self._settings.model_version, self._artifact_namespace)
        metadata = {
            "model_run_id": model_run.model_run_id,
            "trained_at": datetime.now(UTC).isoformat(),
            "latest_historical_game_date": pd.Timestamp(frame["game_date"].max()).date().isoformat(),
            "feature_version": self._settings.feature_version,
            "feature_columns": feature_columns,
            "artifact_namespace": self._artifact_namespace,
        }
        dump_artifact(paths.minutes_model, minutes_model)
        dump_artifact(paths.stat_models, stat_models)
        dump_artifact(paths.calibrators, calibrators)
        dump_artifact(paths.metadata, metadata)
        dump_artifact(paths.population_priors, population_priors)
        self._session.commit()
        if progress_callback is not None:
            progress_callback(total_steps, total_steps, "Training complete")
        return {"model_run_id": model_run.model_run_id, "metrics": training_metrics}

    def predict_upcoming(
        self,
        target_date: date | None = None,
        progress_callback: ProgressCallback | None = None,
        game_ids: set[int] | None = None,
        historical: pd.DataFrame | None = None,
    ) -> list[PropPrediction]:
        """Generate and persist predictions for upcoming games.

        When ``game_ids`` is provided only those games are (re-)predicted and
        their existing predictions for the current model run are replaced.
        When ``game_ids`` is None all upcoming games are run (standard startup
        path) and predictions for all returned games are replaced.
        """
        paths = artifact_paths(self._settings.model_version, self._artifact_namespace)
        minutes_model = load_artifact(paths.minutes_model)
        stat_models = load_artifact(paths.stat_models)
        calibrators = load_artifact(paths.calibrators)
        metadata = load_artifact(paths.metadata)
        population_priors = load_artifact(paths.population_priors) if artifact_exists(paths.population_priors) else None
        if not isinstance(metadata, dict):
            raise ValueError("Model metadata is invalid or corrupted")
        if "model_run_id" not in metadata:
            raise ValueError("Model metadata is missing model_run_id")

        effective_date = target_date or date.today()
        if progress_callback is not None:
            progress_callback(0, 1, "Loading historical context and current lines")
        if historical is None:
            historical = self._loader.load_historical_player_games()
        upcoming = self._loader.load_upcoming_player_lines(effective_date)
        # Scope to specific games when re-predicting after inactive list update
        if game_ids is not None and not upcoming.empty and "game_id" in upcoming.columns:
            upcoming = upcoming[upcoming["game_id"].isin(game_ids)].reset_index(drop=True)
        if not upcoming.empty and "player_on_inactive_list" in upcoming.columns:
            inactive_mask = pd.to_numeric(upcoming["player_on_inactive_list"], errors="coerce").fillna(0.0) > 0.5
            inactive_count = int(inactive_mask.sum())
            if inactive_count:
                _log.info("Skipping %d upcoming rows for players on the official inactive list", inactive_count)
                upcoming = upcoming.loc[~inactive_mask].reset_index(drop=True)
        upcoming = self._annotate_data_sufficiency(historical=historical, upcoming=upcoming)
        feature_set = self._features.build_inference_frame(
            historical,
            upcoming,
            population_priors=population_priors if isinstance(population_priors, dict) else None,
        )
        frame = feature_set.frame.copy()
        if frame.empty:
            return []
        baseline_outputs = RecencyBaseline().predict(frame)
        frame = _with_output_columns(frame, baseline_outputs)
        trained_feature_columns = list(metadata.get("feature_columns", []))
        for column in trained_feature_columns:
            if column not in frame.columns:
                frame[column] = 0.0
        frame = _apply_feature_defaults(frame, trained_feature_columns)

        predicted_minutes = minutes_model.predict(frame)
        predicted_minutes_std = minutes_model.predict_uncertainty(frame)
        frame = _with_minutes_predictions(frame, predicted_minutes, predicted_minutes_std)
        frame = self._features.apply_post_minutes_features(frame, minutes_column="predicted_minutes")
        predictions: list[PropPrediction] = []
        prediction_models: list[Prediction] = []
        model_run_id = int(metadata["model_run_id"])
        metadata_feature_version = str(metadata.get("feature_version") or self._settings.feature_version)
        market_rows = {market.key: market.market_id for market in self._session.query(PropMarket).all()}
        timestamp = datetime.now(UTC)
        total_prediction_rows = int(sum((frame["market_key"] == market_key).sum() for market_key in stat_models.models))
        processed_prediction_rows = 0

        for market_key, market_model in stat_models.models.items():
            subset = frame[frame["market_key"] == market_key].copy()
            if subset.empty:
                continue
            simulations = self._simulate_market_rows(
                market_key=market_key,
                subset=subset,
                stat_models=stat_models,
                progress_callback=progress_callback,
                progress_state=(processed_prediction_rows, total_prediction_rows),
            )
            raw_probs = np.array([simulation.summary.over_probability for simulation in simulations], dtype=float)
            calibrated = calibrators[market_key].transform(
                raw_probs,
                market_priors=self._market_prior(subset, market_key),
            )
            calibrated = self._stabilize_over_probabilities(
                subset=subset,
                market_key=market_key,
                raw_probabilities=raw_probs,
                calibrated_probabilities=np.asarray(calibrated, dtype=float),
            )

            # P1 CHANGE 4: Hard probability clamp - no NBA player prop should exceed 96% or fall below 4%.
            # Even the most lopsided props have 5-10% miss rates. Predictions beyond this range are always feature artifacts.
            PROBABILITY_FLOOR = 0.04
            PROBABILITY_CEILING = 0.96
            calibrated = np.clip(calibrated, PROBABILITY_FLOOR, PROBABILITY_CEILING)

            # Log divergence warnings for monitoring
            line_values = subset["line_value"].to_numpy(dtype=float)
            projected_means = np.array([sim.summary.mean for sim in simulations], dtype=float)
            safe_lines = np.maximum(line_values, 0.5)
            divergence_ratio = np.abs(projected_means - line_values) / safe_lines
            divergent_count = int((divergence_ratio > 0.40).sum())
            if divergent_count > 0:
                _log.info(
                    "%s: %d/%d predictions diverge >40%% from sportsbook line",
                    market_key, divergent_count, len(subset),
                )

            for row, simulation, raw_prob, calibrated_prob in zip(
                subset.to_dict("records"),
                simulations,
                raw_probs,
                calibrated,
                strict=False,
            ):
                summary = simulation.summary
                signal_summary = self._summarize_row_signals(row, market_model.feature_columns)
                tier = str(row.get("_data_sufficiency_tier", "A"))
                data_confidence = _compute_data_confidence(
                    tier=tier,
                    variance=summary.variance,
                    mean=summary.mean,
                    historical_games=int(row.get("historical_games", 0)),
                    days_since_last_game=float(
                        row.get("player_days_since_last_game", row.get("days_rest", np.nan))
                    )
                    if pd.notna(row.get("player_days_since_last_game", row.get("days_rest", np.nan)))
                    else None,
                    injury_return=bool(row.get("player_injury_return_flag", False)),
                    team_changed=bool(row.get("team_changed_recently", row.get("_team_changed", False))),
                )
                prediction_models.append(
                    Prediction(
                        model_run_id=model_run_id,
                        game_id=int(row["game_id"]),
                        player_id=int(row["player_id"]),
                        market_id=market_rows[market_key],
                        line_snapshot_id=int(row["snapshot_id"]),
                        predicted_at=timestamp,
                        projected_mean=summary.mean,
                        projected_variance=summary.variance,
                        projected_median=summary.median,
                        over_probability=float(raw_prob),
                        under_probability=summary.under_probability,
                        confidence_interval_low=summary.ci_low,
                        confidence_interval_high=summary.ci_high,
                        calibration_adjusted_probability=float(calibrated_prob),
                        feature_attribution_summary={
                            "signal_summary": signal_summary,
                            "top_features": signal_summary,
                            "simulation_samples": simulation.samples_used,
                            "simulation_margin_of_error": simulation.margin_of_error,
                            "data_sufficiency_tier": tier,
                            "data_confidence_score": data_confidence,
                        },
                    )
                )
                predictions.append(
                    PropPrediction(
                        player_id=int(row["player_id"]),
                        player_name=str(row["player_name"]),
                        game_id=int(row["game_id"]),
                        market_key=market_key,
                        sportsbook_line=float(row["line_value"]),
                        projected_mean=summary.mean,
                        projected_variance=summary.variance,
                        projected_median=summary.median,
                        over_probability=float(raw_prob),
                        under_probability=summary.under_probability,
                        calibrated_over_probability=float(calibrated_prob),
                        percentile_10=summary.p10,
                        percentile_50=summary.median,
                        percentile_90=summary.p90,
                        confidence_interval_low=summary.ci_low,
                        confidence_interval_high=summary.ci_high,
                        top_features=signal_summary,
                        model_version=self._settings.model_version,
                        feature_version=metadata_feature_version,
                        data_freshness={"predicted_at": timestamp},
                        data_sufficiency_tier=tier,
                        data_confidence_score=data_confidence,
                    )
                )
            processed_prediction_rows += len(subset)
        if prediction_models:
            # Delete any existing predictions for the same (model_run, game) pairs
            # before inserting so repeated calls (e.g. after inactive list update)
            # don't accumulate duplicate rows.
            game_ids_being_replaced = {p.game_id for p in prediction_models}
            self._session.query(Prediction).filter(
                Prediction.model_run_id == model_run_id,
                Prediction.game_id.in_(game_ids_being_replaced),
            ).delete(synchronize_session=False)
            self._session.add_all(prediction_models)
        self._session.commit()
        if progress_callback is not None:
            progress_callback(total_prediction_rows, total_prediction_rows, "Prediction generation complete")
        return predictions

    def _annotate_data_sufficiency(self, historical: pd.DataFrame, upcoming: pd.DataFrame) -> pd.DataFrame:
        """Annotate upcoming rows with data sufficiency tiers instead of dropping them.

        Every row is preserved. A ``_data_sufficiency_tier`` column is added
        so downstream consumers can adjust confidence accordingly.
        """
        return annotate_tiers(upcoming=upcoming, historical=historical)

    def _fit_calibrators(
        self,
        frame: pd.DataFrame,
        feature_columns: list[str],
        full_stat_models: StatModelSuite,
        progress_callback: ProgressCallback | None = None,
        progress_state: tuple[int, int] = (0, 0),
    ) -> tuple[dict[str, ProbabilityCalibrator], dict[str, Any]]:
        ordered = frame.sort_values(["game_date", "start_time", "game_id", "player_id"]).reset_index(drop=True)
        oof_raw: dict[str, list[np.ndarray]] = {market_key: [] for market_key in MARKET_TARGETS}
        oof_labels: dict[str, list[np.ndarray]] = {market_key: [] for market_key in MARKET_TARGETS}
        oof_priors: dict[str, list[np.ndarray]] = {market_key: [] for market_key in MARKET_TARGETS}
        priors_complete: dict[str, bool] = {market_key: True for market_key in MARKET_TARGETS}
        splits = self._calibration_splits(ordered)
        if progress_callback is not None:
            progress_callback(
                progress_state[0],
                progress_state[1],
                f"Calibration setup complete ({len(splits)} folds)",
            )

        for split_index, (train_idx, valid_idx) in enumerate(splits, start=1):
            train_frame = ordered.iloc[train_idx].copy()
            valid_frame = ordered.iloc[valid_idx].copy()
            if train_frame.empty or valid_frame.empty:
                continue
            if progress_callback is not None:
                progress_callback(
                    progress_state[0],
                    progress_state[1],
                    f"Calibration fold {split_index}/{len(splits)}: fitting models",
                )

            minutes_model = MinutesModel(self._settings.training_seed)
            minutes_model.fit(train_frame, feature_columns)
            train_predicted_minutes = minutes_model.predict(train_frame)
            train_predicted_minutes_std = minutes_model.predict_uncertainty(train_frame)
            train_frame = _with_minutes_predictions(
                train_frame,
                train_predicted_minutes,
                train_predicted_minutes_std,
            )
            valid_predicted_minutes = minutes_model.predict(valid_frame)
            valid_predicted_minutes_std = minutes_model.predict_uncertainty(valid_frame)
            valid_frame = _with_minutes_predictions(
                valid_frame,
                valid_predicted_minutes,
                valid_predicted_minutes_std,
            )
            train_frame = self._features.apply_post_minutes_features(train_frame, minutes_column="predicted_minutes")
            valid_frame = self._features.apply_post_minutes_features(valid_frame, minutes_column="predicted_minutes")

            stat_models = StatModelSuite(self._settings.training_seed, l1_alpha=self._l1_alpha)
            stat_models.fit(train_frame, feature_columns, l1_alpha=self._l1_alpha)

            for market_key, target_column in MARKET_TARGETS.items():
                if progress_callback is not None:
                    progress_callback(
                        progress_state[0],
                        progress_state[1],
                        f"Calibration fold {split_index}/{len(splits)}: {market_key.upper()}",
                    )
                valid_market = valid_frame.copy()
                if valid_market.empty or target_column not in valid_market.columns:
                    continue
                means, variances = stat_models.models[market_key].predict(valid_market)
                market_lines = valid_market.get(
                    f"line_{market_key}",
                    pd.Series(means, index=valid_market.index, dtype=float),
                ).fillna(pd.Series(means, index=valid_market.index, dtype=float))
                raw_probabilities = self._parallel_market_probabilities(
                    records=valid_market.to_dict("records"),
                    means=means,
                    variances=variances,
                    lines=market_lines.to_numpy(),
                    market_key=market_key,
                    progress_callback=None,
                    progress_state=(0, 0),
                    detail_prefix=f"Calibrating {market_key.upper()}",
                )
                labels = (valid_market[target_column].to_numpy() > market_lines.to_numpy()).astype(int)
                oof_raw[market_key].append(raw_probabilities)
                oof_labels[market_key].append(labels)
                priors = self._market_prior(valid_market, market_key)
                if priors is None:
                    priors_complete[market_key] = False
                else:
                    oof_priors[market_key].append(priors)

        calibrators: dict[str, ProbabilityCalibrator] = {}
        for market_key, target_column in MARKET_TARGETS.items():
            raw_chunks = oof_raw[market_key]
            label_chunks = oof_labels[market_key]
            if raw_chunks and label_chunks:
                raw_probabilities = np.concatenate(raw_chunks)
                labels = np.concatenate(label_chunks)
                if raw_probabilities.size and len(np.unique(labels)) >= 2:
                    calibrator = ProbabilityCalibrator()
                    market_priors = (
                        np.concatenate(oof_priors[market_key])
                        if priors_complete[market_key] and oof_priors[market_key]
                        else None
                    )
                    calibrator.fit(raw_probabilities, labels, market_priors=market_priors)
                    calibrators[market_key] = calibrator
                    continue
            calibrators[market_key] = self._fit_direct_calibrator(
                ordered,
                market_key,
                target_column,
                full_stat_models,
                progress_callback=progress_callback,
                progress_state=progress_state,
            )

        # v1.2.2 Step 4: Per-market ECE threshold monitoring.
        # After all calibrators are fit we compute a lightweight ECE over the
        # concatenated OOF predictions and log a WARNING for any market whose
        # ECE exceeds the 0.05 threshold. This surfaces calibration regressions
        # at training time rather than only in post-hoc backtests. The alert
        # names the market, the ECE value, and the acceptable ceiling so it is
        # immediately actionable in logs.
        _DEFAULT_ECE_THRESHOLD = 0.05
        _MARKET_ECE_THRESHOLDS = {
            "points": 0.07,
            "rebounds": 0.06,
        }
        _N_BINS = 10
        import logging as _logging
        _calib_logger = _logging.getLogger(__name__)
        diagnostics: dict[str, Any] = {}
        for market_key in MARKET_TARGETS:
            raw_chunks = oof_raw[market_key]
            label_chunks = oof_labels[market_key]
            if not raw_chunks or not label_chunks:
                continue
            probs = np.concatenate(raw_chunks)
            lbls = np.concatenate(label_chunks)
            if probs.size < _N_BINS * 2:
                continue
            bin_edges = np.linspace(0.0, 1.0, _N_BINS + 1)
            bin_indices = np.digitize(probs, bin_edges, right=True).clip(1, _N_BINS) - 1
            ece = 0.0
            for b in range(_N_BINS):
                mask = bin_indices == b
                if not mask.any():
                    continue
                bin_conf = float(probs[mask].mean())
                bin_acc = float(lbls[mask].mean())
                ece += mask.sum() * abs(bin_conf - bin_acc)
            ece /= max(probs.size, 1)
            diagnostics[market_key] = {
                "ece": round(float(ece), 6),
                "sample_count": int(probs.size),
                "edge_bucket_diagnostics": _edge_bucket_diagnostics(probs, lbls),
                "high_confidence_gap": _high_confidence_gap(probs, lbls),
            }
            market_threshold = float(_MARKET_ECE_THRESHOLDS.get(market_key, _DEFAULT_ECE_THRESHOLD))
            if ece > market_threshold:
                _calib_logger.warning(
                    "[v1.2.2 ECE ALERT] %s calibration ECE=%.4f exceeds threshold %.2f — "
                    "review distribution model or add more training data for this market.",
                    market_key.upper(),
                    ece,
                    market_threshold,
                )
            else:
                _calib_logger.info(
                    "[v1.2.2 ECE] %s ECE=%.4f (OK, threshold %.2f)",
                    market_key.upper(),
                    ece,
                    market_threshold,
                )

        return calibrators, diagnostics

    def _fit_direct_calibrator(
        self,
        frame: pd.DataFrame,
        market_key: str,
        target_column: str,
        stat_models: StatModelSuite,
        progress_callback: ProgressCallback | None = None,
        progress_state: tuple[int, int] = (0, 0),
    ) -> ProbabilityCalibrator:
        if progress_callback is not None:
            progress_callback(
                progress_state[0],
                progress_state[1],
                f"Fallback calibrator fit for {market_key.upper()}",
            )
        frame_records = frame.to_dict("records")
        mean_predictions, variances = stat_models.models[market_key].predict(frame)
        market_lines = frame.get(
            f"line_{market_key}",
            pd.Series(mean_predictions, index=frame.index, dtype=float),
        ).fillna(pd.Series(mean_predictions, index=frame.index, dtype=float))
        raw_probabilities = self._parallel_market_probabilities(
            records=frame_records,
            means=mean_predictions,
            variances=variances,
            lines=market_lines.to_numpy(),
            market_key=market_key,
            progress_callback=None,
            progress_state=(0, 0),
            detail_prefix=f"Calibrating {market_key.upper()}",
        )
        labels = (frame[target_column].to_numpy() > market_lines.to_numpy()).astype(int)
        calibrator = ProbabilityCalibrator()
        calibrator.fit(raw_probabilities, labels, market_priors=self._market_prior(frame, market_key))
        return calibrator

    def _calibration_splits(self, frame: pd.DataFrame) -> list[tuple[np.ndarray, np.ndarray]]:
        if len(frame) < 120:
            return []
        purge_days = self._settings.calibration_purge_days
        splitter = TimeSeriesSplit(n_splits=max(8, max(2, len(frame) // 120)))
        splits: list[tuple[np.ndarray, np.ndarray]] = []
        dates = pd.to_datetime(frame["game_date"]).dt.normalize()
        for train_idx, valid_idx in splitter.split(frame):
            if len(train_idx) < 60 or len(valid_idx) < 20:
                continue
            first_valid_date = dates.iloc[valid_idx[0]]
            purge_cutoff = first_valid_date - pd.Timedelta(days=purge_days)
            purged_train_idx = train_idx[dates.iloc[train_idx].values < purge_cutoff]
            if len(purged_train_idx) < 60:
                continue
            splits.append((purged_train_idx, valid_idx))
        return splits

    def _summarize_row_signals(self, row: dict[str, Any], feature_columns: list[str]) -> list[str]:
        ranked: list[tuple[str, float]] = []
        for feature in feature_columns:
            value = row.get(feature)
            if value in (None, 0, 0.0):
                continue
            ranked.append((feature, abs(float(value))))
        ranked.sort(key=lambda item: item[1], reverse=True)
        return [f"Heuristic signal from {feature.replace('_', ' ')} at {value:.2f}" for feature, value in ranked[:5]]

    def _simulate_market_rows(
        self,
        *,
        market_key: str,
        subset: pd.DataFrame,
        stat_models: StatModelSuite,
        progress_callback: ProgressCallback | None = None,
        progress_state: tuple[int, int] = (0, 0),
    ) -> list[Any]:
        means, variances = stat_models.models[market_key].predict(subset)
        subset_records = subset.to_dict("records")
        completed_offset, total_prediction_rows = progress_state
        if market_key == "pra" and {"points", "rebounds", "assists"}.issubset(stat_models.models):
            component_predictions = {
                component_key: stat_models.models[component_key].predict(subset)
                for component_key in ("points", "rebounds", "assists")
            }
            tasks: list[tuple[int, dict[str, Any]]] = list(enumerate(subset_records))

            def build_combo_simulation(task: tuple[int, dict[str, Any]]) -> tuple[int, Any]:
                row_index, row = task
                component_inputs = {
                    component_key: {
                        "mean": float(component_predictions[component_key][0][row_index]),
                        "variance": float(component_predictions[component_key][1][row_index]),
                        "context": self._simulation_context(row, component_key),
                    }
                    for component_key in ("points", "rebounds", "assists")
                }
                return row_index, simulate_joint_combo_probability(
                    line=float(row["line_value"]),
                    minutes_mean=float(row.get("predicted_minutes", 0.0)),
                    minutes_std=float(max(row.get("predicted_minutes_std", row.get("minutes_volatility", 0.0)), 1.0)),
                    component_inputs=component_inputs,
                    combo_key="pra",
                    seed=self._settings.training_seed + int(row["player_id"]),
                    dist_family=self._dist_family_for_market("pra"),
                )

            return self._parallel_task_results(
                tasks=tasks,
                worker=build_combo_simulation,
                progress_callback=progress_callback,
                detail_formatter=lambda task: f"Simulating {task[1]['player_name']} PRA ({task[0] + 1}/{len(tasks)})",
                progress_offset=completed_offset,
                progress_total=total_prediction_rows,
            )
        tasks = list(
            enumerate(
                zip(
                    subset_records,
                    means,
                    variances,
                    subset["line_value"].to_numpy(dtype=float),
                    strict=False,
                )
            )
        )

        def build_single_simulation(
            task: tuple[int, tuple[dict[str, Any], float, float, float]]
        ) -> tuple[int, Any]:
            row_index, payload = task
            row, mean, variance, line = payload
            return row_index, simulate_line_probability(
                mean,
                variance,
                line,
                minutes_mean=float(row.get("predicted_minutes", 0.0)),
                minutes_std=float(max(row.get("predicted_minutes_std", row.get("minutes_volatility", 0.0)), 1.0)),
                seed=self._settings.training_seed + int(row["player_id"]),
                market_key=market_key,
                context=self._simulation_context(row, market_key),
                dist_family=self._dist_family_for_market(market_key),
            )

        return self._parallel_task_results(
            tasks=tasks,
            worker=build_single_simulation,
            progress_callback=progress_callback,
            detail_formatter=lambda task: f"Simulating {task[1][0]['player_name']} {market_key.upper()} ({task[0] + 1}/{len(tasks)})",
            progress_offset=completed_offset,
            progress_total=total_prediction_rows,
        )

    def _parallel_market_probabilities(
        self,
        *,
        records: list[dict[str, Any]],
        means: np.ndarray,
        variances: np.ndarray,
        lines: np.ndarray,
        market_key: str,
        progress_callback: ProgressCallback | None,
        progress_state: tuple[int, int],
        detail_prefix: str,
    ) -> np.ndarray:
        tasks = list(enumerate(zip(records, means, variances, lines, strict=False)))

        def summarize_task(task: tuple[int, tuple[dict[str, Any], float, float, float]]) -> tuple[int, float]:
            index, payload = task
            row, mean, variance, line = payload
            minutes_mean = float(row.get("predicted_minutes") or row.get("minutes_avg_10") or 0.0) or None
            minutes_std = float(row.get("predicted_minutes_std") or row.get("minutes_volatility") or 0.0) or None
            probability = summarize_line_probability(
                mean,
                variance,
                float(line),
                minutes_mean=minutes_mean,
                minutes_std=minutes_std,
                market_key=market_key,
                context=self._simulation_context(row, market_key),
                dist_family=self._dist_family_for_market(market_key),
            ).over_probability
            return index, float(probability)

        values = self._parallel_task_results(
            tasks=tasks,
            worker=summarize_task,
            progress_callback=progress_callback,
            detail_formatter=lambda task: f"{detail_prefix}: {task[1][0]['player_name']} ({task[0] + 1}/{len(tasks)})",
            progress_offset=progress_state[0],
            progress_total=progress_state[1],
        )
        return np.asarray(values, dtype=float)

    def _parallel_task_results(
        self,
        *,
        tasks: list[tuple[int, Any]],
        worker: Callable[[tuple[int, Any]], tuple[int, Any]],
        progress_callback: ProgressCallback | None,
        detail_formatter: Callable[[tuple[int, Any]], str],
        progress_offset: int,
        progress_total: int,
    ) -> list[Any]:
        if not tasks:
            return []
        results: list[Any] = [None] * len(tasks)
        worker_count = min(get_runtime_budget().worker_count, len(tasks))
        if worker_count <= 1:
            for completed_count, task in enumerate(tasks, start=1):
                result_index, result_value = worker(task)
                results[result_index] = result_value
                if progress_callback is not None and progress_total > 0:
                    progress_callback(
                        progress_offset + completed_count,
                        progress_total,
                        detail_formatter(task),
                    )
            return results
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(worker, task): task for task in tasks}
            completed_count = 0
            for future in as_completed(future_map):
                task = future_map[future]
                result_index, result_value = future.result()
                results[result_index] = result_value
                completed_count += 1
                if progress_callback is not None and progress_total > 0:
                    progress_callback(
                        progress_offset + completed_count,
                        progress_total,
                        detail_formatter(task),
                    )
        return results

    def _market_prior(self, frame: pd.DataFrame, market_key: str) -> np.ndarray | None:
        for column in (f"{market_key}_consensus_prob_mean", "consensus_prob_mean", "no_vig_over_probability"):
            if column in frame.columns:
                return _neutralize_probability_series(frame[column]).to_numpy(dtype=float)
        return None

    def _dist_family_for_market(self, market_key: str) -> DistFamily:
        if self._dist_family_override is not None:
            return self._dist_family_override
        return MARKET_DIST_FAMILY_DEFAULTS.get(market_key, DEFAULT_DIST_FAMILY)

    def _stabilize_over_probabilities(
        self,
        *,
        subset: pd.DataFrame,
        market_key: str,
        raw_probabilities: np.ndarray,
        calibrated_probabilities: np.ndarray,
    ) -> np.ndarray:
        calibrated = np.clip(np.asarray(calibrated_probabilities, dtype=float), 1e-4, 1.0 - 1e-4)
        raw = np.clip(np.asarray(raw_probabilities, dtype=float), 1e-4, 1.0 - 1e-4)
        if calibrated.shape != raw.shape or subset.empty:
            return raw
        # Two stabilization passes:
        # 1. Tail-line anchor: when the book prices a prop near-certainty (< 20%
        #    one side), apply a small blend toward the book target.
        # 2. Disagreement anchor: when the model is extreme (> 85% or < 15%) but
        #    the book is near 50/50, the model is likely wrong — shrink toward
        #    the market price proportionally to the disagreement magnitude.
        offered_over = self._offered_over_probabilities(subset)
        book_target = np.clip(0.70 * offered_over + 0.30 * raw, 1e-4, 1.0 - 1e-4)

        # Pass 1: tail-line anchor (existing logic)
        book_tail_probability = np.minimum(offered_over, 1.0 - offered_over)
        tail_pressure = np.clip((0.20 - book_tail_probability) / 0.15, 0.0, 1.0)
        tail_weight = np.clip(0.08 * tail_pressure, 0.0, 0.08)

        # Pass 2: disagreement anchor — activates when model diverges strongly
        # from the market.  The larger the disagreement, the more we trust the
        # book (up to 50% blend).  Only fires when book is in the "normal" range
        # (15-85%) so we don't fight genuinely extreme book pricing.
        disagreement = np.abs(calibrated - offered_over)
        book_is_normal = (offered_over >= 0.15) & (offered_over <= 0.85)
        model_is_extreme = (calibrated > 0.85) | (calibrated < 0.15)
        disagree_pressure = np.where(
            book_is_normal & model_is_extreme,
            np.clip((disagreement - 0.20) / 0.30, 0.0, 1.0),
            0.0,
        )
        disagree_weight = 0.50 * disagree_pressure

        shrink_weight = np.maximum(tail_weight, disagree_weight)
        stabilized = (1.0 - shrink_weight) * calibrated + shrink_weight * book_target
        return np.clip(stabilized, 1e-4, 1.0 - 1e-4)

    def _offered_over_probabilities(self, subset: pd.DataFrame) -> np.ndarray:
        over_probability = _numeric_series(subset.get("no_vig_over_probability"), np.nan).to_numpy(dtype=float)
        raw_implied_over = _numeric_series(subset.get("raw_implied_over_probability"), np.nan).to_numpy(dtype=float)
        raw_implied_under = _numeric_series(subset.get("raw_implied_under_probability"), np.nan).to_numpy(dtype=float)
        consensus_probability = _numeric_series(subset.get("consensus_prob_mean"), np.nan).to_numpy(dtype=float)

        offered = np.where(
            np.isfinite(over_probability) & (over_probability > 0.0),
            over_probability,
            np.nan,
        )
        offered = np.where(
            np.isnan(offered) & np.isfinite(raw_implied_over) & (raw_implied_over > 0.0),
            raw_implied_over,
            offered,
        )
        reverse_under = 1.0 - raw_implied_under
        offered = np.where(
            np.isnan(offered) & np.isfinite(reverse_under) & (reverse_under > 0.0) & (reverse_under < 1.0),
            reverse_under,
            offered,
        )
        offered = np.where(
            np.isnan(offered) & np.isfinite(consensus_probability) & (consensus_probability > 0.0),
            consensus_probability,
            offered,
        )
        offered = np.where(np.isnan(offered), 0.5, offered)
        return np.clip(offered.astype(float), 0.025, 0.975)

    def _simulation_context(self, row: dict[str, Any], market_key: str) -> dict[str, float]:
        base_context = {
            "usage_rate": _row_float(
                row,
                ("usage_rate_blended", "usage_rate_avg_10", "usage_rate"),
                fallback=0.20,
                minimum=0.01,
            ),
            "touches_per_minute": _row_float(
                row,
                ("touches_per_minute_blended", "touches_per_minute_avg_10", "touches_per_minute"),
                fallback=0.0,
                minimum=0.0,
            ),
            "passes_per_minute": _row_float(
                row,
                ("passes_per_minute_blended", "passes_per_minute_avg_10", "passes_per_minute"),
                fallback=0.0,
                minimum=0.0,
            ),
            "assist_creation_proxy_per_minute": _row_float(
                row,
                (
                    "assist_creation_proxy_per_minute_blended",
                    "assist_creation_proxy_per_minute_avg_10",
                    "assist_creation_proxy_per_minute",
                ),
                fallback=0.0,
                minimum=0.0,
            ),
            "rebound_chances_total_per_minute": _row_float(
                row,
                (
                    "rebound_chances_total_per_minute_blended",
                    "rebound_chances_total_per_minute_avg_10",
                    "rebound_chances_total_per_minute",
                ),
                fallback=0.0,
                minimum=0.0,
            ),
            "rebound_conversion_rate": _row_float(
                row,
                ("rebound_conversion_rate_blended", "rebound_conversion_rate_avg_10", "rebound_conversion_rate"),
                fallback=0.35,
                minimum=0.01,
            ),
            "field_goal_attempts_per_minute": _row_float(
                row,
                ("field_goal_attempts_per_minute_blended", "field_goal_attempts_per_minute_avg_10", "field_goal_attempts_per_minute"),
                fallback=0.0,
                minimum=0.0,
            ),
            "free_throw_attempts_per_minute": _row_float(
                row,
                ("free_throw_attempts_per_minute_blended", "free_throw_attempts_per_minute_avg_10", "free_throw_attempts_per_minute"),
                fallback=0.02,
                minimum=0.0,
            ),
            "estimated_three_point_attempts_per_minute": _row_float(
                row,
                (
                    "estimated_three_point_attempts_per_minute_blended",
                    "estimated_three_point_attempts_per_minute_avg_10",
                    "estimated_three_point_attempts_per_minute",
                ),
                fallback=0.0,
                minimum=0.0,
            ),
            "percentage_field_goals_attempted_3pt": _row_float(
                row,
                (
                    "percentage_field_goals_attempted_3pt_blended",
                    "percentage_field_goals_attempted_3pt_avg_10",
                    "percentage_field_goals_attempted_3pt",
                ),
                fallback=0.35,
                minimum=0.01,
            ),
            "true_shooting_percentage": _row_float(
                row,
                ("true_shooting_percentage_blended", "true_shooting_percentage_avg_10", "true_shooting_percentage"),
                fallback=0.58,
                minimum=0.01,
            ),
            "turnover_ratio": _row_float(
                row,
                ("turnover_ratio_blended", "turnover_ratio_avg_10", "turnover_ratio"),
                fallback=0.12,
                minimum=0.01,
            ),
            "points_pace_exposure": _row_float(
                row,
                ("points_pace_exposure",),
                fallback=0.0,
                minimum=0.0,
            ),
            "points_3pt_variance": _row_float(
                row,
                ("points_3pt_variance",),
                fallback=0.0,
                minimum=0.0,
            ),
            "rebounds_std_10": _row_float(
                row,
                ("rebounds_std_10",),
                fallback=0.0,
                minimum=0.0,
            ),
        }
        threes_per_minute = _row_float(
            row,
            ("threes_per_minute_blended", "threes_per_minute_avg_10"),
            fallback=0.0,
            minimum=0.0,
        )
        if base_context["estimated_three_point_attempts_per_minute"] > 1e-6 and threes_per_minute > 0.0:
            base_context["three_point_make_rate"] = threes_per_minute / max(
                base_context["estimated_three_point_attempts_per_minute"],
                1e-6,
            )
        else:
            base_context["three_point_make_rate"] = 0.35
        if market_key == "assists":
            assists_rate = _row_float(
                row,
                ("assists_per_minute_blended", "assists_per_minute_avg_10", "assists_per_minute"),
                fallback=0.0,
                minimum=0.0,
            )
            if base_context["assist_creation_proxy_per_minute"] > 1e-6 and assists_rate > 0.0:
                base_context["assist_conversion_rate"] = assists_rate / max(
                    base_context["assist_creation_proxy_per_minute"],
                    1e-6,
                )
            else:
                base_context["assist_conversion_rate"] = 0.32
        return base_context

    def _provider_context_counts(self) -> dict[str, Any]:
        day_start = datetime.combine(date.today(), datetime.min.time(), tzinfo=UTC)
        rows = (
            self._session.query(RawPayload.provider_name, func.count(RawPayload.payload_id))
            .filter(RawPayload.fetched_at >= day_start)
            .group_by(RawPayload.provider_name)
            .all()
        )
        return {str(provider_name): int(count or 0) for provider_name, count in rows}


PROBABILITY_FEATURE_TOKENS = (
    "raw_implied_over_probability",
    "raw_implied_under_probability",
    "no_vig_over_probability",
    "no_vig_under_probability",
    "consensus_prob_mean",
)
def _apply_feature_defaults(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    available_columns = [column for column in feature_columns if column in frame.columns]
    if not available_columns:
        return frame
    filled = frame.copy()
    for column in available_columns:
        numeric = pd.to_numeric(filled[column], errors="coerce")
        if any(token in column for token in PROBABILITY_FEATURE_TOKENS):
            filled[column] = _neutralize_probability_series(numeric)
        else:
            filled[column] = numeric.fillna(0.0)
    return filled


def _neutralize_probability_series(series: pd.Series) -> pd.Series:
    numeric = pd.Series(pd.to_numeric(series, errors="coerce"))
    invalid_mask = numeric.isna() | (numeric <= 0.005) | (numeric >= 0.995)
    return numeric.mask(invalid_mask, 0.5).clip(lower=0.005, upper=0.995)


def _numeric_series(values: pd.Series | object, fallback: float | np.ndarray) -> pd.Series:
    if isinstance(values, pd.Series):
        numeric = pd.to_numeric(values, errors="coerce")
    else:
        numeric = pd.Series(values)
        numeric = pd.to_numeric(numeric, errors="coerce")
    if np.isscalar(fallback):
        return numeric.fillna(float(fallback))
    fallback_series = pd.Series(np.asarray(fallback, dtype=float), index=numeric.index)
    return numeric.where(numeric.notna(), fallback_series)


def _row_float(
    row: dict[str, Any],
    keys: tuple[str, ...],
    *,
    fallback: float,
    minimum: float = 0.0,
) -> float:
    for key in keys:
        value = row.get(key)
        try:
            numeric = float(value)
        except Exception:
            continue
        if not np.isfinite(numeric):
            continue
        if numeric > minimum:
            return numeric
    return float(fallback)


def _edge_bucket_diagnostics(raw_probabilities: np.ndarray, labels: np.ndarray) -> dict[str, dict[str, float | int]]:
    probs = np.asarray(raw_probabilities, dtype=float)
    y = np.asarray(labels, dtype=int)
    buckets: dict[str, tuple[float, float]] = {
        "low_edge": (0.45, 0.55),
        "medium_edge": (0.35, 0.65),
        "high_edge": (0.20, 0.80),
    }
    diagnostics: dict[str, dict[str, float | int]] = {}
    for bucket_name, (lower, upper) in buckets.items():
        mask = (probs <= lower) | (probs >= upper)
        if not mask.any():
            diagnostics[bucket_name] = {"sample_count": 0, "brier": float("nan"), "accuracy": float("nan")}
            continue
        bucket_probs = probs[mask]
        bucket_labels = y[mask]
        predictions = (bucket_probs >= 0.5).astype(int)
        brier = float(np.mean((bucket_probs - bucket_labels) ** 2))
        accuracy = float(np.mean(predictions == bucket_labels))
        diagnostics[bucket_name] = {
            "sample_count": int(mask.sum()),
            "brier": round(brier, 6),
            "accuracy": round(accuracy, 6),
        }
    return diagnostics


def _high_confidence_gap(raw_probabilities: np.ndarray, labels: np.ndarray) -> dict[str, float | int]:
    probs = np.asarray(raw_probabilities, dtype=float)
    y = np.asarray(labels, dtype=int)
    mask = (probs >= 0.65) | (probs <= 0.35)
    if not mask.any():
        return {"sample_count": 0, "predicted_rate": float("nan"), "actual_rate": float("nan"), "gap": float("nan")}
    bucket_probs = probs[mask]
    bucket_labels = y[mask]
    predicted = float(bucket_probs.mean())
    actual = float(bucket_labels.mean())
    return {
        "sample_count": int(mask.sum()),
        "predicted_rate": round(predicted, 6),
        "actual_rate": round(actual, 6),
        "gap": round(predicted - actual, 6),
    }


def _training_data_quality_checks(frame: pd.DataFrame, feature_columns: list[str]) -> dict[str, object]:
    if frame.empty:
        return {
            "status": "degraded",
            "row_count": 0,
            "reason": "empty_training_frame",
        }
    numeric = frame.select_dtypes(include=["number"])
    finite_ratio = float(np.isfinite(numeric.to_numpy(dtype=float)).mean()) if not numeric.empty else 1.0
    null_fraction = float(numeric.isna().mean().mean()) if not numeric.empty else 0.0
    extreme_minutes_rows = int((pd.to_numeric(frame.get("minutes", 0.0), errors="coerce").fillna(0.0) > 60.0).sum())
    missing_feature_columns = int(sum(1 for column in feature_columns if column not in frame.columns))

    # P2 CHANGE 6A: Check for fillna contamination - rows where rolling avg is 0 but the underlying stat is positive
    # This indicates insufficient history was filled with zeros instead of meaningful fallbacks.
    fillna_contamination = []
    for stat in ("points", "rebounds", "assists"):
        avg_col = f"{stat}_avg_5"
        if avg_col in frame.columns and stat in frame.columns:
            contaminated = ((frame[avg_col] == 0.0) & (frame[stat] > 0)).sum()
            contamination_ratio = contaminated / len(frame) if len(frame) > 0 else 0.0
            fillna_contamination.append((contamination_ratio, stat))

    # P2 CHANGE 6B: Check for insufficient-history rows
    low_history_rows = []
    if "history_games_played" in frame.columns:
        low_history_count = (frame["history_games_played"] < 5).sum()
        low_history_ratio = low_history_count / len(frame) if len(frame) > 0 else 0.0
        low_history_rows.append((low_history_ratio, "insufficient_history"))

    status = "healthy"
    reasons: list[str] = []
    if null_fraction > 0.25:
        status = "degraded"
        reasons.append("high_null_fraction")
    if finite_ratio < 0.95:
        status = "degraded"
        reasons.append("low_finite_ratio")
    if extreme_minutes_rows > 0:
        reasons.append("extreme_minutes_rows_present")
    if missing_feature_columns > 0:
        reasons.append("missing_feature_columns")

    # Add fillna contamination reasons if threshold exceeded (>5%)
    for ratio, stat in fillna_contamination:
        if ratio > 0.05:
            status = "degraded"
            reasons.append(f"fillna_contamination_{stat}")

    # Add insufficient history reason if threshold exceeded (>10%)
    for ratio, label in low_history_rows:
        if ratio > 0.10:
            status = "degraded"
            reasons.append(label)

    return {
        "status": status,
        "row_count": int(len(frame)),
        "numeric_finite_ratio": round(finite_ratio, 6),
        "numeric_null_fraction": round(null_fraction, 6),
        "extreme_minutes_rows": extreme_minutes_rows,
        "missing_feature_columns": missing_feature_columns,
        "fillna_contamination_ratios": {stat: ratio for ratio, stat in fillna_contamination},
        "low_history_ratio": low_history_rows[0][0] if low_history_rows else 0.0,
        "reasons": reasons,
    }


def _with_minutes_predictions(
    frame: pd.DataFrame,
    predicted_minutes: np.ndarray,
    predicted_minutes_std: np.ndarray,
) -> pd.DataFrame:
    return _with_output_columns(
        frame,
        {
            "predicted_minutes": predicted_minutes,
            "predicted_minutes_std": predicted_minutes_std,
        },
    )


def _with_output_columns(frame: pd.DataFrame, outputs: dict[str, object] | pd.DataFrame) -> pd.DataFrame:
    output_frame = outputs if isinstance(outputs, pd.DataFrame) else pd.DataFrame(outputs, index=frame.index)
    if output_frame.empty:
        return frame.copy()
    output_frame = output_frame.loc[:, ~output_frame.columns.duplicated(keep="last")].copy()
    result = frame.drop(columns=list(output_frame.columns), errors="ignore").copy()
    return pd.concat([result, output_frame], axis=1).copy()
