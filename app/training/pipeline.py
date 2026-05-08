from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sqlalchemy import bindparam, func, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.core.resources import get_runtime_budget
from app.models.all import ModelRun, Prediction, PropMarket, RawPayload
from app.schemas.domain import PropPrediction
from app.services.rotation_audit import dataclass_records, write_game_audit
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
    DistributionSummary,
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
from app.training.rotation import (
    PlayerRotationProfile,
    RoleVector,
    RotationWeight,
    RotationWeightTable,
    classify_archetype,
    redistribute,
    status_to_play_probability,
)
from app.training.rotation_monte_carlo import dnp_risk_from_branches, enumerate_or_sample_branches

_log = __import__("logging").getLogger(__name__)

ProgressCallback = Callable[[int | None, int | None, str], None]


@dataclass
class TrainedBundle:
    minutes_model: MinutesModel
    stat_models: StatModelSuite
    calibrators: dict[str, ProbabilityCalibrator]
    metadata: dict[str, Any]


_TIER_BASE_CONFIDENCE = {"A": 0.90, "B": 0.70, "C": 0.45, "D": 0.25}
_SHADOW_BASELINE_MINUTES_THRESHOLD = 12.0
_ROTATION_SHOCK_VERSION = "v1_phase9"


def _ensure_player_team_id_column(frame: pd.DataFrame) -> pd.DataFrame:
    """Align with historical loaders: player's team must appear as ``player_team_id``.

    Some upcoming/projection frames only expose ``team_id``; rotation and pace groupbys require
    ``player_team_id``.
    """

    if frame.empty or "player_team_id" in frame.columns:
        return frame
    if "team_id" not in frame.columns:
        return frame
    result = frame.copy()
    result["player_team_id"] = result["team_id"]
    return result


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

        minutes_feature_columns = sorted(
            set(
                feature_set.feature_columns
                + [
                    column
                    for column in frame.columns
                    if column.endswith("_baseline_mean") or column.endswith("_baseline_variance")
                ]
            )
        )
        frame = _apply_feature_defaults(frame, minutes_feature_columns)
        population_priors = self._features.build_population_priors(frame, minutes_feature_columns)
        current_step += 1
        emit("Fitting minutes model")
        minutes_model = MinutesModel(self._settings.training_seed)
        minutes_model.fit(frame, minutes_feature_columns)
        predicted_minutes = minutes_model.predict(frame)
        predicted_minutes_std = minutes_model.predict_uncertainty(frame)
        frame = _with_minutes_predictions(frame, predicted_minutes, predicted_minutes_std)
        frame = self._features.apply_post_minutes_features(frame, minutes_column="predicted_minutes")
        frame = self._apply_rotation_shadow_mode(frame)
        frame = self._apply_rotation_treatment_mode(frame, write_audit=False)
        feature_columns = _stat_feature_columns(self._features, frame, minutes_feature_columns)
        frame = _apply_feature_defaults(frame, feature_columns)
        training_data_quality = _training_data_quality_checks(frame, feature_columns)

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
                minutes_feature_columns,
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
            "minutes_feature_columns": minutes_feature_columns,
            "artifact_namespace": self._artifact_namespace,
            "rotation_shock": self._rotation_metadata(),
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
        *,
        persist_predictions: bool = True,
    ) -> list[PropPrediction]:
        """Generate and persist predictions for upcoming games.

        When ``game_ids`` is provided only those games are (re-)predicted and
        their existing predictions for the current model run are replaced.
        When ``game_ids`` is None all upcoming games are run (standard startup
        path) and predictions for all returned games are replaced.

        When ``persist_predictions`` is False, rows are computed and returned but
        the database Prediction table is not modified (shadow / compare runs).
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
        rotation_mode = _rotation_shock_mode()
        frame = self._apply_rotation_treatment_mode(frame, write_audit=persist_predictions)
        frame = _apply_feature_defaults(frame, trained_feature_columns)
        if rotation_mode == "full":
            absence_profiles = self._load_rotation_shadow_absence_profiles(frame)
            availability_context = _availability_branch_context(
                frame,
                absence_profiles,
                max_exact_players=8,
                sampled_branch_count=self._settings.simulation_min_samples,
            )
            branch_frames_by_team = self._build_branch_simulation_frames(
                frame=frame,
                absence_profiles=absence_profiles,
                max_exact_players=8,
                sampled_branch_count=self._settings.simulation_min_samples,
            )
        else:
            availability_context = {}
            branch_frames_by_team = {}
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
                branch_frames_by_team=branch_frames_by_team,
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
                availability = _lookup_availability_context(availability_context, row)
                dnp_risk = float(availability.get("dnp_risk", 0.0))
                boom_probability, bust_probability = summary.boom_probability, summary.bust_probability
                p25, p75 = summary.p25, summary.p75
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
                            "availability_branches": int(availability.get("availability_branches", 1)),
                            "dnp_risk": dnp_risk,
                            "boom_probability": boom_probability,
                            "bust_probability": bust_probability,
                            "percentile_25": p25,
                            "percentile_75": p75,
                            "rotation_shock": {
                                "metadata": metadata.get("rotation_shock", {}),
                                "baseline_projected_minutes": float(
                                    _row_optional_float(row, ("baseline_projected_minutes",)) or 0.0
                                ),
                                "adjusted_projected_minutes": float(
                                    _row_optional_float(row, ("adjusted_projected_minutes", "predicted_minutes")) or 0.0
                                ),
                                "rotation_shock_magnitude": float(
                                    _row_optional_float(row, ("rotation_shock_magnitude",)) or 0.0
                                ),
                                "rotation_shock_confidence": float(
                                    _row_optional_float(row, ("rotation_shock_confidence",)) or 1.0
                                ),
                            },
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
                        percentile_25=p25,
                        percentile_75=p75,
                        dnp_risk=dnp_risk,
                        boom_probability=boom_probability,
                        bust_probability=bust_probability,
                        availability_branches=int(availability.get("availability_branches", 1)),
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
        if prediction_models and persist_predictions:
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

    def _apply_rotation_shadow_mode(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not _env_flag_enabled("ROTATION_SHOCK_SHADOW_MODE"):
            return frame
        if frame.empty:
            return frame

        result = _ensure_player_team_id_column(frame.copy())
        if "baseline_projected_minutes" not in result.columns:
            result["baseline_projected_minutes"] = pd.to_numeric(result.get("predicted_minutes", 0.0), errors="coerce").fillna(0.0)
        if "baseline_usage_share" not in result.columns:
            result["baseline_usage_share"] = _first_numeric_column(
                result,
                ("usage_rate_blended", "usage_rate_avg_10", "usage_rate"),
                fallback=0.0,
            )
        if "baseline_usage_rate" not in result.columns:
            result["baseline_usage_rate"] = result["baseline_usage_share"]

        result["adjusted_projected_minutes"] = result["baseline_projected_minutes"]
        result["adjusted_usage_share"] = result["baseline_usage_share"]
        result["adjusted_usage_rate"] = result["baseline_usage_rate"]

        if "player_team_id" not in result.columns:
            return result

        weights_table = self._load_rotation_weights_table()
        absence_profiles = self._load_rotation_shadow_absence_profiles(result)
        game_audits: dict[int, dict[str, list[dict[str, Any]]]] = {}
        for (game_id, team_id), group in result.groupby(["game_id", "player_team_id"], dropna=False):
            if pd.isna(game_id) or pd.isna(team_id):
                continue
            game_id_int = int(game_id)
            team_id_int = int(team_id)
            group_absences = _matching_absence_profiles(absence_profiles, game_id=game_id_int, team_id=team_id_int)
            profiles, play_probabilities = _build_rotation_profiles(group, absent_rows=group_absences)
            redistribution = redistribute(
                game_id=game_id_int,
                team_id=team_id_int,
                players=profiles,
                weights=weights_table,
                play_probabilities=play_probabilities,
                mode="expected_value",
            )
            result.loc[group.index, "team_efficiency_delta"] = float(redistribution.team_efficiency_delta)
            result.loc[group.index, "pace_delta"] = float(redistribution.pace_delta)
            result.loc[group.index, "rotation_shock_magnitude"] = float(redistribution.rotation_shock_magnitude)
            result.loc[group.index, "rotation_shock_confidence"] = float(redistribution.rotation_shock_confidence)
            adjusted_by_player = {player.player_id: player for player in redistribution.adjusted_players}
            for row_index, row in group.iterrows():
                player_id = int(row["player_id"])
                adjusted = adjusted_by_player.get(player_id)
                if adjusted is None:
                    continue
                result.at[row_index, "adjusted_projected_minutes"] = float(adjusted.adjusted_minutes)
                result.at[row_index, "adjusted_usage_share"] = float(adjusted.adjusted_usage_share)
                result.at[row_index, "adjusted_usage_rate"] = float(adjusted.adjusted_usage_share)
            audit = game_audits.setdefault(
                game_id_int,
                {"absences": [], "adjustments": [], "team_environment": []},
            )
            audit["absences"].extend(dataclass_records(list(redistribution.absences)))
            audit["adjustments"].extend(dataclass_records(list(redistribution.teammate_adjustments)))
            audit["team_environment"].append(
                {
                    "game_id": game_id_int,
                    "team_id": team_id_int,
                    "team_efficiency_delta": redistribution.team_efficiency_delta,
                    "pace_delta": redistribution.pace_delta,
                    "rotation_shock_magnitude": redistribution.rotation_shock_magnitude,
                    "rotation_shock_confidence": redistribution.rotation_shock_confidence,
                }
            )
        for game_id, audit in game_audits.items():
            write_game_audit(
                game_id=game_id,
                absences=audit["absences"],
                adjustments=audit["adjustments"],
                team_environment=audit["team_environment"],
            )
        return result

    def _load_rotation_shadow_absence_profiles(
        self, frame: pd.DataFrame, historical_frame: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        explicit_absences = self._load_explicit_rotation_absences(frame)
        return _build_shadow_absence_profiles(frame, explicit_absences, historical_frame=historical_frame)

    def _apply_rotation_treatment_mode(
        self,
        frame: pd.DataFrame,
        *,
        write_audit: bool,
        historical_frame: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        mode = _rotation_shock_mode()
        if mode == "off" or frame.empty:
            return frame
        result = self._ensure_rotation_baseline_columns(_ensure_player_team_id_column(frame.copy()))
        result = _with_neutral_rotation_environment(result)
        if mode == "full":
            result = self._apply_rotation_adjustments(result, write_audit=write_audit, historical_frame=historical_frame)
            result["predicted_minutes"] = result["adjusted_projected_minutes"]
            result = self._features.apply_post_minutes_features(result, minutes_column="predicted_minutes")
        else:
            result["adjusted_projected_minutes"] = pd.to_numeric(
                result.get("baseline_projected_minutes", result.get("predicted_minutes", 0.0)),
                errors="coerce",
            ).fillna(0.0)
            result["adjusted_usage_share"] = pd.to_numeric(
                result.get("baseline_usage_share", result.get("usage_rate_blended", 0.0)),
                errors="coerce",
            ).fillna(0.0)
            result["adjusted_usage_rate"] = pd.to_numeric(
                result.get("baseline_usage_rate", result.get("usage_rate_blended", 0.0)),
                errors="coerce",
            ).fillna(0.0)
        return self._attach_adjusted_rate_columns(result)

    def _apply_rotation_adjustments(
        self,
        frame: pd.DataFrame,
        *,
        write_audit: bool,
        historical_frame: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        result = _ensure_player_team_id_column(frame.copy())
        if "player_team_id" not in result.columns:
            return result
        weights_table = self._load_rotation_weights_table()
        absence_profiles = self._load_rotation_shadow_absence_profiles(result, historical_frame=historical_frame)
        game_audits: dict[int, dict[str, list[dict[str, Any]]]] = {}
        for (game_id, team_id), group in result.groupby(["game_id", "player_team_id"], dropna=False):
            if pd.isna(game_id) or pd.isna(team_id):
                continue
            game_id_int = int(game_id)
            team_id_int = int(team_id)
            group_absences = _matching_absence_profiles(absence_profiles, game_id=game_id_int, team_id=team_id_int)
            profiles, play_probabilities = _build_rotation_profiles(group, absent_rows=group_absences)
            redistribution = redistribute(
                game_id=game_id_int,
                team_id=team_id_int,
                players=profiles,
                weights=weights_table,
                play_probabilities=play_probabilities,
                mode="expected_value",
            )
            result.loc[group.index, "team_efficiency_delta"] = float(redistribution.team_efficiency_delta)
            result.loc[group.index, "pace_delta"] = float(redistribution.pace_delta)
            result.loc[group.index, "rotation_shock_magnitude"] = float(redistribution.rotation_shock_magnitude)
            result.loc[group.index, "rotation_shock_confidence"] = float(redistribution.rotation_shock_confidence)
            adjusted_by_player = {player.player_id: player for player in redistribution.adjusted_players}
            for row_index, row in group.iterrows():
                player_id = int(row["player_id"])
                adjusted = adjusted_by_player.get(player_id)
                if adjusted is None:
                    continue
                result.at[row_index, "adjusted_projected_minutes"] = float(adjusted.adjusted_minutes)
                result.at[row_index, "adjusted_usage_share"] = float(adjusted.adjusted_usage_share)
                result.at[row_index, "adjusted_usage_rate"] = float(adjusted.adjusted_usage_share)
            audit = game_audits.setdefault(
                game_id_int,
                {"absences": [], "adjustments": [], "team_environment": []},
            )
            audit["absences"].extend(dataclass_records(list(redistribution.absences)))
            audit["adjustments"].extend(dataclass_records(list(redistribution.teammate_adjustments)))
            audit["team_environment"].append(
                {
                    "game_id": game_id_int,
                    "team_id": team_id_int,
                    "team_efficiency_delta": redistribution.team_efficiency_delta,
                    "pace_delta": redistribution.pace_delta,
                    "rotation_shock_magnitude": redistribution.rotation_shock_magnitude,
                    "rotation_shock_confidence": redistribution.rotation_shock_confidence,
                }
            )
        if write_audit:
            for game_id, audit in game_audits.items():
                write_game_audit(
                    game_id=game_id,
                    absences=audit["absences"],
                    adjustments=audit["adjustments"],
                    team_environment=audit["team_environment"],
                )
        return result

    def _ensure_rotation_baseline_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        if "predicted_minutes" in result.columns:
            result["baseline_projected_minutes"] = pd.to_numeric(result["predicted_minutes"], errors="coerce").fillna(0.0)
        elif "baseline_projected_minutes" not in result.columns:
            result["baseline_projected_minutes"] = pd.Series(0.0, index=result.index, dtype=float)

        usage_source = _first_numeric_column(
            result,
            ("usage_rate_blended", "usage_rate_avg_10", "usage_rate"),
            fallback=np.nan,
        )
        existing_usage = pd.to_numeric(result.get("baseline_usage_share", pd.Series(np.nan, index=result.index)), errors="coerce")
        result["baseline_usage_share"] = usage_source.where(usage_source.notna(), existing_usage).fillna(0.0)

        existing_usage_rate = pd.to_numeric(result.get("baseline_usage_rate", pd.Series(np.nan, index=result.index)), errors="coerce")
        result["baseline_usage_rate"] = usage_source.where(usage_source.notna(), existing_usage_rate).fillna(
            result["baseline_usage_share"]
        )
        result["adjusted_projected_minutes"] = result["baseline_projected_minutes"]
        result["adjusted_usage_share"] = result["baseline_usage_share"]
        result["adjusted_usage_rate"] = result["baseline_usage_rate"]
        return result

    def _attach_adjusted_rate_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        baseline_minutes = pd.to_numeric(result.get("baseline_projected_minutes", 0.0), errors="coerce").fillna(0.0)
        adjusted_minutes = pd.to_numeric(result.get("adjusted_projected_minutes", baseline_minutes), errors="coerce").fillna(0.0)
        baseline_usage = pd.to_numeric(result.get("baseline_usage_rate", 0.0), errors="coerce").fillna(0.0)
        adjusted_usage = pd.to_numeric(result.get("adjusted_usage_rate", baseline_usage), errors="coerce").fillna(0.0)
        minutes_ratio = (adjusted_minutes / baseline_minutes.clip(lower=1e-6)).clip(lower=0.0, upper=2.0)
        usage_ratio = (adjusted_usage / baseline_usage.clip(lower=1e-6)).clip(lower=0.0, upper=2.0)
        result["adjusted_field_goal_attempts_per_minute"] = _first_numeric_column(
            result,
            ("field_goal_attempts_per_minute_blended", "field_goal_attempts_per_minute_avg_10", "field_goal_attempts_per_minute"),
            fallback=0.0,
        ) * usage_ratio
        result["adjusted_free_throw_attempts_per_minute"] = _first_numeric_column(
            result,
            ("free_throw_attempts_per_minute_blended", "free_throw_attempts_per_minute_avg_10", "free_throw_attempts_per_minute"),
            fallback=0.0,
        ) * usage_ratio
        result["adjusted_assist_creation_proxy_per_minute"] = _first_numeric_column(
            result,
            ("assist_creation_proxy_per_minute_blended", "assist_creation_proxy_per_minute_avg_10", "assist_creation_proxy_per_minute"),
            fallback=0.0,
        ) * usage_ratio
        result["adjusted_rebound_chances_total_per_minute"] = _first_numeric_column(
            result,
            (
                "rebound_chances_total_per_minute_blended",
                "rebound_chances_total_per_minute_avg_10",
                "rebound_chances_total_per_minute",
            ),
            fallback=0.0,
        ) * minutes_ratio
        result["adjusted_estimated_three_point_attempts_per_minute"] = _first_numeric_column(
            result,
            (
                "estimated_three_point_attempts_per_minute_blended",
                "estimated_three_point_attempts_per_minute_avg_10",
                "estimated_three_point_attempts_per_minute",
            ),
            fallback=0.0,
        ) * usage_ratio
        result["adjusted_touches_per_minute"] = _first_numeric_column(
            result,
            ("touches_per_minute_blended", "touches_per_minute_avg_10", "touches_per_minute"),
            fallback=0.0,
        ) * usage_ratio
        result["adjusted_passes_per_minute"] = _first_numeric_column(
            result,
            ("passes_per_minute_blended", "passes_per_minute_avg_10", "passes_per_minute"),
            fallback=0.0,
        ) * usage_ratio
        return result

    def _rotation_metadata(self) -> dict[str, Any]:
        path = Path("data/artifacts/rotation_weights.parquet")
        artifact_hash = ""
        if path.exists():
            artifact_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        return {
            "enabled": _rotation_shock_enabled(),
            "ablation_mode": _rotation_shock_mode(),
            "legacy_pipeline_enabled": _legacy_pipeline_enabled(),
            "version": _ROTATION_SHOCK_VERSION,
            "weights_artifact_hash": artifact_hash,
        }

    def _load_explicit_rotation_absences(self, frame: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "game_id",
            "team_id",
            "player_id",
            "player_name",
            "position",
            "report_timestamp",
            "status",
            "source",
            "rotation_shock_confidence",
            "play_probability",
        ]
        if frame.empty or "game_id" not in frame.columns:
            return pd.DataFrame(columns=columns)
        engine = getattr(self._session, "bind", None)
        if not isinstance(engine, Engine):
            return pd.DataFrame(columns=columns)

        game_ids = sorted(
            {
                int(game_id)
                for game_id in pd.to_numeric(frame["game_id"], errors="coerce").dropna().unique()
            }
        )
        if not game_ids:
            return pd.DataFrame(columns=columns)

        params = {"game_ids": game_ids}
        try:
            injuries = pd.read_sql_query(
                text(
                    """
                    SELECT
                        ir.game_id,
                        ir.team_id,
                        ir.player_id,
                        p.full_name AS player_name,
                        p.position,
                        ir.report_timestamp,
                        ir.status,
                        ir.expected_availability_flag,
                        g.start_time
                    FROM injury_reports ir
                    JOIN games g ON g.game_id = ir.game_id
                    JOIN players p ON p.player_id = ir.player_id
                    WHERE ir.game_id IN :game_ids
                    """
                ).bindparams(bindparam("game_ids", expanding=True)),
                engine,
                params=params,
                parse_dates=["report_timestamp", "start_time"],
            )
            official = pd.read_sql_query(
                text(
                    """
                    SELECT
                        gpa.game_id,
                        COALESCE(t.team_id, p.team_id) AS team_id,
                        gpa.player_id,
                        COALESCE(gpa.player_name, p.full_name) AS player_name,
                        p.position,
                        gpa.fetched_at AS report_timestamp,
                        'inactive' AS status,
                        g.start_time
                    FROM game_player_availability gpa
                    JOIN games g ON g.game_id = gpa.game_id
                    LEFT JOIN players p ON p.player_id = gpa.player_id
                    LEFT JOIN teams t ON t.abbreviation = gpa.team_abbreviation
                    WHERE gpa.game_id IN :game_ids
                      AND gpa.is_active = 0
                      AND gpa.player_id IS NOT NULL
                    """
                ).bindparams(bindparam("game_ids", expanding=True)),
                engine,
                params=params,
                parse_dates=["report_timestamp", "start_time"],
            )
        except Exception as exc:
            _log.debug("Skipping rotation shadow absence query: %s", exc)
            return pd.DataFrame(columns=columns)

        normalized_injuries = _normalize_shadow_injury_absences(injuries)
        normalized_official = _normalize_shadow_official_absences(official)
        combined = pd.concat([normalized_injuries, normalized_official], ignore_index=True, sort=False)
        if combined.empty:
            return pd.DataFrame(columns=columns)
        combined["_source_priority"] = combined["source"].map({"injury_report": 1, "official_inactive": 2}).fillna(0)
        combined = combined.sort_values(
            ["game_id", "team_id", "player_id", "_source_priority", "report_timestamp"],
            kind="mergesort",
        )
        combined = combined.drop_duplicates(subset=["game_id", "team_id", "player_id"], keep="last")
        return combined.drop(columns=["_source_priority"], errors="ignore")[columns].reset_index(drop=True)

    def _load_rotation_weights_table(self) -> RotationWeightTable:
        path = Path("data/artifacts/rotation_weights.parquet")
        if not path.exists():
            return RotationWeightTable()
        frame = pd.read_parquet(path)
        if frame.empty:
            return RotationWeightTable()
        rows: list[RotationWeight] = []
        for row in frame.to_dict("records"):
            rows.append(
                RotationWeight(
                    team_id=row.get("team_id"),
                    season=int(row["season"]) if pd.notna(row.get("season")) else None,
                    absent_archetype=str(row.get("absent_archetype") or "bench_depth"),
                    candidate_archetype=str(row.get("candidate_archetype") or "bench_depth"),
                    minute_gain_weight=float(row.get("minute_gain_weight") or 0.0),
                    usage_gain_weight=float(row.get("usage_gain_weight") or 0.0),
                    minute_delta_mean=float(row.get("minute_delta_mean") or 0.0),
                    usage_delta_mean=float(row.get("usage_delta_mean") or 0.0),
                    minute_delta_variance=float(row["minute_delta_variance"]) if pd.notna(row.get("minute_delta_variance")) else None,
                    usage_delta_variance=float(row["usage_delta_variance"]) if pd.notna(row.get("usage_delta_variance")) else None,
                    sample_size=int(row.get("sample_size") or 0),
                    weight_source=str(row.get("weight_source") or "fallback"),  # type: ignore[arg-type]
                )
            )
        return RotationWeightTable(rows)

    def _annotate_data_sufficiency(self, historical: pd.DataFrame, upcoming: pd.DataFrame) -> pd.DataFrame:
        """Annotate upcoming rows with data sufficiency tiers instead of dropping them.

        Every row is preserved. A ``_data_sufficiency_tier`` column is added
        so downstream consumers can adjust confidence accordingly.
        """
        return annotate_tiers(upcoming=upcoming, historical=historical)

    def _fit_calibrators(
        self,
        frame: pd.DataFrame,
        minutes_feature_columns: list[str],
        stat_feature_columns: list[str],
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
            minutes_model.fit(train_frame, minutes_feature_columns)
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
            train_frame = self._apply_rotation_treatment_mode(train_frame, write_audit=False)
            valid_frame = self._apply_rotation_treatment_mode(valid_frame, write_audit=False)
            train_frame = _apply_feature_defaults(train_frame, stat_feature_columns)
            valid_frame = _apply_feature_defaults(valid_frame, stat_feature_columns)

            stat_models = StatModelSuite(self._settings.training_seed, l1_alpha=self._l1_alpha)
            stat_models.fit(train_frame, stat_feature_columns, l1_alpha=self._l1_alpha)

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
        branch_frames_by_team: dict[tuple[int, int], list[tuple[float, pd.DataFrame]]],
        progress_callback: ProgressCallback | None = None,
        progress_state: tuple[int, int] = (0, 0),
    ) -> list[Any]:
        means, variances = stat_models.models[market_key].predict(subset)
        indexed_rows = list(subset.iterrows())
        completed_offset, total_prediction_rows = progress_state
        if market_key == "pra" and {"points", "rebounds", "assists"}.issubset(stat_models.models):
            tasks: list[tuple[int, tuple[int, dict[str, Any]]]] = [
                (position, (int(frame_index), row.to_dict()))
                for position, (frame_index, row) in enumerate(indexed_rows)
            ]

            def build_combo_simulation(task: tuple[int, tuple[int, dict[str, Any]]]) -> tuple[int, Any]:
                row_index, payload = task
                frame_index, row = payload
                team_key = (_coerce_int(row.get("game_id")), _coerce_int(row.get("player_team_id")))
                branch_frames = (
                    branch_frames_by_team.get((team_key[0], team_key[1]))
                    if team_key[0] is not None and team_key[1] is not None
                    else None
                )
                if not branch_frames:
                    component_inputs = {
                        component_key: {
                            "mean": float(stat_models.models[component_key].predict(subset.iloc[[row_index]])[0][0]),
                            "variance": float(stat_models.models[component_key].predict(subset.iloc[[row_index]])[1][0]),
                            "context": self._simulation_context(row, component_key),
                        }
                        for component_key in ("points", "rebounds", "assists")
                    }
                    return row_index, simulate_joint_combo_probability(
                        line=float(row["line_value"]),
                        minutes_mean=float(row.get("adjusted_projected_minutes", row.get("predicted_minutes", 0.0))),
                        minutes_std=float(max(row.get("predicted_minutes_std", row.get("minutes_volatility", 0.0)), 1.0)),
                        component_inputs=component_inputs,
                        combo_key="pra",
                        seed=self._settings.training_seed + int(row["player_id"]),
                        dist_family=self._dist_family_for_market("pra"),
                    )
                branch_results: list[tuple[float, Any]] = []
                for weight, branch_frame in branch_frames:
                    if frame_index not in branch_frame.index:
                        continue
                    branch_row_frame = branch_frame.loc[[frame_index]]
                    branch_row = branch_row_frame.iloc[0].to_dict()
                    component_inputs = {}
                    for component_key in ("points", "rebounds", "assists"):
                        cm, cv = stat_models.models[component_key].predict(branch_row_frame)
                        component_inputs[component_key] = {
                            "mean": float(cm[0]),
                            "variance": float(cv[0]),
                            "context": self._simulation_context(branch_row, component_key),
                        }
                    branch_results.append(
                        (
                            float(weight),
                            simulate_joint_combo_probability(
                                line=float(branch_row.get("line_value", row["line_value"])),
                                minutes_mean=float(
                                    branch_row.get("adjusted_projected_minutes", branch_row.get("predicted_minutes", 0.0))
                                ),
                                minutes_std=float(
                                    max(branch_row.get("predicted_minutes_std", branch_row.get("minutes_volatility", 0.0)), 1.0)
                                ),
                                component_inputs=component_inputs,
                                combo_key="pra",
                                seed=self._settings.training_seed + int(row["player_id"]),
                                dist_family=self._dist_family_for_market("pra"),
                            ),
                        )
                    )
                return row_index, _mixture_simulation_result(branch_results)

            return self._parallel_task_results(
                tasks=tasks,
                worker=build_combo_simulation,
                progress_callback=progress_callback,
                detail_formatter=lambda task: f"Simulating {task[1][1]['player_name']} PRA ({task[0] + 1}/{len(tasks)})",
                progress_offset=completed_offset,
                progress_total=total_prediction_rows,
            )
        tasks = []
        for position, (frame_index, row) in enumerate(indexed_rows):
            tasks.append(
                (
                    position,
                    (
                        int(frame_index),
                        row.to_dict(),
                        float(means[position]),
                        float(variances[position]),
                        float(row["line_value"]),
                    ),
                )
            )

        def build_single_simulation(
            task: tuple[int, tuple[int, dict[str, Any], float, float, float]]
        ) -> tuple[int, Any]:
            row_index, payload = task
            frame_index, row, mean, variance, line = payload
            team_key = (_coerce_int(row.get("game_id")), _coerce_int(row.get("player_team_id")))
            branch_frames = (
                branch_frames_by_team.get((team_key[0], team_key[1]))
                if team_key[0] is not None and team_key[1] is not None
                else None
            )
            if not branch_frames:
                return row_index, simulate_line_probability(
                    mean,
                    variance,
                    line,
                    minutes_mean=float(row.get("adjusted_projected_minutes", row.get("predicted_minutes", 0.0))),
                    minutes_std=float(max(row.get("predicted_minutes_std", row.get("minutes_volatility", 0.0)), 1.0)),
                    seed=self._settings.training_seed + int(row["player_id"]),
                    market_key=market_key,
                    context=self._simulation_context(row, market_key),
                    dist_family=self._dist_family_for_market(market_key),
                )
            branch_results: list[tuple[float, Any]] = []
            for weight, branch_frame in branch_frames:
                if frame_index not in branch_frame.index:
                    continue
                branch_row_frame = branch_frame.loc[[frame_index]]
                branch_row = branch_row_frame.iloc[0].to_dict()
                branch_mean, branch_variance = stat_models.models[market_key].predict(branch_row_frame)
                branch_results.append(
                    (
                        float(weight),
                        simulate_line_probability(
                            float(branch_mean[0]),
                            float(branch_variance[0]),
                            float(branch_row.get("line_value", line)),
                            minutes_mean=float(
                                branch_row.get("adjusted_projected_minutes", branch_row.get("predicted_minutes", 0.0))
                            ),
                            minutes_std=float(
                                max(branch_row.get("predicted_minutes_std", branch_row.get("minutes_volatility", 0.0)), 1.0)
                            ),
                            seed=self._settings.training_seed + int(row["player_id"]),
                            market_key=market_key,
                            context=self._simulation_context(branch_row, market_key),
                            dist_family=self._dist_family_for_market(market_key),
                        ),
                    )
                )
            return row_index, _mixture_simulation_result(branch_results)

        return self._parallel_task_results(
            tasks=tasks,
            worker=build_single_simulation,
            progress_callback=progress_callback,
            detail_formatter=lambda task: f"Simulating {task[1][1]['player_name']} {market_key.upper()} ({task[0] + 1}/{len(tasks)})",
            progress_offset=completed_offset,
            progress_total=total_prediction_rows,
        )

    def _build_branch_simulation_frames(
        self,
        *,
        frame: pd.DataFrame,
        absence_profiles: pd.DataFrame,
        max_exact_players: int,
        sampled_branch_count: int,
    ) -> dict[tuple[int, int], list[tuple[float, pd.DataFrame]]]:
        if frame.empty or _rotation_shock_mode() != "full":
            return {}
        working = _ensure_player_team_id_column(frame.copy())
        if "player_team_id" not in working.columns:
            return {}
        weights_table = self._load_rotation_weights_table()
        result: dict[tuple[int, int], list[tuple[float, pd.DataFrame]]] = {}
        for (game_id, team_id), group in working.groupby(["game_id", "player_team_id"], dropna=False):
            game_id_int = _coerce_int(game_id)
            team_id_int = _coerce_int(team_id)
            if game_id_int is None or team_id_int is None:
                continue
            group_absences = _matching_absence_profiles(absence_profiles, game_id=game_id_int, team_id=team_id_int)
            profiles, play_probabilities = _build_rotation_profiles(group, absent_rows=group_absences)
            branch_result = enumerate_or_sample_branches(
                play_probabilities,
                max_exact_players=max_exact_players,
                n_samples=max(int(sampled_branch_count), 1),
                seed=42 + game_id_int + team_id_int,
            )
            if not branch_result.branches:
                continue
            branch_frames: list[tuple[float, pd.DataFrame]] = []
            for branch in branch_result.branches:
                realized_probabilities = {
                    player_id: float(branch.active_by_player.get(player_id, 1))
                    for player_id in play_probabilities
                }
                redistribution = redistribute(
                    game_id=game_id_int,
                    team_id=team_id_int,
                    players=profiles,
                    weights=weights_table,
                    play_probabilities=realized_probabilities,
                    mode="realized",
                )
                branch_group = group.copy()
                branch_group["team_efficiency_delta"] = float(redistribution.team_efficiency_delta)
                branch_group["pace_delta"] = float(redistribution.pace_delta)
                branch_group["rotation_shock_magnitude"] = float(redistribution.rotation_shock_magnitude)
                branch_group["rotation_shock_confidence"] = float(redistribution.rotation_shock_confidence)
                adjusted_by_player = {player.player_id: player for player in redistribution.adjusted_players}
                for row_index, row in branch_group.iterrows():
                    adjusted = adjusted_by_player.get(int(row["player_id"]))
                    if adjusted is None:
                        continue
                    branch_group.at[row_index, "adjusted_projected_minutes"] = float(adjusted.adjusted_minutes)
                    branch_group.at[row_index, "adjusted_usage_share"] = float(adjusted.adjusted_usage_share)
                    branch_group.at[row_index, "adjusted_usage_rate"] = float(adjusted.adjusted_usage_share)
                branch_group["predicted_minutes"] = branch_group["adjusted_projected_minutes"]
                branch_group = self._features.apply_post_minutes_features(branch_group, minutes_column="predicted_minutes")
                branch_group = self._attach_adjusted_rate_columns(branch_group)
                branch_frames.append((float(branch.probability), branch_group))
            result[(game_id_int, team_id_int)] = branch_frames
        return result

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
            minutes_mean = _row_optional_float(
                row,
                ("adjusted_projected_minutes", "predicted_minutes", "minutes_avg_10"),
            )
            minutes_std = _row_optional_float(row, ("predicted_minutes_std", "minutes_volatility"))
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
                ("adjusted_usage_rate", "usage_rate_blended", "usage_rate_avg_10", "usage_rate"),
                fallback=0.20,
                minimum=0.01,
            ),
            "touches_per_minute": _row_float(
                row,
                ("adjusted_touches_per_minute", "touches_per_minute_blended", "touches_per_minute_avg_10", "touches_per_minute"),
                fallback=0.0,
                minimum=0.0,
            ),
            "passes_per_minute": _row_float(
                row,
                ("adjusted_passes_per_minute", "passes_per_minute_blended", "passes_per_minute_avg_10", "passes_per_minute"),
                fallback=0.0,
                minimum=0.0,
            ),
            "assist_creation_proxy_per_minute": _row_float(
                row,
                (
                    "adjusted_assist_creation_proxy_per_minute",
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
                    "adjusted_rebound_chances_total_per_minute",
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
                (
                    "adjusted_field_goal_attempts_per_minute",
                    "field_goal_attempts_per_minute_blended",
                    "field_goal_attempts_per_minute_avg_10",
                    "field_goal_attempts_per_minute",
                ),
                fallback=0.0,
                minimum=0.0,
            ),
            "free_throw_attempts_per_minute": _row_float(
                row,
                (
                    "adjusted_free_throw_attempts_per_minute",
                    "free_throw_attempts_per_minute_blended",
                    "free_throw_attempts_per_minute_avg_10",
                    "free_throw_attempts_per_minute",
                ),
                fallback=0.02,
                minimum=0.0,
            ),
            "estimated_three_point_attempts_per_minute": _row_float(
                row,
                (
                    "adjusted_estimated_three_point_attempts_per_minute",
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


def _stat_feature_columns(
    feature_engineer: FeatureEngineer,
    frame: pd.DataFrame,
    base_feature_columns: list[str],
) -> list[str]:
    baseline_output_columns = [
        column
        for column in frame.columns
        if column.endswith("_baseline_mean") or column.endswith("_baseline_variance")
    ]
    return sorted(set(base_feature_columns) | set(feature_engineer._feature_columns(frame)) | set(baseline_output_columns))


def _with_neutral_rotation_environment(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["team_efficiency_delta"] = 0.0
    result["pace_delta"] = 0.0
    result["rotation_shock_magnitude"] = 0.0
    result["rotation_shock_confidence"] = 1.0
    return result


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
        if key.startswith("adjusted_") and numeric >= 0.0:
            return numeric
        if numeric > minimum:
            return numeric
    return float(fallback)


def _row_optional_float(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = row.get(key)
        try:
            numeric = float(value)
        except Exception:
            continue
        if np.isfinite(numeric):
            return numeric
    return None


def _apply_dnp_correction(
    summary: DistributionSummary,
    dnp_risk: float,
    line: float,
) -> tuple[float, float, float, float]:
    """Return (boom_prob, bust_prob, p25, p75) with DNP mixture correction.

    The simulator runs on expected-value adjusted features so summary.mean is
    already the branch-weighted mean. Percentiles and tail probs treat the player
    as always playing. This function applies the availability-mixture correction:

        F_combined(x) = d * I(x >= 0) + (1-d) * F_play(x)

    where d = dnp_risk and F_play is approximated from the simulation summary.
    """
    d = float(np.clip(dnp_risk, 0.0, 1.0))
    if d <= 1e-6:
        return summary.boom_probability, summary.bust_probability, summary.p25, summary.p75

    p_play = 1.0 - d
    # Conditional-on-playing stats from the expected-value-adjusted summary.
    # adjusted_minutes ≈ baseline_minutes * p_play, so:
    #   conditional_mean ≈ summary.mean / p_play
    #   conditional_std  ≈ sqrt(summary.variance / p_play)
    cond_mean = summary.mean / max(p_play, 1e-9)
    cond_std = float(np.sqrt(max(summary.variance, 1e-9) / max(p_play, 1e-9)))

    from scipy.stats import norm  # local to avoid top-level cost when scipy absent

    # Boom: P(stat >= 1.10*line) = p_play * P(stat >= 1.10*line | plays)
    boom_threshold = line * 1.10 if line > 0 else float("inf")
    boom = float(p_play * norm.sf(boom_threshold, loc=cond_mean, scale=cond_std)) if np.isfinite(boom_threshold) else 0.0

    # Bust: P(stat <= 0.70*line) = d + p_play * P(stat <= 0.70*line | plays)
    bust_threshold = max(line * 0.70, 0.0)
    bust = float(np.clip(d + p_play * norm.cdf(bust_threshold, loc=cond_mean, scale=cond_std), 0.0, 1.0))

    # Combined quantile: F_combined^-1(q).  If q <= d the mass at 0 covers it.
    def _cq(q: float) -> float:
        if q <= d:
            return 0.0
        q_play = (q - d) / p_play
        return float(max(norm.ppf(min(q_play, 1.0 - 1e-9), loc=cond_mean, scale=cond_std), 0.0))

    return boom, bust, _cq(0.25), _cq(0.75)


def _mixture_simulation_result(branch_results: list[tuple[float, Any]]) -> Any:
    if not branch_results:
        return simulate_line_probability(
            0.0,
            1.0,
            0.0,
            minutes_mean=0.0,
            minutes_std=1.0,
        )
    total_weight = float(sum(max(weight, 0.0) for weight, _ in branch_results))
    normalized = [
        (max(weight, 0.0) / total_weight if total_weight > 0 else 1.0 / len(branch_results), simulation)
        for weight, simulation in branch_results
    ]
    mean = float(sum(weight * simulation.summary.mean for weight, simulation in normalized))
    second_moment = float(
        sum(weight * (simulation.summary.variance + (simulation.summary.mean**2)) for weight, simulation in normalized)
    )
    variance = max(second_moment - (mean**2), 1e-9)
    median = float(sum(weight * simulation.summary.median for weight, simulation in normalized))
    p10 = float(sum(weight * simulation.summary.p10 for weight, simulation in normalized))
    p25 = float(sum(weight * simulation.summary.p25 for weight, simulation in normalized))
    p75 = float(sum(weight * simulation.summary.p75 for weight, simulation in normalized))
    p90 = float(sum(weight * simulation.summary.p90 for weight, simulation in normalized))
    over_probability = float(sum(weight * simulation.summary.over_probability for weight, simulation in normalized))
    under_probability = float(sum(weight * simulation.summary.under_probability for weight, simulation in normalized))
    ci_low = float(sum(weight * simulation.summary.ci_low for weight, simulation in normalized))
    ci_high = float(sum(weight * simulation.summary.ci_high for weight, simulation in normalized))
    boom_probability = float(sum(weight * simulation.summary.boom_probability for weight, simulation in normalized))
    bust_probability = float(sum(weight * simulation.summary.bust_probability for weight, simulation in normalized))
    samples_used = int(sum(weight * simulation.samples_used for weight, simulation in normalized))
    margin_of_error = float(sum(weight * simulation.margin_of_error for weight, simulation in normalized))
    from app.training.distributions import SimulationResult

    return SimulationResult(
        summary=DistributionSummary(
            mean=mean,
            variance=variance,
            median=median,
            p10=p10,
            p90=p90,
            over_probability=over_probability,
            under_probability=under_probability,
            ci_low=ci_low,
            ci_high=ci_high,
            p25=p25,
            p75=p75,
            boom_probability=boom_probability,
            bust_probability=bust_probability,
        ),
        samples_used=max(samples_used, 1),
        margin_of_error=max(margin_of_error, 0.0),
    )


def _availability_branch_context(
    frame: pd.DataFrame,
    absence_profiles: pd.DataFrame,
    *,
    max_exact_players: int,
    sampled_branch_count: int,
) -> dict[tuple[int, int, int], dict[str, float | int]]:
    if frame.empty:
        return {}
    frame = _ensure_player_team_id_column(frame.copy())
    if "player_team_id" not in frame.columns:
        return {}
    profile_map: dict[tuple[int, int, int], float] = {}
    if not absence_profiles.empty:
        for row in absence_profiles.to_dict("records"):
            game_id = _coerce_int(row.get("game_id"))
            team_id = _coerce_int(row.get("team_id"))
            player_id = _coerce_int(row.get("player_id"))
            if game_id is None or team_id is None or player_id is None:
                continue
            play_probability = float(np.clip(_row_numeric_value(row, ("play_probability",), fallback=1.0), 0.0, 1.0))
            profile_map[(game_id, team_id, player_id)] = play_probability

    result: dict[tuple[int, int, int], dict[str, float | int]] = {}
    grouped = frame.groupby(["game_id", "player_team_id"], dropna=False)
    for (game_id, team_id), group in grouped:
        game_id_int = _coerce_int(game_id)
        team_id_int = _coerce_int(team_id)
        if game_id_int is None or team_id_int is None:
            continue
        player_probabilities: dict[int, float] = {}
        for row in group.to_dict("records"):
            player_id = _coerce_int(row.get("player_id"))
            if player_id is None:
                continue
            play_probability = profile_map.get((game_id_int, team_id_int, player_id), 1.0)
            player_probabilities[player_id] = play_probability
        if not player_probabilities:
            continue
        branch_result = enumerate_or_sample_branches(
            player_probabilities,
            max_exact_players=max_exact_players,
            n_samples=max(int(sampled_branch_count), 1),
            seed=42 + game_id_int + team_id_int,
        )
        branch_count = len(branch_result.branches)
        for player_id, play_probability in player_probabilities.items():
            result[(game_id_int, team_id_int, player_id)] = {
                "availability_branches": int(max(branch_count, 1)),
                "dnp_risk": float(
                    dnp_risk_from_branches(branch_result.branches, player_id=player_id)
                    if branch_result.branches
                    else np.clip(1.0 - play_probability, 0.0, 1.0)
                ),
            }
    return result


def _lookup_availability_context(
    availability_context: dict[tuple[int, int, int], dict[str, float | int]],
    row: dict[str, Any],
) -> dict[str, float | int]:
    game_id = _coerce_int(row.get("game_id"))
    player_id = _coerce_int(row.get("player_id"))
    team_id = _coerce_int(row.get("player_team_id", row.get("team_id")))
    if game_id is None or player_id is None or team_id is None:
        return {"availability_branches": 1, "dnp_risk": 0.0}
    return availability_context.get((game_id, team_id, player_id), {"availability_branches": 1, "dnp_risk": 0.0})


def _coerce_int(value: Any) -> int | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    return numeric


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


_TRUE_ENV_VALUES = {"1", "true", "yes", "on"}
_FALSE_ENV_VALUES = {"0", "false", "no", "off"}
_FLAG_SETTING_ATTRS = {
    "ROTATION_SHOCK_ENABLED": "rotation_shock_enabled",
    "ROTATION_SHOCK_SHADOW_MODE": "rotation_shock_shadow_mode",
    "LEGACY_PIPELINE_ENABLED": "legacy_pipeline_enabled",
}


def _settings_flag_default(name: str, default: bool) -> bool:
    attr = _FLAG_SETTING_ATTRS.get(name)
    if attr is None:
        return default
    try:
        return bool(getattr(get_settings(), attr))
    except Exception:
        return default


def _env_flag_enabled(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return _settings_flag_default(name, default)
    normalized = raw.strip().lower()
    if not normalized:
        return default
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False
    return default


def _rotation_shock_enabled() -> bool:
    return _env_flag_enabled("ROTATION_SHOCK_ENABLED", default=True)


def _legacy_pipeline_enabled() -> bool:
    return _env_flag_enabled("LEGACY_PIPELINE_ENABLED", default=True)


def _rotation_shock_mode() -> str:
    if not _rotation_shock_enabled():
        return "off"
    mode = os.getenv("ROTATION_SHOCK_ABLATION_MODE")
    if mode is None:
        try:
            mode = str(get_settings().rotation_shock_ablation_mode)
        except Exception:
            mode = "full"
    mode = str(mode or "full").strip().lower()
    if mode not in {"off", "features-only", "full"}:
        return "full"
    return mode


def _build_rotation_profiles(
    group: pd.DataFrame,
    *,
    absent_rows: pd.DataFrame | None = None,
) -> tuple[list[PlayerRotationProfile], dict[int, float]]:
    profiles: list[PlayerRotationProfile] = []
    play_probabilities: dict[int, float] = {}
    season_value = _nba_season_value(group["game_date"].iloc[0])
    availability_by_player: dict[int, dict[str, Any]] = {}
    if absent_rows is not None and not absent_rows.empty:
        availability_by_player = {
            int(row["player_id"]): row
            for row in absent_rows.to_dict("records")
            if pd.notna(row.get("player_id"))
        }
    for row in group.to_dict("records"):
        player_id = int(row["player_id"])
        availability = availability_by_player.get(player_id)
        is_inactive = bool(float(row.get("player_on_inactive_list", 0.0) or 0.0) > 0.5)
        status = "inactive" if is_inactive else "available"
        play_probability = status_to_play_probability(status, official_inactive=is_inactive)
        profile_source = "official_inactive" if is_inactive else "injury_report"
        report_timestamp = None
        rotation_confidence = 1.0
        if availability is not None:
            status = str(availability.get("status", status))
            profile_source = str(availability.get("source") or profile_source)
            play_probability = _row_numeric_value(
                availability,
                ("play_probability",),
                fallback=status_to_play_probability(status, official_inactive=profile_source == "official_inactive"),
            )
            report_timestamp = (
                pd.Timestamp(availability.get("report_timestamp"))
                if pd.notna(availability.get("report_timestamp"))
                else None
            )
            rotation_confidence = _row_numeric_value(availability, ("rotation_shock_confidence",), fallback=1.0)
        usage_share = _row_numeric_value(row, ("baseline_usage_share", "usage_rate_blended", "usage_rate_avg_10", "usage_rate"))
        assist_share = _row_numeric_value(row, ("assists_per_minute_blended", "assists_per_minute_avg_10", "assists_per_minute"))
        rebound_share = _row_numeric_value(row, ("rebounds_per_minute_blended", "rebounds_per_minute_avg_10", "rebounds_per_minute"))
        three_rate = _row_numeric_value(
            row,
            (
                "estimated_three_point_attempts_per_minute_blended",
                "estimated_three_point_attempts_per_minute_avg_10",
                "estimated_three_point_attempts_per_minute",
            ),
        )
        archetype = classify_archetype(
            position_group=str(row.get("position", "")),
            usage_share=usage_share,
            assist_share=assist_share,
            rebound_share=rebound_share,
            three_point_rate=three_rate,
            starter_score=float(row.get("starter_flag", 0.0) or 0.0),
        )
        role_vector = RoleVector(
            player_id=player_id,
            season=season_value,
            position_group=str(row.get("position", ""))[:1] or "UNK",
            usage_proxy=float(row.get("usage_proxy", 0.0) or 0.0),
            usage_share=usage_share,
            assist_share=assist_share,
            rebound_share=rebound_share,
            three_point_rate=three_rate,
            rim_attempt_rate=float(row.get("estimated_two_point_attempts_per_minute", 0.0) or 0.0),
            touches_per_minute=_row_numeric_value(row, ("touches_per_minute_blended", "touches_per_minute_avg_10", "touches_per_minute")),
            passes_per_minute=_row_numeric_value(row, ("passes_per_minute_blended", "passes_per_minute_avg_10", "passes_per_minute")),
            rebound_chances_per_minute=_row_numeric_value(
                row,
                (
                    "rebound_chances_total_per_minute_blended",
                    "rebound_chances_total_per_minute_avg_10",
                    "rebound_chances_total_per_minute",
                ),
            ),
            blocks_per_minute=_row_numeric_value(row, ("blocks_per_minute_blended", "blocks_per_minute_avg_10", "blocks_per_minute")),
            starter_score=_row_numeric_value(row, ("starter_flag",), fallback=0.0),
            role_stability=_row_numeric_value(row, ("role_stability",), fallback=0.5),
            archetype_label=archetype,
        )
        profiles.append(
            PlayerRotationProfile(
                game_id=int(row["game_id"]),
                team_id=int(row.get("player_team_id", row.get("team_id"))),
                player_id=player_id,
                player_name=str(row.get("player_name", "")),
                status=status,
                position_group=str(row.get("position", ""))[:1] or "UNK",
                baseline_minutes=_row_numeric_value(row, ("baseline_projected_minutes", "predicted_minutes")),
                baseline_usage_share=usage_share,
                baseline_assist_share=assist_share,
                baseline_rebound_share=rebound_share,
                baseline_three_point_share=three_rate,
                role_vector=role_vector,
                availability_source=cast(Any, profile_source),
                report_timestamp=None if report_timestamp is None else report_timestamp.to_pydatetime(),
                rotation_shock_confidence=rotation_confidence,
            )
        )
        play_probabilities[player_id] = max(0.0, min(1.0, play_probability))
    if absent_rows is not None and not absent_rows.empty:
        active_player_ids = {profile.player_id for profile in profiles}
        for row in absent_rows.to_dict("records"):
            player_id = int(row["player_id"])
            if player_id in active_player_ids:
                continue
            play_probability = _row_numeric_value(
                row,
                ("play_probability",),
                fallback=status_to_play_probability(str(row.get("status", "out"))),
            )
            usage_share = _row_numeric_value(row, ("baseline_usage_share", "baseline_usage_rate", "usage_rate"), fallback=0.0)
            assist_share = _row_numeric_value(row, ("baseline_assist_share", "assists_per_minute"), fallback=0.0)
            rebound_share = _row_numeric_value(row, ("baseline_rebound_share", "rebounds_per_minute"), fallback=0.0)
            three_rate = _row_numeric_value(
                row,
                ("baseline_three_point_share", "estimated_three_point_attempts_per_minute"),
                fallback=0.0,
            )
            starter_score = _row_numeric_value(row, ("starter_flag",), fallback=0.0)
            archetype = str(row.get("archetype_label") or row.get("absent_archetype") or "").strip()
            if not archetype:
                archetype = classify_archetype(
                    position_group=str(row.get("position", "")),
                    usage_share=usage_share,
                    assist_share=assist_share,
                    rebound_share=rebound_share,
                    three_point_rate=three_rate,
                    starter_score=starter_score,
                )
            role_vector = RoleVector(
                player_id=player_id,
                season=int(row.get("season", season_value) or season_value),
                position_group=str(row.get("position", ""))[:1] or "UNK",
                usage_proxy=_row_numeric_value(row, ("baseline_usage_proxy", "usage_proxy"), fallback=0.0),
                usage_share=usage_share,
                assist_share=assist_share,
                rebound_share=rebound_share,
                three_point_rate=three_rate,
                rim_attempt_rate=_row_numeric_value(row, ("rim_attempt_rate",), fallback=0.0),
                touches_per_minute=_row_numeric_value(row, ("touches_per_minute",), fallback=0.0),
                passes_per_minute=_row_numeric_value(row, ("passes_per_minute",), fallback=0.0),
                rebound_chances_per_minute=_row_numeric_value(row, ("rebound_chances_total_per_minute",), fallback=0.0),
                blocks_per_minute=_row_numeric_value(row, ("blocks_per_minute",), fallback=0.0),
                starter_score=starter_score,
                role_stability=_row_numeric_value(row, ("role_stability",), fallback=0.5),
                archetype_label=archetype,
            )
            source = str(row.get("source") or "injury_report")
            report_timestamp = pd.Timestamp(row.get("report_timestamp")) if pd.notna(row.get("report_timestamp")) else None
            profiles.append(
                PlayerRotationProfile(
                    game_id=int(row["game_id"]),
                    team_id=int(row["team_id"]),
                    player_id=player_id,
                    player_name=str(row.get("player_name", "")),
                    status=str(row.get("status", "out")),
                    position_group=str(row.get("position", ""))[:1] or "UNK",
                    baseline_minutes=_row_numeric_value(row, ("baseline_minutes",), fallback=0.0),
                    baseline_usage_share=usage_share,
                    baseline_assist_share=assist_share,
                    baseline_rebound_share=rebound_share,
                    baseline_three_point_share=three_rate,
                    role_vector=role_vector,
                    availability_source=cast(Any, source),
                    report_timestamp=None if report_timestamp is None else report_timestamp.to_pydatetime(),
                    rotation_shock_confidence=_row_numeric_value(row, ("rotation_shock_confidence",), fallback=1.0),
                )
            )
            play_probabilities[player_id] = max(0.0, min(1.0, play_probability))
    return profiles, play_probabilities


def _first_numeric_column(frame: pd.DataFrame, columns: tuple[str, ...], *, fallback: float) -> pd.Series:
    result = pd.Series(float(fallback), index=frame.index, dtype=float)
    for column in reversed(columns):
        if column in frame.columns:
            numeric = pd.to_numeric(frame[column], errors="coerce")
            result = numeric.where(numeric.notna(), result)
    return result.fillna(float(fallback))


def _matching_absence_profiles(absence_profiles: pd.DataFrame, *, game_id: int, team_id: int) -> pd.DataFrame:
    if absence_profiles.empty:
        return absence_profiles
    return absence_profiles[
        (pd.to_numeric(absence_profiles["game_id"], errors="coerce") == int(game_id))
        & (pd.to_numeric(absence_profiles["team_id"], errors="coerce") == int(team_id))
    ].copy()


def _normalize_shadow_injury_absences(injuries: pd.DataFrame) -> pd.DataFrame:
    columns = _shadow_absence_columns()
    if injuries.empty:
        return pd.DataFrame(columns=columns)
    frame = injuries.copy()
    frame["report_timestamp"] = pd.to_datetime(frame["report_timestamp"], errors="coerce")
    frame["start_time"] = pd.to_datetime(frame.get("start_time"), errors="coerce")
    cutoff = frame["start_time"] - pd.to_timedelta(90, unit="m")
    frame = frame[frame["report_timestamp"].notna() & cutoff.notna() & (frame["report_timestamp"] <= cutoff)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    expected_flags = frame["expected_availability_flag"] if "expected_availability_flag" in frame.columns else pd.Series(
        None,
        index=frame.index,
    )
    frame["play_probability"] = [
        status_to_play_probability(
            status,
            expected_availability_flag=None if pd.isna(expected_flag) else bool(expected_flag),
        )
        for status, expected_flag in zip(frame["status"], expected_flags, strict=False)
    ]
    frame = frame[pd.to_numeric(frame["play_probability"], errors="coerce").fillna(1.0) < 1.0 - 1e-6].copy()
    return _normalize_shadow_absences(frame, source="injury_report", confidence=1.0)


def _normalize_shadow_official_absences(official: pd.DataFrame) -> pd.DataFrame:
    if official.empty:
        return pd.DataFrame(columns=_shadow_absence_columns())
    frame = official.copy()
    frame["report_timestamp"] = pd.to_datetime(frame.get("report_timestamp"), errors="coerce")
    frame["start_time"] = pd.to_datetime(frame.get("start_time"), errors="coerce")
    frame["play_probability"] = 0.0
    cutoff = frame["start_time"] - pd.to_timedelta(90, unit="m")
    pregame = frame[frame["report_timestamp"].notna() & cutoff.notna() & (frame["report_timestamp"] <= cutoff)].copy()
    post_hoc = frame.drop(index=pregame.index).copy()
    return pd.concat(
        [
            _normalize_shadow_absences(pregame, source="official_inactive", confidence=1.0),
            _normalize_shadow_absences(post_hoc, source="post_hoc", confidence=0.5),
        ],
        ignore_index=True,
        sort=False,
    )


def _normalize_shadow_absences(frame: pd.DataFrame, *, source: str, confidence: float) -> pd.DataFrame:
    columns = _shadow_absence_columns()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    result = frame.copy()
    for column in ("game_id", "team_id", "player_id"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(subset=["game_id", "team_id", "player_id"]).copy()
    if result.empty:
        return pd.DataFrame(columns=columns)
    result["game_id"] = result["game_id"].astype(int)
    result["team_id"] = result["team_id"].astype(int)
    result["player_id"] = result["player_id"].astype(int)
    result["player_name"] = result.get("player_name", pd.Series("", index=result.index)).fillna("").astype(str)
    result["position"] = result.get("position", pd.Series("", index=result.index)).fillna("").astype(str)
    result["report_timestamp"] = pd.to_datetime(result.get("report_timestamp"), errors="coerce")
    result["status"] = result.get("status", pd.Series("out", index=result.index)).fillna("out").astype(str)
    result["source"] = source
    result["rotation_shock_confidence"] = float(confidence)
    result["play_probability"] = pd.to_numeric(result.get("play_probability", 0.0), errors="coerce").fillna(0.0)
    return result[columns].reset_index(drop=True)


def _build_shadow_absence_profiles(
    frame: pd.DataFrame,
    absences: pd.DataFrame,
    historical_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    output_columns = [
        * _shadow_absence_columns(),
        "season",
        "baseline_minutes",
        "baseline_usage_share",
        "baseline_usage_rate",
        "baseline_usage_proxy",
        "baseline_assist_share",
        "baseline_rebound_share",
        "baseline_three_point_share",
        "starter_flag",
        "archetype_label",
    ]
    if frame.empty or absences.empty:
        return pd.DataFrame(columns=output_columns)

    # Combine eval frame with historical context so absent players who have
    # no box scores in the eval window can still get baselines from training.
    if historical_frame is not None and not historical_frame.empty:
        history = pd.concat([historical_frame, frame], ignore_index=True).drop_duplicates(
            subset=["game_id", "player_id"], keep="last"
        )
    else:
        history = frame.copy()
    if "player_team_id" in history.columns:
        history["_rotation_team_id"] = pd.to_numeric(history["player_team_id"], errors="coerce")
    elif "team_id" in history.columns:
        history["_rotation_team_id"] = pd.to_numeric(history["team_id"], errors="coerce")
    else:
        return pd.DataFrame(columns=output_columns)
    history["_rotation_game_date"] = pd.to_datetime(history.get("game_date"), errors="coerce")
    history["_rotation_sort_time"] = pd.to_datetime(history.get("start_time", history["_rotation_game_date"]), errors="coerce")
    history["_rotation_sort_time"] = history["_rotation_sort_time"].fillna(history["_rotation_game_date"])
    history["_rotation_season"] = _nba_season_series(history["_rotation_game_date"])
    for column in ("player_id", "game_id"):
        history[column] = pd.to_numeric(history[column], errors="coerce")
    history = history.dropna(
        subset=["player_id", "game_id", "_rotation_team_id", "_rotation_season", "_rotation_sort_time"],
    ).copy()
    if history.empty:
        return pd.DataFrame(columns=output_columns)
    history["player_id"] = history["player_id"].astype(int)
    history["game_id"] = history["game_id"].astype(int)
    history["_rotation_team_id"] = history["_rotation_team_id"].astype(int)
    history["_rotation_season"] = history["_rotation_season"].astype(int)

    game_lookup = history[
        ["game_id", "_rotation_team_id", "_rotation_game_date", "_rotation_sort_time", "_rotation_season"]
    ].drop_duplicates()
    game_lookup = game_lookup.rename(columns={"_rotation_team_id": "team_id"})
    explicit = absences.merge(game_lookup, on=["game_id", "team_id"], how="inner")
    if explicit.empty:
        return pd.DataFrame(columns=output_columns)

    active_current = {
        (int(row["game_id"]), int(row["_rotation_team_id"]), int(row["player_id"])): row
        for row in history.to_dict("records")
    }
    rows: list[dict[str, Any]] = []
    histories = {
        (int(player_id), int(team_id), int(season)): group.sort_values("_rotation_sort_time", kind="mergesort")
        for (player_id, team_id, season), group in history.groupby(
            ["player_id", "_rotation_team_id", "_rotation_season"],
            dropna=False,
        )
    }
    for row in explicit.to_dict("records"):
        key = (int(row["player_id"]), int(row["team_id"]), int(row["_rotation_season"]))
        active_row = active_current.get((int(row["game_id"]), int(row["team_id"]), int(row["player_id"])))
        if active_row is not None:
            usage_share = _row_numeric_value(
                active_row,
                ("baseline_usage_share", "usage_rate_blended", "usage_rate_avg_10", "usage_rate"),
            )
            assist_share = _row_numeric_value(
                active_row,
                ("assists_per_minute_blended", "assists_per_minute_avg_10", "assists_per_minute"),
            )
            rebound_share = _row_numeric_value(
                active_row,
                ("rebounds_per_minute_blended", "rebounds_per_minute_avg_10", "rebounds_per_minute"),
            )
            three_rate = _row_numeric_value(
                active_row,
                (
                    "estimated_three_point_attempts_per_minute_blended",
                    "estimated_three_point_attempts_per_minute_avg_10",
                    "estimated_three_point_attempts_per_minute",
                ),
            )
            starter_score = _row_numeric_value(active_row, ("starter_flag",), fallback=0.0)
            rows.append(
                {
                    **{column: row.get(column) for column in _shadow_absence_columns()},
                    "season": int(row["_rotation_season"]),
                    "baseline_minutes": _row_numeric_value(
                        active_row,
                        ("baseline_projected_minutes", "predicted_minutes", "minutes"),
                    ),
                    "baseline_usage_share": usage_share,
                    "baseline_usage_rate": usage_share,
                    "baseline_usage_proxy": _row_numeric_value(active_row, ("usage_proxy",), fallback=0.0),
                    "baseline_assist_share": assist_share,
                    "baseline_rebound_share": rebound_share,
                    "baseline_three_point_share": three_rate,
                    "starter_flag": starter_score,
                    "archetype_label": classify_archetype(
                        position_group=str(row.get("position", active_row.get("position", ""))),
                        usage_share=usage_share,
                        assist_share=assist_share,
                        rebound_share=rebound_share,
                        three_point_rate=three_rate,
                        starter_score=starter_score,
                    ),
                }
            )
            continue
        player_history = histories.get(key)
        if player_history is None:
            continue
        prior = player_history[player_history["_rotation_sort_time"] < row["_rotation_sort_time"]].tail(15)
        if prior.empty:
            continue
        baseline_minutes = float(pd.to_numeric(prior.get("minutes"), errors="coerce").fillna(0.0).mean())
        if baseline_minutes < _SHADOW_BASELINE_MINUTES_THRESHOLD:
            continue
        usage_share = float(_first_numeric_column(prior, ("usage_rate_blended", "usage_rate_avg_10", "usage_rate"), fallback=0.0).mean())
        assist_share = _per_minute_average(prior, total_column="assists", rate_columns=("assists_per_minute_avg_10", "assists_per_minute"))
        rebound_share = _per_minute_average(prior, total_column="rebounds", rate_columns=("rebounds_per_minute_avg_10", "rebounds_per_minute"))
        three_rate = _per_minute_average(
            prior,
            total_column="threes",
            rate_columns=("estimated_three_point_attempts_per_minute_avg_10", "estimated_three_point_attempts_per_minute"),
        )
        starter_score = float(pd.to_numeric(prior.get("starter_flag"), errors="coerce").fillna(0.0).mean())
        archetype = classify_archetype(
            position_group=str(row.get("position", "")),
            usage_share=usage_share,
            assist_share=assist_share,
            rebound_share=rebound_share,
            three_point_rate=three_rate,
            starter_score=starter_score,
        )
        usage_proxy = float(_first_numeric_column(prior, ("usage_proxy",), fallback=0.0).mean())
        if usage_proxy <= 0.0 and {"field_goal_attempts", "free_throw_attempts"}.issubset(prior.columns):
            fga = pd.to_numeric(prior["field_goal_attempts"], errors="coerce").fillna(0.0)
            fta = pd.to_numeric(prior["free_throw_attempts"], errors="coerce").fillna(0.0)
            usage_proxy = float((fga + 0.44 * fta).mean())
        rows.append(
            {
                **{column: row.get(column) for column in _shadow_absence_columns()},
                "season": int(row["_rotation_season"]),
                "baseline_minutes": baseline_minutes,
                "baseline_usage_share": usage_share,
                "baseline_usage_rate": usage_share,
                "baseline_usage_proxy": usage_proxy,
                "baseline_assist_share": assist_share,
                "baseline_rebound_share": rebound_share,
                "baseline_three_point_share": three_rate,
                "starter_flag": starter_score,
                "archetype_label": archetype,
            }
        )
    return pd.DataFrame(rows, columns=output_columns)


def _shadow_absence_columns() -> list[str]:
    return [
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "position",
        "report_timestamp",
        "status",
        "source",
        "rotation_shock_confidence",
        "play_probability",
    ]


def _nba_season_series(game_date: pd.Series) -> pd.Series:
    dates = pd.to_datetime(game_date, errors="coerce")
    return (dates.dt.year - (dates.dt.month < 7).astype(int)).astype("Int64")


def _nba_season_value(game_date: Any) -> int:
    timestamp = pd.Timestamp(game_date)
    if pd.isna(timestamp):
        return datetime.now(UTC).year
    return int(timestamp.year - int(timestamp.month < 7))


def _row_numeric_value(row: dict[str, Any], keys: tuple[str, ...], fallback: float = 0.0) -> float:
    for key in keys:
        value = row.get(key)
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if np.isfinite(numeric):
            return numeric
    return float(fallback)


def _per_minute_average(frame: pd.DataFrame, *, total_column: str, rate_columns: tuple[str, ...]) -> float:
    rate = _first_numeric_column(frame, rate_columns, fallback=np.nan)
    finite_rate = rate[np.isfinite(rate.to_numpy(dtype=float))]
    if not finite_rate.empty:
        return float(finite_rate.mean())
    if total_column not in frame.columns or "minutes" not in frame.columns:
        return 0.0
    totals = pd.to_numeric(frame[total_column], errors="coerce").fillna(0.0)
    minutes = pd.to_numeric(frame["minutes"], errors="coerce").fillna(0.0)
    valid = minutes > 0.0
    if not valid.any():
        return 0.0
    return float((totals[valid] / minutes[valid]).mean())


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
