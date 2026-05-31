"""Pin calibrator application in the projection pricing path (predict_upcoming).

This is the CRITICAL money path: predict_upcoming loads the per-market
ProbabilityCalibrator artifact and applies calibrator.transform(...) to the raw
projection-CDF over/under probabilities before writing
``calibration_adjusted_probability`` / ``calibrated_under_probability`` onto the
persisted ``PropLineEvaluation`` rows.

These tests drive the REAL production method (no calibrator math reimplemented in
the test) against an in-memory DB, then read back the persisted row and assert the
written value equals ``calibrator.transform(raw_over_prob)`` — not the raw CDF
value. Reverting the pipeline to write the raw probability makes these fail.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models.all  # noqa: F401  (registers all ORM tables)
from app.db.base import Base
from app.models.all import PropLineEvaluation, PropMarket
from app.training.calibration import ProbabilityCalibrator
from app.training.distributions import DistributionSummary
from app.training.features import FeatureSet


@pytest.fixture()
def db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    session.add(
        PropMarket(
            market_id=1,
            key="points",
            stat_type="points",
            display_name="Points",
            distribution_family="legacy",
            is_active=True,
        )
    )
    session.commit()
    return session


def _fitted_shift_calibrator() -> ProbabilityCalibrator:
    """A NON-identity calibrator that demonstrably moves a raw probability.

    Trained on a skewed sample so transform(raw) != raw, letting the test detect
    whether the production path actually applied it.
    """
    calibrator = ProbabilityCalibrator()
    raw = np.array([0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70])
    labels = np.array([0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0])
    calibrator.fit(raw, labels)
    return calibrator


def _build_summary(mean: float, variance: float, over_p: float) -> DistributionSummary:
    return DistributionSummary(
        mean=mean,
        variance=variance,
        median=mean,
        p10=mean - 5.0,
        p90=mean + 5.0,
        over_probability=over_p,
        under_probability=1.0 - over_p,
        ci_low=mean - 6.0,
        ci_high=mean + 6.0,
        p25=mean - 2.0,
        p75=mean + 2.0,
        boom_probability=0.3,
        bust_probability=0.2,
    )


def _make_pipeline(db_session: Session, calibrators: dict[str, ProbabilityCalibrator]):
    """Construct a TrainingPipeline wired to a real DB session with all heavy
    artifact/loader/feature/model collaborators mocked, but the calibrator
    application block (the code under test) running for real."""
    from app.training.pipeline import TrainingPipeline

    settings_mock = SimpleNamespace(
        database_url="sqlite:///:memory:",
        app_env="test",
        training_seed=42,
        model_version="vtest",
        feature_version="fvtest",
        simulation_min_samples=200,
    )

    metadata = {"model_run_id": 1, "feature_columns": [], "feature_version": "fvtest"}
    stat_models = SimpleNamespace(
        models={"points": SimpleNamespace(feature_columns=[])}
    )
    minutes_model = MagicMock()
    minutes_model.predict.return_value = np.array([30.0])
    minutes_model.predict_uncertainty.return_value = np.array([4.0])

    def _load_artifact(path):  # noqa: ANN001
        name = str(path)
        if "calibrators" in name:
            return calibrators
        if "minutes_model" in name:
            return minutes_model
        if "stat_models" in name:
            return stat_models
        if "metadata" in name:
            return metadata
        return None

    upcoming = pd.DataFrame(
        [
            {
                "game_id": 100,
                "player_id": 200,
                "player_name": "Test Player",
                "market_key": "points",
                "line_value": 24.5,
                "snapshot_id": 9,
                "over_odds": -110,
                "under_odds": -110,
                "predicted_minutes": 30.0,
                "historical_games": 40,
                "days_rest": 2,
            }
        ]
    )

    with patch("app.training.pipeline.get_settings", return_value=settings_mock), \
         patch("app.training.pipeline.resolve_artifact_namespace", return_value="test-ns"), \
         patch("app.training.pipeline.DatasetLoader"), \
         patch("app.training.pipeline.FeatureEngineer"), \
         patch("app.training.pipeline.artifact_exists", return_value=True), \
         patch("app.training.pipeline.load_artifact", side_effect=_load_artifact):
        pipeline = TrainingPipeline(db_session)
        pipeline._settings = settings_mock  # type: ignore[attr-defined]

    # Loader returns our controlled upcoming frame; historical is unused once we
    # short-circuit feature building.
    loader_mock = MagicMock()
    loader_mock.load_historical_player_games.return_value = pd.DataFrame({"player_id": [200]})
    loader_mock.load_upcoming_player_lines.return_value = upcoming
    pipeline._loader = loader_mock  # type: ignore[attr-defined]

    features_mock = MagicMock()
    features_mock.build_inference_frame.return_value = FeatureSet(
        frame=upcoming.copy(), feature_columns=[]
    )
    features_mock.apply_post_minutes_features.side_effect = lambda frame, **kw: frame
    pipeline._features = features_mock  # type: ignore[attr-defined]

    # Pass-through data-sufficiency annotation (avoids needing the full feature set).
    pipeline._annotate_data_sufficiency = lambda historical, upcoming: upcoming  # type: ignore[attr-defined,assignment]

    return pipeline, settings_mock, _load_artifact, metadata, stat_models, minutes_model


def _run_predict(pipeline, settings_mock, load_artifact_fn, raw_over: float):
    summary = _build_summary(mean=26.0, variance=20.0, over_p=raw_over)
    simulation = SimpleNamespace(summary=summary, samples_used=5000, margin_of_error=0.01)
    pipeline._simulate_market_rows = lambda **kw: [simulation]  # type: ignore[attr-defined]
    baseline_mock = MagicMock()
    baseline_mock.return_value.predict.return_value = {}
    with patch("app.training.pipeline.get_settings", return_value=settings_mock), \
         patch("app.training.pipeline.artifact_exists", return_value=True), \
         patch("app.training.pipeline.RecencyBaseline", baseline_mock), \
         patch("app.training.pipeline.load_artifact", side_effect=load_artifact_fn):
        return pipeline.predict_upcoming(target_date=date(2026, 5, 31), persist_predictions=True)


def test_calibrator_applied_to_persisted_line_evaluation(db_session: Session) -> None:
    """The persisted calibration_adjusted_probability must equal
    calibrator.transform(raw_over_prob), proving the pricing path applies the
    artifact calibrator rather than passing the raw CDF probability through."""
    calibrator = _fitted_shift_calibrator()
    raw_over = 0.62
    expected_calibrated = float(calibrator.transform(np.array([raw_over]))[0])
    # Guard: the fixture calibrator must actually move the value, else the test
    # would pass even on the reverted (raw passthrough) code.
    assert abs(expected_calibrated - raw_over) > 1e-3, "calibrator fixture is effectively identity"

    pipeline, settings_mock, load_fn, *_ = _make_pipeline(db_session, {"points": calibrator})
    _run_predict(pipeline, settings_mock, load_fn, raw_over=raw_over)

    row = db_session.query(PropLineEvaluation).one()
    assert row.over_probability == pytest.approx(raw_over, abs=1e-9)
    assert row.calibration_adjusted_probability == pytest.approx(expected_calibrated, abs=1e-9)
    # Sanity: calibrated value differs from the raw CDF probability.
    assert abs(row.calibration_adjusted_probability - row.over_probability) > 1e-3


def test_no_calibrator_writes_raw_probability(db_session: Session) -> None:
    """When no calibrator artifact exists for the market, the written
    calibration_adjusted_probability is the raw/uncalibrated probability."""
    pipeline, settings_mock, load_fn, *_ = _make_pipeline(db_session, {})  # empty calibrators
    raw_over = 0.62
    _run_predict(pipeline, settings_mock, load_fn, raw_over=raw_over)

    row = db_session.query(PropLineEvaluation).one()
    assert row.calibration_adjusted_probability == pytest.approx(row.over_probability, abs=1e-9)
    assert row.calibration_adjusted_probability == pytest.approx(raw_over, abs=1e-9)


def test_timestamp_present_on_persisted_row(db_session: Session) -> None:
    """Lightweight smoke that the pricing path persisted a complete row."""
    pipeline, settings_mock, load_fn, *_ = _make_pipeline(db_session, {})
    _run_predict(pipeline, settings_mock, load_fn, raw_over=0.55)
    row = db_session.query(PropLineEvaluation).one()
    assert isinstance(row.evaluated_at, datetime) or row.evaluated_at is not None
    _ = datetime.now(UTC)  # keep import meaningful
