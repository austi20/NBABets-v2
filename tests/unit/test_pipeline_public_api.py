"""Public-API surface that backtest scripts depend on.

`scripts/backtest_rotation_shock.py` and `scripts/backtest_with_surrogate.py`
must be able to reach pipeline/backtester internals through a stable public
interface (no leading-underscore access). These tests pin that contract.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _build_pipeline() -> object:
    from app.training.pipeline import TrainingPipeline

    session_mock = MagicMock()
    session_mock.bind = None
    settings_mock = SimpleNamespace(
        database_url="sqlite:///:memory:",
        app_env="test",
        training_seed=42,
    )
    with patch("app.training.pipeline.get_settings", return_value=settings_mock), \
         patch("app.training.pipeline.resolve_artifact_namespace", return_value="test-ns"), \
         patch("app.training.pipeline.DatasetLoader"), \
         patch("app.training.pipeline.FeatureEngineer"):
        return TrainingPipeline(session_mock)


class TestTrainingPipelinePublicAPI:
    def test_features_property_exposes_engineer(self) -> None:
        pipeline = _build_pipeline()
        assert pipeline.features is pipeline._features  # type: ignore[attr-defined]

    def test_settings_property_exposes_settings(self) -> None:
        pipeline = _build_pipeline()
        assert pipeline.settings is pipeline._settings  # type: ignore[attr-defined]

    def test_l1_alpha_property_exposes_value(self) -> None:
        pipeline = _build_pipeline()
        assert pipeline.l1_alpha == pipeline._l1_alpha  # type: ignore[attr-defined]

    def test_dist_family_for_market_is_public(self) -> None:
        pipeline = _build_pipeline()
        assert pipeline.dist_family_for_market("points") == pipeline._dist_family_for_market(  # type: ignore[attr-defined]
            "points"
        )

    def test_simulation_context_is_public(self) -> None:
        pipeline = _build_pipeline()
        row = {"player_id": 1, "game_id": 2}
        assert pipeline.simulation_context(row, "points") == pipeline._simulation_context(  # type: ignore[attr-defined]
            row, "points"
        )

    def test_apply_rotation_treatment_mode_is_public(self) -> None:
        import pandas as pd

        pipeline = _build_pipeline()
        # Public method exists and delegates to the private implementation.
        assert callable(pipeline.apply_rotation_treatment_mode)  # type: ignore[attr-defined]
        empty = pd.DataFrame()
        result = pipeline.apply_rotation_treatment_mode(empty, write_audit=False)  # type: ignore[attr-defined]
        assert result is empty  # rotation mode "off"/empty frame is a no-op passthrough


class TestRollingOriginBacktesterPublicAPI:
    def test_loader_property_get_set(self) -> None:
        from app.evaluation.backtest import RollingOriginBacktester

        session_mock = MagicMock()
        with patch("app.evaluation.backtest.get_settings", return_value=SimpleNamespace(training_seed=1)), \
             patch("app.evaluation.backtest.DatasetLoader"), \
             patch("app.evaluation.backtest.FeatureEngineer"):
            backtester = RollingOriginBacktester(session_mock)

        sentinel = object()
        backtester.loader = sentinel
        assert backtester.loader is sentinel
        assert backtester._loader is sentinel  # type: ignore[attr-defined]
