"""Unit tests for parquet merge behaviour in TrainingPipeline.train()."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_frame(n: int) -> pd.DataFrame:
    """Return a minimal DataFrame with the columns the pipeline touches."""
    return pd.DataFrame(
        {
            "game_date": [date(2024, 1, i + 1) for i in range(n)],
            "player_id": list(range(n)),
        }
    )


class _EarlyExit(Exception):
    """Raised by build_training_frame mock to abort train() before real work."""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _build_pipeline(parquet_root: Path | None) -> object:
    """Construct a TrainingPipeline with all heavy deps mocked out."""
    from app.training.pipeline import TrainingPipeline

    session_mock = MagicMock()
    # resolve_artifact_namespace needs session.bind.url to be falsy
    session_mock.bind = None

    settings_mock = SimpleNamespace(
        database_url="sqlite:///:memory:",
        app_env="test",
        historical_parquet_root=parquet_root,
        training_seed=42,
    )

    with patch("app.training.pipeline.get_settings", return_value=settings_mock), \
         patch("app.training.pipeline.resolve_artifact_namespace", return_value="test-ns"), \
         patch("app.training.pipeline.DatasetLoader"), \
         patch("app.training.pipeline.FeatureEngineer"):
        pipeline = TrainingPipeline(session_mock)

    # Replace internals directly so we control all I/O
    pipeline._settings = settings_mock  # type: ignore[attr-defined]
    return pipeline


class TestParquetMerge:
    def test_merges_sqlite_and_parquet_rows(self, tmp_path: Path) -> None:
        """train() should concat parquet frame with SQLite frame when root is set."""
        from app.training.pipeline import TrainingPipeline

        sqlite_frame = _make_frame(5)
        parquet_frame = _make_frame(3)

        pipeline = _build_pipeline(parquet_root=tmp_path)

        loader_mock = MagicMock()
        loader_mock.load_historical_player_games.return_value = sqlite_frame
        loader_mock.load_historical_player_games_from_parquet.return_value = parquet_frame
        pipeline._loader = loader_mock  # type: ignore[attr-defined]

        # Abort right after the load step via build_training_frame
        features_mock = MagicMock()
        features_mock.build_training_frame.side_effect = _EarlyExit
        pipeline._features = features_mock  # type: ignore[attr-defined]

        with pytest.raises(_EarlyExit):
            pipeline.train()

        # build_training_frame must have been called with the merged 8-row frame
        call_args = features_mock.build_training_frame.call_args
        assert call_args is not None
        merged: pd.DataFrame = call_args.args[0]
        assert len(merged) == 8, f"Expected 8 rows, got {len(merged)}"

    def test_skips_merge_when_parquet_root_is_none(self, tmp_path: Path) -> None:
        """train() must not call load_historical_player_games_from_parquet when root is None."""
        sqlite_frame = _make_frame(5)

        pipeline = _build_pipeline(parquet_root=None)

        loader_mock = MagicMock()
        loader_mock.load_historical_player_games.return_value = sqlite_frame
        pipeline._loader = loader_mock  # type: ignore[attr-defined]

        features_mock = MagicMock()
        features_mock.build_training_frame.side_effect = _EarlyExit
        pipeline._features = features_mock  # type: ignore[attr-defined]

        with pytest.raises(_EarlyExit):
            pipeline.train()

        loader_mock.load_historical_player_games_from_parquet.assert_not_called()
        call_args = features_mock.build_training_frame.call_args
        merged: pd.DataFrame = call_args.args[0]
        assert len(merged) == 5

    def test_skips_merge_when_parquet_root_missing(self, tmp_path: Path) -> None:
        """train() must skip gracefully if parquet_root path does not exist."""
        nonexistent = tmp_path / "no_such_dir"
        sqlite_frame = _make_frame(5)

        pipeline = _build_pipeline(parquet_root=nonexistent)

        loader_mock = MagicMock()
        loader_mock.load_historical_player_games.return_value = sqlite_frame
        pipeline._loader = loader_mock  # type: ignore[attr-defined]

        features_mock = MagicMock()
        features_mock.build_training_frame.side_effect = _EarlyExit
        pipeline._features = features_mock  # type: ignore[attr-defined]

        with pytest.raises(_EarlyExit):
            pipeline.train()

        loader_mock.load_historical_player_games_from_parquet.assert_not_called()

    def test_skips_merge_when_caller_passes_historical(self, tmp_path: Path) -> None:
        """If historical is pre-built by caller, parquet merge must be skipped."""
        caller_frame = _make_frame(7)

        pipeline = _build_pipeline(parquet_root=tmp_path)

        loader_mock = MagicMock()
        pipeline._loader = loader_mock  # type: ignore[attr-defined]

        features_mock = MagicMock()
        features_mock.build_training_frame.side_effect = _EarlyExit
        pipeline._features = features_mock  # type: ignore[attr-defined]

        with pytest.raises(_EarlyExit):
            pipeline.train(historical=caller_frame)

        loader_mock.load_historical_player_games.assert_not_called()
        loader_mock.load_historical_player_games_from_parquet.assert_not_called()
        call_args = features_mock.build_training_frame.call_args
        merged: pd.DataFrame = call_args.args[0]
        assert len(merged) == 7
