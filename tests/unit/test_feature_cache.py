from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from app.training.feature_cache import cached_build
from app.training.features import FeatureSet


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})


def _make_builder():
    fs = FeatureSet(frame=pd.DataFrame({"x": [1, 2, 3]}), feature_columns=["x"])
    return MagicMock(return_value=fs)


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    from app.config import settings as settings_module
    real_settings = settings_module.get_settings()
    monkeypatch.setattr(real_settings, "feature_cache_dir", tmp_path / "cache")
    monkeypatch.setattr(real_settings, "feature_cache_enabled", True)
    return tmp_path / "cache"


def test_cache_hit_returns_identical_frame(cache_dir):
    builder = _make_builder()
    historical = _frame()
    out1 = cached_build(historical, builder)
    out2 = cached_build(historical, builder)
    assert builder.call_count == 1  # second call hit cache
    pd.testing.assert_frame_equal(out1.frame, out2.frame)
    assert out1.feature_columns == out2.feature_columns


def test_cache_miss_on_different_input(cache_dir):
    builder = _make_builder()
    cached_build(_frame(), builder)
    cached_build(pd.DataFrame({"a": [9, 9, 9], "b": [1.0, 1.0, 1.0]}), builder)
    assert builder.call_count == 2


def test_cache_disabled_bypasses(tmp_path, monkeypatch):
    from app.config import settings as settings_module
    real_settings = settings_module.get_settings()
    monkeypatch.setattr(real_settings, "feature_cache_dir", tmp_path / "cache")
    monkeypatch.setattr(real_settings, "feature_cache_enabled", False)
    builder = _make_builder()
    cached_build(_frame(), builder)
    cached_build(_frame(), builder)
    assert builder.call_count == 2


def test_cache_io_failure_falls_back(tmp_path, monkeypatch, caplog):
    from app.config import settings as settings_module
    real_settings = settings_module.get_settings()
    # Point to a path under a regular file so mkdir fails
    blocked = tmp_path / "blocked_file"
    blocked.write_text("not a dir")
    monkeypatch.setattr(real_settings, "feature_cache_dir", blocked / "cache")
    monkeypatch.setattr(real_settings, "feature_cache_enabled", True)
    builder = _make_builder()
    result = cached_build(_frame(), builder)
    assert result.feature_columns == ["x"]
    assert builder.call_count == 1
    # Cached_build's write path swallows the failure; the second call may or may not re-call.
