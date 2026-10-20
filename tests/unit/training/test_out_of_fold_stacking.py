from __future__ import annotations

import numpy as np
import pandas as pd

from app.training import models
from app.training.models import (
    OOF_BLOCKS,
    StatModelSuite,
    _make_regressor,
    _out_of_fold_base_predictions,
    _safe_training_rate,
)

MARKETS = ("points", "rebounds", "assists", "threes", "turnovers")
FEATURES = ["spread", "total", "days_rest", "role_stability", "consistency_score", "minutes_blended"]


def _noise_frame(rows: int, seed: int = 0) -> pd.DataFrame:
    """Targets are pure noise, so any skill a model shows on its own rows is memorization."""
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame({name: rng.normal(size=rows) for name in FEATURES})
    frame["game_date"] = pd.date_range("2024-10-01", periods=rows, freq="h")
    frame["minutes"] = rng.uniform(15, 35, size=rows)
    frame["predicted_minutes"] = frame["minutes"]
    frame["position_group"] = rng.choice(["G", "F", "C"], size=rows)
    frame["starter_flag"] = rng.integers(0, 2, size=rows)
    for market in MARKETS:
        frame[market] = rng.poisson(8, size=rows).astype(float)
        frame[f"{market}_baseline_mean"] = 8.0
    frame["pra"] = frame["points"] + frame["rebounds"] + frame["assists"]
    frame["pra_baseline_mean"] = 24.0
    return frame


def _oof(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    rate_target = _safe_training_rate(frame["points"], frame["minutes"])
    return _out_of_fold_base_predictions(frame, FEATURES, "points", rate_target, 7, 0.0)


def test_first_block_has_no_oof_prediction():
    direct, rate = _oof(_noise_frame(800))

    first_block = 800 // OOF_BLOCKS
    assert np.isnan(direct[:first_block]).all()
    assert np.isnan(rate[:first_block]).all()
    assert not np.isnan(direct[first_block:]).any()


def test_each_block_is_predicted_by_a_model_fit_only_on_earlier_rows():
    frame = _noise_frame(800)
    direct, _ = _oof(frame)

    edge = 800 // OOF_BLOCKS
    earlier_only = _make_regressor(7)
    earlier_only.fit(frame[FEATURES].iloc[:edge], frame["points"].iloc[:edge])
    expected = np.clip(earlier_only.predict(frame[FEATURES].iloc[edge : 2 * edge]), 0.0, None)

    assert np.allclose(direct[edge : 2 * edge], expected)


def test_oof_predictions_do_not_memorize_noise():
    frame = _noise_frame(1200)
    direct, _ = _oof(frame)
    in_sample_model = _make_regressor(7)
    in_sample_model.fit(frame[FEATURES], frame["points"])

    usable = ~np.isnan(direct)
    oof_corr = np.corrcoef(direct[usable], frame["points"].to_numpy()[usable])[0, 1]
    in_sample_corr = np.corrcoef(in_sample_model.predict(frame[FEATURES]), frame["points"])[0, 1]

    assert in_sample_corr > 0.5  # the leak: a model looks skilled on rows it was fit on
    assert abs(oof_corr) < 0.2


def _base_model_weight(frame: pd.DataFrame) -> float:
    suite = StatModelSuite(random_state=3)
    suite.fit(frame, FEATURES)
    weights = suite.models["points"].learned_weights
    return abs(weights["stack_direct_mean"]) + abs(weights["stack_rate_mean"])


def test_stacker_trusts_base_models_far_less_than_the_in_sample_version(monkeypatch):
    frame = _noise_frame(1200)

    out_of_fold = _base_model_weight(frame)
    monkeypatch.setattr(models, "MIN_ROWS_FOR_OOF", 10**9)  # forces the old in sample path
    in_sample = _base_model_weight(frame)

    # on pure noise the old stacker still puts about 1 on each base model
    assert in_sample > 1.0
    assert out_of_fold < 0.5 * in_sample


def test_small_frames_still_fit_with_in_sample_stacking():
    frame = _noise_frame(200)
    suite = StatModelSuite(random_state=3)

    suite.fit(frame, FEATURES)

    assert set(suite.models) >= set(MARKETS)
