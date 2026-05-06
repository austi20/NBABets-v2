from __future__ import annotations

from scripts import retrain_and_predict


def test_parse_args_uses_none_defaults_for_locked_training_params(monkeypatch) -> None:
    monkeypatch.delenv("TRAINING_K_SEASONS", raising=False)
    monkeypatch.delenv("TRAINING_L1_ALPHA", raising=False)
    monkeypatch.delenv("TRAINING_DIST_FAMILY", raising=False)
    monkeypatch.setattr("sys.argv", ["retrain_and_predict.py"])

    args = retrain_and_predict._parse_args()

    assert args.k_seasons is None
    assert args.l1_alpha is None
    assert args.dist_family is None


def test_parse_args_reads_training_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("TRAINING_K_SEASONS", "5")
    monkeypatch.setenv("TRAINING_L1_ALPHA", "0.01")
    monkeypatch.setenv("TRAINING_DIST_FAMILY", "count_aware")
    monkeypatch.setattr("sys.argv", ["retrain_and_predict.py"])

    args = retrain_and_predict._parse_args()

    assert args.k_seasons == 5
    assert args.l1_alpha == 0.01
    assert args.dist_family == "count_aware"
