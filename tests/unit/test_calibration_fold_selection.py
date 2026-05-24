from __future__ import annotations

from app.config.settings import Settings


def test_known_market_uses_table_fold_count() -> None:
    settings = Settings()
    assert settings.calibration_fold_table.get("points") == 8
    assert settings.calibration_fold_table.get("assists") == 12
    assert settings.calibration_fold_table.get("rebounds") == 6


def test_unknown_market_falls_back_to_max_folds() -> None:
    settings = Settings()
    unknown = "fantasy_points"
    assert unknown not in settings.calibration_fold_table
    result = settings.calibration_fold_table.get(unknown, settings.calibration_max_folds)
    assert result == settings.calibration_max_folds
