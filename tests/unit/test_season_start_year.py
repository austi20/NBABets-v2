from datetime import date

from app.services.ingestion import _season_start_year


def test_numeric_code_is_start_year_even_for_spring_games():
    assert _season_start_year("2025", date(2026, 3, 1)) == 2025


def test_non_numeric_code_falls_back_to_game_date():
    assert _season_start_year("2025-26", date(2026, 3, 1)) == 2025
    assert _season_start_year("2025-26", date(2025, 11, 1)) == 2025
