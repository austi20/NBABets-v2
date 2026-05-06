import pandas as pd
import pytest

from app.providers.cache.parquet_store import partition_by_season, read_parquet, write_parquet


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"player": ["a", "b", "c"], "pts": [10, 20, 30], "season": ["2023", "2023", "2024"]})


def test_roundtrip(tmp_path: pytest.TempPathFactory, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "test.parquet"
    write_parquet(sample_df, path)
    result = read_parquet(path)
    pd.testing.assert_frame_equal(sample_df, result)


def test_partition_split(tmp_path: pytest.TempPathFactory, sample_df: pd.DataFrame) -> None:
    partition_by_season(sample_df, tmp_path)

    season_2023 = tmp_path / "season=2023" / "part-0.parquet"
    season_2024 = tmp_path / "season=2024" / "part-0.parquet"

    assert season_2023.exists()
    assert season_2024.exists()

    df_2023 = read_parquet(season_2023)
    df_2024 = read_parquet(season_2024)

    assert len(df_2023) == 2
    assert len(df_2024) == 1
