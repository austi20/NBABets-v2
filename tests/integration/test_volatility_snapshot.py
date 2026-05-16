from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401 - registers all ORM classes
from app.models.all import Game, Player, PlayerGameLog, Team
from app.services.volatility import FeatureSnapshot, build_feature_snapshot


@pytest.fixture
def seeded_session() -> Session:
    """Yields a session with one player + 10 recent games of synthetic data."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    LocalSession = sessionmaker(bind=engine)
    session = LocalSession()

    team = Team(team_id=1001, abbreviation="ZZZ", name="Test", city="Test")
    opp = Team(team_id=1002, abbreviation="OPP", name="Opp", city="Opp")
    session.add_all([team, opp])
    session.flush()
    player = Player(
        player_id=99001,
        full_name="Test Player",
        normalized_name="test player",
        position="G",
    )
    session.add(player)
    session.flush()
    for index in range(10):
        game = Game(
            game_id=900000 + index,
            game_date=date(2026, 4, index + 1),
            start_time=datetime(2026, 4, index + 1, 22, tzinfo=UTC),
            home_team_id=1001,
            away_team_id=1002,
            status="final",
        )
        session.add(game)
        session.flush()
        session.add(
            PlayerGameLog(
                player_id=99001,
                game_id=900000 + index,
                team_id=1001,
                opponent_team_id=1002,
                minutes=30.0 + (index - 5) * 1.5,
                points=int(20 + (index - 5) * 2),
                rebounds=5,
                assists=4,
                threes=int(2 + (index - 5) * 0.5),
                turnovers=2,
                steals=1,
                blocks=0,
                field_goal_attempts=15,
                field_goals_made=8,
                free_throw_attempts=4,
                free_throws_made=3,
                offensive_rebounds=1,
                defensive_rebounds=4,
                plus_minus=0.0,
                fouls=2,
                starter_flag=index >= 5,
            )
        )
    session.commit()
    yield session
    session.close()


def test_build_feature_snapshot_returns_populated_fields(seeded_session: Session) -> None:
    snap = build_feature_snapshot(
        session=seeded_session,
        player_id=99001,
        market_key="points",
        as_of_date=date(2026, 5, 1),
        predicted_minutes_std=4.0,
    )

    assert isinstance(snap, FeatureSnapshot)
    assert snap.stat_mean_10 is not None
    assert snap.stat_std_10 is not None
    assert snap.minutes_mean_10 is not None
    assert snap.minutes_std_10 is not None
    assert snap.predicted_minutes_std == 4.0
    assert snap.starter_flag_rate is not None
    assert 0.0 <= snap.starter_flag_rate <= 1.0
    assert snap.mean_5 is not None
    assert snap.mean_season is not None


def test_build_feature_snapshot_for_unknown_player_yields_all_none(seeded_session: Session) -> None:
    snap = build_feature_snapshot(
        session=seeded_session,
        player_id=12345,  # not seeded
        market_key="points",
        as_of_date=date(2026, 5, 1),
        predicted_minutes_std=None,
    )
    assert snap.stat_mean_10 is None
    assert snap.minutes_mean_10 is None
    assert snap.starter_flag_rate is None
    assert snap.predicted_minutes_std is None


def test_build_feature_snapshot_unknown_market_returns_none_stat_only(seeded_session: Session) -> None:
    snap = build_feature_snapshot(
        session=seeded_session,
        player_id=99001,
        market_key="not_a_real_market",
        as_of_date=date(2026, 5, 1),
        predicted_minutes_std=4.0,
    )
    assert snap.stat_mean_10 is None
    assert snap.stat_std_10 is None
    # Minutes/archetype still populated since they're market-independent
    assert snap.minutes_mean_10 is not None
    assert snap.starter_flag_rate is not None
