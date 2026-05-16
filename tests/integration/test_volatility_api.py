"""Tests for the volatility diagnostic endpoint."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.models import all as _models  # noqa: F401 - registers ORM classes
from app.models.all import (
    Game,
    ModelRun,
    Player,
    PlayerGameLog,
    Prediction,
    PropMarket,
    Team,
)


@pytest.fixture
def client_with_prediction(monkeypatch: pytest.MonkeyPatch) -> tuple[TestClient, int]:
    """In-memory SQLite + a seeded prediction; FastAPI app uses our engine."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    LocalSession = sessionmaker(bind=engine)

    with LocalSession() as session:
        team = Team(team_id=1, abbreviation="ZZZ", name="Test", city="Test")
        opp = Team(team_id=2, abbreviation="OPP", name="Opp", city="Opp")
        session.add_all([team, opp])
        session.flush()
        player = Player(
            player_id=42,
            full_name="Test Player",
            normalized_name="test player",
            position="G",
        )
        session.add(player)
        market = PropMarket(market_id=1, key="points", stat_type="points", display_name="Points")
        session.add(market)
        session.flush()
        for index in range(10):
            game = Game(
                game_id=100 + index,
                game_date=date(2026, 4, index + 1),
                start_time=datetime(2026, 4, index + 1, 22, tzinfo=UTC),
                home_team_id=1,
                away_team_id=2,
                status="final",
            )
            session.add(game)
            session.flush()
            session.add(
                PlayerGameLog(
                    player_id=42,
                    game_id=100 + index,
                    team_id=1,
                    opponent_team_id=2,
                    minutes=30.0,
                    points=20,
                    rebounds=5,
                    assists=4,
                    threes=2,
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
                    starter_flag=True,
                )
            )
        run = ModelRun(
            model_version="test",
            feature_version="test",
            started_at=datetime(2026, 5, 1, tzinfo=UTC),
        )
        session.add(run)
        session.flush()
        prediction = Prediction(
            model_run_id=run.model_run_id,
            game_id=100,
            player_id=42,
            market_id=1,
            predicted_at=datetime(2026, 5, 1, 12, tzinfo=UTC),
            projected_mean=22.0,
            projected_variance=4.0,
            projected_median=22.0,
            over_probability=0.65,
            under_probability=0.35,
            confidence_interval_low=15.0,
            confidence_interval_high=29.0,
            calibration_adjusted_probability=0.65,
        )
        session.add(prediction)
        session.commit()
        prediction_id = prediction.prediction_id

    @contextmanager
    def _fake_session_scope():
        session = LocalSession()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    import app.server.routers.props as props_module
    monkeypatch.setattr(props_module, "session_scope", _fake_session_scope)

    app = FastAPI()
    app.include_router(props_module.router)
    return TestClient(app), prediction_id


def test_diagnostic_returns_contributors(
    client_with_prediction: tuple[TestClient, int],
) -> None:
    client, prediction_id = client_with_prediction
    response = client.get(f"/api/props/predictions/{prediction_id}/volatility")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["prediction_id"] == prediction_id
    assert body["tier"] in ("low", "medium", "high")
    assert 0.0 <= body["coefficient"] <= 1.0
    assert isinstance(body["contributors"], list)
    assert "adjusted_probability" in body
    assert "confidence_multiplier" in body


def test_diagnostic_returns_404_for_unknown_prediction(
    client_with_prediction: tuple[TestClient, int],
) -> None:
    client, _ = client_with_prediction
    response = client.get("/api/props/predictions/99999999/volatility")
    assert response.status_code == 404
