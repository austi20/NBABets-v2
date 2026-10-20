from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.events import LineSnapshot
from app.providers.odds.kalshi_odds import parse_kalshi_player_props
from app.schemas.domain import GamePayload, PlayerPayload, TeamPayload
from app.services.ingestion import IngestionService

GAME_DAY = date(2026, 10, 21)
NOW = datetime(2026, 10, 20, 15, 0, tzinfo=UTC)


def _kalshi_market(name: str, strike: float) -> dict:
    return {
        "ticker": f"KXNBAPTS-26OCT21DALHOU-HOUKDURANT7-{int(strike + 0.5)}",
        "event_ticker": "KXNBAPTS-26OCT21DALHOU",
        "title": f"{name}: {int(strike + 0.5)}+ points",
        "floor_strike": strike,
        "yes_ask_dollars": "0.4800",
        "no_ask_dollars": "0.5400",
    }


def _seed(session: Session) -> IngestionService:
    service = IngestionService(session)
    service.ingest_teams(
        [
            TeamPayload(provider_team_id="1", abbreviation="HOU", name="Houston Rockets", city="Houston"),
            TeamPayload(provider_team_id="2", abbreviation="DAL", name="Dallas Mavericks", city="Dallas"),
        ]
    )
    service.ingest_players([PlayerPayload(provider_player_id="bdl-7", full_name="Kevin Durant", team_abbreviation="HOU")])
    service.ingest_games(
        [
            GamePayload(
                provider_game_id="bdl-game-1",
                game_date=GAME_DAY,
                start_time=datetime(2026, 10, 21, 23, 30, tzinfo=UTC),
                home_team_abbreviation="HOU",
                away_team_abbreviation="DAL",
                season_code="2026",
            )
        ]
    )
    return service


def test_kalshi_snapshot_lands_on_the_bdl_game_and_player():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        service = _seed(session)
        snapshots = parse_kalshi_player_props([_kalshi_market("Kevin Durant", 24.5)], GAME_DAY, NOW)

        stored = service.ingest_line_snapshots(snapshots)

        assert stored == 1
        row = session.scalars(select(LineSnapshot)).one()
        assert row.line_value == 24.5
        assert row.over_odds == 108
        assert row.under_odds == -117
        assert row.meta["odds_source_provider"] == "kalshi"


def test_unknown_player_is_skipped_not_fatal():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        service = _seed(session)
        snapshots = parse_kalshi_player_props([_kalshi_market("Nobody Atall", 24.5)], GAME_DAY, NOW)

        assert service.ingest_line_snapshots(snapshots) == 0
