from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.config.settings import get_settings
from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.models.all import (
    BacktestResult,
    Game,
    GamePlayerAvailability,
    InjuryReport,
    LineSnapshot,
    ModelRun,
    Player,
    Prediction,
    PropMarket,
    RawPayload,
    Sportsbook,
    Team,
)
from app.schemas.domain import PlayerGameLogPayload, ProviderFetchResult
from app.services.provider_cache import LocalProviderCache
from app.services.startup_cache import StartupCacheResetService
from app.training.artifacts import artifact_paths, resolve_artifact_namespace


def test_soft_reset_removes_same_day_computed_outputs_only(monkeypatch) -> None:
    session, root = _make_session(monkeypatch)
    today = date.today()
    _seed_startup_state(session, today=today)
    report_path = root / "reports" / f"automation_daily_{today.strftime('%Y%m%d')}T010101Z.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("report", encoding="utf-8")

    result = StartupCacheResetService(session).soft_reset(target_date=today, board_date=today)
    session.expire_all()

    assert result.deleted_predictions == 1
    assert result.deleted_backtests == 1
    assert result.deleted_model_runs == 1
    assert result.deleted_reports == 1
    assert result.deleted_line_snapshots == 0
    assert session.scalar(select(Prediction.prediction_id)) is None
    assert session.scalar(select(BacktestResult.backtest_result_id)) is None
    assert session.scalar(select(ModelRun.model_run_id)) is None
    assert session.scalar(select(LineSnapshot.snapshot_id)) is not None
    assert session.scalar(select(RawPayload.payload_id)) is not None


def test_hard_reset_clears_board_inputs_provider_cache_and_artifacts(monkeypatch) -> None:
    session, root = _make_session(monkeypatch)
    today = date.today()
    _seed_startup_state(session, today=today)
    report_path = root / "reports" / f"automation_daily_{today.strftime('%Y%m%d')}T020202Z.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("report", encoding="utf-8")
    _seed_provider_cache(today=today, root=root)
    artifact_files = _seed_artifacts(session)

    result = StartupCacheResetService(session).hard_reset(target_date=today, board_date=today)
    session.expire_all()

    assert result.deleted_predictions == 1
    assert result.deleted_line_snapshots == 1
    assert result.deleted_raw_payloads == 1
    assert result.deleted_injury_reports == 1
    assert result.deleted_game_availability == 1
    assert result.deleted_provider_cached_fetches >= 1
    assert result.deleted_provider_cached_log_days == 1
    assert result.deleted_provider_cached_logs == 1
    assert len(result.deleted_artifacts) == 4
    assert session.scalar(select(LineSnapshot.snapshot_id)) is None
    assert session.scalar(select(RawPayload.payload_id)) is None
    assert session.scalar(select(InjuryReport.injury_report_id)) is None
    assert session.scalar(select(GamePlayerAvailability.availability_id)) is None
    for artifact_path in artifact_files:
        assert artifact_path.exists() is False


def _make_session(monkeypatch):
    root = Path("temp") / f"startup_reset_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    db_path = root / "reset.sqlite"
    db_url = f"sqlite:///{db_path.resolve().as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("REPORTS_DIR", str(root / "reports"))
    monkeypatch.setenv("PROVIDER_CACHE_DB_PATH", str(root / "provider_cache.sqlite"))
    monkeypatch.setenv("DUCKDB_PATH", str(root / "data" / "processed" / "nba_props.duckdb"))
    get_settings.cache_clear()
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session(), root


def _seed_startup_state(session, *, today: date) -> None:
    start_time = datetime.combine(today, datetime.min.time(), tzinfo=UTC) + timedelta(hours=19)
    team_home = Team(provider_team_id="h", abbreviation="BOS", name="Boston Celtics", city="Boston", conference="East", division="Atlantic", is_active=True)
    team_away = Team(provider_team_id="a", abbreviation="NYK", name="New York Knicks", city="New York", conference="East", division="Atlantic", is_active=True)
    player = Player(provider_player_id="p1", full_name="Test Player", normalized_name="test player", team=team_home, position="G", status="active", meta={})
    market = PropMarket(key="points", stat_type="points", display_name="Points", distribution_family="negative_binomial", is_active=True)
    sportsbook = Sportsbook(key="book", display_name="Book", region="us", is_active=True)
    game = Game(
        provider_game_id="g1",
        game_date=today,
        start_time=start_time,
        home_team_id=1,
        away_team_id=2,
        spread=None,
        total=None,
        status="scheduled",
        final_home_score=None,
        final_away_score=None,
        meta={},
    )
    session.add_all([team_home, team_away, player, market, sportsbook])
    session.flush()
    game.home_team_id = team_home.team_id
    game.away_team_id = team_away.team_id
    session.add(game)
    session.flush()

    raw_payload = RawPayload(
        provider_type="odds",
        provider_name="balldontlie",
        endpoint="/odds",
        fetched_at=start_time,
        content_hash="payload",
        payload={},
    )
    session.add(raw_payload)
    session.flush()

    model_run = ModelRun(
        model_version="v1",
        feature_version="v1",
        started_at=start_time,
        completed_at=start_time,
        metrics={},
    )
    session.add(model_run)
    session.flush()

    line_snapshot = LineSnapshot(
        timestamp=start_time,
        game_id=game.game_id,
        sportsbook_id=sportsbook.sportsbook_id,
        player_id=player.player_id,
        market_id=market.market_id,
        line_value=20.5,
        over_odds=-110,
        under_odds=-110,
        event_status="scheduled",
        source_payload_id=raw_payload.payload_id,
        meta={"is_live_quote": True, "odds_verification_status": "provider_live"},
    )
    session.add(line_snapshot)
    session.flush()

    session.add(
        Prediction(
            model_run_id=model_run.model_run_id,
            game_id=game.game_id,
            player_id=player.player_id,
            market_id=market.market_id,
            line_snapshot_id=line_snapshot.snapshot_id,
            predicted_at=start_time,
            projected_mean=21.0,
            projected_variance=9.0,
            projected_median=21.0,
            over_probability=0.56,
            under_probability=0.44,
            confidence_interval_low=17.0,
            confidence_interval_high=25.0,
            calibration_adjusted_probability=0.55,
            feature_attribution_summary={},
        )
    )
    session.add(
        BacktestResult(
            model_run_id=model_run.model_run_id,
            market_id=market.market_id,
            computed_at=start_time,
            metrics={},
            artifact_path=None,
        )
    )
    session.add(
        InjuryReport(
            player_id=player.player_id,
            team_id=team_home.team_id,
            report_timestamp=start_time,
            game_id=game.game_id,
            status="questionable",
            designation="Q",
            body_part="knee",
            notes=None,
            expected_availability_flag=False,
            source_payload_id=raw_payload.payload_id,
        )
    )
    session.add(
        GamePlayerAvailability(
            game_id=game.game_id,
            player_id=player.player_id,
            provider_player_id="p1",
            player_name="Test Player",
            team_abbreviation="BOS",
            is_active=False,
            reason="injury",
            fetched_at=start_time,
        )
    )
    session.commit()


def _seed_provider_cache(*, today: date, root: Path) -> None:
    cache = LocalProviderCache(path=root / "provider_cache.sqlite")
    fetched_at = datetime.combine(today, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8)
    cache.put_collection(
        provider_type="odds",
        provider_name="balldontlie",
        method_name="fetch_upcoming_player_props",
        scope_key=f'{{"target_date":"{today.isoformat()}"}}',
        result=ProviderFetchResult(endpoint="cache://odds", fetched_at=fetched_at, payload={}),
        items=[],
    )
    cache.put_player_game_logs(
        provider_name="balldontlie",
        requested_days=[today],
        result=ProviderFetchResult(endpoint="cache://logs", fetched_at=fetched_at, payload={}),
        logs=[
            PlayerGameLogPayload(
                provider_game_id="g1",
                provider_player_id="p1",
                team_abbreviation="BOS",
                minutes=30.0,
                points=20,
                rebounds=5,
                assists=4,
                threes=2,
                steals=1,
                blocks=0,
                turnovers=2,
                fouls=2,
                field_goal_attempts=15,
                field_goals_made=8,
                free_throw_attempts=4,
                free_throws_made=2,
                offensive_rebounds=1,
                defensive_rebounds=4,
                plus_minus=5.0,
                starter_flag=True,
                overtime_flag=False,
                meta={"player_name": "Test Player", "game_date": today.isoformat()},
            )
        ],
    )


def _seed_artifacts(session) -> tuple[Path, ...]:
    settings = get_settings()
    namespace = resolve_artifact_namespace(
        session.bind.url.render_as_string(hide_password=False),
        settings.app_env,
    )
    paths = artifact_paths(settings.model_version, namespace)
    for artifact_path in (paths.minutes_model, paths.stat_models, paths.calibrators, paths.metadata):
        artifact_path.write_text("artifact", encoding="utf-8")
    return (paths.minutes_model, paths.stat_models, paths.calibrators, paths.metadata)
