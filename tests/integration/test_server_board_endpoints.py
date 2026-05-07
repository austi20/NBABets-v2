from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

from httpx import ASGITransport, AsyncClient

from app.server.main import create_app
from app.server.services.board_cache import BoardCacheEntry
from app.services import rotation_audit
from app.services.insights import (
    BoardSummary,
    InjuryStatusBadge,
    LocalAgentStatus,
    ParlayInsight,
    PropInsight,
    ProviderStatus,
)
from app.services.parlays import ParlayLeg, ParlayRecommendation
from app.services.prop_analysis import PropOpportunity, SportsbookQuote
from app.services.query import BoardAvailability


class _FakeBoardCache:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        quote = SportsbookQuote(
            game_id=1001,
            sportsbook_key="book-a",
            sportsbook_name="Book A",
            icon="A",
            market_key="points",
            line_value=24.5,
            over_odds=-110,
            under_odds=-110,
            timestamp=now.isoformat(),
            is_live_quote=True,
            verification_status="provider_live",
            odds_source_provider="balldontlie",
            over_probability=0.56,
            under_probability=0.41,
            push_probability=0.03,
            calibrated_over_probability=0.58,
            calibrated_under_probability=0.39,
            recommended_side="OVER",
            hit_probability=0.58,
            no_vig_market_probability=0.52,
            source_market_key="points",
            is_alternate_line=False,
        )
        opportunity = PropOpportunity(
            rank=1,
            game_id=1001,
            player_id=42,
            player_name="Test Player",
            player_icon="T",
            market_key="points",
            consensus_line=24.5,
            projected_mean=26.2,
            recommended_side="OVER",
            hit_probability=0.58,
            likelihood_score=58,
            calibrated_over_probability=0.58,
            sportsbooks_summary="A Book A",
            top_features=["Usage up"],
            quotes=[quote],
            projected_variance=4.0,
            confidence_interval_low=20.0,
            confidence_interval_high=31.0,
            predicted_at=now.isoformat(),
            data_sufficiency_tier="A",
            data_confidence_score=0.9,
            percentile_25=21.4,
            percentile_75=30.1,
            dnp_risk=0.15,
            boom_probability=0.33,
            bust_probability=0.18,
            availability_branches=4,
        )
        prop_insight = PropInsight(
            best_quote=quote,
            recommended_odds=-110,
            implied_probability=0.5238,
            fair_american_odds=-138,
            edge=0.0562,
            expected_profit_per_unit=0.106,
            confidence_score=82,
            confidence_tier="Strong",
            freshness_label="Just now",
            market_width=0.0,
            injury_label="Clear",
            injury_detail="No injury report on file",
            reason_lines=("Edge is positive",),
            warnings=(),
        )
        leg = ParlayLeg(
            game_id=1001,
            matchup="AAA @ BBB",
            player_name="Test Player",
            market_key="points",
            recommended_side="OVER",
            line_value=24.5,
            american_odds=-110,
            hit_probability=0.58,
            likelihood_score=58,
            is_live_quote=True,
            verification_status="provider_live",
            odds_source_provider="balldontlie",
        )
        parlay = ParlayRecommendation(
            rank=1,
            game_id=1001,
            matchup="AAA @ BBB",
            sportsbook_key="book-a",
            sportsbook_name="Book A",
            sportsbook_icon="A",
            leg_count=2,
            game_count=1,
            game_ids=(1001,),
            game_labels=("AAA @ BBB",),
            joint_probability=0.33,
            combined_decimal_odds=3.5,
            combined_american_odds=250,
            expected_profit_per_unit=0.15,
            implied_probability=0.2857,
            edge=0.0443,
            all_legs_live=True,
            verification_status="provider_live",
            odds_source_provider="balldontlie",
            correlation_penalty=0.98,
            average_leg_hit_probability=0.58,
            weakest_leg_hit_probability=0.58,
            legs=[leg],
        )
        parlay_insight = ParlayInsight(
            confidence_score=74,
            confidence_tier="Strong",
            fragility_label="Moderate",
            reason_lines=("Positive edge",),
            warnings=(),
        )
        provider = ProviderStatus(
            provider_type="odds",
            provider_name="balldontlie",
            endpoint="/v1/odds",
            fetched_at=now,
            freshness_label="Just now",
            status_label="Fresh",
            detail="1 payloads in last 24h",
        )
        injury = InjuryStatusBadge(
            label="Questionable",
            detail="Ankle",
            updated_at=now,
            severity=12,
        )
        local_agent = LocalAgentStatus(
            enabled=True,
            auto_execute_safe=False,
            updated_at=now,
            updated_by="test",
            note="note",
            last_run_status="ok",
            last_run_at=now,
            last_summary="summary",
            last_confidence=0.8,
        )
        self.entry = BoardCacheEntry(
            board_date=date(2026, 5, 5),
            board_availability=BoardAvailability(
                board_date=date(2026, 5, 5),
                scheduled_games=4,
                live_games=0,
                final_games=0,
            ),
            board_summary=BoardSummary(
                board_date=date(2026, 5, 5),
                game_count=4,
                opportunity_count=1,
                sportsbook_count=1,
                quote_count=1,
                live_quote_count=1,
                alt_line_count=0,
                same_game_parlay_count=1,
                multi_game_parlay_count=1,
                latest_quote_at=now,
                latest_prediction_at=now,
            ),
            opportunities=[opportunity],
            opportunity_insights={(1001, 42, "points", 24.5): prop_insight},
            same_game_sections_by_book={"book-a": {2: {1001: [parlay]}}},
            multi_game_sections_by_book={"book-a": {2: [parlay]}},
            parlay_insights={
                ("book-a", 2, 1001, 0): parlay_insight,
                ("book-a", 2, -1, 0): parlay_insight,
            },
            provider_statuses=[provider],
            injury_status_by_player_id={42: injury},
            local_agent_status=local_agent,
        )

    def get_or_build(self, target_date=None):  # noqa: ANN001
        return self.entry


def test_board_props_parlays_and_insights_endpoints() -> None:
    async def _run() -> None:
        app = create_app(board_cache=_FakeBoardCache())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            board_summary = await client.get("/api/board/summary")
            assert board_summary.status_code == 200
            assert board_summary.json()["opportunity_count"] == 1

            board_availability = await client.get("/api/board/availability")
            assert board_availability.status_code == 200
            assert board_availability.json()["scheduled_games"] == 4

            props = await client.get("/api/props?confidence=Watch+&market=points&book=Book A")
            assert props.status_code == 200
            assert props.json()["total"] == 1
            assert props.json()["items"][0]["insight"]["confidence_tier"] == "Strong"
            assert props.json()["items"][0]["opportunity"]["dnp_risk"] == 0.15
            assert props.json()["items"][0]["opportunity"]["availability_branches"] == 4

            prop_detail = await client.get("/api/props/42/points/24.5")
            assert prop_detail.status_code == 200
            assert prop_detail.json()["opportunity"]["player_name"] == "Test Player"
            assert prop_detail.json()["opportunity"]["boom_probability"] == 0.33

            sgp = await client.get("/api/parlays/sgp?game_id=1001&book=book-a")
            assert sgp.status_code == 200
            assert "book-a" in sgp.json()["sections"]

            multi = await client.get("/api/parlays/multi?book=book-a")
            assert multi.status_code == 200
            assert "2" in multi.json()["sections"]["book-a"]

            providers = await client.get("/api/insights/providers")
            assert providers.status_code == 200
            assert providers.json()[0]["provider_type"] == "odds"

            injuries = await client.get("/api/insights/injuries?player_ids=42")
            assert injuries.status_code == 200
            assert injuries.json()["42"]["label"] == "Questionable"

    asyncio.run(_run())


def test_rotation_audit_endpoint_returns_redistribution_payload(tmp_path, monkeypatch) -> None:
    async def _run() -> None:
        monkeypatch.setattr(rotation_audit, "AUDIT_ROOT", tmp_path / "rotation_audit")
        rotation_audit.write_game_audit(
            game_id=1001,
            absences=[{"player_id": 42, "player_name": "Missing Player"}],
            adjustments=[{"player_id": 24, "minutes_delta": 6.0}],
            team_environment={"team_id": 1, "rotation_shock_magnitude": 6.0},
        )
        app = create_app(board_cache=_FakeBoardCache())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props/rotation-audit/1001")

        assert response.status_code == 200
        payload = response.json()
        assert payload["absences"][0]["player_id"] == 42
        assert payload["adjustments"][0]["minutes_delta"] == 6.0
        assert payload["team_environment"][0]["rotation_shock_magnitude"] == 6.0

    asyncio.run(_run())

