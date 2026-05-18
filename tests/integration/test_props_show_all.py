"""Integration tests for the show_all=true bypass on /api/props.

Verifies the show_all query param skips both the backtest tuning filter
(disabled markets + max edge) and the confidence filter, while still
honoring market/book/sort/pagination.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

from httpx import ASGITransport, AsyncClient

from app.server.main import create_app
from app.server.services.board_cache import BoardCacheEntry
from app.services.insights import BoardSummary, PropInsight
from app.services.prop_analysis import PropOpportunity, SportsbookQuote
from app.services.query import BoardAvailability


def _make_quote(
    *,
    market: str,
    line: float,
    hit_probability: float,
    sportsbook_name: str = "Book A",
    sportsbook_key: str = "book-a",
    over_odds: int = -110,
    under_odds: int = -110,
) -> SportsbookQuote:
    now = datetime.now(UTC)
    return SportsbookQuote(
        game_id=1001,
        sportsbook_key=sportsbook_key,
        sportsbook_name=sportsbook_name,
        icon="A",
        market_key=market,
        line_value=line,
        over_odds=over_odds,
        under_odds=under_odds,
        timestamp=now.isoformat(),
        is_live_quote=True,
        verification_status="provider_live",
        odds_source_provider="balldontlie",
        over_probability=hit_probability,
        under_probability=max(0.0, 1.0 - hit_probability - 0.03),
        push_probability=0.03,
        calibrated_over_probability=hit_probability,
        calibrated_under_probability=max(0.0, 1.0 - hit_probability - 0.03),
        recommended_side="OVER",
        hit_probability=hit_probability,
        no_vig_market_probability=0.52,
        source_market_key=market,
        is_alternate_line=False,
    )


def _make_opportunity(
    *,
    player_id: int,
    player_name: str,
    market: str,
    line: float,
    hit_probability: float,
    quote: SportsbookQuote,
) -> PropOpportunity:
    now = datetime.now(UTC)
    return PropOpportunity(
        rank=1,
        game_id=1001,
        player_id=player_id,
        player_name=player_name,
        player_icon="T",
        market_key=market,
        consensus_line=line,
        projected_mean=line + 1.0,
        recommended_side="OVER",
        hit_probability=hit_probability,
        likelihood_score=int(hit_probability * 100),
        calibrated_over_probability=hit_probability,
        sportsbooks_summary="A Book A",
        top_features=["Usage up"],
        quotes=[quote],
        projected_variance=4.0,
        confidence_interval_low=line - 4.0,
        confidence_interval_high=line + 4.0,
        predicted_at=now.isoformat(),
        data_sufficiency_tier="A",
        data_confidence_score=0.9,
        percentile_25=line - 3.0,
        percentile_75=line + 3.0,
        dnp_risk=0.15,
        boom_probability=0.33,
        bust_probability=0.18,
        availability_branches=4,
    )


def _make_insight(quote: SportsbookQuote, *, confidence_tier: str = "Strong") -> PropInsight:
    return PropInsight(
        best_quote=quote,
        recommended_odds=-110,
        implied_probability=0.5238,
        fair_american_odds=-138,
        edge=0.05,
        expected_profit_per_unit=0.1,
        confidence_score=82,
        confidence_tier=confidence_tier,
        freshness_label="Just now",
        market_width=0.0,
        injury_label="Clear",
        injury_detail="No injury report on file",
        reason_lines=("Edge is positive",),
        warnings=(),
    )


class _FakeBoardCache:
    """Board with three opportunities chosen to exercise both filters.

    1. solid_points: market=points, hit_p=0.58, tier=Strong -> always passes
    2. disabled_threes: market=threes, hit_p=0.55, tier=Strong -> dropped by
       backtest filter unless show_all=true
    3. fragile_high_edge: market=points, hit_p=0.85 (edge 0.35 > 0.30),
       tier=Fragile -> dropped by both backtest filter (edge cap) and
       confidence filter unless show_all=true
    """

    def __init__(self) -> None:
        now = datetime.now(UTC)

        solid_quote = _make_quote(market="points", line=24.5, hit_probability=0.58)
        solid_opp = _make_opportunity(
            player_id=42,
            player_name="Solid Player",
            market="points",
            line=24.5,
            hit_probability=0.58,
            quote=solid_quote,
        )
        solid_insight = _make_insight(solid_quote, confidence_tier="Strong")

        threes_quote = _make_quote(
            market="threes",
            line=2.5,
            hit_probability=0.55,
            sportsbook_name="Book B",
            sportsbook_key="book-b",
        )
        threes_opp = _make_opportunity(
            player_id=43,
            player_name="Threes Player",
            market="threes",
            line=2.5,
            hit_probability=0.55,
            quote=threes_quote,
        )
        threes_insight = _make_insight(threes_quote, confidence_tier="Strong")

        fragile_quote = _make_quote(market="points", line=18.5, hit_probability=0.85)
        fragile_opp = _make_opportunity(
            player_id=44,
            player_name="Fragile Player",
            market="points",
            line=18.5,
            hit_probability=0.85,
            quote=fragile_quote,
        )
        fragile_insight = _make_insight(fragile_quote, confidence_tier="Fragile")

        self.entry = BoardCacheEntry(
            board_date=date(2026, 5, 17),
            board_availability=BoardAvailability(
                board_date=date(2026, 5, 17),
                scheduled_games=1,
                live_games=0,
                final_games=0,
            ),
            board_summary=BoardSummary(
                board_date=date(2026, 5, 17),
                game_count=1,
                opportunity_count=3,
                sportsbook_count=2,
                quote_count=3,
                live_quote_count=3,
                alt_line_count=0,
                same_game_parlay_count=0,
                multi_game_parlay_count=0,
                latest_quote_at=now,
                latest_prediction_at=now,
            ),
            opportunities=[solid_opp, threes_opp, fragile_opp],
            opportunity_insights={
                (1001, 42, "points", 24.5): solid_insight,
                (1001, 43, "threes", 2.5): threes_insight,
                (1001, 44, "points", 18.5): fragile_insight,
            },
            same_game_sections_by_book={},
            multi_game_sections_by_book={},
            parlay_insights={},
            provider_statuses=[],
            injury_status_by_player_id={},
            local_agent_status=None,
        )

    def get_or_build(self, target_date=None):  # noqa: ANN001
        return self.entry


def _player_names(payload: dict) -> set[str]:
    return {item["opportunity"]["player_name"] for item in payload["items"]}


def test_show_all_false_drops_disabled_and_overedge_props() -> None:
    """Default behavior: existing filters drop the threes market and the
    over-edge fragile prop."""

    async def _run() -> None:
        app = create_app(board_cache=_FakeBoardCache())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 1
            assert _player_names(payload) == {"Solid Player"}

    asyncio.run(_run())


def test_show_all_true_surfaces_every_prop() -> None:
    """show_all=true bypasses both the backtest filter and the confidence
    filter, surfacing all three opportunities."""

    async def _run() -> None:
        app = create_app(board_cache=_FakeBoardCache())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?show_all=true")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 3
            assert _player_names(payload) == {
                "Solid Player",
                "Threes Player",
                "Fragile Player",
            }

    asyncio.run(_run())


def test_show_all_true_still_honors_market_filter() -> None:
    """show_all=true keeps user-driven market/book filters in effect."""

    async def _run() -> None:
        app = create_app(board_cache=_FakeBoardCache())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?show_all=true&market=threes")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 1
            assert _player_names(payload) == {"Threes Player"}

            book_response = await client.get("/api/props?show_all=true&book=Book B")
            assert book_response.status_code == 200
            book_payload = book_response.json()
            assert book_payload["total"] == 1
            assert _player_names(book_payload) == {"Threes Player"}

    asyncio.run(_run())


class _MultiBookBoard:
    """Single opportunity quoted by three different books at different prices.

    Book A has the worst over price (-130), Book B the best (+105), Book C
    middle (-110). All on points, line 24.5.
    """

    def __init__(self) -> None:
        now = datetime.now(UTC)

        quote_a = _make_quote(
            market="points",
            line=24.5,
            hit_probability=0.58,
            sportsbook_name="Book A",
            sportsbook_key="book-a",
            over_odds=-130,
            under_odds=110,
        )
        quote_b = _make_quote(
            market="points",
            line=24.5,
            hit_probability=0.58,
            sportsbook_name="Book B",
            sportsbook_key="book-b",
            over_odds=105,
            under_odds=-125,
        )
        quote_c = _make_quote(
            market="points",
            line=24.5,
            hit_probability=0.58,
            sportsbook_name="Book C",
            sportsbook_key="book-c",
            over_odds=-110,
            under_odds=-110,
        )

        opportunity = _make_opportunity(
            player_id=42,
            player_name="Solid Player",
            market="points",
            line=24.5,
            hit_probability=0.58,
            quote=quote_b,
        )
        opportunity.quotes.clear()
        opportunity.quotes.extend([quote_a, quote_b, quote_c])
        insight = _make_insight(quote_b, confidence_tier="Strong")

        self.entry = BoardCacheEntry(
            board_date=date(2026, 5, 17),
            board_availability=BoardAvailability(
                board_date=date(2026, 5, 17),
                scheduled_games=1,
                live_games=0,
                final_games=0,
            ),
            board_summary=BoardSummary(
                board_date=date(2026, 5, 17),
                game_count=1,
                opportunity_count=1,
                sportsbook_count=3,
                quote_count=3,
                live_quote_count=3,
                alt_line_count=0,
                same_game_parlay_count=0,
                multi_game_parlay_count=0,
                latest_quote_at=now,
                latest_prediction_at=now,
            ),
            opportunities=[opportunity],
            opportunity_insights={(1001, 42, "points", 24.5): insight},
            same_game_sections_by_book={},
            multi_game_sections_by_book={},
            parlay_insights={},
            provider_statuses=[],
            injury_status_by_player_id={},
            local_agent_status=None,
        )

    def get_or_build(self, target_date=None):  # noqa: ANN001
        return self.entry


def test_book_filter_includes_props_where_book_has_any_quote() -> None:
    """Book filter is membership-based: even when a book doesn't have the best
    price, the prop surfaces when that book quotes it."""

    async def _run() -> None:
        app = create_app(board_cache=_MultiBookBoard())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?book=Book A")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 1
            assert _player_names(payload) == {"Solid Player"}
            assert payload["items"][0]["insight"]["best_quote"]["sportsbook_name"] == "Book A"

    asyncio.run(_run())


def test_flatten_by_book_all_returns_one_row_per_quote() -> None:
    """flatten_by_book=true + book=All emits one item per opportunity quote."""

    async def _run() -> None:
        app = create_app(board_cache=_MultiBookBoard())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?flatten_by_book=true")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 3
            books = [item["insight"]["best_quote"]["sportsbook_name"] for item in payload["items"]]
            assert set(books) == {"Book A", "Book B", "Book C"}

    asyncio.run(_run())


def test_flatten_by_book_specific_book_returns_that_books_row() -> None:
    """Specific book filter returns one item carrying that book's quote."""

    async def _run() -> None:
        app = create_app(board_cache=_MultiBookBoard())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?book=Book C")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 1
            assert payload["items"][0]["insight"]["best_quote"]["sportsbook_name"] == "Book C"
            assert payload["items"][0]["insight"]["best_quote"]["over_odds"] == -110

    asyncio.run(_run())


def test_flatten_by_book_recomputes_edge_per_quote() -> None:
    """Per-row edge reflects the row's actual odds, not the best book's."""

    async def _run() -> None:
        app = create_app(board_cache=_MultiBookBoard())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?flatten_by_book=true&sort=Best Edge")
            assert response.status_code == 200
            payload = response.json()
            by_book = {
                item["insight"]["best_quote"]["sportsbook_name"]: item["insight"]
                for item in payload["items"]
            }
            # Book A's -130 over has higher implied prob than Book B's +105 over,
            # so Book A's edge must be strictly smaller than Book B's edge.
            assert by_book["Book A"]["edge"] < by_book["Book B"]["edge"]
            assert by_book["Book C"]["edge"] < by_book["Book B"]["edge"]

    asyncio.run(_run())


def test_default_unflattened_preserves_best_quote_behavior() -> None:
    """flatten_by_book=false and book=All -> one item per prop using best quote."""

    async def _run() -> None:
        app = create_app(board_cache=_MultiBookBoard())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props")
            assert response.status_code == 200
            payload = response.json()
            assert payload["total"] == 1
            assert payload["items"][0]["insight"]["best_quote"]["sportsbook_name"] == "Book B"

    asyncio.run(_run())


def test_show_all_true_ignores_confidence_filter() -> None:
    """A Fragile-tier prop should surface with show_all=true even when
    confidence=Strong+ is also passed."""

    async def _run() -> None:
        app = create_app(board_cache=_FakeBoardCache())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/props?show_all=true&confidence=Elite")
            assert response.status_code == 200
            payload = response.json()
            assert "Fragile Player" in _player_names(payload)

    asyncio.run(_run())
