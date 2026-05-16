from __future__ import annotations

from pydantic import BaseModel

from app.services.insights import PropInsight
from app.services.prop_analysis import PropOpportunity, SportsbookQuote


class SportsbookQuoteModel(BaseModel):
    game_id: int
    sportsbook_key: str
    sportsbook_name: str
    icon: str
    market_key: str
    line_value: float
    over_odds: int | None
    under_odds: int | None
    timestamp: str
    is_live_quote: bool
    verification_status: str
    odds_source_provider: str
    over_probability: float
    under_probability: float
    push_probability: float
    calibrated_over_probability: float
    calibrated_under_probability: float
    recommended_side: str
    hit_probability: float
    no_vig_market_probability: float
    source_market_key: str
    is_alternate_line: bool

    @classmethod
    def from_dataclass(cls, value: SportsbookQuote) -> SportsbookQuoteModel:
        return cls(**value.__dict__)


class PropOpportunityModel(BaseModel):
    rank: int
    game_id: int
    player_id: int
    player_name: str
    player_icon: str
    market_key: str
    consensus_line: float
    projected_mean: float
    recommended_side: str
    hit_probability: float
    likelihood_score: int
    calibrated_over_probability: float
    sportsbooks_summary: str
    top_features: list[str]
    quotes: list[SportsbookQuoteModel]
    projected_variance: float | None
    confidence_interval_low: float | None
    confidence_interval_high: float | None
    predicted_at: str | None
    data_sufficiency_tier: str
    data_confidence_score: float
    player_team_abbreviation: str | None
    player_position: str | None
    game_label: str | None
    game_start_time: str | None
    percentile_25: float
    percentile_75: float
    dnp_risk: float
    boom_probability: float
    bust_probability: float
    availability_branches: int
    volatility_coefficient: float = 0.0
    volatility_tier: str = "low"
    adjusted_over_probability: float | None = None

    @classmethod
    def from_dataclass(cls, value: PropOpportunity) -> PropOpportunityModel:
        return cls(
            rank=value.rank,
            game_id=value.game_id,
            player_id=value.player_id,
            player_name=value.player_name,
            player_icon=value.player_icon,
            market_key=value.market_key,
            consensus_line=value.consensus_line,
            projected_mean=value.projected_mean,
            recommended_side=value.recommended_side,
            hit_probability=value.hit_probability,
            likelihood_score=value.likelihood_score,
            calibrated_over_probability=value.calibrated_over_probability,
            sportsbooks_summary=value.sportsbooks_summary,
            top_features=value.top_features,
            quotes=[SportsbookQuoteModel.from_dataclass(quote) for quote in value.quotes],
            projected_variance=value.projected_variance,
            confidence_interval_low=value.confidence_interval_low,
            confidence_interval_high=value.confidence_interval_high,
            predicted_at=value.predicted_at,
            data_sufficiency_tier=value.data_sufficiency_tier,
            data_confidence_score=value.data_confidence_score,
            player_team_abbreviation=value.player_team_abbreviation,
            player_position=value.player_position,
            game_label=value.game_label,
            game_start_time=value.game_start_time,
            percentile_25=value.percentile_25,
            percentile_75=value.percentile_75,
            dnp_risk=value.dnp_risk,
            boom_probability=value.boom_probability,
            bust_probability=value.bust_probability,
            availability_branches=value.availability_branches,
            volatility_coefficient=value.volatility_coefficient,
            volatility_tier=value.volatility_tier,
            adjusted_over_probability=value.adjusted_over_probability,
        )


class PropInsightModel(BaseModel):
    best_quote: SportsbookQuoteModel
    recommended_odds: int | None
    implied_probability: float | None
    fair_american_odds: int | None
    edge: float
    expected_profit_per_unit: float
    confidence_score: int
    confidence_tier: str
    freshness_label: str
    market_width: float
    injury_label: str
    injury_detail: str
    reason_lines: tuple[str, ...]
    warnings: tuple[str, ...]

    @classmethod
    def from_dataclass(cls, value: PropInsight) -> PropInsightModel:
        return cls(
            best_quote=SportsbookQuoteModel.from_dataclass(value.best_quote),
            recommended_odds=value.recommended_odds,
            implied_probability=value.implied_probability,
            fair_american_odds=value.fair_american_odds,
            edge=value.edge,
            expected_profit_per_unit=value.expected_profit_per_unit,
            confidence_score=value.confidence_score,
            confidence_tier=value.confidence_tier,
            freshness_label=value.freshness_label,
            market_width=value.market_width,
            injury_label=value.injury_label,
            injury_detail=value.injury_detail,
            reason_lines=value.reason_lines,
            warnings=value.warnings,
        )


class PropWithInsightModel(BaseModel):
    opportunity: PropOpportunityModel
    insight: PropInsightModel


class PropListResponseModel(BaseModel):
    items: list[PropWithInsightModel]
    total: int
    page: int
    page_size: int

