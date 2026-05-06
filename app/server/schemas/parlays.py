from __future__ import annotations

from pydantic import BaseModel

from app.services.insights import ParlayInsight
from app.services.parlays import ParlayLeg, ParlayRecommendation


class ParlayLegModel(BaseModel):
    game_id: int
    matchup: str
    player_name: str
    market_key: str
    recommended_side: str
    line_value: float
    american_odds: int
    hit_probability: float
    likelihood_score: int
    is_live_quote: bool
    verification_status: str
    odds_source_provider: str

    @classmethod
    def from_dataclass(cls, value: ParlayLeg) -> ParlayLegModel:
        return cls(**value.__dict__)


class ParlayInsightModel(BaseModel):
    confidence_score: int
    confidence_tier: str
    fragility_label: str
    reason_lines: tuple[str, ...]
    warnings: tuple[str, ...]

    @classmethod
    def from_dataclass(cls, value: ParlayInsight) -> ParlayInsightModel:
        return cls(**value.__dict__)


class ParlayRecommendationModel(BaseModel):
    rank: int
    game_id: int
    matchup: str
    sportsbook_key: str
    sportsbook_name: str
    sportsbook_icon: str
    leg_count: int
    game_count: int
    game_ids: tuple[int, ...]
    game_labels: tuple[str, ...]
    joint_probability: float
    combined_decimal_odds: float
    combined_american_odds: int
    expected_profit_per_unit: float
    implied_probability: float
    edge: float
    all_legs_live: bool
    verification_status: str
    odds_source_provider: str
    correlation_penalty: float
    average_leg_hit_probability: float
    weakest_leg_hit_probability: float
    legs: list[ParlayLegModel]

    @classmethod
    def from_dataclass(cls, value: ParlayRecommendation) -> ParlayRecommendationModel:
        return cls(
            rank=value.rank,
            game_id=value.game_id,
            matchup=value.matchup,
            sportsbook_key=value.sportsbook_key,
            sportsbook_name=value.sportsbook_name,
            sportsbook_icon=value.sportsbook_icon,
            leg_count=value.leg_count,
            game_count=value.game_count,
            game_ids=value.game_ids,
            game_labels=value.game_labels,
            joint_probability=value.joint_probability,
            combined_decimal_odds=value.combined_decimal_odds,
            combined_american_odds=value.combined_american_odds,
            expected_profit_per_unit=value.expected_profit_per_unit,
            implied_probability=value.implied_probability,
            edge=value.edge,
            all_legs_live=value.all_legs_live,
            verification_status=value.verification_status,
            odds_source_provider=value.odds_source_provider,
            correlation_penalty=value.correlation_penalty,
            average_leg_hit_probability=value.average_leg_hit_probability,
            weakest_leg_hit_probability=value.weakest_leg_hit_probability,
            legs=[ParlayLegModel.from_dataclass(leg) for leg in value.legs],
        )


class ParlayWithInsightModel(BaseModel):
    parlay: ParlayRecommendationModel
    insight: ParlayInsightModel


class SameGameParlaysResponseModel(BaseModel):
    sections: dict[str, dict[str, dict[str, list[ParlayWithInsightModel]]]]


class MultiGameParlaysResponseModel(BaseModel):
    sections: dict[str, dict[str, list[ParlayWithInsightModel]]]

