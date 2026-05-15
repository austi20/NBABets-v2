from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.all import AgentRunEvent, AIProviderEvent, Game, InjuryReport, Prediction, RawPayload
from app.services.board_date import to_local_board_date
from app.services.live_games import sync_live_games_from_nba_api
from app.services.local_autonomy.policy_state import load_local_agent_policy_state
from app.services.parlays import ParlayRecommendation
from app.services.prop_analysis import PropOpportunity, SportsbookQuote


# Markets with high per-game variance that should be suppressed relative to
# stable production markets (points, rebounds, assists, combos).
_VOLATILE_MARKET_PENALTY: dict[str, int] = {
    "threes": 10,
    "turnovers": 12,
    "steals": 8,
    "blocks": 8,
}


@dataclass(frozen=True)
class ProviderStatus:
    provider_type: str
    provider_name: str
    endpoint: str
    fetched_at: datetime | None
    freshness_label: str
    status_label: str
    detail: str


@dataclass(frozen=True)
class InjuryStatusBadge:
    label: str
    detail: str
    updated_at: datetime | None
    severity: int


@dataclass(frozen=True)
class BoardSummary:
    board_date: date | None
    game_count: int
    opportunity_count: int
    sportsbook_count: int
    quote_count: int
    live_quote_count: int
    alt_line_count: int
    same_game_parlay_count: int
    multi_game_parlay_count: int
    latest_quote_at: datetime | None
    latest_prediction_at: datetime | None


@dataclass(frozen=True)
class PropInsight:
    best_quote: SportsbookQuote
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


@dataclass(frozen=True)
class ParlayInsight:
    confidence_score: int
    confidence_tier: str
    fragility_label: str
    reason_lines: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class LocalAgentStatus:
    enabled: bool
    auto_execute_safe: bool
    updated_at: datetime
    updated_by: str
    note: str
    last_run_status: str
    last_run_at: datetime | None
    last_summary: str
    last_confidence: float | None


def load_provider_statuses(session: Session) -> list[ProviderStatus]:
    rows = session.execute(
        select(
            RawPayload.provider_type,
            RawPayload.provider_name,
            RawPayload.endpoint,
            RawPayload.fetched_at,
        ).order_by(RawPayload.fetched_at.desc())
    ).all()
    latest: dict[tuple[str, str], tuple[str, datetime, str]] = {}
    counts: Counter[tuple[str, str]] = Counter()
    cutoff = datetime.now(UTC).replace(microsecond=0)
    for provider_type, provider_name, endpoint, fetched_at in rows:
        fetched_at = _coerce_utc_datetime(fetched_at)
        key = (str(provider_type), str(provider_name))
        if fetched_at is not None and (cutoff - fetched_at).total_seconds() <= 86400:
            counts[key] += 1
        latest.setdefault(key, (str(endpoint), fetched_at, _provider_freshness_label(str(provider_type), fetched_at)))
    statuses: list[ProviderStatus] = []
    for (provider_type, provider_name), (endpoint, fetched_at, status_label) in sorted(latest.items()):
        statuses.append(
            ProviderStatus(
                provider_type=provider_type,
                provider_name=provider_name,
                endpoint=endpoint,
                fetched_at=fetched_at,
                freshness_label=format_relative_age(fetched_at),
                status_label=status_label,
                detail=f"{counts[(provider_type, provider_name)]} payloads in last 24h",
            )
        )
    return statuses


def load_injury_statuses(session: Session, player_ids: set[int]) -> dict[int, InjuryStatusBadge]:
    if not player_ids:
        return {}
    rows = session.execute(
        select(
            InjuryReport.player_id,
            InjuryReport.status,
            InjuryReport.designation,
            InjuryReport.notes,
            InjuryReport.report_timestamp,
        )
        .where(InjuryReport.player_id.in_(player_ids))
        .order_by(InjuryReport.player_id, InjuryReport.report_timestamp.desc())
    ).all()
    statuses: dict[int, InjuryStatusBadge] = {}
    for player_id, status, designation, notes, report_timestamp in rows:
        player_key = int(player_id)
        if player_key in statuses:
            continue
        label = str(designation or status or "Unknown").strip() or "Unknown"
        detail_parts = [part for part in (status, notes) if part]
        statuses[player_key] = InjuryStatusBadge(
            label=label.title(),
            detail=" | ".join(str(part) for part in detail_parts) if detail_parts else "No detail provided",
            updated_at=report_timestamp,
            severity=_injury_severity(label),
        )
    return statuses


def load_local_agent_status(session: Session) -> LocalAgentStatus:
    policy = load_local_agent_policy_state()
    last_run = session.execute(
        select(
            AgentRunEvent.status,
            AgentRunEvent.created_at,
            AgentRunEvent.detail,
            AgentRunEvent.confidence,
        )
        .where(AgentRunEvent.agent_role == "local_autonomy", AgentRunEvent.event_type == "local_autonomy_decision")
        .order_by(AgentRunEvent.created_at.desc())
        .limit(1)
    ).first()
    if last_run is None:
        return LocalAgentStatus(
            enabled=policy.enabled,
            auto_execute_safe=policy.auto_execute_safe,
            updated_at=policy.updated_at,
            updated_by=policy.updated_by,
            note=policy.note,
            last_run_status="never",
            last_run_at=None,
            last_summary="No local autonomy decision has been recorded yet.",
            last_confidence=None,
        )
    status, created_at, detail, confidence = last_run
    return LocalAgentStatus(
        enabled=policy.enabled,
        auto_execute_safe=policy.auto_execute_safe,
        updated_at=policy.updated_at,
        updated_by=policy.updated_by,
        note=policy.note,
        last_run_status=str(status),
        last_run_at=created_at,
        last_summary=str(detail or "--"),
        last_confidence=float(confidence) if confidence is not None else None,
    )


def build_local_ai_terminal_text(
    session: Session,
    *,
    endpoint: str,
    model: str,
    limit: int = 100,
) -> str:
    """Format recent local/fallback AI orchestrator events as a terminal-style log."""
    rows = session.execute(
        select(
            AIProviderEvent.created_at,
            AIProviderEvent.provider_name,
            AIProviderEvent.model_name,
            AIProviderEvent.event_type,
            AIProviderEvent.status,
            AIProviderEvent.latency_ms,
            AIProviderEvent.detail,
            AIProviderEvent.payload,
        )
        .where(AIProviderEvent.provider_name.in_(("local", "fallback")))
        .order_by(AIProviderEvent.created_at.desc())
        .limit(limit)
    ).all()

    header_lines = [
        f"# Local AI (llama.cpp) - {endpoint}",
        f"# model: {model}",
        "# ---",
    ]
    if not rows:
        return "\n".join(header_lines + ['(no events yet - run automation or "Run Local Review" on Overview)']) + "\n"

    body: list[str] = []
    for created_at, provider_name, model_name, event_type, status, latency_ms, detail, payload in reversed(rows):
        ts = format_timestamp(_coerce_utc_datetime(created_at)) if created_at else "--"
        lat = f"{latency_ms}ms" if latency_ms is not None else "--"
        det = (detail or "").replace("\n", " ")
        if len(det) > 160:
            det = det[:157] + "..."
        ep = ""
        if isinstance(payload, dict):
            ep = str(payload.get("endpoint", ""))
        model_s = model_name or "--"
        body.append(
            f"[{ts}] {provider_name} | {model_s} | {event_type} | {status} | {lat} | {ep}\n    {det}"
        )
    return "\n".join(header_lines + body) + "\n"


def build_board_summary(
    session: Session,
    target_date: date | None,
    opportunities: list[PropOpportunity],
    same_game_sections: dict[str, dict[int, dict[int, list[ParlayRecommendation]]]],
    multi_game_sections: dict[str, dict[int, list[ParlayRecommendation]]],
) -> BoardSummary:
    sync_live_games_from_nba_api(session, target_date or date.today())
    prediction_rows = session.execute(
        select(Prediction.predicted_at, Game.game_date, Game.start_time)
        .join(Game, Prediction.game_id == Game.game_id)
        .where(Game.status == "scheduled")
    ).all()
    if target_date is None:
        latest_prediction_at = max((predicted_at for predicted_at, _, _ in prediction_rows), default=None)
    else:
        latest_prediction_at = max(
            (
                predicted_at
                for predicted_at, game_date, start_time in prediction_rows
                if to_local_board_date(game_date, start_time) == target_date
            ),
            default=None,
        )
    game_rows = session.execute(
        select(Game.game_date, Game.start_time).where(Game.status == "scheduled")
    ).all()
    if target_date is None:
        game_count = len(game_rows)
    else:
        game_count = sum(1 for game_date, start_time in game_rows if to_local_board_date(game_date, start_time) == target_date)
    all_quotes = [quote for opportunity in opportunities for quote in opportunity.quotes]
    quote_timestamps = [
        parsed_timestamp
        for quote in all_quotes
        if (parsed_timestamp := parse_timestamp(quote.timestamp)) is not None
    ]
    latest_quote_at = max(quote_timestamps, default=None)
    sportsbook_count = len({quote.sportsbook_key for quote in all_quotes})
    return BoardSummary(
        board_date=target_date,
        game_count=int(game_count),
        opportunity_count=len(opportunities),
        sportsbook_count=sportsbook_count,
        quote_count=len(all_quotes),
        live_quote_count=sum(1 for quote in all_quotes if quote.is_live_quote),
        alt_line_count=sum(1 for quote in all_quotes if quote.is_alternate_line),
        same_game_parlay_count=_count_nested_parlays(same_game_sections),
        multi_game_parlay_count=_count_nested_parlays(multi_game_sections),
        latest_quote_at=latest_quote_at,
        latest_prediction_at=latest_prediction_at,
    )


def build_prop_insight(
    opportunity: PropOpportunity,
    injury: InjuryStatusBadge | None = None,
    *,
    now: datetime | None = None,
) -> PropInsight:
    best_quote = max(
        opportunity.quotes,
        key=lambda quote: (
            _quote_expected_profit(quote),
            _quote_edge(quote),
            quote.hit_probability,
            -abs(quote.line_value - opportunity.consensus_line),
        ),
    )
    recommended_odds = _recommended_odds(best_quote)
    implied_probability = american_implied_probability(recommended_odds)
    edge = _quote_edge(best_quote)
    expected_profit = _quote_expected_profit(best_quote)
    market_width = 0.0
    if opportunity.quotes:
        line_values = [quote.line_value for quote in opportunity.quotes]
        market_width = max(line_values) - min(line_values)
    uncertainty_ratio = _uncertainty_ratio(opportunity)
    quote_timestamps = [
        parsed_timestamp
        for quote in opportunity.quotes
        if (parsed_timestamp := parse_timestamp(quote.timestamp)) is not None
    ]
    latest_quote_at = max(quote_timestamps, default=None)
    freshness_label = format_relative_age(latest_quote_at, now=now)
    confidence_score = _prop_confidence_score(
        opportunity=opportunity,
        best_quote=best_quote,
        edge=edge,
        latest_quote_at=latest_quote_at,
        uncertainty_ratio=uncertainty_ratio,
        injury=injury,
        now=now,
    )
    warnings = _prop_warnings(opportunity, best_quote, market_width, uncertainty_ratio, injury, latest_quote_at, now=now)
    reasons = _prop_reasons(opportunity, best_quote, edge, expected_profit)
    return PropInsight(
        best_quote=best_quote,
        recommended_odds=recommended_odds,
        implied_probability=implied_probability,
        fair_american_odds=fair_american_odds(best_quote.hit_probability),
        edge=edge,
        expected_profit_per_unit=expected_profit,
        confidence_score=confidence_score,
        confidence_tier=confidence_tier(confidence_score),
        freshness_label=freshness_label,
        market_width=market_width,
        injury_label=injury.label if injury else "Clear",
        injury_detail=injury.detail if injury else "No injury report on file",
        reason_lines=reasons,
        warnings=warnings,
    )


def build_parlay_insight(parlay: ParlayRecommendation) -> ParlayInsight:
    score = 0
    score += min(int(parlay.average_leg_hit_probability * 45), 45)
    score += min(int(parlay.weakest_leg_hit_probability * 25), 25)
    score += min(int(max(parlay.edge, 0.0) * 250), 20)
    if parlay.game_count > 1:
        score += min(parlay.game_count * 2, 8)
    if parlay.correlation_penalty >= 0.95:
        score += 4
    score -= max(parlay.leg_count - 6, 0) * 2
    if parlay.weakest_leg_hit_probability < 0.56:
        score -= 8
    score = max(1, min(score, 99))

    warnings: list[str] = []
    if not parlay.all_legs_live:
        warnings.append("One or more legs are not backed by live verified odds.")
    if parlay.weakest_leg_hit_probability < 0.56:
        warnings.append("At least one leg is materially weaker than the rest of the ticket.")
    market_counts = Counter(leg.market_key for leg in parlay.legs)
    dominant_market, dominant_count = market_counts.most_common(1)[0]
    if dominant_count >= max(3, parlay.leg_count // 2):
        warnings.append(f"{dominant_count} legs come from {dominant_market.upper()}, which concentrates market risk.")
    if parlay.game_count == 1 and parlay.leg_count >= 5:
        warnings.append("Large same-game stacks lean on heuristic correlation handling, not joint modeling.")
    if parlay.game_count > 1 and parlay.game_count < min(3, parlay.leg_count):
        warnings.append("This multi-game parlay still relies on some same-game stacking to fill legs.")

    reasons = [
        f"Model edge is {parlay.edge * 100:.2f}% over the book's implied parlay probability.",
        f"Average leg hit rate is {parlay.average_leg_hit_probability * 100:.1f}% with the weakest leg at {parlay.weakest_leg_hit_probability * 100:.1f}%.",
    ]
    if parlay.game_count > 1:
        reasons.append(f"The ticket spans {parlay.game_count} games before using any stacked fill legs.")
    else:
        reasons.append(f"A same-game dependence adjustment factor of {parlay.correlation_penalty:.3f} is already applied.")

    fragility = "Stable"
    if parlay.leg_count >= 8 or parlay.weakest_leg_hit_probability < 0.58:
        fragility = "Fragile"
    elif parlay.leg_count >= 5 or parlay.weakest_leg_hit_probability < 0.62:
        fragility = "Moderate"

    return ParlayInsight(
        confidence_score=score,
        confidence_tier=confidence_tier(score),
        fragility_label=fragility,
        reason_lines=tuple(reasons),
        warnings=tuple(warnings),
    )


def confidence_tier(score: int) -> str:
    if score >= 85:
        return "Elite"
    if score >= 72:
        return "Strong"
    if score >= 60:
        return "Solid"
    if score >= 48:
        return "Watch"
    return "Fragile"


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "--"
    return value.astimezone().strftime("%Y-%m-%d %I:%M %p")


def format_relative_age(value: datetime | None, *, now: datetime | None = None) -> str:
    if value is None:
        return "Unavailable"
    anchor = now or datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    minutes = max(int((anchor - value).total_seconds() // 60), 0)
    if minutes < 1:
        return "Just now"
    if minutes < 60:
        return f"{minutes}m ago"
    hours, rem_minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {rem_minutes}m ago"
    days, rem_hours = divmod(hours, 24)
    return f"{days}d {rem_hours}h ago"


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def american_implied_probability(american_odds: int | None) -> float | None:
    if american_odds is None or american_odds == 0:
        return None
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    return abs(american_odds) / (abs(american_odds) + 100.0)


def fair_american_odds(probability: float | None) -> int | None:
    if probability is None or probability <= 0 or probability >= 1:
        return None
    if probability >= 0.5:
        return int(round(-(100.0 * probability) / max(1.0 - probability, 1e-6)))
    return int(round((100.0 * (1.0 - probability)) / max(probability, 1e-6)))


def _provider_freshness_label(provider_type: str, fetched_at: datetime | None) -> str:
    if fetched_at is None:
        return "Unavailable"
    fetched_at = _coerce_utc_datetime(fetched_at)
    if fetched_at is None:
        return "Unavailable"
    age_seconds = max((datetime.now(UTC) - fetched_at).total_seconds(), 0.0)
    fresh_seconds, aging_seconds = {
        "odds": (7200, 21600),
        "injuries": (14400, 43200),
        "stats": (21600, 86400),
    }.get(provider_type.lower(), (21600, 86400))
    if age_seconds <= fresh_seconds:
        return "Fresh"
    if age_seconds <= aging_seconds:
        return "Aging"
    return "Stale"


def _count_nested_parlays(value: object) -> int:
    if isinstance(value, dict):
        return sum(_count_nested_parlays(item) for item in value.values())
    if isinstance(value, list):
        return len(value)
    return 0


def _recommended_odds(quote: SportsbookQuote) -> int | None:
    return quote.over_odds if quote.recommended_side == "OVER" else quote.under_odds


def _quote_expected_profit(quote: SportsbookQuote) -> float:
    recommended_odds = _recommended_odds(quote)
    if recommended_odds is None:
        return 0.0
    decimal_odds = 1.0 + (recommended_odds / 100.0 if recommended_odds > 0 else 100.0 / abs(recommended_odds))
    return quote.hit_probability * decimal_odds - 1.0


def _quote_edge(quote: SportsbookQuote) -> float:
    implied_probability = american_implied_probability(_recommended_odds(quote))
    if implied_probability is None:
        return 0.0
    return quote.hit_probability - implied_probability


def _prop_confidence_score(
    *,
    opportunity: PropOpportunity,
    best_quote: SportsbookQuote,
    edge: float,
    latest_quote_at: datetime | None,
    uncertainty_ratio: float | None,
    injury: InjuryStatusBadge | None,
    now: datetime | None,
) -> int:
    score = min(int(best_quote.hit_probability * 55), 55)
    score += min(int(max(edge, 0.0) * 300), 20)
    score += min(len(opportunity.quotes) * 4, 12)
    score += min(int(max(opportunity.data_confidence_score, 0.0) * 12), 12)
    age_label = format_relative_age(latest_quote_at, now=now)
    if age_label == "Just now" or age_label.endswith("m ago"):
        score += 8
    elif "h " in age_label:
        score += 4
    if uncertainty_ratio is not None:
        if uncertainty_ratio <= 0.35:
            score += 10
        elif uncertainty_ratio <= 0.6:
            score += 5
    if injury is not None:
        score -= injury.severity
    if best_quote.is_alternate_line:
        score -= 2
    score -= _VOLATILE_MARKET_PENALTY.get(opportunity.market_key, 0)
    return max(1, min(score, 99))


def _uncertainty_ratio(opportunity: PropOpportunity) -> float | None:
    if opportunity.confidence_interval_low is None or opportunity.confidence_interval_high is None:
        return None
    width = max(opportunity.confidence_interval_high - opportunity.confidence_interval_low, 0.0)
    baseline = max(abs(opportunity.projected_mean), 5.0)
    return width / baseline


def _prop_warnings(
    opportunity: PropOpportunity,
    best_quote: SportsbookQuote,
    market_width: float,
    uncertainty_ratio: float | None,
    injury: InjuryStatusBadge | None,
    latest_quote_at: datetime | None,
    *,
    now: datetime | None,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if len(opportunity.quotes) == 1:
        warnings.append("Only one live book is backing this recommendation.")
    if _quote_is_one_sided(best_quote):
        warnings.append("This sportsbook is currently pricing only one side of the line.")
    if market_width >= 2.0:
        warnings.append(f"Books disagree by {market_width:.1f} points on this market.")
    if latest_quote_at is not None and "d " in format_relative_age(latest_quote_at, now=now):
        warnings.append("Quote timestamps are old enough that the board may be stale.")
    elif latest_quote_at is not None and format_relative_age(latest_quote_at, now=now).startswith("6h"):
        warnings.append("Quotes are aging; re-check books before acting.")
    if uncertainty_ratio is not None and uncertainty_ratio > 0.6:
        warnings.append("Model uncertainty band is wide relative to the projection.")
    if opportunity.data_confidence_score < 0.35:
        warnings.append(
            f"Prediction coverage is thin ({opportunity.data_sufficiency_tier}, data confidence {opportunity.data_confidence_score:.2f})."
        )
    if best_quote.push_probability > 0.03:
        warnings.append("This line carries non-trivial push probability.")
    if best_quote.is_alternate_line:
        warnings.append("Best value currently comes from an alternate ladder line.")
    if injury is not None and injury.severity >= 12:
        warnings.append(f"Player health is flagged: {injury.label}.")
    return tuple(warnings)


def _prop_reasons(
    opportunity: PropOpportunity,
    best_quote: SportsbookQuote,
    edge: float,
    expected_profit: float,
) -> tuple[str, ...]:
    quote_label = "Best available side quote" if _quote_is_one_sided(best_quote) else "Best live quote"
    recommended_odds = _recommended_odds(best_quote)
    odds_display = "--" if recommended_odds is None else f"{recommended_odds:+d}"
    reasons = [
        f"{quote_label} is {best_quote.icon} {best_quote.sportsbook_name} {best_quote.recommended_side} at {odds_display}.",
        f"Model edge is {edge * 100:.2f}% with expected profit {expected_profit:.2f} units per 1 staked.",
        f"Data coverage tier is {opportunity.data_sufficiency_tier} with confidence {opportunity.data_confidence_score:.2f}.",
    ]
    if len(opportunity.quotes) > 1:
        reasons.append(f"{len(opportunity.quotes)} books contribute to the displayed market view.")
    if opportunity.top_features:
        reasons.append(f"Primary signal summary: {opportunity.top_features[0]}.")
    return tuple(reasons)


def _quote_is_one_sided(quote: SportsbookQuote) -> bool:
    return (quote.over_odds is None) != (quote.under_odds is None)


def _injury_severity(label: str) -> int:
    lowered = label.lower()
    if "out" in lowered or "doubtful" in lowered or "inactive" in lowered:
        return 24
    if "questionable" in lowered or "game time" in lowered or "probable" in lowered:
        return 12
    return 4
