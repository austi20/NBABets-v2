from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from app.schemas.domain import ProviderFetchResult


class ProviderPayloadSchemaError(ValueError):
    """Raised when a provider response envelope drifts from the expected shape."""


@dataclass(frozen=True)
class PayloadEnvelopeSignature:
    """Lightweight contract on raw JSON envelopes stored on ProviderFetchResult."""

    kind: Literal["dict", "list"]
    required_top_level_keys: frozenset[str] | None = None


WRAPPED_DATA_META = PayloadEnvelopeSignature(kind="dict", required_top_level_keys=frozenset({"data", "meta"}))

NBA_AVAILABILITY_ENVELOPE = PayloadEnvelopeSignature(
    kind="dict",
    required_top_level_keys=frozenset({"game_ids", "inactive_players", "meta"}),
)



def _check_envelope(payload: Any, signature: PayloadEnvelopeSignature) -> None:
    if signature.kind == "list":
        if not isinstance(payload, list):
            raise ProviderPayloadSchemaError(f"expected list payload, got {type(payload).__name__}")
        return
    if not isinstance(payload, dict):
        raise ProviderPayloadSchemaError(f"expected dict payload, got {type(payload).__name__}")
    if signature.required_top_level_keys is None:
        return
    missing = signature.required_top_level_keys - payload.keys()
    if missing:
        raise ProviderPayloadSchemaError(f"payload missing required keys: {sorted(missing)}")


STATS_RESPONSE_SCHEMA: dict[tuple[str, str], PayloadEnvelopeSignature] = {
    ("nba_api", "fetch_teams"): WRAPPED_DATA_META,
    ("nba_api", "fetch_rosters"): WRAPPED_DATA_META,
    ("nba_api", "fetch_schedule"): WRAPPED_DATA_META,
    ("nba_api", "fetch_schedule_range"): WRAPPED_DATA_META,
    ("nba_api", "fetch_player_game_logs"): WRAPPED_DATA_META,
    ("nba_api", "fetch_game_availability"): NBA_AVAILABILITY_ENVELOPE,
    ("balldontlie", "fetch_teams"): WRAPPED_DATA_META,
    ("balldontlie", "fetch_rosters"): WRAPPED_DATA_META,
    ("balldontlie", "fetch_schedule"): WRAPPED_DATA_META,
    ("balldontlie", "fetch_schedule_range"): WRAPPED_DATA_META,
    ("balldontlie", "fetch_player_game_logs"): WRAPPED_DATA_META,
}

ODDS_RESPONSE_SCHEMA: dict[str, PayloadEnvelopeSignature] = {
    "balldontlie": WRAPPED_DATA_META,
}


def validate_stats_provider_response(provider_name: str, method_name: str, fetch_result: ProviderFetchResult) -> None:
    signature = STATS_RESPONSE_SCHEMA.get((provider_name, method_name))
    if signature is None:
        return
    _check_envelope(fetch_result.payload, signature)


def validate_odds_provider_props_response(provider_name: str, fetch_result: ProviderFetchResult) -> None:
    signature = ODDS_RESPONSE_SCHEMA.get(provider_name)
    if signature is None:
        return
    _check_envelope(fetch_result.payload, signature)
