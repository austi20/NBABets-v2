from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.config.settings import Settings
from app.services.automation import _build_release_recommendation, apply_release_policy_override


def test_override_ignored_without_reason() -> None:
    base = _build_release_recommendation(api_tier="C", prediction_count=10, model_run_count=1)
    settings = Settings.model_construct(
        release_policy_override_enabled=True,
        release_policy_override_reason="",
        release_policy_override_until=None,
    )
    out = apply_release_policy_override(base, settings=settings)
    assert out["status"] == "BLOCKED"


def test_override_promotes_blocked_to_policy_override() -> None:
    base = _build_release_recommendation(api_tier="C", prediction_count=10, model_run_count=1)
    settings = Settings.model_construct(
        release_policy_override_enabled=True,
        release_policy_override_reason="Ops-approved hotfix window",
        release_policy_override_until=None,
    )
    out = apply_release_policy_override(base, settings=settings)
    assert out["status"] == "POLICY_OVERRIDE"
    assert "hotfix" in out["rationale"]


def test_override_expired_reverts_to_base() -> None:
    base = _build_release_recommendation(api_tier="C", prediction_count=10, model_run_count=1)
    settings = Settings.model_construct(
        release_policy_override_enabled=True,
        release_policy_override_reason="temporary",
        release_policy_override_until=datetime.now(UTC) - timedelta(days=1),
    )
    out = apply_release_policy_override(base, settings=settings)
    assert out["status"] == "BLOCKED"


def test_override_does_not_change_go() -> None:
    base = _build_release_recommendation(api_tier="A", prediction_count=10, model_run_count=1)
    settings = Settings.model_construct(
        release_policy_override_enabled=True,
        release_policy_override_reason="should not apply",
        release_policy_override_until=None,
    )
    out = apply_release_policy_override(base, settings=settings)
    assert out["status"] == "GO"
