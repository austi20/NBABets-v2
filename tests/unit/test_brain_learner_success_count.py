"""Tests for integer success_count and single profile update."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from app.services.brain.brain import Brain
from app.services.brain.contracts import CorrectionRecord
from app.services.brain.learner import learn_from_outcomes


def _make_brain(tmp_path: Path) -> Brain:
    return Brain(db_path=tmp_path / "brain.db", vault_root=tmp_path / "vault")


def _seed_correction(brain: Brain, *, market: str = "points") -> int:
    record = CorrectionRecord(
        signal_type="overfit",
        action_type="weight_adjust",
        market=market,
        ece_before=0.10,
        outcome="pending",
    )
    return brain.record_correction(record)


def test_success_count_integer_accumulates(tmp_path: Path) -> None:
    brain = _make_brain(tmp_path)

    # Record two corrections for the same problem+action+market
    cid1 = _seed_correction(brain, market="points")
    cid2 = _seed_correction(brain, market="points")

    brain.learn(cid1, "improved", ece_after=0.07)
    brain.learn(cid2, "worsened", ece_after=0.12)

    strategies = brain.recall_strategies("overfit", "points")
    assert strategies, "Expected at least one strategy"
    s = strategies[0]

    # success_count must be an exact integer (1 improved, 1 worsened)
    assert isinstance(s.success_count, int)
    assert s.success_count == 1
    assert s.sample_count == 2
    # success_rate derived from integer: 1/2 = 0.5
    assert abs(s.success_rate - 0.5) < 1e-9

    brain.close()


def test_no_double_profile_update(tmp_path: Path) -> None:
    brain = _make_brain(tmp_path)

    # learn_from_outcomes with no pending corrections and no outcomes
    # should NOT call _update_market_profile at all (no pending, no outcomes -> empty market_eces)
    with patch.object(brain, "_update_market_profile") as mock_update:
        learn_from_outcomes(brain)
        # No outcomes -> market_eces is empty -> no profile updates
        mock_update.assert_not_called()

    brain.close()
