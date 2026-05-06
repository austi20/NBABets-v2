"""Main Brain API: persistent learning coordinator for the NBA prop engine.

Bridges the SQLite store (structured data) with the Obsidian vault (human-readable
knowledge) so the autonomy system can learn from its corrections over time.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.config.settings import get_settings
from app.services.brain.contracts import (
    CorrectionOutcome,
    CorrectionRecord,
    MarketProfile,
    PredictionOutcome,
    StrategyMemory,
)
from app.services.brain.store import BrainStore
from app.services.brain.vault_bridge import (
    write_correction_note,
    write_daily_learning_summary,
    write_market_profile_note,
    write_strategy_note,
)

logger = logging.getLogger(__name__)

# Safety constants
MAX_WEIGHT_CHANGE_PCT = 0.20
MAX_CORRECTIONS_PER_RUN = 3
REVERT_AFTER_RUNS = 2
MIN_STRATEGY_SAMPLES = 5
MIN_STRATEGY_SUCCESS_RATE = 0.30
TRUSTED_STRATEGY_SUCCESS_RATE = 0.60


class Brain:
    """Persistent learning coordinator.

    Usage::

        brain = Brain()
        profile = brain.recall_market("rebounds")
        strategies = brain.recall_strategies("overfit", "rebounds")
        cid = brain.record_correction(record)
        brain.learn(cid, "improved", ece_after=0.08)
    """

    def __init__(self, db_path: Path | None = None, vault_root: Path | None = None) -> None:
        settings = get_settings()
        self._db_path = db_path or settings.brain_db_path
        self._vault_root = vault_root or settings.brain_vault_root
        self._store = BrainStore(self._db_path)

    def close(self) -> None:
        self._store.close()

    # -- Recall ---------------------------------------------------------------

    def recall_market(self, market: str) -> MarketProfile:
        """Return the learned profile for a market, or a blank default."""
        profile = self._store.recall_market(market)
        if profile is not None:
            return profile
        return MarketProfile(market=market)

    def recall_corrections(
        self,
        signal_type: str | None = None,
        market: str | None = None,
        limit: int = 20,
    ) -> list[CorrectionRecord]:
        return self._store.recall_corrections(signal_type, market, limit)

    def recall_strategies(
        self,
        problem_type: str | None = None,
        market: str | None = None,
        min_success_rate: float = 0.0,
    ) -> list[StrategyMemory]:
        return self._store.recall_strategies(problem_type, market, min_success_rate)

    def best_strategy(
        self,
        problem_type: str,
        market: str | None = None,
    ) -> StrategyMemory | None:
        """Return the highest-confidence strategy for a problem+market, or None."""
        strategies = self._store.recall_strategies(
            problem_type, market, min_success_rate=TRUSTED_STRATEGY_SUCCESS_RATE
        )
        if not strategies:
            return None
        # Prefer market-specific over global, then by success rate
        market_specific = [s for s in strategies if s.market == market]
        return market_specific[0] if market_specific else strategies[0]

    # -- Record ---------------------------------------------------------------

    def record_correction(self, record: CorrectionRecord) -> int:
        """Persist a correction and write it to the vault."""
        correction_id = self._store.store_correction(record)
        try:
            write_correction_note(
                correction_id=correction_id,
                signal_type=record.signal_type,
                action_type=record.action_type,
                market=record.market,
                params_before=record.params_before,
                params_after=record.params_after,
                ece_before=record.ece_before,
                outcome=record.outcome,
                vault_root=self._vault_root,
            )
        except Exception:
            logger.warning("Vault write failed for correction %d", correction_id, exc_info=True)
        return correction_id

    def record_outcome(self, outcome: PredictionOutcome) -> int:
        return self._store.store_outcome(outcome)

    # -- Learn ----------------------------------------------------------------

    def learn(
        self,
        correction_id: int,
        outcome: CorrectionOutcome,
        ece_after: float | None = None,
    ) -> None:
        """Close the loop: mark a correction's outcome and update strategies."""
        self._store.resolve_correction(correction_id, outcome, ece_after)

        # Find the correction to update strategy memory
        corrections = self._store.recall_corrections(limit=200)
        record = next((c for c in corrections if c.correction_id == correction_id), None)
        if record is None:
            return

        # Update strategy memory
        existing = self._store.recall_strategies(record.signal_type, record.market)
        matched = next(
            (s for s in existing if s.action_template == record.action_type),
            None,
        )

        improvement = (record.ece_before or 0) - (ece_after or record.ece_before or 0)
        is_success = outcome == "improved"

        if matched:
            new_count = matched.sample_count + 1
            new_successes = (matched.success_rate * matched.sample_count) + (1 if is_success else 0)
            new_avg = (matched.avg_ece_improvement * matched.sample_count + improvement) / new_count
            updated = StrategyMemory(
                strategy_id=matched.strategy_id,
                problem_type=matched.problem_type,
                action_template=matched.action_template,
                market=matched.market,
                parameters=record.params_after if is_success else matched.parameters,
                success_rate=new_successes / new_count,
                avg_ece_improvement=new_avg,
                sample_count=new_count,
                last_used_at=datetime.now(UTC),
            )
            self._store.upsert_strategy(updated)
        else:
            new_strategy = StrategyMemory(
                problem_type=record.signal_type,
                action_template=record.action_type,
                market=record.market,
                parameters=record.params_after,
                success_rate=1.0 if is_success else 0.0,
                avg_ece_improvement=improvement,
                sample_count=1,
                last_used_at=datetime.now(UTC),
            )
            self._store.upsert_strategy(new_strategy)

        # Update market profile
        if record.market:
            self._update_market_profile(record.market, ece_after, outcome)

        # Prune consistently failing strategies
        self._store.prune_weak_strategies(
            min_samples=MIN_STRATEGY_SAMPLES,
            max_success_rate=MIN_STRATEGY_SUCCESS_RATE,
        )

    def _update_market_profile(
        self,
        market: str,
        ece_after: float | None,
        outcome: CorrectionOutcome,
    ) -> None:
        profile = self.recall_market(market)
        ece_history = list(profile.ece_history)
        if ece_after is not None:
            ece_history.append((datetime.now(UTC).date().isoformat(), ece_after))

        corrections = self._store.recall_corrections(market=market)
        resolved = [c for c in corrections if c.outcome != "pending"]
        improved = [c for c in resolved if c.outcome == "improved"]
        avg_imp = (
            sum((c.ece_before or 0) - (c.ece_after or 0) for c in improved) / len(improved)
            if improved
            else 0.0
        )

        # Accumulate failure modes
        failure_modes = set(profile.failure_modes)
        if outcome == "worsened":
            failure_modes.add(f"{corrections[0].action_type if corrections else 'unknown'}_worsened")

        updated = MarketProfile(
            market=market,
            optimal_weights=profile.optimal_weights,
            calibration_strategy=profile.calibration_strategy,
            failure_modes=tuple(sorted(failure_modes)),
            ece_history=tuple(ece_history[-50:]),
            correction_count=len(resolved),
            avg_improvement=avg_imp,
        )
        self._store.save_market_profile(updated)

        # Sync to vault
        try:
            write_market_profile_note(
                market=market,
                ece_history=list(updated.ece_history),
                optimal_weights=updated.optimal_weights,
                calibration_strategy=updated.calibration_strategy,
                failure_modes=list(updated.failure_modes),
                correction_count=updated.correction_count,
                avg_improvement=updated.avg_improvement,
                vault_root=self._vault_root,
            )
        except Exception:
            logger.warning("Vault write failed for market profile %s", market, exc_info=True)

    # -- Feature Weight Overrides (delegated to store) ------------------------

    def get_weight_overrides(self, market: str | None = None) -> dict[str, dict[str, float]]:
        return self._store.get_weight_overrides(market)

    def set_weight_override(
        self,
        market: str,
        feature_name: str,
        scale_factor: float,
        reason: str = "",
        correction_id: int | None = None,
    ) -> None:
        # Enforce safety cap
        clamped = max(1.0 - MAX_WEIGHT_CHANGE_PCT, min(1.0 + MAX_WEIGHT_CHANGE_PCT, scale_factor))
        self._store.set_weight_override(market, feature_name, clamped, reason, correction_id)

    def deactivate_weight_override(self, market: str, feature_name: str) -> None:
        self._store.deactivate_weight_override(market, feature_name)

    # -- Outcomes (delegated) -------------------------------------------------

    def unresolved_outcomes(self, limit: int = 1000) -> list[PredictionOutcome]:
        return self._store.unresolved_outcomes(limit)

    def recall_outcomes(
        self,
        market: str | None = None,
        game_date: str | None = None,
        limit: int = 500,
    ) -> list[PredictionOutcome]:
        return self._store.recall_outcomes(market, game_date, limit)

    # -- Stats & Sync ---------------------------------------------------------

    def correction_stats(self) -> dict[str, Any]:
        return self._store.correction_stats()

    def sync_strategies_to_vault(self) -> int:
        """Write all active strategies to the Obsidian vault. Returns count."""
        strategies = self._store.recall_strategies(min_success_rate=0.0)
        count = 0
        for s in strategies:
            try:
                write_strategy_note(
                    problem_type=s.problem_type,
                    action_template=s.action_template,
                    market=s.market,
                    success_rate=s.success_rate,
                    avg_improvement=s.avg_ece_improvement,
                    sample_count=s.sample_count,
                    parameters=s.parameters,
                    vault_root=self._vault_root,
                )
                count += 1
            except Exception:
                logger.warning("Vault write failed for strategy %s", s.strategy_id, exc_info=True)
        return count

    def write_daily_summary(
        self,
        report_date: str,
        signals_found: int,
        corrections_planned: int,
        corrections_executed: int,
        dry_run: bool,
        notes: str = "",
    ) -> None:
        try:
            write_daily_learning_summary(
                report_date=report_date,
                signals_found=signals_found,
                corrections_planned=corrections_planned,
                corrections_executed=corrections_executed,
                dry_run=dry_run,
                notes=notes,
                vault_root=self._vault_root,
            )
        except Exception:
            logger.warning("Vault write failed for daily summary", exc_info=True)
