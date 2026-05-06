from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.trading.protocols import PortfolioLedger, RiskEngine
from app.trading.types import ExecutionIntent


@dataclass(frozen=True)
class RiskLimits:
    # NBA board has many opportunities; keep per-order sizing conservative.
    per_order_cap: float = 25.0
    per_market_cap: float = 75.0
    max_open_notional: float = 200.0
    daily_loss_cap: float = 120.0
    reject_cooldown_seconds: int = 120


class StaticRiskEngine(RiskEngine):
    def __init__(self, limits: RiskLimits | None = None) -> None:
        self._limits = limits or RiskLimits()

    @property
    def limits(self) -> RiskLimits:
        return self._limits

    def evaluate(self, intent: ExecutionIntent, ledger: PortfolioLedger) -> tuple[bool, str]:
        if float(intent.stake) <= 0.0:
            return False, "stake must be positive"
        if float(intent.stake) > self._limits.per_order_cap:
            return False, f"stake exceeds per-order cap ({self._limits.per_order_cap:.2f})"
        market_total = ledger.market_exposure(intent.market.symbol) + float(intent.stake)
        if market_total > self._limits.per_market_cap:
            return False, f"stake exceeds per-market cap ({self._limits.per_market_cap:.2f})"
        return True, "accepted"


class ExposureRiskEngine(RiskEngine):
    def __init__(self, limits: RiskLimits | None = None, *, killed: bool = False) -> None:
        self._static = StaticRiskEngine(limits)
        self._killed = bool(killed)
        self._cooldown_until: datetime | None = None

    @property
    def limits(self) -> RiskLimits:
        return self._static.limits

    @property
    def killed(self) -> bool:
        return self._killed

    def set_killed(self, killed: bool) -> None:
        self._killed = bool(killed)

    def evaluate(self, intent: ExecutionIntent, ledger: PortfolioLedger) -> tuple[bool, str]:
        now = datetime.now(UTC)
        if self._killed:
            return False, "kill switch is active"
        if self._cooldown_until is not None and now < self._cooldown_until:
            remain = int((self._cooldown_until - now).total_seconds())
            return False, f"reject cooldown active ({remain}s remaining)"
        static_ok, static_reason = self._static.evaluate(intent, ledger)
        if not static_ok:
            return self._reject(static_reason, now)
        open_notional = ledger.open_notional() + float(intent.stake)
        if open_notional > self.limits.max_open_notional:
            return self._reject(
                f"open exposure cap exceeded ({open_notional:.2f} > {self.limits.max_open_notional:.2f})",
                now,
            )
        if ledger.daily_realized_pnl() <= -self.limits.daily_loss_cap:
            return self._reject(
                f"daily loss cap reached ({ledger.daily_realized_pnl():.2f})",
                now,
            )
        return True, "accepted"

    def _reject(self, reason: str, now: datetime) -> tuple[bool, str]:
        self._cooldown_until = now + timedelta(seconds=max(0, self.limits.reject_cooldown_seconds))
        return False, reason
