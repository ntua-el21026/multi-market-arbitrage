"""Deterministic grouping of repeated complete-set observations."""

from __future__ import annotations

from dataclasses import dataclass, replace

from multi_market_arbitrage.core.ids import (
    InstrumentId,
    MarketId,
    OpportunityId,
    OpportunityLifecycleId,
    StrategyId,
    StrategyVersion,
)
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.scanners.models import OpportunityLifecycleStatus


def complete_set_lifecycle_id(
    *,
    strategy_id: StrategyId,
    strategy_version: StrategyVersion,
    market_id: MarketId,
    instrument_ids: tuple[InstrumentId, ...],
) -> OpportunityLifecycleId:
    """Derive the versioned economic-dislocation key, independent of messages."""

    if len(instrument_ids) < 2:
        raise ValueError("complete-set lifecycle requires at least two instruments")
    ordered = tuple(sorted(instrument_ids, key=lambda item: item.value))
    return OpportunityLifecycleId.from_components(
        "polymarket-complete-set-lifecycle-v1",
        strategy_id,
        strategy_version,
        market_id,
        *(instrument.value for instrument in ordered),
    )


@dataclass(frozen=True, slots=True)
class OpportunityLifecycle:
    lifecycle_id: OpportunityLifecycleId
    first_seen: UtcTimestamp
    last_seen: UtcTimestamp
    observation_count: int
    status: OpportunityLifecycleStatus
    latest_opportunity_id: OpportunityId

    def __post_init__(self) -> None:
        if self.observation_count <= 0:
            raise ValueError("lifecycle observation count must be positive")
        if self.last_seen < self.first_seen:
            raise ValueError("lifecycle last_seen cannot precede first_seen")


class OpportunityLifecycleTracker:
    """Single-owner mutable lifecycle registry used by observation and replay."""

    def __init__(self) -> None:
        self._lifecycles: dict[OpportunityLifecycleId, OpportunityLifecycle] = {}

    def observe(
        self,
        *,
        lifecycle_id: OpportunityLifecycleId,
        opportunity_id: OpportunityId,
        observed_at: UtcTimestamp,
        status: OpportunityLifecycleStatus,
    ) -> OpportunityLifecycle:
        current = self._lifecycles.get(lifecycle_id)
        if current is None:
            updated = OpportunityLifecycle(
                lifecycle_id=lifecycle_id,
                first_seen=observed_at,
                last_seen=observed_at,
                observation_count=1,
                status=status,
                latest_opportunity_id=opportunity_id,
            )
        else:
            if observed_at < current.last_seen:
                raise ValueError("lifecycle observations must be applied chronologically")
            updated = replace(
                current,
                last_seen=observed_at,
                observation_count=current.observation_count + 1,
                status=status,
                latest_opportunity_id=opportunity_id,
            )
        self._lifecycles[lifecycle_id] = updated
        return updated

    def expire(
        self,
        lifecycle_id: OpportunityLifecycleId,
        *,
        expired_at: UtcTimestamp,
    ) -> OpportunityLifecycle:
        current = self._lifecycles[lifecycle_id]
        if expired_at < current.last_seen:
            raise ValueError("expiry cannot precede the latest observation")
        expired = replace(
            current,
            last_seen=expired_at,
            status=OpportunityLifecycleStatus.EXPIRED,
        )
        self._lifecycles[lifecycle_id] = expired
        return expired

    def snapshot(self) -> tuple[OpportunityLifecycle, ...]:
        return tuple(
            self._lifecycles[key] for key in sorted(self._lifecycles, key=lambda item: item.value)
        )


__all__ = [
    "OpportunityLifecycle",
    "OpportunityLifecycleTracker",
    "complete_set_lifecycle_id",
]
