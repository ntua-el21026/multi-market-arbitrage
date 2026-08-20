"""Single-owner in-memory bitemporal canonical reference registry."""

from __future__ import annotations

from dataclasses import dataclass, replace

from multi_market_arbitrage.core.ids import MarketId, VersionId
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import UtcTimestamp

from .models import (
    BitemporalFields,
    CostRuleSet,
    InstrumentReference,
    MarketReference,
    PrecisionRule,
    ReferenceBundle,
)

type VersionedReference = MarketReference | InstrumentReference | PrecisionRule | CostRuleSet


def _semantic_content_hash(value: VersionedReference) -> str:
    # Knowledge/effective observation fields are bitemporal record metadata,
    # not part of an already-derived semantic version identity.
    neutral_times = BitemporalFields(observed_at=UtcTimestamp(0), effective_from=None)
    return canonical_sha256(replace(value, times=neutral_times))


@dataclass(frozen=True, slots=True)
class ReferenceObservation:
    bundle: ReferenceBundle
    observed_at: UtcTimestamp
    superseded_at: UtcTimestamp | None

    def known_at(self, timestamp: UtcTimestamp) -> bool:
        return self.observed_at <= timestamp and (
            self.superseded_at is None or timestamp < self.superseded_at
        )


class ReferenceRegistry:
    """Registry supporting current and as-known-at-time lookups without look-ahead."""

    def __init__(self) -> None:
        self._observations: dict[MarketId, tuple[ReferenceObservation, ...]] = {}
        self._versions: dict[VersionId, VersionedReference] = {}
        self._version_content_hashes: dict[VersionId, str] = {}

    def register(self, bundle: ReferenceBundle) -> ReferenceObservation:
        observed_at = max(
            bundle.market.times.observed_at,
            bundle.precision.times.observed_at,
            bundle.cost_rule_set.times.observed_at,
            *(instrument.times.observed_at for instrument in bundle.instruments),
        )
        existing = self._observations.get(bundle.market.market_id, ())
        if existing and observed_at <= existing[-1].observed_at:
            raise ValueError("reference observations must advance strictly in knowledge time")
        if existing:
            closed_previous = replace(existing[-1], superseded_at=observed_at)
            existing = (*existing[:-1], closed_previous)
        observation = ReferenceObservation(
            bundle=bundle,
            observed_at=observed_at,
            superseded_at=None,
        )
        self._observations[bundle.market.market_id] = (*existing, observation)
        versioned_objects: tuple[tuple[VersionId, VersionedReference], ...] = (
            (bundle.market.version_id, bundle.market),
            *((instrument.version_id, instrument) for instrument in bundle.instruments),
            (bundle.precision.version_id, bundle.precision),
            (bundle.cost_rule_set.version_id, bundle.cost_rule_set),
        )
        for version_id, value in versioned_objects:
            content_hash = _semantic_content_hash(value)
            previous_hash = self._version_content_hashes.get(version_id)
            if previous_hash is not None and previous_hash != content_hash:
                raise ValueError("one version ID mapped to different immutable contents")
            if previous_hash is None:
                self._versions[version_id] = value
                self._version_content_hashes[version_id] = content_hash
        return observation

    def current(self, market_id: MarketId) -> ReferenceBundle | None:
        values = self._observations.get(market_id, ())
        return None if not values else values[-1].bundle

    def as_known_at(self, market_id: MarketId, known_at: UtcTimestamp) -> ReferenceBundle | None:
        values = self._observations.get(market_id, ())
        for observation in reversed(values):
            if observation.known_at(known_at):
                return observation.bundle
        return None

    def effective_at(
        self,
        market_id: MarketId,
        *,
        effective_at: UtcTimestamp,
        known_at: UtcTimestamp,
    ) -> ReferenceBundle | None:
        candidate = self.as_known_at(market_id, known_at)
        if candidate is None:
            return None
        fields = candidate.market.times
        if fields.effective_from is not None and effective_at < fields.effective_from:
            return None
        if fields.effective_to is not None and effective_at >= fields.effective_to:
            return None
        return candidate

    def by_version(self, version_id: VersionId) -> VersionedReference | None:
        return self._versions.get(version_id)

    def observations(self, market_id: MarketId) -> tuple[ReferenceObservation, ...]:
        return self._observations.get(market_id, ())


__all__ = ["ReferenceObservation", "ReferenceRegistry"]
