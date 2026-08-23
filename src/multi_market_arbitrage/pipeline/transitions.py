"""Bounded evidence for venue-reported top transitions.

Reported top prices are integrity evidence only.  They never create, remove,
or resize canonical L2 levels; this module records the bounded uncertainty
window while quantity-bearing state catches up.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final

from multi_market_arbitrage.core.exact import Price
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    ConnectionId,
    InstrumentId,
    MarketId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.normalization import CanonicalEvent

REPORTED_TOP_TRANSITION_EXPIRY_SCHEMA: Final = "mma-reported-top-transition-expiry-v1"


class ReportedTopTransitionEntrySource(StrEnum):
    BEST_BID_ASK = "BEST_BID_ASK"
    POST_SNAPSHOT_CATCH_UP = "POST_SNAPSHOT_CATCH_UP"


class ReportedTopTransitionStatus(StrEnum):
    PENDING = "PENDING"
    CONVERGED = "CONVERGED"
    SUPERSEDED = "SUPERSEDED"
    AUTHORITATIVE_SNAPSHOT = "AUTHORITATIVE_SNAPSHOT"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"


class ReportedTopTransitionReason(StrEnum):
    BBO_AHEAD_OF_EXACT_L2 = "BBO_AHEAD_OF_EXACT_L2"
    POST_SNAPSHOT_PAIRED_CATCH_UP = "POST_SNAPSHOT_PAIRED_CATCH_UP"
    SAME_TARGET_EXACT_FRAME = "SAME_TARGET_EXACT_FRAME"
    EXACT_L2_CONVERGED = "EXACT_L2_CONVERGED"
    COMPLETE_REPORTED_TOP_SUPERSEDED = "COMPLETE_REPORTED_TOP_SUPERSEDED"
    AUTHORITATIVE_FULL_SNAPSHOT = "AUTHORITATIVE_FULL_SNAPSHOT"
    FRAME_BOUND_EXCEEDED = "FRAME_BOUND_EXCEEDED"
    TIME_BOUND_EXPIRED = "TIME_BOUND_EXPIRED"
    SOCKET_GENERATION_CHANGED = "SOCKET_GENERATION_CHANGED"
    CONFLICTING_REPORTED_TOP = "CONFLICTING_REPORTED_TOP"
    CROSSED_EXACT_BOOK = "CROSSED_EXACT_BOOK"
    REFERENCE_PRECISION_CHANGED = "REFERENCE_PRECISION_CHANGED"
    INVALID_EXACT_FRAME = "INVALID_EXACT_FRAME"
    DISCONNECT = "DISCONNECT"
    MARKET_RESOLVED = "MARKET_RESOLVED"
    UNRECOGNIZED_NO_PENDING = "UNRECOGNIZED_NO_PENDING"


@dataclass(frozen=True, slots=True)
class ReportedTopTransitionBaselineEvidence:
    """Complete ordinal-1 snapshot evidence bound by the catch-up recognizer."""

    connection_id: ConnectionId
    monotonic_epoch_id: str
    admission_ordinal: int
    ingest_seq: int
    received_at: UtcTimestamp
    received_monotonic_ns: int
    source_raw_refs: tuple[RawSourceRef, ...]
    source_canonical_event_ids: tuple[CanonicalEventId, ...]
    # (instrument, snapshot generation, exact depth/reference comparison hash,
    # best bid, best ask).  The comparison hash deliberately excludes
    # freshness/provenance rebasing by a matching REST audit.
    instrument_states: tuple[tuple[InstrumentId, int, str, Price | None, Price | None], ...]

    def __post_init__(self) -> None:
        if not self.monotonic_epoch_id:
            raise ValueError("reported-top baseline monotonic epoch must be non-empty")
        if min(self.admission_ordinal, self.ingest_seq, self.received_monotonic_ns) < 0:
            raise ValueError("reported-top baseline ordinals and times cannot be negative")
        if not self.source_raw_refs or not self.source_canonical_event_ids:
            raise ValueError("reported-top baseline requires raw and canonical sources")
        if len(self.source_raw_refs) > 4_096:
            raise ValueError("reported-top baseline raw identity exceeds its bound")
        if len(self.source_canonical_event_ids) > 2:
            raise ValueError("reported-top baseline canonical sources exceed two binary legs")
        if len(self.instrument_states) != 2:
            raise ValueError("reported-top baseline requires exactly two binary leg states")
        instrument_ids = {state[0] for state in self.instrument_states}
        if len(instrument_ids) != len(self.instrument_states):
            raise ValueError("reported-top baseline repeats an instrument")
        for (
            _instrument_id,
            generation,
            exact_state_sha256,
            _best_bid,
            _best_ask,
        ) in self.instrument_states:
            if generation < 0 or not is_sha256_hex(exact_state_sha256):
                raise ValueError("reported-top baseline state identity is invalid")


@dataclass(frozen=True, slots=True)
class ReportedTopTransitionCloseSource:
    """Envelope/canonical trigger that ended one transition."""

    raw_source: RawSourceRef
    ingest_seq: int
    channel: str
    connection_id: ConnectionId
    received_at: UtcTimestamp
    received_monotonic: MonotonicTimestamp
    admission_ordinal: int | None
    canonical_event_id: CanonicalEventId | None

    def __post_init__(self) -> None:
        if self.ingest_seq < 0 or not self.channel:
            raise ValueError("reported-top close source identity is invalid")
        if self.admission_ordinal is not None and self.admission_ordinal < 0:
            raise ValueError("reported-top close admission ordinal cannot be negative")


@dataclass(frozen=True, slots=True)
class ReportedTopTransitionExpiryCommand:
    """Durable decision that expires one exact bounded transition."""

    transition_id: str
    instrument_id: InstrumentId
    monotonic_epoch_id: str
    deadline_monotonic_ns: int
    decision_monotonic_ns: int

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.transition_id):
            raise ValueError("reported-top expiry transition ID must be SHA-256 hex")
        if not self.monotonic_epoch_id:
            raise ValueError("reported-top expiry monotonic epoch must be non-empty")
        if min(self.deadline_monotonic_ns, self.decision_monotonic_ns) < 0:
            raise ValueError("reported-top expiry monotonic times cannot be negative")
        if self.decision_monotonic_ns < self.deadline_monotonic_ns:
            raise ValueError("reported-top expiry decision precedes its deadline")

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "deadline_monotonic_ns": self.deadline_monotonic_ns,
                "decision_monotonic_ns": self.decision_monotonic_ns,
                "instrument_id": self.instrument_id.value,
                "monotonic_epoch_id": self.monotonic_epoch_id,
                "schema": REPORTED_TOP_TRANSITION_EXPIRY_SCHEMA,
                "transition_id": self.transition_id,
            }
        )


@dataclass(frozen=True, slots=True)
class ReportedTopTransition:
    """Current bounded uncertainty for exactly one canonical instrument."""

    transition_id: str
    instrument_id: InstrumentId
    market_id: MarketId
    target_best_bid: Price
    target_best_ask: Price
    starting_best_bid: Price | None
    starting_best_ask: Price | None
    starting_generation: int
    starting_economic_sha256: str
    first_source_raw_refs: tuple[RawSourceRef, ...]
    latest_source_raw_refs: tuple[RawSourceRef, ...]
    first_source_canonical_event_id: CanonicalEventId
    latest_source_canonical_event_id: CanonicalEventId
    connection_id: ConnectionId
    monotonic_epoch_id: str
    first_admission_ordinal: int
    latest_admission_ordinal: int
    first_ingest_seq: int
    latest_ingest_seq: int
    first_received_at: UtcTimestamp
    latest_received_at: UtcTimestamp
    first_received_monotonic_ns: int
    latest_received_monotonic_ns: int
    accepted_frame_count: int
    maximum_frames: int
    maximum_duration_ns: int
    entry_source: ReportedTopTransitionEntrySource
    baseline_evidence: ReportedTopTransitionBaselineEvidence | None
    status: ReportedTopTransitionStatus
    reason: ReportedTopTransitionReason

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.transition_id):
            raise ValueError("reported-top transition ID must be SHA-256 hex")
        if not is_sha256_hex(self.starting_economic_sha256):
            raise ValueError("reported-top starting economic identity must be SHA-256 hex")
        if self.starting_generation < 0:
            raise ValueError("reported-top starting generation cannot be negative")
        if self.target_best_bid.value >= self.target_best_ask.value:
            raise ValueError("reported-top target must be a non-crossed complete top")
        if not self.first_source_raw_refs or not self.latest_source_raw_refs:
            raise ValueError("reported-top transition requires first/latest raw identity")
        if max(len(self.first_source_raw_refs), len(self.latest_source_raw_refs)) > 4_096:
            raise ValueError("reported-top raw identity exceeds the event provenance bound")
        if not self.monotonic_epoch_id:
            raise ValueError("reported-top monotonic epoch must be non-empty")
        if (
            min(
                self.first_admission_ordinal,
                self.latest_admission_ordinal,
                self.first_ingest_seq,
                self.latest_ingest_seq,
                self.first_received_monotonic_ns,
                self.latest_received_monotonic_ns,
            )
            < 0
        ):
            raise ValueError("reported-top source ordinals and times cannot be negative")
        if self.latest_admission_ordinal < self.first_admission_ordinal:
            raise ValueError("reported-top admission ordinal regressed")
        if self.latest_ingest_seq < self.first_ingest_seq:
            raise ValueError("reported-top ingest sequence regressed")
        if self.latest_received_monotonic_ns < self.first_received_monotonic_ns:
            raise ValueError("reported-top monotonic time regressed")
        if self.accepted_frame_count <= 0 or self.maximum_frames <= 0:
            raise ValueError("reported-top frame counts must be positive")
        if self.accepted_frame_count > self.maximum_frames:
            raise ValueError("reported-top accepted frame count exceeds its hard bound")
        if self.maximum_duration_ns <= 0:
            raise ValueError("reported-top time bound must be positive")
        if (self.entry_source is ReportedTopTransitionEntrySource.POST_SNAPSHOT_CATCH_UP) != (
            self.baseline_evidence is not None
        ):
            raise ValueError("only post-snapshot transitions require recognizer baseline evidence")
        if self.status is not ReportedTopTransitionStatus.PENDING:
            raise ValueError("only pending reported-top transitions may remain current")

    @classmethod
    def start(
        cls,
        *,
        instrument_id: InstrumentId,
        market_id: MarketId,
        target_best_bid: Price,
        target_best_ask: Price,
        starting_best_bid: Price | None,
        starting_best_ask: Price | None,
        starting_generation: int,
        starting_economic_sha256: str,
        event: CanonicalEvent,
        connection_id: ConnectionId,
        admission_ordinal: int,
        ingest_seq: int,
        received_monotonic: MonotonicTimestamp,
        maximum_frames: int,
        maximum_duration_ns: int,
        entry_source: ReportedTopTransitionEntrySource,
        reason: ReportedTopTransitionReason,
        baseline_evidence: ReportedTopTransitionBaselineEvidence | None = None,
    ) -> ReportedTopTransition:
        identity = canonical_sha256(
            {
                "schema": "mma-reported-top-transition-id-v1",
                "instrument_id": instrument_id,
                "market_id": market_id,
                "target_best_bid": target_best_bid,
                "target_best_ask": target_best_ask,
                "starting_best_bid": starting_best_bid,
                "starting_best_ask": starting_best_ask,
                "starting_generation": starting_generation,
                "starting_economic_sha256": starting_economic_sha256,
                "source_event_id": event.canonical_event_id,
                "connection_id": connection_id,
                "admission_ordinal": admission_ordinal,
                "ingest_seq": ingest_seq,
                "received_at": event.receive_timestamp,
                "received_monotonic": received_monotonic,
                "maximum_frames": maximum_frames,
                "maximum_duration_ns": maximum_duration_ns,
                "entry_source": entry_source,
                "reason": reason,
                "baseline_evidence": baseline_evidence,
            }
        )
        return cls(
            transition_id=identity,
            instrument_id=instrument_id,
            market_id=market_id,
            target_best_bid=target_best_bid,
            target_best_ask=target_best_ask,
            starting_best_bid=starting_best_bid,
            starting_best_ask=starting_best_ask,
            starting_generation=starting_generation,
            starting_economic_sha256=starting_economic_sha256,
            first_source_raw_refs=event.raw_source_refs,
            latest_source_raw_refs=event.raw_source_refs,
            first_source_canonical_event_id=event.canonical_event_id,
            latest_source_canonical_event_id=event.canonical_event_id,
            connection_id=connection_id,
            monotonic_epoch_id=received_monotonic.epoch_id,
            first_admission_ordinal=admission_ordinal,
            latest_admission_ordinal=admission_ordinal,
            first_ingest_seq=ingest_seq,
            latest_ingest_seq=ingest_seq,
            first_received_at=event.receive_timestamp,
            latest_received_at=event.receive_timestamp,
            first_received_monotonic_ns=received_monotonic.nanoseconds,
            latest_received_monotonic_ns=received_monotonic.nanoseconds,
            accepted_frame_count=1,
            maximum_frames=maximum_frames,
            maximum_duration_ns=maximum_duration_ns,
            entry_source=entry_source,
            baseline_evidence=baseline_evidence,
            status=ReportedTopTransitionStatus.PENDING,
            reason=reason,
        )

    @property
    def deadline_monotonic_ns(self) -> int:
        return self.first_received_monotonic_ns + self.maximum_duration_ns

    def advance(
        self,
        *,
        event: CanonicalEvent,
        admission_ordinal: int,
        ingest_seq: int,
        received_monotonic: MonotonicTimestamp,
        reason: ReportedTopTransitionReason,
    ) -> ReportedTopTransition:
        if received_monotonic.epoch_id != self.monotonic_epoch_id:
            raise ValueError("reported-top transition changed monotonic epoch")
        return replace(
            self,
            latest_source_raw_refs=event.raw_source_refs,
            latest_source_canonical_event_id=event.canonical_event_id,
            latest_admission_ordinal=admission_ordinal,
            latest_ingest_seq=ingest_seq,
            latest_received_at=event.receive_timestamp,
            latest_received_monotonic_ns=received_monotonic.nanoseconds,
            accepted_frame_count=self.accepted_frame_count + 1,
            reason=reason,
        )

    def health_record(self, *, now: MonotonicTimestamp) -> dict[str, object]:
        same_epoch = now.epoch_id == self.monotonic_epoch_id
        age_ns = (
            now.nanoseconds - self.first_received_monotonic_ns
            if same_epoch and now.nanoseconds >= self.first_received_monotonic_ns
            else None
        )
        return {
            "accepted_frame_count": self.accepted_frame_count,
            "age_nanoseconds": age_ns,
            "baseline_evidence_sha256": (
                None if self.baseline_evidence is None else canonical_sha256(self.baseline_evidence)
            ),
            "deadline_monotonic_ns": self.deadline_monotonic_ns,
            "entry_source": self.entry_source.value,
            "first_admission_ordinal": self.first_admission_ordinal,
            "first_ingest_seq": self.first_ingest_seq,
            "frames_remaining": self.maximum_frames - self.accepted_frame_count,
            "latest_admission_ordinal": self.latest_admission_ordinal,
            "latest_ingest_seq": self.latest_ingest_seq,
            "maximum_duration_ns": self.maximum_duration_ns,
            "maximum_frames": self.maximum_frames,
            "monotonic_epoch_id": self.monotonic_epoch_id,
            "reason": self.reason.value,
            "status": self.status.value,
            "target_best_ask": str(self.target_best_ask),
            "target_best_bid": str(self.target_best_bid),
            "transition_id": self.transition_id,
        }


@dataclass(frozen=True, slots=True)
class ReportedTopTransitionOutcome:
    transition: ReportedTopTransition
    transition_id: str
    instrument_id: InstrumentId
    market_id: MarketId
    entry_source: ReportedTopTransitionEntrySource
    status: ReportedTopTransitionStatus
    reason: ReportedTopTransitionReason
    connection_id: ConnectionId
    monotonic_epoch_id: str
    first_ingest_seq: int
    latest_ingest_seq: int
    accepted_frame_count: int
    duration_ns: int
    duration_is_lower_bound: bool
    ended_at: UtcTimestamp
    ended_monotonic_epoch_id: str
    ended_monotonic_ns: int
    first_source_raw_refs: tuple[RawSourceRef, ...]
    latest_source_raw_refs: tuple[RawSourceRef, ...]
    close_source: ReportedTopTransitionCloseSource | None
    research_eligibility_restored: bool

    @classmethod
    def close(
        cls,
        transition: ReportedTopTransition,
        *,
        status: ReportedTopTransitionStatus,
        reason: ReportedTopTransitionReason,
        ended_monotonic_ns: int,
        research_eligibility_restored: bool,
        ended_at: UtcTimestamp | None = None,
        ended_monotonic_epoch_id: str | None = None,
        duration_is_lower_bound: bool = False,
        close_source: ReportedTopTransitionCloseSource | None = None,
    ) -> ReportedTopTransitionOutcome:
        if status is ReportedTopTransitionStatus.PENDING:
            raise ValueError("reported-top outcome cannot remain pending")
        actual_epoch_id = (
            transition.monotonic_epoch_id
            if ended_monotonic_epoch_id is None
            else ended_monotonic_epoch_id
        )
        if not actual_epoch_id or ended_monotonic_ns < 0:
            raise ValueError("reported-top outcome monotonic identity is invalid")
        same_epoch = actual_epoch_id == transition.monotonic_epoch_id
        if same_epoch and ended_monotonic_ns < transition.first_received_monotonic_ns:
            raise ValueError("reported-top outcome precedes transition start")
        if not same_epoch and not duration_is_lower_bound:
            raise ValueError("cross-epoch reported-top duration must be marked lower-bound")
        duration_ns = (
            ended_monotonic_ns - transition.first_received_monotonic_ns
            if same_epoch
            else transition.latest_received_monotonic_ns - transition.first_received_monotonic_ns
        )
        return cls(
            transition=transition,
            transition_id=transition.transition_id,
            instrument_id=transition.instrument_id,
            market_id=transition.market_id,
            entry_source=transition.entry_source,
            status=status,
            reason=reason,
            connection_id=transition.connection_id,
            monotonic_epoch_id=transition.monotonic_epoch_id,
            first_ingest_seq=transition.first_ingest_seq,
            latest_ingest_seq=transition.latest_ingest_seq,
            accepted_frame_count=transition.accepted_frame_count,
            duration_ns=duration_ns,
            duration_is_lower_bound=duration_is_lower_bound,
            ended_at=transition.latest_received_at if ended_at is None else ended_at,
            ended_monotonic_epoch_id=actual_epoch_id,
            ended_monotonic_ns=ended_monotonic_ns,
            first_source_raw_refs=transition.first_source_raw_refs,
            latest_source_raw_refs=transition.latest_source_raw_refs,
            close_source=close_source,
            research_eligibility_restored=research_eligibility_restored,
        )


__all__ = [
    "REPORTED_TOP_TRANSITION_EXPIRY_SCHEMA",
    "ReportedTopTransition",
    "ReportedTopTransitionBaselineEvidence",
    "ReportedTopTransitionCloseSource",
    "ReportedTopTransitionEntrySource",
    "ReportedTopTransitionExpiryCommand",
    "ReportedTopTransitionOutcome",
    "ReportedTopTransitionReason",
    "ReportedTopTransitionStatus",
]
