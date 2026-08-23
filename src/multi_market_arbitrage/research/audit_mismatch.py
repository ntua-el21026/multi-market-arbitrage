"""Bounded, read-only causal classification of REST audit mismatches.

The live processor deliberately classifies only from evidence available at the
fenced barrier.  This module is a post-hoc research surface: it joins those
immutable mismatch observations to the verified raw journal and asks whether
an exact venue state was observed before or after the audit window.

Three complete journal passes are intentional.  They avoid retaining a
history-sized WebSocket index while still binding classification controls,
REST responses, audit starts, and exact WebSocket mutations deterministically.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    SubscriptionId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalReader
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_CLASSIFICATION_SCHEMA,
    AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE,
    AUDIT_MISMATCH_MAX_RAW_SOURCES,
    AUDIT_START_V1_SCHEMA,
    AUDIT_START_V2_SCHEMA,
    AuditBookIdentity,
    AuditExactDepthDifference,
    AuditExactDepthIdentity,
    AuditMismatchCause,
    AuditRawSourceSummary,
    AuditTriggerReason,
    audit_exact_depth_identity,
    compare_audit_exact_depth,
    summarize_audit_raw_sources,
)
from multi_market_arbitrage.pipeline.channels import (
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_START,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
)
from multi_market_arbitrage.pipeline.transitions import ReportedTopTransition
from multi_market_arbitrage.state.order_books.models import BookLevel
from multi_market_arbitrage.venues.polymarket.errors import PolymarketAdapterError
from multi_market_arbitrage.venues.polymarket.schemas import (
    NativeBookLevel,
    NativeSide,
    RestBookSnapshot,
    WsBookEvent,
    WsPriceChangeEvent,
    WsTickSizeChangeEvent,
    parse_market_ws_message,
    parse_rest_book,
)

AUDIT_MISMATCH_LINEAGE_REPORT_SCHEMA = "phase1-audit-mismatch-lineage-report-v2"
_JOURNAL_IDENTITY_DOMAIN = b"mma-audit-mismatch-journal-identity-v2\0"
_RESULT_SEQUENCE_DOMAIN = b"mma-audit-mismatch-result-sequence-v2\0"


class _Digest(Protocol):
    def update(self, data: bytes, /) -> None: ...


class AuditMismatchReportError(ValueError):
    """The requested post-hoc report cannot be represented safely."""


class AuditMismatchReportResourceError(AuditMismatchReportError):
    """An explicit post-hoc analysis resource bound was exceeded."""


class AuditMismatchLineageReason(StrEnum):
    """Finite evidence reasons behind a post-hoc cause classification."""

    FUTURE_VENUE_STATE = "FUTURE_VENUE_STATE"
    PRIOR_STATE_EXACTLY_RECONCILED = "PRIOR_STATE_EXACTLY_RECONCILED"
    LATEST_PRIOR_STATE_DIVERGENCE = "LATEST_PRIOR_STATE_DIVERGENCE"
    PRIOR_STATE_RESIDUAL_DIVERGENCE = "PRIOR_STATE_RESIDUAL_DIVERGENCE"
    PRIOR_STATE_COARSE_RECONCILIATION_UNPROVEN = "PRIOR_STATE_COARSE_RECONCILIATION_UNPROVEN"
    VENUE_IDENTITY_UNAVAILABLE = "VENUE_IDENTITY_UNAVAILABLE"
    VENUE_STATE_NOT_FOUND = "VENUE_STATE_NOT_FOUND"
    AMBIGUOUS_VENUE_STATE_ORDER = "AMBIGUOUS_VENUE_STATE_ORDER"
    EXACT_LINEAGE_BOUND_EXCEEDED = "EXACT_LINEAGE_BOUND_EXCEEDED"
    OBSERVATION_REST_IDENTITY_MISMATCH = "OBSERVATION_REST_IDENTITY_MISMATCH"
    ACTIVE_REPORTED_TOP_TRANSITION = "ACTIVE_REPORTED_TOP_TRANSITION"
    FUTURE_REFERENCE_PRECISION_TRANSITION = "FUTURE_REFERENCE_PRECISION_TRANSITION"
    RECONSTRUCTED_LOCAL_BOOK_DEFECT = "RECONSTRUCTED_LOCAL_BOOK_DEFECT"
    SCALAR_MISMATCH_CAUSE_UNPROVEN = "SCALAR_MISMATCH_CAUSE_UNPROVEN"
    SAME_GENERATION_EXACT_CONVERGENCE = "SAME_GENERATION_EXACT_CONVERGENCE"
    SAME_GENERATION_CONFLICT = "SAME_GENERATION_CONFLICT"
    RECOVERY_GENERATION_EXACT_MATCH = "RECOVERY_GENERATION_EXACT_MATCH"
    PRIOR_EXACT_STATE_MATCH = "PRIOR_EXACT_STATE_MATCH"
    PERSISTENT_EXACT_DIVERGENCE = "PERSISTENT_EXACT_DIVERGENCE"
    CAUSAL_GENERATION_AMBIGUOUS = "CAUSAL_GENERATION_AMBIGUOUS"


class AuditVenueStateRelation(StrEnum):
    """Journal-order relation of the matching venue state to the audit."""

    PRIOR = "PRIOR"
    FUTURE = "FUTURE"
    NONE = "NONE"
    AMBIGUOUS = "AMBIGUOUS"


class AuditMismatchCausalRelation(StrEnum):
    """Generation-aware causal relation of exact venue evidence to a mismatch."""

    SAME_GENERATION_NATURAL_CONVERGENCE = "SAME_GENERATION_NATURAL_CONVERGENCE"
    SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE = "SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE"
    RECOVERY_GENERATION_MATCH = "RECOVERY_GENERATION_MATCH"
    PRIOR_STATE_MATCH = "PRIOR_STATE_MATCH"
    PERSISTENT_DIVERGENCE = "PERSISTENT_DIVERGENCE"
    REFERENCE_TRANSITION = "REFERENCE_TRANSITION"
    LOCAL_BOOK_DEFECT = "LOCAL_BOOK_DEFECT"
    AMBIGUOUS = "AMBIGUOUS"
    NOT_FOUND = "NOT_FOUND"


class AuditMatchingStateRepresentation(StrEnum):
    """How an exact matching WebSocket state was established."""

    FULL_SNAPSHOT = "FULL_SNAPSHOT"
    RECONSTRUCTED_INCREMENTALS = "RECONSTRUCTED_INCREMENTALS"


class AuditEqualityProof(StrEnum):
    """Strength of equality evidence retained by one lineage result."""

    FULL_DEPTH_EXACT = "FULL_DEPTH_EXACT"
    COARSE_ONLY = "COARSE_ONLY"
    NOT_PROVEN = "NOT_PROVEN"


class AuditRecoveryEvidenceStatus(StrEnum):
    """Provenance strength for one recovery chronology stage."""

    RAW_JOURNAL = "RAW_JOURNAL"
    RAW_JOURNAL_INFERENCE = "RAW_JOURNAL_INFERENCE"
    DETERMINISTIC_REPLAY = "DETERMINISTIC_REPLAY"
    NOT_RETAINED = "NOT_RETAINED"


@dataclass(frozen=True, slots=True)
class AuditMismatchReportLimits:
    """Hard CPU, input, state, and output bounds for a lineage report."""

    maximum_observations: int = 4_096
    maximum_envelopes_per_pass: int = 5_000_000
    maximum_payload_bytes: int = 16 * 1024 * 1024
    maximum_total_payload_bytes_per_pass: int = 64 * 1024 * 1024 * 1024
    maximum_total_retained_level_values: int = 2_000_000
    maximum_exact_frames_per_observation: int = 8_192
    maximum_exact_action_comparisons: int = 50_000_000
    maximum_artifact_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        values = (
            self.maximum_observations,
            self.maximum_envelopes_per_pass,
            self.maximum_payload_bytes,
            self.maximum_total_payload_bytes_per_pass,
            self.maximum_total_retained_level_values,
            self.maximum_exact_frames_per_observation,
            self.maximum_exact_action_comparisons,
            self.maximum_artifact_bytes,
        )
        if min(values) <= 0:
            raise AuditMismatchReportResourceError(
                "audit-mismatch report resource limits must be positive"
            )
        if self.maximum_exact_frames_per_observation > AUDIT_MISMATCH_MAX_RAW_SOURCES - 1:
            raise AuditMismatchReportResourceError(
                "exact-frame bound exceeds the bounded audit raw-source contract"
            )


@dataclass(frozen=True, slots=True)
class AuditMismatchDifferenceCounts:
    """Legacy-compatible coarse difference evidence from a replay artifact."""

    bid_difference_count: int
    ask_difference_count: int
    local_only_count: int
    rest_only_count: int
    quantity_difference_count: int
    top_changed: bool | None = None

    def __post_init__(self) -> None:
        values = (
            self.bid_difference_count,
            self.ask_difference_count,
            self.local_only_count,
            self.rest_only_count,
            self.quantity_difference_count,
        )
        if min(values) < 0:
            raise AuditMismatchReportError("mismatch difference counts cannot be negative")
        if self.difference_count != (
            self.local_only_count + self.rest_only_count + self.quantity_difference_count
        ):
            raise AuditMismatchReportError("mismatch difference counts do not reconcile")

    @property
    def difference_count(self) -> int:
        return self.bid_difference_count + self.ask_difference_count

    @classmethod
    def from_exact(
        cls,
        difference: AuditExactDepthDifference,
        *,
        top_changed: bool | None = None,
    ) -> AuditMismatchDifferenceCounts:
        return cls(
            bid_difference_count=difference.bid_difference_count,
            ask_difference_count=difference.ask_difference_count,
            local_only_count=difference.local_only_count,
            rest_only_count=difference.rest_only_count,
            quantity_difference_count=difference.quantity_difference_count,
            top_changed=top_changed,
        )


@dataclass(frozen=True, slots=True)
class AuditMismatchObservation:
    """One fenced replay MISMATCH supplied to the post-hoc raw join.

    ``exact_depth_difference`` is required to prove exact lineage.  A fenced
    MISMATCH may have zero level differences when only reference scalars differ.
    Historical coarse artifacts remain accepted, but equal aggregate counts
    alone never upgrade a case to a causal category.
    """

    classification_ingest_seq: int
    instrument_id: InstrumentId
    difference_counts: AuditMismatchDifferenceCounts
    exact_depth_difference: AuditExactDepthDifference | None = None
    native_asset_id: str | None = None
    classification_source: RawSourceRef | None = None
    rest_venue_hash: str | None = None
    rest_timestamp_raw: str | None = None
    local_tick_size: ExactDecimal | None = None
    reported_top_transition: ReportedTopTransition | None = None

    def __post_init__(self) -> None:
        if self.classification_ingest_seq < 0:
            raise AuditMismatchReportError("classification ingest sequence cannot be negative")
        if self.native_asset_id is not None and not self.native_asset_id:
            raise AuditMismatchReportError("native asset ID cannot be empty")
        if self.rest_venue_hash is not None and len(self.rest_venue_hash) > 512:
            raise AuditMismatchReportError("REST venue hash exceeds its bound")
        if self.rest_timestamp_raw is not None and len(self.rest_timestamp_raw) > 128:
            raise AuditMismatchReportError("REST timestamp exceeds its bound")
        if self.local_tick_size is not None and self.local_tick_size <= ExactDecimal.zero():
            raise AuditMismatchReportError("local tick size must be positive")
        transition = self.reported_top_transition
        if transition is not None:
            if transition.instrument_id != self.instrument_id:
                raise AuditMismatchReportError(
                    "reported-top transition belongs to another instrument"
                )
            if transition.latest_ingest_seq >= self.classification_ingest_seq:
                raise AuditMismatchReportError(
                    "reported-top transition does not precede mismatch classification"
                )
        if self.exact_depth_difference is not None:
            exact_counts = AuditMismatchDifferenceCounts.from_exact(
                self.exact_depth_difference,
                top_changed=self.difference_counts.top_changed,
            )
            if exact_counts != self.difference_counts:
                raise AuditMismatchReportError(
                    "coarse and exact mismatch difference evidence disagree"
                )

    @classmethod
    def from_exact(
        cls,
        *,
        classification_ingest_seq: int,
        instrument_id: InstrumentId,
        exact_depth_difference: AuditExactDepthDifference,
        native_asset_id: str | None = None,
        classification_source: RawSourceRef | None = None,
        rest_venue_hash: str | None = None,
        rest_timestamp_raw: str | None = None,
        top_changed: bool | None = None,
        local_tick_size: ExactDecimal | None = None,
        reported_top_transition: ReportedTopTransition | None = None,
    ) -> AuditMismatchObservation:
        return cls(
            classification_ingest_seq=classification_ingest_seq,
            instrument_id=instrument_id,
            difference_counts=AuditMismatchDifferenceCounts.from_exact(
                exact_depth_difference,
                top_changed=top_changed,
            ),
            exact_depth_difference=exact_depth_difference,
            native_asset_id=native_asset_id,
            classification_source=classification_source,
            rest_venue_hash=rest_venue_hash,
            rest_timestamp_raw=rest_timestamp_raw,
            local_tick_size=local_tick_size,
            reported_top_transition=reported_top_transition,
        )


@dataclass(frozen=True, slots=True)
class AuditMismatchCauseCount:
    cause: AuditMismatchCause
    count: int


@dataclass(frozen=True, slots=True)
class AuditRecoveryStageEvidence:
    """One explicitly sourced or explicitly absent recovery chronology stage."""

    status: AuditRecoveryEvidenceStatus
    reason: str
    ingest_seq: int | None = None
    source: RawSourceRef | None = None
    connection_id: ConnectionId | None = None
    subscription_id: SubscriptionId | None = None
    socket_generation: int | None = None
    admission_ordinal: int | None = None
    received_monotonic_ns: int | None = None
    event_type: str | None = None
    exact_depth_identity: AuditExactDepthIdentity | None = None

    def __post_init__(self) -> None:
        if not self.reason or len(self.reason) > 512:
            raise AuditMismatchReportError("recovery-stage reason must be non-empty bounded text")
        if self.ingest_seq is not None and self.ingest_seq < 0:
            raise AuditMismatchReportError("recovery-stage ingest sequence cannot be negative")
        if self.socket_generation is not None and self.socket_generation <= 0:
            raise AuditMismatchReportError("recovery-stage socket generation must be positive")
        if self.admission_ordinal is not None and self.admission_ordinal < 0:
            raise AuditMismatchReportError("recovery-stage admission ordinal cannot be negative")
        if self.received_monotonic_ns is not None and self.received_monotonic_ns < 0:
            raise AuditMismatchReportError("recovery-stage monotonic time cannot be negative")
        if self.event_type is not None and (not self.event_type or len(self.event_type) > 128):
            raise AuditMismatchReportError("recovery-stage event type must be bounded text")
        retained = (
            self.ingest_seq,
            self.source,
            self.connection_id,
            self.subscription_id,
            self.socket_generation,
            self.admission_ordinal,
            self.received_monotonic_ns,
            self.event_type,
            self.exact_depth_identity,
        )
        if self.status is AuditRecoveryEvidenceStatus.NOT_RETAINED and any(
            item is not None for item in retained
        ):
            raise AuditMismatchReportError("absent recovery evidence cannot carry a source")


@dataclass(frozen=True, slots=True)
class AuditRecoveryChronology:
    """Causal recovery stages retained without inventing absent historical facts."""

    mismatch_classification: AuditRecoveryStageEvidence
    local_invalidation: AuditRecoveryStageEvidence
    research_audit_ineligibility: AuditRecoveryStageEvidence
    generation_termination: AuditRecoveryStageEvidence
    resync_initiation: AuditRecoveryStageEvidence
    rest_resnapshot: AuditRecoveryStageEvidence
    replacement_websocket_generation: AuditRecoveryStageEvidence
    recovery_completion: AuditRecoveryStageEvidence
    research_eligibility_restoration: AuditRecoveryStageEvidence


@dataclass(frozen=True, slots=True)
class AuditMismatchLineageResult:
    classification_ingest_seq: int
    instrument_id: InstrumentId
    native_asset_id: str
    attempt: int
    cause: AuditMismatchCause
    reason: AuditMismatchLineageReason
    venue_state_relation: AuditVenueStateRelation
    start_ingest_seq: int
    response_ingest_seq: int
    classification_source: RawSourceRef
    start_source: RawSourceRef
    response_source: RawSourceRef
    matching_websocket_source: RawSourceRef | None
    matching_websocket_ingest_seq: int | None
    rest_venue_hash: str
    rest_timestamp_raw: str
    subsequent_exact_frame_count: int
    subsequent_raw_sources: AuditRawSourceSummary
    observed_difference: AuditMismatchDifferenceCounts
    reconstructed_difference: AuditMismatchDifferenceCounts | None
    exact_reconciliation_proven: bool
    request_id: str
    causal_relation: AuditMismatchCausalRelation
    start_connection_id: ConnectionId
    start_socket_generation: int | None
    start_admission_ordinal: int
    start_processed_ordinal: int
    starting_book: AuditBookIdentity
    starting_exact_depth_identity: AuditExactDepthIdentity | None
    response_boundary_admission_ordinal: int | None
    barrier_processed_ordinal: int | None
    barrier_exact_depth_identity: AuditExactDepthIdentity | None
    normalized_rest_exact_depth_identity: AuditExactDepthIdentity
    exact_mismatch_difference_sha256: str | None
    request_started_monotonic_ns: int
    response_received_monotonic_ns: int
    classification_monotonic_ns: int
    classification_received_monotonic_ns: int
    matching_websocket_subordinal: int | None
    matching_connection_id: ConnectionId | None
    matching_subscription_id: SubscriptionId | None
    matching_socket_generation: int | None
    matching_admission_ordinal: int | None
    matching_event_type: str | None
    matching_state_representation: AuditMatchingStateRepresentation | None
    matching_exact_depth_identity: AuditExactDepthIdentity | None
    matching_venue_hash_diagnostic: str | None
    matching_venue_timestamp_diagnostic: str | None
    matching_source_chain: AuditRawSourceSummary
    matching_exact_frame_count: int
    matching_received_monotonic_ns: int | None
    response_to_match_monotonic_ns: int | None
    classification_to_match_monotonic_ns: int | None
    equality_proof: AuditEqualityProof
    venue_hash_diagnostic_equal: bool | None
    venue_timestamp_diagnostic_equal: bool | None
    recovery_chronology: AuditRecoveryChronology
    unresolved_evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AuditMismatchReportResources:
    journal_passes: int
    source_envelopes: int
    source_payload_bytes: int
    exact_actions_considered: int
    exact_action_comparisons: int
    peak_retained_level_values: int


@dataclass(frozen=True, slots=True)
class AuditMismatchLineageReport:
    schema_version: str
    collector_run_id: CollectorRunId
    source_journal_event_count: int
    source_journal_identity_sha256: str
    observation_count: int
    cause_counts: tuple[AuditMismatchCauseCount, ...]
    results: tuple[AuditMismatchLineageResult, ...]
    result_sequence_sha256: str
    resources: AuditMismatchReportResources

    @property
    def report_sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class _PassSummary:
    event_count: int
    payload_bytes: int
    identity_sha256: str


@dataclass(slots=True)
class _ResolvedObservation:
    observation: AuditMismatchObservation
    classification_source: RawSourceRef | None = None
    classification_connection: str | None = None
    classification_subscription_id: SubscriptionId | None = None
    request_id: str | None = None
    response_ingest_seq: int | None = None
    start_ingest_seq: int | None = None
    start_source: RawSourceRef | None = None
    response_source: RawSourceRef | None = None
    native_asset_id: str | None = None
    attempt: int | None = None
    rest: RestBookSnapshot | None = None
    rest_bids: tuple[BookLevel, ...] = ()
    rest_asks: tuple[BookLevel, ...] = ()
    rest_depth_identity: AuditExactDepthIdentity | None = None
    rest_identity_mismatch: bool = False
    reported_transition_first_source: RawSourceRef | None = None
    reported_transition_latest_source: RawSourceRef | None = None
    request_started_monotonic_ns: int | None = None
    classification_monotonic_ns: int | None = None
    classification_received_monotonic_ns: int | None = None
    response_received_monotonic_ns: int | None = None
    starting_admission_ordinal: int | None = None
    starting_processed_ordinal: int | None = None
    starting_book: AuditBookIdentity | None = None
    response_boundary_ordinal: int | None = None
    barrier_processed_ordinal: int | None = None
    start_socket_generation: int | None = None
    first_rest_resnapshot: _RecoveryRawStage | None = None
    first_replacement_websocket: _RecoveryRawStage | None = None


@dataclass(frozen=True, slots=True)
class _RecoveryRawStage:
    ingest_seq: int
    source: RawSourceRef
    connection_id: ConnectionId
    subscription_id: SubscriptionId
    socket_generation: int | None
    received_monotonic_ns: int
    admission_ordinal: int | None = None
    event_type: str | None = None
    exact_depth_identity: AuditExactDepthIdentity | None = None


@dataclass(frozen=True, slots=True, order=True)
class _JournalPosition:
    ingest_seq: int
    subordinal: int


@dataclass(frozen=True, slots=True)
class _ExactAction:
    position: _JournalPosition
    source: RawSourceRef
    native_asset_id: str
    connection_id: ConnectionId
    subscription_id: SubscriptionId
    admission_ordinal: int | None
    received_monotonic_ns: int
    venue_hash: str
    timestamp_raw: str | None
    event_type: str
    replacement_bids: tuple[BookLevel, ...] | None = None
    replacement_asks: tuple[BookLevel, ...] | None = None
    replacement_depth_identity: AuditExactDepthIdentity | None = None
    side: NativeSide | None = None
    price: Price | None = None
    quantity: Quantity | None = None


@dataclass(frozen=True, slots=True)
class _PrecisionAction:
    position: _JournalPosition
    source: RawSourceRef
    connection_id: ConnectionId
    subscription_id: SubscriptionId
    admission_ordinal: int | None
    received_monotonic_ns: int
    native_asset_id: str
    old_tick_size: ExactDecimal
    new_tick_size: ExactDecimal


@dataclass(slots=True)
class _CausalBook:
    bids: dict[Price, Quantity]
    asks: dict[Price, Quantity]
    sources: list[RawSourceRef]
    source_ids: set[IngestId]
    exact_frame_count: int
    latest_source: RawSourceRef
    latest_position: _JournalPosition
    latest_admission_ordinal: int | None
    latest_received_monotonic_ns: int
    latest_event_type: str
    latest_venue_hash: str
    latest_timestamp_raw: str | None
    latest_representation: AuditMatchingStateRepresentation


@dataclass(frozen=True, slots=True)
class _CausalMatch:
    action: _ExactAction
    depth_identity: AuditExactDepthIdentity
    representation: AuditMatchingStateRepresentation
    sources: tuple[RawSourceRef, ...]
    exact_frame_count: int
    socket_generation: int | None


@dataclass(slots=True)
class _WorkingLineage:
    resolved: _ResolvedObservation
    matching_prior: _ExactAction | None = None
    matching_future: _ExactAction | None = None
    matching_reference_future: _PrecisionAction | None = None
    ambiguous_match: bool = False
    bids: dict[Price, Quantity] | None = None
    asks: dict[Price, Quantity] | None = None
    subsequent_exact_frames: int = 0
    subsequent_sources: list[RawSourceRef] | None = None
    subsequent_source_ids: set[IngestId] | None = None
    overflowed: bool = False
    causal_books: dict[ConnectionId, _CausalBook] | None = None
    causal_prior_match: _CausalMatch | None = None
    causal_original_match: _CausalMatch | None = None
    causal_recovery_match: _CausalMatch | None = None
    causal_original_conflict: bool = False
    causal_ambiguous: bool = False
    causal_search_overflowed: bool = False
    causal_search_exact_frames: int = 0
    causal_search_source_ids: set[IngestId] | None = None
    causal_missing_matching_admission: bool = False
    causal_original_post_frames: int = 0
    causal_replacement_seen: bool = False
    causal_rest_resnapshot_candidate_seen: bool = False


@dataclass(slots=True)
class _LevelResourceTracker:
    retained: int
    peak: int

    def replace(self, before: int, after: int, maximum: int) -> None:
        self.retained += after - before
        self.peak = max(self.peak, self.retained)
        if self.retained > maximum:
            raise AuditMismatchReportResourceError(
                "audit-mismatch retained exact levels exceed the configured limit"
            )


def _source(envelope: RawEventEnvelope) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=envelope.ingest_id,
        payload_sha256=envelope.payload_sha256,
    )


def _text(value: object, *, field: str, maximum: int = 4_096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise AuditMismatchReportError(f"{field} must be non-empty bounded text")
    return value


def _bounded_text(value: object, *, field: str, maximum: int) -> str:
    if not isinstance(value, str) or len(value) > maximum:
        raise AuditMismatchReportError(f"{field} must be bounded text")
    return value


def _integer(value: object, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AuditMismatchReportError(f"{field} must be an integer >= {minimum}")
    return value


def _canonical_control(payload: bytes, *, context: str) -> dict[str, object]:
    try:
        value: object = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AuditMismatchReportError(f"{context} is not valid JSON") from error
    if not isinstance(value, dict):
        raise AuditMismatchReportError(f"{context} must be a JSON object")
    untyped = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in untyped):
        raise AuditMismatchReportError(f"{context} keys must be text")
    control = cast(dict[str, object], untyped)
    if canonical_json_bytes(control) != payload:
        raise AuditMismatchReportError(f"{context} is not canonically encoded")
    return control


def _journal_identity_update(digest: _Digest, envelope: RawEventEnvelope) -> None:
    record = envelope.to_json_line()
    digest.update(len(record).to_bytes(8, "big"))
    digest.update(record)


def _scan_pass(
    envelope_factory: Callable[[], Iterable[RawEventEnvelope]],
    *,
    collector_run_id: CollectorRunId,
    limits: AuditMismatchReportLimits,
    visit: Callable[[RawEventEnvelope], None],
) -> _PassSummary:
    digest = hashlib.sha256()
    digest.update(_JOURNAL_IDENTITY_DOMAIN)
    event_count = 0
    payload_bytes = 0
    for envelope in envelope_factory():
        if event_count >= limits.maximum_envelopes_per_pass:
            raise AuditMismatchReportResourceError(
                "audit-mismatch source envelope count exceeds the configured limit"
            )
        if envelope.collector_run_id != collector_run_id:
            raise AuditMismatchReportError("source journal mixes collector runs")
        if envelope.ingest_seq != event_count:
            raise AuditMismatchReportError(
                "source journal ingest sequence is not a contiguous prefix beginning at zero"
            )
        size = len(envelope.payload_bytes)
        if size > limits.maximum_payload_bytes:
            raise AuditMismatchReportResourceError(
                "audit-mismatch source payload exceeds the configured byte limit"
            )
        payload_bytes += size
        if payload_bytes > limits.maximum_total_payload_bytes_per_pass:
            raise AuditMismatchReportResourceError(
                "audit-mismatch source payload bytes exceed the configured pass limit"
            )
        _journal_identity_update(digest, envelope)
        visit(envelope)
        event_count += 1
    return _PassSummary(
        event_count=event_count,
        payload_bytes=payload_bytes,
        identity_sha256=digest.hexdigest(),
    )


def _require_equal_passes(first: _PassSummary, later: _PassSummary) -> None:
    if first != later:
        raise AuditMismatchReportError("source journal identity changed between analysis passes")


def _native_levels(levels: Sequence[NativeBookLevel]) -> tuple[BookLevel, ...]:
    if len(levels) > AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE:
        raise AuditMismatchReportResourceError(
            "raw book side exceeds the exact-depth diagnostic limit"
        )
    return tuple(
        BookLevel(price=Price(level.price), quantity=Quantity(level.size)) for level in levels
    )


def _normalized_depth(
    bids: Sequence[BookLevel],
    asks: Sequence[BookLevel],
) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
    """Return a complete, order-independent depth with duplicate rejection."""

    def side(levels: Sequence[BookLevel], *, reverse: bool) -> tuple[BookLevel, ...]:
        if len({level.price for level in levels}) != len(levels):
            raise AuditMismatchReportError("raw book side contains a duplicate exact price")
        return tuple(
            sorted(
                levels,
                key=lambda level: level.price.value,
                reverse=reverse,
            )
        )

    return side(bids, reverse=True), side(asks, reverse=False)


def _rest_timestamp_key(value: str) -> str | None:
    if not value or len(value) > 32 or not value.isascii() or not value.isdigit():
        return None
    return str(int(value))


def _ws_timestamp_key(nanoseconds_since_epoch: int) -> str | None:
    milliseconds, remainder = divmod(nanoseconds_since_epoch, 1_000_000)
    return str(milliseconds) if remainder == 0 else None


def _exact_actions(
    envelope: RawEventEnvelope,
    *,
    target_assets: set[str],
    reference_target_assets: set[str],
) -> tuple[_ExactAction | _PrecisionAction, ...]:
    try:
        events = parse_market_ws_message(envelope.payload_bytes)
    except (PolymarketAdapterError, TypeError, ValueError) as error:
        raise AuditMismatchReportError(
            f"cannot parse WebSocket evidence at ingest sequence {envelope.ingest_seq}"
        ) from error
    source = _source(envelope)
    raw_admission_ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
    admission_ordinal = (
        raw_admission_ordinal
        if isinstance(raw_admission_ordinal, int) and not isinstance(raw_admission_ordinal, bool)
        else None
    )
    actions: list[_ExactAction | _PrecisionAction] = []
    subordinal = 0
    for event in events:
        timestamp_raw = _ws_timestamp_key(event.venue_timestamp.nanoseconds_since_epoch)
        if isinstance(event, WsBookEvent):
            if event.asset_id in target_assets:
                replacement_bids, replacement_asks = _normalized_depth(
                    _native_levels(event.bids),
                    _native_levels(event.asks),
                )
                actions.append(
                    _ExactAction(
                        position=_JournalPosition(envelope.ingest_seq, subordinal),
                        source=source,
                        connection_id=envelope.connection_id,
                        subscription_id=envelope.subscription_id,
                        admission_ordinal=admission_ordinal,
                        received_monotonic_ns=envelope.received_monotonic.nanoseconds,
                        native_asset_id=event.asset_id,
                        venue_hash=event.venue_hash,
                        timestamp_raw=timestamp_raw,
                        event_type="book",
                        replacement_bids=replacement_bids,
                        replacement_asks=replacement_asks,
                        replacement_depth_identity=audit_exact_depth_identity(
                            replacement_bids,
                            replacement_asks,
                        ),
                    )
                )
            subordinal += 1
        elif isinstance(event, WsPriceChangeEvent):
            for change in event.changes:
                if change.asset_id in target_assets:
                    actions.append(
                        _ExactAction(
                            position=_JournalPosition(envelope.ingest_seq, subordinal),
                            source=source,
                            connection_id=envelope.connection_id,
                            subscription_id=envelope.subscription_id,
                            admission_ordinal=admission_ordinal,
                            received_monotonic_ns=envelope.received_monotonic.nanoseconds,
                            native_asset_id=change.asset_id,
                            venue_hash=change.venue_hash,
                            timestamp_raw=timestamp_raw,
                            event_type="price_change",
                            side=change.side,
                            price=Price(change.price),
                            quantity=Quantity(change.new_size),
                        )
                    )
                subordinal += 1
        elif isinstance(event, WsTickSizeChangeEvent):
            if event.asset_id in reference_target_assets:
                actions.append(
                    _PrecisionAction(
                        position=_JournalPosition(envelope.ingest_seq, subordinal),
                        source=source,
                        connection_id=envelope.connection_id,
                        subscription_id=envelope.subscription_id,
                        admission_ordinal=admission_ordinal,
                        received_monotonic_ns=envelope.received_monotonic.nanoseconds,
                        native_asset_id=event.asset_id,
                        old_tick_size=event.old_tick_size,
                        new_tick_size=event.new_tick_size,
                    )
                )
            subordinal += 1
    return tuple(actions)


def _reset_working(
    working: _WorkingLineage,
    action: _ExactAction,
    *,
    tracker: _LevelResourceTracker,
    limits: AuditMismatchReportLimits,
) -> None:
    if action.replacement_bids is None or action.replacement_asks is None:
        raise AuditMismatchReportError(
            "only a complete WebSocket book snapshot can anchor exact lineage"
        )
    before = 0
    if working.bids is not None and working.asks is not None:
        before = len(working.bids) + len(working.asks)
    working.matching_prior = action
    working.bids = {level.price: level.quantity for level in action.replacement_bids}
    working.asks = {level.price: level.quantity for level in action.replacement_asks}
    working.subsequent_exact_frames = 0
    working.subsequent_sources = []
    working.subsequent_source_ids = set()
    working.overflowed = False
    tracker.replace(
        before,
        len(working.bids) + len(working.asks),
        limits.maximum_total_retained_level_values,
    )


def _matches_rest_snapshot(
    action: _ExactAction,
    resolved: _ResolvedObservation,
) -> bool:
    """Prove exact full-depth equality without trusting venue diagnostics.

    Polymarket's opaque hash and venue timestamp are retained in the result as
    diagnostics.  They are not a cross-transport sequence or checksum and are
    therefore never required for project-owned exact-depth equality.
    """

    rest = resolved.rest
    if (
        rest is None
        or action.replacement_bids is None
        or action.replacement_asks is None
        or action.replacement_depth_identity is None
        or resolved.rest_depth_identity is None
        or action.replacement_depth_identity != resolved.rest_depth_identity
    ):
        return False
    return (
        action.replacement_bids == resolved.rest_bids
        and action.replacement_asks == resolved.rest_asks
    )


def _discard_working(
    working: _WorkingLineage,
    *,
    tracker: _LevelResourceTracker,
    limits: AuditMismatchReportLimits,
) -> None:
    before = (
        0 if working.bids is None or working.asks is None else len(working.bids) + len(working.asks)
    )
    working.bids = None
    working.asks = None
    tracker.replace(before, 0, limits.maximum_total_retained_level_values)


def _apply_action(
    working: _WorkingLineage,
    action: _ExactAction,
    *,
    tracker: _LevelResourceTracker,
    limits: AuditMismatchReportLimits,
) -> None:
    if working.overflowed or working.bids is None or working.asks is None:
        return
    if working.subsequent_exact_frames >= limits.maximum_exact_frames_per_observation:
        working.overflowed = True
        _discard_working(working, tracker=tracker, limits=limits)
        return
    before = len(working.bids) + len(working.asks)
    if action.replacement_bids is not None and action.replacement_asks is not None:
        working.bids = {level.price: level.quantity for level in action.replacement_bids}
        working.asks = {level.price: level.quantity for level in action.replacement_asks}
    else:
        if action.side is None or action.price is None or action.quantity is None:
            raise AuditMismatchReportError("exact WebSocket action is incomplete")
        levels = working.bids if action.side is NativeSide.BUY else working.asks
        if action.quantity.value:
            levels[action.price] = action.quantity
        else:
            levels.pop(action.price, None)
    after = len(working.bids) + len(working.asks)
    tracker.replace(before, after, limits.maximum_total_retained_level_values)
    working.subsequent_exact_frames += 1
    if working.subsequent_sources is None or working.subsequent_source_ids is None:
        raise AuditMismatchReportError("exact lineage source state is unavailable")
    if action.source.ingest_id not in working.subsequent_source_ids:
        working.subsequent_source_ids.add(action.source.ingest_id)
        working.subsequent_sources.append(action.source)


def _causal_depth(book: _CausalBook) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
    return _normalized_depth(
        tuple(BookLevel(price=price, quantity=quantity) for price, quantity in book.bids.items()),
        tuple(BookLevel(price=price, quantity=quantity) for price, quantity in book.asks.items()),
    )


def _apply_causal_action(
    working: _WorkingLineage,
    action: _ExactAction,
    *,
    tracker: _LevelResourceTracker,
    limits: AuditMismatchReportLimits,
) -> _CausalBook | None:
    """Apply one exact action to its connection-scoped reconstructed book."""

    books = working.causal_books
    if books is None:
        books = {}
        working.causal_books = books
    book = books.get(action.connection_id)
    if action.replacement_bids is not None and action.replacement_asks is not None:
        before = 0 if book is None else len(book.bids) + len(book.asks)
        book = _CausalBook(
            bids={level.price: level.quantity for level in action.replacement_bids},
            asks={level.price: level.quantity for level in action.replacement_asks},
            sources=[action.source],
            source_ids={action.source.ingest_id},
            exact_frame_count=1,
            latest_source=action.source,
            latest_position=action.position,
            latest_admission_ordinal=action.admission_ordinal,
            latest_received_monotonic_ns=action.received_monotonic_ns,
            latest_event_type=action.event_type,
            latest_venue_hash=action.venue_hash,
            latest_timestamp_raw=action.timestamp_raw,
            latest_representation=AuditMatchingStateRepresentation.FULL_SNAPSHOT,
        )
        books[action.connection_id] = book
        tracker.replace(
            before,
            len(book.bids) + len(book.asks),
            limits.maximum_total_retained_level_values,
        )
        return book
    if book is None:
        return None
    if action.side is None or action.price is None or action.quantity is None:
        raise AuditMismatchReportError("causal exact WebSocket action is incomplete")
    # The anchor snapshot is retained in the source chain but does not consume
    # the configured subsequent-frame allowance.  Full snapshots may replace
    # this connection-scoped lineage, while the separate causal search budget
    # below remains cumulative across snapshots and connections.
    new_source = action.source.ingest_id not in book.source_ids
    if new_source and book.exact_frame_count - 1 >= limits.maximum_exact_frames_per_observation:
        working.causal_search_overflowed = True
        return None
    before = len(book.bids) + len(book.asks)
    side = book.bids if action.side is NativeSide.BUY else book.asks
    if action.quantity.value:
        side[action.price] = action.quantity
    else:
        side.pop(action.price, None)
    tracker.replace(
        before,
        len(book.bids) + len(book.asks),
        limits.maximum_total_retained_level_values,
    )
    if new_source:
        book.source_ids.add(action.source.ingest_id)
        book.sources.append(action.source)
        book.exact_frame_count += 1
    book.latest_source = action.source
    book.latest_position = action.position
    book.latest_admission_ordinal = action.admission_ordinal
    book.latest_received_monotonic_ns = action.received_monotonic_ns
    book.latest_event_type = action.event_type
    book.latest_venue_hash = action.venue_hash
    book.latest_timestamp_raw = action.timestamp_raw
    book.latest_representation = AuditMatchingStateRepresentation.RECONSTRUCTED_INCREMENTALS
    return book


def _causal_search_decided(working: _WorkingLineage) -> bool:
    """Return whether further exact causal comparisons cannot change the result."""

    return (
        working.causal_search_overflowed
        or working.causal_original_match is not None
        or working.causal_recovery_match is not None
    )


def _reserve_causal_search_frame(
    working: _WorkingLineage,
    source: RawSourceRef,
    *,
    limits: AuditMismatchReportLimits,
) -> bool:
    """Charge one candidate frame to a monotonic per-observation search budget.

    The causal audit boundary is the response admission fence on the original
    connection and classification on a later connection.  A full snapshot or
    reconnect can replace book lineage, but neither can reset this budget.
    """

    if _causal_search_decided(working):
        return False
    source_ids: set[IngestId] | None = working.causal_search_source_ids
    if source_ids is None:
        source_ids = set[IngestId]()
        working.causal_search_source_ids = source_ids
    if source.ingest_id in source_ids:
        return True
    if working.causal_search_exact_frames >= limits.maximum_exact_frames_per_observation:
        working.causal_search_overflowed = True
        return False
    source_ids.add(source.ingest_id)
    working.causal_search_exact_frames += 1
    return True


def _causal_match(
    action: _ExactAction,
    book: _CausalBook,
    *,
    socket_generation: int | None,
) -> _CausalMatch:
    bids, asks = _causal_depth(book)
    return _CausalMatch(
        action=action,
        depth_identity=audit_exact_depth_identity(bids, asks),
        representation=book.latest_representation,
        sources=tuple(book.sources),
        exact_frame_count=book.exact_frame_count,
        socket_generation=socket_generation,
    )


def _causal_book_matches_rest(book: _CausalBook, resolved: _ResolvedObservation) -> bool:
    bids, asks = _causal_depth(book)
    if resolved.rest_depth_identity is None:
        raise AuditMismatchReportError("causal comparison lacks a normalized REST depth")
    return (
        audit_exact_depth_identity(bids, asks) == resolved.rest_depth_identity
        and bids == resolved.rest_bids
        and asks == resolved.rest_asks
    )


def _top_changed(
    local_bids: Sequence[BookLevel],
    local_asks: Sequence[BookLevel],
    rest_bids: Sequence[BookLevel],
    rest_asks: Sequence[BookLevel],
) -> bool:
    return _best_price(local_bids, bid=True) != _best_price(rest_bids, bid=True) or _best_price(
        local_asks, bid=False
    ) != _best_price(rest_asks, bid=False)


def _best_price(levels: Sequence[BookLevel], *, bid: bool) -> Price | None:
    if not levels:
        return None
    prices = (level.price for level in levels)
    return (
        max(prices, key=lambda price: price.value)
        if bid
        else min(prices, key=lambda price: price.value)
    )


def _local_depth_is_defective(
    bids: Sequence[BookLevel],
    asks: Sequence[BookLevel],
) -> bool:
    best_bid = _best_price(bids, bid=True)
    best_ask = _best_price(asks, bid=False)
    return best_bid is not None and best_ask is not None and best_bid.value >= best_ask.value


def _reported_transition_proves_cause(resolved: _ResolvedObservation) -> bool:
    observation = resolved.observation
    transition = observation.reported_top_transition
    if (
        transition is None
        or resolved.reported_transition_first_source is None
        or resolved.reported_transition_latest_source is None
        or observation.difference_counts.top_changed is not True
    ):
        return False
    rest_best_bid = _best_price(resolved.rest_bids, bid=True)
    rest_best_ask = _best_price(resolved.rest_asks, bid=False)
    return (
        rest_best_bid == transition.target_best_bid
        and rest_best_ask == transition.target_best_ask
        and (
            transition.starting_best_bid != transition.target_best_bid
            or transition.starting_best_ask != transition.target_best_ask
        )
    )


def _coarse_difference(
    difference: AuditExactDepthDifference,
    *,
    local_bids: Sequence[BookLevel],
    local_asks: Sequence[BookLevel],
    rest_bids: Sequence[BookLevel],
    rest_asks: Sequence[BookLevel],
) -> AuditMismatchDifferenceCounts:
    return AuditMismatchDifferenceCounts.from_exact(
        difference,
        top_changed=_top_changed(local_bids, local_asks, rest_bids, rest_asks),
    )


def _coarse_matches(
    observed: AuditMismatchDifferenceCounts,
    reconstructed: AuditMismatchDifferenceCounts,
) -> bool:
    return (
        observed.bid_difference_count == reconstructed.bid_difference_count
        and observed.ask_difference_count == reconstructed.ask_difference_count
        and observed.local_only_count == reconstructed.local_only_count
        and observed.rest_only_count == reconstructed.rest_only_count
        and observed.quantity_difference_count == reconstructed.quantity_difference_count
        and (observed.top_changed is None or observed.top_changed == reconstructed.top_changed)
    )


def _result_digest(results: Sequence[AuditMismatchLineageResult]) -> str:
    digest = hashlib.sha256()
    digest.update(_RESULT_SEQUENCE_DOMAIN)
    for result in results:
        payload = canonical_json_bytes(result)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _absent_recovery_stage(reason: str) -> AuditRecoveryStageEvidence:
    return AuditRecoveryStageEvidence(
        status=AuditRecoveryEvidenceStatus.NOT_RETAINED,
        reason=reason,
    )


def _raw_recovery_stage(
    stage: _RecoveryRawStage,
    *,
    reason: str,
    status: AuditRecoveryEvidenceStatus = AuditRecoveryEvidenceStatus.RAW_JOURNAL,
) -> AuditRecoveryStageEvidence:
    return AuditRecoveryStageEvidence(
        status=status,
        reason=reason,
        ingest_seq=stage.ingest_seq,
        source=stage.source,
        connection_id=stage.connection_id,
        subscription_id=stage.subscription_id,
        socket_generation=stage.socket_generation,
        admission_ordinal=stage.admission_ordinal,
        received_monotonic_ns=stage.received_monotonic_ns,
        event_type=stage.event_type,
        exact_depth_identity=stage.exact_depth_identity,
    )


def _classification_recovery_stage(
    resolved: _ResolvedObservation,
    *,
    status: AuditRecoveryEvidenceStatus,
    reason: str,
) -> AuditRecoveryStageEvidence:
    if (
        resolved.classification_source is None
        or resolved.classification_connection is None
        or resolved.classification_subscription_id is None
        or resolved.classification_received_monotonic_ns is None
    ):
        raise AuditMismatchReportError("classification recovery evidence is incomplete")
    return AuditRecoveryStageEvidence(
        status=status,
        reason=reason,
        ingest_seq=resolved.observation.classification_ingest_seq,
        source=resolved.classification_source,
        connection_id=ConnectionId(resolved.classification_connection),
        subscription_id=resolved.classification_subscription_id,
        socket_generation=resolved.start_socket_generation,
        received_monotonic_ns=resolved.classification_received_monotonic_ns,
        event_type="audit_mismatch_classification",
    )


def _recovery_chronology(resolved: _ResolvedObservation) -> AuditRecoveryChronology:
    classification = _classification_recovery_stage(
        resolved,
        status=AuditRecoveryEvidenceStatus.RAW_JOURNAL,
        reason="raw audit classification bound to the supplied fenced MISMATCH observation",
    )
    replay_invalidation = _classification_recovery_stage(
        resolved,
        status=AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY,
        reason="a fenced MISMATCH deterministically invalidates the local book",
    )
    replay_ineligibility = _classification_recovery_stage(
        resolved,
        status=AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY,
        reason="a fenced MISMATCH deterministically blocks research and further audit eligibility",
    )
    replay_resync = _classification_recovery_stage(
        resolved,
        status=AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY,
        reason=(
            "the MISMATCH observation requires resynchronization; "
            "no separate raw start control exists"
        ),
    )
    replacement = resolved.first_replacement_websocket
    resnapshot = resolved.first_rest_resnapshot
    if replacement is None:
        generation_termination = _absent_recovery_stage(
            "no raw replacement WebSocket generation establishes termination "
            "of the original generation"
        )
        replacement_stage = _absent_recovery_stage(
            "no replacement WebSocket generation for this asset is retained after classification"
        )
    else:
        generation_termination = _raw_recovery_stage(
            replacement,
            status=AuditRecoveryEvidenceStatus.RAW_JOURNAL_INFERENCE,
            reason="first later connection for the asset implies an original-generation boundary",
        )
        replacement_stage = _raw_recovery_stage(
            replacement,
            reason="first raw WebSocket evidence for the asset on a later socket generation",
        )
    if resnapshot is None:
        resnapshot_stage = _absent_recovery_stage(
            "no raw REST_BOOK recovery candidate for this asset is retained after classification"
        )
        completion = _absent_recovery_stage(
            "no canonical replay outcome proves deterministic recovery completion"
        )
        restoration = _absent_recovery_stage(
            "no retained availability evidence proves global research eligibility restoration"
        )
    else:
        resnapshot_stage = _raw_recovery_stage(
            resnapshot,
            reason=(
                "raw REST_BOOK is a recovery candidate; raw channel membership does not prove "
                "canonical application"
            ),
        )
        completion = _absent_recovery_stage(
            "raw REST_BOOK does not prove successful canonical application, a healthy exact "
            "baseline, or deterministic replay completion"
        )
        restoration = _absent_recovery_stage(
            "raw REST_BOOK is only a recovery candidate; neither instrument-baseline "
            "restoration nor global research eligibility is proven without canonical "
            "replay and typed availability-ledger evidence"
        )
    return AuditRecoveryChronology(
        mismatch_classification=classification,
        local_invalidation=replay_invalidation,
        research_audit_ineligibility=replay_ineligibility,
        generation_termination=generation_termination,
        resync_initiation=replay_resync,
        rest_resnapshot=resnapshot_stage,
        replacement_websocket_generation=replacement_stage,
        recovery_completion=completion,
        research_eligibility_restoration=restoration,
    )


def _build_result(working: _WorkingLineage) -> AuditMismatchLineageResult:
    resolved = working.resolved
    observation = resolved.observation
    if (
        resolved.classification_source is None
        or resolved.classification_connection is None
        or resolved.classification_subscription_id is None
        or resolved.start_source is None
        or resolved.response_source is None
        or resolved.start_ingest_seq is None
        or resolved.response_ingest_seq is None
        or resolved.native_asset_id is None
        or resolved.attempt is None
        or resolved.rest is None
        or resolved.rest_depth_identity is None
        or resolved.request_id is None
        or resolved.request_started_monotonic_ns is None
        or resolved.response_received_monotonic_ns is None
        or resolved.classification_monotonic_ns is None
        or resolved.classification_received_monotonic_ns is None
        or resolved.starting_admission_ordinal is None
        or resolved.starting_processed_ordinal is None
        or resolved.starting_book is None
    ):
        raise AuditMismatchReportError("resolved mismatch evidence is incomplete")
    relation = AuditVenueStateRelation.NONE
    matching: _ExactAction | _PrecisionAction | None = None
    matching_source: RawSourceRef | None = None
    matching_ingest_seq: int | None = None
    reconstructed_counts = None
    exact_reconciliation = False
    subsequent_sources = working.subsequent_sources or []

    if resolved.rest_identity_mismatch:
        cause = AuditMismatchCause.UNKNOWN
        reason = AuditMismatchLineageReason.OBSERVATION_REST_IDENTITY_MISMATCH
    elif working.ambiguous_match:
        cause = AuditMismatchCause.UNKNOWN
        reason = AuditMismatchLineageReason.AMBIGUOUS_VENUE_STATE_ORDER
        relation = AuditVenueStateRelation.AMBIGUOUS
    elif _reported_transition_proves_cause(resolved):
        transition = cast(ReportedTopTransition, observation.reported_top_transition)
        cause = AuditMismatchCause.REPORTED_TOP_TRANSITION
        reason = AuditMismatchLineageReason.ACTIVE_REPORTED_TOP_TRANSITION
        matching_source = resolved.reported_transition_latest_source
        matching_ingest_seq = transition.latest_ingest_seq
    elif working.matching_reference_future is not None:
        matching = working.matching_reference_future
        relation = AuditVenueStateRelation.FUTURE
        cause = AuditMismatchCause.REFERENCE_PRECISION_TRANSITION
        reason = AuditMismatchLineageReason.FUTURE_REFERENCE_PRECISION_TRANSITION
    elif working.matching_prior is not None:
        matching = working.matching_prior
        relation = AuditVenueStateRelation.PRIOR
        if working.overflowed:
            cause = AuditMismatchCause.UNKNOWN
            reason = AuditMismatchLineageReason.EXACT_LINEAGE_BOUND_EXCEEDED
        elif working.subsequent_exact_frames == 0:
            if observation.difference_counts.difference_count:
                cause = AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
                reason = AuditMismatchLineageReason.LATEST_PRIOR_STATE_DIVERGENCE
            else:
                cause = AuditMismatchCause.UNKNOWN
                reason = AuditMismatchLineageReason.SCALAR_MISMATCH_CAUSE_UNPROVEN
        elif working.bids is None or working.asks is None:
            raise AuditMismatchReportError("prior exact lineage has no reconstructed state")
        else:
            local_bids = tuple(
                BookLevel(price=price, quantity=quantity)
                for price, quantity in working.bids.items()
            )
            local_asks = tuple(
                BookLevel(price=price, quantity=quantity)
                for price, quantity in working.asks.items()
            )
            reconstructed = compare_audit_exact_depth(
                local_bids=local_bids,
                local_asks=local_asks,
                rest_bids=resolved.rest_bids,
                rest_asks=resolved.rest_asks,
            )
            reconstructed_counts = _coarse_difference(
                reconstructed,
                local_bids=local_bids,
                local_asks=local_asks,
                rest_bids=resolved.rest_bids,
                rest_asks=resolved.rest_asks,
            )
            if _local_depth_is_defective(local_bids, local_asks):
                cause = AuditMismatchCause.LOCAL_BOOK_DEFECT
                reason = AuditMismatchLineageReason.RECONSTRUCTED_LOCAL_BOOK_DEFECT
            elif observation.exact_depth_difference is not None:
                exact_reconciliation = reconstructed == observation.exact_depth_difference
                if exact_reconciliation and observation.difference_counts.difference_count:
                    cause = AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
                    reason = AuditMismatchLineageReason.PRIOR_STATE_EXACTLY_RECONCILED
                elif observation.difference_counts.difference_count == 0:
                    exact_reconciliation = False
                    cause = AuditMismatchCause.UNKNOWN
                    reason = AuditMismatchLineageReason.SCALAR_MISMATCH_CAUSE_UNPROVEN
                else:
                    cause = AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
                    reason = AuditMismatchLineageReason.PRIOR_STATE_RESIDUAL_DIVERGENCE
            elif not _coarse_matches(observation.difference_counts, reconstructed_counts):
                # Different aggregate counts prove that the exact boundary
                # mutations cannot account for the observed mismatch.  Equal
                # counts are not an identity proof and remain UNKNOWN below.
                cause = AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
                reason = AuditMismatchLineageReason.PRIOR_STATE_RESIDUAL_DIVERGENCE
            else:
                cause = AuditMismatchCause.UNKNOWN
                reason = AuditMismatchLineageReason.PRIOR_STATE_COARSE_RECONCILIATION_UNPROVEN
    elif working.matching_future is not None and observation.difference_counts.difference_count > 0:
        matching = working.matching_future
        relation = AuditVenueStateRelation.FUTURE
        cause = AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
        reason = AuditMismatchLineageReason.FUTURE_VENUE_STATE
        exact_reconciliation = True
    else:
        cause = AuditMismatchCause.UNKNOWN
        reason = (
            AuditMismatchLineageReason.SCALAR_MISMATCH_CAUSE_UNPROVEN
            if observation.difference_counts.difference_count == 0
            else AuditMismatchLineageReason.VENUE_STATE_NOT_FOUND
        )

    if matching is not None:
        matching_source = matching.source
        matching_ingest_seq = matching.position.ingest_seq

    depth_mismatch = observation.difference_counts.difference_count > 0
    causal_matching: _CausalMatch | None = None
    if resolved.rest_identity_mismatch:
        causal_relation = AuditMismatchCausalRelation.AMBIGUOUS
        relation = AuditVenueStateRelation.AMBIGUOUS
        matching = None
        matching_source = None
        matching_ingest_seq = None
        exact_reconciliation = False
    elif (
        _reported_transition_proves_cause(resolved) or working.matching_reference_future is not None
    ):
        causal_relation = AuditMismatchCausalRelation.REFERENCE_TRANSITION
    elif cause is AuditMismatchCause.LOCAL_BOOK_DEFECT:
        causal_relation = AuditMismatchCausalRelation.LOCAL_BOOK_DEFECT
        causal_matching = working.causal_prior_match
    elif working.overflowed or working.causal_search_overflowed:
        causal_relation = AuditMismatchCausalRelation.AMBIGUOUS
        cause = AuditMismatchCause.UNKNOWN
        reason = AuditMismatchLineageReason.EXACT_LINEAGE_BOUND_EXCEEDED
        relation = AuditVenueStateRelation.AMBIGUOUS
        matching = None
        matching_source = None
        matching_ingest_seq = None
        exact_reconciliation = False
    elif working.causal_ambiguous:
        causal_relation = AuditMismatchCausalRelation.AMBIGUOUS
        cause = AuditMismatchCause.UNKNOWN
        reason = AuditMismatchLineageReason.CAUSAL_GENERATION_AMBIGUOUS
        relation = AuditVenueStateRelation.AMBIGUOUS
        matching = None
        matching_source = None
        matching_ingest_seq = None
        exact_reconciliation = False
    elif depth_mismatch and working.causal_original_match is not None:
        causal_matching = working.causal_original_match
        matching = None
        matching_source = causal_matching.action.source
        matching_ingest_seq = causal_matching.action.position.ingest_seq
        relation = AuditVenueStateRelation.FUTURE
        if working.causal_original_conflict:
            causal_relation = (
                AuditMismatchCausalRelation.SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE
            )
            cause = AuditMismatchCause.UNKNOWN
            reason = AuditMismatchLineageReason.SAME_GENERATION_CONFLICT
            exact_reconciliation = False
        else:
            causal_relation = AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
            cause = AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
            reason = AuditMismatchLineageReason.SAME_GENERATION_EXACT_CONVERGENCE
            exact_reconciliation = True
    elif depth_mismatch and working.causal_recovery_match is not None:
        causal_matching = working.causal_recovery_match
        matching = None
        matching_source = causal_matching.action.source
        matching_ingest_seq = causal_matching.action.position.ingest_seq
        causal_relation = AuditMismatchCausalRelation.RECOVERY_GENERATION_MATCH
        relation = AuditVenueStateRelation.FUTURE
        cause = AuditMismatchCause.UNKNOWN
        reason = AuditMismatchLineageReason.RECOVERY_GENERATION_EXACT_MATCH
        exact_reconciliation = True
    elif depth_mismatch and working.causal_prior_match is not None:
        causal_matching = working.causal_prior_match
        matching = None
        matching_source = causal_matching.action.source
        matching_ingest_seq = causal_matching.action.position.ingest_seq
        causal_relation = AuditMismatchCausalRelation.PRIOR_STATE_MATCH
        relation = AuditVenueStateRelation.PRIOR
        if reason not in {
            AuditMismatchLineageReason.PRIOR_STATE_EXACTLY_RECONCILED,
            AuditMismatchLineageReason.LATEST_PRIOR_STATE_DIVERGENCE,
            AuditMismatchLineageReason.PRIOR_STATE_RESIDUAL_DIVERGENCE,
            AuditMismatchLineageReason.PRIOR_STATE_COARSE_RECONCILIATION_UNPROVEN,
        }:
            reason = AuditMismatchLineageReason.PRIOR_EXACT_STATE_MATCH
    elif depth_mismatch and working.causal_original_post_frames:
        causal_relation = AuditMismatchCausalRelation.PERSISTENT_DIVERGENCE
        relation = AuditVenueStateRelation.NONE
        cause = AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
        reason = AuditMismatchLineageReason.PERSISTENT_EXACT_DIVERGENCE
        matching = None
        matching_source = None
        matching_ingest_seq = None
        exact_reconciliation = False
    else:
        causal_relation = AuditMismatchCausalRelation.NOT_FOUND
        relation = AuditVenueStateRelation.NONE
        cause = AuditMismatchCause.UNKNOWN
        reason = (
            AuditMismatchLineageReason.SCALAR_MISMATCH_CAUSE_UNPROVEN
            if not depth_mismatch
            else AuditMismatchLineageReason.VENUE_STATE_NOT_FOUND
        )
        matching = None
        matching_source = None
        matching_ingest_seq = None
        exact_reconciliation = False

    start_connection_id = ConnectionId(resolved.classification_connection)
    matching_subordinal: int | None = None
    matching_connection_id: ConnectionId | None = None
    matching_subscription_id: SubscriptionId | None = None
    matching_socket_generation: int | None = None
    matching_admission_ordinal: int | None = None
    matching_event_type: str | None = None
    matching_representation: AuditMatchingStateRepresentation | None = None
    matching_depth_identity: AuditExactDepthIdentity | None = None
    matching_venue_hash: str | None = None
    matching_venue_timestamp: str | None = None
    matching_sources: tuple[RawSourceRef, ...] = ()
    matching_frame_count = 0
    matching_received_monotonic_ns: int | None = None
    if causal_matching is not None:
        action = causal_matching.action
        matching_subordinal = action.position.subordinal
        matching_connection_id = action.connection_id
        matching_subscription_id = action.subscription_id
        matching_source = action.source
        matching_ingest_seq = action.position.ingest_seq
        matching_socket_generation = causal_matching.socket_generation
        matching_admission_ordinal = action.admission_ordinal
        matching_event_type = action.event_type
        matching_representation = causal_matching.representation
        matching_depth_identity = causal_matching.depth_identity
        matching_venue_hash = action.venue_hash
        matching_venue_timestamp = action.timestamp_raw
        matching_sources = causal_matching.sources
        matching_frame_count = causal_matching.exact_frame_count
        matching_received_monotonic_ns = action.received_monotonic_ns
    elif isinstance(matching, _PrecisionAction):
        matching_subordinal = matching.position.subordinal
        matching_connection_id = matching.connection_id
        matching_subscription_id = matching.subscription_id
        matching_socket_generation = (
            resolved.start_socket_generation
            if matching.connection_id == start_connection_id
            else None
        )
        matching_admission_ordinal = matching.admission_ordinal
        matching_event_type = "tick_size_change"
        matching_sources = (matching.source,)
        matching_frame_count = 1
        matching_received_monotonic_ns = matching.received_monotonic_ns
    elif _reported_transition_proves_cause(resolved):
        transition = cast(ReportedTopTransition, observation.reported_top_transition)
        matching_connection_id = transition.connection_id
        matching_subscription_id = resolved.classification_subscription_id
        matching_socket_generation = resolved.start_socket_generation
        matching_admission_ordinal = transition.latest_admission_ordinal
        matching_event_type = "reported_top_transition"
        matching_sources = transition.latest_source_raw_refs
        matching_frame_count = transition.accepted_frame_count
        matching_received_monotonic_ns = transition.latest_received_monotonic_ns

    rest_timestamp_key = _rest_timestamp_key(resolved.rest.timestamp_raw)
    venue_hash_equal = (
        None
        if not resolved.rest.venue_hash or not matching_venue_hash
        else resolved.rest.venue_hash == matching_venue_hash
    )
    venue_timestamp_equal = (
        None
        if rest_timestamp_key is None or matching_venue_timestamp is None
        else rest_timestamp_key == matching_venue_timestamp
    )
    exact = observation.exact_depth_difference
    evidence_gaps: list[str] = []
    if resolved.start_socket_generation is None:
        evidence_gaps.append("START_SOCKET_GENERATION_NOT_BOUND")
    if resolved.response_boundary_ordinal is None:
        evidence_gaps.append("RESPONSE_BOUNDARY_ADMISSION_ORDINAL_NOT_RETAINED")
    if resolved.barrier_processed_ordinal is None:
        evidence_gaps.append("BARRIER_PROCESSED_ORDINAL_NOT_RETAINED")
    if exact is None:
        evidence_gaps.extend(
            (
                "STARTING_EXACT_DEPTH_IDENTITY_NOT_RETAINED",
                "BARRIER_EXACT_DEPTH_IDENTITY_NOT_RETAINED",
                "EXACT_MISMATCH_DIFFERENCE_IDENTITY_NOT_RETAINED",
            )
        )
    if working.causal_search_overflowed:
        evidence_gaps.append("EXACT_FRAME_BOUND_EXCEEDED")
    elif causal_relation is AuditMismatchCausalRelation.AMBIGUOUS:
        evidence_gaps.append("CAUSAL_ORDER_OR_GENERATION_AMBIGUOUS")
    if causal_relation is AuditMismatchCausalRelation.NOT_FOUND:
        evidence_gaps.append("MATCHING_EXACT_STATE_NOT_FOUND")
    if (
        matching_source is not None and matching_admission_ordinal is None
    ) or working.causal_missing_matching_admission:
        evidence_gaps.append("MATCHING_ADMISSION_ORDINAL_NOT_RETAINED")

    response_to_match = (
        None
        if matching_received_monotonic_ns is None
        else matching_received_monotonic_ns - resolved.response_received_monotonic_ns
    )
    classification_to_match = (
        None
        if matching_received_monotonic_ns is None
        else matching_received_monotonic_ns - resolved.classification_received_monotonic_ns
    )

    return AuditMismatchLineageResult(
        classification_ingest_seq=observation.classification_ingest_seq,
        instrument_id=observation.instrument_id,
        native_asset_id=resolved.native_asset_id,
        attempt=resolved.attempt,
        cause=cause,
        reason=reason,
        venue_state_relation=relation,
        start_ingest_seq=resolved.start_ingest_seq,
        response_ingest_seq=resolved.response_ingest_seq,
        classification_source=resolved.classification_source,
        start_source=resolved.start_source,
        response_source=resolved.response_source,
        matching_websocket_source=matching_source,
        matching_websocket_ingest_seq=matching_ingest_seq,
        rest_venue_hash=resolved.rest.venue_hash,
        rest_timestamp_raw=resolved.rest.timestamp_raw,
        subsequent_exact_frame_count=working.subsequent_exact_frames,
        subsequent_raw_sources=summarize_audit_raw_sources(subsequent_sources),
        observed_difference=observation.difference_counts,
        reconstructed_difference=reconstructed_counts,
        exact_reconciliation_proven=exact_reconciliation,
        request_id=resolved.request_id,
        causal_relation=causal_relation,
        start_connection_id=start_connection_id,
        start_socket_generation=resolved.start_socket_generation,
        start_admission_ordinal=resolved.starting_admission_ordinal,
        start_processed_ordinal=resolved.starting_processed_ordinal,
        starting_book=resolved.starting_book,
        starting_exact_depth_identity=None if exact is None else exact.local_depth,
        response_boundary_admission_ordinal=resolved.response_boundary_ordinal,
        barrier_processed_ordinal=resolved.barrier_processed_ordinal,
        barrier_exact_depth_identity=None if exact is None else exact.local_depth,
        normalized_rest_exact_depth_identity=resolved.rest_depth_identity,
        exact_mismatch_difference_sha256=None if exact is None else exact.difference_sha256,
        request_started_monotonic_ns=resolved.request_started_monotonic_ns,
        response_received_monotonic_ns=resolved.response_received_monotonic_ns,
        classification_monotonic_ns=resolved.classification_monotonic_ns,
        classification_received_monotonic_ns=resolved.classification_received_monotonic_ns,
        matching_websocket_subordinal=matching_subordinal,
        matching_connection_id=matching_connection_id,
        matching_subscription_id=matching_subscription_id,
        matching_socket_generation=matching_socket_generation,
        matching_admission_ordinal=matching_admission_ordinal,
        matching_event_type=matching_event_type,
        matching_state_representation=matching_representation,
        matching_exact_depth_identity=matching_depth_identity,
        matching_venue_hash_diagnostic=matching_venue_hash,
        matching_venue_timestamp_diagnostic=matching_venue_timestamp,
        matching_source_chain=summarize_audit_raw_sources(matching_sources),
        matching_exact_frame_count=matching_frame_count,
        matching_received_monotonic_ns=matching_received_monotonic_ns,
        response_to_match_monotonic_ns=response_to_match,
        classification_to_match_monotonic_ns=classification_to_match,
        equality_proof=(
            AuditEqualityProof.FULL_DEPTH_EXACT
            if causal_matching is not None
            or (
                exact is not None
                and exact.difference_count == 0
                and not resolved.rest_identity_mismatch
            )
            else (
                AuditEqualityProof.COARSE_ONLY
                if observation.exact_depth_difference is None
                else AuditEqualityProof.NOT_PROVEN
            )
        ),
        venue_hash_diagnostic_equal=venue_hash_equal,
        venue_timestamp_diagnostic_equal=venue_timestamp_equal,
        recovery_chronology=_recovery_chronology(resolved),
        unresolved_evidence=tuple(sorted(evidence_gaps)),
    )


def build_audit_mismatch_lineage_report(
    envelope_factory: Callable[[], Iterable[RawEventEnvelope]],
    collector_run_id: CollectorRunId,
    observations: Sequence[AuditMismatchObservation],
    *,
    limits: AuditMismatchReportLimits | None = None,
) -> AuditMismatchLineageReport:
    """Join mismatch observations to a replayable raw journal without writes.

    The factory must return the complete verified run from ingest sequence zero
    on every call.  The report checks a bounded identity digest across all
    three passes, so a mutable or inconsistent source fails closed.
    """

    selected_limits = limits or AuditMismatchReportLimits()
    if not observations:
        raise AuditMismatchReportError("at least one MISMATCH observation is required")
    if len(observations) > selected_limits.maximum_observations:
        raise AuditMismatchReportResourceError(
            "mismatch observation count exceeds the configured limit"
        )
    ordered = tuple(sorted(observations, key=lambda item: item.classification_ingest_seq))
    if len({item.classification_ingest_seq for item in ordered}) != len(ordered):
        raise AuditMismatchReportError("mismatch observations repeat a classification sequence")
    resolved = [_ResolvedObservation(observation=item) for item in ordered]
    by_classification = {item.observation.classification_ingest_seq: item for item in resolved}
    reported_first_by_sequence: dict[int, list[_ResolvedObservation]] = {}
    reported_latest_by_sequence: dict[int, list[_ResolvedObservation]] = {}
    connection_generations: dict[ConnectionId, int] = {}
    for item in resolved:
        transition = item.observation.reported_top_transition
        if transition is None:
            continue
        reported_first_by_sequence.setdefault(transition.first_ingest_seq, []).append(item)
        reported_latest_by_sequence.setdefault(transition.latest_ingest_seq, []).append(item)

    def bind_classification(envelope: RawEventEnvelope) -> None:
        if envelope.channel == WS_MARKET and envelope.connection_id not in connection_generations:
            connection_generations[envelope.connection_id] = len(connection_generations) + 1
        reported_source = None
        for reported_item in reported_first_by_sequence.get(envelope.ingest_seq, ()):
            reported_source = reported_source or _source(envelope)
            transition = cast(
                ReportedTopTransition,
                reported_item.observation.reported_top_transition,
            )
            if reported_source not in transition.first_source_raw_refs:
                raise AuditMismatchReportError(
                    "reported-top first source disagrees with the raw journal"
                )
            if (
                envelope.connection_id != transition.connection_id
                or envelope.received_wall_time != transition.first_received_at
                or envelope.received_monotonic.epoch_id != transition.monotonic_epoch_id
                or envelope.received_monotonic.nanoseconds != transition.first_received_monotonic_ns
                or envelope.transport_metadata.get("websocket_admission_ordinal")
                != transition.first_admission_ordinal
            ):
                raise AuditMismatchReportError(
                    "reported-top first source context disagrees with the raw journal"
                )
            reported_item.reported_transition_first_source = reported_source
        for reported_item in reported_latest_by_sequence.get(envelope.ingest_seq, ()):
            reported_source = reported_source or _source(envelope)
            transition = cast(
                ReportedTopTransition,
                reported_item.observation.reported_top_transition,
            )
            if reported_source not in transition.latest_source_raw_refs:
                raise AuditMismatchReportError(
                    "reported-top latest source disagrees with the raw journal"
                )
            if (
                envelope.connection_id != transition.connection_id
                or envelope.received_wall_time != transition.latest_received_at
                or envelope.received_monotonic.epoch_id != transition.monotonic_epoch_id
                or envelope.received_monotonic.nanoseconds
                != transition.latest_received_monotonic_ns
                or envelope.transport_metadata.get("websocket_admission_ordinal")
                != transition.latest_admission_ordinal
            ):
                raise AuditMismatchReportError(
                    "reported-top latest source context disagrees with the raw journal"
                )
            reported_item.reported_transition_latest_source = reported_source
        item = by_classification.get(envelope.ingest_seq)
        if item is None:
            return
        if envelope.channel != CONTROL_REST_AUDIT_CLASSIFY:
            raise AuditMismatchReportError(
                "mismatch observation does not identify an audit classification control"
            )
        source = _source(envelope)
        expected = item.observation.classification_source
        if expected is not None and source != expected:
            raise AuditMismatchReportError("classification raw identity disagrees with artifact")
        transition = item.observation.reported_top_transition
        if transition is not None and transition.connection_id != envelope.connection_id:
            raise AuditMismatchReportError(
                "reported-top transition and audit classification connections disagree"
            )
        control = _canonical_control(
            envelope.payload_bytes,
            context="audit classification control",
        )
        if (
            set(control)
            != {
                "classification_monotonic_ns",
                "hard_expiry_monotonic_ns",
                "request_id",
                "response_ingest_seq",
                "schema",
            }
            or control.get("schema") != AUDIT_CLASSIFICATION_SCHEMA
        ):
            raise AuditMismatchReportError("audit classification control schema is invalid")
        item.classification_source = source
        item.classification_connection = envelope.connection_id.value
        item.classification_subscription_id = envelope.subscription_id
        item.request_id = _text(control.get("request_id"), field="classification request ID")
        item.response_ingest_seq = _integer(
            control.get("response_ingest_seq"),
            field="classification response ingest sequence",
        )
        classification_monotonic_ns = _integer(
            control.get("classification_monotonic_ns"),
            field="classification monotonic time",
        )
        classification_received_monotonic_ns = envelope.received_monotonic.nanoseconds
        if classification_monotonic_ns != classification_received_monotonic_ns:
            raise AuditMismatchReportError(
                "audit classification timestamp differs from raw receive evidence"
            )
        hard_expiry_monotonic_ns = _integer(
            control.get("hard_expiry_monotonic_ns"),
            field="audit hard expiry",
        )
        if classification_monotonic_ns >= hard_expiry_monotonic_ns:
            raise AuditMismatchReportError(
                "a MISMATCH classification reached or exceeded its hard expiry"
            )
        item.classification_monotonic_ns = classification_monotonic_ns
        item.classification_received_monotonic_ns = classification_received_monotonic_ns

    first_pass = _scan_pass(
        envelope_factory,
        collector_run_id=collector_run_id,
        limits=selected_limits,
        visit=bind_classification,
    )
    if any(item.classification_source is None for item in resolved):
        raise AuditMismatchReportError("one or more classification controls are absent")
    if any(
        item.observation.reported_top_transition is not None
        and (
            item.reported_transition_first_source is None
            or item.reported_transition_latest_source is None
        )
        for item in resolved
    ):
        raise AuditMismatchReportError("reported-top transition raw sources are absent")
    response_sequences = {cast(int, item.response_ingest_seq): item for item in resolved}
    if len(response_sequences) != len(resolved):
        raise AuditMismatchReportError("mismatch observations reuse a REST response")
    request_ids = {cast(str, item.request_id): item for item in resolved}
    if len(request_ids) != len(resolved):
        raise AuditMismatchReportError("mismatch observations reuse an audit request ID")
    retained_rest_levels = 0

    def bind_start_and_response(envelope: RawEventEnvelope) -> None:
        nonlocal retained_rest_levels
        response_item = response_sequences.get(envelope.ingest_seq)
        if response_item is not None:
            if envelope.channel != REST_BOOK_AUDIT:
                raise AuditMismatchReportError(
                    "classification references a non-audit REST response"
                )
            metadata = envelope.transport_metadata
            request_id = _text(metadata.get("request_id"), field="REST response request ID")
            if request_id != response_item.request_id:
                raise AuditMismatchReportError(
                    "classification and REST response request IDs disagree"
                )
            if envelope.connection_id.value != response_item.classification_connection:
                raise AuditMismatchReportError(
                    "classification and REST response connections disagree"
                )
            response_item.attempt = _integer(
                metadata.get("audit_attempt"), field="REST response attempt", minimum=1
            )
            raw_response_boundary = metadata.get("response_boundary_websocket_ordinal")
            raw_barrier = metadata.get("barrier_websocket_ordinal")
            if (raw_response_boundary is None) != (raw_barrier is None):
                raise AuditMismatchReportError(
                    "REST response boundary and barrier must be retained together"
                )
            if raw_response_boundary is not None:
                response_item.response_boundary_ordinal = _integer(
                    raw_response_boundary,
                    field="REST response boundary admission ordinal",
                )
                response_item.barrier_processed_ordinal = _integer(
                    raw_barrier,
                    field="REST response barrier processed ordinal",
                )
            response_item.response_received_monotonic_ns = envelope.received_monotonic.nanoseconds
            try:
                rest = parse_rest_book(envelope.payload_bytes)
                rest_bids, rest_asks = _normalized_depth(
                    _native_levels(rest.bids),
                    _native_levels(rest.asks),
                )
                rest_depth_identity = audit_exact_depth_identity(rest_bids, rest_asks)
            except (PolymarketAdapterError, TypeError, ValueError) as error:
                raise AuditMismatchReportError("cannot parse audit REST response") from error
            observation = response_item.observation
            if observation.native_asset_id is not None and (
                observation.native_asset_id != rest.asset_id
            ):
                raise AuditMismatchReportError("artifact and REST response asset IDs disagree")
            if observation.rest_venue_hash is not None and (
                observation.rest_venue_hash != rest.venue_hash
            ):
                raise AuditMismatchReportError("artifact and REST venue hashes disagree")
            if observation.rest_timestamp_raw is not None and (
                observation.rest_timestamp_raw != rest.timestamp_raw
            ):
                raise AuditMismatchReportError("artifact and REST timestamps disagree")
            response_item.response_source = _source(envelope)
            response_item.native_asset_id = rest.asset_id
            response_item.rest = rest
            response_item.rest_bids = rest_bids
            response_item.rest_asks = rest_asks
            response_item.rest_depth_identity = rest_depth_identity
            retained_rest_levels += len(rest_bids) + len(rest_asks)
            if retained_rest_levels > selected_limits.maximum_total_retained_level_values:
                raise AuditMismatchReportResourceError(
                    "retained REST levels exceed the configured report limit"
                )
            exact = observation.exact_depth_difference
            if exact is not None:
                response_item.rest_identity_mismatch = rest_depth_identity != exact.rest_depth
            return
        if envelope.channel != CONTROL_REST_AUDIT_START:
            return
        metadata_request = envelope.transport_metadata.get("request_id")
        if not isinstance(metadata_request, str):
            return
        start_item = request_ids.get(metadata_request)
        if start_item is None:
            return
        if start_item.start_source is not None:
            raise AuditMismatchReportError("audit request has multiple start controls")
        control = _canonical_control(envelope.payload_bytes, context="audit start control")
        v1_keys = {
            "attempt",
            "connection_id",
            "instrument_id",
            "native_asset_id",
            "request_id",
            "request_started_monotonic_ns",
            "schema",
            "starting_admission_ordinal",
            "starting_book_economic_sha256",
            "starting_book_generation",
            "starting_processed_ordinal",
        }
        schema = control.get("schema")
        if schema == AUDIT_START_V1_SCHEMA:
            expected_keys = v1_keys
        elif schema == AUDIT_START_V2_SCHEMA:
            expected_keys = v1_keys | {"trigger_reason"}
            try:
                trigger_reason = AuditTriggerReason(
                    _text(
                        control.get("trigger_reason"),
                        field="audit start trigger reason",
                        maximum=128,
                    )
                )
            except ValueError as error:
                raise AuditMismatchReportError("audit start trigger reason is unknown") from error
            if trigger_reason is AuditTriggerReason.UNKNOWN_LEGACY:
                raise AuditMismatchReportError(
                    "v2 audit start cannot claim a legacy-unknown trigger"
                )
        else:
            raise AuditMismatchReportError("audit start control schema is invalid")
        if set(control) != expected_keys:
            raise AuditMismatchReportError("audit start control schema is invalid")
        if _text(control.get("request_id"), field="audit start request ID") != metadata_request:
            raise AuditMismatchReportError("audit start payload and metadata request IDs disagree")
        instrument_id = InstrumentId(
            _text(control.get("instrument_id"), field="audit start instrument ID")
        )
        if instrument_id != start_item.observation.instrument_id:
            raise AuditMismatchReportError("artifact and audit start instruments disagree")
        connection_id = _text(control.get("connection_id"), field="audit start connection ID")
        if (
            connection_id != envelope.connection_id.value
            or connection_id != start_item.classification_connection
        ):
            raise AuditMismatchReportError("audit start connection identity is inconsistent")
        start_item.start_ingest_seq = envelope.ingest_seq
        start_item.start_source = _source(envelope)
        native_asset_id = _text(control.get("native_asset_id"), field="audit start native asset ID")
        if start_item.observation.native_asset_id is not None and (
            start_item.observation.native_asset_id != native_asset_id
        ):
            raise AuditMismatchReportError("artifact and audit start asset IDs disagree")
        start_item.native_asset_id = native_asset_id
        attempt = _integer(control.get("attempt"), field="audit start attempt", minimum=1)
        if start_item.attempt is not None and start_item.attempt != attempt:
            raise AuditMismatchReportError("audit start and REST response attempts disagree")
        start_item.attempt = attempt
        start_item.request_started_monotonic_ns = _integer(
            control.get("request_started_monotonic_ns"), field="request start time"
        )
        start_item.starting_admission_ordinal = _integer(
            control.get("starting_admission_ordinal"), field="starting admission ordinal"
        )
        start_item.starting_processed_ordinal = _integer(
            control.get("starting_processed_ordinal"), field="starting processed ordinal"
        )
        starting_book_generation = _integer(
            control.get("starting_book_generation"), field="starting book generation"
        )
        economic_hash = _text(
            control.get("starting_book_economic_sha256"),
            field="starting book economic hash",
            maximum=64,
        )
        if not is_sha256_hex(economic_hash):
            raise AuditMismatchReportError("starting book economic hash is invalid")
        start_item.starting_book = AuditBookIdentity(
            instrument_id=instrument_id,
            generation=starting_book_generation,
            economic_content_sha256=economic_hash,
        )

    second_pass = _scan_pass(
        envelope_factory,
        collector_run_id=collector_run_id,
        limits=selected_limits,
        visit=bind_start_and_response,
    )
    _require_equal_passes(first_pass, second_pass)
    for item in resolved:
        if (
            item.start_ingest_seq is None
            or item.start_source is None
            or item.response_source is None
            or item.rest is None
            or item.rest_depth_identity is None
            or item.native_asset_id is None
            or item.attempt is None
            or item.response_ingest_seq is None
            or item.request_id is None
            or item.request_started_monotonic_ns is None
            or item.classification_monotonic_ns is None
            or item.classification_received_monotonic_ns is None
            or item.response_received_monotonic_ns is None
            or item.starting_admission_ordinal is None
            or item.starting_processed_ordinal is None
            or item.starting_book is None
        ):
            raise AuditMismatchReportError("one or more mismatch raw joins are incomplete")
        if (
            not item.start_ingest_seq
            < item.response_ingest_seq
            < (item.observation.classification_ingest_seq)
        ):
            raise AuditMismatchReportError("audit start/response/classification order is invalid")
        if item.native_asset_id != item.rest.asset_id:
            raise AuditMismatchReportError("audit start and REST response asset IDs disagree")
        if (
            item.response_boundary_ordinal is not None
            and item.barrier_processed_ordinal is not None
        ):
            if item.response_boundary_ordinal != item.barrier_processed_ordinal:
                raise AuditMismatchReportError(
                    "a MISMATCH observation must have an equal response boundary and barrier"
                )
            if item.starting_admission_ordinal > item.response_boundary_ordinal:
                raise AuditMismatchReportError(
                    "audit starting admission ordinal exceeds its response boundary"
                )
            if item.starting_processed_ordinal > item.barrier_processed_ordinal:
                raise AuditMismatchReportError(
                    "audit starting processed ordinal exceeds its barrier"
                )
        if not (
            item.request_started_monotonic_ns
            <= item.response_received_monotonic_ns
            <= item.classification_received_monotonic_ns
        ):
            raise AuditMismatchReportError("audit raw monotonic chronology is invalid")
        item.start_socket_generation = connection_generations.get(
            ConnectionId(cast(str, item.classification_connection))
        )

    working = [_WorkingLineage(resolved=item) for item in resolved]
    by_asset: dict[str, list[_WorkingLineage]] = {}
    reference_by_asset: dict[str, list[_WorkingLineage]] = {}
    for item in working:
        asset_id = cast(str, item.resolved.native_asset_id)
        by_asset.setdefault(asset_id, []).append(item)
        observation = item.resolved.observation
        if (
            observation.exact_depth_difference is not None
            and observation.difference_counts.difference_count == 0
            and observation.local_tick_size is not None
        ):
            reference_by_asset.setdefault(asset_id, []).append(item)
    tracker = _LevelResourceTracker(retained=retained_rest_levels, peak=retained_rest_levels)
    exact_actions_considered = 0
    exact_action_comparisons = 0

    def reserve_comparisons(count: int) -> None:
        nonlocal exact_action_comparisons
        remaining = selected_limits.maximum_exact_action_comparisons - exact_action_comparisons
        if count > remaining:
            raise AuditMismatchReportResourceError(
                "audit-mismatch exact action-to-observation comparisons exceed the configured limit"
            )
        exact_action_comparisons += count

    def join_exact_lineage(envelope: RawEventEnvelope) -> None:
        nonlocal exact_actions_considered
        if envelope.channel == REST_BOOK:
            try:
                recovery_rest = parse_rest_book(envelope.payload_bytes)
                recovery_bids, recovery_asks = _normalized_depth(
                    _native_levels(recovery_rest.bids),
                    _native_levels(recovery_rest.asks),
                )
            except (PolymarketAdapterError, TypeError, ValueError) as error:
                raise AuditMismatchReportError(
                    f"cannot parse recovery REST book at ingest sequence {envelope.ingest_seq}"
                ) from error
            recovery_candidates = by_asset.get(recovery_rest.asset_id, ())
            reserve_comparisons(len(recovery_candidates))
            for item in recovery_candidates:
                resolved_item = item.resolved
                if envelope.ingest_seq <= resolved_item.observation.classification_ingest_seq:
                    continue
                # This raw channel is a recovery candidate only.  The lineage
                # report has no canonically replayed apply result, resulting
                # health, or exact owner identity, so it cannot claim that the
                # local baseline was authoritatively replaced.
                item.causal_rest_resnapshot_candidate_seen = True
                if resolved_item.first_rest_resnapshot is None:
                    resolved_item.first_rest_resnapshot = _RecoveryRawStage(
                        ingest_seq=envelope.ingest_seq,
                        source=_source(envelope),
                        connection_id=envelope.connection_id,
                        subscription_id=envelope.subscription_id,
                        socket_generation=None,
                        received_monotonic_ns=envelope.received_monotonic.nanoseconds,
                        event_type="rest_book",
                        exact_depth_identity=audit_exact_depth_identity(
                            recovery_bids,
                            recovery_asks,
                        ),
                    )
            return
        if envelope.channel != WS_MARKET:
            return
        for action in _exact_actions(
            envelope,
            target_assets=set(by_asset),
            reference_target_assets=set(reference_by_asset),
        ):
            exact_actions_considered += 1
            candidates = (
                reference_by_asset[action.native_asset_id]
                if isinstance(action, _PrecisionAction)
                else by_asset[action.native_asset_id]
            )
            # Reserve the complete fan-out before examining any candidate for
            # this action.  The report therefore never performs work outside
            # the declared global ceiling or emits a partially classified
            # result when the remaining comparison budget is insufficient.
            reserve_comparisons(len(candidates))
            for item in candidates:
                resolved_item = item.resolved
                classification_seq = resolved_item.observation.classification_ingest_seq
                if isinstance(action, _PrecisionAction):
                    observation = resolved_item.observation
                    rest = cast(RestBookSnapshot, resolved_item.rest)
                    if (
                        action.position.ingest_seq > classification_seq
                        and item.matching_reference_future is None
                        and observation.local_tick_size == action.old_tick_size
                        and action.old_tick_size != action.new_tick_size
                        and rest.tick_size == action.new_tick_size
                    ):
                        item.matching_reference_future = action
                    continue

                start_connection = ConnectionId(cast(str, resolved_item.classification_connection))
                start_generation = resolved_item.start_socket_generation
                action_generation = connection_generations.get(action.connection_id)
                start_admission = cast(int, resolved_item.starting_admission_ordinal)
                response_boundary = resolved_item.response_boundary_ordinal
                is_original = action.connection_id == start_connection
                is_later_generation = (
                    not is_original
                    and start_generation is not None
                    and action_generation is not None
                    and action_generation > start_generation
                    and action.position.ingest_seq > classification_seq
                )

                if is_original:
                    admission = action.admission_ordinal
                    ordered_after_boundary = (
                        admission is not None
                        and response_boundary is not None
                        and admission > response_boundary
                    )
                    already_decided = _causal_search_decided(item)
                    if (
                        not already_decided
                        and ordered_after_boundary
                        and not _reserve_causal_search_frame(
                            item,
                            action.source,
                            limits=selected_limits,
                        )
                    ) or already_decided:
                        book = None
                    else:
                        book = _apply_causal_action(
                            item,
                            action,
                            tracker=tracker,
                            limits=selected_limits,
                        )
                    if already_decided or item.causal_search_overflowed:
                        pass
                    elif (
                        admission is None
                        and book is not None
                        and _causal_book_matches_rest(book, resolved_item)
                    ):
                        item.causal_missing_matching_admission = True
                        item.causal_ambiguous = True
                    elif admission is not None and admission <= start_admission:
                        if action.position.ingest_seq >= cast(
                            int,
                            resolved_item.start_ingest_seq,
                        ):
                            item.causal_ambiguous = True
                        elif book is not None and _causal_book_matches_rest(book, resolved_item):
                            item.causal_prior_match = _causal_match(
                                action,
                                book,
                                socket_generation=action_generation,
                            )
                    elif (
                        admission is not None
                        and response_boundary is not None
                        and admission <= response_boundary
                    ):
                        if action.position.ingest_seq > classification_seq:
                            item.causal_ambiguous = True
                        elif book is not None and _causal_book_matches_rest(book, resolved_item):
                            item.causal_prior_match = _causal_match(
                                action,
                                book,
                                socket_generation=action_generation,
                            )
                    elif not ordered_after_boundary or book is None:
                        item.causal_ambiguous = True
                    else:
                        matches_rest = _causal_book_matches_rest(book, resolved_item)
                        recovery_has_started = item.causal_replacement_seen
                        if not recovery_has_started:
                            item.causal_original_post_frames += 1
                        if item.causal_rest_resnapshot_candidate_seen and not recovery_has_started:
                            # Raw REST evidence alone cannot establish whether
                            # the canonical baseline changed before this frame.
                            item.causal_ambiguous = True
                        elif matches_rest:
                            candidate = _causal_match(
                                action,
                                book,
                                socket_generation=action_generation,
                            )
                            if recovery_has_started:
                                item.causal_recovery_match = candidate
                            else:
                                item.causal_original_match = candidate
                        elif not recovery_has_started:
                            item.causal_original_conflict = True
                elif is_later_generation:
                    item.causal_replacement_seen = True
                    if resolved_item.first_replacement_websocket is None:
                        resolved_item.first_replacement_websocket = _RecoveryRawStage(
                            ingest_seq=action.position.ingest_seq,
                            source=action.source,
                            connection_id=action.connection_id,
                            subscription_id=action.subscription_id,
                            socket_generation=action_generation,
                            received_monotonic_ns=action.received_monotonic_ns,
                            admission_ordinal=action.admission_ordinal,
                            event_type=action.event_type,
                            exact_depth_identity=action.replacement_depth_identity,
                        )
                    if _reserve_causal_search_frame(
                        item,
                        action.source,
                        limits=selected_limits,
                    ):
                        book = _apply_causal_action(
                            item,
                            action,
                            tracker=tracker,
                            limits=selected_limits,
                        )
                        recovery_matches = book is not None and _causal_book_matches_rest(
                            book,
                            resolved_item,
                        )
                        if recovery_matches and action.admission_ordinal is None:
                            item.causal_missing_matching_admission = True
                        if recovery_matches and book is not None:
                            item.causal_recovery_match = _causal_match(
                                action,
                                book,
                                socket_generation=action_generation,
                            )
                elif (
                    action.position.ingest_seq > classification_seq
                    and start_generation is None
                    and not _causal_search_decided(item)
                ):
                    item.causal_ambiguous = True

                matches = _matches_rest_snapshot(action, resolved_item)
                start_seq = cast(int, resolved_item.start_ingest_seq)
                if action.position.ingest_seq < start_seq:
                    if matches:
                        _reset_working(
                            item,
                            action,
                            tracker=tracker,
                            limits=selected_limits,
                        )
                    elif (
                        item.matching_prior is not None
                        and action.position > item.matching_prior.position
                    ):
                        _apply_action(
                            item,
                            action,
                            tracker=tracker,
                            limits=selected_limits,
                        )
                elif action.position.ingest_seq <= classification_seq:
                    if matches:
                        item.ambiguous_match = True
                elif matches and item.matching_future is None:
                    item.matching_future = action

    third_pass = _scan_pass(
        envelope_factory,
        collector_run_id=collector_run_id,
        limits=selected_limits,
        visit=join_exact_lineage,
    )
    _require_equal_passes(first_pass, third_pass)
    results = tuple(_build_result(item) for item in working)
    counts = Counter(result.cause for result in results)
    cause_counts = tuple(
        AuditMismatchCauseCount(cause=cause, count=counts[cause]) for cause in AuditMismatchCause
    )
    return AuditMismatchLineageReport(
        schema_version=AUDIT_MISMATCH_LINEAGE_REPORT_SCHEMA,
        collector_run_id=collector_run_id,
        source_journal_event_count=first_pass.event_count,
        source_journal_identity_sha256=first_pass.identity_sha256,
        observation_count=len(ordered),
        cause_counts=cause_counts,
        results=results,
        result_sequence_sha256=_result_digest(results),
        resources=AuditMismatchReportResources(
            journal_passes=3,
            source_envelopes=first_pass.event_count,
            source_payload_bytes=first_pass.payload_bytes,
            exact_actions_considered=exact_actions_considered,
            exact_action_comparisons=exact_action_comparisons,
            peak_retained_level_values=tracker.peak,
        ),
    )


def build_journal_audit_mismatch_lineage_report(
    raw_root: Path,
    collector_run_id: CollectorRunId,
    observations: Sequence[AuditMismatchObservation],
    *,
    limits: AuditMismatchReportLimits | None = None,
) -> AuditMismatchLineageReport:
    """Read one explicit sealed journal run and build a post-hoc report."""

    requested = raw_root.absolute()
    if requested.is_symlink() or not requested.is_dir():
        raise AuditMismatchReportError("raw journal root must be a non-symlink directory")
    resolved = requested.resolve()

    def envelopes() -> Iterable[RawEventEnvelope]:
        return JournalReader(resolved, collector_run_id).iter_verified()

    return build_audit_mismatch_lineage_report(
        envelopes,
        collector_run_id,
        observations,
        limits=limits,
    )


def _artifact_side(value: object, *, side: str) -> tuple[int, int, int]:
    if not isinstance(value, dict):
        raise AuditMismatchReportError(f"artifact {side} difference must be an object")
    item = cast(dict[object, object], value)
    return (
        _integer(item.get("local_only"), field=f"artifact {side} local-only count"),
        _integer(item.get("rest_only"), field=f"artifact {side} REST-only count"),
        _integer(
            item.get("quantity_differences"),
            field=f"artifact {side} quantity-difference count",
        ),
    )


def load_instrumented_replay_mismatch_observations(
    artifact_path: Path,
    *,
    allowed_root: Path,
    limits: AuditMismatchReportLimits | None = None,
) -> tuple[AuditMismatchObservation, ...]:
    """Load the bounded mismatch subset of the failed-run replay artifact.

    The historical v1 artifact retained exact counts but not the full
    difference digest.  Consequently, equal coarse reconciliation remains
    ``UNKNOWN``; callers can supply observations produced from current exact
    diagnostics to prove those cases without weakening the classifier.
    """

    selected_limits = limits or AuditMismatchReportLimits()
    root = allowed_root.resolve()
    requested = artifact_path.absolute()
    if requested.is_symlink():
        raise AuditMismatchReportError("replay artifact cannot be a symbolic link")
    resolved = requested.resolve()
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise AuditMismatchReportError("replay artifact escapes or is not a regular file")
    if resolved.stat().st_size > selected_limits.maximum_artifact_bytes:
        raise AuditMismatchReportResourceError("replay artifact exceeds the configured byte limit")
    try:
        document = json.loads(resolved.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AuditMismatchReportError("cannot read replay artifact JSON") from error
    if not isinstance(document, dict):
        raise AuditMismatchReportError("replay artifact must be a JSON object")
    root_object = cast(dict[object, object], document)
    if root_object.get("schema_version") != "gate1-instrumented-protocol-replay-v1":
        raise AuditMismatchReportError("unsupported instrumented replay artifact schema")
    mismatch_value = root_object.get("mismatch_differences")
    outcomes_value = root_object.get("outcomes")
    if not isinstance(mismatch_value, dict) or not isinstance(outcomes_value, dict):
        raise AuditMismatchReportError("replay artifact mismatch evidence is missing")
    mismatch = cast(dict[object, object], mismatch_value)
    outcomes = cast(dict[object, object], outcomes_value)
    details_value = mismatch.get("details")
    proofs_value = outcomes.get("mismatch_proofs")
    counts_value = outcomes.get("counts")
    if (
        not isinstance(details_value, list)
        or not isinstance(proofs_value, dict)
        or not isinstance(counts_value, dict)
    ):
        raise AuditMismatchReportError("replay artifact mismatch evidence is invalid")
    details = cast(list[object], details_value)
    count = _integer(mismatch.get("count"), field="artifact mismatch count")
    if count != len(details) or count > selected_limits.maximum_observations:
        raise AuditMismatchReportResourceError(
            "artifact mismatch detail count is inconsistent or exceeds its bound"
        )
    proofs = cast(dict[object, object], proofs_value)
    outcome_counts = cast(dict[object, object], counts_value)
    proof_fields = (
        "barrier_processed_equals_boundary",
        "requires_resync",
        "stable_start_to_barrier_identity",
        "start_admission_not_after_boundary",
        "total",
    )
    if any(proofs.get(field) != count for field in proof_fields) or (
        outcome_counts.get("MISMATCH") != count
    ):
        raise AuditMismatchReportError(
            "artifact does not prove every detail is one stable fenced MISMATCH"
        )
    observations: list[AuditMismatchObservation] = []
    for value in details:
        if not isinstance(value, dict):
            raise AuditMismatchReportError("artifact mismatch detail must be an object")
        detail = cast(dict[object, object], value)
        bid_local, bid_rest, bid_quantity = _artifact_side(detail.get("bid"), side="bid")
        ask_local, ask_rest, ask_quantity = _artifact_side(detail.get("ask"), side="ask")
        top_changed = detail.get("top_changed")
        if not isinstance(top_changed, bool):
            raise AuditMismatchReportError("artifact top-changed evidence must be boolean")
        observations.append(
            AuditMismatchObservation(
                classification_ingest_seq=_integer(
                    detail.get("ingest_seq"), field="artifact classification ingest sequence"
                ),
                instrument_id=InstrumentId(
                    _text(detail.get("instrument_id"), field="artifact instrument ID")
                ),
                difference_counts=AuditMismatchDifferenceCounts(
                    bid_difference_count=bid_local + bid_rest + bid_quantity,
                    ask_difference_count=ask_local + ask_rest + ask_quantity,
                    local_only_count=bid_local + ask_local,
                    rest_only_count=bid_rest + ask_rest,
                    quantity_difference_count=bid_quantity + ask_quantity,
                    top_changed=top_changed,
                ),
                rest_venue_hash=_bounded_text(
                    detail.get("venue_hash"), field="artifact REST venue hash", maximum=512
                ),
                rest_timestamp_raw=_bounded_text(
                    detail.get("venue_timestamp"),
                    field="artifact REST venue timestamp",
                    maximum=128,
                ),
            )
        )
    ordered = tuple(sorted(observations, key=lambda item: item.classification_ingest_seq))
    if len({item.classification_ingest_seq for item in ordered}) != len(ordered):
        raise AuditMismatchReportError("artifact repeats a mismatch classification sequence")
    return ordered


__all__ = [
    "AUDIT_MISMATCH_LINEAGE_REPORT_SCHEMA",
    "AuditEqualityProof",
    "AuditMatchingStateRepresentation",
    "AuditMismatchCausalRelation",
    "AuditMismatchCauseCount",
    "AuditMismatchDifferenceCounts",
    "AuditMismatchLineageReason",
    "AuditMismatchLineageReport",
    "AuditMismatchLineageResult",
    "AuditMismatchObservation",
    "AuditMismatchReportError",
    "AuditMismatchReportLimits",
    "AuditMismatchReportResourceError",
    "AuditMismatchReportResources",
    "AuditRecoveryChronology",
    "AuditRecoveryEvidenceStatus",
    "AuditRecoveryStageEvidence",
    "AuditVenueStateRelation",
    "build_audit_mismatch_lineage_report",
    "build_journal_audit_mismatch_lineage_report",
    "load_instrumented_replay_mismatch_observations",
]
