"""One deterministic parser-to-state-to-scanner path for observation and replay."""

from __future__ import annotations

import hashlib
import json
import sys
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final, Protocol, cast

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityAuthoritySnapshot,
    AuditAvailabilityBinding,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
)
from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.core.errors import ProgrammingInvariantViolation
from multi_market_arbitrage.core.exact import ExactDecimal, Price
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    ConnectionId,
    CostRuleSetVersionId,
    IngestId,
    InstrumentId,
    MarketId,
    MarketVersionId,
    PrecisionRuleVersionId,
    VersionId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import (
    Provenance,
    RawSourceInvariantError,
    RawSourceRef,
    merge_raw_source_refs,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.core.validation import ValidationReasonCode
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalDurabilityState, JournalReceipt
from multi_market_arbitrage.normalization import (
    BestBidAskEvent,
    BookSide,
    CanonicalEvent,
    CanonicalEventType,
    MarketResolvedEvent,
    NewMarketEvent,
    NormalizationContext,
    NormalizationError,
    NormalizedRestBook,
    OrderBookSnapshotEvent,
    PolymarketNormalizer,
    PriceChangeEvent,
    TickSizeChangeEvent,
)
from multi_market_arbitrage.reference import (
    PolymarketReferenceBuilder,
    ReferenceBuildError,
    ReferenceBundle,
    ReferenceRegistry,
)
from multi_market_arbitrage.scanners.models import OpportunityLifecycleStatus
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScannerConfig,
    CompleteSetScanResult,
    OpportunityLifecycle,
    OpportunityLifecycleTracker,
    PolymarketCompleteSetScanner,
    ScanDisposition,
    complete_set_lifecycle_id,
)
from multi_market_arbitrage.semantic import BinarySemanticProof, prove_binary_complement
from multi_market_arbitrage.state.order_books import (
    BookMutation,
    BookStateError,
    CanonicalL2BookOwner,
    ImmutableOrderBookSnapshot,
    InvalidationReason,
    RestAuditStatus,
)
from multi_market_arbitrage.venues.polymarket.errors import (
    MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS,
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
    PolymarketSchemaError,
)
from multi_market_arbitrage.venues.polymarket.schemas import (
    ClobMarketInfo,
    GammaEvent,
    GammaMarket,
    JsonNumber,
    RestBookSnapshot,
    WsNewMarketEvent,
    decode_json_exact,
    parse_clob_market_info,
    parse_gamma_event,
    parse_gamma_market,
    parse_gamma_market_relationship,
    parse_gamma_markets,
    parse_market_ws_message,
    parse_rest_book,
)

from .audit import (
    AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    AUDIT_CLASSIFICATION_PREPARE_SCHEMA,
    AUDIT_CLASSIFICATION_V1_SCHEMA,
    AUDIT_CLASSIFICATION_V2_SCHEMA,
    AUDIT_EXPIRY_SCHEMA,
    AUDIT_FAILURE_V1_SCHEMA,
    AUDIT_FAILURE_V2_SCHEMA,
    AUDIT_REPLACEMENT_RELEASE_SCHEMA,
    AUDIT_START_V1_SCHEMA,
    AUDIT_START_V2_SCHEMA,
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditClassificationPrepareCommand,
    AuditDivergencePending,
    AuditDivergenceResolution,
    AuditExpiryCommand,
    AuditFailureCommand,
    AuditFailureReason,
    AuditFaultFailureEvidenceKind,
    AuditFaultLifecycleRecord,
    AuditFaultOrigin,
    AuditFaultResolution,
    AuditFaultStatus,
    AuditMismatchCause,
    AuditMismatchDiagnostic,
    AuditOutcome,
    AuditOutcomeEvidence,
    AuditReplacementReleaseCommand,
    AuditRestBookIdentity,
    AuditStartEvidence,
    AuditTriggerReason,
    WebSocketAdmissionWatermark,
    audit_exact_depth_identity,
    audit_fault_evidence_blocks_current_scope,
    audit_fault_origin,
    build_audit_mismatch_diagnostic,
    compare_audit_exact_depth,
    initial_audit_fault_status,
)
from .channels import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REPORTED_TOP_TRANSITION_EXPIRED,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
    CONTROL_REST_AUDIT_EXPIRED,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_START,
    GAMMA_EVENT_PREFIX,
    GAMMA_MARKET_PREFIX,
    GAMMA_MARKET_RELATIONSHIP_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    channel_native_id,
)
from .transitions import (
    REPORTED_TOP_TRANSITION_EXPIRY_SCHEMA,
    ReportedTopTransition,
    ReportedTopTransitionBaselineEvidence,
    ReportedTopTransitionCloseSource,
    ReportedTopTransitionEntrySource,
    ReportedTopTransitionExpiryCommand,
    ReportedTopTransitionOutcome,
    ReportedTopTransitionReason,
    ReportedTopTransitionStatus,
)

_SCAN_RESULT_HISTORY_LIMIT = 256
_VALIDATED_OPPORTUNITY_HISTORY_LIMIT = 1024
_REFERENCE_REFRESH_HISTORY_LIMIT = 256
_AUDIT_OUTCOME_HISTORY_LIMIT = 256
_AUDIT_MISMATCH_DIAGNOSTIC_HISTORY_LIMIT = 64
_AUDIT_DIVERGENCE_RESOLUTION_HISTORY_LIMIT = 64
_AUDIT_FAULT_LIFECYCLE_HISTORY_LIMIT = 64
_REPORTED_TOP_TRANSITION_OUTCOME_HISTORY_LIMIT = 256
MAXIMUM_ACTIVE_AUDIT_FAULTS = 4096
MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES = 2
_MAX_TRACKED_WEBSOCKET_CONNECTIONS = 4096
_HTTP_ENDPOINT_CLASSES = tuple(
    endpoint
    for endpoint in sorted(PolymarketEndpointClass, key=lambda item: item.value)
    if endpoint is not PolymarketEndpointClass.MARKET_WEBSOCKET
)
_CHAIN_NAMESPACE = b"mma-canonical-processor-chain-v1\0"
_TERMINAL_HASH_SCHEMA = "mma-canonical-processor-terminal-v3"
_V1_AUDIT_GENERATION_REBASE_SOURCE_COMMITS = frozenset(
    {
        # Failed Gate-1 producer whose pre-ADR-0053 generation semantics are
        # preserved in immutable raw audit-start controls.
        "a382b93220011428703fe02483f7034f59c1f045",
    }
)
_HISTORICAL_DIRECT_DEFERRED_AUDIT_SOURCE_COMMITS = frozenset(
    {
        # Immutable retained producers that predate PREPARE_ATTEST_V1.  Their
        # source completion head is the positive compatibility authority;
        # omission of the selector is not accepted for any newer producer.
        "a382b93220011428703fe02483f7034f59c1f045",
        "ff7054ac2167026ce1ed14111b7e144cfd4fd865",
        "ee240c51184cccfc6e425906097758b5ca019a95",
    }
)


def _chain_seed(name: str) -> bytes:
    hasher = hashlib.sha256()
    hasher.update(_CHAIN_NAMESPACE)
    hasher.update(name.encode("ascii"))
    return hasher.digest()


def _chain_extend(previous: bytes, name: str, value: object) -> bytes:
    payload = canonical_json_bytes(value)
    hasher = hashlib.sha256()
    hasher.update(_CHAIN_NAMESPACE)
    hasher.update(name.encode("ascii"))
    hasher.update(previous)
    hasher.update(len(payload).to_bytes(8, "big"))
    hasher.update(payload)
    return hasher.digest()


@dataclass(frozen=True, slots=True)
class ReferenceRefreshRequest:
    """Deterministic external-reference work requested by a canonical event."""

    source_event_id: CanonicalEventId
    event_type: CanonicalEventType
    recovery_reason: ProcessorRecoveryReason
    market_id: MarketId | None
    native_market_id: str
    native_condition_id: str | None
    native_asset_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.recovery_reason not in {
            ProcessorRecoveryReason.REFERENCE_REFRESH,
            ProcessorRecoveryReason.TARGET_LIFECYCLE,
        }:
            raise ValueError("reference refresh request requires a typed refresh/lifecycle reason")
        lifecycle = self.event_type is CanonicalEventType.MARKET_RESOLVED
        if lifecycle != (self.recovery_reason is ProcessorRecoveryReason.TARGET_LIFECYCLE):
            raise ValueError("reference refresh reason conflicts with its canonical event type")


class ScanTriggerReason(StrEnum):
    """Authoritative transition categories that make a market scan-dirty."""

    BOOK_UPDATE = "BOOK_UPDATE"
    REST_SNAPSHOT = "REST_SNAPSHOT"
    REST_AUDIT = "REST_AUDIT"
    REFERENCE = "REFERENCE"
    LIFECYCLE = "LIFECYCLE"
    INVALIDATION = "INVALIDATION"


class ProcessorRecoveryReason(StrEnum):
    """Finite ADR-0055 processor-to-coordinator recovery taxonomy."""

    AUDIT_MISMATCH_RECOVERY = "AUDIT_MISMATCH_RECOVERY"
    REPORTED_TOP_TRANSITION_FAILURE = "REPORTED_TOP_TRANSITION_FAILURE"
    FRESHNESS_EXPIRY = "FRESHNESS_EXPIRY"
    REFERENCE_REFRESH = "REFERENCE_REFRESH"
    TRANSPORT_DISCONNECT = "TRANSPORT_DISCONNECT"
    CANONICAL_STATE_INVALIDATION = "CANONICAL_STATE_INVALIDATION"
    SCHEMA_PROTOCOL_ERROR = "SCHEMA_PROTOCOL_ERROR"
    BACKPRESSURE_RESOURCE = "BACKPRESSURE_RESOURCE"
    TARGET_LIFECYCLE = "TARGET_LIFECYCLE"
    UNKNOWN = "UNKNOWN"


_INVALIDATION_RECOVERY_REASONS: Final[dict[InvalidationReason, ProcessorRecoveryReason]] = {
    InvalidationReason.DISCONNECT: ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
    InvalidationReason.SCHEMA_VIOLATION: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
    InvalidationReason.INVALID_PRICE: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
    InvalidationReason.INVALID_QUANTITY: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
    InvalidationReason.UNKNOWN_PRECISION: ProcessorRecoveryReason.REFERENCE_REFRESH,
    InvalidationReason.UNKNOWN_REFERENCE_DATA: ProcessorRecoveryReason.REFERENCE_REFRESH,
    InvalidationReason.BACKPRESSURE_DATA_LOSS: ProcessorRecoveryReason.BACKPRESSURE_RESOURCE,
    InvalidationReason.STALE_SNAPSHOT: ProcessorRecoveryReason.FRESHNESS_EXPIRY,
    InvalidationReason.IMPOSSIBLE_STATE: ProcessorRecoveryReason.CANONICAL_STATE_INVALIDATION,
    InvalidationReason.PROTOCOL_VIOLATION: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
    InvalidationReason.REST_AUDIT_MISMATCH: ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY,
    InvalidationReason.MARKET_RESOLVED_REFERENCE_REFRESH: (
        ProcessorRecoveryReason.TARGET_LIFECYCLE
    ),
}


def processor_recovery_reason_for_invalidation(
    reason: InvalidationReason | None,
) -> ProcessorRecoveryReason:
    """Map complete canonical invalidation evidence without guessing missing evidence."""

    if reason is None:
        return ProcessorRecoveryReason.UNKNOWN
    try:
        return _INVALIDATION_RECOVERY_REASONS[reason]
    except KeyError as error:  # pragma: no cover - enum/mapping exhaustiveness is tested
        raise ProgrammingInvariantViolation(
            f"canonical invalidation lacks a recovery taxonomy mapping: {reason!r}",
            component="canonical-processor",
        ) from error


@dataclass(frozen=True, slots=True)
class ProcessorRecoveryRequest:
    """Exact, bounded reason a processed envelope requires recovery."""

    reason: ProcessorRecoveryReason
    source: RawSourceRef
    affected_instrument_ids: tuple[InstrumentId, ...]
    original_reason_code: str
    effective_invalidation_reason: InvalidationReason | None = None
    audit_request_id: str | None = None
    audit_divergence_id: str | None = None
    reported_top_transition_id: str | None = None

    def __post_init__(self) -> None:
        if not self.affected_instrument_ids:
            raise ValueError("processor recovery request requires an affected instrument")
        if len(set(self.affected_instrument_ids)) != len(self.affected_instrument_ids):
            raise ValueError("processor recovery instruments must be unique")
        if not self.original_reason_code or len(self.original_reason_code) > 256:
            raise ValueError("processor recovery original reason must be bounded text")
        if self.reason is ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY and (
            self.audit_request_id is None or self.audit_divergence_id is None
        ):
            raise ValueError("audit mismatch recovery requires request and divergence IDs")
        if self.reason is ProcessorRecoveryReason.UNKNOWN and (
            self.effective_invalidation_reason is not None
        ):
            raise ValueError("known invalidation evidence cannot use UNKNOWN recovery reason")
        if self.effective_invalidation_reason is not None:
            mapped = processor_recovery_reason_for_invalidation(self.effective_invalidation_reason)
            if self.reason not in {
                mapped,
                ProcessorRecoveryReason.REPORTED_TOP_TRANSITION_FAILURE,
            }:
                raise ValueError("processor recovery reason conflicts with invalidation evidence")


class _RestAuditProtocolMode(StrEnum):
    LEGACY = "LEGACY"
    DEFERRED_BOUND = "DEFERRED_BOUND"


class _AuditReplacementSnapshotEvidenceError(NormalizationError):
    """A rejected v2 capture frame whose canonical payload was not applied."""


class _AuditFaultCapacityError(NormalizationError):
    """An audit transaction cannot reserve every required lifecycle slot."""


@dataclass(frozen=True, slots=True)
class MarketScanTrigger:
    market_id: MarketId
    reasons: tuple[ScanTriggerReason, ...]


@dataclass(frozen=True, slots=True)
class BookFreshnessUpdate:
    """Monotonic receive boundary that advances one book's freshness deadline."""

    market_id: MarketId
    instrument_id: InstrumentId
    received_monotonic: MonotonicTimestamp


@dataclass(frozen=True, slots=True)
class ProcessedEnvelope:
    ingest_seq: int
    success: bool
    canonical_event_count: int
    scan_count: int
    needs_resync: bool
    affected_instrument_ids: tuple[InstrumentId, ...]
    reference_refresh_requests: tuple[ReferenceRefreshRequest, ...]
    book_freshness_updates: tuple[BookFreshnessUpdate, ...]
    scan_triggers: tuple[MarketScanTrigger, ...]
    error_type: str | None
    # Receive activity relevant to audit-race quiet windows.  This is separate
    # from authoritative book freshness because quantity-free BBO messages must
    # not extend scanner eligibility or REST-audit hard-expiry deadlines.
    relevant_l2_receive_updates: tuple[BookFreshnessUpdate, ...] = ()
    audit_outcomes: tuple[AuditOutcomeEvidence, ...] = ()
    scan_source_received_monotonic_ns: int | None = None
    source: RawSourceRef | None = None
    recovery_requests: tuple[ProcessorRecoveryRequest, ...] = ()
    audit_divergences_opened: tuple[AuditDivergencePending, ...] = ()
    audit_divergences_resolved: tuple[AuditDivergenceResolution, ...] = ()
    audit_divergences_eligibility_restored: tuple[AuditDivergenceEligibilityRestoration, ...] = ()


@dataclass(frozen=True, slots=True)
class AuditDivergenceEligibilityRestoration:
    """Exact v2 authority emitted when one durable release is accepted.

    This is a streaming processor result, not retained diagnostic history.  It
    binds the accepted release control to the complete shared availability
    binding and to the exact replacement-capture digest committed by that
    control.  Historical v1 snapshot-only cleanup deliberately emits no such
    authority because those journals predate both the release control and the
    contribution binding.
    """

    divergence_id: str
    request_id: str
    instrument_id: InstrumentId
    market_id: MarketId
    audit_start_schema: str
    authoritative_snapshot_source: RawSourceRef
    replacement_release_source: RawSourceRef
    replacement_connection_id: ConnectionId
    capture_state_sha256: str
    availability_binding: AuditAvailabilityBinding
    restored_ingest_seq: int
    restored_at: UtcTimestamp
    restored_monotonic: MonotonicTimestamp

    def __post_init__(self) -> None:
        if self.audit_start_schema != AUDIT_START_V2_SCHEMA:
            raise ValueError("eligibility-restoration authority requires a v2 audit start")
        if not is_sha256_hex(self.divergence_id) or not is_sha256_hex(self.capture_state_sha256):
            raise ValueError("eligibility-restoration identities must be SHA-256 hex")
        if self.restored_ingest_seq < 0:
            raise ValueError("eligibility-restoration ingest sequence cannot be negative")
        binding = self.availability_binding
        if (
            binding.divergence_id != self.divergence_id
            or binding.request_id != self.request_id
            or binding.instrument_id != self.instrument_id.value
            or binding.market_id != self.market_id.value
            or binding.authoritative_resolution_source != self.authoritative_snapshot_source
            or binding.replacement_release_source != self.replacement_release_source
        ):
            raise ValueError(
                "eligibility-restoration event differs from its availability authority"
            )


@dataclass(frozen=True, slots=True)
class _RawReferenceSource:
    ingest_id: IngestId
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class _PostSnapshotBaseline:
    """One ordinal-1 two-leg baseline eligible for the narrow catch-up rule."""

    market_id: MarketId
    evidence: ReportedTopTransitionBaselineEvidence


@dataclass(frozen=True, slots=True)
class _AuditFaultRecordingContext:
    """Exact v2 failure metadata active for one outcome-recording call."""

    command: AuditFailureCommand
    original_reason: AuditFailureReason


@dataclass(frozen=True, slots=True)
class _PreparedAuditClassification:
    """Bounded, no-outcome first phase awaiting its durable attestation."""

    command: AuditClassificationPrepareCommand
    envelope: RawEventEnvelope
    processed_watermark: WebSocketAdmissionWatermark
    book: AuditBookIdentity
    owner_health: BookHealth
    reported_top_transition: ReportedTopTransition | None
    audit_ineligible_nonfault: bool
    audit_fault_current_count: int
    audit_fault_scope_block_count: int


@dataclass(frozen=True, slots=True)
class ProcessorAuditReplacementSnapshotSource:
    """One bounded full-snapshot source retained by a current capture."""

    instrument_id: InstrumentId
    connection_id: ConnectionId
    admission_ordinal: int
    ingest_seq: int
    received_monotonic: MonotonicTimestamp
    raw_source: RawSourceRef

    def __post_init__(self) -> None:
        if min(self.admission_ordinal, self.ingest_seq) < 0:
            raise ValueError("audit replacement snapshot ordinals cannot be negative")


@dataclass(frozen=True, slots=True)
class ProcessorAuditReplacementCaptureSnapshot:
    """Complete bounded authority for one currently pending replacement capture.

    The pre-open connection set is represented by its exact count and canonical
    identity rather than copied into periodic health.  ``audit_start_schema``
    is additive operational authority; the immutable terminal-hash projection
    deliberately continues to use the legacy-compatible capture record.
    """

    instrument_id: InstrumentId
    divergence_id: str
    market_id: MarketId
    original_connection_id: ConnectionId
    required_instrument_ids: tuple[InstrumentId, ...]
    audit_start_schema: str
    resolution_source: RawSourceRef
    resolution_ingest_seq: int
    resolution_monotonic: MonotonicTimestamp
    pre_open_connection_count: int
    pre_open_connection_ids_sha256: str
    mismatch_cause: AuditMismatchCause
    availability_binding: AuditAvailabilityBinding | None
    replacement_connection_id: ConnectionId | None
    captured_snapshot_sources: tuple[ProcessorAuditReplacementSnapshotSource, ...]

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.divergence_id):
            raise ValueError("audit replacement divergence ID must be SHA-256 hex")
        if self.audit_start_schema not in {AUDIT_START_V1_SCHEMA, AUDIT_START_V2_SCHEMA}:
            raise ValueError("audit replacement capture start schema is unsupported")
        if min(self.resolution_ingest_seq, self.pre_open_connection_count) < 0:
            raise ValueError("audit replacement capture counters cannot be negative")
        if not is_sha256_hex(self.pre_open_connection_ids_sha256):
            raise ValueError("audit replacement pre-open identity must be SHA-256 hex")
        if self.instrument_id not in self.required_instrument_ids:
            raise ValueError("audit replacement authority key must be in its required scope")
        if self.audit_start_schema == AUDIT_START_V2_SCHEMA:
            binding = self.availability_binding
            if (
                binding is None
                or binding.divergence_id != self.divergence_id
                or binding.instrument_id != self.instrument_id.value
                or binding.market_id != self.market_id.value
                or binding.authoritative_resolution_source != self.resolution_source
                or binding.replacement_release_source is not None
            ):
                raise ValueError("v2 replacement capture availability binding is inconsistent")
        elif self.availability_binding is not None:
            raise ValueError("legacy replacement capture cannot invent availability authority")
        if (
            tuple(sorted(set(self.required_instrument_ids), key=lambda item: item.value))
            != self.required_instrument_ids
        ):
            raise ValueError("audit replacement instruments must be unique and sorted")
        captured_instrument_ids = tuple(
            source.instrument_id for source in self.captured_snapshot_sources
        )
        if (
            tuple(sorted(set(captured_instrument_ids), key=lambda item: item.value))
            != captured_instrument_ids
        ):
            raise ValueError("captured replacement instruments must be unique and sorted")
        if not set(captured_instrument_ids).issubset(self.required_instrument_ids):
            raise ValueError("replacement capture contains an unrelated instrument")
        if self.captured_snapshot_sources and self.replacement_connection_id is None:
            raise ValueError("replacement capture sources require a connection")
        if self.replacement_connection_id == self.original_connection_id:
            raise ValueError("replacement capture cannot use the mismatched connection")
        if any(
            source.connection_id != self.replacement_connection_id
            for source in self.captured_snapshot_sources
        ):
            raise ValueError("replacement snapshot sources span socket connections")


@dataclass(frozen=True, slots=True)
class _AuditReplacementSnapshotSource:
    instrument_id: InstrumentId
    connection_id: ConnectionId
    admission_ordinal: int
    ingest_seq: int
    received_monotonic: MonotonicTimestamp
    raw_source: RawSourceRef


@dataclass(frozen=True, slots=True)
class _AuditReplacementCapture:
    """Bounded proof still required after an authoritative REST recovery.

    ADR-0054 separates instrument-baseline restoration from global research
    eligibility.  A resolved divergence therefore remains audit-ineligible
    until one replacement WebSocket connection has supplied full snapshots for
    every instrument in the affected market.
    """

    divergence_id: str
    market_id: MarketId
    original_connection_id: ConnectionId
    required_instrument_ids: tuple[InstrumentId, ...]
    # This internal discriminator is intentionally omitted from ``to_record``.
    # ``divergence_id`` already binds the audit-start schema, while preserving
    # the exact legacy v1 capture/history serialization keeps its durable
    # identity preimage stable. Immutable historical terminal hashes remain
    # evidence; terminal-v3 replay intentionally produces a distinct hash.
    audit_start_schema: str
    resolution_source: RawSourceRef
    resolution_ingest_seq: int
    resolution_monotonic: MonotonicTimestamp
    pre_open_connection_ids: frozenset[ConnectionId]
    pre_open_connection_count: int
    pre_open_connection_ids_sha256: str
    mismatch_cause: AuditMismatchCause
    # Additive current-state authority, intentionally omitted from ``to_record``
    # so the durable v1/v2 capture/history identity preimage remains stable.
    # This does not promise equality with a historical pre-v3 terminal hash.
    availability_binding: AuditAvailabilityBinding | None
    replacement_connection_id: ConnectionId | None = None
    captured_snapshot_sources: tuple[_AuditReplacementSnapshotSource, ...] = ()

    def __post_init__(self) -> None:
        if self.audit_start_schema not in {AUDIT_START_V1_SCHEMA, AUDIT_START_V2_SCHEMA}:
            raise ValueError("audit replacement capture start schema is unsupported")
        if self.audit_start_schema == AUDIT_START_V2_SCHEMA:
            if (
                self.availability_binding is None
                or self.availability_binding.divergence_id != self.divergence_id
                or self.availability_binding.market_id != self.market_id.value
                or self.availability_binding.authoritative_resolution_source
                != self.resolution_source
                or self.availability_binding.replacement_release_source is not None
            ):
                raise ValueError("v2 replacement capture availability binding is inconsistent")
        elif self.availability_binding is not None:
            raise ValueError("legacy replacement capture cannot invent availability authority")
        if self.resolution_ingest_seq < 0:
            raise ValueError("audit replacement capture resolution sequence cannot be negative")
        if self.pre_open_connection_count != len(self.pre_open_connection_ids):
            raise ValueError("audit replacement pre-open connection count is inconsistent")
        if not is_sha256_hex(self.pre_open_connection_ids_sha256):
            raise ValueError("audit replacement pre-open identity must be SHA-256 hex")
        if not self.required_instrument_ids:
            raise ValueError("audit replacement capture requires affected instruments")
        if tuple(sorted(set(self.required_instrument_ids), key=lambda item: item.value)) != (
            self.required_instrument_ids
        ):
            raise ValueError("audit replacement instruments must be unique and sorted")
        captured_instrument_ids = tuple(
            source.instrument_id for source in self.captured_snapshot_sources
        )
        if tuple(sorted(set(captured_instrument_ids), key=lambda item: item.value)) != (
            captured_instrument_ids
        ):
            raise ValueError("captured replacement instruments must be unique and sorted")
        if not set(captured_instrument_ids).issubset(self.required_instrument_ids):
            raise ValueError("replacement capture contains an unrelated instrument")
        if self.captured_snapshot_sources and self.replacement_connection_id is None:
            raise ValueError("replacement capture sources require a connection")
        if self.replacement_connection_id == self.original_connection_id:
            raise ValueError("replacement capture cannot use the mismatched connection")
        if any(
            source.connection_id != self.replacement_connection_id
            for source in self.captured_snapshot_sources
        ):
            raise ValueError("replacement snapshot sources span socket connections")

    def observe_snapshot(
        self,
        *,
        admission_ordinal: int,
        connection_id: ConnectionId,
        ingest_seq: int,
        instrument_id: InstrumentId,
        maximum_age_ns: int,
        received_monotonic: MonotonicTimestamp,
        raw_source: RawSourceRef,
    ) -> _AuditReplacementCapture:
        if maximum_age_ns < 0:
            raise ValueError("replacement snapshot maximum age cannot be negative")
        if connection_id == self.original_connection_id:
            return self
        if instrument_id not in self.required_instrument_ids:
            return self
        captured = self.captured_snapshot_sources
        if self.replacement_connection_id != connection_id:
            captured = ()
        captured = tuple(
            source
            for source in captured
            if source.received_monotonic.epoch_id == received_monotonic.epoch_id
            and 0
            <= received_monotonic.nanoseconds - source.received_monotonic.nanoseconds
            <= maximum_age_ns
        )
        captured_by_instrument = {source.instrument_id: source for source in captured}
        captured_by_instrument[instrument_id] = _AuditReplacementSnapshotSource(
            instrument_id=instrument_id,
            connection_id=connection_id,
            admission_ordinal=admission_ordinal,
            ingest_seq=ingest_seq,
            received_monotonic=received_monotonic,
            raw_source=raw_source,
        )
        return replace(
            self,
            replacement_connection_id=connection_id,
            captured_snapshot_sources=tuple(
                captured_by_instrument[key]
                for key in sorted(captured_by_instrument, key=lambda item: item.value)
            ),
        )

    @property
    def complete(self) -> bool:
        return (
            tuple(source.instrument_id for source in self.captured_snapshot_sources)
            == self.required_instrument_ids
        )

    def authority_snapshot(
        self,
        *,
        instrument_id: InstrumentId,
    ) -> ProcessorAuditReplacementCaptureSnapshot:
        return ProcessorAuditReplacementCaptureSnapshot(
            instrument_id=instrument_id,
            divergence_id=self.divergence_id,
            market_id=self.market_id,
            original_connection_id=self.original_connection_id,
            required_instrument_ids=self.required_instrument_ids,
            audit_start_schema=self.audit_start_schema,
            resolution_source=self.resolution_source,
            resolution_ingest_seq=self.resolution_ingest_seq,
            resolution_monotonic=self.resolution_monotonic,
            pre_open_connection_count=self.pre_open_connection_count,
            pre_open_connection_ids_sha256=self.pre_open_connection_ids_sha256,
            mismatch_cause=self.mismatch_cause,
            availability_binding=self.availability_binding,
            replacement_connection_id=self.replacement_connection_id,
            captured_snapshot_sources=tuple(
                ProcessorAuditReplacementSnapshotSource(
                    instrument_id=source.instrument_id,
                    connection_id=source.connection_id,
                    admission_ordinal=source.admission_ordinal,
                    ingest_seq=source.ingest_seq,
                    received_monotonic=source.received_monotonic,
                    raw_source=source.raw_source,
                )
                for source in self.captured_snapshot_sources
            ),
        )

    def to_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "captured_snapshot_sources": self.captured_snapshot_sources,
            "divergence_id": self.divergence_id,
            "market_id": self.market_id,
            "original_connection_id": self.original_connection_id,
            "replacement_connection_id": self.replacement_connection_id,
            "required_instrument_ids": self.required_instrument_ids,
        }
        if self.audit_start_schema == AUDIT_START_V2_SCHEMA:
            record["pre_open_connection_count"] = self.pre_open_connection_count
            record["pre_open_connection_ids_sha256"] = self.pre_open_connection_ids_sha256
            record["mismatch_cause"] = self.mismatch_cause
            record["resolution_ingest_seq"] = self.resolution_ingest_seq
            record["resolution_monotonic"] = self.resolution_monotonic
            record["resolution_source"] = self.resolution_source
        return record


class ProcessorRetentionDomain(StrEnum):
    """Finite diagnostic samples retained outside semantic processor state."""

    SCAN_RESULTS = "scan_results"
    VALIDATED_OPPORTUNITY_HASHES = "validated_opportunity_hashes"
    REFERENCE_REFRESH_REQUESTS = "reference_refresh_requests"
    AUDIT_OUTCOMES = "audit_outcomes"
    AUDIT_MISMATCH_DIAGNOSTICS = "audit_mismatch_diagnostics"
    AUDIT_DIVERGENCE_RESOLUTIONS = "audit_divergence_resolutions"
    AUDIT_FAULT_LIFECYCLES = "audit_fault_lifecycles"
    REPORTED_TOP_TRANSITION_OUTCOMES = "reported_top_transition_outcomes"


@dataclass(frozen=True, slots=True)
class ProcessorRetentionSnapshot:
    """O(1) occupancy metadata for one bounded diagnostic retention domain."""

    domain: ProcessorRetentionDomain
    retained: int
    capacity: int
    total: int
    truncated: bool
    container_shallow_bytes: int
    representative_object_shallow_bytes: int
    byte_estimate_method: str

    def __post_init__(self) -> None:
        if min(self.retained, self.capacity, self.total) < 0:
            raise ValueError("processor retention counts cannot be negative")
        if self.retained > self.capacity or self.retained > self.total:
            raise ValueError("processor retention occupancy is inconsistent")
        if self.truncated != (self.total > self.retained):
            raise ValueError("processor retention truncation flag is inconsistent")
        if (
            min(
                self.container_shallow_bytes,
                self.representative_object_shallow_bytes,
            )
            < 0
        ):
            raise ValueError("processor retention byte estimates cannot be negative")
        if not self.byte_estimate_method or len(self.byte_estimate_method.encode("utf-8")) > 256:
            raise ValueError("processor retention byte-estimate method must be explicit")

    @property
    def estimated_retained_bytes(self) -> int:
        """Return the occupancy-consistent shallow retained-byte estimate."""

        return self.container_shallow_bytes + (
            self.retained * self.representative_object_shallow_bytes
        )


@dataclass(frozen=True, slots=True)
class ProcessorBookOperationalSnapshot:
    """Depth-free scalar state for one canonical book owner."""

    instrument_id: InstrumentId
    market_id: MarketId
    generation: int
    health: BookHealth
    invalidation_reason: InvalidationReason | None
    market_version_id: MarketVersionId
    precision_rule_version_id: PrecisionRuleVersionId
    cost_rule_set_version_id: CostRuleSetVersionId


@dataclass(frozen=True, slots=True)
class ProcessorAuditHttpEndpointSnapshot:
    """Finite exact audit-fault lifecycle counts for one public endpoint."""

    endpoint_class: PolymarketEndpointClass
    observed: int
    current: int
    resolved: int
    permanent: int
    retry_exhaustions_observed: int
    retry_exhaustions_current: int
    retry_exhaustions_resolved: int
    retry_exhaustions_permanent: int

    def __post_init__(self) -> None:
        counts = (
            self.observed,
            self.current,
            self.resolved,
            self.permanent,
            self.retry_exhaustions_observed,
            self.retry_exhaustions_current,
            self.retry_exhaustions_resolved,
            self.retry_exhaustions_permanent,
        )
        if min(counts) < 0:
            raise ValueError("audit HTTP endpoint lifecycle counts cannot be negative")
        if self.endpoint_class is PolymarketEndpointClass.MARKET_WEBSOCKET:
            raise ValueError("audit HTTP lifecycle cannot name the WebSocket endpoint")
        if self.observed != self.current + self.resolved + self.permanent:
            raise ValueError("audit HTTP endpoint lifecycle counts do not reconcile")
        if self.retry_exhaustions_observed != (
            self.retry_exhaustions_current
            + self.retry_exhaustions_resolved
            + self.retry_exhaustions_permanent
        ):
            raise ValueError("audit HTTP endpoint retry lifecycle counts do not reconcile")
        if any(
            retry_count > fault_count
            for retry_count, fault_count in (
                (self.retry_exhaustions_observed, self.observed),
                (self.retry_exhaustions_current, self.current),
                (self.retry_exhaustions_resolved, self.resolved),
                (self.retry_exhaustions_permanent, self.permanent),
            )
        ):
            raise ValueError("audit HTTP retry lifecycle exceeds its fault lifecycle")


@dataclass(frozen=True, slots=True)
class ProcessorOperationalSnapshot:
    """Immutable bounded processor projection for periodic operational health.

    This projection deliberately excludes book depth, retained history tuples,
    lifecycle serialization, content hashes, and the terminal hash. All
    historical authority is represented by exact cumulative counters and the
    incrementally maintained chain identifiers.
    """

    processed_raw_messages: int
    parse_failures: int
    canonical_events: int
    reference_updates: int
    scans: int
    scan_disposition_counts: tuple[tuple[ScanDisposition, int], ...]
    candidates: int
    validated_opportunities: int
    invalidations: int
    resyncs: int
    recovery_reason_counts: tuple[tuple[ProcessorRecoveryReason, int], ...]
    rest_audits: int
    rest_audit_matches: int
    rest_audit_mismatches: int
    rest_audit_raced: int
    rest_audit_failed: int
    audit_start_generation_rebases: int
    audit_mismatch_cause_counts: tuple[tuple[AuditMismatchCause, int], ...]
    audit_start_trigger_counts: tuple[tuple[AuditTriggerReason, int], ...]
    audit_outcome_trigger_counts: tuple[tuple[AuditTriggerReason, AuditOutcome, int], ...]
    audit_divergences_observed: int
    audit_divergences_resolved_by_authoritative_resnapshot: int
    audit_divergences_resolved_same_generation: int
    audit_divergences_pending: int
    audit_divergences_unresolved: int
    audit_divergences_unknown: int
    audit_local_book_defects: int
    audit_divergence_lifecycle_reconciled: bool
    audit_availability_authority: AuditAvailabilityAuthoritySnapshot
    audit_faults_observed: int
    audit_faults_current: int
    audit_faults_resolved: int
    audit_faults_permanent: int
    audit_http_faults_observed: int
    audit_http_faults_resolved: int
    audit_http_faults_permanent: int
    audit_http_retry_exhaustions_observed: int
    audit_http_retry_exhaustions_resolved: int
    audit_http_retry_exhaustions_permanent: int
    audit_http_endpoint_snapshots: tuple[ProcessorAuditHttpEndpointSnapshot, ...]
    audit_fault_lifecycle_reconciled: bool
    audit_replacement_captures_pending: int
    audit_replacement_capture_authority: tuple[ProcessorAuditReplacementCaptureSnapshot, ...]
    pending_rest_audits: int
    audit_ineligible_books: int
    reported_top_transitions_started: int
    reported_top_transitions_converged: int
    reported_top_transitions_superseded: int
    reported_top_transitions_resnapshotted: int
    reported_top_transitions_expired: int
    reported_top_transitions_invalidated: int
    reported_top_transition_frames: int
    reported_top_transition_duration_total_ns: int
    reported_top_transition_duration_max_ns: int
    reported_top_transition_duration_lower_bound_count: int
    reported_top_transition_entry_counts: tuple[tuple[ReportedTopTransitionEntrySource, int], ...]
    reported_top_transition_reason_counts: tuple[tuple[ReportedTopTransitionReason, int], ...]
    reported_top_transition_research_eligibility_restored: int
    reported_top_no_pending_recognized: int
    reported_top_no_pending_rejected: int
    pending_reported_top_transitions: int
    reference_refresh_requests: int
    healthy_books: int
    raw_input_chain_sha256: str
    canonical_event_chain_sha256: str
    error_chain_sha256: str
    scan_chain_sha256: str
    validated_opportunity_chain_sha256: str
    reference_refresh_chain_sha256: str
    audit_start_chain_sha256: str
    audit_outcome_chain_sha256: str
    audit_classification_attestation_chain_sha256: str
    audit_divergence_history_chain_sha256: str
    audit_fault_lifecycle_chain_sha256: str
    reported_top_transition_chain_sha256: str
    recovery_request_chain_sha256: str
    books: tuple[ProcessorBookOperationalSnapshot, ...]
    audit_ineligible_instruments: tuple[InstrumentId, ...]
    audit_divergence_pending_records: tuple[AuditDivergencePending, ...]
    audit_divergence_resolution_representatives: tuple[AuditDivergenceResolution, ...]
    audit_mismatch_diagnostic_representatives: tuple[AuditMismatchDiagnostic, ...]
    audit_fault_current_representative_capacity: int
    audit_fault_current_instrument_ids: tuple[InstrumentId, ...]
    audit_fault_current_representatives: tuple[AuditFaultLifecycleRecord, ...]
    audit_fault_current_representatives_truncated: bool
    audit_fault_lifecycle_representatives: tuple[AuditFaultLifecycleRecord, ...]
    reported_top_transitions: tuple[ReportedTopTransition, ...]
    audit_last_successful_match_monotonic_ns: int | None
    audit_barrier_lag_frames_max: int | None
    audit_race_budget_exhaustions: int
    retention: tuple[ProcessorRetentionSnapshot, ...]

    def retention_for(self, domain: ProcessorRetentionDomain) -> ProcessorRetentionSnapshot:
        for item in self.retention:
            if item.domain is domain:
                return item
        raise KeyError(domain)


@dataclass(frozen=True, slots=True)
class ProcessorSummary:
    processed_raw_messages: int
    parse_failures: int
    canonical_events: int
    reference_updates: int
    scans: int
    scan_disposition_counts: tuple[tuple[ScanDisposition, int], ...]
    candidates: int
    validated_opportunities: int
    invalidations: int
    resyncs: int
    recovery_reason_counts: tuple[tuple[ProcessorRecoveryReason, int], ...]
    rest_audits: int
    rest_audit_matches: int
    rest_audit_mismatches: int
    rest_audit_raced: int
    rest_audit_failed: int
    audit_start_generation_rebases: int
    audit_mismatch_cause_counts: tuple[tuple[AuditMismatchCause, int], ...]
    audit_start_trigger_counts: tuple[tuple[AuditTriggerReason, int], ...]
    audit_outcome_trigger_counts: tuple[tuple[AuditTriggerReason, AuditOutcome, int], ...]
    audit_divergences_observed: int
    audit_divergences_resolved_by_authoritative_resnapshot: int
    audit_divergences_resolved_same_generation: int
    audit_divergences_pending: int
    audit_divergences_unresolved: int
    audit_divergences_unknown: int
    audit_local_book_defects: int
    audit_divergence_lifecycle_reconciled: bool
    audit_availability_authority: AuditAvailabilityAuthoritySnapshot
    audit_faults_observed: int
    audit_faults_current: int
    audit_faults_resolved: int
    audit_faults_permanent: int
    audit_http_faults_observed: int
    audit_http_faults_resolved: int
    audit_http_faults_permanent: int
    audit_http_retry_exhaustions_observed: int
    audit_http_retry_exhaustions_resolved: int
    audit_http_retry_exhaustions_permanent: int
    audit_http_endpoint_snapshots: tuple[ProcessorAuditHttpEndpointSnapshot, ...]
    audit_fault_lifecycle_reconciled: bool
    audit_replacement_captures_pending: int
    audit_replacement_capture_authority: tuple[ProcessorAuditReplacementCaptureSnapshot, ...]
    audit_fault_current_instrument_ids: tuple[InstrumentId, ...]
    pending_rest_audits: int
    audit_ineligible_books: int
    reported_top_transitions_started: int
    reported_top_transitions_converged: int
    reported_top_transitions_superseded: int
    reported_top_transitions_resnapshotted: int
    reported_top_transitions_expired: int
    reported_top_transitions_invalidated: int
    reported_top_transition_frames: int
    reported_top_transition_duration_total_ns: int
    reported_top_transition_duration_max_ns: int
    reported_top_transition_duration_lower_bound_count: int
    reported_top_transition_entry_counts: tuple[tuple[ReportedTopTransitionEntrySource, int], ...]
    reported_top_transition_reason_counts: tuple[tuple[ReportedTopTransitionReason, int], ...]
    reported_top_transition_research_eligibility_restored: int
    reported_top_no_pending_recognized: int
    reported_top_no_pending_rejected: int
    pending_reported_top_transitions: int
    reference_refresh_requests: int
    unique_opportunity_lifecycles: int
    healthy_books: int
    raw_input_chain_sha256: str
    canonical_event_chain_sha256: str
    error_chain_sha256: str
    scan_chain_sha256: str
    validated_opportunity_chain_sha256: str
    reference_refresh_chain_sha256: str
    audit_start_chain_sha256: str
    audit_outcome_chain_sha256: str
    audit_classification_attestation_chain_sha256: str
    audit_divergence_history_chain_sha256: str
    audit_fault_lifecycle_chain_sha256: str
    reported_top_transition_chain_sha256: str
    recovery_request_chain_sha256: str
    lifecycle_state_sha256: str
    scan_results_truncated: bool
    opportunity_hashes_truncated: bool
    reference_refresh_requests_truncated: bool
    audit_outcomes_truncated: bool
    audit_mismatch_diagnostics_truncated: bool
    audit_divergence_resolution_records_truncated: bool
    audit_fault_lifecycle_records_truncated: bool
    reported_top_transition_outcomes_truncated: bool
    terminal_hash_schema: str
    terminal_hash: str
    opportunity_hashes: tuple[str, ...]
    audit_outcomes: tuple[AuditOutcomeEvidence, ...]
    audit_mismatch_diagnostics: tuple[AuditMismatchDiagnostic, ...]
    audit_divergence_pending_records: tuple[AuditDivergencePending, ...]
    audit_divergence_resolution_records: tuple[AuditDivergenceResolution, ...]
    audit_fault_current_records: tuple[AuditFaultLifecycleRecord, ...]
    audit_fault_lifecycle_records: tuple[AuditFaultLifecycleRecord, ...]
    reported_top_transitions: tuple[ReportedTopTransition, ...]
    reported_top_transition_outcomes: tuple[ReportedTopTransitionOutcome, ...]


class AnalyticalOutputSink(Protocol):
    """Structural sink accepted by the canonical processing hot path."""

    def append_reference(
        self,
        bundle: ReferenceBundle,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> None: ...

    def append_canonical(self, event: CanonicalEvent) -> None: ...

    def append_scan(
        self,
        result: CompleteSetScanResult,
        *,
        lifecycle: OpportunityLifecycle | None = None,
    ) -> None: ...


class CanonicalProcessor:
    """Stateful single-owner processing core shared without replay forks."""

    def __init__(
        self,
        config: Phase1Config,
        *,
        analytical_outputs: AnalyticalOutputSink | None = None,
        scan_latency_observer: Callable[[int], None] | None = None,
        source_journal_git_commit: str | None = None,
    ) -> None:
        self._config = config
        self._registry = ReferenceRegistry()
        self._builder = PolymarketReferenceBuilder()
        self._normalizer = PolymarketNormalizer(self._builder)
        self._scanner = PolymarketCompleteSetScanner(
            CompleteSetScannerConfig.from_values(
                maximum_book_age_seconds=config.scanner.maximum_book_age_seconds,
                maximum_interleg_skew_seconds=(config.scanner.maximum_interleg_skew_seconds),
                minimum_edge_thresholds=config.scanner.minimum_edge_thresholds,
                quantity_increment=config.scanner.quantity_increment,
                maximum_capacity_evaluations=config.scanner.maximum_capacity_evaluations,
            )
        )
        self._lifecycle_tracker = OpportunityLifecycleTracker()
        self._outputs = analytical_outputs
        self._scan_latency_observer = scan_latency_observer
        self._source_journal_git_commit = source_journal_git_commit
        self._allow_v1_audit_generation_rebase = (
            source_journal_git_commit in _V1_AUDIT_GENERATION_REBASE_SOURCE_COMMITS
        )
        self._allow_historical_direct_deferred_audit = (
            source_journal_git_commit in _HISTORICAL_DIRECT_DEFERRED_AUDIT_SOURCE_COMMITS
        )
        self._gamma_by_condition: dict[str, GammaMarket] = {}
        self._gamma_by_id: dict[str, GammaMarket] = {}
        self._events_by_id: dict[str, GammaEvent] = {}
        self._clob_by_condition: dict[str, ClobMarketInfo] = {}
        self._gamma_source_by_condition: dict[str, _RawReferenceSource] = {}
        self._gamma_relationship_source_by_condition: dict[str, _RawReferenceSource] = {}
        self._gamma_parent_binding_by_id: dict[str, str | None] = {}
        self._event_source_by_id: dict[str, _RawReferenceSource] = {}
        self._clob_source_by_condition: dict[str, _RawReferenceSource] = {}
        self._rest_source_by_condition: dict[str, _RawReferenceSource] = {}
        self._reference_provenance: dict[MarketId, Provenance] = {}
        self._owners: dict[InstrumentId, CanonicalL2BookOwner] = {}
        self._market_instruments: dict[MarketId, tuple[InstrumentId, ...]] = {}
        self._semantic_proofs: dict[
            MarketId,
            tuple[ReferenceBundle, BinarySemanticProof],
        ] = {}
        self._scan_results: deque[CompleteSetScanResult] = deque(maxlen=_SCAN_RESULT_HISTORY_LIMIT)
        self._validated_opportunity_hashes: deque[str] = deque(
            maxlen=_VALIDATED_OPPORTUNITY_HISTORY_LIMIT
        )
        self._reference_refresh_requests: deque[ReferenceRefreshRequest] = deque(
            maxlen=_REFERENCE_REFRESH_HISTORY_LIMIT
        )
        self._audit_outcomes: deque[AuditOutcomeEvidence] = deque(
            maxlen=_AUDIT_OUTCOME_HISTORY_LIMIT
        )
        self._audit_mismatch_diagnostics: deque[AuditMismatchDiagnostic] = deque(
            maxlen=_AUDIT_MISMATCH_DIAGNOSTIC_HISTORY_LIMIT
        )
        self._latest_audit_mismatch_diagnostics: dict[
            AuditMismatchCause, AuditMismatchDiagnostic
        ] = {}
        self._pending_rest_audits: dict[str, AuditStartEvidence] = {}
        self._pending_rest_audit_start_sources: dict[str, RawSourceRef] = {}
        self._pending_rest_audit_responses: dict[str, RawEventEnvelope] = {}
        self._pending_rest_audit_classification_preparations: dict[
            str, _PreparedAuditClassification
        ] = {}
        self._prepare_attest_rest_audit_requests: set[str] = set()
        self._pending_rest_audit_hard_expiries: dict[str, int] = {}
        self._pending_rest_audit_response_deadlines: dict[str, int] = {}
        self._rest_audit_protocol_mode: _RestAuditProtocolMode | None = None
        self._rest_audit_prepare_attest_required: bool | None = None
        self._rest_audit_run_deadline_epoch_id: str | None = None
        self._rest_audit_run_deadline_ns: int | None = None
        self._audit_ineligible_instruments: set[InstrumentId] = set()
        self._audit_ineligible_nonfault_instruments: set[InstrumentId] = set()
        self._pending_audit_divergences: dict[InstrumentId, AuditDivergencePending] = {}
        self._audit_availability_bindings: dict[str, AuditAvailabilityBinding] = {}
        self._audit_replacement_captures: dict[InstrumentId, _AuditReplacementCapture] = {}
        self._audit_divergence_resolutions: deque[AuditDivergenceResolution] = deque(
            maxlen=_AUDIT_DIVERGENCE_RESOLUTION_HISTORY_LIMIT
        )
        self._audit_fault_lifecycle_records: deque[AuditFaultLifecycleRecord] = deque(
            maxlen=_AUDIT_FAULT_LIFECYCLE_HISTORY_LIMIT
        )
        self._current_audit_faults: dict[str, AuditFaultLifecycleRecord] = {}
        self._permanent_audit_faults: dict[str, AuditFaultLifecycleRecord] = {}
        self._latest_current_audit_fault_by_instrument: dict[
            InstrumentId, AuditFaultLifecycleRecord
        ] = {}
        self._audit_fault_current_count_by_instrument: dict[InstrumentId, int] = {}
        self._audit_fault_scope_block_count_by_instrument: dict[InstrumentId, int] = {}
        self._latest_audit_fault_by_status: dict[AuditFaultStatus, AuditFaultLifecycleRecord] = {}
        self._latest_audit_divergence_resolution: AuditDivergenceResolution | None = None
        self._reported_top_transitions: dict[InstrumentId, ReportedTopTransition] = {}
        self._reported_top_transition_outcomes: deque[ReportedTopTransitionOutcome] = deque(
            maxlen=_REPORTED_TOP_TRANSITION_OUTCOME_HISTORY_LIMIT
        )
        self._transition_resync_instruments: set[InstrumentId] = set()
        self._post_snapshot_baselines: dict[MarketId, _PostSnapshotBaseline] = {}
        self._audit_economic_generations: dict[InstrumentId, int] = {}
        self._processed_ws_watermarks: dict[ConnectionId, int] = {}
        self._seen_websocket_connections: set[ConnectionId] = set()
        self._active_refresh_requests: list[ReferenceRefreshRequest] | None = None
        self._active_dirty_markets: dict[MarketId, set[ScanTriggerReason]] | None = None
        self._active_freshness_updates: dict[InstrumentId, BookFreshnessUpdate] | None = None
        self._active_relevant_l2_updates: dict[InstrumentId, BookFreshnessUpdate] | None = None
        self._active_received_monotonic: MonotonicTimestamp | None = None
        self._active_envelope: RawEventEnvelope | None = None
        self._active_scan_observed_at: UtcTimestamp | None = None
        self._active_audit_outcomes: list[AuditOutcomeEvidence] | None = None
        self._active_audit_fault_recording_context: _AuditFaultRecordingContext | None = None
        self._active_recovery_requests: list[ProcessorRecoveryRequest] | None = None
        self._active_audit_divergences_opened: list[AuditDivergencePending] | None = None
        self._active_audit_divergences_resolved: list[AuditDivergenceResolution] | None = None
        self._active_audit_divergences_eligibility_restored: (
            list[AuditDivergenceEligibilityRestoration] | None
        ) = None
        self._active_canonical_event: CanonicalEvent | None = None
        self._last_accepted_l2_monotonic: dict[InstrumentId, MonotonicTimestamp] = {}
        self._raw_input_chain = _chain_seed("raw-input")
        self._canonical_event_chain = _chain_seed("canonical-event")
        self._error_chain = _chain_seed("expected-data-error")
        self._scan_chain = _chain_seed("scan")
        self._validated_opportunity_chain = _chain_seed("validated-opportunity")
        self._reference_refresh_chain = _chain_seed("reference-refresh")
        self._audit_start_chain = _chain_seed("rest-audit-start")
        self._audit_outcome_chain = _chain_seed("rest-audit-outcome")
        self._audit_classification_attestation_chain = _chain_seed(
            "rest-audit-classification-attestation"
        )
        self._audit_divergence_history_chain = _chain_seed("rest-audit-divergence")
        self._audit_availability_open_chain = audit_availability_chain_seed("open")
        self._audit_availability_resolution_chain = audit_availability_chain_seed("resolution")
        self._audit_availability_release_chain = audit_availability_chain_seed("release")
        self._audit_fault_lifecycle_chain = _chain_seed("rest-audit-fault-lifecycle")
        self._audit_compatibility_chain = _chain_seed("rest-audit-compatibility")
        self._reported_top_transition_chain = _chain_seed("reported-top-transition")
        self._recovery_request_chain = _chain_seed("processor-recovery-request")
        self._processed = 0
        self._parse_failures = 0
        self._canonical_events = 0
        self._scan_count = 0
        self._scan_disposition_counts = dict.fromkeys(ScanDisposition, 0)
        self._candidate_count = 0
        self._validated_opportunity_count = 0
        self._reference_refresh_count = 0
        self._reference_updates = 0
        self._invalidations = 0
        self._resyncs = 0
        self._recovery_reason_counts = dict.fromkeys(ProcessorRecoveryReason, 0)
        self._rest_audits = 0
        self._rest_audit_matches = 0
        self._rest_audit_mismatches = 0
        self._rest_audit_raced = 0
        self._rest_audit_failed = 0
        self._audit_mismatch_diagnostic_count = 0
        self._audit_last_successful_match_monotonic_ns: int | None = None
        self._audit_barrier_lag_frames_max: int | None = None
        self._audit_race_budget_exhaustions = 0
        self._audit_start_generation_rebases = 0
        self._audit_mismatch_cause_counts = dict.fromkeys(AuditMismatchCause, 0)
        self._audit_start_trigger_counts = dict.fromkeys(AuditTriggerReason, 0)
        self._audit_outcome_trigger_counts = {
            (trigger, outcome): 0 for trigger in AuditTriggerReason for outcome in AuditOutcome
        }
        self._audit_divergences_observed = 0
        self._audit_divergences_resolved_by_authoritative_resnapshot = 0
        self._audit_availability_opened = 0
        self._audit_availability_resolved = 0
        self._audit_availability_released = 0
        self._audit_availability_legacy_unverifiable = 0
        self._audit_faults_observed = 0
        self._audit_faults_resolved = 0
        self._audit_faults_permanent = 0
        self._audit_http_faults_observed = 0
        self._audit_http_faults_resolved = 0
        self._audit_http_faults_permanent = 0
        self._audit_http_retry_exhaustions_observed = 0
        self._audit_http_retry_exhaustions_resolved = 0
        self._audit_http_retry_exhaustions_permanent = 0
        self._audit_http_fault_endpoint_observed = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_fault_endpoint_current = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_fault_endpoint_resolved = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_fault_endpoint_permanent = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_retry_endpoint_observed = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_retry_endpoint_current = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_retry_endpoint_resolved = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._audit_http_retry_endpoint_permanent = dict.fromkeys(_HTTP_ENDPOINT_CLASSES, 0)
        self._reported_top_transitions_started = 0
        self._reported_top_transitions_converged = 0
        self._reported_top_transitions_superseded = 0
        self._reported_top_transitions_resnapshotted = 0
        self._reported_top_transitions_expired = 0
        self._reported_top_transitions_invalidated = 0
        self._reported_top_transition_frames = 0
        self._reported_top_transition_duration_total_ns = 0
        self._reported_top_transition_duration_max_ns = 0
        self._reported_top_transition_duration_lower_bound_count = 0
        self._reported_top_transition_entry_counts = dict.fromkeys(
            ReportedTopTransitionEntrySource,
            0,
        )
        self._reported_top_transition_reason_counts = dict.fromkeys(
            ReportedTopTransitionReason,
            0,
        )
        self._reported_top_transition_research_eligibility_restored = 0
        self._reported_top_no_pending_recognized = 0
        self._reported_top_no_pending_rejected = 0

    @property
    def discovered_markets(self) -> tuple[GammaMarket, ...]:
        return tuple(sorted(self._gamma_by_condition.values(), key=lambda item: item.condition_id))

    @property
    def owners(self) -> tuple[CanonicalL2BookOwner, ...]:
        return tuple(self._owners[key] for key in sorted(self._owners, key=lambda item: item.value))

    @property
    def scan_results(self) -> tuple[CompleteSetScanResult, ...]:
        return tuple(self._scan_results)

    @property
    def reference_refresh_requests(self) -> tuple[ReferenceRefreshRequest, ...]:
        return tuple(self._reference_refresh_requests)

    def reference_provenance(self, market_id: MarketId) -> Provenance | None:
        return self._reference_provenance.get(market_id)

    @property
    def current_bundles(self) -> tuple[ReferenceBundle, ...]:
        bundles: list[ReferenceBundle] = []
        for gamma in self.discovered_markets:
            market_id = self._market_id_for_condition(gamma.condition_id)
            bundle = self._registry.current(market_id)
            if bundle is not None:
                bundles.append(bundle)
        return tuple(bundles)

    @property
    def audit_outcomes(self) -> tuple[AuditOutcomeEvidence, ...]:
        return tuple(self._audit_outcomes)

    @property
    def audit_divergence_pending_records(self) -> tuple[AuditDivergencePending, ...]:
        return tuple(
            self._pending_audit_divergences[instrument_id]
            for instrument_id in sorted(
                self._pending_audit_divergences,
                key=lambda item: item.value,
            )
        )

    def audit_recovery_blocks_native_asset(self, native_asset_id: str) -> bool:
        """Return whether divergence recovery owns this asset's market scope.

        Audit divergence recovery is market-scoped even though the mismatch and
        authoritative REST baseline are instrument-scoped.  The coordinator
        must not start another audit for either leg until the pending divergence
        has been resnapshotted and the replacement connection has supplied every
        required full snapshot.
        """

        instrument_id = self._owner_for_native_asset(native_asset_id).instrument_id
        for pending in sorted(
            self._pending_audit_divergences.values(),
            key=lambda item: item.instrument_id.value,
        ):
            required_instrument_ids = self._market_instruments.get(pending.market_id, ())
            if not required_instrument_ids or pending.instrument_id not in required_instrument_ids:
                raise ProgrammingInvariantViolation(
                    "audit divergence lost its bounded market recovery scope",
                    component="canonical-processor",
                )
            if instrument_id in required_instrument_ids:
                return True
        return any(
            instrument_id in capture.required_instrument_ids
            for capture in sorted(
                self._audit_replacement_captures.values(),
                key=lambda item: item.divergence_id,
            )
        )

    @staticmethod
    def _audit_replacement_capture_fresh_at(
        capture: _AuditReplacementCapture,
        decision_monotonic: MonotonicTimestamp,
        *,
        maximum_age_ns: int,
    ) -> bool:
        return (
            capture.complete
            and capture.resolution_monotonic.epoch_id == decision_monotonic.epoch_id
            and decision_monotonic.nanoseconds >= capture.resolution_monotonic.nanoseconds
            and all(
                source.admission_ordinal > 0
                and source.ingest_seq > capture.resolution_ingest_seq
                and source.received_monotonic.epoch_id == decision_monotonic.epoch_id
                and source.received_monotonic.nanoseconds
                >= capture.resolution_monotonic.nanoseconds
                and 0
                <= decision_monotonic.nanoseconds - source.received_monotonic.nanoseconds
                <= maximum_age_ns
                for source in capture.captured_snapshot_sources
            )
        )

    @staticmethod
    def _audit_replacement_release_command(
        instrument_id: InstrumentId,
        capture: _AuditReplacementCapture,
        decision_monotonic: MonotonicTimestamp,
    ) -> AuditReplacementReleaseCommand:
        replacement_connection_id = capture.replacement_connection_id
        if replacement_connection_id is None:
            raise ProgrammingInvariantViolation(
                "complete audit replacement capture lacks a connection",
                component="canonical-processor",
            )
        return AuditReplacementReleaseCommand.create(
            divergence_id=capture.divergence_id,
            instrument_id=instrument_id,
            market_id=capture.market_id,
            replacement_connection_id=replacement_connection_id,
            capture_record=capture.to_record(),
            monotonic_epoch_id=decision_monotonic.epoch_id,
            decision_monotonic_ns=decision_monotonic.nanoseconds,
        )

    def _audit_replacement_release_group(
        self,
        capture: _AuditReplacementCapture,
    ) -> tuple[tuple[InstrumentId, _AuditReplacementCapture], ...]:
        """Return every capture sharing the exact market recovery scope.

        More than one audit leg can diverge before either authoritative
        resnapshot is processed.  Their replacement captures must be projected
        as one deterministic group so that a partially ready sibling cannot be
        mistaken for an unrelated blocker or released around.
        """

        return tuple(
            (instrument_id, candidate)
            for instrument_id, candidate in sorted(
                self._audit_replacement_captures.items(),
                key=lambda item: item[0].value,
            )
            if candidate.market_id == capture.market_id
        )

    def _mark_audit_ineligible(
        self,
        instrument_id: InstrumentId,
        *,
        nonfault: bool,
    ) -> None:
        self._audit_ineligible_instruments.add(instrument_id)
        if nonfault:
            self._audit_ineligible_nonfault_instruments.add(instrument_id)

    def _clear_audit_ineligible(self, instrument_id: InstrumentId) -> None:
        self._audit_ineligible_instruments.discard(instrument_id)
        self._audit_ineligible_nonfault_instruments.discard(instrument_id)

    def _historical_fault_is_only_audit_ineligibility(
        self,
        instrument_id: InstrumentId,
    ) -> bool:
        if (
            instrument_id not in self._audit_ineligible_instruments
            or instrument_id in self._audit_ineligible_nonfault_instruments
        ):
            return False
        permanent_faults = tuple(
            record
            for record in self._permanent_audit_faults.values()
            if record.instrument_id == instrument_id
        )
        return bool(permanent_faults) and all(
            not record.blocks_current_scope
            and not audit_fault_evidence_blocks_current_scope(
                kind=record.failure_evidence_kind,
                audit_start_schema=record.audit_start_schema,
                failure_schema=record.failure_schema,
            )
            for record in permanent_faults
        )

    def _audit_replacement_release_group_ready_at(
        self,
        group: tuple[tuple[InstrumentId, _AuditReplacementCapture], ...],
        *,
        connection_id: ConnectionId,
        decision_monotonic: MonotonicTimestamp,
    ) -> bool:
        """Prove capture release is the only remaining scope ineligibility.

        Sequential controls are safe for a multi-capture group because the
        scanner remains blocked by the pending suffix.  This predicate is
        evaluated both by projection and by each control's acceptance, so only
        the final accepted control can remove the last capture and trigger a
        scan.
        """

        if not group:
            return False
        release_instrument_ids = {instrument_id for instrument_id, _capture in group}
        market_ids = {capture.market_id for _instrument_id, capture in group}
        required_instrument_ids = {
            required_instrument_id
            for _instrument_id, capture in group
            for required_instrument_id in capture.required_instrument_ids
        }
        if (
            len(market_ids) != 1
            or not release_instrument_ids.issubset(required_instrument_ids)
            or any(
                capture.audit_start_schema != AUDIT_START_V2_SCHEMA
                or capture.mismatch_cause
                in {AuditMismatchCause.LOCAL_BOOK_DEFECT, AuditMismatchCause.UNKNOWN}
                or capture.replacement_connection_id != connection_id
                or not self._audit_replacement_capture_fresh_at(
                    capture,
                    decision_monotonic,
                    maximum_age_ns=self._scanner.config.maximum_book_age_ns,
                )
                for _instrument_id, capture in group
            )
        ):
            return False
        if any(
            pending.market_id in market_ids for pending in self._pending_audit_divergences.values()
        ):
            return False
        if set(self._reported_top_transitions).intersection(required_instrument_ids):
            return False
        if any(
            self._audit_fault_blocks_instrument(instrument_id)
            for instrument_id in required_instrument_ids
        ):
            return False
        scope_ineligible = self._audit_ineligible_instruments.intersection(required_instrument_ids)
        historical_fault_only_ineligible = scope_ineligible - release_instrument_ids
        if (
            not release_instrument_ids.issubset(scope_ineligible)
            or any(
                not self._historical_fault_is_only_audit_ineligibility(instrument_id)
                for instrument_id in historical_fault_only_ineligible
            )
            or self._transition_resync_instruments.intersection(required_instrument_ids)
        ):
            return False
        maximum_age_ns = self._scanner.config.maximum_book_age_ns
        return all(
            self._owners[instrument_id].health is BookHealth.HEALTHY
            and (latest_l2 := self._last_accepted_l2_monotonic.get(instrument_id)) is not None
            and latest_l2.epoch_id == decision_monotonic.epoch_id
            and 0 <= decision_monotonic.nanoseconds - latest_l2.nanoseconds <= maximum_age_ns
            for instrument_id in required_instrument_ids
        )

    def audit_replacement_release_commands(
        self,
        *,
        connection_id: ConnectionId,
        decision_monotonic: MonotonicTimestamp,
    ) -> tuple[AuditReplacementReleaseCommand, ...]:
        """Return exact current v2 captures eligible for durable release.

        This is a deterministic, non-mutating projection.  The observation
        coordinator owns the independent live freshness/health check; replay
        state changes only when the returned command is durably journaled and
        processed through the normal canonical route.
        """

        commands: list[AuditReplacementReleaseCommand] = []
        visited_markets: set[MarketId] = set()
        for instrument_id in sorted(
            self._audit_replacement_captures,
            key=lambda item: item.value,
        ):
            capture = self._audit_replacement_captures[instrument_id]
            if capture.market_id in visited_markets:
                continue
            visited_markets.add(capture.market_id)
            group = self._audit_replacement_release_group(capture)
            if not self._audit_replacement_release_group_ready_at(
                group,
                connection_id=connection_id,
                decision_monotonic=decision_monotonic,
            ):
                continue
            commands.extend(
                self._audit_replacement_release_command(
                    group_instrument_id,
                    group_capture,
                    decision_monotonic,
                )
                for group_instrument_id, group_capture in group
            )
        return tuple(commands)

    @property
    def audit_replacement_captures_pending(self) -> int:
        """Resolved baselines still awaiting replacement full-snapshot coverage."""

        return len(self._audit_replacement_captures)

    @property
    def audit_ineligible_instruments(self) -> tuple[InstrumentId, ...]:
        """Books excluded from opportunity eligibility after an unproven audit."""

        return tuple(
            sorted(
                self._audit_ineligible_instruments
                | self._reported_top_transitions.keys()
                | self._audit_fault_scope_block_count_by_instrument.keys(),
                key=lambda item: item.value,
            )
        )

    @property
    def reported_top_transitions(self) -> tuple[ReportedTopTransition, ...]:
        return tuple(
            self._reported_top_transitions[instrument_id]
            for instrument_id in sorted(
                self._reported_top_transitions,
                key=lambda item: item.value,
            )
        )

    def reported_top_transition_for(
        self, instrument_id: InstrumentId
    ) -> ReportedTopTransition | None:
        return self._reported_top_transitions.get(instrument_id)

    def processed_websocket_watermark(
        self, connection_id: ConnectionId
    ) -> WebSocketAdmissionWatermark:
        """Return the durable canonical prefix for one socket generation."""

        return WebSocketAdmissionWatermark(
            connection_id=connection_id,
            ordinal=self._processed_ws_watermarks.get(connection_id, 0),
        )

    def audit_book_identity(self, native_asset_id: str) -> AuditBookIdentity:
        """Capture one relevant identity from the single canonical owner."""

        owner = self._owner_for_native_asset(native_asset_id)
        return AuditBookIdentity(
            instrument_id=owner.instrument_id,
            generation=self._audit_economic_generations.get(owner.instrument_id, 0),
            economic_content_sha256=owner.audit_comparison_sha256(),
        )

    def _owner_for_native_asset(self, native_asset_id: str) -> CanonicalL2BookOwner:
        matches: list[InstrumentId] = []
        for bundle in self.current_bundles:
            try:
                instrument = bundle.instrument_for_token(native_asset_id)
            except KeyError:
                continue
            matches.append(instrument.instrument_id)
        if len(matches) != 1 or matches[0] not in self._owners:
            raise NormalizationError(
                "REST audit asset does not resolve to exactly one canonical owner"
            )
        return self._owners[matches[0]]

    @staticmethod
    def _market_id_for_condition(condition_id: str) -> MarketId:
        from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID

        return MarketId.from_native(POLYMARKET_VENUE_ID, condition_id)

    def process_durable(
        self, envelope: RawEventEnvelope, receipt: JournalReceipt
    ) -> ProcessedEnvelope:
        """Process live input only after the journal established durability."""

        if (
            receipt.ingest_id != envelope.ingest_id
            or receipt.collector_run_id != envelope.collector_run_id
            or receipt.ingest_seq != envelope.ingest_seq
            or receipt.state
            not in {
                JournalDurabilityState.DURABLE,
                JournalDurabilityState.SEALED,
                JournalDurabilityState.VERIFIED,
            }
        ):
            raise ValueError("raw envelope cannot be parsed before matching durable receipt")
        return self._process_preserved(envelope)

    def process_replayed(self, envelope: RawEventEnvelope) -> ProcessedEnvelope:
        """Process an envelope already integrity-verified by ``JournalReader``."""

        return self._process_preserved(envelope)

    def _rejected_prepare_attest_control_is_inert(self, envelope: RawEventEnvelope) -> bool:
        """Identify a rejected PREPARE transaction without trusting its metadata."""

        if envelope.channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE:
            return True
        if envelope.channel not in {
            CONTROL_REST_AUDIT_CLASSIFY,
            CONTROL_REST_AUDIT_FAILED,
        }:
            return False
        candidate_request_ids: set[str] = set()
        metadata_request_id = envelope.transport_metadata.get("request_id")
        if isinstance(metadata_request_id, str):
            candidate_request_ids.add(metadata_request_id)
        try:
            decoded = decode_json_exact(envelope.payload_bytes)
        except ValueError:
            decoded = None
        if isinstance(decoded, dict):
            payload_request_id = cast(dict[object, object], decoded).get("request_id")
            if isinstance(payload_request_id, str):
                candidate_request_ids.add(payload_request_id)
        return bool(candidate_request_ids.intersection(self._prepare_attest_rest_audit_requests))

    def _process_preserved(self, envelope: RawEventEnvelope) -> ProcessedEnvelope:
        if (
            self._active_refresh_requests is not None
            or self._active_dirty_markets is not None
            or self._active_freshness_updates is not None
            or self._active_relevant_l2_updates is not None
            or self._active_received_monotonic is not None
            or self._active_envelope is not None
            or self._active_scan_observed_at is not None
            or self._active_audit_outcomes is not None
            or self._active_recovery_requests is not None
            or self._active_audit_divergences_opened is not None
            or self._active_audit_divergences_resolved is not None
            or self._active_audit_divergences_eligibility_restored is not None
            or self._active_canonical_event is not None
        ):
            raise ProgrammingInvariantViolation(
                "canonical processor cannot process envelopes reentrantly",
                component="canonical-processor",
            )
        self._processed += 1
        self._raw_input_chain = _chain_extend(
            self._raw_input_chain,
            "raw-input",
            (envelope.ingest_seq, envelope.payload_sha256, envelope.channel),
        )
        before_events = self._canonical_events
        before_scans = self._scan_count
        owner_state_before = {
            owner.instrument_id: (owner.health, owner.invalidation_reason) for owner in self.owners
        }
        current_refresh_requests: list[ReferenceRefreshRequest] = []
        current_dirty_markets: dict[MarketId, set[ScanTriggerReason]] = {}
        current_freshness_updates: dict[InstrumentId, BookFreshnessUpdate] = {}
        current_relevant_l2_updates: dict[InstrumentId, BookFreshnessUpdate] = {}
        current_audit_outcomes: list[AuditOutcomeEvidence] = []
        current_recovery_requests: list[ProcessorRecoveryRequest] = []
        current_audit_divergences_opened: list[AuditDivergencePending] = []
        current_audit_divergences_resolved: list[AuditDivergenceResolution] = []
        current_audit_divergences_eligibility_restored: list[
            AuditDivergenceEligibilityRestoration
        ] = []
        self._active_refresh_requests = current_refresh_requests
        self._active_dirty_markets = current_dirty_markets
        self._active_freshness_updates = current_freshness_updates
        self._active_relevant_l2_updates = current_relevant_l2_updates
        self._active_received_monotonic = envelope.received_monotonic
        self._active_envelope = envelope
        self._active_scan_observed_at = None
        self._active_audit_outcomes = current_audit_outcomes
        self._active_recovery_requests = current_recovery_requests
        self._active_audit_divergences_opened = current_audit_divergences_opened
        self._active_audit_divergences_resolved = current_audit_divergences_resolved
        self._active_audit_divergences_eligibility_restored = (
            current_audit_divergences_eligibility_restored
        )
        try:
            try:
                self._route(envelope)
            except (
                BookStateError,
                NormalizationError,
                PolymarketSchemaError,
                ReferenceBuildError,
            ) as error:
                error_type = (
                    "NormalizationError"
                    if isinstance(
                        error,
                        (
                            _AuditFaultCapacityError,
                            _AuditReplacementSnapshotEvidenceError,
                        ),
                    )
                    else type(error).__name__
                )
                self._parse_failures += 1
                self._error_chain = _chain_extend(
                    self._error_chain,
                    "expected-data-error",
                    (envelope.ingest_seq, error_type),
                )
                # Release controls are validated completely before capture or
                # book mutation.  A malformed/stale control must remain a
                # fail-closed protocol error, but invalidating the already
                # recovered books would itself mutate the evidence the control
                # is meant to authorize.  The still-pending capture continues
                # to block every scan and research consumer.
                affected = (
                    ()
                    if envelope.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE
                    or self._rejected_prepare_attest_control_is_inert(envelope)
                    or (
                        isinstance(error, _AuditFaultCapacityError)
                        and envelope.channel == CONTROL_BOOK_INVALIDATION
                        and bool(self._prepare_attest_rest_audit_requests)
                    )
                    or isinstance(error, _AuditReplacementSnapshotEvidenceError)
                    else self.invalidate_all(
                        self._expected_data_error_invalidation_reason(
                            error,
                            owner_state_before=owner_state_before,
                        )
                    )
                )
                recovery_affected = {
                    instrument_id
                    for request in current_recovery_requests
                    for instrument_id in request.affected_instrument_ids
                }
                affected = tuple(
                    sorted(
                        {*affected, *recovery_affected},
                        key=lambda item: item.value,
                    )
                )
                scan_triggers = self._flush_dirty_scans(
                    envelope.received_wall_time
                    if self._active_scan_observed_at is None
                    else self._active_scan_observed_at
                )
                audit_response_receive_times = tuple(
                    outcome.response_received_monotonic_ns
                    for outcome in current_audit_outcomes
                    if outcome.response_received_monotonic_ns is not None
                )
                processed = ProcessedEnvelope(
                    ingest_seq=envelope.ingest_seq,
                    success=False,
                    canonical_event_count=self._canonical_events - before_events,
                    scan_count=self._scan_count - before_scans,
                    needs_resync=bool(affected),
                    affected_instrument_ids=affected,
                    reference_refresh_requests=tuple(current_refresh_requests),
                    book_freshness_updates=tuple(
                        current_freshness_updates[key]
                        for key in sorted(current_freshness_updates, key=lambda item: item.value)
                    ),
                    scan_triggers=scan_triggers,
                    error_type=error_type,
                    relevant_l2_receive_updates=tuple(
                        current_relevant_l2_updates[key]
                        for key in sorted(
                            current_relevant_l2_updates,
                            key=lambda item: item.value,
                        )
                    ),
                    audit_outcomes=tuple(current_audit_outcomes),
                    scan_source_received_monotonic_ns=(
                        min(audit_response_receive_times) if audit_response_receive_times else None
                    ),
                    source=RawSourceRef(
                        ingest_id=envelope.ingest_id,
                        payload_sha256=envelope.payload_sha256,
                    ),
                    recovery_requests=tuple(current_recovery_requests),
                    audit_divergences_opened=tuple(current_audit_divergences_opened),
                    audit_divergences_resolved=tuple(current_audit_divergences_resolved),
                    audit_divergences_eligibility_restored=tuple(
                        current_audit_divergences_eligibility_restored
                    ),
                )
            else:
                scan_triggers = self._flush_dirty_scans(
                    envelope.received_wall_time
                    if self._active_scan_observed_at is None
                    else self._active_scan_observed_at
                )
                audit_response_receive_times = tuple(
                    outcome.response_received_monotonic_ns
                    for outcome in current_audit_outcomes
                    if outcome.response_received_monotonic_ns is not None
                )
                unhealthy = tuple(
                    owner.instrument_id
                    for owner in self.owners
                    if owner.health in {BookHealth.INVALID, BookHealth.STALE}
                )
                newly_unhealthy = tuple(
                    instrument_id
                    for instrument_id in unhealthy
                    if owner_state_before.get(instrument_id)
                    != (
                        self._owners[instrument_id].health,
                        self._owners[instrument_id].invalidation_reason,
                    )
                )
                self._record_existing_unhealthy_recovery_requests(
                    newly_unhealthy,
                    existing=tuple(current_recovery_requests),
                )
                recovery_affected = {
                    instrument_id
                    for request in current_recovery_requests
                    for instrument_id in request.affected_instrument_ids
                }
                affected = tuple(
                    sorted(
                        {*unhealthy, *recovery_affected},
                        key=lambda item: item.value,
                    )
                )
                processed = ProcessedEnvelope(
                    ingest_seq=envelope.ingest_seq,
                    success=True,
                    canonical_event_count=self._canonical_events - before_events,
                    scan_count=self._scan_count - before_scans,
                    needs_resync=bool(affected),
                    affected_instrument_ids=affected,
                    reference_refresh_requests=tuple(current_refresh_requests),
                    book_freshness_updates=tuple(
                        current_freshness_updates[key]
                        for key in sorted(current_freshness_updates, key=lambda item: item.value)
                    ),
                    scan_triggers=scan_triggers,
                    error_type=None,
                    relevant_l2_receive_updates=tuple(
                        current_relevant_l2_updates[key]
                        for key in sorted(
                            current_relevant_l2_updates,
                            key=lambda item: item.value,
                        )
                    ),
                    audit_outcomes=tuple(current_audit_outcomes),
                    scan_source_received_monotonic_ns=(
                        min(audit_response_receive_times) if audit_response_receive_times else None
                    ),
                    source=RawSourceRef(
                        ingest_id=envelope.ingest_id,
                        payload_sha256=envelope.payload_sha256,
                    ),
                    recovery_requests=tuple(current_recovery_requests),
                    audit_divergences_opened=tuple(current_audit_divergences_opened),
                    audit_divergences_resolved=tuple(current_audit_divergences_resolved),
                    audit_divergences_eligibility_restored=tuple(
                        current_audit_divergences_eligibility_restored
                    ),
                )
        finally:
            self._active_refresh_requests = None
            self._active_dirty_markets = None
            self._active_freshness_updates = None
            self._active_relevant_l2_updates = None
            self._active_received_monotonic = None
            self._active_envelope = None
            self._active_scan_observed_at = None
            self._active_audit_outcomes = None
            self._active_recovery_requests = None
            self._active_audit_divergences_opened = None
            self._active_audit_divergences_resolved = None
            self._active_audit_divergences_eligibility_restored = None
            self._active_canonical_event = None
        return processed

    def _active_raw_source(self) -> RawSourceRef:
        envelope = self._active_envelope
        if envelope is None:
            raise ProgrammingInvariantViolation(
                "processor recovery evidence was recorded outside envelope processing",
                component="canonical-processor",
            )
        return RawSourceRef(
            ingest_id=envelope.ingest_id,
            payload_sha256=envelope.payload_sha256,
        )

    def _expected_data_error_invalidation_reason(
        self,
        error: BookStateError | NormalizationError | PolymarketSchemaError | ReferenceBuildError,
        *,
        owner_state_before: dict[
            InstrumentId,
            tuple[BookHealth, InvalidationReason | None],
        ],
    ) -> InvalidationReason:
        """Retain known initiating evidence instead of relabeling every failure as schema."""

        if isinstance(error, ReferenceBuildError):
            return InvalidationReason.UNKNOWN_REFERENCE_DATA
        if isinstance(error, BookStateError):
            retained = {
                owner.invalidation_reason
                for owner in self.owners
                if owner.invalidation_reason is not None
                and owner_state_before.get(owner.instrument_id)
                != (owner.health, owner.invalidation_reason)
            }
            if len(retained) == 1:
                return next(iter(retained))
            # A state-owner rejection without one unambiguous retained input
            # reason is a canonical-state failure, not evidence of malformed
            # transport/schema input.
            return InvalidationReason.IMPOSSIBLE_STATE
        return InvalidationReason.SCHEMA_VIOLATION

    def _record_recovery_request(
        self,
        *,
        reason: ProcessorRecoveryReason,
        affected_instrument_ids: tuple[InstrumentId, ...],
        original_reason_code: str,
        effective_invalidation_reason: InvalidationReason | None = None,
        audit_request_id: str | None = None,
        audit_divergence_id: str | None = None,
        reported_top_transition_id: str | None = None,
    ) -> None:
        active = self._active_recovery_requests
        if active is None or not affected_instrument_ids:
            return
        request = ProcessorRecoveryRequest(
            reason=reason,
            source=self._active_raw_source(),
            affected_instrument_ids=tuple(
                sorted(set(affected_instrument_ids), key=lambda item: item.value)
            ),
            original_reason_code=original_reason_code,
            effective_invalidation_reason=effective_invalidation_reason,
            audit_request_id=audit_request_id,
            audit_divergence_id=audit_divergence_id,
            reported_top_transition_id=reported_top_transition_id,
        )
        if request not in active:
            active.append(request)
            self._recovery_reason_counts[request.reason] += 1
            self._recovery_request_chain = _chain_extend(
                self._recovery_request_chain,
                "processor-recovery-request",
                request,
            )

    def _record_existing_unhealthy_recovery_requests(
        self,
        instrument_ids: tuple[InstrumentId, ...],
        *,
        existing: tuple[ProcessorRecoveryRequest, ...],
    ) -> None:
        """Recover typed scalar owner evidence for every otherwise-uncovered book."""

        # A request emitted by the current envelope is the initiating evidence.
        # Other unhealthy owners were attributed by earlier envelopes and must
        # not be reopened or rebound to this raw source.
        if existing:
            return
        covered = {
            instrument_id
            for request in existing
            for instrument_id in request.affected_instrument_ids
        }
        grouped: dict[
            tuple[ProcessorRecoveryReason, str, InvalidationReason | None],
            list[InstrumentId],
        ] = {}
        for instrument_id in instrument_ids:
            if instrument_id in covered:
                continue
            owner = self._owners[instrument_id]
            invalidation_reason = owner.invalidation_reason
            if owner.health is BookHealth.STALE:
                key = (
                    ProcessorRecoveryReason.FRESHNESS_EXPIRY,
                    "BOOK_HEALTH_STALE",
                    None,
                )
            elif invalidation_reason is not None:
                key = (
                    processor_recovery_reason_for_invalidation(invalidation_reason),
                    invalidation_reason.value,
                    invalidation_reason,
                )
            else:
                key = (
                    ProcessorRecoveryReason.UNKNOWN,
                    "UNATTRIBUTED_EXISTING_UNHEALTHY_BOOK",
                    None,
                )
            grouped.setdefault(key, []).append(instrument_id)
        for (reason, original_reason_code, invalidation_reason), affected in sorted(
            grouped.items(),
            key=lambda item: (
                item[0][0].value,
                item[0][1],
                "" if item[0][2] is None else item[0][2].value,
            ),
        ):
            self._record_recovery_request(
                reason=reason,
                affected_instrument_ids=tuple(affected),
                original_reason_code=original_reason_code,
                effective_invalidation_reason=invalidation_reason,
            )

    def _record_book_mutation(
        self,
        mutation: BookMutation,
        *,
        reason: ScanTriggerReason,
        freshness_received_monotonic: MonotonicTimestamp | None = None,
    ) -> None:
        if mutation.audit_economic_changed:
            self._advance_audit_economic_generation(mutation.instrument_id)
        dirty = self._active_dirty_markets
        freshness = self._active_freshness_updates
        received_monotonic = (
            self._active_received_monotonic
            if freshness_received_monotonic is None
            else freshness_received_monotonic
        )
        if dirty is None or freshness is None or received_monotonic is None:
            return
        if mutation.scanner_dirty:
            dirty.setdefault(mutation.market_id, set()).add(reason)
        owner = self._owners.get(mutation.instrument_id)
        if mutation.freshness_changed and owner is not None:
            if owner.health is BookHealth.HEALTHY:
                self._last_accepted_l2_monotonic[mutation.instrument_id] = received_monotonic
                update = BookFreshnessUpdate(
                    market_id=mutation.market_id,
                    instrument_id=mutation.instrument_id,
                    received_monotonic=received_monotonic,
                )
                freshness[mutation.instrument_id] = update
                relevant = self._active_relevant_l2_updates
                if relevant is not None:
                    relevant[mutation.instrument_id] = update
            else:
                freshness.pop(mutation.instrument_id, None)

    def _record_relevant_l2_receive(
        self,
        *,
        market_id: MarketId,
        instrument_id: InstrumentId,
    ) -> None:
        relevant = self._active_relevant_l2_updates
        received_monotonic = self._active_received_monotonic
        if relevant is None or received_monotonic is None:
            return
        relevant[instrument_id] = BookFreshnessUpdate(
            market_id=market_id,
            instrument_id=instrument_id,
            received_monotonic=received_monotonic,
        )

    def _advance_audit_economic_generation(self, instrument_id: InstrumentId) -> None:
        self._audit_economic_generations[instrument_id] = (
            self._audit_economic_generations.get(instrument_id, 0) + 1
        )

    @staticmethod
    def _owner_top(owner: CanonicalL2BookOwner) -> tuple[Price | None, Price | None]:
        snapshot = owner.snapshot()
        if snapshot.health is not BookHealth.HEALTHY:
            return None, None
        best_bid = None if snapshot.best_bid is None else snapshot.best_bid.price
        best_ask = None if snapshot.best_ask is None else snapshot.best_ask.price
        return best_bid, best_ask

    @classmethod
    def _owner_matches_best_bid_ask(
        cls,
        owner: CanonicalL2BookOwner,
        reported: BestBidAskEvent,
    ) -> bool:
        return cls._owner_top(owner) == (reported.best_bid, reported.best_ask)

    @staticmethod
    def _reported_target(payload: PriceChangeEvent) -> tuple[Price, Price] | None:
        final = payload.changes[-1]
        if final.reported_best_bid is None or final.reported_best_ask is None:
            return None
        return final.reported_best_bid, final.reported_best_ask

    def _active_websocket_evidence(
        self,
    ) -> tuple[RawEventEnvelope, int]:
        envelope = self._active_envelope
        if envelope is None or envelope.channel != WS_MARKET:
            raise ProgrammingInvariantViolation(
                "reported-top transition was changed outside WebSocket processing",
                component="canonical-processor",
            )
        ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
        if ordinal is None:
            # Historical/small-test envelopes may predate admission ordinals.
            # The transition still binds the explicit ingest ordinal and can
            # never enter the post-snapshot recognizer without true 1->2
            # admission evidence.
            ordinal = envelope.ingest_seq
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
            raise NormalizationError("reported-top transition source ordinal is invalid")
        return envelope, ordinal

    def _start_reported_top_transition(
        self,
        *,
        event: CanonicalEvent,
        owner: CanonicalL2BookOwner,
        target: tuple[Price, Price],
        entry_source: ReportedTopTransitionEntrySource,
        reason: ReportedTopTransitionReason,
        baseline_evidence: ReportedTopTransitionBaselineEvidence | None = None,
    ) -> ReportedTopTransition:
        envelope, ordinal = self._active_websocket_evidence()
        snapshot = owner.snapshot()
        transition = ReportedTopTransition.start(
            instrument_id=owner.instrument_id,
            market_id=owner.market_id,
            target_best_bid=target[0],
            target_best_ask=target[1],
            starting_best_bid=(None if snapshot.best_bid is None else snapshot.best_bid.price),
            starting_best_ask=(None if snapshot.best_ask is None else snapshot.best_ask.price),
            starting_generation=snapshot.generation,
            starting_economic_sha256=owner.economic_content_sha256(),
            event=event,
            connection_id=envelope.connection_id,
            admission_ordinal=ordinal,
            ingest_seq=envelope.ingest_seq,
            received_monotonic=envelope.received_monotonic,
            maximum_frames=self._config.polymarket.reported_top_transition_max_frames,
            maximum_duration_ns=int(
                self._config.polymarket.reported_top_transition_max_seconds * 1_000_000_000
            ),
            entry_source=entry_source,
            reason=reason,
            baseline_evidence=baseline_evidence,
        )
        self._reported_top_transitions[owner.instrument_id] = transition
        self._reported_top_transitions_started += 1
        self._reported_top_transition_entry_counts[entry_source] += 1
        self._reported_top_transition_frames += 1
        if entry_source is ReportedTopTransitionEntrySource.POST_SNAPSHOT_CATCH_UP:
            self._reported_top_no_pending_recognized += 1
        self._reported_top_transition_chain = _chain_extend(
            self._reported_top_transition_chain,
            "reported-top-transition",
            ("START", transition),
        )
        return transition

    def _advance_reported_top_transition(
        self,
        transition: ReportedTopTransition,
        *,
        event: CanonicalEvent,
        reason: ReportedTopTransitionReason,
    ) -> ReportedTopTransition | None:
        envelope, ordinal = self._active_websocket_evidence()
        if (
            envelope.connection_id != transition.connection_id
            or envelope.received_monotonic.epoch_id != transition.monotonic_epoch_id
        ):
            self._fail_reported_top_transition(
                transition.instrument_id,
                status=ReportedTopTransitionStatus.INVALIDATED,
                reason=ReportedTopTransitionReason.SOCKET_GENERATION_CHANGED,
                invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
            )
            return None
        if envelope.received_monotonic.nanoseconds >= transition.deadline_monotonic_ns:
            self._fail_reported_top_transition(
                transition.instrument_id,
                status=ReportedTopTransitionStatus.EXPIRED,
                reason=ReportedTopTransitionReason.TIME_BOUND_EXPIRED,
                invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
            )
            return None
        if transition.accepted_frame_count >= transition.maximum_frames:
            self._fail_reported_top_transition(
                transition.instrument_id,
                status=ReportedTopTransitionStatus.EXPIRED,
                reason=ReportedTopTransitionReason.FRAME_BOUND_EXCEEDED,
                invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
            )
            return None
        advanced = transition.advance(
            event=event,
            admission_ordinal=ordinal,
            ingest_seq=envelope.ingest_seq,
            received_monotonic=envelope.received_monotonic,
            reason=reason,
        )
        self._reported_top_transitions[transition.instrument_id] = advanced
        self._reported_top_transition_frames += 1
        self._reported_top_transition_chain = _chain_extend(
            self._reported_top_transition_chain,
            "reported-top-transition",
            ("ADVANCE", advanced),
        )
        return advanced

    def _reported_top_transition_close_source(
        self,
    ) -> ReportedTopTransitionCloseSource | None:
        envelope = self._active_envelope
        if envelope is None:
            return None
        raw_ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
        if raw_ordinal is None:
            admission_ordinal = None
        elif isinstance(raw_ordinal, bool) or not isinstance(raw_ordinal, int) or raw_ordinal < 0:
            raise NormalizationError("reported-top close source admission ordinal is invalid")
        else:
            admission_ordinal = raw_ordinal
        canonical_event = self._active_canonical_event
        return ReportedTopTransitionCloseSource(
            raw_source=RawSourceRef(
                ingest_id=envelope.ingest_id,
                payload_sha256=envelope.payload_sha256,
            ),
            ingest_seq=envelope.ingest_seq,
            channel=envelope.channel,
            connection_id=envelope.connection_id,
            received_at=envelope.received_wall_time,
            received_monotonic=envelope.received_monotonic,
            admission_ordinal=admission_ordinal,
            canonical_event_id=(
                None if canonical_event is None else canonical_event.canonical_event_id
            ),
        )

    def _close_reported_top_transition(
        self,
        instrument_id: InstrumentId,
        *,
        status: ReportedTopTransitionStatus,
        reason: ReportedTopTransitionReason,
        ended_monotonic_ns: int | None = None,
        research_eligibility_restored: bool,
    ) -> ReportedTopTransitionOutcome | None:
        transition = self._reported_top_transitions.pop(instrument_id, None)
        if transition is None:
            return None
        current = self._active_received_monotonic
        envelope = self._active_envelope
        duration_is_lower_bound = current is None or (
            current.epoch_id != transition.monotonic_epoch_id
        )
        if ended_monotonic_ns is None:
            ended_monotonic_ns = (
                transition.latest_received_monotonic_ns if current is None else current.nanoseconds
            )
        ended_epoch_id = transition.monotonic_epoch_id if current is None else current.epoch_id
        outcome = ReportedTopTransitionOutcome.close(
            transition,
            status=status,
            reason=reason,
            ended_monotonic_ns=ended_monotonic_ns,
            ended_at=(
                transition.latest_received_at if envelope is None else envelope.received_wall_time
            ),
            ended_monotonic_epoch_id=ended_epoch_id,
            duration_is_lower_bound=duration_is_lower_bound,
            close_source=self._reported_top_transition_close_source(),
            research_eligibility_restored=research_eligibility_restored,
        )
        self._reported_top_transition_outcomes.append(outcome)
        self._reported_top_transition_duration_total_ns += outcome.duration_ns
        self._reported_top_transition_duration_max_ns = max(
            self._reported_top_transition_duration_max_ns,
            outcome.duration_ns,
        )
        self._reported_top_transition_reason_counts[reason] += 1
        if outcome.duration_is_lower_bound:
            self._reported_top_transition_duration_lower_bound_count += 1
        if outcome.research_eligibility_restored:
            self._reported_top_transition_research_eligibility_restored += 1
        if status is ReportedTopTransitionStatus.CONVERGED:
            self._reported_top_transitions_converged += 1
        elif status is ReportedTopTransitionStatus.SUPERSEDED:
            self._reported_top_transitions_superseded += 1
        elif status is ReportedTopTransitionStatus.AUTHORITATIVE_SNAPSHOT:
            self._reported_top_transitions_resnapshotted += 1
        elif status is ReportedTopTransitionStatus.EXPIRED:
            self._reported_top_transitions_expired += 1
        elif status is ReportedTopTransitionStatus.INVALIDATED:
            self._reported_top_transitions_invalidated += 1
        self._reported_top_transition_chain = _chain_extend(
            self._reported_top_transition_chain,
            "reported-top-transition",
            ("CLOSE", outcome),
        )
        return outcome

    def _fail_reported_top_transition(
        self,
        instrument_id: InstrumentId,
        *,
        status: ReportedTopTransitionStatus,
        reason: ReportedTopTransitionReason,
        invalidation_reason: InvalidationReason,
    ) -> None:
        owner = self._owners[instrument_id]
        was_invalid = owner.health is BookHealth.INVALID
        mutation = owner.invalidate(invalidation_reason)
        self._transition_resync_instruments.add(instrument_id)
        outcome = self._close_reported_top_transition(
            instrument_id,
            status=status,
            reason=reason,
            research_eligibility_restored=False,
        )
        self._record_book_mutation(mutation, reason=ScanTriggerReason.INVALIDATION)
        if not was_invalid:
            self._invalidations += 1
        if outcome is not None:
            self._record_recovery_request(
                reason=ProcessorRecoveryReason.REPORTED_TOP_TRANSITION_FAILURE,
                affected_instrument_ids=(instrument_id,),
                original_reason_code=reason.value,
                effective_invalidation_reason=invalidation_reason,
                reported_top_transition_id=outcome.transition_id,
            )

    def _apply_best_bid_ask(self, event: CanonicalEvent) -> None:
        payload = event.payload
        if not isinstance(payload, BestBidAskEvent):
            raise ProgrammingInvariantViolation(
                "best-bid/ask route received another canonical payload",
                component="canonical-processor",
            )
        owner = self._owners[payload.instrument_id]
        self._record_relevant_l2_receive(
            market_id=payload.market_id,
            instrument_id=payload.instrument_id,
        )
        if payload.instrument_id in self._transition_resync_instruments:
            return
        if owner.health is not BookHealth.HEALTHY:
            # The instrument already requires an authoritative snapshot.  A
            # quantity-free reported top cannot repair or further classify an
            # invalid exact book, and must not create a second uncertainty
            # state while resynchronization is pending.
            return
        transition = self._reported_top_transitions.get(payload.instrument_id)
        owner_top = self._owner_top(owner)
        target = payload.best_bid, payload.best_ask
        if transition is None:
            if owner_top == target:
                return
            self._start_reported_top_transition(
                event=event,
                owner=owner,
                target=target,
                entry_source=ReportedTopTransitionEntrySource.BEST_BID_ASK,
                reason=ReportedTopTransitionReason.BBO_AHEAD_OF_EXACT_L2,
            )
            self._advance_audit_economic_generation(payload.instrument_id)
            self._mark_market_dirty(payload.market_id, ScanTriggerReason.BOOK_UPDATE)
            return
        if target == (transition.target_best_bid, transition.target_best_ask):
            advanced = self._advance_reported_top_transition(
                transition,
                event=event,
                reason=ReportedTopTransitionReason.BBO_AHEAD_OF_EXACT_L2,
            )
            if advanced is not None:
                self._advance_audit_economic_generation(payload.instrument_id)
                self._mark_market_dirty(payload.market_id, ScanTriggerReason.BOOK_UPDATE)
            return
        if owner_top == target and target != (
            transition.starting_best_bid,
            transition.starting_best_ask,
        ):
            self._close_reported_top_transition(
                payload.instrument_id,
                status=ReportedTopTransitionStatus.SUPERSEDED,
                reason=ReportedTopTransitionReason.COMPLETE_REPORTED_TOP_SUPERSEDED,
                research_eligibility_restored=(
                    payload.instrument_id not in self._audit_ineligible_instruments
                ),
            )
            self._advance_audit_economic_generation(payload.instrument_id)
            self._mark_market_dirty(payload.market_id, ScanTriggerReason.BOOK_UPDATE)
            return
        if (
            owner_top
            == target
            == (
                transition.starting_best_bid,
                transition.starting_best_ask,
            )
        ):
            # An out-and-back reported top does not prove that the missing
            # quantity-bearing transition was restored.  Retain the original
            # target and uncertainty while binding this later source frame.
            advanced = self._advance_reported_top_transition(
                transition,
                event=event,
                reason=ReportedTopTransitionReason.BBO_AHEAD_OF_EXACT_L2,
            )
            if advanced is not None:
                self._advance_audit_economic_generation(payload.instrument_id)
                self._mark_market_dirty(payload.market_id, ScanTriggerReason.BOOK_UPDATE)
            return
        # An unrelated complete target is conflicting evidence.
        self._fail_reported_top_transition(
            payload.instrument_id,
            status=ReportedTopTransitionStatus.INVALIDATED,
            reason=ReportedTopTransitionReason.CONFLICTING_REPORTED_TOP,
            invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
        )

    def _ws_l2_exceeds_freshness_window(self, instrument_id: InstrumentId) -> bool:
        current = self._active_received_monotonic
        if current is None:
            raise ProgrammingInvariantViolation(
                "WebSocket L2 freshness was checked outside envelope processing",
                component="canonical-processor",
            )
        previous = self._last_accepted_l2_monotonic.get(instrument_id)
        if previous is None:
            return False
        if current.epoch_id != previous.epoch_id or current.nanoseconds < previous.nanoseconds:
            return True
        return current.nanoseconds > (
            previous.nanoseconds + self._scanner.config.maximum_book_age_ns
        )

    def _mark_market_dirty(self, market_id: MarketId, reason: ScanTriggerReason) -> None:
        dirty = self._active_dirty_markets
        if dirty is not None:
            dirty.setdefault(market_id, set()).add(reason)

    def _flush_dirty_scans(self, observed_at: UtcTimestamp) -> tuple[MarketScanTrigger, ...]:
        dirty = self._active_dirty_markets
        if dirty is None:
            raise ProgrammingInvariantViolation(
                "market scan-dirty state was flushed outside envelope processing",
                component="canonical-processor",
            )
        triggers = tuple(
            MarketScanTrigger(
                market_id=market_id,
                reasons=tuple(sorted(dirty[market_id], key=lambda item: item.value)),
            )
            for market_id in sorted(dirty, key=lambda item: item.value)
        )
        dirty.clear()
        for trigger in triggers:
            bundle = self._registry.current(trigger.market_id)
            if bundle is not None:
                if self._scan_latency_observer is None:
                    self._scan_market(bundle, observed_at)
                else:
                    started_ns = time.perf_counter_ns()
                    self._scan_market(bundle, observed_at)
                    self._scan_latency_observer(time.perf_counter_ns() - started_ns)
        return triggers

    def _route(self, envelope: RawEventEnvelope) -> None:
        channel = envelope.channel
        # Only an ordered WebSocket frame may enforce the bound inline.  An
        # unrelated REST/control envelope can be processed while an earlier
        # admitted socket frame is deferred at an audit receive fence; allowing
        # it to expire the transition would overtake a pre-deadline convergence
        # frame.  Idle expiry is owned by the FIFO-empty durable supervisor
        # control, which also binds the exact closing source.
        if channel == WS_MARKET:
            self._expire_reported_top_transitions(envelope)
        if channel == GAMMA_MARKETS_KEYSET:
            for market in parse_gamma_markets(envelope.payload_bytes).markets:
                self._remember_gamma(market)
                self._gamma_source_by_condition[market.condition_id] = self._raw_source(envelope)
                self._try_publish_bundle(market.condition_id, envelope.received_wall_time)
            return
        if channel.startswith(GAMMA_MARKET_PREFIX):
            expected = channel_native_id(channel, GAMMA_MARKET_PREFIX)
            market = parse_gamma_market(envelope.payload_bytes)
            if market.gamma_market_id != expected:
                raise NormalizationError("Gamma direct-market response ID differs from raw channel")
            prior = self._gamma_by_id.get(expected)
            self._validate_gamma_identity(market)
            carried_relationship = (
                market.event_id is None
                and expected in self._gamma_parent_binding_by_id
                and prior is not None
                and market.condition_id == prior.condition_id
                and market.clob_token_ids == prior.clob_token_ids
            )
            if carried_relationship:
                # The direct Gamma surface is known to omit the embedded
                # relationship for some markets.  Preserve an already proven
                # binding until the immediately following exact-ID
                # relationship response revalidates it.
                market = replace(
                    market,
                    event_id=self._gamma_parent_binding_by_id[expected],
                )
            else:
                self._gamma_relationship_source_by_condition.pop(market.condition_id, None)
                if prior is not None and prior.condition_id != market.condition_id:
                    self._gamma_relationship_source_by_condition.pop(prior.condition_id, None)
            self._remember_gamma(market)
            self._gamma_source_by_condition[market.condition_id] = self._raw_source(envelope)
            self._try_publish_bundle(market.condition_id, envelope.received_wall_time)
            return
        if channel.startswith(GAMMA_MARKET_RELATIONSHIP_PREFIX):
            expected = channel_native_id(channel, GAMMA_MARKET_RELATIONSHIP_PREFIX)
            relationship = parse_gamma_market_relationship(envelope.payload_bytes)
            if relationship.gamma_market_id != expected:
                raise NormalizationError("Gamma relationship response ID differs from raw channel")
            direct = self._gamma_by_id.get(expected)
            if direct is None:
                raise NormalizationError(
                    "Gamma relationship response has no preceding direct-market response"
                )
            if (
                relationship.condition_id != direct.condition_id
                or relationship.clob_token_ids != direct.clob_token_ids
            ):
                raise NormalizationError(
                    "Gamma relationship response differs from the direct market identity"
                )
            if (
                expected in self._gamma_parent_binding_by_id
                and relationship.event_id != self._gamma_parent_binding_by_id[expected]
            ):
                raise NormalizationError(
                    "Gamma relationship response changes the frozen parent event identity"
                )
            enriched = replace(direct, event_id=relationship.event_id)
            self._remember_gamma(enriched)
            self._gamma_parent_binding_by_id[expected] = relationship.event_id
            self._gamma_relationship_source_by_condition[enriched.condition_id] = self._raw_source(
                envelope
            )
            self._try_publish_bundle(enriched.condition_id, envelope.received_wall_time)
            return
        if channel.startswith(GAMMA_EVENT_PREFIX):
            expected = channel_native_id(channel, GAMMA_EVENT_PREFIX)
            event = parse_gamma_event(envelope.payload_bytes)
            if event.event_id != expected:
                raise NormalizationError("Gamma event response ID differs from raw channel")
            self._events_by_id[event.event_id] = event
            self._event_source_by_id[event.event_id] = self._raw_source(envelope)
            for market in self._gamma_by_condition.values():
                if market.event_id == event.event_id:
                    self._try_publish_bundle(market.condition_id, envelope.received_wall_time)
            return
        if channel.startswith(CLOB_MARKET_PREFIX):
            condition = channel_native_id(channel, CLOB_MARKET_PREFIX)
            self._clob_by_condition[condition] = parse_clob_market_info(
                envelope.payload_bytes, condition_id=condition
            )
            self._clob_source_by_condition[condition] = self._raw_source(envelope)
            self._try_publish_bundle(condition, envelope.received_wall_time)
            return
        if channel == REST_BOOK:
            native = parse_rest_book(envelope.payload_bytes)
            self._rest_source_by_condition[native.condition_id] = self._raw_source(envelope)
            bundle = self._try_publish_bundle(
                native.condition_id,
                envelope.received_wall_time,
                rest_book=native,
            )
            if bundle is None:
                raise NormalizationError("REST book arrived before matching Gamma/CLOB reference")
            context = self._normalization_context(envelope, bundle)
            rest = self._normalizer.normalize_rest_book(native, context)
            pending_divergence = self._pending_audit_divergences.get(rest.instrument_id)
            if (
                pending_divergence is not None
                and envelope.received_monotonic.nanoseconds
                < pending_divergence.classification_received_monotonic_ns
            ):
                raise ProgrammingInvariantViolation(
                    "authoritative REST recovery precedes mismatch classification",
                    component="canonical-processor",
                )
            owner = self._owners[rest.instrument_id]
            was_unhealthy = owner.health is not BookHealth.HEALTHY
            self._prepare_for_rest_snapshot(owner)
            mutation = owner.apply_rest_snapshot(rest)
            self._close_reported_top_transition(
                rest.instrument_id,
                status=ReportedTopTransitionStatus.AUTHORITATIVE_SNAPSHOT,
                reason=ReportedTopTransitionReason.AUTHORITATIVE_FULL_SNAPSHOT,
                research_eligibility_restored=(
                    rest.instrument_id not in self._pending_audit_divergences
                    and rest.instrument_id not in self._audit_replacement_captures
                    and not self._permanent_audit_fault_blocks_instrument(rest.instrument_id)
                ),
            )
            self._record_book_mutation(
                mutation,
                reason=ScanTriggerReason.REST_SNAPSHOT,
            )
            self._resolve_audit_divergence_by_rest_snapshot(
                envelope=envelope,
                rest=rest,
                owner=owner,
            )
            self._resolve_current_audit_faults(
                instrument_id=rest.instrument_id,
                resolution=AuditFaultResolution.AUTHORITATIVE_RESNAPSHOT,
                resolution_source=RawSourceRef(
                    ingest_id=envelope.ingest_id,
                    payload_sha256=envelope.payload_sha256,
                ),
                resolved_at=envelope.received_wall_time,
                resolved_monotonic_ns=envelope.received_monotonic.nanoseconds,
            )
            replacement_capture = self._audit_replacement_captures.get(rest.instrument_id)
            if (
                replacement_capture is None
                or replacement_capture.audit_start_schema == AUDIT_START_V2_SCHEMA
            ):
                self._transition_resync_instruments.discard(rest.instrument_id)
            if replacement_capture is None and not self._audit_fault_blocks_instrument(
                rest.instrument_id
            ):
                self._clear_audit_ineligible(rest.instrument_id)
            if was_unhealthy:
                self._resyncs += 1
            return
        if channel == CONTROL_REST_AUDIT_START:
            self._stage_rest_audit(envelope)
            return
        if channel == CONTROL_REST_AUDIT_FAILED:
            self._fail_rest_audit_from_control(envelope)
            return
        if channel == CONTROL_REST_AUDIT_EXPIRED:
            self._expire_rest_audit_eligibility(envelope)
            return
        if channel == CONTROL_REST_AUDIT_CLASSIFY_PREPARE:
            self._prepare_staged_rest_audit_classification(envelope)
            return
        if channel == CONTROL_REST_AUDIT_CLASSIFY:
            self._classify_staged_rest_audit_response(envelope)
            return
        if channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE:
            self._release_audit_replacement_capture_from_control(envelope)
            return
        if channel == CONTROL_REPORTED_TOP_TRANSITION_EXPIRED:
            self._expire_reported_top_transition_from_control(envelope)
            return
        if channel == REST_BOOK_AUDIT:
            if "classification_deferred" in envelope.transport_metadata:
                if envelope.transport_metadata["classification_deferred"] is not True:
                    raise NormalizationError(
                        "REST audit deferred-classification marker must be true"
                    )
                self._stage_rest_audit_response(envelope)
            else:
                self._complete_rest_audit_response(envelope)
            return
        if channel == CONTROL_BOOK_INVALIDATION:
            invalidation_reason = self._parse_control_invalidation(envelope.payload_bytes)
            pending_request_ids = tuple(sorted(self._pending_rest_audits))
            # One invalidation control closes the whole pending-audit batch.
            # Reserve every required lifecycle slot before consuming the first
            # request so two pending audits cannot half-commit at the bound.
            self._assert_audit_fault_capacity_available(required_slots=len(pending_request_ids))
            for request_id in pending_request_ids:
                start = self._pending_rest_audits.pop(request_id)
                start_source = self._pending_rest_audit_start_sources.pop(request_id, None)
                if start_source is None:
                    raise ProgrammingInvariantViolation(
                        "pending REST audit lacks its start-control source",
                        component="canonical-processor",
                    )
                self._pending_rest_audit_responses.pop(request_id, None)
                self._pending_rest_audit_classification_preparations.pop(request_id, None)
                self._prepare_attest_rest_audit_requests.discard(request_id)
                self._pending_rest_audit_hard_expiries.pop(request_id, None)
                self._pending_rest_audit_response_deadlines.pop(request_id, None)
                self._record_audit_outcome(
                    self._failed_audit_outcome(
                        start,
                        reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
                        start_source=start_source,
                        classification_source=self._active_raw_source(),
                    )
                )
            self.invalidate_all(invalidation_reason)
            return
        if channel == WS_MARKET:
            self._advance_websocket_watermark(envelope)
            self._route_websocket(envelope)
            return
        raise ValueError(f"unsupported raw channel: {channel}")

    @staticmethod
    def _parse_control_invalidation(payload: bytes) -> InvalidationReason:
        decoded = decode_json_exact(payload)
        if not isinstance(decoded, dict):
            raise NormalizationError("book-invalidation control payload must be an object")
        control = cast(dict[object, object], decoded)
        if set(control) != {"reason"}:
            raise NormalizationError(
                "book-invalidation control payload must contain exactly the reason field"
            )
        reason_value = control["reason"]
        if not isinstance(reason_value, str):
            raise NormalizationError("book-invalidation control reason must be a string")
        try:
            reason = InvalidationReason(reason_value)
        except ValueError as error:
            raise NormalizationError(
                f"unknown book-invalidation control reason: {reason_value!r}"
            ) from error
        if payload != canonical_json_bytes({"reason": reason.value}):
            raise NormalizationError(
                "book-invalidation control payload must use canonical JSON encoding"
            )
        return reason

    @staticmethod
    def _control_object(
        payload: bytes,
        *,
        schema: str,
        expected_keys: frozenset[str],
    ) -> dict[str, object]:
        decoded = decode_json_exact(payload)
        if not isinstance(decoded, dict):
            raise NormalizationError("audit control payload must be an object")
        control = cast(dict[str, object], decoded)
        if frozenset(control) != expected_keys:
            raise NormalizationError("audit control payload fields differ from schema")
        if control.get("schema") != schema:
            raise NormalizationError("audit control payload schema is unsupported")
        return control

    @staticmethod
    def _control_text(control: dict[str, object], key: str, *, maximum: int = 2048) -> str:
        value = control[key]
        if not isinstance(value, str) or not value or len(value) > maximum:
            raise NormalizationError(f"audit control {key} must be bounded non-empty text")
        return value

    @staticmethod
    def _control_integer(
        control: dict[str, object],
        key: str,
        *,
        minimum: int = 0,
        maximum: int | None = None,
    ) -> int:
        value = control[key]
        if not isinstance(value, JsonNumber):
            raise NormalizationError(f"audit control {key} is outside its integer bounds")
        lexeme = str(value)
        if (
            not lexeme.isascii()
            or not lexeme.isdigit()
            or (len(lexeme) > 1 and lexeme.startswith("0"))
            or len(lexeme) > 20
        ):
            raise NormalizationError(f"audit control {key} must be a canonical integer")
        parsed = int(lexeme)
        if parsed < minimum or (maximum is not None and parsed > maximum):
            raise NormalizationError(f"audit control {key} is outside its integer bounds")
        return parsed

    def _parse_audit_start(self, payload: bytes) -> AuditStartEvidence:
        decoded = decode_json_exact(payload)
        if not isinstance(decoded, dict):
            raise NormalizationError("audit start control payload must be an object")
        start_record = cast(dict[str, object], decoded)
        raw_schema = start_record.get("schema")
        if raw_schema == AUDIT_START_V1_SCHEMA:
            schema = AUDIT_START_V1_SCHEMA
            trigger_reason = AuditTriggerReason.UNKNOWN_LEGACY
            expected_keys = frozenset(
                {
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
            )
        elif raw_schema == AUDIT_START_V2_SCHEMA:
            schema = AUDIT_START_V2_SCHEMA
            expected_keys = frozenset(
                {
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
                    "trigger_reason",
                }
            )
            try:
                trigger_reason = AuditTriggerReason(
                    self._control_text(start_record, "trigger_reason", maximum=128)
                )
            except (KeyError, ValueError) as error:
                raise NormalizationError("audit start trigger reason is unknown") from error
        else:
            raise NormalizationError("audit start control schema is unsupported")
        control = self._control_object(
            payload,
            schema=schema,
            expected_keys=expected_keys,
        )
        try:
            connection_id = ConnectionId(self._control_text(control, "connection_id"))
            instrument_id = InstrumentId(self._control_text(control, "instrument_id"))
            start = AuditStartEvidence(
                request_id=self._control_text(control, "request_id", maximum=128),
                connection_id=connection_id,
                native_asset_id=self._control_text(control, "native_asset_id"),
                attempt=self._control_integer(control, "attempt", minimum=1),
                request_started_monotonic_ns=self._control_integer(
                    control,
                    "request_started_monotonic_ns",
                ),
                starting_admission_watermark=WebSocketAdmissionWatermark(
                    connection_id=connection_id,
                    ordinal=self._control_integer(control, "starting_admission_ordinal"),
                ),
                starting_processed_watermark=WebSocketAdmissionWatermark(
                    connection_id=connection_id,
                    ordinal=self._control_integer(control, "starting_processed_ordinal"),
                ),
                starting_book=AuditBookIdentity(
                    instrument_id=instrument_id,
                    generation=self._control_integer(control, "starting_book_generation"),
                    economic_content_sha256=self._control_text(
                        control,
                        "starting_book_economic_sha256",
                        maximum=64,
                    ),
                ),
                trigger_reason=trigger_reason,
                schema=schema,
            )
        except ValueError as error:
            raise NormalizationError("audit start evidence is invalid") from error
        if payload != start.to_payload():
            raise NormalizationError("audit start evidence must use canonical encoding")
        return start

    def _stage_rest_audit(self, envelope: RawEventEnvelope) -> None:
        start = self._parse_audit_start(envelope.payload_bytes)
        if start.connection_id != envelope.connection_id:
            raise NormalizationError("audit start envelope belongs to another socket generation")
        if start.request_started_monotonic_ns != envelope.received_monotonic.nanoseconds:
            raise NormalizationError("audit start timestamp differs from raw receive evidence")
        if start.request_id in self._pending_rest_audits:
            raise NormalizationError("audit request ID is already pending")
        if len(self._pending_rest_audits) >= 2:
            raise NormalizationError("at most two REST audit requests may be pending")
        if any(
            pending.native_asset_id == start.native_asset_id
            for pending in self._pending_rest_audits.values()
        ):
            raise NormalizationError("only one REST audit request per instrument may be pending")
        raw_hard_expiry = envelope.transport_metadata.get("hard_expiry_monotonic_ns")
        raw_response_deadline = envelope.transport_metadata.get("response_deadline_monotonic_ns")
        raw_run_deadline = envelope.transport_metadata.get("run_deadline_monotonic_ns")
        raw_classification_protocol = envelope.transport_metadata.get("classification_protocol")
        bound_deadlines = (
            raw_hard_expiry,
            raw_response_deadline,
            raw_run_deadline,
        )
        if any(value is None for value in bound_deadlines) and not all(
            value is None for value in bound_deadlines
        ):
            raise NormalizationError(
                "audit start must bind hard expiry, response deadline, and run deadline"
            )
        requested_mode = (
            _RestAuditProtocolMode.LEGACY
            if raw_hard_expiry is None
            else _RestAuditProtocolMode.DEFERRED_BOUND
        )
        prepare_attest_required = False
        if raw_hard_expiry is None and raw_classification_protocol is not None:
            raise NormalizationError("legacy audit start cannot select a classification protocol")
        if (
            self._rest_audit_protocol_mode is not None
            and requested_mode is not self._rest_audit_protocol_mode
        ):
            raise NormalizationError("audit start changes the run protocol mode")
        if raw_hard_expiry is not None:
            if raw_classification_protocol not in {
                None,
                AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
            }:
                raise NormalizationError("audit classification protocol is unsupported")
            if (
                raw_classification_protocol is None
                and not self._allow_historical_direct_deferred_audit
            ):
                raise NormalizationError(
                    "current-source bound audit start requires prepare/attest classification"
                )
            prepare_attest_required = (
                raw_classification_protocol == AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL
            )
            if prepare_attest_required and start.schema != AUDIT_START_V2_SCHEMA:
                raise NormalizationError("prepare/attest classification requires a v2 start")
            if (
                self._rest_audit_prepare_attest_required is not None
                and prepare_attest_required != self._rest_audit_prepare_attest_required
            ):
                raise NormalizationError("audit classification protocol changes within one run")
            if (
                isinstance(raw_hard_expiry, bool)
                or not isinstance(raw_hard_expiry, int)
                or raw_hard_expiry <= start.request_started_monotonic_ns
            ):
                raise NormalizationError("audit start hard expiry metadata is invalid")
            if (
                isinstance(raw_response_deadline, bool)
                or not isinstance(raw_response_deadline, int)
                or raw_response_deadline <= start.request_started_monotonic_ns
                or raw_response_deadline > raw_hard_expiry
            ):
                raise NormalizationError("audit start response deadline metadata is invalid")
            if (
                isinstance(raw_run_deadline, bool)
                or not isinstance(raw_run_deadline, int)
                or raw_run_deadline <= start.request_started_monotonic_ns
                or raw_response_deadline != min(raw_hard_expiry, raw_run_deadline)
            ):
                raise NormalizationError("audit start run deadline metadata is invalid")
            accepted_freshness = self._last_accepted_l2_monotonic.get(
                start.starting_book.instrument_id
            )
            if accepted_freshness is None:
                raise NormalizationError("deferred audit start lacks accepted book freshness")
            if accepted_freshness.epoch_id != envelope.received_monotonic.epoch_id:
                raise NormalizationError(
                    "deferred audit start freshness belongs to another monotonic epoch"
                )
            if accepted_freshness.nanoseconds > start.request_started_monotonic_ns:
                raise NormalizationError(
                    "deferred audit start precedes its accepted book freshness"
                )
            expected_hard_expiry = (
                accepted_freshness.nanoseconds + self._scanner.config.maximum_book_age_ns
            )
            if raw_hard_expiry != expected_hard_expiry:
                raise NormalizationError(
                    "audit start hard expiry differs from accepted book freshness"
                )
        current_processed = self.processed_websocket_watermark(start.connection_id)
        if current_processed != start.starting_processed_watermark:
            raise NormalizationError("audit start processed watermark is ambiguous")
        current = self.audit_book_identity(start.native_asset_id)
        if current != start.starting_book:
            if (
                self._allow_v1_audit_generation_rebase
                and current.instrument_id == start.starting_book.instrument_id
                and current.economic_content_sha256 == start.starting_book.economic_content_sha256
            ):
                # Historical v1 audit controls bind the owner generation used
                # by the code that produced them.  A corrected replay may
                # renumber that non-economic counter while preserving the
                # exact book hash and the same processed WebSocket fence.  In
                # that one narrow case, rebase the fence to the corrected
                # generation and bind the compatibility decision into the
                # terminal identity.  Economic-hash disagreement remains a
                # real race and is never rebased.
                recorded = start.starting_book
                start = replace(start, starting_book=current)
                self._audit_start_generation_rebases += 1
                self._audit_compatibility_chain = _chain_extend(
                    self._audit_compatibility_chain,
                    "rest-audit-generation-rebase",
                    (start.request_id, recorded, current),
                )
            else:
                raise NormalizationError("audit start book identity changed before staging")
        if self.audit_recovery_blocks_native_asset(start.native_asset_id):
            raise NormalizationError("audit start conflicts with unresolved divergence recovery")
        self._rest_audit_protocol_mode = requested_mode
        if raw_hard_expiry is not None:
            self._rest_audit_prepare_attest_required = prepare_attest_required
        if raw_run_deadline is not None:
            if self._rest_audit_run_deadline_ns is not None and (
                raw_run_deadline != self._rest_audit_run_deadline_ns
                or envelope.received_monotonic.epoch_id != self._rest_audit_run_deadline_epoch_id
            ):
                raise NormalizationError("audit start changes the bound run deadline")
            self._rest_audit_run_deadline_epoch_id = envelope.received_monotonic.epoch_id
            self._rest_audit_run_deadline_ns = cast(int, raw_run_deadline)
        start_source = RawSourceRef(
            ingest_id=envelope.ingest_id,
            payload_sha256=envelope.payload_sha256,
        )
        self._pending_rest_audits[start.request_id] = start
        self._pending_rest_audit_start_sources[start.request_id] = start_source
        self._audit_start_trigger_counts[start.trigger_reason] += 1
        self._audit_start_chain = _chain_extend(
            self._audit_start_chain,
            "rest-audit-start",
            (start, start_source),
        )
        if raw_hard_expiry is not None:
            self._pending_rest_audit_hard_expiries[start.request_id] = raw_hard_expiry
            self._pending_rest_audit_response_deadlines[start.request_id] = cast(
                int, raw_response_deadline
            )
            if prepare_attest_required:
                self._prepare_attest_rest_audit_requests.add(start.request_id)

    def _parse_audit_failure(self, payload: bytes) -> AuditFailureCommand:
        decoded = decode_json_exact(payload)
        if not isinstance(decoded, dict):
            raise NormalizationError("audit failure control payload must be an object")
        failure_record = cast(dict[str, object], decoded)
        raw_schema = failure_record.get("schema")
        if raw_schema == AUDIT_FAILURE_V1_SCHEMA:
            control = self._control_object(
                payload,
                schema=AUDIT_FAILURE_V1_SCHEMA,
                expected_keys=frozenset({"reason", "request_id", "schema"}),
            )
        elif raw_schema == AUDIT_FAILURE_V2_SCHEMA:
            control = self._control_object(
                payload,
                schema=AUDIT_FAILURE_V2_SCHEMA,
                expected_keys=frozenset(
                    {
                        "category",
                        "endpoint_class",
                        "error_type",
                        "http_attempt",
                        "reason",
                        "request_id",
                        "retry_after_disposition",
                        "retry_after_nanoseconds",
                        "retry_blocked_reason",
                        "retry_exhausted",
                        "retryable",
                        "schema",
                        "status_code",
                    }
                ),
            )
        else:
            raise NormalizationError("audit failure control schema is unsupported")

        def nullable_integer(key: str, *, minimum: int = 0) -> int | None:
            return (
                None
                if control[key] is None
                else self._control_integer(control, key, minimum=minimum)
            )

        def nullable_boolean(key: str) -> bool | None:
            value = control[key]
            if value is None or isinstance(value, bool):
                return value
            raise NormalizationError(f"audit control {key} must be boolean or null")

        try:
            reason = AuditFailureReason(self._control_text(control, "reason", maximum=128))
            request_id = self._control_text(control, "request_id", maximum=128)
            if raw_schema == AUDIT_FAILURE_V1_SCHEMA:
                command = AuditFailureCommand(
                    request_id=request_id,
                    reason=reason,
                    schema=AUDIT_FAILURE_V1_SCHEMA,
                )
            else:
                raw_endpoint = control["endpoint_class"]
                raw_category = control["category"]
                raw_retry_after_disposition = control["retry_after_disposition"]
                raw_retry_blocked_reason = control["retry_blocked_reason"]
                command = AuditFailureCommand(
                    request_id=request_id,
                    reason=reason,
                    error_type=self._control_text(control, "error_type", maximum=256),
                    endpoint_class=(
                        None
                        if raw_endpoint is None
                        else PolymarketEndpointClass(
                            self._control_text(control, "endpoint_class", maximum=128)
                        )
                    ),
                    category=(
                        None
                        if raw_category is None
                        else PolymarketHttpFailureCategory(
                            self._control_text(control, "category", maximum=128)
                        )
                    ),
                    retryable=nullable_boolean("retryable"),
                    attempt=nullable_integer("http_attempt"),
                    status_code=nullable_integer("status_code", minimum=100),
                    retry_after_nanoseconds=(
                        None
                        if control["retry_after_nanoseconds"] is None
                        else self._control_integer(
                            control,
                            "retry_after_nanoseconds",
                            maximum=MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS,
                        )
                    ),
                    retry_after_disposition=(
                        None
                        if raw_retry_after_disposition is None
                        else PolymarketRetryAfterDisposition(
                            self._control_text(
                                control,
                                "retry_after_disposition",
                                maximum=128,
                            )
                        )
                    ),
                    retry_exhausted=nullable_boolean("retry_exhausted"),
                    retry_blocked_reason=(
                        None
                        if raw_retry_blocked_reason is None
                        else PolymarketRetryBlockedReason(
                            self._control_text(
                                control,
                                "retry_blocked_reason",
                                maximum=128,
                            )
                        )
                    ),
                    schema=AUDIT_FAILURE_V2_SCHEMA,
                )
                command.validate_against_public_read_policy(
                    maximum_attempts=(self._config.polymarket.public_reads.maximum_attempts),
                    maximum_retry_after_nanoseconds=round(
                        self._config.polymarket.public_reads.maximum_retry_after_seconds
                        * 1_000_000_000
                    ),
                )
        except ValueError as error:
            raise NormalizationError("audit failure evidence is invalid") from error
        if payload != command.to_payload():
            raise NormalizationError("audit failure evidence must use canonical encoding")
        return command

    def _expire_rest_audit_eligibility(self, envelope: RawEventEnvelope) -> None:
        control = self._control_object(
            envelope.payload_bytes,
            schema=AUDIT_EXPIRY_SCHEMA,
            expected_keys=frozenset(
                {
                    "last_accepted_receive_monotonic_ns",
                    "native_asset_id",
                    "schema",
                }
            ),
        )
        native_asset_id = self._control_text(control, "native_asset_id")
        raw_freshness = control["last_accepted_receive_monotonic_ns"]
        if raw_freshness is None:
            freshness_ns = None
        else:
            freshness_ns = self._control_integer(
                control,
                "last_accepted_receive_monotonic_ns",
            )
        command = AuditExpiryCommand(
            native_asset_id=native_asset_id,
            last_accepted_receive_monotonic_ns=freshness_ns,
        )
        if envelope.payload_bytes != command.to_payload():
            raise NormalizationError("audit expiry evidence must use canonical encoding")
        if any(
            pending.native_asset_id == native_asset_id
            for pending in self._pending_rest_audits.values()
        ):
            raise NormalizationError("audit expiry conflicts with a pending request")
        identity = self.audit_book_identity(native_asset_id)
        self._mark_audit_ineligible(identity.instrument_id, nonfault=True)
        owner = self._owners[identity.instrument_id]
        self._mark_market_dirty(owner.market_id, ScanTriggerReason.REST_AUDIT)
        self._record_recovery_request(
            reason=ProcessorRecoveryReason.FRESHNESS_EXPIRY,
            affected_instrument_ids=(identity.instrument_id,),
            original_reason_code=AuditFailureReason.BOOK_FRESHNESS_EXPIRED.value,
        )

    def _record_audit_failure_outcome(
        self,
        evidence: AuditOutcomeEvidence,
        *,
        command: AuditFailureCommand,
        original_reason: AuditFailureReason,
    ) -> None:
        """Record one explicit failure while preserving the historical hook ABI."""

        if self._active_audit_fault_recording_context is not None:
            raise ProgrammingInvariantViolation(
                "audit fault recording context cannot be nested",
                component="canonical-processor",
            )
        self._active_audit_fault_recording_context = _AuditFaultRecordingContext(
            command=command,
            original_reason=original_reason,
        )
        try:
            # Historical offline instruments monkeypatch this method with the
            # original two-argument ``(self, evidence)`` callable. Keep every
            # invocation bytecode-compatible while the immutable active
            # context carries current v2 lifecycle metadata to the original.
            self._record_audit_outcome(evidence)
        finally:
            self._active_audit_fault_recording_context = None

    def _record_audit_outcome(
        self,
        evidence: AuditOutcomeEvidence,
    ) -> None:
        active = self._active_audit_outcomes
        if active is None:
            raise ProgrammingInvariantViolation(
                "audit outcome was recorded outside envelope processing",
                component="canonical-processor",
            )
        if evidence.outcome is AuditOutcome.FAILED:
            self._assert_audit_fault_capacity_available()
        self._rest_audits += 1
        if evidence.outcome is AuditOutcome.MATCH:
            self._rest_audit_matches += 1
            response_received_monotonic_ns = evidence.response_received_monotonic_ns
            if response_received_monotonic_ns is not None:
                current_match_ns = self._audit_last_successful_match_monotonic_ns
                self._audit_last_successful_match_monotonic_ns = (
                    response_received_monotonic_ns
                    if current_match_ns is None
                    else max(current_match_ns, response_received_monotonic_ns)
                )
        elif evidence.outcome is AuditOutcome.MISMATCH:
            self._rest_audit_mismatches += 1
            diagnostic = evidence.mismatch_diagnostic
            cause = AuditMismatchCause.UNKNOWN if diagnostic is None else diagnostic.cause
            self._audit_mismatch_cause_counts[cause] += 1
            if diagnostic is not None:
                self._audit_mismatch_diagnostic_count += 1
                self._audit_mismatch_diagnostics.append(diagnostic)
                self._latest_audit_mismatch_diagnostics[cause] = diagnostic
        elif evidence.outcome is AuditOutcome.RACED:
            self._rest_audit_raced += 1
        else:
            self._rest_audit_failed += 1
            fault_context = self._active_audit_fault_recording_context
            self._record_audit_fault_lifecycle(
                evidence,
                command=None if fault_context is None else fault_context.command,
                original_reason=None if fault_context is None else fault_context.original_reason,
            )
        if evidence.reason is AuditFailureReason.RACE_RETRY_EXHAUSTED:
            self._audit_race_budget_exhaustions += 1
        response_boundary_ordinal = evidence.response_boundary_ordinal
        if response_boundary_ordinal is not None:
            barrier_lag_frames = response_boundary_ordinal - evidence.starting_processed_ordinal
            current_maximum = self._audit_barrier_lag_frames_max
            self._audit_barrier_lag_frames_max = (
                barrier_lag_frames
                if current_maximum is None
                else max(current_maximum, barrier_lag_frames)
            )
        self._audit_outcome_trigger_counts[(evidence.trigger_reason, evidence.outcome)] += 1
        self._audit_outcome_chain = _chain_extend(
            self._audit_outcome_chain,
            "rest-audit-outcome",
            evidence,
        )
        self._audit_outcomes.append(evidence)
        active.append(evidence)

    def _assert_audit_fault_capacity_available(self, *, required_slots: int = 1) -> None:
        """Reserve bounded lifecycle slots before an audit transaction mutates state.

        Response classification can discover a durable failure only after it
        validates the response and fence.  Refuse the transaction before
        consuming pending request state when no failure lifecycle slot remains;
        otherwise a late capacity error could partially commit counters, book
        eligibility, or PREPARE/attestation state.
        """

        if required_slots < 0:
            raise ProgrammingInvariantViolation(
                "audit fault capacity reservation cannot be negative",
                component="canonical-processor",
            )
        if (
            len(self._current_audit_faults) + len(self._permanent_audit_faults) + required_slots
            > MAXIMUM_ACTIVE_AUDIT_FAULTS
        ):
            raise _AuditFaultCapacityError("active audit fault hard bound reached")

    def _record_audit_fault_lifecycle(
        self,
        evidence: AuditOutcomeEvidence,
        *,
        command: AuditFailureCommand | None,
        original_reason: AuditFailureReason | None,
    ) -> None:
        reason = evidence.reason
        if evidence.outcome is not AuditOutcome.FAILED or reason is None:
            raise ProgrammingInvariantViolation(
                "audit fault lifecycle requires a FAILED outcome",
                component="canonical-processor",
            )
        start_source = evidence.audit_start_source
        failure_source = evidence.classification_source
        envelope = self._active_envelope
        if start_source is None or failure_source is None or envelope is None:
            raise ProgrammingInvariantViolation(
                "current audit failure lacks exact durable control identities",
                component="canonical-processor",
            )
        if command is None:
            failure_schema: str | None = None
            if envelope.channel == CONTROL_REST_AUDIT_CLASSIFY:
                decoded = decode_json_exact(envelope.payload_bytes)
                if not isinstance(decoded, dict):
                    raise ProgrammingInvariantViolation(
                        "validated audit classification lost its durable schema",
                        component="canonical-processor",
                    )
                raw_schema = cast(dict[str, object], decoded).get("schema")
                if raw_schema not in {
                    AUDIT_CLASSIFICATION_V1_SCHEMA,
                    AUDIT_CLASSIFICATION_V2_SCHEMA,
                }:
                    raise ProgrammingInvariantViolation(
                        "validated audit classification has unknown durable schema",
                        component="canonical-processor",
                    )
                failure_evidence_kind = AuditFaultFailureEvidenceKind.AUDIT_CLASSIFICATION_CONTROL
                failure_schema = cast(str, raw_schema)
            elif envelope.channel == REST_BOOK_AUDIT:
                failure_evidence_kind = AuditFaultFailureEvidenceKind.REST_AUDIT_RESPONSE
            elif envelope.channel == CONTROL_BOOK_INVALIDATION:
                failure_evidence_kind = AuditFaultFailureEvidenceKind.BOOK_INVALIDATION_CONTROL
            else:
                raise ProgrammingInvariantViolation(
                    "implicit audit failure has no typed durable source kind",
                    component="canonical-processor",
                )
            command = AuditFailureCommand(
                request_id=evidence.request_id,
                reason=reason,
                error_type=reason.value,
            )
        else:
            failure_evidence_kind = AuditFaultFailureEvidenceKind.AUDIT_FAILURE_CONTROL
            failure_schema = command.schema
        owner = self._owners[evidence.starting_book.instrument_id]
        effective_original_reason = command.reason if original_reason is None else original_reason
        if reason is not AuditFailureReason.HTTP_REQUEST_FAILED:
            # A local coordinator/processor override is the effective fault.
            # Preserve the original transport metadata as history, but never
            # let it soften a known internal failure into recoverable state.
            origin = AuditFaultOrigin.INTERNAL
            status = AuditFaultStatus.PERMANENT
        else:
            origin = audit_fault_origin(command)
            status = initial_audit_fault_status(command, origin=origin)
        blocks_current_scope = audit_fault_evidence_blocks_current_scope(
            kind=failure_evidence_kind,
            audit_start_schema=evidence.audit_start_schema,
            failure_schema=failure_schema,
        )
        fault_id = canonical_sha256(
            {
                "audit_start_schema": evidence.audit_start_schema,
                "audit_start_source": start_source,
                "blocks_current_scope": blocks_current_scope,
                "failure_evidence_kind": failure_evidence_kind,
                "failure_schema": failure_schema,
                "failure_source": failure_source,
                "original_reason": effective_original_reason,
                "reason": reason,
                "request_id": evidence.request_id,
                "schema": "mma-rest-audit-fault-id-v1",
            }
        )
        record = AuditFaultLifecycleRecord(
            fault_id=fault_id,
            request_id=evidence.request_id,
            instrument_id=evidence.starting_book.instrument_id,
            market_id=owner.market_id,
            native_asset_id=evidence.native_asset_id,
            original_reason=effective_original_reason,
            reason=reason,
            origin=origin,
            status=status,
            request_started_monotonic_ns=evidence.request_started_monotonic_ns,
            audit_start_source=start_source,
            failure_source=failure_source,
            observed_at=envelope.received_wall_time,
            observed_monotonic_ns=envelope.received_monotonic.nanoseconds,
            error_type=command.error_type or reason.value,
            audit_start_schema=evidence.audit_start_schema,
            failure_evidence_kind=failure_evidence_kind,
            failure_schema=failure_schema,
            blocks_current_scope=blocks_current_scope,
            response_completed_monotonic_ns=evidence.response_received_monotonic_ns,
            endpoint_class=command.endpoint_class,
            category=command.category,
            retryable=command.retryable,
            attempt=command.attempt,
            status_code=command.status_code,
            retry_after_nanoseconds=command.retry_after_nanoseconds,
            retry_after_disposition=command.retry_after_disposition,
            retry_exhausted=command.retry_exhausted,
            retry_blocked_reason=command.retry_blocked_reason,
        )
        if fault_id in self._current_audit_faults or fault_id in self._permanent_audit_faults:
            raise ProgrammingInvariantViolation(
                "duplicate audit fault lifecycle identity",
                component="canonical-processor",
            )
        self._assert_audit_fault_capacity_available()
        self._audit_faults_observed += 1
        endpoint_class = record.endpoint_class
        if endpoint_class is not None:
            self._audit_http_faults_observed += 1
            self._audit_http_fault_endpoint_observed[endpoint_class] += 1
        if record.retry_exhausted is True:
            self._audit_http_retry_exhaustions_observed += 1
            if endpoint_class is not None:
                self._audit_http_retry_endpoint_observed[endpoint_class] += 1
        if status is AuditFaultStatus.CURRENT:
            self._current_audit_faults[fault_id] = record
            self._latest_current_audit_fault_by_instrument[record.instrument_id] = record
            self._audit_fault_current_count_by_instrument[record.instrument_id] = (
                self._audit_fault_current_count_by_instrument.get(record.instrument_id, 0) + 1
            )
            if endpoint_class is not None:
                self._audit_http_fault_endpoint_current[endpoint_class] += 1
            if record.retry_exhausted is True and endpoint_class is not None:
                self._audit_http_retry_endpoint_current[endpoint_class] += 1
        else:
            self._permanent_audit_faults[fault_id] = record
            self._audit_faults_permanent += 1
            if endpoint_class is not None:
                self._audit_http_faults_permanent += 1
                self._audit_http_fault_endpoint_permanent[endpoint_class] += 1
            if record.retry_exhausted is True:
                self._audit_http_retry_exhaustions_permanent += 1
                if endpoint_class is not None:
                    self._audit_http_retry_endpoint_permanent[endpoint_class] += 1
        if record.blocks_current_scope:
            self._audit_fault_scope_block_count_by_instrument[record.instrument_id] = (
                self._audit_fault_scope_block_count_by_instrument.get(record.instrument_id, 0) + 1
            )
            self._mark_audit_ineligible(record.instrument_id, nonfault=False)
        self._audit_fault_lifecycle_records.append(record)
        self._latest_audit_fault_by_status[status] = record
        self._audit_fault_lifecycle_chain = _chain_extend(
            self._audit_fault_lifecycle_chain,
            "rest-audit-fault-observed",
            record,
        )

    def _resolve_current_audit_faults(
        self,
        *,
        instrument_id: InstrumentId,
        resolution: AuditFaultResolution,
        resolution_source: RawSourceRef,
        resolved_at: UtcTimestamp,
        resolved_monotonic_ns: int,
    ) -> None:
        candidates = tuple(
            record
            for _, record in sorted(self._current_audit_faults.items())
            if record.instrument_id == instrument_id
        )
        if len(candidates) != self._audit_fault_current_count_by_instrument.get(instrument_id, 0):
            raise ProgrammingInvariantViolation(
                "audit fault current instrument authority differs from active records",
                component="canonical-processor",
            )
        blocking_candidates = sum(record.blocks_current_scope for record in candidates)
        blocking_count = self._audit_fault_scope_block_count_by_instrument.get(instrument_id, 0)
        if blocking_candidates > blocking_count:
            raise ProgrammingInvariantViolation(
                "audit fault scope-block authority differs from active records",
                component="canonical-processor",
            )
        for record in candidates:
            resolved = replace(
                record,
                status=AuditFaultStatus.RESOLVED,
                blocks_current_scope=False,
                resolution=resolution,
                resolution_source=resolution_source,
                resolved_at=resolved_at,
                resolved_monotonic_ns=resolved_monotonic_ns,
            )
            self._current_audit_faults.pop(record.fault_id)
            self._audit_faults_resolved += 1
            endpoint_class = record.endpoint_class
            if endpoint_class is not None:
                self._audit_http_faults_resolved += 1
                self._audit_http_fault_endpoint_current[endpoint_class] -= 1
                self._audit_http_fault_endpoint_resolved[endpoint_class] += 1
            if record.retry_exhausted is True:
                self._audit_http_retry_exhaustions_resolved += 1
                if endpoint_class is not None:
                    self._audit_http_retry_endpoint_current[endpoint_class] -= 1
                    self._audit_http_retry_endpoint_resolved[endpoint_class] += 1
            self._audit_fault_lifecycle_records.append(resolved)
            self._latest_audit_fault_by_status[AuditFaultStatus.RESOLVED] = resolved
            self._audit_fault_lifecycle_chain = _chain_extend(
                self._audit_fault_lifecycle_chain,
                "rest-audit-fault-resolved",
                resolved,
            )
        remaining_blocking_count = blocking_count - blocking_candidates
        if remaining_blocking_count:
            self._audit_fault_scope_block_count_by_instrument[instrument_id] = (
                remaining_blocking_count
            )
        else:
            self._audit_fault_scope_block_count_by_instrument.pop(instrument_id, None)
        self._latest_current_audit_fault_by_instrument.pop(instrument_id, None)
        self._audit_fault_current_count_by_instrument.pop(instrument_id, None)

    def _audit_fault_blocks_instrument(self, instrument_id: InstrumentId) -> bool:
        return instrument_id in self._audit_fault_scope_block_count_by_instrument

    def _permanent_audit_fault_blocks_instrument(self, instrument_id: InstrumentId) -> bool:
        return any(
            record.instrument_id == instrument_id and record.blocks_current_scope
            for record in self._permanent_audit_faults.values()
        )

    def _audit_http_endpoint_snapshots(self) -> tuple[ProcessorAuditHttpEndpointSnapshot, ...]:
        return tuple(
            ProcessorAuditHttpEndpointSnapshot(
                endpoint_class=endpoint_class,
                observed=self._audit_http_fault_endpoint_observed[endpoint_class],
                current=self._audit_http_fault_endpoint_current[endpoint_class],
                resolved=self._audit_http_fault_endpoint_resolved[endpoint_class],
                permanent=self._audit_http_fault_endpoint_permanent[endpoint_class],
                retry_exhaustions_observed=(
                    self._audit_http_retry_endpoint_observed[endpoint_class]
                ),
                retry_exhaustions_current=(self._audit_http_retry_endpoint_current[endpoint_class]),
                retry_exhaustions_resolved=(
                    self._audit_http_retry_endpoint_resolved[endpoint_class]
                ),
                retry_exhaustions_permanent=(
                    self._audit_http_retry_endpoint_permanent[endpoint_class]
                ),
            )
            for endpoint_class in _HTTP_ENDPOINT_CLASSES
        )

    @staticmethod
    def _audit_rest_identity(rest: NormalizedRestBook) -> AuditRestBookIdentity:
        return AuditRestBookIdentity.create(
            instrument_id=rest.instrument_id,
            market_id=rest.market_id,
            depth=audit_exact_depth_identity(rest.bids, rest.asks),
            tick_size=rest.tick_size,
            minimum_order_size=rest.minimum_order_size,
            neg_risk=rest.neg_risk,
        )

    def _open_audit_divergence(
        self,
        *,
        outcome: AuditOutcomeEvidence,
        market_id: MarketId,
        start_source: RawSourceRef,
        classification_control: RawEventEnvelope | None,
    ) -> AuditDivergencePending:
        instrument_id = outcome.starting_book.instrument_id
        if instrument_id in self._pending_audit_divergences:
            raise ProgrammingInvariantViolation(
                "a second audit divergence opened before authoritative recovery",
                component="canonical-processor",
            )
        diagnostic = outcome.mismatch_diagnostic
        if diagnostic is None:
            raise ProgrammingInvariantViolation(
                "audit mismatch divergence lacks its exact diagnostic",
                component="canonical-processor",
            )
        classification_source = (
            diagnostic.response_source
            if classification_control is None
            else RawSourceRef(
                ingest_id=classification_control.ingest_id,
                payload_sha256=classification_control.payload_sha256,
            )
        )
        classification_wall_time = (
            outcome.response_received_wall_time
            if classification_control is None
            else classification_control.received_wall_time
        )
        classification_monotonic_ns = (
            outcome.response_received_monotonic_ns
            if classification_control is None
            else classification_control.received_monotonic.nanoseconds
        )
        if classification_wall_time is None or classification_monotonic_ns is None:
            raise ProgrammingInvariantViolation(
                "audit mismatch lacks classification chronology",
                component="canonical-processor",
            )
        pending = AuditDivergencePending.create(
            market_id=market_id,
            audit_outcome=outcome,
            audit_start_control_source=start_source,
            classification_source=classification_source,
            classification_control_source=(
                None if classification_control is None else classification_source
            ),
            classification_received_wall_time=classification_wall_time,
            classification_received_monotonic_ns=classification_monotonic_ns,
        )
        if pending.audit_outcome.audit_start_schema == AUDIT_START_V2_SCHEMA:
            binding = AuditAvailabilityBinding(
                request_id=pending.request_id,
                divergence_id=pending.divergence_id,
                instrument_id=pending.instrument_id.value,
                market_id=pending.market_id.value,
                audit_start_source=pending.audit_start_control_source,
                audit_response_source=diagnostic.response_source,
                audit_classification_source=pending.classification_source,
            )
            if binding.divergence_id in self._audit_availability_bindings:
                raise ProgrammingInvariantViolation(
                    "audit availability authority reopened one divergence",
                    component="canonical-processor",
                )
            self._audit_availability_bindings[binding.divergence_id] = binding
            self._audit_availability_opened += 1
            self._audit_availability_open_chain = audit_availability_chain_extend(
                self._audit_availability_open_chain,
                "open",
                binding,
            )
        elif pending.audit_outcome.audit_start_schema == AUDIT_START_V1_SCHEMA:
            # Immutable v1 starts predate the durable replacement-release
            # source. Preserve them, but never synthesize exact-v2 authority.
            self._audit_availability_legacy_unverifiable += 1
        else:  # pragma: no cover - AuditOutcomeEvidence rejects unknown schemas
            raise ProgrammingInvariantViolation(
                "audit availability authority encountered an unknown start schema",
                component="canonical-processor",
            )
        self._pending_audit_divergences[instrument_id] = pending
        self._audit_divergences_observed += 1
        self._audit_divergence_history_chain = _chain_extend(
            self._audit_divergence_history_chain,
            "rest-audit-divergence-opened",
            pending,
        )
        active = self._active_audit_divergences_opened
        if active is not None:
            active.append(pending)
        self._record_recovery_request(
            reason=ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY,
            affected_instrument_ids=(instrument_id,),
            original_reason_code=diagnostic.cause.value,
            effective_invalidation_reason=InvalidationReason.REST_AUDIT_MISMATCH,
            audit_request_id=pending.request_id,
            audit_divergence_id=pending.divergence_id,
        )
        return pending

    def _resolve_audit_divergence_by_rest_snapshot(
        self,
        *,
        envelope: RawEventEnvelope,
        rest: NormalizedRestBook,
        owner: CanonicalL2BookOwner,
    ) -> None:
        pending = self._pending_audit_divergences.get(rest.instrument_id)
        if pending is None:
            return
        snapshot = owner.snapshot()
        exact_difference = compare_audit_exact_depth(
            local_bids=snapshot.bids,
            local_asks=snapshot.asks,
            rest_bids=rest.bids,
            rest_asks=rest.asks,
        )
        if snapshot.health is not BookHealth.HEALTHY or exact_difference.difference_count:
            raise ProgrammingInvariantViolation(
                "authoritative REST recovery did not become the exact healthy local book",
                component="canonical-processor",
            )
        authoritative_source = RawSourceRef(
            ingest_id=envelope.ingest_id,
            payload_sha256=envelope.payload_sha256,
        )
        if authoritative_source not in rest.provenance.raw_source_refs:
            raise ProgrammingInvariantViolation(
                "authoritative REST recovery lacks its raw response identity",
                component="canonical-processor",
            )
        diagnostic = pending.audit_outcome.mismatch_diagnostic
        if diagnostic is None:
            raise ProgrammingInvariantViolation(
                "pending audit divergence lost its mismatch diagnostic",
                component="canonical-processor",
            )
        mismatch_source = diagnostic.response_source
        if authoritative_source.ingest_id == mismatch_source.ingest_id:
            raise ProgrammingInvariantViolation(
                "audit divergence requires a separately journaled REST resnapshot",
                component="canonical-processor",
            )
        response_monotonic_ns = pending.audit_outcome.response_received_monotonic_ns
        if response_monotonic_ns is None:
            raise ProgrammingInvariantViolation(
                "audit divergence lost its response chronology",
                component="canonical-processor",
            )
        if envelope.received_monotonic.nanoseconds < max(
            response_monotonic_ns,
            pending.classification_received_monotonic_ns,
        ):
            raise ProgrammingInvariantViolation(
                "authoritative REST recovery precedes mismatch classification",
                component="canonical-processor",
            )
        resolution = AuditDivergenceResolution(
            divergence_id=pending.divergence_id,
            request_id=pending.request_id,
            instrument_id=rest.instrument_id,
            market_id=rest.market_id,
            audit_start_schema=pending.audit_outcome.audit_start_schema,
            trigger_reason=pending.audit_outcome.trigger_reason,
            mismatch_response_source=mismatch_source,
            authoritative_snapshot_source=authoritative_source,
            authoritative_rest_book=self._audit_rest_identity(rest),
            recovered_book=AuditBookIdentity(
                instrument_id=owner.instrument_id,
                generation=self._audit_economic_generations.get(owner.instrument_id, 0),
                economic_content_sha256=owner.audit_comparison_sha256(),
            ),
            observed_divergence=pending,
            resolved_at=envelope.received_wall_time,
            resolved_monotonic_ns=envelope.received_monotonic.nanoseconds,
        )
        resolved_availability_binding: AuditAvailabilityBinding | None = None
        if pending.audit_outcome.audit_start_schema == AUDIT_START_V2_SCHEMA:
            binding = self._audit_availability_bindings.get(pending.divergence_id)
            if binding is None or binding.authoritative_resolution_source is not None:
                raise ProgrammingInvariantViolation(
                    "audit availability resolution lacks one current authority binding",
                    component="canonical-processor",
                )
            resolved_binding = binding.with_authoritative_resolution(authoritative_source)
            resolved_availability_binding = resolved_binding
            self._audit_availability_bindings[pending.divergence_id] = resolved_binding
            self._audit_availability_resolved += 1
            self._audit_availability_resolution_chain = audit_availability_chain_extend(
                self._audit_availability_resolution_chain,
                "resolution",
                resolved_binding,
            )
        self._pending_audit_divergences.pop(rest.instrument_id)
        self._audit_divergences_resolved_by_authoritative_resnapshot += 1
        self._audit_divergence_resolutions.append(resolution)
        self._latest_audit_divergence_resolution = resolution
        active_resolutions = self._active_audit_divergences_resolved
        if active_resolutions is not None:
            active_resolutions.append(resolution)
        self._audit_divergence_history_chain = _chain_extend(
            self._audit_divergence_history_chain,
            "rest-audit-divergence-resolved",
            resolution,
        )
        required_instrument_ids = tuple(
            sorted(
                self._market_instruments.get(rest.market_id, ()),
                key=lambda item: item.value,
            )
        )
        if (
            rest.instrument_id not in required_instrument_ids
            or rest.instrument_id in self._audit_replacement_captures
        ):
            raise ProgrammingInvariantViolation(
                "audit recovery cannot establish bounded replacement-capture scope",
                component="canonical-processor",
            )
        pre_open_connection_ids = frozenset(self._seen_websocket_connections)
        ordered_pre_open_connection_ids = tuple(
            item.value
            for item in sorted(
                pre_open_connection_ids,
                key=lambda item: item.value,
            )
        )
        self._audit_replacement_captures[rest.instrument_id] = _AuditReplacementCapture(
            divergence_id=pending.divergence_id,
            market_id=rest.market_id,
            original_connection_id=pending.audit_outcome.connection_id,
            required_instrument_ids=required_instrument_ids,
            audit_start_schema=pending.audit_outcome.audit_start_schema,
            resolution_source=authoritative_source,
            resolution_ingest_seq=envelope.ingest_seq,
            resolution_monotonic=envelope.received_monotonic,
            pre_open_connection_ids=pre_open_connection_ids,
            pre_open_connection_count=len(ordered_pre_open_connection_ids),
            pre_open_connection_ids_sha256=canonical_sha256(
                {
                    "connection_ids": ordered_pre_open_connection_ids,
                    "schema": "mma-audit-replacement-pre-open-connections-v1",
                }
            ),
            mismatch_cause=(
                AuditMismatchCause.UNKNOWN
                if pending.audit_outcome.mismatch_diagnostic is None
                else pending.audit_outcome.mismatch_diagnostic.cause
            ),
            availability_binding=resolved_availability_binding,
        )

    def _failed_audit_outcome(
        self,
        start: AuditStartEvidence,
        *,
        reason: AuditFailureReason,
        start_source: RawSourceRef,
        classification_source: RawSourceRef,
        response: RawEventEnvelope | None = None,
        response_boundary_ordinal: int | None = None,
        mark_audit_ineligible: bool = True,
        mark_market_dirty: bool = True,
    ) -> AuditOutcomeEvidence:
        owner = self._owners[start.starting_book.instrument_id]
        barrier = self.audit_book_identity(start.native_asset_id)
        processed = self.processed_websocket_watermark(start.connection_id).ordinal
        if mark_audit_ineligible:
            self._mark_audit_ineligible(owner.instrument_id, nonfault=False)
        if mark_market_dirty:
            self._mark_market_dirty(owner.market_id, ScanTriggerReason.REST_AUDIT)
        return AuditOutcomeEvidence(
            request_id=start.request_id,
            connection_id=start.connection_id,
            native_asset_id=start.native_asset_id,
            attempt=start.attempt,
            request_started_monotonic_ns=start.request_started_monotonic_ns,
            starting_admission_ordinal=start.starting_admission_watermark.ordinal,
            starting_processed_ordinal=start.starting_processed_watermark.ordinal,
            starting_book=start.starting_book,
            outcome=AuditOutcome.FAILED,
            reason=reason,
            response_ingest_id=None if response is None else response.ingest_id,
            raw_response_sha256=None if response is None else response.payload_sha256,
            response_received_wall_time=(None if response is None else response.received_wall_time),
            response_received_monotonic_ns=(
                None if response is None else response.received_monotonic.nanoseconds
            ),
            response_boundary_ordinal=response_boundary_ordinal,
            barrier_processed_ordinal=None if response is None else processed,
            barrier_book=None if response is None else barrier,
            requires_resync=False,
            audit_start_schema=start.schema,
            trigger_reason=start.trigger_reason,
            audit_start_source=start_source,
            classification_source=classification_source,
        )

    def _fail_rest_audit_from_control(self, envelope: RawEventEnvelope) -> None:
        command = self._parse_audit_failure(envelope.payload_bytes)
        request_id = command.request_id
        reason = command.reason
        if (
            request_id in self._prepare_attest_rest_audit_requests
            and command.schema != AUDIT_FAILURE_V2_SCHEMA
        ):
            raise NormalizationError(
                "prepare/attest audit failure requires current-schema authority"
            )
        self._assert_audit_fault_capacity_available()
        start = self._pending_rest_audits.pop(request_id, None)
        if start is None:
            raise NormalizationError("audit failure references no pending request")
        start_source = self._pending_rest_audit_start_sources.pop(request_id, None)
        if start_source is None:
            raise ProgrammingInvariantViolation(
                "pending REST audit lacks its start-control source",
                component="canonical-processor",
            )
        if envelope.connection_id != start.connection_id:
            reason = AuditFailureReason.SOCKET_GENERATION_CHANGED
        self._pending_rest_audit_responses.pop(request_id, None)
        self._pending_rest_audit_classification_preparations.pop(request_id, None)
        self._prepare_attest_rest_audit_requests.discard(request_id)
        self._pending_rest_audit_hard_expiries.pop(request_id, None)
        self._pending_rest_audit_response_deadlines.pop(request_id, None)
        self._record_audit_failure_outcome(
            self._failed_audit_outcome(
                start,
                reason=reason,
                start_source=start_source,
                classification_source=self._active_raw_source(),
            ),
            command=command,
            original_reason=command.reason,
        )

    def _advance_websocket_watermark(self, envelope: RawEventEnvelope) -> None:
        if (
            envelope.connection_id not in self._seen_websocket_connections
            and len(self._seen_websocket_connections) >= _MAX_TRACKED_WEBSOCKET_CONNECTIONS
        ):
            raise NormalizationError("WebSocket connection history exceeds its deterministic bound")
        self._seen_websocket_connections.add(envelope.connection_id)
        metadata = envelope.transport_metadata
        raw_ordinal = metadata.get("websocket_admission_ordinal")
        if raw_ordinal is None:
            if any(
                start.connection_id == envelope.connection_id
                for start in self._pending_rest_audits.values()
            ):
                raise NormalizationError(
                    "pending audit encountered WebSocket input without admission ordinal"
                )
            return
        if isinstance(raw_ordinal, bool) or not isinstance(raw_ordinal, int):
            raise NormalizationError("WebSocket admission ordinal must be an integer")
        expected = self._processed_ws_watermarks.get(envelope.connection_id, 0) + 1
        if raw_ordinal != expected:
            raise NormalizationError("WebSocket admission ordinals are not contiguous")
        self._processed_ws_watermarks[envelope.connection_id] = raw_ordinal

    def _audit_response_metadata(self, envelope: RawEventEnvelope) -> tuple[str, int, int, int]:
        metadata = envelope.transport_metadata
        request_id = metadata.get("request_id")
        boundary = metadata.get("response_boundary_websocket_ordinal")
        barrier = metadata.get("barrier_websocket_ordinal")
        attempt = metadata.get("audit_attempt")
        if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
            raise NormalizationError("REST audit response request ID is invalid")
        for name, value in (
            ("response boundary", boundary),
            ("barrier", barrier),
            ("attempt", attempt),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise NormalizationError(f"REST audit {name} metadata is invalid")
        if cast(int, attempt) <= 0:
            raise NormalizationError("REST audit attempt metadata must be positive")
        return request_id, cast(int, boundary), cast(int, barrier), cast(int, attempt)

    def _stage_rest_audit_response(self, envelope: RawEventEnvelope) -> None:
        """Hold durable raw bytes until a replayable classification decision arrives."""

        if len(envelope.payload_bytes) > self._config.polymarket.maximum_http_response_bytes:
            raise NormalizationError("deferred REST audit response exceeds its byte bound")
        request_id, _, _, _ = self._audit_response_metadata(envelope)
        if request_id not in self._pending_rest_audits:
            raise NormalizationError("REST audit response references no pending request")
        if request_id not in self._pending_rest_audit_hard_expiries:
            raise NormalizationError("deferred REST audit response requires a bound start")
        response_deadline_ns = self._pending_rest_audit_response_deadlines.get(request_id)
        if response_deadline_ns is None:
            raise NormalizationError("deferred REST audit response lacks a response deadline")
        if (
            self._rest_audit_run_deadline_epoch_id is None
            or envelope.received_monotonic.epoch_id != self._rest_audit_run_deadline_epoch_id
        ):
            raise NormalizationError("deferred REST audit response changes monotonic epoch")
        if envelope.received_monotonic.nanoseconds >= response_deadline_ns:
            raise NormalizationError("REST audit response reached or exceeded its bound deadline")
        if request_id in self._pending_rest_audit_responses:
            raise NormalizationError("REST audit response is already staged")
        metadata = envelope.transport_metadata
        if any(
            key in metadata
            for key in (
                "classification_monotonic_ns",
                "coordinator_failure_reason",
                "hard_expiry_monotonic_ns",
            )
        ):
            raise NormalizationError("deferred REST audit response embeds a premature decision")
        self._pending_rest_audit_responses[request_id] = envelope

    def _parse_audit_classification_prepare(
        self,
        envelope: RawEventEnvelope,
    ) -> AuditClassificationPrepareCommand:
        control = self._control_object(
            envelope.payload_bytes,
            schema=AUDIT_CLASSIFICATION_PREPARE_SCHEMA,
            expected_keys=frozenset(
                {
                    "hard_expiry_monotonic_ns",
                    "request_id",
                    "response_ingest_seq",
                    "schema",
                }
            ),
        )
        try:
            command = AuditClassificationPrepareCommand(
                request_id=self._control_text(control, "request_id", maximum=128),
                response_ingest_seq=self._control_integer(
                    control,
                    "response_ingest_seq",
                ),
                hard_expiry_monotonic_ns=self._control_integer(
                    control,
                    "hard_expiry_monotonic_ns",
                ),
            )
        except ValueError as error:
            raise NormalizationError("audit classification preparation is invalid") from error
        if envelope.payload_bytes != command.to_payload():
            raise NormalizationError("audit classification preparation must use canonical encoding")
        return command

    def _prepare_staged_rest_audit_classification(
        self,
        envelope: RawEventEnvelope,
    ) -> None:
        """Stage an inert first phase; never compare, mutate, restore, or scan."""

        command = self._parse_audit_classification_prepare(envelope)
        if command.request_id not in self._prepare_attest_rest_audit_requests:
            raise NormalizationError("audit start did not authorize prepare/attest classification")
        response = self._pending_rest_audit_responses.get(command.request_id)
        if response is None:
            raise NormalizationError("audit classification preparation has no staged response")
        if response.ingest_seq != command.response_ingest_seq:
            raise NormalizationError("audit classification preparation references another response")
        if response.connection_id != envelope.connection_id:
            raise NormalizationError(
                "audit classification preparation belongs to another socket generation"
            )
        if (
            self._rest_audit_run_deadline_epoch_id is None
            or response.received_monotonic.epoch_id != self._rest_audit_run_deadline_epoch_id
            or envelope.received_monotonic.epoch_id != response.received_monotonic.epoch_id
        ):
            raise NormalizationError("audit classification preparation changes monotonic epoch")
        if envelope.received_monotonic.nanoseconds < response.received_monotonic.nanoseconds:
            raise NormalizationError("audit classification preparation precedes its response")
        hard_expiry_ns = self._pending_rest_audit_hard_expiries.get(command.request_id)
        if hard_expiry_ns is None or command.hard_expiry_monotonic_ns != hard_expiry_ns:
            raise NormalizationError("audit classification preparation changes hard expiry")
        if envelope.transport_metadata != {"request_id": command.request_id}:
            raise NormalizationError("audit classification preparation metadata is not exact")
        if command.request_id in self._pending_rest_audit_classification_preparations:
            raise NormalizationError("audit classification is already prepared")
        start = self._pending_rest_audits.get(command.request_id)
        if start is None:
            raise NormalizationError("audit classification preparation has no pending start")
        instrument_id = start.starting_book.instrument_id
        owner = self._owners[instrument_id]
        self._pending_rest_audit_classification_preparations[command.request_id] = (
            _PreparedAuditClassification(
                command=command,
                envelope=envelope,
                processed_watermark=self.processed_websocket_watermark(start.connection_id),
                book=self.audit_book_identity(start.native_asset_id),
                owner_health=owner.health,
                reported_top_transition=self._reported_top_transitions.get(instrument_id),
                audit_ineligible_nonfault=(
                    instrument_id in self._audit_ineligible_nonfault_instruments
                ),
                audit_fault_current_count=self._audit_fault_current_count_by_instrument.get(
                    instrument_id, 0
                ),
                audit_fault_scope_block_count=(
                    self._audit_fault_scope_block_count_by_instrument.get(instrument_id, 0)
                ),
            )
        )

    def _parse_audit_classification(self, envelope: RawEventEnvelope) -> AuditClassificationCommand:
        decoded = decode_json_exact(envelope.payload_bytes)
        if not isinstance(decoded, dict):
            raise NormalizationError("audit classification payload must be an object")
        raw_schema = cast(dict[str, object], decoded).get("schema")
        if raw_schema not in {
            AUDIT_CLASSIFICATION_V1_SCHEMA,
            AUDIT_CLASSIFICATION_V2_SCHEMA,
        }:
            raise NormalizationError("audit classification control schema is unsupported")
        control = self._control_object(
            envelope.payload_bytes,
            schema=cast(str, raw_schema),
            expected_keys=frozenset(
                {
                    "classification_monotonic_ns",
                    "hard_expiry_monotonic_ns",
                    "request_id",
                    "response_ingest_seq",
                    "schema",
                }
            ),
        )
        try:
            command = AuditClassificationCommand(
                request_id=self._control_text(control, "request_id", maximum=128),
                response_ingest_seq=self._control_integer(
                    control,
                    "response_ingest_seq",
                ),
                classification_monotonic_ns=self._control_integer(
                    control,
                    "classification_monotonic_ns",
                ),
                hard_expiry_monotonic_ns=self._control_integer(
                    control,
                    "hard_expiry_monotonic_ns",
                ),
                schema=cast(str, raw_schema),
            )
        except ValueError as error:
            raise NormalizationError("audit classification decision is invalid") from error
        if envelope.payload_bytes != command.to_payload():
            raise NormalizationError("audit classification decision must use canonical encoding")
        if command.classification_monotonic_ns != envelope.received_monotonic.nanoseconds:
            raise NormalizationError("audit classification timestamp differs from raw evidence")
        return command

    def _classify_staged_rest_audit_response(self, envelope: RawEventEnvelope) -> None:
        command = self._parse_audit_classification(envelope)
        response = self._pending_rest_audit_responses.get(command.request_id)
        if response is None:
            raise NormalizationError("audit classification references no staged response")
        if response.ingest_seq != command.response_ingest_seq:
            raise NormalizationError("audit classification references another raw response")
        if response.connection_id != envelope.connection_id:
            raise NormalizationError("audit classification belongs to another socket generation")
        if (
            self._rest_audit_run_deadline_epoch_id is None
            or response.received_monotonic.epoch_id != self._rest_audit_run_deadline_epoch_id
            or envelope.received_monotonic.epoch_id != response.received_monotonic.epoch_id
        ):
            raise NormalizationError("audit classification changes monotonic epoch")
        if command.classification_monotonic_ns < response.received_monotonic.nanoseconds:
            raise NormalizationError("audit classification precedes its response receipt")
        hard_expiry_ns = self._pending_rest_audit_hard_expiries.get(command.request_id)
        if hard_expiry_ns is None:
            raise NormalizationError("deferred audit start lacks a bound hard expiry")
        if command.hard_expiry_monotonic_ns != hard_expiry_ns:
            raise NormalizationError("audit classification changes its staged hard expiry")
        prepared = self._pending_rest_audit_classification_preparations.get(command.request_id)
        requires_prepare = command.request_id in self._prepare_attest_rest_audit_requests
        if requires_prepare and prepared is None:
            raise NormalizationError("prepare/attest audit classification lacks preparation")
        if prepared is not None and not requires_prepare:
            raise NormalizationError("legacy audit classification cannot consume a preparation")
        if requires_prepare and command.schema != AUDIT_CLASSIFICATION_V2_SCHEMA:
            raise NormalizationError(
                "prepare/attest audit classification requires current-schema authority"
            )
        deadline_failure_dominates = False
        attestation_binding: dict[str, object] | None = None
        if prepared is not None:
            metadata = envelope.transport_metadata
            if set(metadata) != {
                "classification_prepare_durable_monotonic_ns",
                "classification_prepare_ingest_id",
                "classification_prepare_payload_sha256",
                "classification_prepare_processed_monotonic_ns",
                "request_id",
            }:
                raise NormalizationError("audit classification attestation metadata is not exact")
            prepare_ingest_id = metadata["classification_prepare_ingest_id"]
            prepare_payload_sha256 = metadata["classification_prepare_payload_sha256"]
            durable_ns = metadata["classification_prepare_durable_monotonic_ns"]
            processed_ns = metadata["classification_prepare_processed_monotonic_ns"]
            if (
                metadata["request_id"] != command.request_id
                or prepare_ingest_id != prepared.envelope.ingest_id.value
                or prepare_payload_sha256 != prepared.envelope.payload_sha256
                or not isinstance(prepare_payload_sha256, str)
                or not is_sha256_hex(prepare_payload_sha256)
            ):
                raise NormalizationError("audit classification attestation source is invalid")
            if (
                isinstance(durable_ns, bool)
                or not isinstance(durable_ns, int)
                or durable_ns < 0
                or isinstance(processed_ns, bool)
                or not isinstance(processed_ns, int)
                or processed_ns < 0
                or processed_ns != command.classification_monotonic_ns
            ):
                raise NormalizationError("audit classification attestation timing is invalid")
            if prepared.command != AuditClassificationPrepareCommand(
                request_id=command.request_id,
                response_ingest_seq=command.response_ingest_seq,
                hard_expiry_monotonic_ns=command.hard_expiry_monotonic_ns,
            ):
                raise NormalizationError("audit classification attestation changes preparation")
            start = self._pending_rest_audits.get(command.request_id)
            if start is None:
                raise NormalizationError("audit classification attestation has no pending start")
            timing_reconciled = (
                response.received_monotonic.nanoseconds
                <= prepared.envelope.received_monotonic.nanoseconds
                <= durable_ns
                <= processed_ns
                and durable_ns < hard_expiry_ns
                and processed_ns < hard_expiry_ns
            )
            final_state_reconciled = self._prepared_audit_final_state_reconciles(
                start=start,
                prepared=prepared,
            )
            if not timing_reconciled or not final_state_reconciled:
                coordinator_failure = AuditFailureReason.BOOK_FRESHNESS_EXPIRED
                deadline_failure_dominates = True
            else:
                coordinator_failure = None
            attestation_binding = {
                "classification_protocol": AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
                "classification_source": RawSourceRef(
                    ingest_id=envelope.ingest_id,
                    payload_sha256=envelope.payload_sha256,
                ),
                "final_state_reconciled": final_state_reconciled,
                "prepared_state": {
                    "audit_fault_current_count": prepared.audit_fault_current_count,
                    "audit_fault_scope_block_count": (prepared.audit_fault_scope_block_count),
                    "audit_ineligible_nonfault": prepared.audit_ineligible_nonfault,
                    "book": prepared.book,
                    "owner_health": prepared.owner_health,
                    "processed_watermark": prepared.processed_watermark,
                    "reported_top_transition": prepared.reported_top_transition,
                },
                "preparation_command": prepared.command,
                "preparation_durable_monotonic_ns": durable_ns,
                "preparation_processed_monotonic_ns": processed_ns,
                "preparation_received_monotonic": prepared.envelope.received_monotonic,
                "preparation_source": RawSourceRef(
                    ingest_id=prepared.envelope.ingest_id,
                    payload_sha256=prepared.envelope.payload_sha256,
                ),
                "request_id": command.request_id,
                "schema": "mma-rest-audit-classification-attestation-binding-v1",
                "timing_reconciled": timing_reconciled,
            }
        else:
            coordinator_failure = (
                AuditFailureReason.BOOK_FRESHNESS_EXPIRED
                if command.classification_monotonic_ns >= command.hard_expiry_monotonic_ns
                else None
            )
        self._active_scan_observed_at = response.received_wall_time
        self._complete_rest_audit_response(
            response,
            coordinator_failure=coordinator_failure,
            coordinator_decision_supplied=True,
            classification_control=envelope,
            deadline_failure_dominates=deadline_failure_dominates,
        )
        if attestation_binding is not None:
            self._audit_classification_attestation_chain = _chain_extend(
                self._audit_classification_attestation_chain,
                "rest-audit-classification-attestation",
                attestation_binding,
            )

    def _prepared_audit_final_state_reconciles(
        self,
        *,
        start: AuditStartEvidence,
        prepared: _PreparedAuditClassification,
    ) -> bool:
        """Check prepare-to-attestation stability without producing any side effect."""

        instrument_id = start.starting_book.instrument_id
        owner = self._owners[start.starting_book.instrument_id]
        return (
            self.processed_websocket_watermark(start.connection_id) == prepared.processed_watermark
            and self.audit_book_identity(start.native_asset_id) == prepared.book
            and owner.health is prepared.owner_health
            and self._reported_top_transitions.get(instrument_id)
            == prepared.reported_top_transition
            and (instrument_id in self._audit_ineligible_nonfault_instruments)
            is prepared.audit_ineligible_nonfault
            and self._audit_fault_current_count_by_instrument.get(instrument_id, 0)
            == prepared.audit_fault_current_count
            and self._audit_fault_scope_block_count_by_instrument.get(instrument_id, 0)
            == prepared.audit_fault_scope_block_count
        )

    def _audit_response_coordinator_failure(
        self,
        envelope: RawEventEnvelope,
    ) -> AuditFailureReason | None:
        metadata = envelope.transport_metadata
        raw_classification = metadata.get("classification_monotonic_ns")
        raw_expiry = metadata.get("hard_expiry_monotonic_ns")
        raw_reason = metadata.get("coordinator_failure_reason")
        if raw_classification is None and raw_expiry is None and raw_reason is None:
            return None
        for name, value in (
            ("classification monotonic", raw_classification),
            ("hard expiry monotonic", raw_expiry),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise NormalizationError(f"REST audit {name} metadata is invalid")
        classification_ns = cast(int, raw_classification)
        expiry_ns = cast(int, raw_expiry)
        if raw_reason is None:
            if classification_ns >= expiry_ns:
                raise NormalizationError("expired REST audit lacks a forced failure decision")
            return None
        if raw_reason != AuditFailureReason.BOOK_FRESHNESS_EXPIRED.value:
            raise NormalizationError("REST audit coordinator failure reason is unsupported")
        if classification_ns < expiry_ns:
            raise NormalizationError("REST audit expiry decision precedes the hard expiry")
        return AuditFailureReason.BOOK_FRESHNESS_EXPIRED

    def _complete_rest_audit_response(
        self,
        envelope: RawEventEnvelope,
        *,
        coordinator_failure: AuditFailureReason | None = None,
        coordinator_decision_supplied: bool = False,
        classification_control: RawEventEnvelope | None = None,
        deadline_failure_dominates: bool = False,
    ) -> None:
        request_id, boundary, barrier_ordinal, attempt = self._audit_response_metadata(envelope)
        if (
            not coordinator_decision_supplied
            and request_id in self._pending_rest_audit_hard_expiries
        ):
            raise NormalizationError(
                "bound REST audit start requires deferred response classification"
            )
        # Any response can resolve to FAILED after payload/fence validation.
        # Reserve bounded lifecycle capacity before consuming staged response,
        # start, or PREPARE state so a rejected current-protocol transaction is
        # wholly inert and can be retried after capacity becomes available.
        self._assert_audit_fault_capacity_available()
        start = self._pending_rest_audits.pop(request_id, None)
        if start is None:
            raise NormalizationError("REST audit response references no pending request")
        start_source = self._pending_rest_audit_start_sources.pop(request_id, None)
        if start_source is None:
            raise ProgrammingInvariantViolation(
                "pending REST audit lacks its start-control raw identity",
                component="canonical-processor",
            )
        classification_source = RawSourceRef(
            ingest_id=(
                envelope.ingest_id
                if classification_control is None
                else classification_control.ingest_id
            ),
            payload_sha256=(
                envelope.payload_sha256
                if classification_control is None
                else classification_control.payload_sha256
            ),
        )
        self._pending_rest_audit_hard_expiries.pop(request_id, None)
        self._pending_rest_audit_response_deadlines.pop(request_id, None)
        self._pending_rest_audit_responses.pop(request_id, None)
        self._pending_rest_audit_classification_preparations.pop(request_id, None)
        self._prepare_attest_rest_audit_requests.discard(request_id)
        if deadline_failure_dominates:
            if coordinator_failure is not AuditFailureReason.BOOK_FRESHNESS_EXPIRED:
                raise ProgrammingInvariantViolation(
                    "audit deadline dominance lacks the exact expiry failure",
                    component="canonical-processor",
                )
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.BOOK_FRESHNESS_EXPIRED,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                    mark_market_dirty=False,
                )
            )
            return
        if envelope.connection_id != start.connection_id or attempt != start.attempt:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        if envelope.received_monotonic.nanoseconds < start.request_started_monotonic_ns:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.RESPONSE_INVALID,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        processed = self.processed_websocket_watermark(start.connection_id).ordinal
        if (
            boundary != barrier_ordinal
            or processed != boundary
            or boundary < start.starting_admission_watermark.ordinal
        ):
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.WATERMARK_AMBIGUOUS,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        if not coordinator_decision_supplied:
            coordinator_failure = self._audit_response_coordinator_failure(envelope)
        if coordinator_failure is not None:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=coordinator_failure,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        barrier_book = self.audit_book_identity(start.native_asset_id)
        try:
            native = parse_rest_book(envelope.payload_bytes)
            if native.asset_id != start.native_asset_id:
                raise NormalizationError("REST audit response identifies another asset")
            market_id = self._market_id_for_condition(native.condition_id)
            bundle = self._registry.as_known_at(market_id, envelope.received_wall_time)
            if bundle is None:
                raise NormalizationError(
                    "REST book audit arrived before matching as-known reference data"
                )
            context = self._normalization_context(envelope, bundle)
            rest = self._normalizer.normalize_rest_book(native, context)
        except BookStateError, NormalizationError, PolymarketSchemaError:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.RESPONSE_INVALID,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return

        if barrier_book != start.starting_book:
            # A raced request is not affirmative integrity evidence.  Make the
            # leg explicitly audit-ineligible until a later fenced MATCH (or a
            # full REST resnapshot) restores it; this also keeps expiry misses
            # fail closed while acquisition continues on the same generation.
            self._mark_audit_ineligible(
                start.starting_book.instrument_id,
                nonfault=True,
            )
            evidence = AuditOutcomeEvidence(
                request_id=start.request_id,
                connection_id=start.connection_id,
                native_asset_id=start.native_asset_id,
                attempt=start.attempt,
                request_started_monotonic_ns=start.request_started_monotonic_ns,
                starting_admission_ordinal=start.starting_admission_watermark.ordinal,
                starting_processed_ordinal=start.starting_processed_watermark.ordinal,
                starting_book=start.starting_book,
                outcome=AuditOutcome.RACED,
                reason=None,
                response_ingest_id=envelope.ingest_id,
                raw_response_sha256=envelope.payload_sha256,
                response_received_wall_time=envelope.received_wall_time,
                response_received_monotonic_ns=envelope.received_monotonic.nanoseconds,
                response_boundary_ordinal=boundary,
                barrier_processed_ordinal=processed,
                barrier_book=barrier_book,
                requires_resync=False,
                audit_start_schema=start.schema,
                trigger_reason=start.trigger_reason,
                audit_start_source=start_source,
                classification_source=classification_source,
            )
            self._record_audit_outcome(evidence)
            return

        owner = self._owners[start.starting_book.instrument_id]
        if owner.instrument_id in self._reported_top_transitions:
            # A quantity-free BBO that contradicts local depth proves that the
            # local L2 image is unresolved.  A REST body equal to that stale
            # image does not prove venue-side causal freshness, so it must not
            # become MATCH evidence or refresh/clear the pending signal.
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                    # The pending BBO already owns current ineligibility.  Keep
                    # it separate so its later ordered reconciliation cannot
                    # erase an unrelated durable audit-failure cause.
                    mark_audit_ineligible=False,
                )
            )
            return
        local_snapshot = owner.snapshot()
        previous_health = owner.health
        try:
            result = owner.audit_rest_snapshot(rest, checked_at=envelope.received_wall_time)
        except RawSourceInvariantError as error:
            raise ProgrammingInvariantViolation(
                "REST audit provenance contains conflicting raw source pairs",
                component="canonical-processor",
            ) from error
        except BookStateError:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        self._record_book_mutation(
            result.mutation,
            reason=ScanTriggerReason.REST_AUDIT,
            freshness_received_monotonic=envelope.received_monotonic,
        )
        if result.status is RestAuditStatus.NOT_COMPARABLE:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE,
                    start_source=start_source,
                    classification_source=classification_source,
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        outcome = (
            AuditOutcome.MATCH if result.status is RestAuditStatus.MATCH else AuditOutcome.MISMATCH
        )
        mismatch_diagnostic: AuditMismatchDiagnostic | None = None
        if outcome is AuditOutcome.MISMATCH:
            comparison = compare_audit_exact_depth(
                local_bids=local_snapshot.bids,
                local_asks=local_snapshot.asks,
                rest_bids=rest.bids,
                rest_asks=rest.asks,
            )
            if comparison.difference_count:
                # The exact fence proves the local image did not change over
                # the request/response window.  Without later causal evidence,
                # the strongest immediate classification is the observed
                # exact-depth divergence itself; no timing heuristic upgrades
                # it to a venue/window cause.
                cause = AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
            else:
                # Pending reported-top transitions and unhealthy owners were
                # rejected as non-comparable above, so neither is a reachable
                # MISMATCH cause here.  Equal depth can differ only on a
                # reference scalar, but the fenced response alone cannot prove
                # a precision transition rather than bad REST/reference data.
                # The bounded post-hoc raw join may upgrade it only when a
                # causally bound transition frame exists.
                cause = AuditMismatchCause.UNKNOWN
            local_provenance = (
                local_snapshot.state_source_provenance or local_snapshot.last_source_provenance
            )
            response_source = RawSourceRef(
                ingest_id=envelope.ingest_id,
                payload_sha256=envelope.payload_sha256,
            )
            try:
                mismatch_diagnostic = build_audit_mismatch_diagnostic(
                    cause=cause,
                    local_book=barrier_book,
                    local_bids=local_snapshot.bids,
                    local_asks=local_snapshot.asks,
                    local_raw_sources=(
                        () if local_provenance is None else local_provenance.raw_source_refs
                    ),
                    rest_instrument_id=rest.instrument_id,
                    rest_market_id=rest.market_id,
                    rest_bids=rest.bids,
                    rest_asks=rest.asks,
                    rest_tick_size=rest.tick_size,
                    rest_minimum_order_size=rest.minimum_order_size,
                    rest_neg_risk=rest.neg_risk,
                    rest_raw_sources=rest.provenance.raw_source_refs,
                    response_source=response_source,
                )
            except ValueError as error:
                raise ProgrammingInvariantViolation(
                    "bounded REST mismatch diagnostic could not be constructed",
                    component="canonical-processor",
                ) from error
        if outcome is AuditOutcome.MISMATCH and previous_health is not BookHealth.INVALID:
            self._invalidations += 1
        if outcome is AuditOutcome.MISMATCH:
            self._mark_audit_ineligible(owner.instrument_id, nonfault=True)
            # A mismatch invalidates this exact local book.  Suppress further
            # incrementals for this instrument until an authoritative REST or
            # full-book snapshot restores it, rather than allowing the next
            # delta to escape as a BookStateError and globally cascade.
            self._transition_resync_instruments.add(owner.instrument_id)
        evidence = AuditOutcomeEvidence(
            request_id=start.request_id,
            connection_id=start.connection_id,
            native_asset_id=start.native_asset_id,
            attempt=start.attempt,
            request_started_monotonic_ns=start.request_started_monotonic_ns,
            starting_admission_ordinal=start.starting_admission_watermark.ordinal,
            starting_processed_ordinal=start.starting_processed_watermark.ordinal,
            starting_book=start.starting_book,
            outcome=outcome,
            reason=None,
            response_ingest_id=envelope.ingest_id,
            raw_response_sha256=envelope.payload_sha256,
            response_received_wall_time=envelope.received_wall_time,
            response_received_monotonic_ns=envelope.received_monotonic.nanoseconds,
            response_boundary_ordinal=boundary,
            barrier_processed_ordinal=processed,
            barrier_book=barrier_book,
            requires_resync=outcome is AuditOutcome.MISMATCH,
            mismatch_diagnostic=mismatch_diagnostic,
            audit_start_schema=start.schema,
            trigger_reason=start.trigger_reason,
            audit_start_source=start_source,
            classification_source=classification_source,
        )
        self._record_audit_outcome(evidence)
        if outcome is AuditOutcome.MATCH:
            self._resolve_current_audit_faults(
                instrument_id=owner.instrument_id,
                resolution=AuditFaultResolution.LATER_FENCED_MATCH,
                resolution_source=classification_source,
                resolved_at=(
                    envelope.received_wall_time
                    if classification_control is None
                    else classification_control.received_wall_time
                ),
                resolved_monotonic_ns=(
                    envelope.received_monotonic.nanoseconds
                    if classification_control is None
                    else classification_control.received_monotonic.nanoseconds
                ),
            )
            if (
                owner.instrument_id not in self._audit_replacement_captures
                and not self._audit_fault_blocks_instrument(owner.instrument_id)
            ):
                self._clear_audit_ineligible(owner.instrument_id)
        if outcome is AuditOutcome.MISMATCH:
            self._open_audit_divergence(
                outcome=evidence,
                market_id=rest.market_id,
                start_source=start_source,
                classification_control=classification_control,
            )

    @staticmethod
    def _prepare_for_rest_snapshot(owner: CanonicalL2BookOwner) -> None:
        health = owner.health
        if health is BookHealth.UNINITIALIZED:
            owner.mark_syncing()
            return
        if health in {BookHealth.INVALID, BookHealth.STALE, BookHealth.UNAVAILABLE}:
            owner.begin_resync()
            owner.mark_syncing()
            return
        if health is BookHealth.RESYNCING:
            owner.mark_syncing()
            return
        if health is BookHealth.CLOSED:
            raise BookStateError("REST snapshot cannot reopen a closed book")

    @staticmethod
    def _raw_source(envelope: RawEventEnvelope) -> _RawReferenceSource:
        return _RawReferenceSource(
            ingest_id=envelope.ingest_id,
            payload_sha256=envelope.payload_sha256,
        )

    def _record_audit_replacement_snapshot(self, instrument_id: InstrumentId) -> None:
        """Advance post-resnapshot eligibility from exact replacement WS evidence."""

        if not self._audit_replacement_captures:
            return
        envelope, admission_ordinal = self._active_websocket_evidence()
        v2_candidates = tuple(
            capture
            for capture in self._audit_replacement_captures.values()
            if capture.audit_start_schema == AUDIT_START_V2_SCHEMA
            and instrument_id in capture.required_instrument_ids
            and envelope.connection_id != capture.original_connection_id
        )
        if v2_candidates:
            explicit_ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
            if (
                isinstance(explicit_ordinal, bool)
                or not isinstance(explicit_ordinal, int)
                or explicit_ordinal <= 0
            ):
                raise NormalizationError(
                    "v2 audit replacement snapshot requires an explicit admission ordinal"
                )
            admission_ordinal = explicit_ordinal
            if any(
                envelope.ingest_seq <= capture.resolution_ingest_seq
                or envelope.received_monotonic.epoch_id != capture.resolution_monotonic.epoch_id
                or envelope.received_monotonic.nanoseconds
                < capture.resolution_monotonic.nanoseconds
                for capture in v2_candidates
            ):
                raise NormalizationError(
                    "v2 audit replacement snapshot predates authoritative resolution"
                )
        raw_source = RawSourceRef(
            ingest_id=envelope.ingest_id,
            payload_sha256=envelope.payload_sha256,
        )
        for divergence_instrument_id in tuple(
            sorted(self._audit_replacement_captures, key=lambda item: item.value)
        ):
            pending = self._audit_replacement_captures[divergence_instrument_id]
            advanced = pending.observe_snapshot(
                admission_ordinal=admission_ordinal,
                connection_id=envelope.connection_id,
                ingest_seq=envelope.ingest_seq,
                instrument_id=instrument_id,
                maximum_age_ns=self._scanner.config.maximum_book_age_ns,
                received_monotonic=envelope.received_monotonic,
                raw_source=raw_source,
            )
            if advanced is pending:
                continue
            self._audit_replacement_captures[divergence_instrument_id] = advanced
            if not advanced.complete:
                continue
            if advanced.audit_start_schema == AUDIT_START_V1_SCHEMA:
                # Immutable v1 journals predate the durable release channel.
                # Preserve their exact bounded snapshot-only transition and
                # byte-structurally identical history payload.  New v2 state
                # remains pending until the coordinator control below.
                self._finalize_audit_replacement_capture(
                    divergence_instrument_id,
                    advanced,
                    restored_ingest_seq=envelope.ingest_seq,
                    restored_at=envelope.received_wall_time,
                    restored_monotonic=envelope.received_monotonic,
                    release_control_source=None,
                )

    def _validate_v2_audit_replacement_snapshot_envelope(
        self,
        envelope: RawEventEnvelope,
        snapshot_instruments: set[InstrumentId],
    ) -> None:
        candidates = tuple(
            capture
            for capture in self._audit_replacement_captures.values()
            if capture.audit_start_schema == AUDIT_START_V2_SCHEMA
            and snapshot_instruments.intersection(capture.required_instrument_ids)
        )
        if not candidates:
            return
        admission_ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
        if (
            isinstance(admission_ordinal, bool)
            or not isinstance(admission_ordinal, int)
            or admission_ordinal <= 0
        ):
            raise _AuditReplacementSnapshotEvidenceError(
                "v2 audit replacement snapshot requires an explicit admission ordinal"
            )
        if any(
            envelope.connection_id == capture.original_connection_id
            or envelope.connection_id in capture.pre_open_connection_ids
            for capture in candidates
        ):
            raise _AuditReplacementSnapshotEvidenceError(
                "v2 audit replacement snapshot requires a new post-resolution connection"
            )
        if any(
            envelope.ingest_seq <= capture.resolution_ingest_seq
            or envelope.received_monotonic.epoch_id != capture.resolution_monotonic.epoch_id
            or envelope.received_monotonic.nanoseconds < capture.resolution_monotonic.nanoseconds
            for capture in candidates
        ):
            raise _AuditReplacementSnapshotEvidenceError(
                "v2 audit replacement snapshot predates authoritative resolution"
            )

    def _finalize_audit_replacement_capture(
        self,
        instrument_id: InstrumentId,
        capture: _AuditReplacementCapture,
        *,
        restored_ingest_seq: int,
        restored_at: UtcTimestamp,
        restored_monotonic: MonotonicTimestamp,
        release_control_source: RawSourceRef | None,
    ) -> None:
        if self._audit_replacement_captures.get(instrument_id) != capture:
            raise ProgrammingInvariantViolation(
                "audit replacement release differs from current capture state",
                component="canonical-processor",
            )
        released_binding: AuditAvailabilityBinding | None = None
        if capture.audit_start_schema == AUDIT_START_V2_SCHEMA:
            if release_control_source is None:
                raise ProgrammingInvariantViolation(
                    "v2 audit availability release lacks its durable control source",
                    component="canonical-processor",
                )
            binding = self._audit_availability_bindings.get(capture.divergence_id)
            if binding is None or binding.authoritative_resolution_source is None:
                raise ProgrammingInvariantViolation(
                    "audit availability release lacks resolved authority",
                    component="canonical-processor",
                )
            released_binding = binding.with_replacement_release(release_control_source)
        elif release_control_source is not None:
            raise ProgrammingInvariantViolation(
                "legacy audit availability cannot invent a release source",
                component="canonical-processor",
            )
        self._audit_replacement_captures.pop(instrument_id)
        if released_binding is not None:
            self._audit_availability_released += 1
            self._audit_availability_release_chain = audit_availability_chain_extend(
                self._audit_availability_release_chain,
                "release",
                released_binding,
            )
            self._audit_availability_bindings.pop(capture.divergence_id)
            replacement_connection_id = capture.replacement_connection_id
            if replacement_connection_id is None or release_control_source is None:
                raise ProgrammingInvariantViolation(
                    "released audit replacement capture lacks its connection or control source",
                    component="canonical-processor",
                )
            restoration = AuditDivergenceEligibilityRestoration(
                divergence_id=capture.divergence_id,
                request_id=released_binding.request_id,
                instrument_id=instrument_id,
                market_id=capture.market_id,
                audit_start_schema=capture.audit_start_schema,
                authoritative_snapshot_source=capture.resolution_source,
                replacement_release_source=release_control_source,
                replacement_connection_id=replacement_connection_id,
                capture_state_sha256=canonical_sha256(capture.to_record()),
                availability_binding=released_binding,
                restored_ingest_seq=restored_ingest_seq,
                restored_at=restored_at,
                restored_monotonic=restored_monotonic,
            )
            active_restorations = self._active_audit_divergences_eligibility_restored
            if active_restorations is not None:
                active_restorations.append(restoration)
        if not self._audit_fault_blocks_instrument(instrument_id):
            self._clear_audit_ineligible(instrument_id)
        self._transition_resync_instruments.discard(instrument_id)
        history_record: dict[str, object] = {
            "replacement_capture": capture.to_record(),
            "restored_at": restored_at,
            "restored_monotonic": restored_monotonic,
        }
        if release_control_source is not None:
            history_record["release_control_source"] = release_control_source
        self._audit_divergence_history_chain = _chain_extend(
            self._audit_divergence_history_chain,
            "rest-audit-divergence-eligibility-restored",
            history_record,
        )
        self._mark_market_dirty(
            capture.market_id,
            ScanTriggerReason.REST_AUDIT,
        )

    def _release_audit_replacement_capture_from_control(
        self,
        envelope: RawEventEnvelope,
    ) -> None:
        control = self._control_object(
            envelope.payload_bytes,
            schema=AUDIT_REPLACEMENT_RELEASE_SCHEMA,
            expected_keys=frozenset(
                {
                    "capture_state_sha256",
                    "capture_record",
                    "decision_monotonic_ns",
                    "divergence_id",
                    "instrument_id",
                    "market_id",
                    "monotonic_epoch_id",
                    "replacement_connection_id",
                    "schema",
                }
            ),
        )
        try:
            plain_control: object = json.loads(envelope.payload_bytes)
            if not isinstance(plain_control, dict):
                raise ValueError("audit replacement release must be an object")
            typed_plain_control = cast(dict[str, object], plain_control)
            capture_record = typed_plain_control.get("capture_record")
            if not isinstance(capture_record, dict):
                raise ValueError("audit replacement capture record must be an object")
            typed_capture_record = cast(dict[str, object], capture_record)
            command = AuditReplacementReleaseCommand(
                divergence_id=self._control_text(control, "divergence_id", maximum=64),
                instrument_id=InstrumentId(
                    self._control_text(control, "instrument_id", maximum=256)
                ),
                market_id=MarketId(self._control_text(control, "market_id", maximum=256)),
                replacement_connection_id=ConnectionId(
                    self._control_text(
                        control,
                        "replacement_connection_id",
                        maximum=256,
                    )
                ),
                capture_state_sha256=self._control_text(
                    control,
                    "capture_state_sha256",
                    maximum=64,
                ),
                capture_record_json=canonical_json_bytes(typed_capture_record).decode("utf-8"),
                monotonic_epoch_id=self._control_text(
                    control,
                    "monotonic_epoch_id",
                    maximum=256,
                ),
                decision_monotonic_ns=self._control_integer(
                    control,
                    "decision_monotonic_ns",
                ),
            )
        except ValueError as error:
            raise NormalizationError("audit replacement release evidence is invalid") from error
        if envelope.payload_bytes != command.to_payload():
            raise NormalizationError(
                "audit replacement release evidence must use canonical encoding"
            )
        capture = self._audit_replacement_captures.get(command.instrument_id)
        if capture is None:
            raise NormalizationError("audit replacement release identifies no pending capture")
        if capture.audit_start_schema != AUDIT_START_V2_SCHEMA:
            raise NormalizationError(
                "historical audit replacement capture cannot use a release control"
            )
        decision_monotonic = MonotonicTimestamp(
            command.monotonic_epoch_id,
            command.decision_monotonic_ns,
        )
        expected = self._audit_replacement_release_command(
            command.instrument_id,
            capture,
            decision_monotonic,
        )
        if command != expected:
            raise NormalizationError(
                "audit replacement release differs from current capture evidence"
            )
        if (
            envelope.connection_id != command.replacement_connection_id
            or envelope.received_monotonic != decision_monotonic
            or envelope.ingest_seq
            <= max(source.ingest_seq for source in capture.captured_snapshot_sources)
        ):
            raise NormalizationError(
                "audit replacement release differs from its durable receive boundary"
            )
        if not self._audit_replacement_capture_fresh_at(
            capture,
            decision_monotonic,
            maximum_age_ns=self._scanner.config.maximum_book_age_ns,
        ):
            raise NormalizationError(
                "audit replacement release capture is incomplete or freshness-invalid"
            )
        if not self._audit_replacement_release_group_ready_at(
            self._audit_replacement_release_group(capture),
            connection_id=command.replacement_connection_id,
            decision_monotonic=decision_monotonic,
        ):
            raise NormalizationError(
                "audit replacement release scope has another eligibility blocker"
            )
        self._finalize_audit_replacement_capture(
            command.instrument_id,
            capture,
            restored_ingest_seq=envelope.ingest_seq,
            restored_at=envelope.received_wall_time,
            restored_monotonic=envelope.received_monotonic,
            release_control_source=RawSourceRef(
                ingest_id=envelope.ingest_id,
                payload_sha256=envelope.payload_sha256,
            ),
        )

    def _validate_gamma_identity(self, market: GammaMarket) -> None:
        prior_by_id = self._gamma_by_id.get(market.gamma_market_id)
        if prior_by_id is not None and (
            prior_by_id.condition_id != market.condition_id
            or prior_by_id.clob_token_ids != market.clob_token_ids
        ):
            raise NormalizationError(
                "Gamma market refresh changes the frozen condition/token identity"
            )
        prior_by_condition = self._gamma_by_condition.get(market.condition_id)
        if prior_by_condition is not None and (
            prior_by_condition.gamma_market_id != market.gamma_market_id
            or prior_by_condition.clob_token_ids != market.clob_token_ids
        ):
            raise NormalizationError(
                "Gamma condition refresh changes the frozen market/token identity"
            )
        if (
            market.event_id is not None
            and market.gamma_market_id in self._gamma_parent_binding_by_id
            and self._gamma_parent_binding_by_id[market.gamma_market_id] != market.event_id
        ):
            raise NormalizationError("Gamma market refresh changes the frozen parent event")

    def _remember_gamma(self, market: GammaMarket) -> None:
        self._validate_gamma_identity(market)
        self._gamma_by_condition[market.condition_id] = market
        self._gamma_by_id[market.gamma_market_id] = market
        if market.event_id is not None:
            self._gamma_parent_binding_by_id.setdefault(
                market.gamma_market_id,
                market.event_id,
            )

    def _try_publish_bundle(
        self,
        condition_id: str,
        observed_at: UtcTimestamp,
        *,
        rest_book: RestBookSnapshot | None = None,
    ) -> ReferenceBundle | None:
        gamma = self._gamma_by_condition.get(condition_id)
        clob = self._clob_by_condition.get(condition_id)
        if gamma is None or clob is None:
            return None
        parent = None if gamma.event_id is None else self._events_by_id.get(gamma.event_id)
        bundle = self._builder.build(
            gamma=gamma,
            clob=clob,
            rest_book=rest_book,
            parent_event=parent,
            observed_at=observed_at,
        )
        reference_provenance = self._reference_raw_provenance(condition_id, bundle)
        return self._publish_bundle(
            bundle,
            observed_at=observed_at,
            reference_provenance=reference_provenance,
        )

    def _publish_bundle(
        self,
        bundle: ReferenceBundle,
        *,
        observed_at: UtcTimestamp,
        reference_provenance: Provenance,
    ) -> ReferenceBundle:
        """Publish one reference observation through the sole integration boundary."""

        current = self._registry.current(bundle.market.market_id)
        had_initialized_book = any(
            self._owners[instrument_id].generation > 0
            for instrument_id in self._market_instruments.get(bundle.market.market_id, ())
            if instrument_id in self._owners
        )
        reference_changed = current is not None and (
            current.current_version_ids != bundle.current_version_ids
            or current.issues != bundle.issues
            or current.payout_asset_id != bundle.payout_asset_id
            or current.payout_per_set != bundle.payout_per_set
        )
        if (
            current is not None
            and current.current_version_ids == bundle.current_version_ids
            and current.issues == bundle.issues
            and self._reference_provenance.get(bundle.market.market_id) == reference_provenance
        ):
            return current
        self._registry.register(bundle)
        self._reference_provenance[bundle.market.market_id] = reference_provenance
        self._reference_updates += 1
        instrument_ids = tuple(item.instrument_id for item in bundle.instruments)
        self._market_instruments[bundle.market.market_id] = instrument_ids
        for instrument_id in instrument_ids:
            owner = self._owners.get(instrument_id)
            if owner is None:
                self._owners[instrument_id] = CanonicalL2BookOwner(bundle, instrument_id)
            else:
                previous_health = owner.health
                mutation = owner.apply_reference_bundle(bundle)
                if had_initialized_book:
                    self._record_book_mutation(
                        mutation,
                        reason=ScanTriggerReason.REFERENCE,
                    )
                if previous_health is BookHealth.HEALTHY and owner.health is BookHealth.INVALID:
                    self._invalidations += 1
        if had_initialized_book and reference_changed:
            self._mark_market_dirty(bundle.market.market_id, ScanTriggerReason.REFERENCE)
        if self._outputs is not None:
            self._outputs.append_reference(bundle, observed_at, reference_provenance)
        return bundle

    def _publish_dynamic_tick_reference(
        self,
        event: CanonicalEvent,
        previous_bundle: ReferenceBundle,
    ) -> ReferenceBundle:
        payload = event.payload
        if not isinstance(payload, TickSizeChangeEvent):
            raise ProgrammingInvariantViolation(
                "dynamic tick publication requires a tick-size canonical event",
                component="canonical-processor",
            )
        if event.venue_timestamp is None:
            raise ProgrammingInvariantViolation(
                "dynamic tick canonical event lacks its venue effective time",
                component="canonical-processor",
            )
        replacement = self._builder.with_tick_size(
            previous_bundle,
            new_tick_size=payload.new_tick_size,
            observed_at=event.receive_timestamp,
            effective_from=event.venue_timestamp,
            cross_source_confirmed=payload.cross_source_confirmed,
        )
        if replacement.precision.version_id != payload.new_precision_rule_version_id:
            raise ProgrammingInvariantViolation(
                "normalizer and processor derived different precision version identities",
                component="canonical-processor",
            )
        provenance = self._dynamic_reference_provenance(
            previous_bundle.market.market_id,
            event,
            replacement,
        )
        return self._publish_bundle(
            replacement,
            observed_at=event.receive_timestamp,
            reference_provenance=provenance,
        )

    def _dynamic_reference_provenance(
        self,
        market_id: MarketId,
        event: CanonicalEvent,
        replacement: ReferenceBundle,
    ) -> Provenance:
        prior = self._reference_provenance.get(market_id)
        if prior is None:
            raise ProgrammingInvariantViolation(
                "dynamic reference update has no prior raw reference provenance",
                component="canonical-processor",
            )
        try:
            ordered = tuple(
                sorted(
                    merge_raw_source_refs(prior.raw_source_refs, event.raw_source_refs),
                    key=lambda item: item.ingest_id.value,
                )
            )
        except RawSourceInvariantError as error:
            raise ProgrammingInvariantViolation(
                "dynamic reference provenance contains conflicting raw source pairs",
                component="canonical-processor",
            ) from error
        attributes = dict(prior.attributes)
        surfaces = tuple(
            item for item in attributes.get("reference_raw_surfaces", "").split(",") if item
        )
        dynamic_surface = "clob-wss:tick_size_change"
        attributes["reference_raw_surfaces"] = ",".join(
            (*surfaces, *((dynamic_surface,) if dynamic_surface not in surfaces else ()))
        )
        attributes["dynamic_reference_event_id"] = event.canonical_event_id.value
        return Provenance.from_raw_source_refs(
            ordered,
            reference_version_ids=replacement.current_version_ids,
            config_hash=self._config.config_hash,
            code_version=self._config.system.code_version,
            attributes=tuple(attributes.items()),
        )

    def _reference_raw_provenance(self, condition_id: str, bundle: ReferenceBundle) -> Provenance:
        gamma = self._gamma_by_condition[condition_id]
        named_sources: list[tuple[str, _RawReferenceSource]] = []
        gamma_source = self._gamma_source_by_condition.get(condition_id)
        if gamma_source is not None:
            named_sources.append(("gamma-market", gamma_source))
        relationship_source = self._gamma_relationship_source_by_condition.get(condition_id)
        if relationship_source is not None:
            named_sources.append(("gamma-market-relationship", relationship_source))
        if gamma.event_id is not None:
            event_source = self._event_source_by_id.get(gamma.event_id)
            if event_source is not None:
                named_sources.append(("gamma-parent-event", event_source))
        clob_source = self._clob_source_by_condition.get(condition_id)
        if clob_source is not None:
            named_sources.append(("clob-market-info", clob_source))
        rest_source = self._rest_source_by_condition.get(condition_id)
        if rest_source is not None:
            named_sources.append(("clob-rest-book", rest_source))
        try:
            ordered = tuple(
                sorted(
                    merge_raw_source_refs(
                        (
                            RawSourceRef(
                                ingest_id=source.ingest_id,
                                payload_sha256=source.payload_sha256,
                            )
                            for _name, source in named_sources
                        )
                    ),
                    key=lambda item: item.ingest_id.value,
                )
            )
        except RawSourceInvariantError as error:
            raise ProgrammingInvariantViolation(
                "reference surfaces contain conflicting raw source pairs",
                component="canonical-processor",
            ) from error
        return Provenance.from_raw_source_refs(
            ordered,
            reference_version_ids=bundle.current_version_ids,
            config_hash=self._config.config_hash,
            code_version=self._config.system.code_version,
            attributes=(("reference_raw_surfaces", ",".join(name for name, _ in named_sources)),),
        )

    def _normalization_context(
        self, envelope: RawEventEnvelope, bundle: ReferenceBundle | None
    ) -> NormalizationContext:
        return NormalizationContext(
            source_ingest_id=envelope.ingest_id,
            source_payload_sha256=envelope.payload_sha256,
            received_at=envelope.received_wall_time,
            reference_bundle=bundle,
        )

    def _expire_reported_top_transitions(self, envelope: RawEventEnvelope) -> None:
        for transition in tuple(self.reported_top_transitions):
            if envelope.received_monotonic.epoch_id != transition.monotonic_epoch_id:
                self._fail_reported_top_transition(
                    transition.instrument_id,
                    status=ReportedTopTransitionStatus.INVALIDATED,
                    reason=ReportedTopTransitionReason.SOCKET_GENERATION_CHANGED,
                    invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                )
            elif envelope.received_monotonic.nanoseconds >= transition.deadline_monotonic_ns:
                self._fail_reported_top_transition(
                    transition.instrument_id,
                    status=ReportedTopTransitionStatus.EXPIRED,
                    reason=ReportedTopTransitionReason.TIME_BOUND_EXPIRED,
                    invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                )

    def _expire_reported_top_transition_from_control(
        self,
        envelope: RawEventEnvelope,
    ) -> None:
        control = self._control_object(
            envelope.payload_bytes,
            schema=REPORTED_TOP_TRANSITION_EXPIRY_SCHEMA,
            expected_keys=frozenset(
                {
                    "deadline_monotonic_ns",
                    "decision_monotonic_ns",
                    "instrument_id",
                    "monotonic_epoch_id",
                    "schema",
                    "transition_id",
                }
            ),
        )
        try:
            command = ReportedTopTransitionExpiryCommand(
                transition_id=self._control_text(
                    control,
                    "transition_id",
                    maximum=64,
                ),
                instrument_id=InstrumentId(
                    self._control_text(control, "instrument_id", maximum=256)
                ),
                monotonic_epoch_id=self._control_text(
                    control,
                    "monotonic_epoch_id",
                    maximum=256,
                ),
                deadline_monotonic_ns=self._control_integer(
                    control,
                    "deadline_monotonic_ns",
                ),
                decision_monotonic_ns=self._control_integer(
                    control,
                    "decision_monotonic_ns",
                ),
            )
        except ValueError as error:
            raise NormalizationError("reported-top expiry evidence is invalid") from error
        if envelope.payload_bytes != command.to_payload():
            raise NormalizationError("reported-top expiry evidence must use canonical encoding")
        transition = self._reported_top_transitions.get(command.instrument_id)
        if transition is None:
            raise NormalizationError("reported-top expiry does not identify a current transition")
        if (
            transition.transition_id != command.transition_id
            or transition.monotonic_epoch_id != command.monotonic_epoch_id
            or transition.deadline_monotonic_ns != command.deadline_monotonic_ns
        ):
            raise NormalizationError(
                "reported-top expiry differs from the current transition bound"
            )
        if (
            envelope.received_monotonic.epoch_id != command.monotonic_epoch_id
            or envelope.received_monotonic.nanoseconds != command.decision_monotonic_ns
        ):
            raise NormalizationError(
                "reported-top expiry decision differs from its durable receive boundary"
            )
        self._fail_reported_top_transition(
            command.instrument_id,
            status=ReportedTopTransitionStatus.EXPIRED,
            reason=ReportedTopTransitionReason.TIME_BOUND_EXPIRED,
            invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
        )

    @staticmethod
    def _one_tick_difference(left: Price, right: Price, tick: ExactDecimal) -> bool:
        difference = left.value - right.value
        return difference == tick or difference == ExactDecimal.zero() - tick

    def _recognize_post_snapshot_catch_up(
        self,
        envelope: RawEventEnvelope,
        events: tuple[CanonicalEvent, ...],
    ) -> dict[InstrumentId, ReportedTopTransitionBaselineEvidence]:
        """Recognize only the three source-proven ordinal-2 corpus patterns."""

        ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
        if ordinal != 2 or len(events) != 2:
            return {}
        if not all(isinstance(event.payload, PriceChangeEvent) for event in events):
            return {}
        price_events = events
        payloads = tuple(cast(PriceChangeEvent, event.payload) for event in price_events)
        market_ids = {payload.market_id for payload in payloads}
        if len(market_ids) != 1:
            return {}
        market_id = next(iter(market_ids))
        baseline = self._post_snapshot_baselines.get(market_id)
        evidence = None if baseline is None else baseline.evidence
        if (
            evidence is None
            or evidence.admission_ordinal != 1
            or evidence.connection_id != envelope.connection_id
            or evidence.monotonic_epoch_id != envelope.received_monotonic.epoch_id
            or evidence.ingest_seq >= envelope.ingest_seq
            or evidence.received_monotonic_ns >= envelope.received_monotonic.nanoseconds
        ):
            return {}
        expected_instruments = self._market_instruments.get(market_id, ())
        instruments = tuple(payload.instrument_id for payload in payloads)
        if set(instruments) != set(expected_instruments) or len(set(instruments)) != 2:
            return {}
        if any(
            instrument_id in self._reported_top_transitions
            or instrument_id in self._transition_resync_instruments
            for instrument_id in instruments
        ):
            return {}
        if any(len(payload.changes) != 1 for payload in payloads):
            return {}
        changes = tuple(payload.changes[0] for payload in payloads)
        if (
            changes[0].side is changes[1].side
            or changes[0].price.value + changes[1].price.value != ExactDecimal.one()
            or changes[0].new_quantity != changes[1].new_quantity
            or changes[0].new_quantity.value <= ExactDecimal.zero()
        ):
            return {}
        baseline_by_instrument = {
            instrument_id: (generation, exact_state_sha256, best_bid, best_ask)
            for instrument_id, generation, exact_state_sha256, best_bid, best_ask in (
                evidence.instrument_states
            )
        }
        targets: list[tuple[Price, Price]] = []
        target_change_sides: list[BookSide] = []
        source_change_matches_target_side: list[bool] = []
        for event, payload, change in zip(price_events, payloads, changes, strict=True):
            owner = self._owners[payload.instrument_id]
            baseline_state = baseline_by_instrument.get(payload.instrument_id)
            if baseline_state is None:
                return {}
            current_top = self._owner_top(owner)
            if (
                owner.generation != baseline_state[0]
                or owner.audit_comparison_sha256() != baseline_state[1]
                or current_top != (baseline_state[2], baseline_state[3])
            ):
                return {}
            target = self._reported_target(payload)
            if target is None or target[0].value >= target[1].value:
                return {}
            if current_top[0] is None or current_top[1] is None:
                return {}
            complete_current_top = cast(tuple[Price, Price], current_top)
            changed_sides = int(target[0] != complete_current_top[0]) + int(
                target[1] != complete_current_top[1]
            )
            tick = owner.tick_size
            if (
                changed_sides != 1
                or tick is None
                or not any(
                    self._one_tick_difference(candidate, prior, tick)
                    for candidate, prior in zip(target, complete_current_top, strict=True)
                    if candidate != prior
                )
            ):
                return {}
            try:
                preview = owner.preview_price_change(payload, event=event)
            except BookStateError:
                return {}
            # Every source-proven ordinal-2 case changed exact depth without
            # changing the current derived top.  Requiring both facts excludes
            # economic no-ops and broader complementary mismatch patterns.
            if (
                not preview.levels_changed
                or preview.crossed
                or (preview.best_bid, preview.best_ask) != complete_current_top
                or (preview.best_bid, preview.best_ask) == target
            ):
                return {}
            target_side = BookSide.BID if target[0] != complete_current_top[0] else BookSide.ASK
            target_price = target[0] if target_side is BookSide.BID else target[1]
            matches_target_side = change.side is target_side
            if matches_target_side:
                # Corpus case 84229: the exact change adds/resizes the future
                # reported top level, while the old better level remains.
                if change.price != target_price:
                    return {}
            else:
                # Corpus cases 26146/57885: the paired exact changes are deep
                # complementary updates on the opposite side.  They must be
                # strictly worse than the baseline top and cannot themselves
                # equal either reported top.
                if change.price in target:
                    return {}
                if change.side is BookSide.BID:
                    if change.price.value >= complete_current_top[0].value:
                        return {}
                elif change.price.value <= complete_current_top[1].value:
                    return {}
            target_change_sides.append(target_side)
            source_change_matches_target_side.append(matches_target_side)
            targets.append(target)
        first_target, second_target = targets
        if (
            first_target[0].value + second_target[1].value != ExactDecimal.one()
            or first_target[1].value + second_target[0].value != ExactDecimal.one()
            or target_change_sides[0] is target_change_sides[1]
            or source_change_matches_target_side[0] != source_change_matches_target_side[1]
        ):
            return {}
        return dict.fromkeys(instruments, evidence)

    def _remember_complete_ordinal_one_snapshot(
        self,
        envelope: RawEventEnvelope,
        events: tuple[CanonicalEvent, ...],
    ) -> None:
        ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
        if (
            isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or ordinal != 1
            or len(events) != 2
        ):
            return
        if not all(isinstance(event.payload, OrderBookSnapshotEvent) for event in events):
            return
        snapshots = tuple(cast(OrderBookSnapshotEvent, event.payload) for event in events)
        market_ids = {snapshot.market_id for snapshot in snapshots}
        if len(market_ids) != 1:
            return
        market_id = next(iter(market_ids))
        expected = self._market_instruments.get(market_id, ())
        if {snapshot.instrument_id for snapshot in snapshots} != set(expected):
            return
        try:
            raw_refs = tuple(
                sorted(
                    merge_raw_source_refs(*(event.raw_source_refs for event in events)),
                    key=lambda item: item.ingest_id.value,
                )
            )
        except RawSourceInvariantError as error:
            raise ProgrammingInvariantViolation(
                "post-snapshot baseline contains conflicting raw sources",
                component="canonical-processor",
            ) from error
        evidence = ReportedTopTransitionBaselineEvidence(
            connection_id=envelope.connection_id,
            monotonic_epoch_id=envelope.received_monotonic.epoch_id,
            admission_ordinal=ordinal,
            ingest_seq=envelope.ingest_seq,
            received_at=envelope.received_wall_time,
            received_monotonic_ns=envelope.received_monotonic.nanoseconds,
            source_raw_refs=raw_refs,
            source_canonical_event_ids=tuple(event.canonical_event_id for event in events),
            instrument_states=tuple(
                (
                    instrument_id,
                    self._owners[instrument_id].generation,
                    self._owners[instrument_id].audit_comparison_sha256(),
                    self._owner_top(self._owners[instrument_id])[0],
                    self._owner_top(self._owners[instrument_id])[1],
                )
                for instrument_id in sorted(expected, key=lambda item: item.value)
            ),
        )
        self._post_snapshot_baselines[market_id] = _PostSnapshotBaseline(
            market_id=market_id,
            evidence=evidence,
        )

    def _route_websocket(self, envelope: RawEventEnvelope) -> None:
        native_events = parse_market_ws_message(envelope.payload_bytes)
        condition_ids = {
            condition
            for native in native_events
            if not isinstance(native, WsNewMarketEvent)
            if (condition := getattr(native, "condition_id", None)) is not None
        }
        if len(condition_ids) > 1:
            raise NormalizationError(
                "one raw WebSocket message cannot mix multiple reference conditions"
            )
        market_id: MarketId | None = None
        bundle: ReferenceBundle | None = None
        if condition_ids:
            condition = next(iter(condition_ids))
            market_id = self._market_id_for_condition(condition)
            bundle = self._registry.as_known_at(market_id, envelope.received_wall_time)
            if bundle is None:
                raise NormalizationError(
                    "WebSocket message has no as-known-at-time reference bundle"
                )
        events = self._normalizer.normalize_parsed(
            envelope.payload_bytes,
            native_events,
            self._normalization_context(envelope, bundle),
        )
        recognized = self._recognize_post_snapshot_catch_up(envelope, events)
        snapshot_instruments = {
            event.payload.instrument_id
            for event in events
            if isinstance(event.payload, OrderBookSnapshotEvent)
        }
        self._validate_v2_audit_replacement_snapshot_envelope(
            envelope,
            snapshot_instruments,
        )
        for transition in tuple(self.reported_top_transitions):
            if (
                transition.connection_id != envelope.connection_id
                and transition.instrument_id not in snapshot_instruments
            ):
                self._fail_reported_top_transition(
                    transition.instrument_id,
                    status=ReportedTopTransitionStatus.INVALIDATED,
                    reason=ReportedTopTransitionReason.SOCKET_GENERATION_CHANGED,
                    invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                )
        for event in events:
            self._active_canonical_event = event
            try:
                self._apply_canonical(
                    event,
                    recognized_post_snapshot=(
                        None if event.instrument_id is None else recognized.get(event.instrument_id)
                    ),
                )
            finally:
                self._active_canonical_event = None
            if isinstance(event.payload, TickSizeChangeEvent):
                if bundle is None:
                    raise ProgrammingInvariantViolation(
                        "tick-size event normalized without a reference bundle",
                        component="canonical-processor",
                    )
                bundle = self._publish_dynamic_tick_reference(event, bundle)
        if market_id is not None:
            ordinal = envelope.transport_metadata.get("websocket_admission_ordinal")
            if ordinal != 1:
                self._post_snapshot_baselines.pop(market_id, None)
        self._remember_complete_ordinal_one_snapshot(envelope, events)

    def _apply_canonical(
        self,
        event: CanonicalEvent,
        *,
        recognized_post_snapshot: ReportedTopTransitionBaselineEvidence | None = None,
    ) -> None:
        self._canonical_events += 1
        self._canonical_event_chain = _chain_extend(
            self._canonical_event_chain,
            "canonical-event",
            event.canonical_event_id.value,
        )
        if self._outputs is not None:
            self._outputs.append_canonical(event)
        refresh_request = self._reference_refresh_request(event)
        if refresh_request is not None:
            self._record_reference_refresh(refresh_request)
        if event.market_id is None:
            return
        if isinstance(event.payload, BestBidAskEvent):
            self._apply_best_bid_ask(event)
            return
        target_ids = self._market_instruments.get(event.market_id, ())
        if event.instrument_id is not None:
            target_ids = (event.instrument_id,)
        for instrument_id in target_ids:
            owner = self._owners[instrument_id]
            previous = owner.health
            if instrument_id in self._transition_resync_instruments and not isinstance(
                event.payload, (OrderBookSnapshotEvent, MarketResolvedEvent)
            ):
                # Exact incremental state cannot be based on an invalidated
                # transition image.  Retain the raw/canonical record and wait
                # for an authoritative baseline without creating cascaded
                # BookStateError failures.
                continue
            if isinstance(event.payload, PriceChangeEvent) and (
                self._ws_l2_exceeds_freshness_window(instrument_id)
            ):
                mutation = owner.invalidate(InvalidationReason.STALE_SNAPSHOT)
                if previous is not BookHealth.INVALID:
                    self._invalidations += 1
                self._record_book_mutation(
                    mutation,
                    reason=ScanTriggerReason.INVALIDATION,
                )
                self._record_recovery_request(
                    reason=processor_recovery_reason_for_invalidation(
                        InvalidationReason.STALE_SNAPSHOT
                    ),
                    affected_instrument_ids=(instrument_id,),
                    original_reason_code=InvalidationReason.STALE_SNAPSHOT.value,
                    effective_invalidation_reason=InvalidationReason.STALE_SNAPSHOT,
                )
                continue
            try:
                if isinstance(event.payload, PriceChangeEvent):
                    transition = self._reported_top_transitions.get(instrument_id)
                    try:
                        preview = owner.preview_price_change(event.payload, event=event)
                    except BookStateError:
                        if transition is None:
                            raise
                        self._fail_reported_top_transition(
                            instrument_id,
                            status=ReportedTopTransitionStatus.INVALIDATED,
                            reason=ReportedTopTransitionReason.INVALID_EXACT_FRAME,
                            invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                        )
                        continue
                    reported_target = self._reported_target(event.payload)
                    if transition is not None:
                        if transition.accepted_frame_count >= transition.maximum_frames:
                            self._fail_reported_top_transition(
                                instrument_id,
                                status=ReportedTopTransitionStatus.EXPIRED,
                                reason=ReportedTopTransitionReason.FRAME_BOUND_EXCEEDED,
                                invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                            )
                            continue
                        bound_target = (
                            transition.target_best_bid,
                            transition.target_best_ask,
                        )
                        if reported_target is not None and reported_target != bound_target:
                            if (
                                not preview.crossed
                                and (preview.best_bid, preview.best_ask) == reported_target
                            ):
                                mutation = owner.apply_price_change(event.payload, event=event)
                                self._close_reported_top_transition(
                                    instrument_id,
                                    status=ReportedTopTransitionStatus.SUPERSEDED,
                                    reason=(
                                        ReportedTopTransitionReason.COMPLETE_REPORTED_TOP_SUPERSEDED
                                    ),
                                    research_eligibility_restored=(
                                        instrument_id not in self._audit_ineligible_instruments
                                    ),
                                )
                                if not mutation.audit_economic_changed:
                                    self._advance_audit_economic_generation(instrument_id)
                            else:
                                self._fail_reported_top_transition(
                                    instrument_id,
                                    status=ReportedTopTransitionStatus.INVALIDATED,
                                    reason=(ReportedTopTransitionReason.CONFLICTING_REPORTED_TOP),
                                    invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                                )
                                continue
                        elif preview.crossed:
                            self._fail_reported_top_transition(
                                instrument_id,
                                status=ReportedTopTransitionStatus.INVALIDATED,
                                reason=ReportedTopTransitionReason.CROSSED_EXACT_BOOK,
                                invalidation_reason=InvalidationReason.IMPOSSIBLE_STATE,
                            )
                            continue
                        else:
                            mutation = owner.apply_price_change(
                                event.payload,
                                event=event,
                                reported_top_transition_target=bound_target,
                            )
                            advanced = self._advance_reported_top_transition(
                                transition,
                                event=event,
                                reason=ReportedTopTransitionReason.SAME_TARGET_EXACT_FRAME,
                            )
                            if advanced is None:
                                continue
                            if (preview.best_bid, preview.best_ask) == bound_target:
                                self._close_reported_top_transition(
                                    instrument_id,
                                    status=ReportedTopTransitionStatus.CONVERGED,
                                    reason=ReportedTopTransitionReason.EXACT_L2_CONVERGED,
                                    research_eligibility_restored=(
                                        instrument_id not in self._audit_ineligible_instruments
                                    ),
                                )
                                if not mutation.audit_economic_changed:
                                    self._advance_audit_economic_generation(instrument_id)
                    elif (
                        reported_target is not None
                        and (
                            preview.best_bid,
                            preview.best_ask,
                        )
                        != reported_target
                    ):
                        if recognized_post_snapshot is None:
                            self._reported_top_no_pending_rejected += 1
                            envelope, ordinal = self._active_websocket_evidence()
                            self._reported_top_transition_chain = _chain_extend(
                                self._reported_top_transition_chain,
                                "reported-top-transition",
                                (
                                    "REJECT",
                                    instrument_id,
                                    event.canonical_event_id,
                                    envelope.connection_id,
                                    ordinal,
                                    envelope.ingest_seq,
                                    reported_target,
                                    preview,
                                    ReportedTopTransitionReason.UNRECOGNIZED_NO_PENDING,
                                ),
                            )
                            was_invalid = owner.health is BookHealth.INVALID
                            mutation = owner.invalidate(InvalidationReason.PROTOCOL_VIOLATION)
                            self._transition_resync_instruments.add(instrument_id)
                            self._record_book_mutation(
                                mutation,
                                reason=ScanTriggerReason.INVALIDATION,
                            )
                            self._record_recovery_request(
                                reason=processor_recovery_reason_for_invalidation(
                                    InvalidationReason.PROTOCOL_VIOLATION
                                ),
                                affected_instrument_ids=(instrument_id,),
                                original_reason_code=(
                                    ReportedTopTransitionReason.UNRECOGNIZED_NO_PENDING.value
                                ),
                                effective_invalidation_reason=(
                                    InvalidationReason.PROTOCOL_VIOLATION
                                ),
                            )
                            if not was_invalid:
                                self._invalidations += 1
                            continue
                        transition = self._start_reported_top_transition(
                            event=event,
                            owner=owner,
                            target=reported_target,
                            entry_source=(ReportedTopTransitionEntrySource.POST_SNAPSHOT_CATCH_UP),
                            reason=(ReportedTopTransitionReason.POST_SNAPSHOT_PAIRED_CATCH_UP),
                            baseline_evidence=recognized_post_snapshot,
                        )
                        if preview.crossed:
                            self._fail_reported_top_transition(
                                instrument_id,
                                status=ReportedTopTransitionStatus.INVALIDATED,
                                reason=ReportedTopTransitionReason.CROSSED_EXACT_BOOK,
                                invalidation_reason=InvalidationReason.IMPOSSIBLE_STATE,
                            )
                            continue
                        try:
                            mutation = owner.apply_price_change(
                                event.payload,
                                event=event,
                                reported_top_transition_target=(
                                    transition.target_best_bid,
                                    transition.target_best_ask,
                                ),
                            )
                        except BookStateError:
                            self._fail_reported_top_transition(
                                instrument_id,
                                status=ReportedTopTransitionStatus.INVALIDATED,
                                reason=ReportedTopTransitionReason.INVALID_EXACT_FRAME,
                                invalidation_reason=InvalidationReason.PROTOCOL_VIOLATION,
                            )
                            continue
                    else:
                        mutation = owner.apply_price_change(event.payload, event=event)
                else:
                    mutation = owner.apply_event(event)
            except RawSourceInvariantError as error:
                raise ProgrammingInvariantViolation(
                    "book-state provenance contains conflicting raw source pairs",
                    component="canonical-processor",
                ) from error
            except BookStateError:
                if previous is not BookHealth.INVALID:
                    self._invalidations += 1
                raise
            if previous is not BookHealth.INVALID and owner.health is BookHealth.INVALID:
                self._invalidations += 1
            if (
                isinstance(event.payload, TickSizeChangeEvent)
                and instrument_id in self._reported_top_transitions
            ):
                self._fail_reported_top_transition(
                    instrument_id,
                    status=ReportedTopTransitionStatus.INVALIDATED,
                    reason=(ReportedTopTransitionReason.REFERENCE_PRECISION_CHANGED),
                    invalidation_reason=InvalidationReason.UNKNOWN_PRECISION,
                )
            if isinstance(event.payload, OrderBookSnapshotEvent):
                self._record_audit_replacement_snapshot(owner.instrument_id)
                replacement_capture = self._audit_replacement_captures.get(owner.instrument_id)
                if (
                    replacement_capture is None
                    or replacement_capture.audit_start_schema == AUDIT_START_V2_SCHEMA
                ):
                    self._transition_resync_instruments.discard(owner.instrument_id)
                self._close_reported_top_transition(
                    owner.instrument_id,
                    status=ReportedTopTransitionStatus.AUTHORITATIVE_SNAPSHOT,
                    reason=ReportedTopTransitionReason.AUTHORITATIVE_FULL_SNAPSHOT,
                    research_eligibility_restored=(
                        owner.instrument_id not in self._audit_ineligible_instruments
                    ),
                )
            elif isinstance(event.payload, MarketResolvedEvent):
                self._transition_resync_instruments.discard(owner.instrument_id)
                self._close_reported_top_transition(
                    owner.instrument_id,
                    status=ReportedTopTransitionStatus.INVALIDATED,
                    reason=ReportedTopTransitionReason.MARKET_RESOLVED,
                    research_eligibility_restored=False,
                )
            if isinstance(event.payload, MarketResolvedEvent):
                reason = ScanTriggerReason.LIFECYCLE
            elif isinstance(event.payload, TickSizeChangeEvent):
                reason = ScanTriggerReason.REFERENCE
            elif isinstance(event.payload, (OrderBookSnapshotEvent, PriceChangeEvent)):
                reason = ScanTriggerReason.BOOK_UPDATE
            else:
                reason = ScanTriggerReason.BOOK_UPDATE
            self._record_book_mutation(mutation, reason=reason)

    def _record_reference_refresh(self, request: ReferenceRefreshRequest) -> None:
        active = self._active_refresh_requests
        if active is None:
            raise ProgrammingInvariantViolation(
                "reference refresh was emitted outside envelope processing",
                component="canonical-processor",
            )
        self._reference_refresh_count += 1
        self._reference_refresh_chain = _chain_extend(
            self._reference_refresh_chain,
            "reference-refresh",
            request,
        )
        self._reference_refresh_requests.append(request)
        active.append(request)

    def _reference_refresh_request(self, event: CanonicalEvent) -> ReferenceRefreshRequest | None:
        payload = event.payload
        if isinstance(payload, NewMarketEvent):
            if not payload.requires_reference_refresh:
                return None
            return ReferenceRefreshRequest(
                source_event_id=event.canonical_event_id,
                event_type=event.event_type,
                recovery_reason=ProcessorRecoveryReason.REFERENCE_REFRESH,
                market_id=None,
                native_market_id=payload.native_market_id,
                native_condition_id=payload.native_condition_id,
                native_asset_ids=payload.native_asset_ids,
            )
        requires_current_reference_refresh = (
            isinstance(payload, MarketResolvedEvent) and payload.requires_reference_refresh
        ) or (isinstance(payload, TickSizeChangeEvent) and not payload.cross_source_confirmed)
        if not requires_current_reference_refresh:
            return None
        if event.market_id is None:
            raise ProgrammingInvariantViolation(
                "current-market refresh request lacks a canonical market identity",
                component="canonical-processor",
            )
        bundle = self._registry.current(event.market_id)
        if bundle is None:
            raise ProgrammingInvariantViolation(
                "current-market refresh request lacks current reference data",
                component="canonical-processor",
            )
        return ReferenceRefreshRequest(
            source_event_id=event.canonical_event_id,
            event_type=event.event_type,
            recovery_reason=(
                ProcessorRecoveryReason.TARGET_LIFECYCLE
                if isinstance(payload, MarketResolvedEvent)
                else ProcessorRecoveryReason.REFERENCE_REFRESH
            ),
            market_id=event.market_id,
            native_market_id=bundle.market.gamma_market_id,
            native_condition_id=bundle.market.native_condition_id,
            native_asset_ids=tuple(item.native_token_id for item in bundle.instruments),
        )

    def _scan_market(self, bundle: ReferenceBundle, observed_at: UtcTimestamp) -> None:
        if not self._config.scanner.enabled:
            return
        instrument_ids = self._market_instruments.get(bundle.market.market_id, ())
        if len(instrument_ids) != 2:
            return
        divergence_blocked = {
            *self._pending_audit_divergences,
            *self._audit_replacement_captures,
        }
        if divergence_blocked.intersection(instrument_ids):
            # ADR-0054 forbids invoking the scanner while an exact mismatch is
            # unresolved or while its REST-restored baseline still lacks full
            # replacement-generation capture coverage.  Dirty state remains
            # coalesced; the final qualifying WS snapshot marks the market
            # dirty again after releasing this block.
            return
        audit_ineligible = set(self.audit_ineligible_instruments)
        snapshot_views_list: list[tuple[ImmutableOrderBookSnapshot, str]] = []
        for instrument_id in instrument_ids:
            owner = self._owners[instrument_id]
            if instrument_id in audit_ineligible:
                snapshot = replace(
                    owner.snapshot(),
                    health=BookHealth.STALE,
                    invalidation_reason=None,
                )
                snapshot_views_list.append((snapshot, snapshot.economic_content_sha256()))
            else:
                snapshot_views_list.append(owner.snapshot_with_economic_hash())
        snapshot_views = tuple(snapshot_views_list)
        books = tuple(item[0] for item in snapshot_views)
        book_state_hashes = {
            snapshot.instrument_id: state_hash for snapshot, state_hash in snapshot_views
        }
        provenance = self._scan_provenance(bundle, books)
        proof_entry = self._semantic_proofs.get(bundle.market.market_id)
        if proof_entry is None or proof_entry[0] is not bundle:
            proof = prove_binary_complement(bundle)
            self._semantic_proofs[bundle.market.market_id] = (
                bundle,
                proof,
            )
        else:
            proof = proof_entry[1]
        try:
            result = self._scanner.scan(
                bundle=bundle,
                proof=proof,
                books=books,
                observed_at=observed_at,
                provenance=provenance,
                book_state_hashes=book_state_hashes,
            )
        except RawSourceInvariantError as error:
            raise ProgrammingInvariantViolation(
                "scanner provenance contains conflicting raw source pairs",
                component="canonical-processor",
            ) from error
        self._scan_count += 1
        self._scan_disposition_counts[result.disposition] += 1
        self._scan_chain = _chain_extend(self._scan_chain, "scan", result.terminal_hash)
        if result.candidate is not None:
            self._candidate_count += 1
        if result.disposition is ScanDisposition.VALIDATED_OPPORTUNITY:
            self._validated_opportunity_count += 1
            self._validated_opportunity_chain = _chain_extend(
                self._validated_opportunity_chain,
                "validated-opportunity",
                result.terminal_hash,
            )
            self._validated_opportunity_hashes.append(result.terminal_hash)
        lifecycle: OpportunityLifecycle | None = None
        if result.candidate is not None:
            status = self._lifecycle_status(result)
            lifecycle = self._lifecycle_tracker.observe(
                lifecycle_id=result.candidate.lifecycle_id,
                opportunity_id=result.candidate.opportunity_id,
                observed_at=observed_at,
                status=status,
            )
        else:
            lifecycle_id = complete_set_lifecycle_id(
                strategy_id=self._scanner.strategy_id,
                strategy_version=self._scanner.strategy_version,
                market_id=bundle.market.market_id,
                instrument_ids=instrument_ids,
            )
            lifecycle = self._lifecycle_tracker.expire_if_active(
                lifecycle_id,
                expired_at=observed_at,
            )
        self._scan_results.append(result)
        if self._outputs is not None:
            self._outputs.append_scan(result, lifecycle=lifecycle)

    @staticmethod
    def _lifecycle_status(result: CompleteSetScanResult) -> OpportunityLifecycleStatus:
        if result.opportunity is not None:
            return result.opportunity.status
        reason_codes = {
            code for validation in result.validation_results for code in validation.reason_codes
        }
        if reason_codes & {
            ValidationReasonCode.UNKNOWN_COST,
            ValidationReasonCode.COST_RULE_MISMATCH,
        }:
            return OpportunityLifecycleStatus.REJECTED_COSTS
        return OpportunityLifecycleStatus.REJECTED_DEPTH

    def _scan_provenance(
        self,
        bundle: ReferenceBundle,
        books: tuple[ImmutableOrderBookSnapshot, ...],
    ) -> Provenance:
        raw_source_groups: list[tuple[RawSourceRef, ...]] = []
        reference_provenance = self._reference_provenance.get(bundle.market.market_id)
        if reference_provenance is not None:
            raw_source_groups.append(reference_provenance.raw_source_refs)
        for book in books:
            source = book.state_source_provenance or book.last_source_provenance
            if source is None:
                continue
            raw_source_groups.append(source.raw_source_refs)
        try:
            raw_source_refs = tuple(
                sorted(
                    merge_raw_source_refs(*raw_source_groups),
                    key=lambda item: item.ingest_id.value,
                )
            )
        except RawSourceInvariantError as error:
            raise ProgrammingInvariantViolation(
                "scan provenance contains conflicting raw source pairs",
                component="canonical-processor",
            ) from error
        version_by_identity: dict[tuple[type[VersionId], str], VersionId] = {}
        for version in bundle.current_version_ids:
            version_by_identity[(type(version), version.value)] = version
        return Provenance.from_raw_source_refs(
            raw_source_refs,
            reference_version_ids=tuple(
                version_by_identity[key]
                for key in sorted(
                    version_by_identity,
                    key=lambda item: (item[0].__name__, item[1]),
                )
            ),
            config_hash=self._config.config_hash,
            code_version=self._config.system.code_version,
            attributes=(("economic_layer", "BOOK_UPPER_BOUND"),),
        )

    def invalidate_all(self, reason: InvalidationReason) -> tuple[InstrumentId, ...]:
        transition_reason = (
            ReportedTopTransitionReason.DISCONNECT
            if reason is InvalidationReason.DISCONNECT
            else ReportedTopTransitionReason.INVALID_EXACT_FRAME
        )
        for transition in tuple(self.reported_top_transitions):
            self._transition_resync_instruments.add(transition.instrument_id)
            self._close_reported_top_transition(
                transition.instrument_id,
                status=ReportedTopTransitionStatus.INVALIDATED,
                reason=transition_reason,
                research_eligibility_restored=False,
            )
        self._post_snapshot_baselines.clear()
        affected: list[InstrumentId] = []
        for owner in self.owners:
            if owner.health is BookHealth.CLOSED:
                continue
            was_invalid = owner.health is BookHealth.INVALID
            mutation = owner.invalidate(reason)
            self._record_book_mutation(
                mutation,
                reason=ScanTriggerReason.INVALIDATION,
            )
            affected.append(owner.instrument_id)
            if not was_invalid:
                self._invalidations += 1
        affected_tuple = tuple(affected)
        self._record_recovery_request(
            reason=processor_recovery_reason_for_invalidation(reason),
            affected_instrument_ids=affected_tuple,
            original_reason_code=reason.value,
            effective_invalidation_reason=reason,
        )
        return affected_tuple

    def on_disconnect(self) -> tuple[InstrumentId, ...]:
        return self.invalidate_all(InvalidationReason.DISCONNECT)

    def on_backpressure(self) -> tuple[InstrumentId, ...]:
        return self.invalidate_all(InvalidationReason.BACKPRESSURE_DATA_LOSS)

    def snapshots(self) -> tuple[ImmutableOrderBookSnapshot, ...]:
        return tuple(owner.snapshot() for owner in self.owners)

    @staticmethod
    def _retention_snapshot[HistoryT](
        domain: ProcessorRetentionDomain,
        *,
        history: deque[HistoryT],
        total: int,
    ) -> ProcessorRetentionSnapshot:
        retained = len(history)
        capacity = history.maxlen
        if capacity is None:  # pragma: no cover - all processor histories are bounded
            raise ProgrammingInvariantViolation(
                "processor diagnostic retention lost its hard bound",
                component="canonical-processor",
            )
        # This deliberately does not traverse retained objects.  The estimate is
        # the concrete deque allocation plus the shallow size of the latest
        # same-domain object multiplied by occupancy.  Full recursive/deep size
        # remains an offline saturation-benchmark concern because walking every
        # retained record would reintroduce history-sized periodic health work.
        container_shallow_bytes = sys.getsizeof(history)
        representative_object_shallow_bytes = 0 if not history else sys.getsizeof(history[-1])
        return ProcessorRetentionSnapshot(
            domain=domain,
            retained=retained,
            capacity=capacity,
            total=total,
            truncated=total > retained,
            container_shallow_bytes=container_shallow_bytes,
            representative_object_shallow_bytes=representative_object_shallow_bytes,
            byte_estimate_method="deque-container-plus-latest-object-shallow-size-times-retained",
        )

    def _audit_availability_authority_snapshot(
        self,
    ) -> AuditAvailabilityAuthoritySnapshot:
        """Return bounded exact-v2 mismatch/recovery/release authority."""

        return AuditAvailabilityAuthoritySnapshot(
            opened=self._audit_availability_opened,
            resolved=self._audit_availability_resolved,
            released=self._audit_availability_released,
            legacy_unverifiable=self._audit_availability_legacy_unverifiable,
            open_chain_sha256=self._audit_availability_open_chain,
            resolution_chain_sha256=self._audit_availability_resolution_chain,
            release_chain_sha256=self._audit_availability_release_chain,
            current_bindings=tuple(
                self._audit_availability_bindings[key]
                for key in sorted(self._audit_availability_bindings)
            ),
        )

    def _audit_replacement_capture_authority_snapshot(
        self,
    ) -> tuple[ProcessorAuditReplacementCaptureSnapshot, ...]:
        """Return every current capture field without copying unbounded history."""

        return tuple(
            self._audit_replacement_captures[instrument_id].authority_snapshot(
                instrument_id=instrument_id
            )
            for instrument_id in sorted(
                self._audit_replacement_captures,
                key=lambda item: item.value,
            )
        )

    def operational_snapshot(self) -> ProcessorOperationalSnapshot:
        """Return the bounded O(books + finite matrices) periodic-health view."""

        recovery_reason_counts = tuple(
            (reason, self._recovery_reason_counts[reason])
            for reason in sorted(ProcessorRecoveryReason, key=lambda item: item.value)
        )
        audit_availability_authority = self._audit_availability_authority_snapshot()
        audit_replacement_capture_authority = self._audit_replacement_capture_authority_snapshot()
        audit_mismatch_cause_counts = tuple(
            (cause, self._audit_mismatch_cause_counts[cause])
            for cause in sorted(AuditMismatchCause, key=lambda item: item.value)
        )
        audit_start_trigger_counts = tuple(
            (trigger, self._audit_start_trigger_counts[trigger])
            for trigger in sorted(AuditTriggerReason, key=lambda item: item.value)
        )
        audit_outcome_trigger_counts = tuple(
            (trigger, outcome, self._audit_outcome_trigger_counts[(trigger, outcome)])
            for trigger in sorted(AuditTriggerReason, key=lambda item: item.value)
            for outcome in sorted(AuditOutcome, key=lambda item: item.value)
        )
        audit_divergence_pending_records = self.audit_divergence_pending_records
        audit_divergences_pending = len(audit_divergence_pending_records)
        audit_divergences_resolved_same_generation = 0
        audit_divergence_lifecycle_reconciled = (
            self._audit_divergences_observed == self._rest_audit_mismatches
            and self._audit_divergences_observed
            == (
                self._audit_divergences_resolved_by_authoritative_resnapshot
                + audit_divergences_resolved_same_generation
                + audit_divergences_pending
            )
        )
        audit_faults_current = len(self._current_audit_faults)
        audit_fault_current_instrument_ids = tuple(
            sorted(
                self._audit_fault_current_count_by_instrument,
                key=lambda item: item.value,
            )
        )
        audit_fault_current_representatives = tuple(
            self._latest_current_audit_fault_by_instrument[instrument_id]
            for instrument_id in audit_fault_current_instrument_ids[
                :MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
            ]
        )
        audit_fault_scope_block_instrument_ids = set(
            self._audit_fault_scope_block_count_by_instrument
        )
        audit_http_endpoint_snapshots = self._audit_http_endpoint_snapshots()
        audit_fault_lifecycle_reconciled = (
            self._audit_faults_observed
            == (
                audit_faults_current
                + self._audit_faults_resolved
                + len(self._permanent_audit_faults)
            )
            and self._audit_faults_permanent == len(self._permanent_audit_faults)
            and sum(self._audit_fault_current_count_by_instrument.values()) == audit_faults_current
            and set(audit_fault_current_instrument_ids)
            == set(self._latest_current_audit_fault_by_instrument)
            and all(count > 0 for count in self._audit_fault_current_count_by_instrument.values())
            and set(audit_fault_current_instrument_ids).issubset(
                audit_fault_scope_block_instrument_ids
            )
            and all(
                count > 0 for count in self._audit_fault_scope_block_count_by_instrument.values()
            )
            and audit_fault_scope_block_instrument_ids.issubset(self._audit_ineligible_instruments)
            and self._audit_http_faults_observed
            == sum(item.observed for item in audit_http_endpoint_snapshots)
            and self._audit_http_faults_resolved
            == sum(item.resolved for item in audit_http_endpoint_snapshots)
            and self._audit_http_faults_permanent
            == sum(item.permanent for item in audit_http_endpoint_snapshots)
            and self._audit_http_retry_exhaustions_observed
            == sum(item.retry_exhaustions_observed for item in audit_http_endpoint_snapshots)
            and self._audit_http_retry_exhaustions_resolved
            == sum(item.retry_exhaustions_resolved for item in audit_http_endpoint_snapshots)
            and self._audit_http_retry_exhaustions_permanent
            == sum(item.retry_exhaustions_permanent for item in audit_http_endpoint_snapshots)
        )
        audit_fault_lifecycle_representatives = tuple(
            self._latest_audit_fault_by_status[status]
            for status in sorted(self._latest_audit_fault_by_status, key=lambda item: item.value)
        )
        reported_top_transition_entry_counts = tuple(
            (source, self._reported_top_transition_entry_counts[source])
            for source in sorted(
                ReportedTopTransitionEntrySource,
                key=lambda item: item.value,
            )
        )
        reported_top_transition_reason_counts = tuple(
            (reason, self._reported_top_transition_reason_counts[reason])
            for reason in sorted(
                ReportedTopTransitionReason,
                key=lambda item: item.value,
            )
        )
        reported_top_transitions = self.reported_top_transitions
        reported_top_transition_outcome_count = (
            self._reported_top_transitions_converged
            + self._reported_top_transitions_superseded
            + self._reported_top_transitions_resnapshotted
            + self._reported_top_transitions_expired
            + self._reported_top_transitions_invalidated
        )
        audit_ineligible_instruments = self.audit_ineligible_instruments
        books = tuple(
            ProcessorBookOperationalSnapshot(
                instrument_id=owner.instrument_id,
                market_id=owner.market_id,
                generation=owner.generation,
                health=owner.health,
                invalidation_reason=owner.invalidation_reason,
                market_version_id=owner.market_version_id,
                precision_rule_version_id=owner.precision_rule_version_id,
                cost_rule_set_version_id=owner.cost_rule_set_version_id,
            )
            for owner in self.owners
        )
        latest_resolution = self._latest_audit_divergence_resolution
        retention = (
            self._retention_snapshot(
                ProcessorRetentionDomain.SCAN_RESULTS,
                history=self._scan_results,
                total=self._scan_count,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.VALIDATED_OPPORTUNITY_HASHES,
                history=self._validated_opportunity_hashes,
                total=self._validated_opportunity_count,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.REFERENCE_REFRESH_REQUESTS,
                history=self._reference_refresh_requests,
                total=self._reference_refresh_count,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.AUDIT_OUTCOMES,
                history=self._audit_outcomes,
                total=self._rest_audits,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.AUDIT_MISMATCH_DIAGNOSTICS,
                history=self._audit_mismatch_diagnostics,
                total=self._audit_mismatch_diagnostic_count,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.AUDIT_DIVERGENCE_RESOLUTIONS,
                history=self._audit_divergence_resolutions,
                total=self._audit_divergences_resolved_by_authoritative_resnapshot,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES,
                history=self._audit_fault_lifecycle_records,
                total=self._audit_faults_observed + self._audit_faults_resolved,
            ),
            self._retention_snapshot(
                ProcessorRetentionDomain.REPORTED_TOP_TRANSITION_OUTCOMES,
                history=self._reported_top_transition_outcomes,
                total=reported_top_transition_outcome_count,
            ),
        )
        return ProcessorOperationalSnapshot(
            processed_raw_messages=self._processed,
            parse_failures=self._parse_failures,
            canonical_events=self._canonical_events,
            reference_updates=self._reference_updates,
            scans=self._scan_count,
            scan_disposition_counts=tuple(
                (disposition, self._scan_disposition_counts[disposition])
                for disposition in sorted(ScanDisposition, key=lambda item: item.value)
            ),
            candidates=self._candidate_count,
            validated_opportunities=self._validated_opportunity_count,
            invalidations=self._invalidations,
            resyncs=self._resyncs,
            recovery_reason_counts=recovery_reason_counts,
            rest_audits=self._rest_audits,
            rest_audit_matches=self._rest_audit_matches,
            rest_audit_mismatches=self._rest_audit_mismatches,
            rest_audit_raced=self._rest_audit_raced,
            rest_audit_failed=self._rest_audit_failed,
            audit_start_generation_rebases=self._audit_start_generation_rebases,
            audit_mismatch_cause_counts=audit_mismatch_cause_counts,
            audit_start_trigger_counts=audit_start_trigger_counts,
            audit_outcome_trigger_counts=audit_outcome_trigger_counts,
            audit_divergences_observed=self._audit_divergences_observed,
            audit_divergences_resolved_by_authoritative_resnapshot=(
                self._audit_divergences_resolved_by_authoritative_resnapshot
            ),
            audit_divergences_resolved_same_generation=(audit_divergences_resolved_same_generation),
            audit_divergences_pending=audit_divergences_pending,
            audit_divergences_unresolved=audit_divergences_pending,
            audit_divergences_unknown=self._audit_mismatch_cause_counts[AuditMismatchCause.UNKNOWN],
            audit_local_book_defects=self._audit_mismatch_cause_counts[
                AuditMismatchCause.LOCAL_BOOK_DEFECT
            ],
            audit_divergence_lifecycle_reconciled=audit_divergence_lifecycle_reconciled,
            audit_availability_authority=audit_availability_authority,
            audit_faults_observed=self._audit_faults_observed,
            audit_faults_current=audit_faults_current,
            audit_faults_resolved=self._audit_faults_resolved,
            audit_faults_permanent=self._audit_faults_permanent,
            audit_http_faults_observed=self._audit_http_faults_observed,
            audit_http_faults_resolved=self._audit_http_faults_resolved,
            audit_http_faults_permanent=self._audit_http_faults_permanent,
            audit_http_retry_exhaustions_observed=(self._audit_http_retry_exhaustions_observed),
            audit_http_retry_exhaustions_resolved=(self._audit_http_retry_exhaustions_resolved),
            audit_http_retry_exhaustions_permanent=(self._audit_http_retry_exhaustions_permanent),
            audit_http_endpoint_snapshots=audit_http_endpoint_snapshots,
            audit_fault_lifecycle_reconciled=audit_fault_lifecycle_reconciled,
            audit_replacement_captures_pending=len(self._audit_replacement_captures),
            audit_replacement_capture_authority=audit_replacement_capture_authority,
            pending_rest_audits=len(self._pending_rest_audits),
            audit_ineligible_books=len(audit_ineligible_instruments),
            reported_top_transitions_started=self._reported_top_transitions_started,
            reported_top_transitions_converged=self._reported_top_transitions_converged,
            reported_top_transitions_superseded=self._reported_top_transitions_superseded,
            reported_top_transitions_resnapshotted=self._reported_top_transitions_resnapshotted,
            reported_top_transitions_expired=self._reported_top_transitions_expired,
            reported_top_transitions_invalidated=self._reported_top_transitions_invalidated,
            reported_top_transition_frames=self._reported_top_transition_frames,
            reported_top_transition_duration_total_ns=(
                self._reported_top_transition_duration_total_ns
            ),
            reported_top_transition_duration_max_ns=(self._reported_top_transition_duration_max_ns),
            reported_top_transition_duration_lower_bound_count=(
                self._reported_top_transition_duration_lower_bound_count
            ),
            reported_top_transition_entry_counts=reported_top_transition_entry_counts,
            reported_top_transition_reason_counts=reported_top_transition_reason_counts,
            reported_top_transition_research_eligibility_restored=(
                self._reported_top_transition_research_eligibility_restored
            ),
            reported_top_no_pending_recognized=self._reported_top_no_pending_recognized,
            reported_top_no_pending_rejected=self._reported_top_no_pending_rejected,
            pending_reported_top_transitions=len(reported_top_transitions),
            reference_refresh_requests=self._reference_refresh_count,
            healthy_books=sum(book.health is BookHealth.HEALTHY for book in books),
            raw_input_chain_sha256=self._raw_input_chain.hex(),
            canonical_event_chain_sha256=self._canonical_event_chain.hex(),
            error_chain_sha256=self._error_chain.hex(),
            scan_chain_sha256=self._scan_chain.hex(),
            validated_opportunity_chain_sha256=self._validated_opportunity_chain.hex(),
            reference_refresh_chain_sha256=self._reference_refresh_chain.hex(),
            audit_start_chain_sha256=self._audit_start_chain.hex(),
            audit_outcome_chain_sha256=self._audit_outcome_chain.hex(),
            audit_classification_attestation_chain_sha256=(
                self._audit_classification_attestation_chain.hex()
            ),
            audit_divergence_history_chain_sha256=self._audit_divergence_history_chain.hex(),
            audit_fault_lifecycle_chain_sha256=self._audit_fault_lifecycle_chain.hex(),
            reported_top_transition_chain_sha256=self._reported_top_transition_chain.hex(),
            recovery_request_chain_sha256=self._recovery_request_chain.hex(),
            books=books,
            audit_ineligible_instruments=audit_ineligible_instruments,
            audit_divergence_pending_records=audit_divergence_pending_records,
            audit_divergence_resolution_representatives=(
                () if latest_resolution is None else (latest_resolution,)
            ),
            audit_mismatch_diagnostic_representatives=tuple(
                self._latest_audit_mismatch_diagnostics[cause]
                for cause in sorted(
                    self._latest_audit_mismatch_diagnostics,
                    key=lambda item: item.value,
                )
            ),
            audit_fault_current_representative_capacity=(
                MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES
            ),
            audit_fault_current_instrument_ids=audit_fault_current_instrument_ids,
            audit_fault_current_representatives=audit_fault_current_representatives,
            audit_fault_current_representatives_truncated=(
                audit_faults_current > len(audit_fault_current_representatives)
            ),
            audit_fault_lifecycle_representatives=audit_fault_lifecycle_representatives,
            reported_top_transitions=reported_top_transitions,
            audit_last_successful_match_monotonic_ns=(
                self._audit_last_successful_match_monotonic_ns
            ),
            audit_barrier_lag_frames_max=self._audit_barrier_lag_frames_max,
            audit_race_budget_exhaustions=self._audit_race_budget_exhaustions,
            retention=retention,
        )

    def summary(self) -> ProcessorSummary:
        recovery_reason_counts = tuple(
            (reason, self._recovery_reason_counts[reason])
            for reason in sorted(ProcessorRecoveryReason, key=lambda item: item.value)
        )
        audit_availability_authority = self._audit_availability_authority_snapshot()
        audit_replacement_capture_authority = self._audit_replacement_capture_authority_snapshot()
        snapshots = self.snapshots()
        snapshot_hashes = tuple(
            (owner.instrument_id.value, owner.content_sha256()) for owner in self.owners
        )
        reference_versions = tuple(
            (
                bundle.market.market_id.value,
                tuple(version.value for version in bundle.current_version_ids),
                self._reference_provenance.get(bundle.market.market_id),
            )
            for bundle in self.current_bundles
        )
        opportunity_hashes = tuple(self._validated_opportunity_hashes)
        raw_input_chain_sha256 = self._raw_input_chain.hex()
        canonical_event_chain_sha256 = self._canonical_event_chain.hex()
        error_chain_sha256 = self._error_chain.hex()
        scan_chain_sha256 = self._scan_chain.hex()
        validated_opportunity_chain_sha256 = self._validated_opportunity_chain.hex()
        reference_refresh_chain_sha256 = self._reference_refresh_chain.hex()
        audit_start_chain_sha256 = self._audit_start_chain.hex()
        audit_outcome_chain_sha256 = self._audit_outcome_chain.hex()
        audit_classification_attestation_chain_sha256 = (
            self._audit_classification_attestation_chain.hex()
        )
        audit_divergence_history_chain_sha256 = self._audit_divergence_history_chain.hex()
        audit_fault_lifecycle_chain_sha256 = self._audit_fault_lifecycle_chain.hex()
        audit_compatibility_chain_sha256 = self._audit_compatibility_chain.hex()
        reported_top_transition_chain_sha256 = self._reported_top_transition_chain.hex()
        recovery_request_chain_sha256 = self._recovery_request_chain.hex()
        audit_outcomes = tuple(self._audit_outcomes)
        audit_mismatch_diagnostics = tuple(self._audit_mismatch_diagnostics)
        audit_mismatch_cause_counts = tuple(
            (cause, self._audit_mismatch_cause_counts[cause])
            for cause in sorted(AuditMismatchCause, key=lambda item: item.value)
        )
        audit_start_trigger_counts = tuple(
            (trigger, self._audit_start_trigger_counts[trigger])
            for trigger in sorted(AuditTriggerReason, key=lambda item: item.value)
        )
        audit_outcome_trigger_counts = tuple(
            (trigger, outcome, self._audit_outcome_trigger_counts[(trigger, outcome)])
            for trigger in sorted(AuditTriggerReason, key=lambda item: item.value)
            for outcome in sorted(AuditOutcome, key=lambda item: item.value)
        )
        audit_divergence_pending_records = self.audit_divergence_pending_records
        audit_divergence_resolution_records = tuple(self._audit_divergence_resolutions)
        audit_fault_current_records = tuple(
            record for _, record in sorted(self._current_audit_faults.items())
        )
        audit_fault_scope_blocking_records = tuple(
            record
            for record in (
                *audit_fault_current_records,
                *self._permanent_audit_faults.values(),
            )
            if record.blocks_current_scope
        )
        audit_fault_scope_block_instrument_ids = set(
            self._audit_fault_scope_block_count_by_instrument
        )
        audit_fault_current_instrument_ids = tuple(
            sorted(
                self._audit_fault_current_count_by_instrument,
                key=lambda item: item.value,
            )
        )
        audit_http_endpoint_snapshots = self._audit_http_endpoint_snapshots()
        audit_fault_lifecycle_records = tuple(self._audit_fault_lifecycle_records)
        audit_replacement_capture_state = tuple(
            self._audit_replacement_captures[instrument_id].to_record()
            for instrument_id in sorted(
                self._audit_replacement_captures,
                key=lambda item: item.value,
            )
        )
        audit_divergences_pending = len(audit_divergence_pending_records)
        audit_divergences_resolved_same_generation = 0
        audit_divergence_lifecycle_reconciled = (
            self._audit_divergences_observed == self._rest_audit_mismatches
            and self._audit_divergences_observed
            == (
                self._audit_divergences_resolved_by_authoritative_resnapshot
                + audit_divergences_resolved_same_generation
                + audit_divergences_pending
            )
        )
        audit_fault_lifecycle_reconciled = (
            self._audit_faults_observed
            == (
                len(audit_fault_current_records)
                + self._audit_faults_resolved
                + len(self._permanent_audit_faults)
            )
            and self._audit_faults_permanent == len(self._permanent_audit_faults)
            and sum(self._audit_fault_current_count_by_instrument.values())
            == len(audit_fault_current_records)
            and set(audit_fault_current_instrument_ids)
            == set(self._latest_current_audit_fault_by_instrument)
            and set(audit_fault_current_instrument_ids)
            == {record.instrument_id for record in audit_fault_current_records}
            and sum(self._audit_fault_scope_block_count_by_instrument.values())
            == len(audit_fault_scope_blocking_records)
            and audit_fault_scope_block_instrument_ids
            == {record.instrument_id for record in audit_fault_scope_blocking_records}
            and audit_fault_scope_block_instrument_ids.issubset(self._audit_ineligible_instruments)
            and self._audit_http_faults_observed
            == sum(item.observed for item in audit_http_endpoint_snapshots)
            and self._audit_http_faults_resolved
            == sum(item.resolved for item in audit_http_endpoint_snapshots)
            and self._audit_http_faults_permanent
            == sum(item.permanent for item in audit_http_endpoint_snapshots)
            and self._audit_http_retry_exhaustions_observed
            == sum(item.retry_exhaustions_observed for item in audit_http_endpoint_snapshots)
            and self._audit_http_retry_exhaustions_resolved
            == sum(item.retry_exhaustions_resolved for item in audit_http_endpoint_snapshots)
            and self._audit_http_retry_exhaustions_permanent
            == sum(item.retry_exhaustions_permanent for item in audit_http_endpoint_snapshots)
        )
        reported_top_transition_entry_counts = tuple(
            (source, self._reported_top_transition_entry_counts[source])
            for source in sorted(
                ReportedTopTransitionEntrySource,
                key=lambda item: item.value,
            )
        )
        reported_top_transition_reason_counts = tuple(
            (reason, self._reported_top_transition_reason_counts[reason])
            for reason in sorted(
                ReportedTopTransitionReason,
                key=lambda item: item.value,
            )
        )
        reported_top_transitions = self.reported_top_transitions
        reported_top_transition_outcomes = tuple(self._reported_top_transition_outcomes)
        audit_ineligible_instrument_ids = tuple(
            item.value for item in self.audit_ineligible_instruments
        )
        audit_ineligible_books = len(audit_ineligible_instrument_ids)
        lifecycles = self._lifecycle_tracker.snapshot()
        lifecycle_state_sha256 = canonical_sha256(lifecycles)
        healthy_books = sum(snapshot.health is BookHealth.HEALTHY for snapshot in snapshots)
        scan_disposition_counts = tuple(
            (disposition, self._scan_disposition_counts[disposition])
            for disposition in sorted(ScanDisposition, key=lambda item: item.value)
        )
        pending_request_ids = set(self._pending_rest_audits)
        if (
            not set(self._pending_rest_audit_start_sources) == pending_request_ids
            or not set(self._pending_rest_audit_responses).issubset(pending_request_ids)
            or not set(self._pending_rest_audit_classification_preparations).issubset(
                pending_request_ids
            )
            or not self._prepare_attest_rest_audit_requests.issubset(pending_request_ids)
        ):
            raise ProgrammingInvariantViolation(
                "pending REST audit classification state is not reconciled",
                component="canonical-processor",
            )
        pending_rest_audit_state = tuple(
            {
                "classification_preparation": (
                    None
                    if (
                        preparation := self._pending_rest_audit_classification_preparations.get(
                            request_id
                        )
                    )
                    is None
                    else {
                        "command": preparation.command,
                        "envelope": preparation.envelope.to_record(),
                        "prepared_state": {
                            "audit_fault_current_count": (preparation.audit_fault_current_count),
                            "audit_fault_scope_block_count": (
                                preparation.audit_fault_scope_block_count
                            ),
                            "audit_ineligible_nonfault": (preparation.audit_ineligible_nonfault),
                            "book": preparation.book,
                            "owner_health": preparation.owner_health,
                            "processed_watermark": preparation.processed_watermark,
                            "reported_top_transition": (preparation.reported_top_transition),
                        },
                    }
                ),
                "classification_protocol": (
                    AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL
                    if request_id in self._prepare_attest_rest_audit_requests
                    else None
                ),
                "hard_expiry_monotonic_ns": self._pending_rest_audit_hard_expiries.get(request_id),
                "request_id": request_id,
                "response": (
                    None
                    if (response := self._pending_rest_audit_responses.get(request_id)) is None
                    else response.to_record()
                ),
                "response_deadline_monotonic_ns": (
                    self._pending_rest_audit_response_deadlines.get(request_id)
                ),
                "start": self._pending_rest_audits[request_id],
                "start_source": self._pending_rest_audit_start_sources[request_id],
            }
            for request_id in sorted(self._pending_rest_audits)
        )
        transition_resync_state = tuple(
            instrument_id.value
            for instrument_id in sorted(
                self._transition_resync_instruments,
                key=lambda item: item.value,
            )
        )
        post_snapshot_baseline_state = tuple(
            self._post_snapshot_baselines[market_id]
            for market_id in sorted(
                self._post_snapshot_baselines,
                key=lambda item: item.value,
            )
        )
        audit_economic_generation_state = tuple(
            (instrument_id.value, self._audit_economic_generations[instrument_id])
            for instrument_id in sorted(
                self._audit_economic_generations,
                key=lambda item: item.value,
            )
        )
        terminal_hash = canonical_sha256(
            {
                "schema": _TERMINAL_HASH_SCHEMA,
                "config_hash": self._config.config_hash.value,
                "v1_audit_generation_rebase_enabled": (self._allow_v1_audit_generation_rebase),
                "counters": {
                    "candidates": self._candidate_count,
                    "canonical_events": self._canonical_events,
                    "healthy_books": healthy_books,
                    "invalidations": self._invalidations,
                    "parse_failures": self._parse_failures,
                    "processed_raw_messages": self._processed,
                    "reference_refresh_requests": self._reference_refresh_count,
                    "reference_updates": self._reference_updates,
                    "rest_audit_failed": self._rest_audit_failed,
                    "audit_start_generation_rebases": (self._audit_start_generation_rebases),
                    "audit_start_triggers": audit_start_trigger_counts,
                    "audit_outcome_triggers": audit_outcome_trigger_counts,
                    "audit_mismatch_causes": audit_mismatch_cause_counts,
                    "audit_divergences_observed": self._audit_divergences_observed,
                    "audit_divergences_resolved_by_authoritative_resnapshot": (
                        self._audit_divergences_resolved_by_authoritative_resnapshot
                    ),
                    "audit_divergences_resolved_same_generation": (
                        audit_divergences_resolved_same_generation
                    ),
                    "audit_divergences_pending": audit_divergences_pending,
                    "audit_replacement_captures_pending": len(audit_replacement_capture_state),
                    "audit_divergence_lifecycle_reconciled": (
                        audit_divergence_lifecycle_reconciled
                    ),
                    "audit_faults_observed": self._audit_faults_observed,
                    "audit_faults_current": len(audit_fault_current_records),
                    "audit_faults_resolved": self._audit_faults_resolved,
                    "audit_faults_permanent": self._audit_faults_permanent,
                    "audit_fault_lifecycle_reconciled": audit_fault_lifecycle_reconciled,
                    "audit_http_faults_observed": self._audit_http_faults_observed,
                    "audit_http_faults_resolved": self._audit_http_faults_resolved,
                    "audit_http_faults_permanent": self._audit_http_faults_permanent,
                    "audit_http_retry_exhaustions_observed": (
                        self._audit_http_retry_exhaustions_observed
                    ),
                    "audit_http_retry_exhaustions_resolved": (
                        self._audit_http_retry_exhaustions_resolved
                    ),
                    "audit_http_retry_exhaustions_permanent": (
                        self._audit_http_retry_exhaustions_permanent
                    ),
                    "rest_audit_matches": self._rest_audit_matches,
                    "rest_audit_mismatches": self._rest_audit_mismatches,
                    "rest_audit_raced": self._rest_audit_raced,
                    "rest_audits": self._rest_audits,
                    "pending_rest_audits": len(self._pending_rest_audits),
                    "staged_rest_audit_responses": len(self._pending_rest_audit_responses),
                    "audit_ineligible_books": audit_ineligible_books,
                    "pending_reported_top_transitions": len(reported_top_transitions),
                    "reported_top_no_pending_recognized": (
                        self._reported_top_no_pending_recognized
                    ),
                    "reported_top_no_pending_rejected": self._reported_top_no_pending_rejected,
                    "reported_top_transition_duration_max_ns": (
                        self._reported_top_transition_duration_max_ns
                    ),
                    "reported_top_transition_duration_total_ns": (
                        self._reported_top_transition_duration_total_ns
                    ),
                    "reported_top_transition_duration_lower_bound_count": (
                        self._reported_top_transition_duration_lower_bound_count
                    ),
                    "reported_top_transition_entry_counts": (reported_top_transition_entry_counts),
                    "reported_top_transition_frames": self._reported_top_transition_frames,
                    "reported_top_transition_reason_counts": (
                        reported_top_transition_reason_counts
                    ),
                    "reported_top_transition_research_eligibility_restored": (
                        self._reported_top_transition_research_eligibility_restored
                    ),
                    "reported_top_transitions_converged": (
                        self._reported_top_transitions_converged
                    ),
                    "reported_top_transitions_expired": self._reported_top_transitions_expired,
                    "reported_top_transitions_invalidated": (
                        self._reported_top_transitions_invalidated
                    ),
                    "reported_top_transitions_resnapshotted": (
                        self._reported_top_transitions_resnapshotted
                    ),
                    "reported_top_transitions_started": self._reported_top_transitions_started,
                    "reported_top_transitions_superseded": (
                        self._reported_top_transitions_superseded
                    ),
                    "resyncs": self._resyncs,
                    "recovery_reasons": recovery_reason_counts,
                    "scans": self._scan_count,
                    "scan_dispositions": scan_disposition_counts,
                    "validated_opportunities": self._validated_opportunity_count,
                    "unique_opportunity_lifecycles": len(lifecycles),
                },
                "history_chains": {
                    "canonical_event": canonical_event_chain_sha256,
                    "expected_data_error": error_chain_sha256,
                    "raw_input": raw_input_chain_sha256,
                    "reference_refresh": reference_refresh_chain_sha256,
                    "rest_audit_start": audit_start_chain_sha256,
                    "rest_audit_outcome": audit_outcome_chain_sha256,
                    "rest_audit_classification_attestation": (
                        audit_classification_attestation_chain_sha256
                    ),
                    "rest_audit_divergence": audit_divergence_history_chain_sha256,
                    "rest_audit_fault_lifecycle": audit_fault_lifecycle_chain_sha256,
                    "rest_audit_compatibility": audit_compatibility_chain_sha256,
                    "reported_top_transition": reported_top_transition_chain_sha256,
                    "processor_recovery_request": recovery_request_chain_sha256,
                    "scan": scan_chain_sha256,
                    "validated_opportunity": validated_opportunity_chain_sha256,
                },
                "reference_versions": reference_versions,
                "lifecycle_state_sha256": lifecycle_state_sha256,
                "audit_ineligible_instrument_ids": audit_ineligible_instrument_ids,
                "audit_divergences_pending": audit_divergence_pending_records,
                "audit_faults_current": audit_fault_current_records,
                "audit_replacement_captures": audit_replacement_capture_state,
                "audit_economic_generations": audit_economic_generation_state,
                "pending_rest_audits": pending_rest_audit_state,
                "reported_top_transitions": reported_top_transitions,
                "reported_top_transition_resync_instruments": transition_resync_state,
                "post_snapshot_baselines": post_snapshot_baseline_state,
                "rest_audit_protocol_mode": self._rest_audit_protocol_mode,
                "rest_audit_prepare_attest_required": (self._rest_audit_prepare_attest_required),
                "rest_audit_run_deadline": (
                    None
                    if self._rest_audit_run_deadline_ns is None
                    else {
                        "epoch_id": self._rest_audit_run_deadline_epoch_id,
                        "monotonic_ns": self._rest_audit_run_deadline_ns,
                    }
                ),
                "snapshot_hashes": snapshot_hashes,
            }
        )
        return ProcessorSummary(
            processed_raw_messages=self._processed,
            parse_failures=self._parse_failures,
            canonical_events=self._canonical_events,
            reference_updates=self._reference_updates,
            scans=self._scan_count,
            scan_disposition_counts=scan_disposition_counts,
            candidates=self._candidate_count,
            validated_opportunities=self._validated_opportunity_count,
            invalidations=self._invalidations,
            resyncs=self._resyncs,
            recovery_reason_counts=recovery_reason_counts,
            rest_audits=self._rest_audits,
            rest_audit_matches=self._rest_audit_matches,
            rest_audit_mismatches=self._rest_audit_mismatches,
            rest_audit_raced=self._rest_audit_raced,
            rest_audit_failed=self._rest_audit_failed,
            audit_start_generation_rebases=self._audit_start_generation_rebases,
            audit_mismatch_cause_counts=audit_mismatch_cause_counts,
            audit_start_trigger_counts=audit_start_trigger_counts,
            audit_outcome_trigger_counts=audit_outcome_trigger_counts,
            audit_divergences_observed=self._audit_divergences_observed,
            audit_divergences_resolved_by_authoritative_resnapshot=(
                self._audit_divergences_resolved_by_authoritative_resnapshot
            ),
            audit_divergences_resolved_same_generation=(audit_divergences_resolved_same_generation),
            audit_divergences_pending=audit_divergences_pending,
            audit_divergences_unresolved=audit_divergences_pending,
            audit_divergences_unknown=self._audit_mismatch_cause_counts[AuditMismatchCause.UNKNOWN],
            audit_local_book_defects=self._audit_mismatch_cause_counts[
                AuditMismatchCause.LOCAL_BOOK_DEFECT
            ],
            audit_divergence_lifecycle_reconciled=(audit_divergence_lifecycle_reconciled),
            audit_availability_authority=audit_availability_authority,
            audit_faults_observed=self._audit_faults_observed,
            audit_faults_current=len(audit_fault_current_records),
            audit_faults_resolved=self._audit_faults_resolved,
            audit_faults_permanent=self._audit_faults_permanent,
            audit_http_faults_observed=self._audit_http_faults_observed,
            audit_http_faults_resolved=self._audit_http_faults_resolved,
            audit_http_faults_permanent=self._audit_http_faults_permanent,
            audit_http_retry_exhaustions_observed=(self._audit_http_retry_exhaustions_observed),
            audit_http_retry_exhaustions_resolved=(self._audit_http_retry_exhaustions_resolved),
            audit_http_retry_exhaustions_permanent=(self._audit_http_retry_exhaustions_permanent),
            audit_http_endpoint_snapshots=audit_http_endpoint_snapshots,
            audit_fault_lifecycle_reconciled=audit_fault_lifecycle_reconciled,
            audit_replacement_captures_pending=len(audit_replacement_capture_state),
            audit_replacement_capture_authority=audit_replacement_capture_authority,
            audit_fault_current_instrument_ids=audit_fault_current_instrument_ids,
            pending_rest_audits=len(self._pending_rest_audits),
            audit_ineligible_books=audit_ineligible_books,
            reported_top_transitions_started=self._reported_top_transitions_started,
            reported_top_transitions_converged=self._reported_top_transitions_converged,
            reported_top_transitions_superseded=self._reported_top_transitions_superseded,
            reported_top_transitions_resnapshotted=self._reported_top_transitions_resnapshotted,
            reported_top_transitions_expired=self._reported_top_transitions_expired,
            reported_top_transitions_invalidated=self._reported_top_transitions_invalidated,
            reported_top_transition_frames=self._reported_top_transition_frames,
            reported_top_transition_duration_total_ns=(
                self._reported_top_transition_duration_total_ns
            ),
            reported_top_transition_duration_max_ns=(self._reported_top_transition_duration_max_ns),
            reported_top_transition_duration_lower_bound_count=(
                self._reported_top_transition_duration_lower_bound_count
            ),
            reported_top_transition_entry_counts=reported_top_transition_entry_counts,
            reported_top_transition_reason_counts=reported_top_transition_reason_counts,
            reported_top_transition_research_eligibility_restored=(
                self._reported_top_transition_research_eligibility_restored
            ),
            reported_top_no_pending_recognized=self._reported_top_no_pending_recognized,
            reported_top_no_pending_rejected=self._reported_top_no_pending_rejected,
            pending_reported_top_transitions=len(reported_top_transitions),
            reference_refresh_requests=self._reference_refresh_count,
            unique_opportunity_lifecycles=len(lifecycles),
            healthy_books=healthy_books,
            raw_input_chain_sha256=raw_input_chain_sha256,
            canonical_event_chain_sha256=canonical_event_chain_sha256,
            error_chain_sha256=error_chain_sha256,
            scan_chain_sha256=scan_chain_sha256,
            validated_opportunity_chain_sha256=validated_opportunity_chain_sha256,
            reference_refresh_chain_sha256=reference_refresh_chain_sha256,
            audit_start_chain_sha256=audit_start_chain_sha256,
            audit_outcome_chain_sha256=audit_outcome_chain_sha256,
            audit_classification_attestation_chain_sha256=(
                audit_classification_attestation_chain_sha256
            ),
            audit_divergence_history_chain_sha256=(audit_divergence_history_chain_sha256),
            audit_fault_lifecycle_chain_sha256=audit_fault_lifecycle_chain_sha256,
            reported_top_transition_chain_sha256=reported_top_transition_chain_sha256,
            recovery_request_chain_sha256=recovery_request_chain_sha256,
            lifecycle_state_sha256=lifecycle_state_sha256,
            scan_results_truncated=self._scan_count > len(self._scan_results),
            opportunity_hashes_truncated=(
                self._validated_opportunity_count > len(self._validated_opportunity_hashes)
            ),
            reference_refresh_requests_truncated=(
                self._reference_refresh_count > len(self._reference_refresh_requests)
            ),
            audit_outcomes_truncated=self._rest_audits > len(self._audit_outcomes),
            audit_mismatch_diagnostics_truncated=(
                self._audit_mismatch_diagnostic_count > len(self._audit_mismatch_diagnostics)
            ),
            audit_divergence_resolution_records_truncated=(
                self._audit_divergences_resolved_by_authoritative_resnapshot
                > len(self._audit_divergence_resolutions)
            ),
            audit_fault_lifecycle_records_truncated=(
                self._audit_faults_observed + self._audit_faults_resolved
                > len(self._audit_fault_lifecycle_records)
            ),
            reported_top_transition_outcomes_truncated=(
                (
                    self._reported_top_transitions_converged
                    + self._reported_top_transitions_superseded
                    + self._reported_top_transitions_resnapshotted
                    + self._reported_top_transitions_expired
                    + self._reported_top_transitions_invalidated
                )
                > len(self._reported_top_transition_outcomes)
            ),
            terminal_hash_schema=_TERMINAL_HASH_SCHEMA,
            terminal_hash=terminal_hash,
            opportunity_hashes=opportunity_hashes,
            audit_outcomes=audit_outcomes,
            audit_mismatch_diagnostics=audit_mismatch_diagnostics,
            audit_divergence_pending_records=audit_divergence_pending_records,
            audit_divergence_resolution_records=audit_divergence_resolution_records,
            audit_fault_current_records=audit_fault_current_records,
            audit_fault_lifecycle_records=audit_fault_lifecycle_records,
            reported_top_transitions=reported_top_transitions,
            reported_top_transition_outcomes=reported_top_transition_outcomes,
        )


__all__ = [
    "MAXIMUM_ACTIVE_AUDIT_FAULTS",
    "MAXIMUM_AUDIT_FAULT_CURRENT_REPRESENTATIVES",
    "AnalyticalOutputSink",
    "AuditDivergenceEligibilityRestoration",
    "BookFreshnessUpdate",
    "CanonicalProcessor",
    "MarketScanTrigger",
    "ProcessedEnvelope",
    "ProcessorAuditHttpEndpointSnapshot",
    "ProcessorAuditReplacementCaptureSnapshot",
    "ProcessorAuditReplacementSnapshotSource",
    "ProcessorBookOperationalSnapshot",
    "ProcessorOperationalSnapshot",
    "ProcessorRecoveryReason",
    "ProcessorRecoveryRequest",
    "ProcessorRetentionDomain",
    "ProcessorRetentionSnapshot",
    "ProcessorSummary",
    "ReferenceRefreshRequest",
    "ScanTriggerReason",
    "processor_recovery_reason_for_invalidation",
]
