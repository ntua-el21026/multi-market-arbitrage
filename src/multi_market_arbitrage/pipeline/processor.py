"""One deterministic parser-to-state-to-scanner path for observation and replay."""

from __future__ import annotations

import hashlib
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Protocol, cast

from multi_market_arbitrage.core.config import Phase1Config
from multi_market_arbitrage.core.errors import ProgrammingInvariantViolation
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    VersionId,
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
    CanonicalEvent,
    CanonicalEventType,
    MarketResolvedEvent,
    NewMarketEvent,
    NormalizationContext,
    NormalizationError,
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
from multi_market_arbitrage.venues.polymarket.errors import PolymarketSchemaError
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
    AUDIT_CLASSIFICATION_SCHEMA,
    AUDIT_EXPIRY_SCHEMA,
    AUDIT_FAILURE_SCHEMA,
    AUDIT_START_SCHEMA,
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditExpiryCommand,
    AuditFailureReason,
    AuditOutcome,
    AuditOutcomeEvidence,
    AuditStartEvidence,
    WebSocketAdmissionWatermark,
)
from .channels import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_EXPIRED,
    CONTROL_REST_AUDIT_FAILED,
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

_RECENT_HISTORY_LIMIT = 4096
_CHAIN_NAMESPACE = b"mma-canonical-processor-chain-v1\0"


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
    market_id: MarketId | None
    native_market_id: str
    native_condition_id: str | None
    native_asset_ids: tuple[str, ...]


class ScanTriggerReason(StrEnum):
    """Authoritative transition categories that make a market scan-dirty."""

    BOOK_UPDATE = "BOOK_UPDATE"
    REST_SNAPSHOT = "REST_SNAPSHOT"
    REST_AUDIT = "REST_AUDIT"
    REFERENCE = "REFERENCE"
    LIFECYCLE = "LIFECYCLE"
    INVALIDATION = "INVALIDATION"


class _RestAuditProtocolMode(StrEnum):
    LEGACY = "LEGACY"
    DEFERRED_BOUND = "DEFERRED_BOUND"


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
    audit_outcomes: tuple[AuditOutcomeEvidence, ...] = ()
    scan_source_received_monotonic_ns: int | None = None


@dataclass(frozen=True, slots=True)
class _RawReferenceSource:
    ingest_id: IngestId
    payload_sha256: str


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
    rest_audits: int
    rest_audit_matches: int
    rest_audit_mismatches: int
    rest_audit_raced: int
    rest_audit_failed: int
    pending_rest_audits: int
    audit_ineligible_books: int
    reference_refresh_requests: int
    unique_opportunity_lifecycles: int
    healthy_books: int
    raw_input_chain_sha256: str
    canonical_event_chain_sha256: str
    error_chain_sha256: str
    scan_chain_sha256: str
    validated_opportunity_chain_sha256: str
    reference_refresh_chain_sha256: str
    audit_outcome_chain_sha256: str
    lifecycle_state_sha256: str
    scan_results_truncated: bool
    opportunity_hashes_truncated: bool
    reference_refresh_requests_truncated: bool
    audit_outcomes_truncated: bool
    terminal_hash: str
    opportunity_hashes: tuple[str, ...]
    audit_outcomes: tuple[AuditOutcomeEvidence, ...]


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
        self._scan_results: deque[CompleteSetScanResult] = deque(maxlen=_RECENT_HISTORY_LIMIT)
        self._validated_opportunity_hashes: deque[str] = deque(maxlen=_RECENT_HISTORY_LIMIT)
        self._reference_refresh_requests: deque[ReferenceRefreshRequest] = deque(
            maxlen=_RECENT_HISTORY_LIMIT
        )
        self._audit_outcomes: deque[AuditOutcomeEvidence] = deque(maxlen=_RECENT_HISTORY_LIMIT)
        self._pending_rest_audits: dict[str, AuditStartEvidence] = {}
        self._pending_rest_audit_responses: dict[str, RawEventEnvelope] = {}
        self._pending_rest_audit_hard_expiries: dict[str, int] = {}
        self._pending_rest_audit_response_deadlines: dict[str, int] = {}
        self._rest_audit_protocol_mode: _RestAuditProtocolMode | None = None
        self._rest_audit_run_deadline_epoch_id: str | None = None
        self._rest_audit_run_deadline_ns: int | None = None
        self._audit_ineligible_instruments: set[InstrumentId] = set()
        self._audit_economic_generations: dict[InstrumentId, int] = {}
        self._processed_ws_watermarks: dict[ConnectionId, int] = {}
        self._active_refresh_requests: list[ReferenceRefreshRequest] | None = None
        self._active_dirty_markets: dict[MarketId, set[ScanTriggerReason]] | None = None
        self._active_freshness_updates: dict[InstrumentId, BookFreshnessUpdate] | None = None
        self._active_received_monotonic: MonotonicTimestamp | None = None
        self._active_scan_observed_at: UtcTimestamp | None = None
        self._active_audit_outcomes: list[AuditOutcomeEvidence] | None = None
        self._last_accepted_l2_monotonic: dict[InstrumentId, MonotonicTimestamp] = {}
        self._raw_input_chain = _chain_seed("raw-input")
        self._canonical_event_chain = _chain_seed("canonical-event")
        self._error_chain = _chain_seed("expected-data-error")
        self._scan_chain = _chain_seed("scan")
        self._validated_opportunity_chain = _chain_seed("validated-opportunity")
        self._reference_refresh_chain = _chain_seed("reference-refresh")
        self._audit_outcome_chain = _chain_seed("rest-audit-outcome")
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
        self._rest_audits = 0
        self._rest_audit_matches = 0
        self._rest_audit_mismatches = 0
        self._rest_audit_raced = 0
        self._rest_audit_failed = 0

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
    def audit_ineligible_instruments(self) -> tuple[InstrumentId, ...]:
        """Books excluded from opportunity eligibility after an unproven audit."""

        return tuple(sorted(self._audit_ineligible_instruments, key=lambda item: item.value))

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

    def _process_preserved(self, envelope: RawEventEnvelope) -> ProcessedEnvelope:
        if (
            self._active_refresh_requests is not None
            or self._active_dirty_markets is not None
            or self._active_freshness_updates is not None
            or self._active_received_monotonic is not None
            or self._active_scan_observed_at is not None
            or self._active_audit_outcomes is not None
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
        current_refresh_requests: list[ReferenceRefreshRequest] = []
        current_dirty_markets: dict[MarketId, set[ScanTriggerReason]] = {}
        current_freshness_updates: dict[InstrumentId, BookFreshnessUpdate] = {}
        current_audit_outcomes: list[AuditOutcomeEvidence] = []
        self._active_refresh_requests = current_refresh_requests
        self._active_dirty_markets = current_dirty_markets
        self._active_freshness_updates = current_freshness_updates
        self._active_received_monotonic = envelope.received_monotonic
        self._active_scan_observed_at = None
        self._active_audit_outcomes = current_audit_outcomes
        try:
            try:
                self._route(envelope)
            except (
                BookStateError,
                NormalizationError,
                PolymarketSchemaError,
                ReferenceBuildError,
            ) as error:
                error_type = type(error).__name__
                self._parse_failures += 1
                self._error_chain = _chain_extend(
                    self._error_chain,
                    "expected-data-error",
                    (envelope.ingest_seq, error_type),
                )
                affected = self.invalidate_all(InvalidationReason.SCHEMA_VIOLATION)
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
                    audit_outcomes=tuple(current_audit_outcomes),
                    scan_source_received_monotonic_ns=(
                        min(audit_response_receive_times) if audit_response_receive_times else None
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
                processed = ProcessedEnvelope(
                    ingest_seq=envelope.ingest_seq,
                    success=True,
                    canonical_event_count=self._canonical_events - before_events,
                    scan_count=self._scan_count - before_scans,
                    needs_resync=bool(unhealthy),
                    affected_instrument_ids=unhealthy,
                    reference_refresh_requests=tuple(current_refresh_requests),
                    book_freshness_updates=tuple(
                        current_freshness_updates[key]
                        for key in sorted(current_freshness_updates, key=lambda item: item.value)
                    ),
                    scan_triggers=scan_triggers,
                    error_type=None,
                    audit_outcomes=tuple(current_audit_outcomes),
                    scan_source_received_monotonic_ns=(
                        min(audit_response_receive_times) if audit_response_receive_times else None
                    ),
                )
        finally:
            self._active_refresh_requests = None
            self._active_dirty_markets = None
            self._active_freshness_updates = None
            self._active_received_monotonic = None
            self._active_scan_observed_at = None
            self._active_audit_outcomes = None
        return processed

    def _record_book_mutation(
        self,
        mutation: BookMutation,
        *,
        reason: ScanTriggerReason,
        freshness_received_monotonic: MonotonicTimestamp | None = None,
    ) -> None:
        if mutation.audit_economic_changed:
            self._audit_economic_generations[mutation.instrument_id] = (
                self._audit_economic_generations.get(mutation.instrument_id, 0) + 1
            )
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
                freshness[mutation.instrument_id] = BookFreshnessUpdate(
                    market_id=mutation.market_id,
                    instrument_id=mutation.instrument_id,
                    received_monotonic=received_monotonic,
                )
            else:
                freshness.pop(mutation.instrument_id, None)

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
            owner = self._owners[rest.instrument_id]
            self._audit_ineligible_instruments.discard(rest.instrument_id)
            was_unhealthy = owner.health is not BookHealth.HEALTHY
            self._prepare_for_rest_snapshot(owner)
            mutation = owner.apply_rest_snapshot(rest)
            self._record_book_mutation(
                mutation,
                reason=ScanTriggerReason.REST_SNAPSHOT,
            )
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
        if channel == CONTROL_REST_AUDIT_CLASSIFY:
            self._classify_staged_rest_audit_response(envelope)
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
            for request_id in tuple(sorted(self._pending_rest_audits)):
                start = self._pending_rest_audits.pop(request_id)
                self._pending_rest_audit_responses.pop(request_id, None)
                self._pending_rest_audit_hard_expiries.pop(request_id, None)
                self._pending_rest_audit_response_deadlines.pop(request_id, None)
                self._record_audit_outcome(
                    self._failed_audit_outcome(
                        start,
                        reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
                    )
                )
            self.invalidate_all(self._parse_control_invalidation(envelope.payload_bytes))
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
        if parsed < minimum:
            raise NormalizationError(f"audit control {key} is outside its integer bounds")
        return parsed

    def _parse_audit_start(self, payload: bytes) -> AuditStartEvidence:
        control = self._control_object(
            payload,
            schema=AUDIT_START_SCHEMA,
            expected_keys=frozenset(
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
            ),
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
        if (
            self._rest_audit_protocol_mode is not None
            and requested_mode is not self._rest_audit_protocol_mode
        ):
            raise NormalizationError("audit start changes the run protocol mode")
        if raw_hard_expiry is not None:
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
            raise NormalizationError("audit start book identity changed before staging")
        self._rest_audit_protocol_mode = requested_mode
        if raw_run_deadline is not None:
            if self._rest_audit_run_deadline_ns is not None and (
                raw_run_deadline != self._rest_audit_run_deadline_ns
                or envelope.received_monotonic.epoch_id != self._rest_audit_run_deadline_epoch_id
            ):
                raise NormalizationError("audit start changes the bound run deadline")
            self._rest_audit_run_deadline_epoch_id = envelope.received_monotonic.epoch_id
            self._rest_audit_run_deadline_ns = cast(int, raw_run_deadline)
        self._pending_rest_audits[start.request_id] = start
        if raw_hard_expiry is not None:
            self._pending_rest_audit_hard_expiries[start.request_id] = raw_hard_expiry
            self._pending_rest_audit_response_deadlines[start.request_id] = cast(
                int, raw_response_deadline
            )

    def _parse_audit_failure(self, payload: bytes) -> tuple[str, AuditFailureReason]:
        control = self._control_object(
            payload,
            schema=AUDIT_FAILURE_SCHEMA,
            expected_keys=frozenset({"reason", "request_id", "schema"}),
        )
        request_id = self._control_text(control, "request_id", maximum=128)
        try:
            reason = AuditFailureReason(self._control_text(control, "reason", maximum=128))
        except ValueError as error:
            raise NormalizationError("audit failure reason is unknown") from error
        if payload != canonical_json_bytes(
            {
                "reason": reason.value,
                "request_id": request_id,
                "schema": AUDIT_FAILURE_SCHEMA,
            }
        ):
            raise NormalizationError("audit failure evidence must use canonical encoding")
        return request_id, reason

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
        self._audit_ineligible_instruments.add(identity.instrument_id)
        owner = self._owners[identity.instrument_id]
        self._mark_market_dirty(owner.market_id, ScanTriggerReason.REST_AUDIT)

    def _record_audit_outcome(self, evidence: AuditOutcomeEvidence) -> None:
        active = self._active_audit_outcomes
        if active is None:
            raise ProgrammingInvariantViolation(
                "audit outcome was recorded outside envelope processing",
                component="canonical-processor",
            )
        self._rest_audits += 1
        if evidence.outcome is AuditOutcome.MATCH:
            self._rest_audit_matches += 1
        elif evidence.outcome is AuditOutcome.MISMATCH:
            self._rest_audit_mismatches += 1
        elif evidence.outcome is AuditOutcome.RACED:
            self._rest_audit_raced += 1
        else:
            self._rest_audit_failed += 1
        self._audit_outcome_chain = _chain_extend(
            self._audit_outcome_chain,
            "rest-audit-outcome",
            evidence,
        )
        self._audit_outcomes.append(evidence)
        active.append(evidence)

    def _failed_audit_outcome(
        self,
        start: AuditStartEvidence,
        *,
        reason: AuditFailureReason,
        response: RawEventEnvelope | None = None,
        response_boundary_ordinal: int | None = None,
    ) -> AuditOutcomeEvidence:
        owner = self._owners[start.starting_book.instrument_id]
        barrier = self.audit_book_identity(start.native_asset_id)
        processed = self.processed_websocket_watermark(start.connection_id).ordinal
        self._audit_ineligible_instruments.add(owner.instrument_id)
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
        )

    def _fail_rest_audit_from_control(self, envelope: RawEventEnvelope) -> None:
        request_id, reason = self._parse_audit_failure(envelope.payload_bytes)
        start = self._pending_rest_audits.pop(request_id, None)
        if start is None:
            raise NormalizationError("audit failure references no pending request")
        if envelope.connection_id != start.connection_id:
            reason = AuditFailureReason.SOCKET_GENERATION_CHANGED
        self._pending_rest_audit_responses.pop(request_id, None)
        self._pending_rest_audit_hard_expiries.pop(request_id, None)
        self._pending_rest_audit_response_deadlines.pop(request_id, None)
        self._record_audit_outcome(self._failed_audit_outcome(start, reason=reason))

    def _advance_websocket_watermark(self, envelope: RawEventEnvelope) -> None:
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

    def _parse_audit_classification(self, envelope: RawEventEnvelope) -> AuditClassificationCommand:
        control = self._control_object(
            envelope.payload_bytes,
            schema=AUDIT_CLASSIFICATION_SCHEMA,
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
        self._active_scan_observed_at = response.received_wall_time
        self._pending_rest_audit_responses.pop(command.request_id)
        coordinator_failure = (
            AuditFailureReason.BOOK_FRESHNESS_EXPIRED
            if command.classification_monotonic_ns >= command.hard_expiry_monotonic_ns
            else None
        )
        self._complete_rest_audit_response(
            response,
            coordinator_failure=coordinator_failure,
            coordinator_decision_supplied=True,
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
    ) -> None:
        request_id, boundary, barrier_ordinal, attempt = self._audit_response_metadata(envelope)
        if (
            not coordinator_decision_supplied
            and request_id in self._pending_rest_audit_hard_expiries
        ):
            raise NormalizationError(
                "bound REST audit start requires deferred response classification"
            )
        start = self._pending_rest_audits.pop(request_id, None)
        if start is None:
            raise NormalizationError("REST audit response references no pending request")
        self._pending_rest_audit_hard_expiries.pop(request_id, None)
        self._pending_rest_audit_response_deadlines.pop(request_id, None)
        if envelope.connection_id != start.connection_id or attempt != start.attempt:
            self._record_audit_outcome(
                self._failed_audit_outcome(
                    start,
                    reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
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
            self._audit_ineligible_instruments.add(start.starting_book.instrument_id)
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
            )
            self._record_audit_outcome(evidence)
            return

        owner = self._owners[start.starting_book.instrument_id]
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
                    response=envelope,
                    response_boundary_ordinal=boundary,
                )
            )
            return
        outcome = (
            AuditOutcome.MATCH if result.status is RestAuditStatus.MATCH else AuditOutcome.MISMATCH
        )
        if outcome is AuditOutcome.MISMATCH and previous_health is not BookHealth.INVALID:
            self._invalidations += 1
        if outcome is AuditOutcome.MATCH:
            self._audit_ineligible_instruments.discard(owner.instrument_id)
        elif outcome is AuditOutcome.MISMATCH:
            self._audit_ineligible_instruments.add(owner.instrument_id)
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
        )
        self._record_audit_outcome(evidence)

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
        for event in events:
            self._apply_canonical(event)
            if isinstance(event.payload, TickSizeChangeEvent):
                if bundle is None:
                    raise ProgrammingInvariantViolation(
                        "tick-size event normalized without a reference bundle",
                        component="canonical-processor",
                    )
                bundle = self._publish_dynamic_tick_reference(event, bundle)

    def _apply_canonical(self, event: CanonicalEvent) -> None:
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
        target_ids = self._market_instruments.get(event.market_id, ())
        if event.instrument_id is not None:
            target_ids = (event.instrument_id,)
        for instrument_id in target_ids:
            owner = self._owners[instrument_id]
            previous = owner.health
            if isinstance(event.payload, (OrderBookSnapshotEvent, PriceChangeEvent)) and (
                self._ws_l2_exceeds_freshness_window(instrument_id)
            ):
                mutation = owner.invalidate(InvalidationReason.STALE_SNAPSHOT)
                if previous is not BookHealth.INVALID:
                    self._invalidations += 1
                self._record_book_mutation(
                    mutation,
                    reason=ScanTriggerReason.INVALIDATION,
                )
                continue
            try:
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
        snapshot_views_list: list[tuple[ImmutableOrderBookSnapshot, str]] = []
        for instrument_id in instrument_ids:
            owner = self._owners[instrument_id]
            if instrument_id in self._audit_ineligible_instruments:
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
        return tuple(affected)

    def on_disconnect(self) -> tuple[InstrumentId, ...]:
        return self.invalidate_all(InvalidationReason.DISCONNECT)

    def on_backpressure(self) -> tuple[InstrumentId, ...]:
        return self.invalidate_all(InvalidationReason.BACKPRESSURE_DATA_LOSS)

    def snapshots(self) -> tuple[ImmutableOrderBookSnapshot, ...]:
        return tuple(owner.snapshot() for owner in self.owners)

    def summary(self) -> ProcessorSummary:
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
        audit_outcome_chain_sha256 = self._audit_outcome_chain.hex()
        audit_outcomes = tuple(self._audit_outcomes)
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
        pending_rest_audit_state = tuple(
            {
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
            }
            for request_id in sorted(self._pending_rest_audits)
        )
        terminal_hash = canonical_sha256(
            {
                "config_hash": self._config.config_hash.value,
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
                    "rest_audit_matches": self._rest_audit_matches,
                    "rest_audit_mismatches": self._rest_audit_mismatches,
                    "rest_audit_raced": self._rest_audit_raced,
                    "rest_audits": self._rest_audits,
                    "pending_rest_audits": len(self._pending_rest_audits),
                    "staged_rest_audit_responses": len(self._pending_rest_audit_responses),
                    "audit_ineligible_books": audit_ineligible_books,
                    "resyncs": self._resyncs,
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
                    "rest_audit_outcome": audit_outcome_chain_sha256,
                    "scan": scan_chain_sha256,
                    "validated_opportunity": validated_opportunity_chain_sha256,
                },
                "reference_versions": reference_versions,
                "lifecycle_state_sha256": lifecycle_state_sha256,
                "audit_ineligible_instrument_ids": audit_ineligible_instrument_ids,
                "pending_rest_audits": pending_rest_audit_state,
                "rest_audit_protocol_mode": self._rest_audit_protocol_mode,
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
            rest_audits=self._rest_audits,
            rest_audit_matches=self._rest_audit_matches,
            rest_audit_mismatches=self._rest_audit_mismatches,
            rest_audit_raced=self._rest_audit_raced,
            rest_audit_failed=self._rest_audit_failed,
            pending_rest_audits=len(self._pending_rest_audits),
            audit_ineligible_books=audit_ineligible_books,
            reference_refresh_requests=self._reference_refresh_count,
            unique_opportunity_lifecycles=len(lifecycles),
            healthy_books=healthy_books,
            raw_input_chain_sha256=raw_input_chain_sha256,
            canonical_event_chain_sha256=canonical_event_chain_sha256,
            error_chain_sha256=error_chain_sha256,
            scan_chain_sha256=scan_chain_sha256,
            validated_opportunity_chain_sha256=validated_opportunity_chain_sha256,
            reference_refresh_chain_sha256=reference_refresh_chain_sha256,
            audit_outcome_chain_sha256=audit_outcome_chain_sha256,
            lifecycle_state_sha256=lifecycle_state_sha256,
            scan_results_truncated=self._scan_count > len(self._scan_results),
            opportunity_hashes_truncated=(
                self._validated_opportunity_count > len(self._validated_opportunity_hashes)
            ),
            reference_refresh_requests_truncated=(
                self._reference_refresh_count > len(self._reference_refresh_requests)
            ),
            audit_outcomes_truncated=self._rest_audits > len(self._audit_outcomes),
            terminal_hash=terminal_hash,
            opportunity_hashes=opportunity_hashes,
            audit_outcomes=audit_outcomes,
        )


__all__ = [
    "AnalyticalOutputSink",
    "BookFreshnessUpdate",
    "CanonicalProcessor",
    "MarketScanTrigger",
    "ProcessedEnvelope",
    "ProcessorSummary",
    "ReferenceRefreshRequest",
    "ScanTriggerReason",
]
