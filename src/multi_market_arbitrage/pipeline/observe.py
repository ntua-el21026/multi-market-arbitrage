"""Bounded unauthenticated Polymarket public-data observation service."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path

from multi_market_arbitrage.core.config import Phase1Config, Phase1RuntimeRole
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    InstrumentId,
    SubscriptionId,
    VenueId,
    WriterId,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalBackpressureError,
    JournalMetricsSnapshot,
    JournalReceipt,
    JournalTicket,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.normalization import CanonicalEventType
from multi_market_arbitrage.observability import (
    HealthCheckpoint,
    HealthDecision,
    HealthDecisionReason,
    HealthEvidenceRuntimeSnapshot,
    HealthEvidenceSummary,
    InMemoryMetrics,
    MetricsSnapshot,
    PeriodicHealthCheckpointEmitter,
    StructuredLogger,
    SupervisedHealthCheckpointWriter,
    configure_json_logger,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import (
    AnalyticalRuntimeSnapshot,
    Phase1AnalyticalOutputs,
    ResearchOutputSummary,
    SupervisedAnalyticalOutputs,
)
from multi_market_arbitrage.state.order_books import InvalidationReason
from multi_market_arbitrage.venues.polymarket import (
    PolymarketHttpError,
    PolymarketHttpFailureCategory,
    PolymarketMarketWebSocket,
    PolymarketPublicClient,
    PolymarketWebSocketError,
    PolymarketWebSocketFailureCategory,
    PolymarketWebSocketProtocolError,
    PolymarketWebSocketTransportError,
    PublicReadMetricsSnapshot,
    PublicReadPolicy,
    ReceivedWebSocketMessage,
    parse_gamma_market,
)
from multi_market_arbitrage.venues.polymarket.schemas import GammaMarket

from .audit import (
    AuditClassificationCommand,
    AuditExpiryCommand,
    AuditFailureCommand,
    AuditFailureReason,
    AuditHttpFailureHandoff,
    AuditHttpResponseHandoff,
    AuditOutcome,
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
)
from .freshness import IntegrityDeadline, IntegrityDeadlineReason
from .health import (
    ObservationRuntimeHealth,
    ObservationRuntimeHealthSnapshot,
    ProcessResourceSnapshot,
    SupervisedProcessResourceSampler,
    build_observation_health_checkpoint,
)
from .ingress import (
    BoundedRawIngress,
    RawIngressBackpressureError,
    RawIngressItem,
    RawIngressMetricsSnapshot,
)
from .processor import (
    CanonicalProcessor,
    ProcessedEnvelope,
    ProcessorSummary,
    ReferenceRefreshRequest,
)
from .targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
    TargetSelectionError,
    require_supported_gamma_market,
    resolve_polymarket_target,
)


class _GenerationExit(StrEnum):
    DEADLINE = "DEADLINE"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"
    REFERENCE_REFRESH = "REFERENCE_REFRESH"
    INGRESS_OVERFLOW = "INGRESS_OVERFLOW"


class _AuditLaunchResult(StrEnum):
    STARTED = "STARTED"
    FAILED_CLOSED = "FAILED_CLOSED"
    EXPIRED = "EXPIRED"
    GENERATION_ENDED = "GENERATION_ENDED"
    RUN_ENDED = "RUN_ENDED"


def _empty_ingress_metrics() -> RawIngressMetricsSnapshot:
    return RawIngressMetricsSnapshot(
        received=0,
        admitted=0,
        dequeued=0,
        overflows=0,
        queue_depth=0,
        queue_high_water=0,
        queued_bytes=0,
        queued_bytes_high_water=0,
        received_bytes=0,
        admitted_bytes=0,
    )


@dataclass(frozen=True, slots=True)
class _CaptureCutoffMoment:
    """Wall and monotonic clocks sampled when capture admission stops."""

    wall_time: UtcTimestamp
    monotonic_ns: int


@dataclass(frozen=True, slots=True)
class _GenerationResult:
    reason: _GenerationExit
    deviations: tuple[str, ...] = ()
    refresh_requests: tuple[ReferenceRefreshRequest, ...] = ()
    ingress_metrics: RawIngressMetricsSnapshot = field(default_factory=_empty_ingress_metrics)
    capture_cutoff: _CaptureCutoffMoment | None = None


@dataclass(frozen=True, slots=True)
class _StreamResult:
    reconnects: int
    completed: bool
    selected_market_resolved: bool
    deviations: tuple[str, ...]
    ingress_metrics: RawIngressMetricsSnapshot = field(default_factory=_empty_ingress_metrics)
    capture_cutoff: _CaptureCutoffMoment | None = None


@dataclass(frozen=True, slots=True)
class _HealthCheckpointMoment:
    """Owner snapshots and clocks frozen at one capture boundary."""

    emitted_at: UtcTimestamp
    monotonic_now_ns: int
    runtime: ObservationRuntimeHealthSnapshot
    journal: JournalMetricsSnapshot
    journal_failure_type: str | None
    analytical: AnalyticalRuntimeSnapshot
    health_writer: HealthEvidenceRuntimeSnapshot
    public_reads: PublicReadMetricsSnapshot
    resources: ProcessResourceSnapshot | None
    book_last_valid_receive_monotonic_ns: tuple[tuple[InstrumentId, int], ...]


@dataclass(slots=True)
class _AuditResponseBoundaryFence:
    """Request-scoped receive fence announced before HTTP response cleanup awaits."""

    request_id: str
    connection_id: ConnectionId
    announced: bool = False
    received_wall_time: UtcTimestamp | None = None
    received_monotonic_ns: int | None = None
    watermark: WebSocketAdmissionWatermark | None = None
    error_type: str | None = None

    def announce(
        self,
        *,
        ingress: BoundedRawIngress,
        received_wall_time: UtcTimestamp,
        received_monotonic_ns: int,
    ) -> None:
        """Capture the contiguous prefix synchronously on the socket event loop."""

        if self.announced:
            self.watermark = None
            self.error_type = "DuplicateResponseBoundaryAnnouncement"
            return
        self.announced = True
        self.received_wall_time = received_wall_time
        self.received_monotonic_ns = received_monotonic_ns
        try:
            watermark = ingress.snapshot_watermark()
        except RawIngressBackpressureError as error:
            self.error_type = type(error).__name__
            return
        if watermark.connection_id != self.connection_id:
            self.error_type = "ResponseBoundaryConnectionMismatch"
            return
        self.watermark = watermark

    def abandon(self) -> None:
        """Clear a provisional boundary when its HTTP attempt does not complete."""

        self.announced = False
        self.received_wall_time = None
        self.received_monotonic_ns = None
        self.watermark = None
        self.error_type = None

    @property
    def barrier_ordinal(self) -> int | None:
        return None if self.watermark is None else self.watermark.ordinal


@dataclass(slots=True)
class _PendingAuditRequest:
    """Coordinator-owned state for one of the bounded concurrent audit legs."""

    start: AuditStartEvidence
    response_fence: _AuditResponseBoundaryFence
    task: asyncio.Task[None]
    freshness_marker_ns: int | None
    hard_expiry_ns: int
    asset_order: int
    response: AuditHttpResponseHandoff | None = None


@dataclass(frozen=True, slots=True)
class _StagedAuditRequest:
    """Durable START awaiting the back-to-back first-attempt launch boundary."""

    start: AuditStartEvidence
    freshness_marker_ns: int | None
    hard_expiry_ns: int
    response_deadline_ns: int
    asset_order: int


_EMPTY_INGRESS_METRICS = _empty_ingress_metrics()


def _merge_ingress_metrics(
    left: RawIngressMetricsSnapshot,
    right: RawIngressMetricsSnapshot,
) -> RawIngressMetricsSnapshot:
    return RawIngressMetricsSnapshot(
        received=left.received + right.received,
        admitted=left.admitted + right.admitted,
        dequeued=left.dequeued + right.dequeued,
        overflows=left.overflows + right.overflows,
        queue_depth=right.queue_depth,
        queue_high_water=max(left.queue_high_water, right.queue_high_water),
        queued_bytes=right.queued_bytes,
        queued_bytes_high_water=max(
            left.queued_bytes_high_water,
            right.queued_bytes_high_water,
        ),
        received_bytes=left.received_bytes + right.received_bytes,
        admitted_bytes=left.admitted_bytes + right.admitted_bytes,
        connection_id=right.connection_id,
        last_frame_received_monotonic_ns=max(
            value
            for value in (
                left.last_frame_received_monotonic_ns,
                right.last_frame_received_monotonic_ns,
            )
            if value is not None
        )
        if (
            left.last_frame_received_monotonic_ns is not None
            or right.last_frame_received_monotonic_ns is not None
        )
        else None,
    )


@dataclass(frozen=True, slots=True)
class ObservationSummary:
    collector_run_id: CollectorRunId
    status: ComponentHealth
    started_at: UtcTimestamp
    ended_at: UtcTimestamp
    requested_duration_seconds: float
    actual_duration_nanoseconds: int
    target_selection_mode: ObservationTargetMode
    target: ResolvedPolymarketTarget | None
    selected_condition_ids: tuple[str, ...]
    selected_asset_ids: tuple[str, ...]
    journal_root: Path
    journal_metrics: JournalMetricsSnapshot
    processor: ProcessorSummary
    analytical_outputs: ResearchOutputSummary
    health_evidence: HealthEvidenceSummary
    health_decision: HealthDecision
    public_read_metrics: PublicReadMetricsSnapshot
    runtime_health: ObservationRuntimeHealthSnapshot
    observability_metrics: MetricsSnapshot
    ingress_metrics: RawIngressMetricsSnapshot
    reconnects: int
    selected_market_resolved: bool
    protocol_deviations: tuple[str, ...]
    websocket_observation_nanoseconds: int


class _RawCaptureBoundary:
    """Assign run-scoped order and await durable storage before processing."""

    def __init__(
        self,
        *,
        collector_run_id: CollectorRunId,
        writer: RawJournalWriter,
        processor: CanonicalProcessor,
        metrics: InMemoryMetrics,
    ) -> None:
        self.collector_run_id = collector_run_id
        self.writer = writer
        self.processor = processor
        self.metrics = metrics
        self._next_sequence = 0
        self._book_freshness_ns: dict[InstrumentId, int] = {}

    def _capture_envelope(
        self,
        *,
        channel: str,
        payload: bytes,
        transport_type: str,
        semantics_hash: str,
        connection_id: ConnectionId | None,
        subscription_id: SubscriptionId | None,
        transport_metadata: dict[str, str | int | bool] | None,
        received_wall_time: UtcTimestamp,
        received_monotonic_ns: int,
    ) -> RawEventEnvelope:
        envelope = RawEventEnvelope.capture(
            collector_run_id=self.collector_run_id,
            ingest_seq=self._next_sequence,
            venue_id=self.writer.config.venue_id,
            connection_id=connection_id or ConnectionId.new(),
            subscription_id=subscription_id or SubscriptionId.new(),
            channel=channel,
            transport_type=transport_type,
            subscription_semantics_hash=semantics_hash,
            received_wall_time=received_wall_time,
            received_monotonic=MonotonicTimestamp(
                self.collector_run_id.value,
                received_monotonic_ns,
            ),
            payload_bytes=payload,
            payload_encoding="utf-8",
            transport_metadata=transport_metadata,
        )
        self._next_sequence += 1
        return envelope

    def _record_processed(
        self,
        envelope: RawEventEnvelope,
        result: ProcessedEnvelope,
        *,
        scan_completed_monotonic_ns: int,
    ) -> None:
        self.metrics.increment_counter("mma_raw_processed_total")
        if not result.success:
            self.metrics.increment_counter("mma_parse_failures_total")
        if result.needs_resync:
            self._book_freshness_ns.clear()
        for update in result.book_freshness_updates:
            self._book_freshness_ns[update.instrument_id] = update.received_monotonic.nanoseconds
        if result.scan_count:
            scan_source_received_ns = (
                envelope.received_monotonic.nanoseconds
                if result.scan_source_received_monotonic_ns is None
                else result.scan_source_received_monotonic_ns
            )
            self.metrics.observe_histogram(
                "mma_receive_to_scan_complete_ns",
                max(
                    0,
                    scan_completed_monotonic_ns - scan_source_received_ns,
                ),
            )

    def _record_durable_receipt(
        self,
        envelope: RawEventEnvelope,
        receipt: JournalReceipt,
    ) -> None:
        self.metrics.increment_counter("mma_raw_durable_total")
        enqueued = receipt.enqueued_monotonic
        commit_started = receipt.commit_started_monotonic
        durable = receipt.durable_monotonic
        if enqueued is not None and commit_started is not None:
            self.metrics.observe_histogram(
                "mma_journal_enqueue_to_commit_ns",
                commit_started.nanoseconds - enqueued.nanoseconds,
            )
        if commit_started is not None and durable is not None:
            self.metrics.observe_histogram(
                "mma_journal_commit_to_durable_ns",
                durable.nanoseconds - commit_started.nanoseconds,
            )
        self.metrics.observe_histogram(
            "mma_journal_durability_group_size",
            receipt.durability_group_size,
        )
        if durable is not None:
            self.metrics.observe_histogram(
                "mma_receive_to_durable_ns",
                max(
                    0,
                    durable.nanoseconds - envelope.received_monotonic.nanoseconds,
                ),
            )

    @property
    def book_freshness_receive_ns(self) -> tuple[int, ...]:
        return tuple(self._book_freshness_ns.values())

    @property
    def book_freshness_receive_ns_by_instrument(self) -> dict[InstrumentId, int]:
        """Return a detached typed view of accepted L2 monotonic receive times."""

        return dict(self._book_freshness_ns)

    async def preserve_then_process(
        self,
        *,
        channel: str,
        payload: bytes,
        transport_type: str,
        semantics_hash: str,
        connection_id: ConnectionId | None = None,
        subscription_id: SubscriptionId | None = None,
        transport_metadata: dict[str, str | int | bool] | None = None,
        received_wall_time: UtcTimestamp | None = None,
        received_monotonic_ns: int | None = None,
    ) -> ProcessedEnvelope:
        self.metrics.increment_counter("mma_raw_received_total")
        self.metrics.observe_histogram("mma_raw_payload_bytes", len(payload))
        captured_monotonic_ns = (
            time.monotonic_ns() if received_monotonic_ns is None else received_monotonic_ns
        )
        envelope = self._capture_envelope(
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=received_wall_time or UtcTimestamp.now(),
            received_monotonic_ns=captured_monotonic_ns,
        )
        try:
            receipt = await self.writer.submit(envelope)
        except JournalBackpressureError:
            self.metrics.increment_counter("mma_journal_backpressure_failures_total")
            # A failed capture is never replay-authoritative; invalidate live
            # state immediately and let the fatal journal fault abort the run.
            self.processor.on_backpressure()
            raise
        self._record_durable_receipt(envelope, receipt)
        self.metrics.set_gauge("mma_journal_queue_depth", self.writer.metrics.queue_depth)
        result = self.processor.process_durable(envelope, receipt)
        self._record_processed(
            envelope,
            result,
            scan_completed_monotonic_ns=time.monotonic_ns(),
        )
        return result

    async def preserve_websocket_batch(
        self,
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        """Admit a bounded batch, then release canonical work in exact FIFO order."""

        admitted: list[tuple[RawEventEnvelope, JournalTicket]] = []
        for item in items:
            message = item.message
            if item.watermark.connection_id != connection_id:
                raise ValueError("raw ingress item belongs to another socket generation")
            self.metrics.increment_counter("mma_raw_received_total")
            self.metrics.observe_histogram("mma_raw_payload_bytes", len(message.payload_bytes))
            self.metrics.observe_histogram(
                "mma_socket_receive_to_ingress_ns",
                max(0, item.admitted_monotonic_ns - message.received_monotonic_ns),
            )
            self.metrics.observe_histogram(
                "mma_ingress_queue_dwell_ns",
                item.queue_dwell_nanoseconds,
            )
            envelope = self._capture_envelope(
                channel=WS_MARKET,
                payload=message.payload_bytes,
                transport_type="WEBSOCKET",
                semantics_hash=semantics_hash,
                connection_id=connection_id,
                subscription_id=subscription_id,
                transport_metadata={
                    "websocket_admission_ordinal": item.watermark.ordinal,
                    "websocket_message_type": message.message_type,
                },
                received_wall_time=message.received_wall_time,
                received_monotonic_ns=message.received_monotonic_ns,
            )
            try:
                ticket = await self.writer.enqueue(envelope)
            except JournalBackpressureError:
                self.metrics.increment_counter("mma_journal_backpressure_failures_total")
                self.processor.on_backpressure()
                raise
            admitted.append((envelope, ticket))

        processed: list[ProcessedEnvelope] = []
        for envelope, ticket in admitted:
            receipt = await ticket.wait_durable()
            self._record_durable_receipt(envelope, receipt)
            result = self.processor.process_durable(envelope, receipt)
            self._record_processed(
                envelope,
                result,
                scan_completed_monotonic_ns=time.monotonic_ns(),
            )
            processed.append(result)
        self.metrics.set_gauge("mma_journal_queue_depth", self.writer.metrics.queue_depth)
        return tuple(processed)

    async def preserve_invalidation(self, reason: InvalidationReason) -> ProcessedEnvelope:
        """Persist a local continuity fact so replay applies the same transition."""

        return await self.preserve_then_process(
            channel=CONTROL_BOOK_INVALIDATION,
            payload=canonical_json_bytes({"reason": reason.value}),
            transport_type="CONTROL",
            semantics_hash=_semantics(CONTROL_BOOK_INVALIDATION, reason.value),
        )


def _semantics(channel: str, detail: str = "") -> str:
    return hashlib.sha256(f"mma-public-read-v1\0{channel}\0{detail}".encode()).hexdigest()


def _eligible_market(market: GammaMarket) -> bool:
    return (
        market.active is True
        and market.closed is False
        and market.archived is False
        and market.accepting_orders is True
        and market.enable_order_book is True
        and market.neg_risk is False
        and len(market.clob_token_ids) == 2
        and tuple(item.casefold() for item in market.outcomes) == ("yes", "no")
    )


def _logging_level(name: str) -> int:
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    return levels[name]


def _public_read_policy(config: Phase1Config) -> PublicReadPolicy:
    configured = config.polymarket.public_reads
    return PublicReadPolicy(
        maximum_concurrency=configured.maximum_concurrency,
        maximum_attempts=configured.maximum_attempts,
        initial_backoff_seconds=configured.initial_backoff_seconds,
        maximum_backoff_seconds=configured.maximum_backoff_seconds,
        jitter_fraction=configured.jitter_fraction,
        jitter_seed=configured.jitter_seed,
        maximum_retry_after_seconds=configured.maximum_retry_after_seconds,
        rate_limit_window_seconds=configured.rate_limit_window_seconds,
        gamma_general_requests_per_window=(configured.gamma_general_requests_per_window),
        gamma_markets_requests_per_window=(configured.gamma_markets_requests_per_window),
        clob_general_requests_per_window=(configured.clob_general_requests_per_window),
        clob_book_requests_per_window=configured.clob_book_requests_per_window,
        clob_books_requests_per_window=configured.clob_books_requests_per_window,
    )


def _failed_health_summary(
    *,
    root: Path,
    collector_run_id: CollectorRunId,
    writer: SupervisedHealthCheckpointWriter,
    error: BaseException,
) -> HealthEvidenceSummary:
    """Represent a diagnostic-owner failure without failing raw acquisition."""

    writer.record_failure(error)
    runtime = writer.runtime_snapshot
    path = root / (f"run-{collector_run_id.value}.health-checkpoints.jsonl.partial")
    return HealthEvidenceSummary(
        schema_version="gate1-health-evidence-summary-v1",
        collector_run_id=collector_run_id,
        path=path,
        completed=False,
        gate1_evidence_eligible=False,
        accepted=runtime.accepted,
        written=runtime.written,
        rejected=runtime.rejected,
        queue_high_water=runtime.queue_high_water,
        file_bytes=runtime.file_bytes,
        content_sha256=None,
        failure_type=runtime.failure_type or type(error).__name__,
    )


def _final_health_decision(
    checkpoint_decision: HealthDecision | None,
    *,
    evidence: HealthEvidenceSummary,
    analytical_outputs: ResearchOutputSummary,
    terminal_runtime_reasons: tuple[HealthDecisionReason, ...] = (),
) -> HealthDecision:
    """Reconcile the last checkpoint with close-time derived-owner outcomes."""

    if checkpoint_decision is None:
        reasons = {
            HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY,
            *terminal_runtime_reasons,
        }
        if not analytical_outputs.research_eligible:
            reasons.add(HealthDecisionReason.ANALYTICAL_UNHEALTHY)
        return HealthDecision(
            capture_healthy=False,
            research_eligible=False,
            gate1_eligible_so_far=False,
            reason_codes=tuple(sorted(reasons, key=str)),
        )
    reasons = set(checkpoint_decision.reason_codes)
    reasons.update(terminal_runtime_reasons)
    capture_healthy = checkpoint_decision.capture_healthy and not terminal_runtime_reasons
    research_eligible = checkpoint_decision.research_eligible and not terminal_runtime_reasons
    if not evidence.gate1_evidence_eligible:
        reasons.add(HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY)
    if not analytical_outputs.research_eligible:
        reasons.add(HealthDecisionReason.ANALYTICAL_UNHEALTHY)
        research_eligible = False
    return HealthDecision(
        capture_healthy=capture_healthy,
        research_eligible=research_eligible,
        gate1_eligible_so_far=(
            checkpoint_decision.gate1_eligible_so_far
            and evidence.gate1_evidence_eligible
            and analytical_outputs.research_eligible
            and not reasons
        ),
        reason_codes=tuple(sorted(reasons, key=str)),
    )


def _accumulate_gate1_decision(
    current: HealthDecision,
    previous: HealthDecision | None,
    *,
    cumulative_reason_codes: tuple[HealthDecisionReason, ...] = (),
) -> HealthDecision:
    """Keep Gate-1 eligibility and its reasons sticky across checkpoint cadence."""

    reasons = set(current.reason_codes)
    reasons.update(cumulative_reason_codes)
    if previous is not None:
        reasons.update(previous.reason_codes)
    return HealthDecision(
        capture_healthy=current.capture_healthy,
        research_eligible=current.research_eligible,
        gate1_eligible_so_far=(
            current.gate1_eligible_so_far
            and not cumulative_reason_codes
            and (previous is None or previous.gate1_eligible_so_far)
        ),
        reason_codes=tuple(sorted(reasons, key=str)),
    )


class PolymarketObservationService:
    """Run one finite, read-only capture using only explicit public adapters."""

    def __init__(
        self,
        config: Phase1Config,
        *,
        workspace_root: Path,
        code_commit: str,
        logger: StructuredLogger | None = None,
        metrics: InMemoryMetrics | None = None,
    ) -> None:
        if config.system.runtime_role is not Phase1RuntimeRole.OBSERVE:
            raise ValueError("observation requires system.runtime_role=OBSERVE")
        self._config = config
        self._workspace_root = workspace_root.resolve()
        self._code_commit = code_commit
        self._metrics = metrics or InMemoryMetrics(max_series=256)
        self._logger = logger or configure_json_logger(
            "multi_market_arbitrage.observe",
            level=_logging_level(config.logging.level.value),
            bound_fields={
                "service": "mma-phase1-observer",
                "runtime_role": config.system.runtime_role.value,
                "config_hash": config.config_hash,
                "venue": POLYMARKET_VENUE_ID,
            },
        )

    def _path(self, configured: Path) -> Path:
        resolved = (self._workspace_root / configured).resolve()
        if not resolved.is_relative_to(self._workspace_root):
            raise ValueError("configured observation output resolves outside workspace")
        return resolved

    async def run(
        self,
        *,
        duration_seconds: float,
        gamma_market_id: str | None = None,
    ) -> ObservationSummary:
        maximum_duration = self._config.system.maximum_observation_duration_seconds
        if (
            not math.isfinite(duration_seconds)
            or duration_seconds <= 0
            or duration_seconds > maximum_duration
        ):
            raise ValueError(
                "observation duration must be finite, positive, and no greater than "
                f"{maximum_duration} seconds"
            )
        if gamma_market_id is not None and (
            not gamma_market_id or gamma_market_id != gamma_market_id.strip()
        ):
            raise ValueError("explicit Gamma market ID must be a non-empty trimmed string")
        loop = asyncio.get_running_loop()
        started_monotonic_ns = time.monotonic_ns()
        deadline = loop.time() + duration_seconds
        started = UtcTimestamp.now()
        run_id = CollectorRunId.new()
        logger = self._logger.bind(collector_run_id=run_id)
        journal_root = self._path(self._config.storage.journal_directory)
        analytical_root = self._path(self._config.storage.analytical_directory)
        health_root = self._path(self._config.storage.health_directory)
        partition_date = started.to_datetime().date().isoformat()
        persistent_outputs = Phase1AnalyticalOutputs(
            root=analytical_root,
            partition_date=partition_date,
            batch_size=self._config.storage.parquet_batch_size,
            collector_run_id=run_id,
            code_commit=self._code_commit,
            code_version=self._config.system.code_version,
            config_hash=self._config.config_hash.value,
            maximum_record_bytes=self._config.storage.analytical_record_max_bytes,
            maximum_buffered_bytes=self._config.storage.analytical_buffer_max_bytes,
        )
        outputs = SupervisedAnalyticalOutputs(
            persistent_outputs,
            queue_capacity=self._config.storage.analytical_queue_capacity,
            metrics=self._metrics,
        )
        processor = CanonicalProcessor(
            self._config,
            analytical_outputs=outputs,
            scan_latency_observer=lambda elapsed_ns: self._metrics.observe_histogram(
                "mma_scanner_latency_ns",
                elapsed_ns,
            ),
        )
        writer = RawJournalWriter(
            JournalWriterConfig(
                root=journal_root,
                collector_run_id=run_id,
                venue_id=self._venue_id(),
                stream="polymarket-public",
                writer_id=WriterId.new(),
                writer_software_version=self._config.system.code_version,
                git_commit=self._code_commit,
                queue_capacity=self._config.system.queue_capacity,
                max_events_per_segment=self._config.storage.journal_segment_max_events,
                max_uncompressed_bytes_per_segment=(
                    self._config.storage.journal_segment_max_uncompressed_bytes
                ),
                max_uncompressed_bytes_per_run=(
                    self._config.storage.journal_run_max_uncompressed_bytes
                ),
                submit_timeout_seconds=self._config.storage.journal_submit_timeout_seconds,
                group_commit_max_events=(self._config.storage.journal_group_commit_max_events),
                group_commit_max_delay_seconds=(
                    self._config.storage.journal_group_commit_max_delay_seconds
                ),
            )
        )
        selected: tuple[GammaMarket, ...] = ()
        selected_assets: tuple[str, ...] = ()
        target: ResolvedPolymarketTarget | None = None
        target_selection_mode = (
            ObservationTargetMode.EXPLICIT_GATE1
            if gamma_market_id is not None
            else ObservationTargetMode.DEVELOPER_DISCOVERY
        )
        stream_result = _StreamResult(0, False, False, ())
        deviations: list[str] = []
        runtime_health = ObservationRuntimeHealth()
        websocket_observation_nanoseconds = 0
        public_read_metrics = PublicReadMetricsSnapshot(
            endpoints=(),
            active_attempts=0,
            active_attempts_high_water=0,
        )
        health_writer = SupervisedHealthCheckpointWriter(
            root=health_root,
            collector_run_id=run_id,
            queue_capacity=self._config.health.queue_capacity,
            maximum_checkpoints=self._config.health.maximum_checkpoints,
            maximum_file_bytes=self._config.health.maximum_file_bytes,
        )
        health_emitter: PeriodicHealthCheckpointEmitter | None = None
        health_started = False
        health_start_error: BaseException | None = None
        latest_health_decision: HealthDecision | None = None
        health_checkpoint_builder: Callable[[], HealthCheckpoint] | None = None
        terminal_health_moment: _HealthCheckpointMoment | None = None
        terminal_capture_cutoff: _CaptureCutoffMoment | None = None
        resource_sampler: SupervisedProcessResourceSampler | None = None
        resource_sampler_started = False
        capture = _RawCaptureBoundary(
            collector_run_id=run_id,
            writer=writer,
            processor=processor,
            metrics=self._metrics,
        )
        run_completed = False
        analytical_summary: ResearchOutputSummary | None = None
        logger.info("observation_started", "bounded public observation started")
        writer_started = False
        try:
            await writer.start()
            writer_started = True
            await outputs.start()
        except BaseException:
            if writer_started:
                await writer.close(completed=False)
            persistent_outputs.abort(failure_type="ObservationStartupError")
            raise
        try:
            async with PolymarketPublicClient(
                gamma_url=self._config.polymarket.gamma_base_url,
                clob_url=self._config.polymarket.clob_base_url,
                timeout_seconds=self._config.polymarket.request_timeout_seconds,
                max_response_bytes=self._config.polymarket.maximum_http_response_bytes,
                public_read_policy=_public_read_policy(self._config),
                metrics=self._metrics,
            ) as client:

                def record_resource_sampler_failure(error: BaseException) -> None:
                    health_writer.record_failure(error)
                    logger.error(
                        "health_evidence_failed",
                        "resource sampler failed; raw capture remains active",
                        exception_type=type(error).__name__,
                    )

                resource_sampler = SupervisedProcessResourceSampler(
                    storage_path=journal_root,
                    interval_seconds=self._config.health.checkpoint_interval_seconds,
                    on_failure=record_resource_sampler_failure,
                )

                def build_health_checkpoint() -> HealthCheckpoint:
                    nonlocal latest_health_decision
                    moment = terminal_health_moment
                    monotonic_now_ns = (
                        time.monotonic_ns() if moment is None else moment.monotonic_now_ns
                    )
                    resources = (
                        resource_sampler.snapshot.latest if moment is None else moment.resources
                    )
                    if resources is None:
                        raise RuntimeError("resource sampler has not published a sample")
                    runtime_snapshot = (
                        runtime_health.snapshot(now_ns=monotonic_now_ns)
                        if moment is None
                        else moment.runtime
                    )
                    checkpoint = build_observation_health_checkpoint(
                        config=self._config,
                        collector_run_id=run_id,
                        git_commit=self._code_commit,
                        target=target,
                        runtime=runtime_snapshot,
                        processor=processor,
                        journal=writer.metrics if moment is None else moment.journal,
                        journal_failure_type=(
                            (None if writer.failure is None else type(writer.failure).__name__)
                            if moment is None
                            else moment.journal_failure_type
                        ),
                        analytical=(
                            outputs.runtime_snapshot if moment is None else moment.analytical
                        ),
                        health_writer=(
                            health_writer.runtime_snapshot
                            if moment is None
                            else moment.health_writer
                        ),
                        public_reads=(
                            client.public_read_metrics_snapshot()
                            if moment is None
                            else moment.public_reads
                        ),
                        metrics=self._metrics,
                        resources=resources,
                        book_last_valid_receive_monotonic_ns=(
                            capture.book_freshness_receive_ns_by_instrument
                            if moment is None
                            else dict(moment.book_last_valid_receive_monotonic_ns)
                        ),
                        emitted_at=(UtcTimestamp.now() if moment is None else moment.emitted_at),
                        monotonic_now_ns=monotonic_now_ns,
                    )
                    current_health_decision = HealthDecision(
                        capture_healthy=checkpoint.capture_healthy,
                        research_eligible=checkpoint.research_eligible,
                        gate1_eligible_so_far=checkpoint.gate1_eligible_so_far,
                        reason_codes=checkpoint.reason_codes,
                    )
                    latest_health_decision = _accumulate_gate1_decision(
                        current_health_decision,
                        latest_health_decision,
                        cumulative_reason_codes=(
                            (HealthDecisionReason.SOCKET_DISCONNECTED,)
                            if runtime_snapshot.unavailable_intervals > 0
                            else ()
                        ),
                    )
                    return replace(
                        checkpoint,
                        gate1_eligible_so_far=(latest_health_decision.gate1_eligible_so_far),
                        reason_codes=latest_health_decision.reason_codes,
                    )

                health_checkpoint_builder = build_health_checkpoint

                def stop_periodic_health_at_capture_cutoff(
                    cutoff: _CaptureCutoffMoment,
                ) -> None:
                    """Seal periodic evidence synchronously at the acquisition cutoff."""

                    nonlocal terminal_capture_cutoff
                    if terminal_capture_cutoff is not None:
                        if terminal_capture_cutoff != cutoff:
                            raise RuntimeError("capture cutoff changed after it was sealed")
                        return
                    terminal_capture_cutoff = cutoff
                    if not health_started:
                        return
                    if health_emitter is None:
                        raise RuntimeError("started health evidence has no emitter")
                    health_emitter.stop_periodic_emission_nowait()

                async def emit_terminal_health_checkpoint() -> None:
                    """Freeze capture state, then admit exactly one terminal checkpoint."""

                    nonlocal terminal_capture_cutoff, terminal_health_moment
                    if terminal_capture_cutoff is None:
                        if stream_result.capture_cutoff is not None:
                            terminal_capture_cutoff = stream_result.capture_cutoff
                        else:
                            cutoff_monotonic_ns = time.monotonic_ns()
                            terminal_capture_cutoff = _CaptureCutoffMoment(
                                wall_time=UtcTimestamp.now(),
                                monotonic_ns=cutoff_monotonic_ns,
                            )
                    if target is None or not health_started:
                        return
                    if health_emitter is None:
                        raise RuntimeError("started health evidence has no emitter")

                    cutoff_monotonic_ns = terminal_capture_cutoff.monotonic_ns
                    cutoff_emitted_at = terminal_capture_cutoff.wall_time
                    cutoff_runtime = runtime_health.snapshot(now_ns=cutoff_monotonic_ns)
                    cutoff_journal = writer.metrics
                    cutoff_journal_failure_type = (
                        None if writer.failure is None else type(writer.failure).__name__
                    )
                    cutoff_analytical = outputs.runtime_snapshot
                    cutoff_health_writer = health_writer.runtime_snapshot
                    cutoff_public_reads = client.public_read_metrics_snapshot()
                    cutoff_book_receive_times = tuple(
                        sorted(
                            capture.book_freshness_receive_ns_by_instrument.items(),
                            key=lambda item: item[0].value,
                        )
                    )
                    cutoff_resources = resource_sampler.snapshot.latest

                    # Cancel the periodic producer at the capture boundary.  The
                    # writer remains open so raw/analytical owners may close and
                    # the health file may be published later without rebuilding.
                    await health_emitter.stop_periodic_emission()
                    if cutoff_resources is None and resource_sampler_started:
                        try:
                            async with asyncio.timeout(
                                max(
                                    1.0,
                                    self._config.health.checkpoint_interval_seconds,
                                )
                            ):
                                cutoff_resources = (
                                    await resource_sampler.wait_for_initial_attempt()
                                ).latest
                        except TimeoutError as error:
                            health_writer.record_failure(error)
                    terminal_health_moment = _HealthCheckpointMoment(
                        emitted_at=cutoff_emitted_at,
                        monotonic_now_ns=cutoff_monotonic_ns,
                        runtime=cutoff_runtime,
                        journal=cutoff_journal,
                        journal_failure_type=cutoff_journal_failure_type,
                        analytical=cutoff_analytical,
                        health_writer=cutoff_health_writer,
                        public_reads=cutoff_public_reads,
                        resources=cutoff_resources,
                        book_last_valid_receive_monotonic_ns=(cutoff_book_receive_times),
                    )
                    health_emitter.emit_terminal_now()

                health_emitter = PeriodicHealthCheckpointEmitter(
                    writer=health_writer,
                    interval_seconds=self._config.health.checkpoint_interval_seconds,
                    build=build_health_checkpoint,
                    emit_initial=False,
                    enabled=lambda: (
                        target is not None and resource_sampler.snapshot.latest is not None
                    ),
                    final_enabled=lambda: target is not None,
                )
                try:
                    await health_emitter.start()
                except BaseException as error:
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                    health_start_error = error
                    health_writer.record_failure(error)
                    logger.error(
                        "health_evidence_failed",
                        "health evidence owner failed; raw capture remains active",
                        exception_type=type(error).__name__,
                    )
                else:
                    health_started = True
                    try:
                        await resource_sampler.start()
                    except BaseException as error:
                        if isinstance(error, (KeyboardInterrupt, SystemExit)):
                            raise
                        record_resource_sampler_failure(error)
                    else:
                        resource_sampler_started = True
                try:
                    if gamma_market_id is None:
                        selected, selected_assets, target = await self._bootstrap(
                            client=client,
                            capture=capture,
                            processor=processor,
                            deadline=deadline,
                        )
                    else:
                        selected, selected_assets, target = await self._bootstrap_explicit(
                            client=client,
                            capture=capture,
                            processor=processor,
                            gamma_market_id=gamma_market_id,
                            deadline=deadline,
                        )
                except PolymarketHttpError as error:
                    if error.category is not PolymarketHttpFailureCategory.DEADLINE_EXCEEDED:
                        raise
                    stream_result = _StreamResult(0, True, False, ())
                if loop.time() < deadline and selected_assets:
                    websocket_started_ns = time.monotonic_ns()
                    stream_result = await self._observe_websocket(
                        client=client,
                        capture=capture,
                        selected=selected,
                        asset_ids=selected_assets,
                        deadline=deadline,
                        logger=logger,
                        runtime_health=runtime_health,
                        on_capture_cutoff=stop_periodic_health_at_capture_cutoff,
                    )
                    websocket_observation_nanoseconds = max(
                        0,
                        (
                            time.monotonic_ns()
                            if stream_result.capture_cutoff is None
                            else stream_result.capture_cutoff.monotonic_ns
                        )
                        - websocket_started_ns,
                    )
                    deviations.extend(stream_result.deviations)
                await emit_terminal_health_checkpoint()
                run_completed = True
                public_read_metrics = client.public_read_metrics_snapshot()
        except Exception as error:
            logger.error(
                "observation_failed",
                "bounded public observation failed",
                exception_type=type(error).__name__,
                exc_info=True,
            )
            raise
        finally:
            raw_completion_sha256: str | None = None
            try:
                await writer.close(completed=run_completed)
                if run_completed:
                    raw_head_path = completion_path_for_run(
                        journal_root,
                        run_id,
                        "polymarket-public",
                    )
                    raw_completion_sha256 = load_run_completion_head(
                        raw_head_path
                    ).completion_sha256
            finally:
                journal_metrics = writer.metrics
                try:
                    analytical_summary = await outputs.close(
                        source_journal_completion_sha256=raw_completion_sha256,
                        completed=run_completed and raw_completion_sha256 is not None,
                    )
                finally:
                    if resource_sampler_started:
                        if resource_sampler is None:
                            raise RuntimeError("started resource sampler is unavailable")
                        try:
                            await resource_sampler.close()
                        except BaseException as error:
                            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                                raise
                            health_writer.record_failure(error)
                    if health_started:
                        if health_emitter is None:
                            raise RuntimeError("started health evidence has no emitter")
                        try:
                            health_evidence = await health_emitter.close(
                                completed=(
                                    run_completed
                                    and raw_completion_sha256 is not None
                                    and analytical_summary is not None
                                    and analytical_summary.research_eligible
                                ),
                            )
                        except BaseException as error:
                            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                                raise
                            health_evidence = _failed_health_summary(
                                root=health_root,
                                collector_run_id=run_id,
                                writer=health_writer,
                                error=error,
                            )
                    else:
                        failure = health_start_error or RuntimeError(
                            "health evidence owner was not started"
                        )
                        health_evidence = _failed_health_summary(
                            root=health_root,
                            collector_run_id=run_id,
                            writer=health_writer,
                            error=failure,
                        )
        if analytical_summary is None:
            raise RuntimeError("analytical output close did not produce a summary")
        if (
            target is None
            and latest_health_decision is None
            and health_checkpoint_builder is not None
        ):
            # A pre-target deadline intentionally writes no Gate-1 evidence, but
            # the returned scoped decision must still reflect the terminal
            # socket/target/book state rather than defaulting to healthy.
            try:
                health_checkpoint_builder()
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
        if terminal_capture_cutoff is None:
            raise RuntimeError("completed observation lacks a paired terminal cutoff")
        ended = terminal_capture_cutoff.wall_time
        summary_cutoff_monotonic_ns = terminal_capture_cutoff.monotonic_ns
        actual_duration_nanoseconds = max(
            0,
            summary_cutoff_monotonic_ns - started_monotonic_ns,
        )
        runtime_health_snapshot = runtime_health.snapshot(now_ns=summary_cutoff_monotonic_ns)
        terminal_runtime_reasons: list[HealthDecisionReason] = []
        if not runtime_health_snapshot.socket_connected:
            terminal_runtime_reasons.append(HealthDecisionReason.SOCKET_DISCONNECTED)
        if (
            runtime_health_snapshot.data_loss_count > 0
            or runtime_health_snapshot.ingress.overflows > 0
        ):
            terminal_runtime_reasons.append(HealthDecisionReason.INGRESS_DATA_LOSS)
        if runtime_health_snapshot.protocol_deviations > 0:
            terminal_runtime_reasons.append(HealthDecisionReason.PROTOCOL_DEVIATION)
        health_decision = _final_health_decision(
            latest_health_decision,
            evidence=health_evidence,
            analytical_outputs=analytical_summary,
            terminal_runtime_reasons=tuple(terminal_runtime_reasons),
        )
        status = (
            ComponentHealth.HEALTHY
            if (
                target is not None
                and stream_result.completed
                and not deviations
                and analytical_summary.research_eligible
                and health_evidence.gate1_evidence_eligible
                and health_decision.capture_healthy
                and health_decision.research_eligible
            )
            else ComponentHealth.DEGRADED
        )
        processor_summary = processor.summary()
        self._metrics.set_gauge("mma_healthy_books", processor_summary.healthy_books)
        self._metrics.set_gauge("mma_journal_queue_high_water", journal_metrics.queue_high_water)
        logger.info(
            "observation_completed",
            "bounded public observation completed",
            book_health=status.value,
            count=journal_metrics.durable,
            duration_ms=actual_duration_nanoseconds / 1_000_000,
        )
        return ObservationSummary(
            collector_run_id=run_id,
            status=status,
            started_at=started,
            ended_at=ended,
            requested_duration_seconds=duration_seconds,
            actual_duration_nanoseconds=actual_duration_nanoseconds,
            target_selection_mode=target_selection_mode,
            target=target,
            selected_condition_ids=tuple(item.condition_id for item in selected),
            selected_asset_ids=selected_assets,
            journal_root=journal_root,
            journal_metrics=journal_metrics,
            processor=processor_summary,
            analytical_outputs=analytical_summary,
            health_evidence=health_evidence,
            health_decision=health_decision,
            public_read_metrics=public_read_metrics,
            runtime_health=runtime_health_snapshot,
            observability_metrics=self._metrics.snapshot(),
            ingress_metrics=stream_result.ingress_metrics,
            reconnects=stream_result.reconnects,
            selected_market_resolved=stream_result.selected_market_resolved,
            protocol_deviations=tuple(deviations),
            websocket_observation_nanoseconds=websocket_observation_nanoseconds,
        )

    @staticmethod
    def _venue_id() -> VenueId:
        return POLYMARKET_VENUE_ID

    async def _bootstrap(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        processor: CanonicalProcessor,
        deadline: float | None = None,
    ) -> tuple[
        tuple[GammaMarket, ...],
        tuple[str, ...],
        ResolvedPolymarketTarget,
    ]:
        discovery = await client.fetch_markets_keyset_raw(
            limit=self._config.polymarket.discovery_limit,
            closed=False,
            deadline=deadline,
        )
        await capture.preserve_then_process(
            channel=GAMMA_MARKETS_KEYSET,
            payload=discovery,
            transport_type="HTTP",
            semantics_hash=_semantics(
                GAMMA_MARKETS_KEYSET,
                f"limit={self._config.polymarket.discovery_limit};closed=false",
            ),
        )
        eligible = tuple(
            market for market in processor.discovered_markets if _eligible_market(market)
        )
        if not eligible:
            raise RuntimeError("bounded discovery returned no open ordinary binary market")
        # Phase 1 deliberately observes one exact condition per socket, keeping
        # every raw message tied to one current reference scope.
        selected = eligible[:1]
        for market in selected:
            await self._refresh_reference(client, capture, market, deadline=deadline)
            for asset_id in market.clob_token_ids:
                await self._fetch_book(
                    client,
                    capture,
                    asset_id,
                    audit=False,
                    deadline=deadline,
                )
        selected_assets = tuple(asset for market in selected for asset in market.clob_token_ids)
        target = self._resolve_target(
            processor,
            selected[0],
            requested_gamma_market_id=None,
        )
        return selected, selected_assets, target

    async def _bootstrap_explicit(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        processor: CanonicalProcessor,
        gamma_market_id: str,
        deadline: float | None = None,
    ) -> tuple[
        tuple[GammaMarket, ...],
        tuple[str, ...],
        ResolvedPolymarketTarget,
    ]:
        """Resolve one operator-selected market without consulting discovery.

        Every response is preserved before it is interpreted.  Any missing,
        mismatched, unsupported, or non-canonical response aborts this exact
        target; this path never substitutes a discovered market.
        """

        gamma_payload = await client.fetch_gamma_market_raw(
            gamma_market_id,
            deadline=deadline,
        )
        gamma_channel = f"{GAMMA_MARKET_PREFIX}{gamma_market_id}"
        gamma_result = await capture.preserve_then_process(
            channel=gamma_channel,
            payload=gamma_payload,
            transport_type="HTTP",
            semantics_hash=_semantics(gamma_channel),
        )
        self._require_target_surface(gamma_result, surface="Gamma market")
        matching_markets = tuple(
            market
            for market in processor.discovered_markets
            if market.gamma_market_id == gamma_market_id
        )
        if len(matching_markets) != 1:
            raise TargetSelectionError(
                "direct Gamma response did not resolve exactly the requested market ID"
            )
        market = matching_markets[0]
        require_supported_gamma_market(
            market,
            requested_gamma_market_id=gamma_market_id,
        )

        if market.event_id is None:
            market = await self._bind_parent_event_relationship(
                client=client,
                capture=capture,
                direct_market=market,
                deadline=deadline,
            )

        event_id = market.event_id
        if event_id is not None:
            event_payload = await client.fetch_gamma_event_raw(
                event_id,
                deadline=deadline,
            )
            event_channel = f"{GAMMA_EVENT_PREFIX}{event_id}"
            event_result = await capture.preserve_then_process(
                channel=event_channel,
                payload=event_payload,
                transport_type="HTTP",
                semantics_hash=_semantics(event_channel),
            )
            self._require_target_surface(event_result, surface="Gamma event")

        clob_payload = await client.fetch_clob_market_raw(
            market.condition_id,
            deadline=deadline,
        )
        clob_channel = f"{CLOB_MARKET_PREFIX}{market.condition_id}"
        clob_result = await capture.preserve_then_process(
            channel=clob_channel,
            payload=clob_payload,
            transport_type="HTTP",
            semantics_hash=_semantics(clob_channel),
        )
        self._require_target_surface(clob_result, surface="CLOB market")

        for asset_id in market.clob_token_ids:
            book_result = await self._fetch_book(
                client,
                capture,
                asset_id,
                audit=False,
                deadline=deadline,
            )
            self._require_target_surface(
                book_result,
                surface=f"CLOB book for token {asset_id}",
            )

        target = self._resolve_target(
            processor,
            market,
            requested_gamma_market_id=gamma_market_id,
        )
        return (market,), market.clob_token_ids, target

    async def _bind_parent_event_relationship(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        direct_market: GammaMarket,
        deadline: float | None,
    ) -> GammaMarket:
        """Resolve an exact zero-or-one parent relationship omitted by the direct surface."""

        gamma_market_id = direct_market.gamma_market_id
        relationship_payload = await client.fetch_gamma_market_relationship_raw(
            gamma_market_id,
            deadline=deadline,
        )
        relationship_channel = f"{GAMMA_MARKET_RELATIONSHIP_PREFIX}{gamma_market_id}"
        relationship_result = await capture.preserve_then_process(
            channel=relationship_channel,
            payload=relationship_payload,
            transport_type="HTTP",
            semantics_hash=_semantics(relationship_channel),
        )
        self._require_target_surface(
            relationship_result,
            surface="Gamma exact-ID market relationship",
        )
        matching_markets = tuple(
            market
            for market in capture.processor.discovered_markets
            if market.gamma_market_id == gamma_market_id
        )
        if len(matching_markets) != 1:
            raise TargetSelectionError(
                "Gamma relationship response did not resolve exactly the requested market ID"
            )
        resolved = matching_markets[0]
        if (
            resolved.condition_id != direct_market.condition_id
            or resolved.clob_token_ids != direct_market.clob_token_ids
        ):
            raise TargetSelectionError(
                "Gamma relationship response differs from the direct market identity"
            )
        return resolved

    @staticmethod
    def _resolve_target(
        processor: CanonicalProcessor,
        market: GammaMarket,
        *,
        requested_gamma_market_id: str | None,
    ) -> ResolvedPolymarketTarget:
        bundles = tuple(
            bundle
            for bundle in processor.current_bundles
            if bundle.market.native_condition_id == market.condition_id
            and bundle.market.gamma_market_id == market.gamma_market_id
        )
        if len(bundles) != 1:
            raise TargetSelectionError(
                "selected target did not resolve exactly one canonical reference bundle"
            )
        return resolve_polymarket_target(
            market,
            bundles[0],
            requested_gamma_market_id=requested_gamma_market_id,
        )

    @staticmethod
    def _require_target_surface(result: ProcessedEnvelope, *, surface: str) -> None:
        if result.success:
            return
        detail = (
            "unknown canonicalization failure" if result.error_type is None else result.error_type
        )
        raise TargetSelectionError(f"{surface} could not be canonicalized: {detail}")

    async def _refresh_reference(
        self,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        market: GammaMarket,
        *,
        deadline: float | None = None,
    ) -> None:
        await self._refresh_reference_by_id(
            client,
            capture,
            gamma_market_id=market.gamma_market_id,
            fallback=market,
            deadline=deadline,
        )

    async def _refresh_reference_by_id(
        self,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        *,
        gamma_market_id: str,
        fallback: GammaMarket | None = None,
        deadline: float | None = None,
    ) -> None:
        gamma_payload = await client.fetch_gamma_market_raw(
            gamma_market_id,
            deadline=deadline,
        )
        channel = f"{GAMMA_MARKET_PREFIX}{gamma_market_id}"
        gamma_result = await capture.preserve_then_process(
            channel=channel,
            payload=gamma_payload,
            transport_type="HTTP",
            semantics_hash=_semantics(channel),
        )
        self._require_target_surface(gamma_result, surface="Gamma market")
        direct_market = parse_gamma_market(gamma_payload)
        if fallback is not None and (
            direct_market.gamma_market_id != fallback.gamma_market_id
            or direct_market.condition_id != fallback.condition_id
            or direct_market.clob_token_ids != fallback.clob_token_ids
        ):
            raise TargetSelectionError(
                "Gamma refresh differs from the frozen selected market identity"
            )
        current = next(
            (
                item
                for item in capture.processor.discovered_markets
                if item.gamma_market_id == gamma_market_id
            ),
            fallback,
        )
        if current is None:
            raise RuntimeError("Gamma direct-market refresh did not publish its market")
        if direct_market.event_id is None:
            current = await self._bind_parent_event_relationship(
                client=client,
                capture=capture,
                direct_market=direct_market,
                deadline=deadline,
            )
        if fallback is not None and current.event_id != fallback.event_id:
            raise TargetSelectionError(
                "Gamma refresh differs from the frozen selected parent event"
            )
        event_id = current.event_id
        if event_id is not None:
            event_payload = await client.fetch_gamma_event_raw(
                event_id,
                deadline=deadline,
            )
            event_channel = f"{GAMMA_EVENT_PREFIX}{event_id}"
            event_result = await capture.preserve_then_process(
                channel=event_channel,
                payload=event_payload,
                transport_type="HTTP",
                semantics_hash=_semantics(event_channel),
            )
            self._require_target_surface(event_result, surface="Gamma event")
        clob_payload = await client.fetch_clob_market_raw(
            current.condition_id,
            deadline=deadline,
        )
        clob_channel = f"{CLOB_MARKET_PREFIX}{current.condition_id}"
        await capture.preserve_then_process(
            channel=clob_channel,
            payload=clob_payload,
            transport_type="HTTP",
            semantics_hash=_semantics(clob_channel),
        )

    @staticmethod
    async def _fetch_book(
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        asset_id: str,
        *,
        audit: bool,
        deadline: float | None = None,
    ) -> ProcessedEnvelope:
        payload = await client.fetch_order_book_raw(
            asset_id,
            deadline=deadline,
        )
        channel = REST_BOOK_AUDIT if audit else REST_BOOK
        return await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=_semantics(channel, f"token_id={asset_id}"),
        )

    async def _resnapshot(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
        refresh_reference: bool = True,
    ) -> None:
        for market in selected:
            if refresh_reference:
                await self._refresh_reference(
                    client,
                    capture,
                    market,
                    deadline=deadline,
                )
            for asset_id in market.clob_token_ids:
                await self._fetch_book(
                    client,
                    capture,
                    asset_id,
                    audit=False,
                    deadline=deadline,
                )

    async def _stage_audit_start(
        self,
        *,
        capture: _RawCaptureBoundary,
        ingress: BoundedRawIngress,
        native_asset_id: str,
        attempt: int,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        request_started_monotonic_ns: int | None = None,
        hard_expiry_monotonic_ns: int | None = None,
        response_deadline_monotonic_ns: int | None = None,
        run_deadline_monotonic_ns: int | None = None,
    ) -> tuple[AuditStartEvidence, ProcessedEnvelope]:
        request_started_ns = (
            time.monotonic_ns()
            if request_started_monotonic_ns is None
            else request_started_monotonic_ns
        )
        start = AuditStartEvidence(
            request_id=uuid.uuid4().hex,
            connection_id=connection_id,
            native_asset_id=native_asset_id,
            attempt=attempt,
            request_started_monotonic_ns=request_started_ns,
            starting_admission_watermark=ingress.snapshot_watermark(),
            starting_processed_watermark=(
                capture.processor.processed_websocket_watermark(connection_id)
            ),
            starting_book=capture.processor.audit_book_identity(native_asset_id),
        )
        metadata: dict[str, str | int | bool] = {
            "audit_attempt": attempt,
            "request_id": start.request_id,
        }
        if hard_expiry_monotonic_ns is None and (
            response_deadline_monotonic_ns is not None or run_deadline_monotonic_ns is not None
        ):
            raise ValueError("audit response/run deadline requires a hard expiry")
        if hard_expiry_monotonic_ns is not None:
            if run_deadline_monotonic_ns is None:
                raise ValueError("bound audit start requires the run deadline")
            metadata["hard_expiry_monotonic_ns"] = hard_expiry_monotonic_ns
            metadata["response_deadline_monotonic_ns"] = (
                hard_expiry_monotonic_ns
                if response_deadline_monotonic_ns is None
                else response_deadline_monotonic_ns
            )
            metadata["run_deadline_monotonic_ns"] = run_deadline_monotonic_ns
        result = await capture.preserve_then_process(
            channel=CONTROL_REST_AUDIT_START,
            payload=start.to_payload(),
            transport_type="CONTROL",
            semantics_hash=_semantics(CONTROL_REST_AUDIT_START),
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=metadata,
            received_wall_time=UtcTimestamp.now(),
            received_monotonic_ns=request_started_ns,
        )
        return start, result

    @staticmethod
    async def _run_audit_http_request(
        *,
        client: PolymarketPublicClient,
        ingress: BoundedRawIngress,
        start: AuditStartEvidence,
        deadline: float,
        handoff: asyncio.Queue[AuditHttpResponseHandoff | AuditHttpFailureHandoff],
        response_fence: _AuditResponseBoundaryFence | None = None,
        deadline_monotonic_ns: int | None = None,
        deadline_failure_reason: AuditFailureReason = AuditFailureReason.OBSERVATION_DEADLINE,
    ) -> None:
        if response_fence is None:
            response_fence = _AuditResponseBoundaryFence(
                request_id=start.request_id,
                connection_id=start.connection_id,
            )

        def announce_response_boundary(
            received_wall_time: UtcTimestamp,
            received_monotonic_ns: int,
        ) -> Callable[[], None]:
            response_fence.announce(
                ingress=ingress,
                received_wall_time=received_wall_time,
                received_monotonic_ns=received_monotonic_ns,
            )
            return response_fence.abandon

        try:
            response = await client.fetch_order_book_raw_with_evidence(
                start.native_asset_id,
                deadline=deadline,
                on_response_body_complete=announce_response_boundary,
            )
        except PolymarketHttpError as error:
            reason = (
                deadline_failure_reason
                if error.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
                else AuditFailureReason.HTTP_REQUEST_FAILED
            )
            item: AuditHttpResponseHandoff | AuditHttpFailureHandoff = AuditHttpFailureHandoff(
                request_id=start.request_id,
                connection_id=start.connection_id,
                reason=reason,
                error_type=f"{type(error).__name__}:{error.category.value}",
            )
        else:
            if response_fence.error_type is not None:
                item = AuditHttpFailureHandoff(
                    request_id=start.request_id,
                    connection_id=start.connection_id,
                    reason=AuditFailureReason.RESPONSE_WATERMARK_UNAVAILABLE,
                    error_type=response_fence.error_type,
                )
            elif (
                not response_fence.announced
                or response_fence.watermark is None
                or response_fence.received_wall_time is None
                or response_fence.received_monotonic_ns is None
            ):
                item = AuditHttpFailureHandoff(
                    request_id=start.request_id,
                    connection_id=start.connection_id,
                    reason=AuditFailureReason.RESPONSE_WATERMARK_UNAVAILABLE,
                    error_type="MissingResponseBoundaryAnnouncement",
                )
            elif (
                deadline_monotonic_ns is not None
                and response_fence.received_monotonic_ns >= deadline_monotonic_ns
            ):
                item = AuditHttpFailureHandoff(
                    request_id=start.request_id,
                    connection_id=start.connection_id,
                    reason=deadline_failure_reason,
                    error_type="ResponseReceivedAtOrAfterObservationDeadline",
                )
            elif (
                response_fence.received_wall_time != response.evidence.response_received_wall_time
                or response_fence.received_monotonic_ns
                != response.evidence.response_received_monotonic_ns
            ):
                item = AuditHttpFailureHandoff(
                    request_id=start.request_id,
                    connection_id=start.connection_id,
                    reason=AuditFailureReason.RESPONSE_WATERMARK_UNAVAILABLE,
                    error_type="ResponseBoundaryEvidenceMismatch",
                )
            else:
                item = AuditHttpResponseHandoff(
                    request_id=start.request_id,
                    connection_id=start.connection_id,
                    payload_bytes=response.payload_bytes,
                    received_wall_time=response_fence.received_wall_time,
                    received_monotonic_ns=response_fence.received_monotonic_ns,
                    response_boundary_watermark=response_fence.watermark,
                )
        try:
            handoff.put_nowait(item)
        except asyncio.QueueFull as error:
            raise RuntimeError("bounded audit response handoff unexpectedly saturated") from error

    @staticmethod
    async def _preserve_audit_failure(
        *,
        capture: _RawCaptureBoundary,
        failure: AuditHttpFailureHandoff,
        subscription_id: SubscriptionId,
    ) -> ProcessedEnvelope:
        return await capture.preserve_then_process(
            channel=CONTROL_REST_AUDIT_FAILED,
            payload=AuditFailureCommand(
                request_id=failure.request_id,
                reason=failure.reason,
            ).to_payload(),
            transport_type="CONTROL",
            semantics_hash=_semantics(CONTROL_REST_AUDIT_FAILED),
            connection_id=failure.connection_id,
            subscription_id=subscription_id,
            transport_metadata={"request_id": failure.request_id},
        )

    @staticmethod
    async def _preserve_audit_expiry(
        *,
        capture: _RawCaptureBoundary,
        native_asset_id: str,
        last_accepted_receive_monotonic_ns: int | None,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> ProcessedEnvelope:
        command = AuditExpiryCommand(
            native_asset_id=native_asset_id,
            last_accepted_receive_monotonic_ns=last_accepted_receive_monotonic_ns,
        )
        return await capture.preserve_then_process(
            channel=CONTROL_REST_AUDIT_EXPIRED,
            payload=command.to_payload(),
            transport_type="CONTROL",
            semantics_hash=_semantics(CONTROL_REST_AUDIT_EXPIRED),
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata={"native_asset_id": native_asset_id},
        )

    @staticmethod
    async def _preserve_audit_response(
        *,
        capture: _RawCaptureBoundary,
        response: AuditHttpResponseHandoff,
        start: AuditStartEvidence,
        barrier_ordinal: int,
        subscription_id: SubscriptionId,
        classification_monotonic_ns: int | None = None,
        hard_expiry_monotonic_ns: int | None = None,
        forced_failure_reason: AuditFailureReason | None = None,
    ) -> ProcessedEnvelope:
        metadata: dict[str, str | int | bool] = {
            "audit_attempt": start.attempt,
            "barrier_websocket_ordinal": barrier_ordinal,
            "classification_deferred": True,
            "request_id": response.request_id,
            "response_boundary_websocket_ordinal": (response.response_boundary_watermark.ordinal),
        }
        staged = await capture.preserve_then_process(
            channel=REST_BOOK_AUDIT,
            payload=response.payload_bytes,
            transport_type="HTTP",
            semantics_hash=_semantics(
                REST_BOOK_AUDIT,
                f"token_id={start.native_asset_id}",
            ),
            connection_id=response.connection_id,
            subscription_id=subscription_id,
            transport_metadata=metadata,
            received_wall_time=response.received_wall_time,
            received_monotonic_ns=response.received_monotonic_ns,
        )
        if not staged.success or staged.audit_outcomes:
            raise RuntimeError("durable audit response could not enter deferred classification")
        decision_ns = (
            time.monotonic_ns()
            if classification_monotonic_ns is None
            else classification_monotonic_ns
        )
        hard_expiry_ns = (
            (2**63) - 1 if hard_expiry_monotonic_ns is None else hard_expiry_monotonic_ns
        )
        derived_failure = (
            AuditFailureReason.BOOK_FRESHNESS_EXPIRED if decision_ns >= hard_expiry_ns else None
        )
        if forced_failure_reason is not None and forced_failure_reason is not derived_failure:
            raise ValueError("forced audit failure differs from the classification deadline")
        decision = AuditClassificationCommand(
            request_id=response.request_id,
            response_ingest_seq=staged.ingest_seq,
            classification_monotonic_ns=decision_ns,
            hard_expiry_monotonic_ns=hard_expiry_ns,
        )
        return await capture.preserve_then_process(
            channel=CONTROL_REST_AUDIT_CLASSIFY,
            payload=decision.to_payload(),
            transport_type="CONTROL",
            semantics_hash=_semantics(CONTROL_REST_AUDIT_CLASSIFY),
            connection_id=response.connection_id,
            subscription_id=subscription_id,
            transport_metadata={"request_id": response.request_id},
            received_monotonic_ns=decision_ns,
        )

    async def _handle_refresh_requests(
        self,
        *,
        requests: tuple[ReferenceRefreshRequest, ...],
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
        runtime_health: ObservationRuntimeHealth | None = None,
        metrics_already_recorded: bool = False,
    ) -> bool:
        selected_by_gamma_id = {market.gamma_market_id: market for market in selected}
        selected_by_condition = {market.condition_id: market for market in selected}
        selected_by_asset = {
            asset_id: market for market in selected for asset_id in market.clob_token_ids
        }
        selected_resolved = False
        refresh_gamma_ids: set[str] = set()
        for request in requests:
            if not metrics_already_recorded:
                self._metrics.increment_counter(
                    "mma_reference_refresh_requests_total",
                    labels={"event_type": request.event_type.value},
                )
            matching_market = selected_by_gamma_id.get(request.native_market_id)
            if matching_market is None and request.native_condition_id is not None:
                matching_market = selected_by_condition.get(request.native_condition_id)
            if matching_market is None:
                for asset_id in request.native_asset_ids:
                    matching_market = selected_by_asset.get(asset_id)
                    if matching_market is not None:
                        break
            if matching_market is None:
                if not metrics_already_recorded:
                    self._metrics.increment_counter(
                        "mma_reference_refresh_requests_ignored_total",
                        labels={"event_type": request.event_type.value},
                    )
                continue
            refresh_gamma_ids.add(matching_market.gamma_market_id)
            if (
                request.event_type is CanonicalEventType.MARKET_RESOLVED
                and request.native_condition_id == matching_market.condition_id
            ):
                selected_resolved = True
        for market in selected:
            if market.gamma_market_id not in refresh_gamma_ids:
                continue
            await self._refresh_reference_by_id(
                client,
                capture,
                gamma_market_id=market.gamma_market_id,
                fallback=market,
                deadline=deadline,
            )
        if refresh_gamma_ids and not selected_resolved:
            if runtime_health is not None:
                runtime_health.record_resync()
            await self._resnapshot(
                client=client,
                capture=capture,
                selected=tuple(
                    market for market in selected if market.gamma_market_id in refresh_gamma_ids
                ),
                deadline=deadline,
                refresh_reference=False,
            )
        return selected_resolved

    async def _run_generation(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
        asset_ids: tuple[str, ...],
        deadline: float,
        deadline_ns: int,
        periodic_audit_due_ns: int,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        unavailable_started_ns: int | None,
        runtime_health: ObservationRuntimeHealth | None = None,
        on_capture_cutoff: Callable[[_CaptureCutoffMoment], None] | None = None,
    ) -> _GenerationResult:
        if runtime_health is None:
            runtime_health = ObservationRuntimeHealth()
        socket = PolymarketMarketWebSocket(
            asset_ids,
            url=self._config.polymarket.market_websocket_url,
            ping_interval_seconds=self._config.polymarket.heartbeat_interval_seconds,
            max_queue=self._config.system.queue_capacity,
            max_message_bytes=self._config.polymarket.maximum_websocket_message_bytes,
        )
        ingress = BoundedRawIngress(
            max_items=self._config.system.queue_capacity,
            max_bytes=self._config.polymarket.maximum_ingress_queue_bytes,
            connection_id=connection_id,
        )
        runtime_health.begin_generation(connection_id, ingress)
        ready = asyncio.Event()
        acquisition_ended_monotonic_ns: int | None = None

        async def acquire() -> None:
            nonlocal acquisition_ended_monotonic_ns
            # `raw_messages` is retained solely for existing adapter fixtures;
            # production uses pump_received so stamping+admission has no yield gap.
            try:
                if hasattr(socket, "pump_received"):
                    await socket.pump_received(ingress.admit_nowait, ready=ready)
                    return
                ready.set()
                async for payload in socket.raw_messages():
                    ingress.admit_nowait(
                        ReceivedWebSocketMessage(
                            payload_bytes=payload,
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=time.monotonic_ns(),
                            message_type="BINARY",
                        )
                    )
            finally:
                acquisition_ended_monotonic_ns = time.monotonic_ns()

        acquisition_task = asyncio.create_task(
            acquire(),
            name="polymarket-public-socket-acquisition",
        )
        ready_task = asyncio.create_task(
            ready.wait(),
            name="polymarket-public-subscription-ready",
        )
        maximum_book_age_ns = int(self._config.scanner.maximum_book_age_seconds * 1e9)
        freshness_audit_guard_ns = int(
            self._config.polymarket.snapshot_audit_freshness_guard_seconds * 1e9
        )
        integrity_interval_ns = int(self._config.polymarket.snapshot_audit_interval_seconds * 1e9)
        pending_exit: _GenerationExit | None = None
        capture_cutoff: _CaptureCutoffMoment | None = None
        pending_transport_error: PolymarketWebSocketError | None = None
        deviations: list[str] = []
        refresh_requests: list[ReferenceRefreshRequest] = []
        # One binary market has exactly two explicitly selected legs.  Both
        # first attempts may be in flight, but the canonical processor below
        # remains the sole serialized state owner.
        if not 0 < len(asset_ids) <= 2:
            raise RuntimeError("continuous audit requires one or two bounded legs")
        audit_handoff = asyncio.Queue[AuditHttpResponseHandoff | AuditHttpFailureHandoff](maxsize=2)
        pending_audits = dict[str, _PendingAuditRequest]()
        deferred_item: RawIngressItem | None = None
        pending_ingress_get_task: asyncio.Task[RawIngressItem] | None = None
        audit_cycle_active = False
        cycle_unfinished_assets: set[str] = set()
        first_attempts_pending: set[str] = set()
        deferred_retry_actions: dict[str, tuple[int, bool]] = {}
        expired_freshness_markers: dict[str, int | None] = {}
        maximum_race_retries = self._config.polymarket.snapshot_audit_max_race_retries
        stable_asset_order = {asset_id: index for index, asset_id in enumerate(asset_ids)}
        selected_gamma_ids = {market.gamma_market_id for market in selected}
        selected_condition_ids = {market.condition_id for market in selected}
        selected_asset_ids = {asset_id for market in selected for asset_id in market.clob_token_ids}

        def refresh_matches_selected(request: ReferenceRefreshRequest) -> bool:
            return (
                request.native_market_id in selected_gamma_ids
                or (
                    request.native_condition_id is not None
                    and request.native_condition_id in selected_condition_ids
                )
                or any(asset_id in selected_asset_ids for asset_id in request.native_asset_ids)
            )

        def audit_launch_block_reason() -> str | None:
            if time.monotonic_ns() >= deadline_ns:
                return "run_deadline"
            return (
                "generation_exit"
                if (
                    pending_exit is not None
                    or pending_transport_error is not None
                    or acquisition_task.done()
                )
                else None
            )

        def leg_freshness_ns(native_asset_id: str) -> int | None:
            instrument_id = capture.processor.audit_book_identity(native_asset_id).instrument_id
            return capture.book_freshness_receive_ns_by_instrument.get(instrument_id)

        def leg_hard_expiry_ns(native_asset_id: str, *, now_ns: int) -> int:
            freshness_ns = leg_freshness_ns(native_asset_id)
            return now_ns if freshness_ns is None else freshness_ns + maximum_book_age_ns

        def ordered_cycle_assets(*, now_ns: int) -> tuple[str, ...]:
            return tuple(
                sorted(
                    asset_ids,
                    key=lambda asset_id: (
                        leg_hard_expiry_ns(asset_id, now_ns=now_ns),
                        stable_asset_order[asset_id],
                    ),
                )
            )

        def current_deadline() -> IntegrityDeadline:
            if audit_cycle_active:
                return IntegrityDeadline(
                    deadline_ns=deadline_ns,
                    reason=IntegrityDeadlineReason.RUN_DEADLINE,
                )
            now_ns = time.monotonic_ns()
            freshness_deadlines: list[int] = []
            hard_expiries: list[int] = []
            for asset_id in asset_ids:
                freshness_ns = leg_freshness_ns(asset_id)
                if expired_freshness_markers.get(asset_id, object()) == freshness_ns:
                    # This exact accepted freshness boundary has already
                    # expired.  Wait for a new accepted L2 update rather than
                    # spinning public reads after the per-leg hard deadline.
                    continue
                hard_expiry_ns = (
                    now_ns if freshness_ns is None else freshness_ns + maximum_book_age_ns
                )
                hard_expiries.append(hard_expiry_ns)
                if hard_expiry_ns <= deadline_ns:
                    freshness_deadlines.append(
                        max(now_ns, hard_expiry_ns - freshness_audit_guard_ns)
                    )
            freshness_deadline_ns = min(freshness_deadlines) if freshness_deadlines else deadline_ns
            periodic_deadline_ns = periodic_audit_due_ns
            if (
                deadline_ns - now_ns <= freshness_audit_guard_ns
                and len(hard_expiries) == len(asset_ids)
                and all(expiry_ns > deadline_ns for expiry_ns in hard_expiries)
            ):
                periodic_deadline_ns = deadline_ns
            next_ns = min(deadline_ns, periodic_deadline_ns, freshness_deadline_ns)
            if next_ns == deadline_ns:
                reason = IntegrityDeadlineReason.RUN_DEADLINE
            elif next_ns == periodic_deadline_ns:
                reason = IntegrityDeadlineReason.PERIODIC_AUDIT
            else:
                reason = IntegrityDeadlineReason.BOOK_FRESHNESS
            return IntegrityDeadline(deadline_ns=next_ns, reason=reason)

        def finish_cycle_leg(native_asset_id: str) -> None:
            nonlocal audit_cycle_active, periodic_audit_due_ns
            first_attempts_pending.discard(native_asset_id)
            deferred_retry_actions.pop(native_asset_id, None)
            cycle_unfinished_assets.discard(native_asset_id)
            if not cycle_unfinished_assets:
                audit_cycle_active = False
                periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns

        async def mark_expired_leg(native_asset_id: str, *, reason: str) -> None:
            nonlocal pending_exit
            freshness_ns = leg_freshness_ns(native_asset_id)
            result = await self._preserve_audit_expiry(
                capture=capture,
                native_asset_id=native_asset_id,
                last_accepted_receive_monotonic_ns=freshness_ns,
                connection_id=connection_id,
                subscription_id=subscription_id,
            )
            if not result.success:
                raise RuntimeError("audit expiry control failed canonical processing")
            if result.needs_resync:
                pending_exit = _GenerationExit.RESYNC_REQUIRED
            expired_freshness_markers[native_asset_id] = freshness_ns
            self._metrics.increment_counter(
                "mma_rest_audit_legs_suppressed_total",
                labels={"reason": reason},
            )
            deviations.append(f"REST audit leg expired before launch: {native_asset_id}")
            finish_cycle_leg(native_asset_id)

        def start_staged_audit_http(
            staged: _StagedAuditRequest,
            *,
            launch_monotonic_ns: int,
        ) -> None:
            start = staged.start
            response_fence = _AuditResponseBoundaryFence(
                request_id=start.request_id,
                connection_id=start.connection_id,
            )
            public_deadline = min(
                deadline,
                asyncio.get_running_loop().time()
                + max(0.0, (staged.hard_expiry_ns - launch_monotonic_ns) / 1e9),
            )
            task = asyncio.create_task(
                self._run_audit_http_request(
                    client=client,
                    ingress=ingress,
                    start=start,
                    deadline=public_deadline,
                    handoff=audit_handoff,
                    response_fence=response_fence,
                    deadline_monotonic_ns=staged.response_deadline_ns,
                    deadline_failure_reason=(
                        AuditFailureReason.BOOK_FRESHNESS_EXPIRED
                        if staged.hard_expiry_ns < deadline_ns
                        else AuditFailureReason.OBSERVATION_DEADLINE
                    ),
                ),
                name=f"polymarket-rest-audit-{start.request_id}",
            )
            pending_audits[start.request_id] = _PendingAuditRequest(
                start=start,
                response_fence=response_fence,
                task=task,
                freshness_marker_ns=staged.freshness_marker_ns,
                hard_expiry_ns=staged.hard_expiry_ns,
                asset_order=staged.asset_order,
            )

        async def close_staged_audit(
            staged: _StagedAuditRequest,
            *,
            reason: AuditFailureReason,
        ) -> None:
            result = await self._preserve_audit_failure(
                capture=capture,
                failure=AuditHttpFailureHandoff(
                    request_id=staged.start.request_id,
                    connection_id=staged.start.connection_id,
                    reason=reason,
                    error_type=reason.value,
                ),
                subscription_id=subscription_id,
            )
            if result.audit_outcomes:
                self._metrics.increment_counter(
                    "mma_rest_audit_outcomes_total",
                    labels={"outcome": result.audit_outcomes[-1].outcome.value},
                )
            if reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED:
                expired_freshness_markers[staged.start.native_asset_id] = staged.freshness_marker_ns
            finish_cycle_leg(staged.start.native_asset_id)

        async def launch_audit(
            native_asset_id: str,
            *,
            attempt: int,
            deferred_starts: list[_StagedAuditRequest] | None = None,
        ) -> _AuditLaunchResult:
            launch_block_reason = audit_launch_block_reason()
            if launch_block_reason == "generation_exit":
                self._metrics.increment_counter(
                    "mma_rest_audit_launches_suppressed_total",
                    labels={"reason": launch_block_reason},
                )
                finish_cycle_leg(native_asset_id)
                return _AuditLaunchResult.GENERATION_ENDED
            request_started_ns = time.monotonic_ns()
            if request_started_ns >= deadline_ns:
                self._metrics.increment_counter(
                    "mma_rest_audit_launches_suppressed_total",
                    labels={"reason": "run_deadline_before_stage"},
                )
                return _AuditLaunchResult.RUN_ENDED
            freshness_marker_ns = leg_freshness_ns(native_asset_id)
            hard_expiry_ns = (
                request_started_ns
                if freshness_marker_ns is None
                else freshness_marker_ns + maximum_book_age_ns
            )
            if request_started_ns >= hard_expiry_ns:
                await mark_expired_leg(native_asset_id, reason="leg_expired_before_stage")
                return _AuditLaunchResult.EXPIRED
            hard_deadline_ns = min(deadline_ns, hard_expiry_ns)
            start, result = await self._stage_audit_start(
                capture=capture,
                ingress=ingress,
                native_asset_id=native_asset_id,
                attempt=attempt,
                connection_id=connection_id,
                subscription_id=subscription_id,
                request_started_monotonic_ns=request_started_ns,
                hard_expiry_monotonic_ns=hard_expiry_ns,
                response_deadline_monotonic_ns=hard_deadline_ns,
                run_deadline_monotonic_ns=deadline_ns,
            )
            if not result.success or result.needs_resync:
                deviations.append(f"audit start failed closed at ingest_seq={result.ingest_seq}")
                if result.success:
                    await self._preserve_audit_failure(
                        capture=capture,
                        failure=AuditHttpFailureHandoff(
                            request_id=start.request_id,
                            connection_id=start.connection_id,
                            reason=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE,
                            error_type=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE.value,
                        ),
                        subscription_id=subscription_id,
                    )
                finish_cycle_leg(native_asset_id)
                return _AuditLaunchResult.FAILED_CLOSED
            after_stage_ns = time.monotonic_ns()
            if (
                pending_exit is not None
                or pending_transport_error is not None
                or acquisition_task.done()
            ):
                await self._preserve_audit_failure(
                    capture=capture,
                    failure=AuditHttpFailureHandoff(
                        request_id=start.request_id,
                        connection_id=start.connection_id,
                        reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
                        error_type=AuditFailureReason.SOCKET_GENERATION_CHANGED.value,
                    ),
                    subscription_id=subscription_id,
                )
                finish_cycle_leg(native_asset_id)
                return _AuditLaunchResult.GENERATION_ENDED
            if after_stage_ns >= hard_deadline_ns:
                self._metrics.increment_counter(
                    "mma_rest_audit_launches_suppressed_total",
                    labels={
                        "reason": (
                            "run_deadline_after_stage"
                            if deadline_ns <= hard_expiry_ns
                            else "leg_expired_after_stage"
                        )
                    },
                )
                failure_reason = (
                    AuditFailureReason.RUN_ENDED
                    if deadline_ns <= hard_expiry_ns
                    else AuditFailureReason.BOOK_FRESHNESS_EXPIRED
                )
                await self._preserve_audit_failure(
                    capture=capture,
                    failure=AuditHttpFailureHandoff(
                        request_id=start.request_id,
                        connection_id=start.connection_id,
                        reason=failure_reason,
                        error_type=failure_reason.value,
                    ),
                    subscription_id=subscription_id,
                )
                if hard_expiry_ns <= deadline_ns:
                    expired_freshness_markers[native_asset_id] = freshness_marker_ns
                finish_cycle_leg(native_asset_id)
                return (
                    _AuditLaunchResult.RUN_ENDED
                    if failure_reason is AuditFailureReason.RUN_ENDED
                    else _AuditLaunchResult.EXPIRED
                )
            staged = _StagedAuditRequest(
                start=start,
                freshness_marker_ns=freshness_marker_ns,
                hard_expiry_ns=hard_expiry_ns,
                response_deadline_ns=hard_deadline_ns,
                asset_order=stable_asset_order[native_asset_id],
            )
            if deferred_starts is None:
                start_staged_audit_http(staged, launch_monotonic_ns=after_stage_ns)
            else:
                deferred_starts.append(staged)
            return _AuditLaunchResult.STARTED

        async def stage_failed_leg(
            native_asset_id: str,
            *,
            attempt: int,
            reason: AuditFailureReason,
        ) -> _AuditLaunchResult:
            """Durably close a bounded leg without starting another HTTP read."""

            launch_block_reason = audit_launch_block_reason()
            if launch_block_reason is not None:
                self._metrics.increment_counter(
                    "mma_rest_audit_launches_suppressed_total",
                    labels={"reason": launch_block_reason},
                )
                finish_cycle_leg(native_asset_id)
                return (
                    _AuditLaunchResult.RUN_ENDED
                    if launch_block_reason == "run_deadline"
                    else _AuditLaunchResult.GENERATION_ENDED
                )
            request_started_ns = time.monotonic_ns()
            if request_started_ns >= deadline_ns:
                finish_cycle_leg(native_asset_id)
                return _AuditLaunchResult.RUN_ENDED
            freshness_marker_ns = leg_freshness_ns(native_asset_id)
            hard_expiry_ns = (
                request_started_ns
                if freshness_marker_ns is None
                else freshness_marker_ns + maximum_book_age_ns
            )
            if request_started_ns >= hard_expiry_ns:
                await mark_expired_leg(
                    native_asset_id,
                    reason="leg_expired_before_terminal_record",
                )
                return _AuditLaunchResult.EXPIRED
            hard_deadline_ns = min(deadline_ns, hard_expiry_ns)
            start, result = await self._stage_audit_start(
                capture=capture,
                ingress=ingress,
                native_asset_id=native_asset_id,
                attempt=attempt,
                connection_id=connection_id,
                subscription_id=subscription_id,
                request_started_monotonic_ns=request_started_ns,
                hard_expiry_monotonic_ns=hard_expiry_ns,
                response_deadline_monotonic_ns=hard_deadline_ns,
                run_deadline_monotonic_ns=deadline_ns,
            )
            if not result.success or result.needs_resync:
                if result.success:
                    failure = AuditHttpFailureHandoff(
                        request_id=start.request_id,
                        connection_id=start.connection_id,
                        reason=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE,
                        error_type=AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE.value,
                    )
                    result = await self._preserve_audit_failure(
                        capture=capture,
                        failure=failure,
                        subscription_id=subscription_id,
                    )
                finish_cycle_leg(native_asset_id)
                return _AuditLaunchResult.FAILED_CLOSED
            after_stage_ns = time.monotonic_ns()
            if (
                pending_exit is not None
                or pending_transport_error is not None
                or acquisition_task.done()
            ):
                result = await self._preserve_audit_failure(
                    capture=capture,
                    failure=AuditHttpFailureHandoff(
                        request_id=start.request_id,
                        connection_id=start.connection_id,
                        reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
                        error_type=AuditFailureReason.SOCKET_GENERATION_CHANGED.value,
                    ),
                    subscription_id=subscription_id,
                )
                if result.audit_outcomes:
                    self._metrics.increment_counter(
                        "mma_rest_audit_outcomes_total",
                        labels={"outcome": result.audit_outcomes[-1].outcome.value},
                    )
                finish_cycle_leg(native_asset_id)
                return _AuditLaunchResult.GENERATION_ENDED
            if after_stage_ns >= hard_deadline_ns:
                failure_reason = (
                    AuditFailureReason.RUN_ENDED
                    if deadline_ns <= hard_expiry_ns
                    else AuditFailureReason.BOOK_FRESHNESS_EXPIRED
                )
                result = await self._preserve_audit_failure(
                    capture=capture,
                    failure=AuditHttpFailureHandoff(
                        request_id=start.request_id,
                        connection_id=start.connection_id,
                        reason=failure_reason,
                        error_type=failure_reason.value,
                    ),
                    subscription_id=subscription_id,
                )
                if result.audit_outcomes:
                    self._metrics.increment_counter(
                        "mma_rest_audit_outcomes_total",
                        labels={"outcome": result.audit_outcomes[-1].outcome.value},
                    )
                if failure_reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED:
                    expired_freshness_markers[native_asset_id] = freshness_marker_ns
                finish_cycle_leg(native_asset_id)
                return (
                    _AuditLaunchResult.RUN_ENDED
                    if failure_reason is AuditFailureReason.RUN_ENDED
                    else _AuditLaunchResult.EXPIRED
                )
            result = await self._preserve_audit_failure(
                capture=capture,
                failure=AuditHttpFailureHandoff(
                    request_id=start.request_id,
                    connection_id=start.connection_id,
                    reason=reason,
                    error_type=reason.value,
                ),
                subscription_id=subscription_id,
            )
            if result.audit_outcomes:
                self._metrics.increment_counter(
                    "mma_rest_audit_outcomes_total",
                    labels={"outcome": result.audit_outcomes[-1].outcome.value},
                )
            expired_freshness_markers[native_asset_id] = freshness_marker_ns
            finish_cycle_leg(native_asset_id)
            return _AuditLaunchResult.STARTED

        async def stop_audit_request(request_id: str) -> None:
            pending = pending_audits.get(request_id)
            if pending is None:
                return
            if not pending.task.done():
                pending.task.cancel()
            with suppress(asyncio.CancelledError):
                await pending.task

        async def fail_audit_request(
            request_id: str,
            reason: AuditFailureReason,
        ) -> bool:
            pending = pending_audits.get(request_id)
            if pending is None:
                return False
            if pending.response is not None:
                # The body has already crossed the immutable response
                # boundary and was dequeued into coordinator-owned state.
                # Return to the ordered response drain; never replace it with
                # a synthetic generation failure.
                return True
            if not pending.task.done():
                pending.task.cancel()
            with suppress(asyncio.CancelledError):
                await pending.task
            queued: list[AuditHttpResponseHandoff | AuditHttpFailureHandoff] = []
            while not audit_handoff.empty():
                queued.append(audit_handoff.get_nowait())
            for item in queued:
                audit_handoff.put_nowait(item)
            if any(
                isinstance(item, AuditHttpResponseHandoff) or item.request_id == request_id
                for item in queued
            ):
                # Cancellation can release a body that was already immutable
                # at any response boundary, while another request may also
                # complete during this await.  Stop the fail-all pass and let
                # the normal serialized path preserve/classify queued bodies
                # in global fence order.
                return True
            pending_audits.pop(request_id, None)
            result = await self._preserve_audit_failure(
                capture=capture,
                failure=AuditHttpFailureHandoff(
                    request_id=pending.start.request_id,
                    connection_id=pending.start.connection_id,
                    reason=reason,
                    error_type=reason.value,
                ),
                subscription_id=subscription_id,
            )
            if result.audit_outcomes:
                self._metrics.increment_counter(
                    "mma_rest_audit_outcomes_total",
                    labels={"outcome": result.audit_outcomes[-1].outcome.value},
                )
            return False

        def announced_audit_order(
            pending: _PendingAuditRequest,
        ) -> tuple[int, int, int, int, int, str]:
            barrier_ordinal = pending.response_fence.barrier_ordinal
            received_monotonic_ns = pending.response_fence.received_monotonic_ns
            if barrier_ordinal is None or received_monotonic_ns is None:
                raise RuntimeError("audit response fence is not fully announced")
            return (
                barrier_ordinal,
                received_monotonic_ns,
                pending.hard_expiry_ns,
                pending.asset_order,
                pending.start.request_started_monotonic_ns,
                pending.start.request_id,
            )

        def earliest_announced_audit() -> _PendingAuditRequest | None:
            announced = tuple(
                pending
                for pending in pending_audits.values()
                if pending.response_fence.barrier_ordinal is not None
            )
            return min(announced, key=announced_audit_order) if announced else None

        async def fail_all_audits(reason: AuditFailureReason) -> bool:
            while pending_audits:
                earliest = earliest_announced_audit()
                if earliest is not None:
                    if earliest.response is not None:
                        # The normal drain owns this immutable body.  A stored
                        # later-fence response never wins here: the globally
                        # earliest announced blocker is selected first.
                        return True
                    request_id = earliest.start.request_id
                else:
                    request_id = min(
                        pending_audits,
                        key=lambda key: (
                            pending_audits[key].start.request_started_monotonic_ns,
                            pending_audits[key].asset_order,
                            key,
                        ),
                    )
                if await fail_audit_request(request_id, reason):
                    return True
            return False

        async def drain_completed_exit_handoffs() -> bool:
            """Drain only immutable handoffs, never a provisional fence."""

            if not audit_handoff.empty():
                return True
            drainable = tuple(
                pending.task for pending in pending_audits.values() if pending.task.done()
            )
            if not drainable:
                return False
            await asyncio.gather(*drainable, return_exceptions=True)
            return not audit_handoff.empty()

        async def complete_audit_response(
            response: AuditHttpResponseHandoff,
        ) -> ProcessedEnvelope:
            pending = pending_audits.get(response.request_id)
            if pending is None:
                raise RuntimeError("audit response handoff has no matching active request")
            processed = capture.processor.processed_websocket_watermark(connection_id).ordinal
            return await self._preserve_audit_response(
                capture=capture,
                response=response,
                start=pending.start,
                barrier_ordinal=processed,
                subscription_id=subscription_id,
                hard_expiry_monotonic_ns=pending.hard_expiry_ns,
            )

        async def stop_acquisition() -> None:
            if capture_cutoff is not None and on_capture_cutoff is not None:
                on_capture_cutoff(capture_cutoff)
            if not acquisition_task.done():
                acquisition_task.cancel()
            with suppress(
                asyncio.CancelledError,
                PolymarketWebSocketError,
                RawIngressBackpressureError,
            ):
                await acquisition_task

        try:
            done, _ = await asyncio.wait(
                {ready_task, acquisition_task},
                timeout=max(0.0, (deadline_ns - time.monotonic_ns()) / 1e9),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if acquisition_task in done and ready_task not in done:
                acquisition_task.result()
                raise PolymarketWebSocketTransportError(
                    category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                    retryable=True,
                    detail="public market WebSocket ended before readiness",
                )
            if ready_task not in done:
                capture_cutoff = _CaptureCutoffMoment(
                    wall_time=UtcTimestamp.now(),
                    monotonic_ns=time.monotonic_ns(),
                )
                await stop_acquisition()
                return _GenerationResult(
                    _GenerationExit.DEADLINE,
                    ingress_metrics=ingress.metrics,
                    capture_cutoff=capture_cutoff,
                )
            runtime_health.mark_connected(now_ns=time.monotonic_ns())
            if unavailable_started_ns is not None:
                self._metrics.observe_histogram(
                    "mma_socket_unavailable_ns",
                    max(0, time.monotonic_ns() - unavailable_started_ns),
                )

            while True:
                handoffs: list[AuditHttpResponseHandoff | AuditHttpFailureHandoff] = []
                while not audit_handoff.empty():
                    handoffs.append(audit_handoff.get_nowait())
                handoffs.sort(
                    key=lambda item: (
                        pending_audits[item.request_id].hard_expiry_ns,
                        pending_audits[item.request_id].asset_order,
                        pending_audits[item.request_id].start.request_started_monotonic_ns,
                        item.request_id,
                    )
                )
                handed_off_ids = {item.request_id for item in handoffs}
                for pending in pending_audits.values():
                    if (
                        pending.task.done()
                        and pending.response is None
                        and pending.start.request_id not in handed_off_ids
                    ):
                        pending.task.result()
                        raise RuntimeError("audit task completed without a handoff")
                for handoff in handoffs:
                    pending = pending_audits.get(handoff.request_id)
                    if pending is None:
                        raise RuntimeError("audit handoff references no pending request")
                    await pending.task
                    if acquisition_task.done() and pending_exit is not _GenerationExit.DEADLINE:
                        response_preceded_generation_end = (
                            isinstance(handoff, AuditHttpResponseHandoff)
                            and acquisition_ended_monotonic_ns is not None
                            and handoff.received_monotonic_ns < acquisition_ended_monotonic_ns
                        )
                        if not response_preceded_generation_end:
                            handoff = AuditHttpFailureHandoff(
                                request_id=pending.start.request_id,
                                connection_id=pending.start.connection_id,
                                reason=AuditFailureReason.SOCKET_GENERATION_CHANGED,
                                error_type=AuditFailureReason.SOCKET_GENERATION_CHANGED.value,
                            )
                    if isinstance(handoff, AuditHttpFailureHandoff):
                        pending_audits.pop(handoff.request_id)
                        result = await self._preserve_audit_failure(
                            capture=capture,
                            failure=handoff,
                            subscription_id=subscription_id,
                        )
                        if result.audit_outcomes:
                            outcome = result.audit_outcomes[-1]
                            self._metrics.increment_counter(
                                "mma_rest_audit_outcomes_total",
                                labels={"outcome": outcome.outcome.value},
                            )
                        if pending.start.attempt == 1:
                            first_attempts_pending.discard(pending.start.native_asset_id)
                        expired_freshness_markers[pending.start.native_asset_id] = (
                            pending.freshness_marker_ns
                        )
                        finish_cycle_leg(pending.start.native_asset_id)
                        deviations.append(
                            f"REST audit HTTP failure isolated at ingest_seq={result.ingest_seq}"
                        )
                        continue
                    if (
                        pending.response_fence.request_id != handoff.request_id
                        or pending.response_fence.barrier_ordinal
                        != handoff.response_boundary_watermark.ordinal
                    ):
                        raise RuntimeError("audit fence differs from its response handoff")
                    pending.response = handoff
                    processed_at_handoff = capture.processor.processed_websocket_watermark(
                        connection_id
                    ).ordinal
                    self._metrics.observe_histogram(
                        "mma_rest_audit_barrier_lag_messages",
                        max(
                            0,
                            handoff.response_boundary_watermark.ordinal - processed_at_handoff,
                        ),
                    )

                # Classify only the globally earliest announced receive fence.
                # This preserves each request's exact response boundary even
                # when the two HTTP responses complete in the opposite order.
                while True:
                    announced = tuple(
                        pending
                        for pending in pending_audits.values()
                        if pending.response_fence.barrier_ordinal is not None
                    )
                    if not announced:
                        break
                    earliest = min(
                        announced,
                        key=lambda pending: (
                            pending.response_fence.barrier_ordinal,
                            pending.response_fence.received_monotonic_ns,
                            pending.hard_expiry_ns,
                            pending.asset_order,
                            pending.start.request_started_monotonic_ns,
                            pending.start.request_id,
                        ),
                    )
                    response = earliest.response
                    if response is None:
                        break
                    boundary = response.response_boundary_watermark.ordinal
                    processed = capture.processor.processed_websocket_watermark(
                        connection_id
                    ).ordinal
                    if processed < boundary and not (
                        ingress.empty
                        and (deferred_item is None or deferred_item.watermark.ordinal > boundary)
                    ):
                        break
                    if processed > boundary:
                        raise RuntimeError("canonical owner crossed a pending audit fence")
                    result = await complete_audit_response(response)
                    pending_audits.pop(earliest.start.request_id)
                    if not result.audit_outcomes:
                        raise RuntimeError("audit response produced no canonical outcome")
                    outcome = result.audit_outcomes[-1]
                    start = earliest.start
                    if start.attempt == 1:
                        first_attempts_pending.discard(start.native_asset_id)
                    self._metrics.increment_counter(
                        "mma_rest_audit_outcomes_total",
                        labels={"outcome": outcome.outcome.value},
                    )
                    self._metrics.observe_histogram(
                        "mma_rest_audit_classification_latency_ns",
                        max(0, time.monotonic_ns() - response.received_monotonic_ns),
                    )
                    if outcome.outcome is AuditOutcome.MISMATCH:
                        pending_exit = _GenerationExit.RESYNC_REQUIRED
                        deviations.append(f"REST audit MISMATCH at ingest_seq={result.ingest_seq}")
                        finish_cycle_leg(start.native_asset_id)
                        await stop_acquisition()
                        break
                    if outcome.outcome is AuditOutcome.FAILED:
                        expired_freshness_markers[start.native_asset_id] = (
                            earliest.freshness_marker_ns
                        )
                        deviations.append(
                            f"REST audit FAILED isolated at ingest_seq={result.ingest_seq}"
                        )
                        finish_cycle_leg(start.native_asset_id)
                        continue
                    if outcome.outcome is AuditOutcome.RACED:
                        launch_block_reason = audit_launch_block_reason()
                        if launch_block_reason is not None:
                            self._metrics.increment_counter(
                                "mma_rest_audit_retries_suppressed_total",
                                labels={"reason": launch_block_reason},
                            )
                            finish_cycle_leg(start.native_asset_id)
                        elif start.attempt <= maximum_race_retries:
                            self._metrics.increment_counter("mma_rest_audit_retries_total")
                            deferred_retry_actions[start.native_asset_id] = (
                                start.attempt + 1,
                                False,
                            )
                        else:
                            self._metrics.increment_counter(
                                "mma_rest_audit_race_budget_exhaustions_total"
                            )
                            deferred_retry_actions[start.native_asset_id] = (
                                start.attempt + 1,
                                True,
                            )
                        continue
                    expired_freshness_markers.pop(start.native_asset_id, None)
                    finish_cycle_leg(start.native_asset_id)

                if (
                    not first_attempts_pending
                    and deferred_retry_actions
                    and pending_exit is None
                    and pending_transport_error is None
                ):
                    for asset_id in sorted(
                        deferred_retry_actions,
                        key=lambda item: (
                            leg_hard_expiry_ns(item, now_ns=time.monotonic_ns()),
                            stable_asset_order[item],
                        ),
                    ):
                        if len(pending_audits) >= 2:
                            break
                        attempt, terminal = deferred_retry_actions.pop(asset_id)
                        if terminal:
                            launched = await stage_failed_leg(
                                asset_id,
                                attempt=attempt,
                                reason=AuditFailureReason.RACE_RETRY_EXHAUSTED,
                            )
                            if launched is not _AuditLaunchResult.RUN_ENDED:
                                deviations.append("REST audit race retry budget exhausted")
                        else:
                            launched = await launch_audit(asset_id, attempt=attempt)
                            if launched is _AuditLaunchResult.FAILED_CLOSED:
                                pending_exit = _GenerationExit.RESYNC_REQUIRED
                                await stop_acquisition()
                                break

                now_ns = time.monotonic_ns()
                due = current_deadline()
                due_ns = due.deadline_ns
                if (
                    pending_exit is None
                    and pending_transport_error is None
                    and acquisition_task.done()
                ):
                    try:
                        acquisition_task.result()
                    except RawIngressBackpressureError:
                        pending_exit = _GenerationExit.INGRESS_OVERFLOW
                        deviations.append("bounded application ingress queue exhausted")
                        runtime_health.record_data_loss()
                        self._metrics.increment_counter("mma_ingress_overflows_total")
                    except PolymarketWebSocketError as error:
                        pending_transport_error = error
                    else:
                        if ingress.empty:
                            raise PolymarketWebSocketTransportError(
                                category=(PolymarketWebSocketFailureCategory.CONNECTION_CLOSED),
                                retryable=True,
                                detail=(
                                    "public market WebSocket ended without an explicit "
                                    "transport error"
                                ),
                            )
                if pending_exit is None and now_ns >= due_ns:
                    if due.reason is IntegrityDeadlineReason.RUN_DEADLINE:
                        pending_exit = _GenerationExit.DEADLINE
                        capture_cutoff = _CaptureCutoffMoment(
                            wall_time=UtcTimestamp.now(),
                            monotonic_ns=now_ns,
                        )
                        await stop_acquisition()
                    else:
                        audit_cycle_active = True
                        self._metrics.increment_counter("mma_rest_audit_cycles_total")
                        ordered_assets = ordered_cycle_assets(now_ns=now_ns)
                        cycle_unfinished_assets.update(ordered_assets)
                        first_attempts_pending.update(ordered_assets)
                        staged_first_attempts: list[_StagedAuditRequest] = []
                        staging_abort_reason: AuditFailureReason | None = None
                        for asset_id in ordered_assets:
                            launched = await launch_audit(
                                asset_id,
                                attempt=1,
                                deferred_starts=staged_first_attempts,
                            )
                            if launched is not _AuditLaunchResult.STARTED:
                                first_attempts_pending.discard(asset_id)
                            if launched is _AuditLaunchResult.FAILED_CLOSED:
                                pending_exit = _GenerationExit.RESYNC_REQUIRED
                                staging_abort_reason = AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE
                                await stop_acquisition()
                                break
                            if launched is _AuditLaunchResult.RUN_ENDED:
                                pending_exit = _GenerationExit.DEADLINE
                                staging_abort_reason = AuditFailureReason.RUN_ENDED
                                capture_cutoff = _CaptureCutoffMoment(
                                    wall_time=UtcTimestamp.now(),
                                    monotonic_ns=time.monotonic_ns(),
                                )
                                await stop_acquisition()
                                break
                            if launched is _AuditLaunchResult.GENERATION_ENDED:
                                staging_abort_reason = AuditFailureReason.SOCKET_GENERATION_CHANGED
                                break
                        launch_ns = time.monotonic_ns()
                        if staging_abort_reason is None and (
                            pending_exit is not None
                            or pending_transport_error is not None
                            or acquisition_task.done()
                        ):
                            staging_abort_reason = AuditFailureReason.SOCKET_GENERATION_CHANGED
                        if staging_abort_reason is None and launch_ns >= deadline_ns:
                            pending_exit = _GenerationExit.DEADLINE
                            staging_abort_reason = AuditFailureReason.RUN_ENDED
                            capture_cutoff = _CaptureCutoffMoment(
                                wall_time=UtcTimestamp.now(),
                                monotonic_ns=launch_ns,
                            )
                            await stop_acquisition()

                        launchable: list[_StagedAuditRequest] = []
                        staged_failures: list[tuple[_StagedAuditRequest, AuditFailureReason]] = []
                        for staged in staged_first_attempts:
                            failure_reason = staging_abort_reason
                            if failure_reason is None and launch_ns >= staged.response_deadline_ns:
                                failure_reason = (
                                    AuditFailureReason.RUN_ENDED
                                    if deadline_ns <= staged.hard_expiry_ns
                                    else AuditFailureReason.BOOK_FRESHNESS_EXPIRED
                                )
                            if failure_reason is None:
                                launchable.append(staged)
                            else:
                                staged_failures.append((staged, failure_reason))

                        # Both first-attempt GET tasks become runnable only
                        # after every START is durable.  Creating all eligible
                        # tasks synchronously leaves no await-based starvation
                        # point between the two legs.
                        for staged in launchable:
                            start_staged_audit_http(
                                staged,
                                launch_monotonic_ns=launch_ns,
                            )
                        for staged, failure_reason in staged_failures:
                            await close_staged_audit(staged, reason=failure_reason)

                if (
                    pending_audits
                    and pending_exit is not _GenerationExit.DEADLINE
                    and (pending_exit is not None or pending_transport_error is not None)
                ):
                    if await drain_completed_exit_handoffs():
                        continue
                    earliest = earliest_announced_audit()
                    # A deferred post-fence ingress item must not trap an audit
                    # cleanup that releases only when generation exit cancels
                    # it. Unblock the globally earliest announced fence before
                    # returning to normal ordered drain.
                    if (earliest is None or earliest.response is None) and await fail_all_audits(
                        AuditFailureReason.SOCKET_GENERATION_CHANGED
                    ):
                        continue

                first_item: RawIngressItem | None = None
                if deferred_item is not None:
                    first_item = deferred_item
                    deferred_item = None
                elif not ingress.empty:
                    first_item = ingress.get_nowait()
                elif pending_exit is not None:
                    if pending_audits and await drain_completed_exit_handoffs():
                        continue
                    if pending_audits and pending_exit is _GenerationExit.DEADLINE:
                        # Already-started reads share the absolute deadline.
                        # Let their bounded handoffs publish, then classify any
                        # body received before the cutoff; this starts no work.
                        await asyncio.gather(*(pending.task for pending in pending_audits.values()))
                        continue
                    if pending_audits and await fail_all_audits(
                        AuditFailureReason.SOCKET_GENERATION_CHANGED
                    ):
                        continue
                    return _GenerationResult(
                        pending_exit,
                        deviations=tuple(deviations),
                        refresh_requests=tuple(refresh_requests),
                        ingress_metrics=ingress.metrics,
                        capture_cutoff=capture_cutoff,
                    )
                elif pending_transport_error is not None:
                    if pending_audits and await drain_completed_exit_handoffs():
                        continue
                    if pending_audits and await fail_all_audits(
                        AuditFailureReason.SOCKET_GENERATION_CHANGED
                    ):
                        continue
                    raise pending_transport_error
                elif acquisition_task.done():
                    try:
                        acquisition_task.result()
                    except RawIngressBackpressureError:
                        pending_exit = _GenerationExit.INGRESS_OVERFLOW
                        deviations.append("bounded application ingress queue exhausted")
                        runtime_health.record_data_loss()
                        self._metrics.increment_counter("mma_ingress_overflows_total")
                    else:
                        raise PolymarketWebSocketTransportError(
                            category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                            retryable=True,
                            detail=(
                                "public market WebSocket ended without an explicit transport error"
                            ),
                        )
                    continue

                if first_item is None:
                    item_task = asyncio.create_task(
                        ingress.get(),
                        name="polymarket-public-ingress-dequeue",
                    )
                    pending_ingress_get_task = item_task
                    active_audit_tasks = {
                        pending.task
                        for pending in pending_audits.values()
                        if not pending.task.done()
                    }
                    if not audit_handoff.empty() or any(
                        pending.task.done() and pending.response is None
                        for pending in pending_audits.values()
                    ):
                        if item_task.done():
                            deferred_item = item_task.result()
                        else:
                            item_task.cancel()
                            with suppress(asyncio.CancelledError):
                                await item_task
                        pending_ingress_get_task = None
                        continue
                    if acquisition_task.done():
                        item_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await item_task
                        pending_ingress_get_task = None
                        continue
                    wait_tasks: set[asyncio.Task[object]] = {item_task}
                    wait_tasks.add(acquisition_task)
                    wait_tasks.update(active_audit_tasks)
                    done, _ = await asyncio.wait(
                        wait_tasks,
                        timeout=max(0.0, (due_ns - time.monotonic_ns()) / 1e9),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if active_audit_tasks.intersection(done):
                        if item_task in done:
                            deferred_item = item_task.result()
                            pending_ingress_get_task = None
                        else:
                            item_task.cancel()
                            with suppress(asyncio.CancelledError):
                                await item_task
                            pending_ingress_get_task = None
                        continue
                    if item_task in done:
                        first_item = item_task.result()
                        pending_ingress_get_task = None
                    else:
                        item_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await item_task
                        pending_ingress_get_task = None
                        continue

                batch = [first_item]
                announced_barriers = tuple(
                    pending.response_fence.barrier_ordinal
                    for pending in pending_audits.values()
                    if pending.response_fence.barrier_ordinal is not None
                )
                barrier_limit = min(announced_barriers) if announced_barriers else None
                if barrier_limit is not None and first_item.watermark.ordinal > barrier_limit:
                    deferred_item = first_item
                    audit_tasks = {
                        pending.task
                        for pending in pending_audits.values()
                        if not pending.task.done()
                    }
                    if not audit_handoff.empty() or any(
                        pending.task.done() and pending.response is None
                        for pending in pending_audits.values()
                    ):
                        continue
                    if acquisition_task.done():
                        earliest = earliest_announced_audit()
                        if earliest is None:
                            raise RuntimeError("audit fence disappeared while ingress was deferred")
                        if earliest.response is None:
                            # A response body may be immutable while its HTTP
                            # task is still awaiting connection cleanup.  Once
                            # the socket generation ends, cancellation is what
                            # releases that exact body into the handoff.  Do it
                            # here instead of spinning on the deferred
                            # post-fence frame or waiting forever on cleanup.
                            await fail_all_audits(AuditFailureReason.SOCKET_GENERATION_CHANGED)
                            continue
                        processed = capture.processor.processed_websocket_watermark(
                            connection_id
                        ).ordinal
                        if processed < barrier_limit:
                            raise RuntimeError(
                                "completed audit fence is missing its contiguous ingress prefix"
                            )
                        # The response became coordinator-owned after this
                        # iteration's ordered drain.  Re-enter it so the body
                        # is classified before the deferred later frame.
                        continue
                    wait_tasks = set(audit_tasks)
                    if not acquisition_task.done():
                        wait_tasks.add(acquisition_task)
                    if wait_tasks:
                        await asyncio.wait(
                            wait_tasks,
                            timeout=max(0.0, (due_ns - time.monotonic_ns()) / 1e9),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    continue
                while len(batch) < self._config.storage.journal_group_commit_max_events:
                    try:
                        next_item = ingress.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if barrier_limit is not None and next_item.watermark.ordinal > barrier_limit:
                        deferred_item = next_item
                        break
                    batch.append(next_item)
                results = await capture.preserve_websocket_batch(
                    tuple(batch),
                    semantics_hash=socket.subscription_semantics_sha256,
                    connection_id=connection_id,
                    subscription_id=subscription_id,
                )
                self._metrics.set_gauge("mma_ingress_queue_depth", ingress.metrics.queue_depth)
                self._metrics.set_gauge(
                    "mma_ingress_queue_high_water",
                    ingress.metrics.queue_high_water,
                )
                for result in results:
                    relevant_refresh = False
                    for request in result.reference_refresh_requests:
                        self._metrics.increment_counter(
                            "mma_reference_refresh_requests_total",
                            labels={"event_type": request.event_type.value},
                        )
                        if refresh_matches_selected(request):
                            refresh_requests.append(request)
                            relevant_refresh = True
                        else:
                            self._metrics.increment_counter(
                                "mma_reference_refresh_requests_ignored_total",
                                labels={"event_type": request.event_type.value},
                            )
                    if not result.success:
                        runtime_health.record_protocol_deviation()
                        deviations.append(
                            f"{result.error_type or 'processing-failure'} at "
                            f"ingest_seq={result.ingest_seq}"
                        )
                    if result.needs_resync:
                        pending_exit = _GenerationExit.RESYNC_REQUIRED
                    elif relevant_refresh and pending_exit is None:
                        pending_exit = _GenerationExit.REFERENCE_REFRESH
                if pending_exit is not None:
                    await stop_acquisition()
        finally:
            if pending_ingress_get_task is not None and not pending_ingress_get_task.done():
                pending_ingress_get_task.cancel()
            if pending_ingress_get_task is not None:
                with suppress(asyncio.CancelledError):
                    await pending_ingress_get_task
            if not ready_task.done():
                ready_task.cancel()
            with suppress(asyncio.CancelledError):
                await ready_task
            for request_id in tuple(pending_audits):
                await stop_audit_request(request_id)
            await stop_acquisition()

    async def _observe_websocket(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
        asset_ids: tuple[str, ...],
        deadline: float,
        logger: StructuredLogger,
        runtime_health: ObservationRuntimeHealth | None = None,
        on_capture_cutoff: Callable[[_CaptureCutoffMoment], None] | None = None,
    ) -> _StreamResult:
        if runtime_health is None:
            runtime_health = ObservationRuntimeHealth()
        reconnects = 0
        deviations: list[str] = []
        delay = self._config.polymarket.reconnect_initial_seconds
        loop = asyncio.get_running_loop()
        deadline_ns = time.monotonic_ns() + int(max(0.0, deadline - loop.time()) * 1e9)
        integrity_interval_ns = int(self._config.polymarket.snapshot_audit_interval_seconds * 1e9)
        periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
        unavailable_started_ns: int | None = None
        ingress_metrics = _EMPTY_INGRESS_METRICS
        while loop.time() < deadline:
            connection_id = ConnectionId.new()
            subscription_id = SubscriptionId.new()
            self._metrics.increment_counter("mma_websocket_generations_total")
            logger.info(
                "websocket_generation_started",
                "public market WebSocket generation started",
                connection_id=connection_id,
                subscription_id=subscription_id,
                attempt=reconnects,
            )
            try:
                try:
                    generation = await self._run_generation(
                        client=client,
                        capture=capture,
                        selected=selected,
                        asset_ids=asset_ids,
                        deadline=deadline,
                        deadline_ns=deadline_ns,
                        periodic_audit_due_ns=periodic_audit_due_ns,
                        connection_id=connection_id,
                        subscription_id=subscription_id,
                        unavailable_started_ns=unavailable_started_ns,
                        runtime_health=runtime_health,
                        on_capture_cutoff=on_capture_cutoff,
                    )
                except BaseException:
                    runtime_health.finish_generation(
                        continuity_interrupted=True,
                        now_ns=time.monotonic_ns(),
                    )
                    raise
                runtime_health.finish_generation(
                    continuity_interrupted=generation.reason is not _GenerationExit.DEADLINE,
                    now_ns=(
                        time.monotonic_ns()
                        if generation.capture_cutoff is None
                        else generation.capture_cutoff.monotonic_ns
                    ),
                )
                ingress_metrics = _merge_ingress_metrics(
                    ingress_metrics,
                    generation.ingress_metrics,
                )
                deviations.extend(generation.deviations)
                if generation.reason is _GenerationExit.DEADLINE:
                    return _StreamResult(
                        reconnects,
                        True,
                        False,
                        tuple(deviations),
                        ingress_metrics,
                        generation.capture_cutoff,
                    )
                unavailable_started_ns = time.monotonic_ns()
                if generation.reason is _GenerationExit.REFERENCE_REFRESH:
                    selected_resolved = await self._handle_refresh_requests(
                        requests=generation.refresh_requests,
                        client=client,
                        capture=capture,
                        selected=selected,
                        deadline=deadline,
                        runtime_health=runtime_health,
                        metrics_already_recorded=True,
                    )
                    if selected_resolved:
                        return _StreamResult(
                            reconnects,
                            True,
                            True,
                            tuple(deviations),
                            ingress_metrics,
                        )
                    periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                    continue
                if generation.reason is _GenerationExit.INGRESS_OVERFLOW:
                    await capture.preserve_invalidation(InvalidationReason.BACKPRESSURE_DATA_LOSS)
                # Parser/state uncertainty already invalidated through the
                # journaled venue envelope. The old generator is closed; now
                # establish a baseline before creating a new socket.
                self._metrics.increment_counter("mma_resynchronizations_total")
                runtime_health.record_resync()
                await self._resnapshot(
                    client=client,
                    capture=capture,
                    selected=selected,
                    deadline=deadline,
                )
                periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
            except PolymarketHttpError as error:
                if error.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED:
                    return _StreamResult(
                        reconnects,
                        True,
                        False,
                        tuple(deviations),
                        ingress_metrics,
                    )
                deviations.append(f"{type(error).__name__}:{error.category.value}")
                return _StreamResult(
                    reconnects,
                    False,
                    False,
                    tuple(deviations),
                    ingress_metrics,
                )
            except PolymarketWebSocketError as error:
                unavailable_started_ns = time.monotonic_ns()
                await capture.preserve_invalidation(InvalidationReason.DISCONNECT)
                deviations.append(type(error).__name__)
                if isinstance(error, PolymarketWebSocketProtocolError):
                    runtime_health.record_protocol_deviation()
                self._metrics.increment_counter("mma_websocket_disconnects_total")
                logger.warning(
                    "websocket_disconnected",
                    "public market WebSocket disconnected",
                    connection_id=connection_id,
                    exception_type=type(error).__name__,
                    attempt=reconnects,
                )
                if reconnects >= self._config.polymarket.max_reconnect_attempts:
                    deviations.append("maximum reconnect attempts exhausted")
                    return _StreamResult(
                        reconnects,
                        False,
                        False,
                        tuple(deviations),
                        ingress_metrics,
                    )
                reconnects += 1
                runtime_health.record_reconnect()
                self._metrics.increment_counter("mma_websocket_reconnects_total")
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return _StreamResult(
                        reconnects,
                        True,
                        False,
                        tuple(deviations),
                        ingress_metrics,
                    )
                await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, self._config.polymarket.reconnect_max_seconds)
                if loop.time() < deadline:
                    runtime_health.record_resync()
                    await self._resnapshot(
                        client=client,
                        capture=capture,
                        selected=selected,
                        deadline=deadline,
                    )
                    periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
            except asyncio.CancelledError:
                raise
        return _StreamResult(
            reconnects,
            True,
            False,
            tuple(deviations),
            ingress_metrics,
        )


__all__ = ["ObservationSummary", "PolymarketObservationService"]
