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
from dataclasses import dataclass, field
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
    AuditFailureCommand,
    AuditFailureReason,
    AuditHttpFailureHandoff,
    AuditHttpHandoff,
    AuditHttpResponseHandoff,
    AuditOutcome,
    AuditStartEvidence,
    WebSocketAdmissionWatermark,
)
from .channels import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
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
from .freshness import IntegrityDeadline, IntegrityDeadlineReason, next_integrity_deadline
from .health import (
    ObservationRuntimeHealth,
    ObservationRuntimeHealthSnapshot,
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
class _GenerationResult:
    reason: _GenerationExit
    deviations: tuple[str, ...] = ()
    refresh_requests: tuple[ReferenceRefreshRequest, ...] = ()
    ingress_metrics: RawIngressMetricsSnapshot = field(default_factory=_empty_ingress_metrics)


@dataclass(frozen=True, slots=True)
class _StreamResult:
    reconnects: int
    completed: bool
    selected_market_resolved: bool
    deviations: tuple[str, ...]
    ingress_metrics: RawIngressMetricsSnapshot = field(default_factory=_empty_ingress_metrics)


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
            self.metrics.observe_histogram(
                "mma_receive_to_scan_complete_ns",
                max(
                    0,
                    scan_completed_monotonic_ns - envelope.received_monotonic.nanoseconds,
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
) -> HealthDecision:
    """Reconcile the last checkpoint with close-time derived-owner outcomes."""

    if checkpoint_decision is None:
        reasons = {HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY}
        if not analytical_outputs.research_eligible:
            reasons.add(HealthDecisionReason.ANALYTICAL_UNHEALTHY)
        return HealthDecision(
            capture_healthy=True,
            research_eligible=analytical_outputs.research_eligible,
            gate1_eligible_so_far=False,
            reason_codes=tuple(sorted(reasons, key=str)),
        )
    reasons = set(checkpoint_decision.reason_codes)
    research_eligible = checkpoint_decision.research_eligible
    if not evidence.gate1_evidence_eligible:
        reasons.add(HealthDecisionReason.HEALTH_EVIDENCE_UNHEALTHY)
    if not analytical_outputs.research_eligible:
        reasons.add(HealthDecisionReason.ANALYTICAL_UNHEALTHY)
        research_eligible = False
    return HealthDecision(
        capture_healthy=checkpoint_decision.capture_healthy,
        research_eligible=research_eligible,
        gate1_eligible_so_far=(
            checkpoint_decision.gate1_eligible_so_far
            and evidence.gate1_evidence_eligible
            and analytical_outputs.research_eligible
            and not reasons
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
        resource_sampler: SupervisedProcessResourceSampler | None = None
        resource_sampler_started = False
        capture = _RawCaptureBoundary(
            collector_run_id=run_id,
            writer=writer,
            processor=processor,
            metrics=self._metrics,
        )
        run_completed = False
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
                    monotonic_now_ns = int(loop.time() * 1_000_000_000)
                    resources = resource_sampler.snapshot.latest
                    if resources is None:
                        raise RuntimeError("resource sampler has not published a sample")
                    checkpoint = build_observation_health_checkpoint(
                        config=self._config,
                        collector_run_id=run_id,
                        git_commit=self._code_commit,
                        target=target,
                        runtime=runtime_health.snapshot(now_ns=monotonic_now_ns),
                        processor=processor,
                        journal=writer.metrics,
                        journal_failure_type=(
                            None if writer.failure is None else type(writer.failure).__name__
                        ),
                        analytical=outputs.runtime_snapshot,
                        health_writer=health_writer.runtime_snapshot,
                        public_reads=client.public_read_metrics_snapshot(),
                        metrics=self._metrics,
                        resources=resources,
                        emitted_at=UtcTimestamp.now(),
                        monotonic_now_ns=monotonic_now_ns,
                    )
                    latest_health_decision = HealthDecision(
                        capture_healthy=checkpoint.capture_healthy,
                        research_eligible=checkpoint.research_eligible,
                        gate1_eligible_so_far=checkpoint.gate1_eligible_so_far,
                        reason_codes=checkpoint.reason_codes,
                    )
                    return checkpoint

                health_checkpoint_builder = build_health_checkpoint

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
                    )
                    websocket_observation_nanoseconds = max(
                        0,
                        time.monotonic_ns() - websocket_started_ns,
                    )
                    deviations.extend(stream_result.deviations)
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
                                completed=run_completed,
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
        if latest_health_decision is None and health_checkpoint_builder is not None:
            # A pre-target deadline intentionally writes no Gate-1 evidence, but
            # the returned scoped decision must still reflect the terminal
            # socket/target/book state rather than defaulting to healthy.
            try:
                health_checkpoint_builder()
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
        ended = UtcTimestamp.now()
        actual_duration_nanoseconds = time.monotonic_ns() - started_monotonic_ns
        health_decision = _final_health_decision(
            latest_health_decision,
            evidence=health_evidence,
            analytical_outputs=analytical_summary,
        )
        runtime_health_snapshot = runtime_health.snapshot(now_ns=time.monotonic_ns())
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
    ) -> tuple[AuditStartEvidence, ProcessedEnvelope]:
        request_started_ns = time.monotonic_ns()
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
        result = await capture.preserve_then_process(
            channel=CONTROL_REST_AUDIT_START,
            payload=start.to_payload(),
            transport_type="CONTROL",
            semantics_hash=_semantics(CONTROL_REST_AUDIT_START),
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata={"audit_attempt": attempt, "request_id": start.request_id},
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
        handoff: asyncio.Queue[AuditHttpHandoff],
        response_fence: _AuditResponseBoundaryFence | None = None,
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
                AuditFailureReason.OBSERVATION_DEADLINE
                if error.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
                else AuditFailureReason.HTTP_REQUEST_FAILED
            )
            item: AuditHttpHandoff = AuditHttpFailureHandoff(
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
    async def _preserve_audit_response(
        *,
        capture: _RawCaptureBoundary,
        response: AuditHttpResponseHandoff,
        start: AuditStartEvidence,
        barrier_ordinal: int,
        subscription_id: SubscriptionId,
    ) -> ProcessedEnvelope:
        return await capture.preserve_then_process(
            channel=REST_BOOK_AUDIT,
            payload=response.payload_bytes,
            transport_type="HTTP",
            semantics_hash=_semantics(
                REST_BOOK_AUDIT,
                f"token_id={start.native_asset_id}",
            ),
            connection_id=response.connection_id,
            subscription_id=subscription_id,
            transport_metadata={
                "audit_attempt": start.attempt,
                "barrier_websocket_ordinal": barrier_ordinal,
                "request_id": response.request_id,
                "response_boundary_websocket_ordinal": (
                    response.response_boundary_watermark.ordinal
                ),
            },
            received_wall_time=response.received_wall_time,
            received_monotonic_ns=response.received_monotonic_ns,
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

        async def acquire() -> None:
            # `raw_messages` is retained solely for existing adapter fixtures;
            # production uses pump_received so stamping+admission has no yield gap.
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
        pending_transport_error: PolymarketWebSocketError | None = None
        deviations: list[str] = []
        refresh_requests: list[ReferenceRefreshRequest] = []
        audit_handoff: asyncio.Queue[AuditHttpHandoff] = asyncio.Queue(maxsize=1)
        audit_task: asyncio.Task[None] | None = None
        active_audit: AuditStartEvidence | None = None
        active_response_fence: _AuditResponseBoundaryFence | None = None
        pending_response: AuditHttpResponseHandoff | None = None
        deferred_item: RawIngressItem | None = None
        audit_cycle_active = False
        audit_asset_index = 0
        audit_attempt = 0
        maximum_race_retries = self._config.polymarket.snapshot_audit_max_race_retries
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

        def generation_is_exiting() -> bool:
            return (
                pending_exit is not None
                or pending_transport_error is not None
                or acquisition_task.done()
            )

        def current_deadline() -> IntegrityDeadline:
            if audit_cycle_active:
                return IntegrityDeadline(
                    deadline_ns=deadline_ns,
                    reason=IntegrityDeadlineReason.RUN_DEADLINE,
                )
            return next_integrity_deadline(
                run_deadline_ns=deadline_ns,
                periodic_audit_deadline_ns=periodic_audit_due_ns,
                valid_book_receive_ns=capture.book_freshness_receive_ns,
                required_books=len(asset_ids),
                maximum_book_age_ns=maximum_book_age_ns,
                now_ns=time.monotonic_ns(),
                freshness_audit_guard_ns=freshness_audit_guard_ns,
            )

        async def launch_audit(
            native_asset_id: str,
            *,
            attempt: int,
            issue_http_request: bool = True,
        ) -> bool:
            nonlocal active_audit, active_response_fence, audit_task
            start, result = await self._stage_audit_start(
                capture=capture,
                ingress=ingress,
                native_asset_id=native_asset_id,
                attempt=attempt,
                connection_id=connection_id,
                subscription_id=subscription_id,
            )
            if result.needs_resync:
                deviations.append(f"audit start failed closed at ingest_seq={result.ingest_seq}")
                if result.success:
                    active_audit = start
                    await fail_active_audit(AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE)
                return False
            active_audit = start
            if not issue_http_request:
                active_response_fence = None
                audit_task = None
                return True
            active_response_fence = _AuditResponseBoundaryFence(
                request_id=start.request_id,
                connection_id=start.connection_id,
            )
            audit_task = asyncio.create_task(
                self._run_audit_http_request(
                    client=client,
                    ingress=ingress,
                    start=start,
                    deadline=deadline,
                    handoff=audit_handoff,
                    response_fence=active_response_fence,
                ),
                name=f"polymarket-rest-audit-{start.request_id}",
            )
            return True

        async def stop_audit_task() -> None:
            nonlocal audit_task
            task = audit_task
            audit_task = None
            if task is None:
                return
            if not task.done():
                task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        async def fail_active_audit(reason: AuditFailureReason) -> None:
            nonlocal active_audit, active_response_fence, pending_response
            start = active_audit
            if start is None:
                return
            await stop_audit_task()
            pending_response = None
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
            active_audit = None
            active_response_fence = None
            if result.audit_outcomes:
                self._metrics.increment_counter(
                    "mma_rest_audit_outcomes_total",
                    labels={"outcome": result.audit_outcomes[-1].outcome.value},
                )

        async def complete_audit_response(
            response: AuditHttpResponseHandoff,
        ) -> ProcessedEnvelope:
            start = active_audit
            if start is None or start.request_id != response.request_id:
                raise RuntimeError("audit response handoff has no matching active request")
            processed = capture.processor.processed_websocket_watermark(connection_id).ordinal
            return await self._preserve_audit_response(
                capture=capture,
                response=response,
                start=start,
                barrier_ordinal=processed,
                subscription_id=subscription_id,
            )

        async def stop_acquisition() -> None:
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
                await stop_acquisition()
                return _GenerationResult(
                    _GenerationExit.DEADLINE,
                    ingress_metrics=ingress.metrics,
                )
            runtime_health.mark_connected(now_ns=time.monotonic_ns())
            if unavailable_started_ns is not None:
                self._metrics.observe_histogram(
                    "mma_socket_unavailable_ns",
                    max(0, time.monotonic_ns() - unavailable_started_ns),
                )

            while True:
                if audit_task is not None and audit_task.done():
                    audit_task.result()
                    audit_task = None
                    if audit_handoff.empty():
                        raise RuntimeError("audit task completed without a handoff")
                    handoff = audit_handoff.get_nowait()
                    if isinstance(handoff, AuditHttpFailureHandoff):
                        result = await self._preserve_audit_failure(
                            capture=capture,
                            failure=handoff,
                            subscription_id=subscription_id,
                        )
                        active_audit = None
                        active_response_fence = None
                        if result.audit_outcomes:
                            outcome = result.audit_outcomes[-1]
                            self._metrics.increment_counter(
                                "mma_rest_audit_outcomes_total",
                                labels={"outcome": outcome.outcome.value},
                            )
                        audit_cycle_active = False
                        periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                        deviations.append(
                            f"REST audit HTTP failure isolated at ingest_seq={result.ingest_seq}"
                        )
                    else:
                        if (
                            active_response_fence is None
                            or active_response_fence.request_id != handoff.request_id
                            or active_response_fence.barrier_ordinal
                            != handoff.response_boundary_watermark.ordinal
                        ):
                            raise RuntimeError(
                                "active audit fence differs from its response handoff"
                            )
                        pending_response = handoff
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

                if pending_response is not None:
                    classified_response: AuditHttpResponseHandoff | None = None
                    processed = capture.processor.processed_websocket_watermark(
                        connection_id
                    ).ordinal
                    boundary = pending_response.response_boundary_watermark.ordinal
                    if processed >= boundary or (
                        processed < boundary
                        and ingress.empty
                        and (deferred_item is None or deferred_item.watermark.ordinal > boundary)
                    ):
                        classified_response = pending_response
                        result = await complete_audit_response(classified_response)
                        pending_response = None
                    else:
                        result = None
                    if result is not None:
                        if classified_response is None:
                            raise RuntimeError(
                                "audit result is missing its response-boundary handoff"
                            )
                        start = active_audit
                        if start is None or not result.audit_outcomes:
                            raise RuntimeError("audit response produced no canonical outcome")
                        outcome = result.audit_outcomes[-1]
                        active_audit = None
                        active_response_fence = None
                        self._metrics.increment_counter(
                            "mma_rest_audit_outcomes_total",
                            labels={"outcome": outcome.outcome.value},
                        )
                        self._metrics.observe_histogram(
                            "mma_rest_audit_classification_latency_ns",
                            max(
                                0,
                                time.monotonic_ns() - classified_response.received_monotonic_ns,
                            ),
                        )
                        if outcome.outcome is AuditOutcome.MISMATCH:
                            pending_exit = _GenerationExit.RESYNC_REQUIRED
                            deviations.append(
                                f"REST audit MISMATCH at ingest_seq={result.ingest_seq}"
                            )
                            await stop_acquisition()
                        elif outcome.outcome is AuditOutcome.FAILED:
                            audit_cycle_active = False
                            periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                            deviations.append(
                                f"REST audit FAILED isolated at ingest_seq={result.ingest_seq}"
                            )
                        elif outcome.outcome is AuditOutcome.RACED:
                            if generation_is_exiting():
                                audit_cycle_active = False
                                periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                                self._metrics.increment_counter(
                                    "mma_rest_audit_retries_suppressed_total",
                                    labels={"reason": "generation_exit"},
                                )
                            elif start.attempt <= maximum_race_retries:
                                self._metrics.increment_counter("mma_rest_audit_retries_total")
                                audit_attempt = start.attempt + 1
                                if not await launch_audit(
                                    start.native_asset_id,
                                    attempt=audit_attempt,
                                ):
                                    pending_exit = _GenerationExit.RESYNC_REQUIRED
                                    await stop_acquisition()
                            else:
                                self._metrics.increment_counter(
                                    "mma_rest_audit_race_budget_exhaustions_total"
                                )
                                audit_attempt = start.attempt + 1
                                if await launch_audit(
                                    start.native_asset_id,
                                    attempt=audit_attempt,
                                    issue_http_request=False,
                                ):
                                    await fail_active_audit(AuditFailureReason.RACE_RETRY_EXHAUSTED)
                                audit_cycle_active = False
                                periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                                deviations.append("REST audit race retry budget exhausted")
                        else:
                            if generation_is_exiting():
                                audit_cycle_active = False
                                periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                                self._metrics.increment_counter(
                                    "mma_rest_audit_legs_suppressed_total",
                                    labels={"reason": "generation_exit"},
                                )
                            else:
                                audit_asset_index += 1
                                if audit_asset_index < len(asset_ids):
                                    audit_attempt = 1
                                    if not await launch_audit(
                                        asset_ids[audit_asset_index],
                                        attempt=audit_attempt,
                                    ):
                                        pending_exit = _GenerationExit.RESYNC_REQUIRED
                                        await stop_acquisition()
                                else:
                                    audit_cycle_active = False
                                    periodic_audit_due_ns = (
                                        time.monotonic_ns() + integrity_interval_ns
                                    )

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
                        await stop_acquisition()
                    else:
                        audit_cycle_active = True
                        audit_asset_index = 0
                        audit_attempt = 1
                        self._metrics.increment_counter("mma_rest_audit_cycles_total")
                        if not await launch_audit(asset_ids[0], attempt=audit_attempt):
                            pending_exit = _GenerationExit.RESYNC_REQUIRED
                            await stop_acquisition()

                first_item: RawIngressItem | None = None
                if deferred_item is not None:
                    first_item = deferred_item
                    deferred_item = None
                elif not ingress.empty:
                    first_item = ingress.get_nowait()
                elif pending_exit is not None:
                    if active_audit is not None:
                        reason = (
                            AuditFailureReason.RUN_ENDED
                            if pending_exit is _GenerationExit.DEADLINE
                            else AuditFailureReason.SOCKET_GENERATION_CHANGED
                        )
                        await fail_active_audit(reason)
                    return _GenerationResult(
                        pending_exit,
                        deviations=tuple(deviations),
                        refresh_requests=tuple(refresh_requests),
                        ingress_metrics=ingress.metrics,
                    )
                elif pending_transport_error is not None:
                    if active_audit is not None:
                        await fail_active_audit(AuditFailureReason.SOCKET_GENERATION_CHANGED)
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
                    wait_tasks: set[asyncio.Task[object]] = {
                        item_task,
                        acquisition_task,
                    }
                    if audit_task is not None:
                        wait_tasks.add(audit_task)
                    done, _ = await asyncio.wait(
                        wait_tasks,
                        timeout=max(0.0, (due_ns - time.monotonic_ns()) / 1e9),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if audit_task is not None and audit_task in done:
                        if item_task in done:
                            deferred_item = item_task.result()
                        else:
                            item_task.cancel()
                            with suppress(asyncio.CancelledError):
                                await item_task
                        continue
                    if item_task in done:
                        first_item = item_task.result()
                    else:
                        item_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await item_task
                        continue

                batch = [first_item]
                barrier_limit = (
                    active_response_fence.barrier_ordinal
                    if active_response_fence is not None
                    else None
                )
                if barrier_limit is not None and first_item.watermark.ordinal > barrier_limit:
                    deferred_item = first_item
                    if audit_task is not None and not audit_task.done():
                        await asyncio.wait(
                            {audit_task, acquisition_task},
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
            if not ready_task.done():
                ready_task.cancel()
            with suppress(asyncio.CancelledError):
                await ready_task
            await stop_audit_task()
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
                    )
                except BaseException:
                    runtime_health.finish_generation(
                        continuity_interrupted=True,
                        now_ns=time.monotonic_ns(),
                    )
                    raise
                runtime_health.finish_generation(
                    continuity_interrupted=generation.reason is not _GenerationExit.DEADLINE,
                    now_ns=time.monotonic_ns(),
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
