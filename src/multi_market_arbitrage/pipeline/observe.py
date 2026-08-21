"""Bounded unauthenticated Polymarket public-data observation service."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
from collections.abc import Awaitable, Callable
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
    InMemoryMetrics,
    MetricsSnapshot,
    StructuredLogger,
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
    PolymarketMarketWebSocket,
    PolymarketPublicClient,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.errors import PolymarketTransportError
from multi_market_arbitrage.venues.polymarket.schemas import GammaMarket

from .channels import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    GAMMA_EVENT_PREFIX,
    GAMMA_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
)
from .freshness import IntegrityDeadline, IntegrityDeadlineReason, next_integrity_deadline
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


class _GenerationExit(StrEnum):
    DEADLINE = "DEADLINE"
    AUDIT_DUE = "AUDIT_DUE"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"
    REFERENCE_REFRESH = "REFERENCE_REFRESH"
    INGRESS_OVERFLOW = "INGRESS_OVERFLOW"


class _ObservationDeadlineReached(RuntimeError):
    """The finite public-observation deadline elapsed during remote I/O."""


async def _await_remote_before_deadline[T](
    operation: Callable[[], Awaitable[T]],
    *,
    deadline: float | None,
) -> T:
    """Bound only remote I/O, leaving returned bytes available for journaling."""

    if deadline is None:
        return await operation()
    try:
        async with asyncio.timeout_at(deadline):
            return await operation()
    except TimeoutError as error:
        raise _ObservationDeadlineReached from error


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
    )


@dataclass(frozen=True, slots=True)
class ObservationSummary:
    collector_run_id: CollectorRunId
    status: ComponentHealth
    started_at: UtcTimestamp
    ended_at: UtcTimestamp
    requested_duration_seconds: float
    actual_duration_nanoseconds: int
    selected_condition_ids: tuple[str, ...]
    selected_asset_ids: tuple[str, ...]
    journal_root: Path
    journal_metrics: JournalMetricsSnapshot
    processor: ProcessorSummary
    analytical_outputs: ResearchOutputSummary
    observability_metrics: MetricsSnapshot
    ingress_metrics: RawIngressMetricsSnapshot
    reconnects: int
    selected_market_resolved: bool
    protocol_deviations: tuple[str, ...]


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
    ) -> ProcessedEnvelope:
        self.metrics.increment_counter("mma_raw_received_total")
        self.metrics.observe_histogram("mma_raw_payload_bytes", len(payload))
        received_monotonic_ns = time.monotonic_ns()
        envelope = self._capture_envelope(
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=UtcTimestamp.now(),
            received_monotonic_ns=received_monotonic_ns,
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
                transport_metadata={"websocket_message_type": message.message_type},
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

    async def run(self, *, duration_seconds: float) -> ObservationSummary:
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
        loop = asyncio.get_running_loop()
        started_monotonic_ns = time.monotonic_ns()
        deadline = loop.time() + duration_seconds
        started = UtcTimestamp.now()
        run_id = CollectorRunId.new()
        logger = self._logger.bind(collector_run_id=run_id)
        journal_root = self._path(self._config.storage.journal_directory)
        analytical_root = self._path(self._config.storage.analytical_directory)
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
        stream_result = _StreamResult(0, False, False, ())
        deviations: list[str] = []
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
            ) as client:
                try:
                    selected, selected_assets = await self._bootstrap(
                        client=client,
                        capture=capture,
                        processor=processor,
                        deadline=deadline,
                    )
                except _ObservationDeadlineReached:
                    stream_result = _StreamResult(0, True, False, ())
                if loop.time() < deadline and selected_assets:
                    stream_result = await self._observe_websocket(
                        client=client,
                        capture=capture,
                        selected=selected,
                        asset_ids=selected_assets,
                        deadline=deadline,
                        logger=logger,
                    )
                    deviations.extend(stream_result.deviations)
                run_completed = True
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
                analytical_summary = await outputs.close(
                    source_journal_completion_sha256=raw_completion_sha256,
                    completed=run_completed and raw_completion_sha256 is not None,
                )
        ended = UtcTimestamp.now()
        actual_duration_nanoseconds = time.monotonic_ns() - started_monotonic_ns
        status = (
            ComponentHealth.HEALTHY
            if (stream_result.completed and not deviations and analytical_summary.research_eligible)
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
            selected_condition_ids=tuple(item.condition_id for item in selected),
            selected_asset_ids=selected_assets,
            journal_root=journal_root,
            journal_metrics=journal_metrics,
            processor=processor_summary,
            analytical_outputs=analytical_summary,
            observability_metrics=self._metrics.snapshot(),
            ingress_metrics=stream_result.ingress_metrics,
            reconnects=stream_result.reconnects,
            selected_market_resolved=stream_result.selected_market_resolved,
            protocol_deviations=tuple(deviations),
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
    ) -> tuple[tuple[GammaMarket, ...], tuple[str, ...]]:
        discovery = await _await_remote_before_deadline(
            lambda: client.fetch_markets_keyset_raw(
                limit=self._config.polymarket.discovery_limit,
                closed=False,
            ),
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
        return selected, selected_assets

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
        gamma_payload = await _await_remote_before_deadline(
            lambda: client.fetch_gamma_market_raw(gamma_market_id),
            deadline=deadline,
        )
        channel = f"{GAMMA_MARKET_PREFIX}{gamma_market_id}"
        await capture.preserve_then_process(
            channel=channel,
            payload=gamma_payload,
            transport_type="HTTP",
            semantics_hash=_semantics(channel),
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
        event_id = current.event_id
        if event_id is not None:
            event_payload = await _await_remote_before_deadline(
                lambda: client.fetch_gamma_event_raw(event_id),
                deadline=deadline,
            )
            event_channel = f"{GAMMA_EVENT_PREFIX}{event_id}"
            await capture.preserve_then_process(
                channel=event_channel,
                payload=event_payload,
                transport_type="HTTP",
                semantics_hash=_semantics(event_channel),
            )
        clob_payload = await _await_remote_before_deadline(
            lambda: client.fetch_clob_market_raw(current.condition_id),
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
        payload = await _await_remote_before_deadline(
            lambda: client.fetch_order_book_raw(asset_id),
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

    async def _audit_and_resnapshot(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
    ) -> tuple[str, ...]:
        deviations: list[str] = []
        mismatch_detected = False
        self._metrics.increment_counter("mma_rest_audit_cycles_total")
        for market in selected:
            for asset_id in market.clob_token_ids:
                mismatches_before = capture.processor.summary().rest_audit_mismatches
                result = await self._fetch_book(
                    client,
                    capture,
                    asset_id,
                    audit=True,
                    deadline=deadline,
                )
                mismatches_after = capture.processor.summary().rest_audit_mismatches
                if mismatches_after > mismatches_before:
                    mismatch_detected = True
                    deviations.append(f"REST audit mismatch at ingest_seq={result.ingest_seq}")
                    self._metrics.increment_counter("mma_rest_audit_mismatches_total")
        # The stream generation is closed before comparison, so a matching
        # authoritative REST view can advance freshness without a duplicate
        # fetch. A mismatch remains fail-closed and gets a joint rebaseline.
        if mismatch_detected:
            await self._resnapshot(
                client=client,
                capture=capture,
                selected=selected,
                deadline=deadline,
            )
        return tuple(deviations)

    async def _handle_refresh_requests(
        self,
        *,
        requests: tuple[ReferenceRefreshRequest, ...],
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
    ) -> bool:
        selected_by_gamma_id = {market.gamma_market_id: market for market in selected}
        selected_by_condition = {market.condition_id: market for market in selected}
        selected_by_asset = {
            asset_id: market for market in selected for asset_id in market.clob_token_ids
        }
        selected_resolved = False
        refresh_gamma_ids: set[str] = set()
        for request in requests:
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
        capture: _RawCaptureBoundary,
        asset_ids: tuple[str, ...],
        deadline_ns: int,
        periodic_audit_due_ns: int,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        unavailable_started_ns: int | None,
    ) -> _GenerationResult:
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
        )
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
        pending_exit: _GenerationExit | None = None
        pending_transport_error: PolymarketTransportError | None = None
        deviations: list[str] = []
        refresh_requests: list[ReferenceRefreshRequest] = []

        def current_deadline() -> IntegrityDeadline:
            return next_integrity_deadline(
                run_deadline_ns=deadline_ns,
                periodic_audit_deadline_ns=periodic_audit_due_ns,
                valid_book_receive_ns=capture.book_freshness_receive_ns,
                required_books=len(asset_ids),
                maximum_book_age_ns=maximum_book_age_ns,
                now_ns=time.monotonic_ns(),
            )

        async def stop_acquisition() -> None:
            if not acquisition_task.done():
                acquisition_task.cancel()
            with suppress(
                asyncio.CancelledError,
                PolymarketTransportError,
                RawIngressBackpressureError,
            ):
                await acquisition_task

        try:
            initial_due = current_deadline()
            done, _ = await asyncio.wait(
                {ready_task, acquisition_task},
                timeout=max(0.0, (initial_due.deadline_ns - time.monotonic_ns()) / 1e9),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if acquisition_task in done and ready_task not in done:
                acquisition_task.result()
                raise PolymarketTransportError("public market WebSocket ended before readiness")
            if ready_task not in done:
                await stop_acquisition()
                reason = (
                    _GenerationExit.DEADLINE
                    if initial_due.reason is IntegrityDeadlineReason.RUN_DEADLINE
                    else _GenerationExit.AUDIT_DUE
                )
                return _GenerationResult(reason, ingress_metrics=ingress.metrics)
            if unavailable_started_ns is not None:
                self._metrics.observe_histogram(
                    "mma_socket_unavailable_ns",
                    max(0, time.monotonic_ns() - unavailable_started_ns),
                )

            while True:
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
                        self._metrics.increment_counter("mma_ingress_overflows_total")
                    except PolymarketTransportError as error:
                        pending_transport_error = error
                    else:
                        if ingress.empty:
                            raise PolymarketTransportError(
                                "public market WebSocket ended without an explicit transport error"
                            )
                if pending_exit is None and now_ns >= due_ns:
                    pending_exit = (
                        _GenerationExit.DEADLINE
                        if due.reason is IntegrityDeadlineReason.RUN_DEADLINE
                        else _GenerationExit.AUDIT_DUE
                    )
                    await stop_acquisition()

                first_item: RawIngressItem | None = None
                if not ingress.empty:
                    first_item = ingress.get_nowait()
                elif pending_exit is not None:
                    return _GenerationResult(
                        pending_exit,
                        deviations=tuple(deviations),
                        refresh_requests=tuple(refresh_requests),
                        ingress_metrics=ingress.metrics,
                    )
                elif pending_transport_error is not None:
                    raise pending_transport_error
                elif acquisition_task.done():
                    try:
                        acquisition_task.result()
                    except RawIngressBackpressureError:
                        pending_exit = _GenerationExit.INGRESS_OVERFLOW
                        deviations.append("bounded application ingress queue exhausted")
                        self._metrics.increment_counter("mma_ingress_overflows_total")
                    else:
                        raise PolymarketTransportError(
                            "public market WebSocket ended without an explicit transport error"
                        )
                    continue

                if first_item is None:
                    item_task = asyncio.create_task(
                        ingress.get(),
                        name="polymarket-public-ingress-dequeue",
                    )
                    done, _ = await asyncio.wait(
                        {item_task, acquisition_task},
                        timeout=max(0.0, (due_ns - time.monotonic_ns()) / 1e9),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if item_task in done:
                        first_item = item_task.result()
                    else:
                        item_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await item_task
                        continue

                batch = [first_item]
                while len(batch) < self._config.storage.journal_group_commit_max_events:
                    try:
                        batch.append(ingress.get_nowait())
                    except asyncio.QueueEmpty:
                        break
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
                    refresh_requests.extend(result.reference_refresh_requests)
                    if result.needs_resync:
                        pending_exit = _GenerationExit.RESYNC_REQUIRED
                        deviations.append(
                            f"{result.error_type or 'state-invalidation'} at "
                            f"ingest_seq={result.ingest_seq}"
                        )
                    elif result.reference_refresh_requests and pending_exit is None:
                        pending_exit = _GenerationExit.REFERENCE_REFRESH
                if pending_exit is not None:
                    await stop_acquisition()
        finally:
            if not ready_task.done():
                ready_task.cancel()
            with suppress(asyncio.CancelledError):
                await ready_task
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
    ) -> _StreamResult:
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
                generation = await self._run_generation(
                    capture=capture,
                    asset_ids=asset_ids,
                    deadline_ns=deadline_ns,
                    periodic_audit_due_ns=periodic_audit_due_ns,
                    connection_id=connection_id,
                    subscription_id=subscription_id,
                    unavailable_started_ns=unavailable_started_ns,
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
                if generation.reason is _GenerationExit.AUDIT_DUE:
                    logger.info(
                        "rest_integrity_audit",
                        "closed stream generation for public REST integrity audit",
                        connection_id=connection_id,
                    )
                    deviations.extend(
                        await self._audit_and_resnapshot(
                            client=client,
                            capture=capture,
                            selected=selected,
                            deadline=deadline,
                        )
                    )
                    periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
                    continue
                if generation.reason is _GenerationExit.REFERENCE_REFRESH:
                    selected_resolved = await self._handle_refresh_requests(
                        requests=generation.refresh_requests,
                        client=client,
                        capture=capture,
                        selected=selected,
                        deadline=deadline,
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
                await self._resnapshot(
                    client=client,
                    capture=capture,
                    selected=selected,
                    deadline=deadline,
                )
                periodic_audit_due_ns = time.monotonic_ns() + integrity_interval_ns
            except _ObservationDeadlineReached:
                return _StreamResult(
                    reconnects,
                    True,
                    False,
                    tuple(deviations),
                    ingress_metrics,
                )
            except PolymarketTransportError as error:
                unavailable_started_ns = time.monotonic_ns()
                await capture.preserve_invalidation(InvalidationReason.DISCONNECT)
                deviations.append(type(error).__name__)
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
