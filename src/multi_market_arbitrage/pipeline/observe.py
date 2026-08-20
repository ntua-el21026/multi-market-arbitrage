"""Bounded unauthenticated Polymarket public-data observation service."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
from collections.abc import AsyncGenerator
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from multi_market_arbitrage.core.config import Phase1Config, Phase1RuntimeRole
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
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
    JournalWriterConfig,
    RawJournalWriter,
)
from multi_market_arbitrage.normalization import CanonicalEventType
from multi_market_arbitrage.observability import (
    InMemoryMetrics,
    MetricsSnapshot,
    StructuredLogger,
    configure_json_logger,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import Phase1AnalyticalOutputs, ResearchOutputSummary
from multi_market_arbitrage.state.order_books import InvalidationReason
from multi_market_arbitrage.venues.polymarket import (
    PolymarketMarketWebSocket,
    PolymarketPublicClient,
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


@dataclass(frozen=True, slots=True)
class _GenerationResult:
    reason: _GenerationExit
    deviations: tuple[str, ...] = ()
    refresh_requests: tuple[ReferenceRefreshRequest, ...] = ()


@dataclass(frozen=True, slots=True)
class _StreamResult:
    reconnects: int
    completed: bool
    selected_market_resolved: bool
    deviations: tuple[str, ...]


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
        envelope = RawEventEnvelope.capture(
            collector_run_id=self.collector_run_id,
            ingest_seq=self._next_sequence,
            venue_id=self.writer.config.venue_id,
            connection_id=connection_id or ConnectionId.new(),
            subscription_id=subscription_id or SubscriptionId.new(),
            channel=channel,
            transport_type=transport_type,
            subscription_semantics_hash=semantics_hash,
            received_wall_time=UtcTimestamp.now(),
            received_monotonic=MonotonicTimestamp.now(self.collector_run_id.value),
            payload_bytes=payload,
            payload_encoding="utf-8",
            transport_metadata=transport_metadata,
        )
        self._next_sequence += 1
        try:
            receipt = await self.writer.submit(envelope)
        except JournalBackpressureError:
            self.metrics.increment_counter("mma_journal_backpressure_failures_total")
            # A failed capture is never replay-authoritative; invalidate live
            # state immediately and let the fatal journal fault abort the run.
            self.processor.on_backpressure()
            raise
        self.metrics.increment_counter("mma_raw_durable_total")
        self.metrics.set_gauge("mma_journal_queue_depth", self.writer.metrics.queue_depth)
        result = self.processor.process_durable(envelope, receipt)
        self.metrics.increment_counter("mma_raw_processed_total")
        if not result.success:
            self.metrics.increment_counter("mma_parse_failures_total")
        return result

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
        outputs = Phase1AnalyticalOutputs(
            root=analytical_root,
            partition_date=partition_date,
            batch_size=self._config.storage.parquet_batch_size,
            collector_run_id=run_id,
            code_commit=self._code_commit,
            code_version=self._config.system.code_version,
            config_hash=self._config.config_hash.value,
        )
        processor = CanonicalProcessor(self._config, analytical_outputs=outputs)
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
        try:
            await writer.start()
        except BaseException:
            outputs.close()
            raise
        try:
            async with PolymarketPublicClient(
                gamma_url=self._config.polymarket.gamma_base_url,
                clob_url=self._config.polymarket.clob_base_url,
                timeout_seconds=self._config.polymarket.request_timeout_seconds,
                max_response_bytes=self._config.polymarket.maximum_http_response_bytes,
            ) as client:
                try:
                    async with asyncio.timeout(max(0.0, deadline - loop.time())):
                        selected, selected_assets = await self._bootstrap(
                            client=client,
                            capture=capture,
                            processor=processor,
                        )
                except TimeoutError:
                    deviations.append("observation deadline reached during bootstrap")
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
            try:
                await writer.close(completed=run_completed)
            finally:
                journal_metrics = writer.metrics
                analytical_summary = outputs.close()
        ended = UtcTimestamp.now()
        actual_duration_nanoseconds = time.monotonic_ns() - started_monotonic_ns
        status = (
            ComponentHealth.HEALTHY
            if stream_result.completed and not deviations
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
    ) -> tuple[tuple[GammaMarket, ...], tuple[str, ...]]:
        discovery = await client.fetch_markets_keyset_raw(
            limit=self._config.polymarket.discovery_limit,
            closed=False,
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
            await self._refresh_reference(client, capture, market)
            for asset_id in market.clob_token_ids:
                await self._fetch_book(client, capture, asset_id, audit=False)
        selected_assets = tuple(asset for market in selected for asset in market.clob_token_ids)
        return selected, selected_assets

    async def _refresh_reference(
        self,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        market: GammaMarket,
    ) -> None:
        await self._refresh_reference_by_id(
            client,
            capture,
            gamma_market_id=market.gamma_market_id,
            fallback=market,
        )

    async def _refresh_reference_by_id(
        self,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        *,
        gamma_market_id: str,
        fallback: GammaMarket | None = None,
    ) -> None:
        gamma_payload = await client.fetch_gamma_market_raw(gamma_market_id)
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
        if current.event_id is not None:
            event_payload = await client.fetch_gamma_event_raw(current.event_id)
            event_channel = f"{GAMMA_EVENT_PREFIX}{current.event_id}"
            await capture.preserve_then_process(
                channel=event_channel,
                payload=event_payload,
                transport_type="HTTP",
                semantics_hash=_semantics(event_channel),
            )
        clob_payload = await client.fetch_clob_market_raw(current.condition_id)
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
    ) -> ProcessedEnvelope:
        payload = await client.fetch_order_book_raw(asset_id)
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
    ) -> None:
        for market in selected:
            await self._refresh_reference(client, capture, market)
            for asset_id in market.clob_token_ids:
                await self._fetch_book(client, capture, asset_id, audit=False)

    async def _audit_and_resnapshot(
        self,
        *,
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
    ) -> tuple[str, ...]:
        deviations: list[str] = []
        self._metrics.increment_counter("mma_rest_audit_cycles_total")
        for market in selected:
            for asset_id in market.clob_token_ids:
                mismatches_before = capture.processor.summary().rest_audit_mismatches
                result = await self._fetch_book(client, capture, asset_id, audit=True)
                mismatches_after = capture.processor.summary().rest_audit_mismatches
                if mismatches_after > mismatches_before:
                    deviations.append(f"REST audit mismatch at ingest_seq={result.ingest_seq}")
                    self._metrics.increment_counter("mma_rest_audit_mismatches_total")
        # The stream generation is already closed. Replace audited state with
        # fresh REST baselines before subscribing again, preventing buffered
        # pre-snapshot messages from crossing generations.
        await self._resnapshot(client=client, capture=capture, selected=selected)
        return tuple(deviations)

    async def _handle_refresh_requests(
        self,
        *,
        requests: tuple[ReferenceRefreshRequest, ...],
        client: PolymarketPublicClient,
        capture: _RawCaptureBoundary,
        selected: tuple[GammaMarket, ...],
    ) -> bool:
        selected_conditions = {market.condition_id for market in selected}
        selected_resolved = False
        for request in requests:
            self._metrics.increment_counter(
                "mma_reference_refresh_requests_total",
                labels={"event_type": request.event_type.value},
            )
            if (
                request.event_type is CanonicalEventType.MARKET_RESOLVED
                and request.native_condition_id in selected_conditions
            ):
                selected_resolved = True
            await self._refresh_reference_by_id(
                client,
                capture,
                gamma_market_id=request.native_market_id,
            )
        if requests and not selected_resolved:
            await self._resnapshot(client=client, capture=capture, selected=selected)
        return selected_resolved

    async def _run_generation(
        self,
        *,
        capture: _RawCaptureBoundary,
        asset_ids: tuple[str, ...],
        deadline: float,
        audit_due: float,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> _GenerationResult:
        socket = PolymarketMarketWebSocket(
            asset_ids,
            url=self._config.polymarket.market_websocket_url,
            ping_interval_seconds=self._config.polymarket.heartbeat_interval_seconds,
            max_queue=self._config.system.queue_capacity,
            max_message_bytes=self._config.polymarket.maximum_websocket_message_bytes,
        )
        messages = cast(AsyncGenerator[bytes], socket.raw_messages())
        message_task: asyncio.Task[bytes] | None = None
        try:
            while True:
                now = asyncio.get_running_loop().time()
                if now >= deadline:
                    return _GenerationResult(_GenerationExit.DEADLINE)
                if now >= audit_due:
                    return _GenerationResult(_GenerationExit.AUDIT_DUE)
                if message_task is None:
                    message_task = asyncio.create_task(
                        anext(messages),
                        name="polymarket-public-next-message",
                    )
                done, _ = await asyncio.wait(
                    {message_task},
                    timeout=max(0.0, min(deadline, audit_due) - now),
                )
                if not done:
                    continue
                payload = message_task.result()
                message_task = None
                result = await capture.preserve_then_process(
                    channel=WS_MARKET,
                    payload=payload,
                    transport_type="WEBSOCKET",
                    semantics_hash=socket.subscription_semantics_sha256,
                    connection_id=connection_id,
                    subscription_id=subscription_id,
                    transport_metadata={"websocket_message_type": "TEXT_OR_BINARY"},
                )
                if result.reference_refresh_requests:
                    return _GenerationResult(
                        _GenerationExit.REFERENCE_REFRESH,
                        refresh_requests=result.reference_refresh_requests,
                    )
                if result.needs_resync:
                    return _GenerationResult(
                        _GenerationExit.RESYNC_REQUIRED,
                        deviations=(
                            f"{result.error_type or 'state-invalidation'} at "
                            f"ingest_seq={result.ingest_seq}",
                        ),
                    )
        finally:
            if message_task is not None and not message_task.done():
                message_task.cancel()
            if message_task is not None:
                with suppress(asyncio.CancelledError, StopAsyncIteration):
                    await message_task
            with suppress(asyncio.CancelledError, RuntimeError):
                await messages.aclose()

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
        integrity_interval = min(
            self._config.polymarket.snapshot_audit_interval_seconds,
            self._config.scanner.maximum_book_age_seconds,
        )
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
                    deadline=deadline,
                    audit_due=min(deadline, loop.time() + integrity_interval),
                    connection_id=connection_id,
                    subscription_id=subscription_id,
                )
                deviations.extend(generation.deviations)
                if generation.reason is _GenerationExit.DEADLINE:
                    return _StreamResult(reconnects, True, False, tuple(deviations))
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
                        )
                    )
                    continue
                if generation.reason is _GenerationExit.REFERENCE_REFRESH:
                    selected_resolved = await self._handle_refresh_requests(
                        requests=generation.refresh_requests,
                        client=client,
                        capture=capture,
                        selected=selected,
                    )
                    if selected_resolved:
                        return _StreamResult(reconnects, True, True, tuple(deviations))
                    continue
                # Parser/state uncertainty already invalidated through the
                # journaled venue envelope. The old generator is closed; now
                # establish a baseline before creating a new socket.
                self._metrics.increment_counter("mma_resynchronizations_total")
                await self._resnapshot(client=client, capture=capture, selected=selected)
            except PolymarketTransportError as error:
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
                    return _StreamResult(reconnects, False, False, tuple(deviations))
                reconnects += 1
                self._metrics.increment_counter("mma_websocket_reconnects_total")
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return _StreamResult(reconnects, True, False, tuple(deviations))
                await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, self._config.polymarket.reconnect_max_seconds)
                if loop.time() < deadline:
                    await self._resnapshot(client=client, capture=capture, selected=selected)
            except asyncio.CancelledError:
                raise
        return _StreamResult(reconnects, True, False, tuple(deviations))


__all__ = ["ObservationSummary", "PolymarketObservationService"]
