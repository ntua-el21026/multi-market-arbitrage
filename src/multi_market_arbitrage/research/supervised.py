"""Bounded supervised owners isolating derived analytical persistence."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar, cast

from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.normalization import CanonicalEvent
from multi_market_arbitrage.observability import InMemoryMetrics
from multi_market_arbitrage.reference import ReferenceBundle
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScanResult,
    OpportunityLifecycle,
)

from .outputs import (
    AnalyticalFragmentRetentionSnapshot,
    AnalyticalOutputRuntimeSnapshot,
    Phase1AnalyticalOutputs,
    ResearchOutputSummary,
)


@dataclass(frozen=True, slots=True)
class _ReferenceWrite:
    bundle: ReferenceBundle
    observed_at: UtcTimestamp
    provenance: Provenance


@dataclass(frozen=True, slots=True)
class _CanonicalWrite:
    event: CanonicalEvent


@dataclass(frozen=True, slots=True)
class _ScanWrite:
    result: CompleteSetScanResult
    lifecycle: OpportunityLifecycle | None


@dataclass(frozen=True, slots=True)
class AnalyticalQueueSnapshot:
    domain: str
    depth: int
    high_water: int


@dataclass(frozen=True, slots=True)
class AnalyticalRuntimeSnapshot:
    healthy: bool
    accepted_records: int
    processed_records: int
    rejected_records: int
    queues: tuple[AnalyticalQueueSnapshot, ...]
    failure_type: str | None
    buffered_records: int = 0
    buffered_bytes: int = 0
    buffered_records_high_water: int = 0
    buffered_bytes_high_water: int = 0
    flushes: int = 0
    flushed_records: int = 0
    pyarrow_current_bytes: int = 0
    pyarrow_peak_bytes: int = 0
    fragment_retention: tuple[AnalyticalFragmentRetentionSnapshot, ...] = ()


CommandT = TypeVar("CommandT")


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


_STOP = _Stop()


class _DomainOwner[CommandT]:
    def __init__(self, capacity: int, name: str) -> None:
        self.name = name
        self.queue: asyncio.Queue[CommandT | _Stop] = asyncio.Queue(maxsize=capacity)
        self.task: asyncio.Task[None] | None = None
        self.high_water = 0


class SupervisedAnalyticalOutputs:
    """Processor-compatible, non-blocking handoff to three writer owners."""

    def __init__(
        self,
        outputs: Phase1AnalyticalOutputs,
        *,
        queue_capacity: int,
        metrics: InMemoryMetrics | None = None,
    ) -> None:
        if queue_capacity <= 0:
            raise ValueError("analytical queue capacity must be positive")
        self._outputs = outputs
        self._metrics = metrics
        self._references: _DomainOwner[_ReferenceWrite] = _DomainOwner(queue_capacity, "reference")
        self._canonical: _DomainOwner[_CanonicalWrite] = _DomainOwner(
            queue_capacity, "canonical_events"
        )
        self._scans: _DomainOwner[_ScanWrite] = _DomainOwner(queue_capacity, "complete_set_scans")
        self._healthy = True
        self._closed = False
        self._accepted = 0
        self._processed = 0
        self._rejected = 0
        self._failure_type: str | None = None
        if metrics is not None:
            metrics.set_gauge("mma_analytical_healthy", 1)

    async def start(self) -> None:
        if self._closed or any(
            owner.task is not None for owner in (self._references, self._canonical, self._scans)
        ):
            raise RuntimeError("analytical output owners can only be started once")
        self._references.task = asyncio.create_task(
            self._run_references(), name="mma-analytical-reference-writer"
        )
        self._canonical.task = asyncio.create_task(
            self._run_canonical(), name="mma-analytical-canonical-writer"
        )
        self._scans.task = asyncio.create_task(self._run_scans(), name="mma-analytical-scan-writer")

    @property
    def research_eligible(self) -> bool:
        return self._healthy and not self._closed

    @property
    def failure_type(self) -> str | None:
        return self._failure_type

    @property
    def runtime_snapshot(self) -> AnalyticalRuntimeSnapshot:
        owners = (self._references, self._canonical, self._scans)
        output_snapshot_method = getattr(self._outputs, "operational_snapshot", None)
        output_snapshot = (
            cast(
                Callable[[], AnalyticalOutputRuntimeSnapshot],
                output_snapshot_method,
            )()
            if callable(output_snapshot_method)
            else AnalyticalOutputRuntimeSnapshot(0, 0, 0, 0, 0, 0, 0, 0)
        )
        return AnalyticalRuntimeSnapshot(
            healthy=self._healthy,
            accepted_records=self._accepted,
            processed_records=self._processed,
            rejected_records=self._rejected,
            queues=tuple(
                AnalyticalQueueSnapshot(
                    domain=owner.name,
                    depth=owner.queue.qsize(),
                    high_water=owner.high_water,
                )
                for owner in owners
            ),
            failure_type=self._failure_type,
            buffered_records=output_snapshot.buffered_records,
            buffered_bytes=output_snapshot.buffered_bytes,
            buffered_records_high_water=output_snapshot.buffered_records_high_water,
            buffered_bytes_high_water=output_snapshot.buffered_bytes_high_water,
            flushes=output_snapshot.flushes,
            flushed_records=output_snapshot.flushed_records,
            pyarrow_current_bytes=output_snapshot.pyarrow_current_bytes,
            pyarrow_peak_bytes=output_snapshot.pyarrow_peak_bytes,
            fragment_retention=output_snapshot.fragment_retention,
        )

    def _reject_or_put(self, owner: _DomainOwner[CommandT], command: CommandT) -> None:
        if not self._healthy or self._closed:
            self._rejected += 1
            if self._metrics is not None:
                self._metrics.increment_counter("mma_analytical_records_rejected_total")
            return
        try:
            owner.queue.put_nowait(command)
        except asyncio.QueueFull:
            self._rejected += 1
            if self._metrics is not None:
                self._metrics.increment_counter(
                    "mma_analytical_queue_saturations_total",
                    labels={"domain": owner.name},
                )
                self._metrics.increment_counter("mma_analytical_records_rejected_total")
            self._degrade(RuntimeError("analytical output queue saturated"))
            return
        self._accepted += 1
        owner.high_water = max(owner.high_water, owner.queue.qsize())
        if self._metrics is not None:
            labels = {"domain": owner.name}
            self._metrics.increment_counter(
                "mma_analytical_records_accepted_total",
                labels=labels,
            )
            self._metrics.set_gauge(
                "mma_analytical_queue_depth",
                owner.queue.qsize(),
                labels=labels,
            )
            self._metrics.set_gauge(
                "mma_analytical_queue_high_water",
                owner.high_water,
                labels=labels,
            )

    def _degrade(self, error: BaseException) -> None:
        if self._healthy:
            self._healthy = False
            self._failure_type = type(error).__name__
            if self._metrics is not None:
                self._metrics.set_gauge("mma_analytical_healthy", 0)
                self._metrics.increment_counter(
                    "mma_analytical_failures_total",
                    labels={"failure_type": self._failure_type},
                )

    def append_reference(
        self,
        bundle: ReferenceBundle,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> None:
        self._reject_or_put(self._references, _ReferenceWrite(bundle, observed_at, provenance))

    def append_canonical(self, event: CanonicalEvent) -> None:
        self._reject_or_put(self._canonical, _CanonicalWrite(event))

    def append_scan(
        self,
        result: CompleteSetScanResult,
        *,
        lifecycle: OpportunityLifecycle | None = None,
    ) -> None:
        self._reject_or_put(self._scans, _ScanWrite(result, lifecycle))

    async def _consume(
        self,
        owner: _DomainOwner[CommandT],
        write: Callable[[CommandT], None],
    ) -> None:
        while True:
            command = await owner.queue.get()
            if self._metrics is not None:
                self._metrics.set_gauge(
                    "mma_analytical_queue_depth",
                    owner.queue.qsize(),
                    labels={"domain": owner.name},
                )
            if isinstance(command, _Stop):
                return
            if not self._healthy:
                self._rejected += 1
                if self._metrics is not None:
                    self._metrics.increment_counter("mma_analytical_records_rejected_total")
                continue
            try:
                await asyncio.to_thread(write, command)
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)
            else:
                self._processed += 1
                if self._metrics is not None:
                    self._metrics.increment_counter(
                        "mma_analytical_records_processed_total",
                        labels={"domain": owner.name},
                    )

    async def _run_references(self) -> None:
        def write(item: _ReferenceWrite) -> None:
            self._outputs.append_reference(item.bundle, item.observed_at, item.provenance)

        await self._consume(self._references, write)

    async def _run_canonical(self) -> None:
        def write(item: _CanonicalWrite) -> None:
            self._outputs.append_canonical(item.event)

        await self._consume(self._canonical, write)

    async def _run_scans(self) -> None:
        def write(item: _ScanWrite) -> None:
            self._outputs.append_scan(item.result, lifecycle=item.lifecycle)

        await self._consume(self._scans, write)

    async def close(
        self,
        *,
        source_journal_completion_sha256: str | None = None,
        completed: bool = True,
    ) -> ResearchOutputSummary:
        if self._closed:
            raise RuntimeError("analytical output owners are already closed")
        self._closed = True
        if not completed:
            self._degrade(RuntimeError("authoritative raw journal run is incomplete"))
        owners = (self._references, self._canonical, self._scans)
        for owner in owners:
            if owner.task is None:
                raise RuntimeError("analytical output owners were not started")
            await owner.queue.put(_STOP)
        await asyncio.shield(
            asyncio.gather(*(owner.task for owner in owners if owner.task is not None))
        )
        base: ResearchOutputSummary | None = None
        if self._healthy:
            try:
                base = await asyncio.to_thread(
                    self._outputs.close,
                    source_journal_completion_sha256=source_journal_completion_sha256,
                )
            except BaseException as error:
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                self._degrade(error)
        if base is None:
            base = await asyncio.to_thread(
                self._outputs.abort,
                failure_type=self._failure_type,
            )
        return ResearchOutputSummary(
            root=base.root,
            reference_manifest_hashes=base.reference_manifest_hashes,
            canonical_manifest_hashes=base.canonical_manifest_hashes,
            scan_manifest_hashes=base.scan_manifest_hashes,
            health="HEALTHY" if self._healthy else "DEGRADED",
            research_eligible=self._healthy and base.completion_sha256 is not None,
            accepted_records=self._accepted,
            processed_records=self._processed,
            rejected_records=self._rejected,
            queue_high_water=max(owner.high_water for owner in owners),
            failure_type=self._failure_type,
            completion_sha256=base.completion_sha256,
        )


__all__ = [
    "AnalyticalQueueSnapshot",
    "AnalyticalRuntimeSnapshot",
    "SupervisedAnalyticalOutputs",
]
