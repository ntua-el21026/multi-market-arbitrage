from __future__ import annotations

import asyncio
import hashlib
import threading
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    CollectorRunId,
    IngestId,
    InstrumentId,
    MarketId,
    NormalizerVersion,
    SchemaVersion,
    VenueId,
)
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.normalization import (
    BookSide,
    CanonicalEvent,
    CanonicalEventType,
    LastTradePriceEvent,
)
from multi_market_arbitrage.research import (
    Phase1AnalyticalOutputs,
    ResearchOutputSummary,
    SupervisedAnalyticalOutputs,
)
from multi_market_arbitrage.storage import AnalyticalDatasetWriter


class _FaultingOutputs:
    def __init__(self, root: Path, *, fail: bool) -> None:
        self.root = root
        self.fail = fail
        self.appended = 0
        self.aborted = False
        self.closed = False

    def append_canonical(self, event: CanonicalEvent) -> None:
        del event
        if self.fail:
            raise OSError("injected Parquet write fault")
        self.appended += 1

    def append_reference(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def append_scan(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def close(
        self,
        *,
        source_journal_completion_sha256: str | None = None,
    ) -> ResearchOutputSummary:
        del source_journal_completion_sha256
        self.closed = True
        return ResearchOutputSummary(self.root, (), (), (), completion_sha256="0" * 64)

    def abort(self, *, failure_type: str | None = None) -> ResearchOutputSummary:
        self.aborted = True
        return ResearchOutputSummary(
            self.root,
            (),
            (),
            (),
            health="DEGRADED",
            research_eligible=False,
            failure_type=failure_type,
        )


def _event() -> CanonicalEvent:
    return CanonicalEvent(
        canonical_event_id=CanonicalEventId("event"),
        schema_version=SchemaVersion("schema"),
        normalizer_version=NormalizerVersion("normalizer"),
        event_type=CanonicalEventType.LAST_TRADE_PRICE,
        venue_id=VenueId("polymarket"),
        market_id=MarketId("market"),
        instrument_id=InstrumentId("instrument"),
        source_ingest_ids=(IngestId("ingest"),),
        source_payload_hashes=(hashlib.sha256(b"raw").hexdigest(),),
        reference_version_ids=(),
        venue_timestamp=None,
        receive_timestamp=UtcTimestamp(1),
        deduplication_key=None,
        payload=LastTradePriceEvent(
            instrument_id=InstrumentId("instrument"),
            market_id=MarketId("market"),
            price=Price(ExactDecimal.parse("0.5")),
            quantity=Quantity(ExactDecimal.parse("1")),
            taker_side=BookSide.BID,
            fee_rate_bps=None,
            transaction_hash=None,
        ),
    )


@pytest.mark.asyncio
async def test_writer_fault_is_isolated_and_disables_research(tmp_path: Path) -> None:
    faulting = _FaultingOutputs(tmp_path, fail=True)
    supervised = SupervisedAnalyticalOutputs(
        cast(Phase1AnalyticalOutputs, faulting),
        queue_capacity=4,
    )
    await supervised.start()
    supervised.append_canonical(_event())
    for _ in range(100):
        if supervised.failure_type is not None:
            break
        await asyncio.sleep(0)
    summary = await supervised.close()

    assert summary.health == "DEGRADED"
    assert summary.research_eligible is False
    assert summary.failure_type == "OSError"
    assert summary.accepted_records == 1
    assert summary.processed_records == 0
    assert faulting.aborted is True
    assert faulting.closed is False


@pytest.mark.asyncio
async def test_analytical_queue_saturation_is_explicit_and_bounded(tmp_path: Path) -> None:
    sink = _FaultingOutputs(tmp_path, fail=False)
    supervised = SupervisedAnalyticalOutputs(
        cast(Phase1AnalyticalOutputs, sink),
        queue_capacity=1,
    )
    # Admission is non-blocking. With no worker started, the second immutable
    # handoff deterministically saturates the bounded domain queue.
    supervised.append_canonical(_event())
    supervised.append_canonical(_event())
    await supervised.start()
    summary = await supervised.close()

    assert summary.health == "DEGRADED"
    assert summary.research_eligible is False
    assert summary.accepted_records == 1
    assert summary.rejected_records >= 2  # rejected trigger plus drained accepted command
    assert summary.queue_high_water == 1
    assert sink.aborted is True


@pytest.mark.asyncio
async def test_analytical_runtime_reports_buffers_flushes_and_pyarrow_memory(
    tmp_path: Path,
) -> None:
    persistent = Phase1AnalyticalOutputs(
        root=tmp_path,
        partition_date="2026-08-24",
        batch_size=1,
        collector_run_id=CollectorRunId("runtime-telemetry"),
        code_commit="fixture",
        code_version="test",
        config_hash=hashlib.sha256(b"config").hexdigest(),
    )
    supervised = SupervisedAnalyticalOutputs(persistent, queue_capacity=2)
    await supervised.start()
    try:
        supervised.append_canonical(_event())
        for _ in range(500):
            snapshot = supervised.runtime_snapshot
            if snapshot.processed_records == 1:
                break
            await asyncio.sleep(0.01)

        snapshot = supervised.runtime_snapshot
        assert snapshot.processed_records == 1
        assert snapshot.buffered_records == 0
        assert snapshot.buffered_bytes == 0
        assert snapshot.buffered_records_high_water == 1
        assert snapshot.buffered_bytes_high_water > 0
        assert snapshot.flushes == 1
        assert snapshot.flushed_records == 1
        assert snapshot.pyarrow_current_bytes >= 0
        assert snapshot.pyarrow_peak_bytes >= snapshot.pyarrow_current_bytes
        retention = {item.domain: item for item in snapshot.fragment_retention}
        assert set(retention) == {
            "canonical_events",
            "complete_set_scans",
            "reference",
        }
        canonical = retention["canonical_events"]
        assert canonical.retained == 1
        assert canonical.capacity == canonical.maximum_dataset_fragments == 4_096
        assert canonical.dataset_fragments_at_open == 0
        assert canonical.dataset_fragment_reservations == 1
        assert canonical.estimated_retained_bytes <= canonical.maximum_estimated_retained_bytes
    finally:
        await supervised.close()


@pytest.mark.asyncio
async def test_slow_analytical_flush_stays_off_the_event_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()
    flush_entered = asyncio.Event()
    release_flush = threading.Event()
    original_flush = AnalyticalDatasetWriter.flush

    def gated_flush(writer: AnalyticalDatasetWriter) -> object:
        if writer.buffered_records:
            loop.call_soon_threadsafe(flush_entered.set)
            if not release_flush.wait(timeout=5):
                raise TimeoutError("test did not release the analytical flush")
        return original_flush(writer)

    monkeypatch.setattr(AnalyticalDatasetWriter, "flush", gated_flush)
    persistent = Phase1AnalyticalOutputs(
        root=tmp_path,
        partition_date="2026-08-24",
        batch_size=1,
        collector_run_id=CollectorRunId("slow-analytical-flush"),
        code_commit="fixture",
        code_version="test",
        config_hash=hashlib.sha256(b"config").hexdigest(),
    )
    supervised = SupervisedAnalyticalOutputs(persistent, queue_capacity=2)
    await supervised.start()
    summary: ResearchOutputSummary | None = None
    try:
        supervised.append_canonical(_event())
        async with asyncio.timeout(5):
            await flush_entered.wait()

        # The physical Parquet flush is blocked in the storage worker.  A new
        # event-loop callback must still run, while no analytical completion is
        # claimed before the flush is released.
        event_loop_advanced = asyncio.Event()
        loop.call_soon(event_loop_advanced.set)
        async with asyncio.timeout(1):
            await event_loop_advanced.wait()
        blocked = supervised.runtime_snapshot
        assert blocked.accepted_records == 1
        assert blocked.processed_records == 0
        assert blocked.flushes == 0
    finally:
        release_flush.set()
        summary = await supervised.close()

    assert summary.health == "HEALTHY"
    assert summary.research_eligible
    assert summary.accepted_records == summary.processed_records == 1
    assert persistent.operational_snapshot().flushes == 1


@pytest.mark.parametrize("fault", ["parquet", "fsync", "manifest"])
@pytest.mark.asyncio
async def test_physical_analytical_faults_publish_no_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from multi_market_arbitrage.storage import analytical as analytical_module

    def raise_fault(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError(f"injected {fault} fault")

    if fault == "parquet":
        monkeypatch.setattr(analytical_module, "pyarrow_write_table", raise_fault)
    elif fault == "fsync":
        monkeypatch.setattr(analytical_module.os, "fsync", raise_fault)
    else:
        monkeypatch.setattr(analytical_module, "atomic_write_bytes", raise_fault)

    persistent = Phase1AnalyticalOutputs(
        root=tmp_path,
        partition_date="2026-08-21",
        batch_size=1,
        collector_run_id=CollectorRunId(f"run-{fault}"),
        code_commit="fixture",
        code_version="test",
        config_hash=hashlib.sha256(b"config").hexdigest(),
    )
    supervised = SupervisedAnalyticalOutputs(persistent, queue_capacity=2)
    await supervised.start()
    supervised.append_canonical(_event())
    for _ in range(100):
        if supervised.failure_type is not None:
            break
        await asyncio.sleep(0)
    summary = await supervised.close()

    assert summary.research_eligible is False
    assert summary.failure_type == "OSError"
    completion_heads = await asyncio.to_thread(
        lambda: tuple(tmp_path.glob("_analytical-run.*.completed.json"))
    )
    assert not completion_heads
