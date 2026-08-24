from __future__ import annotations

from typing import cast

from hypothesis import given, settings
from hypothesis import strategies as st

from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, InstrumentId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityInterval,
    AvailabilityIntervalContribution,
    AvailabilityScope,
    SupervisedAvailabilityIntervalWriter,
)
from multi_market_arbitrage.pipeline.health import (
    AvailabilityInterruption,
    ObservationRuntimeHealth,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress


class _CapturingAvailabilityWriter:
    def __init__(self) -> None:
        self.bundles: list[
            tuple[AvailabilityInterval, tuple[AvailabilityIntervalContribution, ...]]
        ] = []

    def enqueue_nowait(
        self,
        interval: AvailabilityInterval,
        *,
        contributions: tuple[AvailabilityIntervalContribution, ...] = (),
    ) -> bool:
        self.bundles.append((interval, contributions))
        return True


def _normalize_bundle(
    bundle: tuple[AvailabilityInterval, tuple[AvailabilityIntervalContribution, ...]],
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    interval, contributions = bundle
    interval_record = interval.to_record()
    interval_record.pop("interval_id")
    contribution_records: list[dict[str, object]] = []
    for contribution in contributions:
        record = contribution.to_record()
        record.pop("contribution_id")
        record.pop("interval_id")
        contribution_records.append(record)
    return interval_record, tuple(contribution_records)


def _run_union(
    boundaries: tuple[tuple[int, int], ...],
    order: tuple[int, ...],
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    run_id = CollectorRunId("availability-union-property")
    connection_id = ConnectionId("availability-union-property-connection")
    capture = _CapturingAvailabilityWriter()
    runtime = ObservationRuntimeHealth(
        collector_run_id=run_id,
        availability_writer=cast(SupervisedAvailabilityIntervalWriter, capture),
    )
    runtime.begin_generation(
        connection_id,
        BoundedRawIngress(max_items=1, max_bytes=1, connection_id=connection_id),
    )
    runtime.mark_connected(now_ns=0, now_utc=UtcTimestamp(0))
    interruptions = tuple(
        AvailabilityInterruption(
            cause=AvailabilityCause.UNKNOWN,
            reason_code=f"PROPERTY_CONTRIBUTOR_{index}",
            source_evidence_ids=(f"property-source:{index}",),
            affected_instruments=(InstrumentId(f"property-instrument-{index}"),),
            affected_scopes=(AvailabilityScope.CANONICAL_BOOKS,),
        )
        for index in range(len(boundaries))
    )
    for index in order:
        monotonic_ns, wall_ns = boundaries[index]
        runtime.mark_interrupted(
            now_ns=monotonic_ns,
            now_utc=UtcTimestamp(wall_ns),
            interruption=interruptions[index],
        )
    cutoff_monotonic_ns = max(monotonic_ns for monotonic_ns, _ in boundaries) + 1
    runtime.finalize_open_interval(
        cutoff_at=UtcTimestamp(20_000),
        cutoff_monotonic_ns=cutoff_monotonic_ns,
    )
    assert len(capture.bundles) == 1
    return _normalize_bundle(capture.bundles[0])


@given(
    data=st.data(),
    boundaries=st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=100),
            st.integers(min_value=1, max_value=10_000),
        ),
        min_size=1,
        max_size=8,
    ).map(tuple),
)
@settings(max_examples=100, deadline=None)
def test_availability_union_semantics_are_invariant_under_discovery_permutations(
    data: st.DataObject,
    boundaries: tuple[tuple[int, int], ...],
) -> None:
    baseline_order = tuple(range(len(boundaries)))
    discovered_order = tuple(data.draw(st.permutations(baseline_order)))
    baseline = _run_union(boundaries, baseline_order)
    discovered = _run_union(boundaries, discovered_order)

    assert discovered == baseline
    interval, contributions = discovered
    causal_boundaries = tuple(
        (record["started_monotonic_ns"], record["started_at_utc_ns"]) for record in contributions
    )
    assert causal_boundaries == tuple(sorted(causal_boundaries))
    assert tuple(record["initiating_contributor"] for record in contributions) == (
        True,
        *(False for _ in contributions[1:]),
    )
    opener = contributions[0]
    assert interval["started_monotonic_ns"] == opener["started_monotonic_ns"]
    assert interval["started_at_utc_ns"] == opener["started_at_utc_ns"]
    assert interval["cause_type"] == opener["cause_type"]
    assert interval["reason_code"] == opener["reason_code"]
    assert interval["source_evidence_ids"] == opener["source_evidence_ids"]
    assert interval["duration_ns"] == max(item[0] for item in boundaries) + 1 - min(
        item[0] for item in boundaries
    )
