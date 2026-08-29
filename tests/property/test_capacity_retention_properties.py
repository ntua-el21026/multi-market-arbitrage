from __future__ import annotations

import sys
from collections import deque

from hypothesis import given, settings
from hypothesis import strategies as st

from multi_market_arbitrage.pipeline.processor import (
    CanonicalProcessor,
    ProcessorRetentionDomain,
)


@given(
    capacity=st.integers(min_value=1, max_value=64),
    first_overflow=st.integers(min_value=1, max_value=128),
    second_overflow=st.integers(min_value=1, max_value=128),
)
@settings(max_examples=30, deadline=None, derandomize=True)
def test_processor_retention_projection_stays_bounded_after_eviction(
    capacity: int,
    first_overflow: int,
    second_overflow: int,
) -> None:
    history: deque[bytes] = deque(maxlen=capacity)
    first_total = capacity + first_overflow
    for index in range(first_total):
        history.append(bytes((index % 251,)) * 8)
    first_tail = tuple(history)
    first = CanonicalProcessor._retention_snapshot(  # pyright: ignore[reportPrivateUsage]
        ProcessorRetentionDomain.SCAN_RESULTS,
        history=history,
        total=first_total,
    )

    for index in range(second_overflow):
        history.append(bytes(((first_total + index) % 251,)) * 8)
    second_total = first_total + second_overflow
    second = CanonicalProcessor._retention_snapshot(  # pyright: ignore[reportPrivateUsage]
        ProcessorRetentionDomain.SCAN_RESULTS,
        history=history,
        total=second_total,
    )

    assert len(first_tail) == len(history) == capacity
    assert tuple(history) != first_tail
    assert first.retained == second.retained == capacity
    assert first.capacity == second.capacity == capacity
    assert first.truncated
    assert second.truncated
    assert first.total == first_total
    assert second.total == second_total
    assert first.representative_object_shallow_bytes == sys.getsizeof(first_tail[-1])
    assert second.representative_object_shallow_bytes == sys.getsizeof(history[-1])
    assert first.estimated_retained_bytes == (
        first.container_shallow_bytes + capacity * first.representative_object_shallow_bytes
    )
    assert second.estimated_retained_bytes == (
        second.container_shallow_bytes + capacity * second.representative_object_shallow_bytes
    )
    assert first.byte_estimate_method == second.byte_estimate_method
