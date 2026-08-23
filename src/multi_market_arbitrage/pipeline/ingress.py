"""Bounded application-owned ingress immediately behind public sockets."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

from multi_market_arbitrage.core.ids import ConnectionId
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.venues.polymarket import ReceivedWebSocketMessage

from .audit import WebSocketAdmissionWatermark


class RawIngressBackpressureError(RuntimeError):
    """The application ingress boundary cannot admit another complete frame."""

    def __init__(
        self,
        detail: str,
        *,
        received_wall_time: UtcTimestamp | None = None,
        received_monotonic_ns: int | None = None,
    ) -> None:
        if (received_wall_time is None) != (received_monotonic_ns is None):
            raise ValueError("backpressure receive timestamps must be paired")
        self.received_wall_time = received_wall_time
        self.received_monotonic_ns = received_monotonic_ns
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class RawIngressItem:
    message: ReceivedWebSocketMessage
    admitted_monotonic_ns: int
    watermark: WebSocketAdmissionWatermark

    @property
    def queue_dwell_nanoseconds(self) -> int:
        return max(0, time.monotonic_ns() - self.admitted_monotonic_ns)


@dataclass(frozen=True, slots=True)
class RawIngressMetricsSnapshot:
    received: int
    admitted: int
    dequeued: int
    overflows: int
    queue_depth: int
    queue_high_water: int
    queued_bytes: int
    queued_bytes_high_water: int
    received_bytes: int = 0
    admitted_bytes: int = 0
    connection_id: ConnectionId | None = None
    last_frame_received_monotonic_ns: int | None = None


class BoundedRawIngress:
    """Single-producer/single-consumer queue bounded by records and bytes."""

    def __init__(
        self,
        *,
        max_items: int,
        max_bytes: int,
        connection_id: ConnectionId | None = None,
    ) -> None:
        if max_items <= 0 or max_bytes <= 0:
            raise ValueError("raw ingress bounds must be positive")
        self._queue: asyncio.Queue[RawIngressItem] = asyncio.Queue(maxsize=max_items)
        self._max_bytes = max_bytes
        self._connection_id = connection_id or ConnectionId.new()
        self._received = 0
        self._admitted = 0
        self._dequeued = 0
        self._overflows = 0
        self._received_bytes = 0
        self._admitted_bytes = 0
        self._last_frame_received_monotonic_ns: int | None = None
        self._queued_bytes = 0
        self._queue_high_water = 0
        self._queued_bytes_high_water = 0

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def connection_id(self) -> ConnectionId:
        return self._connection_id

    def snapshot_watermark(self) -> WebSocketAdmissionWatermark:
        """Return the exact contiguous successful-admission prefix."""

        if self._overflows:
            raise RawIngressBackpressureError(
                "bounded raw ingress continuity is ambiguous after overflow"
            )
        return WebSocketAdmissionWatermark(
            connection_id=self._connection_id,
            ordinal=self._admitted,
        )

    def admit_nowait(self, message: ReceivedWebSocketMessage) -> None:
        """Admit one immutable frame or fail explicitly without partial mutation."""

        self._received += 1
        payload_size = len(message.payload_bytes)
        self._received_bytes += payload_size
        self._last_frame_received_monotonic_ns = message.received_monotonic_ns
        if self._queue.full() or payload_size > self._max_bytes - self._queued_bytes:
            self._overflows += 1
            raise RawIngressBackpressureError(
                "bounded raw ingress queue exhausted; state continuity is unknown",
                received_wall_time=message.received_wall_time,
                received_monotonic_ns=message.received_monotonic_ns,
            )
        item = RawIngressItem(
            message=message,
            admitted_monotonic_ns=time.monotonic_ns(),
            watermark=WebSocketAdmissionWatermark(
                connection_id=self._connection_id,
                ordinal=self._admitted + 1,
            ),
        )
        self._queue.put_nowait(item)
        self._admitted += 1
        self._admitted_bytes += payload_size
        self._queued_bytes += payload_size
        self._queue_high_water = max(self._queue_high_water, self._queue.qsize())
        self._queued_bytes_high_water = max(
            self._queued_bytes_high_water,
            self._queued_bytes,
        )

    async def get(self) -> RawIngressItem:
        item = await self._queue.get()
        self._dequeued += 1
        self._queued_bytes -= len(item.message.payload_bytes)
        return item

    def get_nowait(self) -> RawIngressItem:
        item = self._queue.get_nowait()
        self._dequeued += 1
        self._queued_bytes -= len(item.message.payload_bytes)
        return item

    @property
    def metrics(self) -> RawIngressMetricsSnapshot:
        return RawIngressMetricsSnapshot(
            received=self._received,
            admitted=self._admitted,
            dequeued=self._dequeued,
            overflows=self._overflows,
            queue_depth=self._queue.qsize(),
            queue_high_water=self._queue_high_water,
            queued_bytes=self._queued_bytes,
            queued_bytes_high_water=self._queued_bytes_high_water,
            received_bytes=self._received_bytes,
            admitted_bytes=self._admitted_bytes,
            connection_id=self._connection_id,
            last_frame_received_monotonic_ns=self._last_frame_received_monotonic_ns,
        )


async def drain_websocket_to_ingress(
    messages: AsyncIterator[ReceivedWebSocketMessage],
    ingress: BoundedRawIngress,
) -> None:
    """Dedicated acquisition loop; it never waits for journal or canonical work."""

    async for message in messages:
        ingress.admit_nowait(message)


__all__ = [
    "BoundedRawIngress",
    "RawIngressBackpressureError",
    "RawIngressItem",
    "RawIngressMetricsSnapshot",
    "drain_websocket_to_ingress",
]
