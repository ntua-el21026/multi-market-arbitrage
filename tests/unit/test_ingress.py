from __future__ import annotations

import asyncio
import time
from types import TracebackType

import pytest

from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.pipeline.ingress import (
    BoundedRawIngress,
    RawIngressBackpressureError,
)
from multi_market_arbitrage.venues.polymarket import (
    PolymarketMarketWebSocket,
    ReceivedWebSocketMessage,
)


class _BurstSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self._messages = [b"first", b"second"]
        self.blocked = asyncio.Event()
        self.received_two = asyncio.Event()

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> bytes:
        if self._messages:
            value = self._messages.pop(0)
            if not self._messages:
                self.received_two.set()
            return value
        await self.blocked.wait()
        raise AssertionError("unreachable")


class _SocketContext:
    def __init__(self, socket: _BurstSocket) -> None:
        self._socket = socket

    async def __aenter__(self) -> _BurstSocket:
        return self._socket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


@pytest.mark.asyncio
async def test_socket_owner_stamps_and_admits_before_downstream_consumes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket = _BurstSocket()

    def fake_connect(uri: str, **kwargs: object) -> _SocketContext:
        del uri, kwargs
        return _SocketContext(socket)

    monkeypatch.setattr(
        "multi_market_arbitrage.venues.polymarket.client.websockets.connect",
        fake_connect,
    )
    ingress = BoundedRawIngress(max_items=4, max_bytes=1024)
    stream = PolymarketMarketWebSocket(["yes", "no"])
    ready = asyncio.Event()
    producer = asyncio.create_task(
        stream.pump_received(ingress.admit_nowait, ready=ready),
    )
    await ready.wait()
    await asyncio.wait_for(socket.received_two.wait(), timeout=1)
    await asyncio.sleep(0)

    # Simulate arbitrarily slow journal/processor work after both socket reads.
    await asyncio.sleep(0.01)
    downstream_started_ns = time.monotonic_ns()
    first = await ingress.get()
    second = await ingress.get()
    producer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await producer

    assert first.message.received_monotonic_ns <= second.message.received_monotonic_ns
    assert second.message.received_monotonic_ns < downstream_started_ns
    assert first.message.payload_bytes == b"first"
    assert second.message.payload_bytes == b"second"
    assert ingress.metrics.received == ingress.metrics.admitted == 2


def test_ingress_overflow_is_explicit_and_does_not_mutate_admitted_prefix() -> None:
    ingress = BoundedRawIngress(max_items=1, max_bytes=5)
    first = ReceivedWebSocketMessage(
        payload_bytes=b"12345",
        received_wall_time=UtcTimestamp(1),
        received_monotonic_ns=1,
        message_type="BINARY",
    )
    second = ReceivedWebSocketMessage(
        payload_bytes=b"x",
        received_wall_time=UtcTimestamp(2),
        received_monotonic_ns=2,
        message_type="TEXT",
    )
    ingress.admit_nowait(first)
    with pytest.raises(RawIngressBackpressureError, match="continuity is unknown"):
        ingress.admit_nowait(second)

    assert ingress.metrics.received == 2
    assert ingress.metrics.admitted == 1
    assert ingress.metrics.overflows == 1
    assert ingress.metrics.queued_bytes == 5
    assert ingress.get_nowait().message == first
    assert ingress.metrics.queue_depth == 0
    assert ingress.metrics.queued_bytes == 0
