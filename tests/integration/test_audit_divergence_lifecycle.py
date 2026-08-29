from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import shutil
import subprocess
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from types import TracebackType
from typing import ClassVar, cast

import pytest

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, SubscriptionId
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.journal import JournalReader, RawJournalWriter
from multi_market_arbitrage.observability import (
    AvailabilityCause,
    AvailabilityIntervalContributionV2,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    HealthDecisionReason,
    InMemoryMetrics,
    SupervisedAvailabilityIntervalWriter,
    health_decision_reasons_from_mask,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
)
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.ingress import RawIngressItem
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor, ProcessedEnvelope
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.replay import replay_journal
from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketHttpTransportError,
    PolymarketPublicClient,
    PolymarketWebSocketFailureCategory,
    PolymarketWebSocketTransportError,
    PublicReadEvidence,
    PublicReadMetricsSnapshot,
    PublicReadResult,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


def _record_decision_reasons(
    record: dict[str, object],
    *,
    sticky: bool = False,
) -> tuple[HealthDecisionReason, ...]:
    decisions = cast(dict[str, object], record["decisions"])
    key = "gate1_sticky_reason_mask" if sticky else "reason_mask"
    return health_decision_reasons_from_mask(cast(int, decisions[key]))


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _direct_gamma_payload() -> bytes:
    page = cast(dict[str, object], json.loads(_fixture("polymarket_gamma_market_keyset.json")))
    markets = cast(list[object], page["markets"])
    return _canonical_bytes(markets[0])


def _no_book_payload() -> bytes:
    return _canonical_bytes(
        {
            "asks": [{"price": "0.35", "size": "20"}],
            "asset_id": "token-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "hash": "opaque-rest-hash-no",
            "last_trade_price": "0.34",
            "market": "0xcondition",
            "min_order_size": "5",
            "neg_risk": False,
            "tick_size": "0.01",
            "timestamp": "1787198400124",
        }
    )


def _divergent_yes_payload() -> bytes:
    payload = cast(dict[str, object], json.loads(_fixture("polymarket_rest_book_yes.json")))
    bids = cast(list[dict[str, object]], payload["bids"])
    bids[0]["size"] = "11"
    payload["hash"] = "opaque-divergent-rest-hash"
    payload["timestamp"] = "1787198401123"
    return _canonical_bytes(payload)


def _divergent_no_payload() -> bytes:
    payload = cast(dict[str, object], json.loads(_no_book_payload()))
    bids = cast(list[dict[str, object]], payload["bids"])
    bids[0]["size"] = "21"
    payload["hash"] = "opaque-divergent-rest-hash-no"
    payload["timestamp"] = "1787198401124"
    return _canonical_bytes(payload)


def _replacement_ws_book_payload(asset_id: str, *, divergent_no: bool = False) -> bytes:
    rest = cast(
        dict[str, object],
        json.loads(
            _divergent_yes_payload()
            if asset_id == "token-yes"
            else (_divergent_no_payload() if divergent_no else _no_book_payload())
        ),
    )
    return _canonical_bytes(
        {
            "asks": rest["asks"],
            "asset_id": rest["asset_id"],
            "bids": rest["bids"],
            "event_type": "book",
            "hash": f"replacement-ws-{asset_id}",
            "market": rest["market"],
            "timestamp": rest["timestamp"],
        }
    )


def _sibling_bbo_payload() -> bytes:
    return _canonical_bytes(
        {
            "asset_id": "token-no",
            "best_ask": "0.35",
            "best_bid": "0.31",
            "event_type": "best_bid_ask",
            "market": "0xcondition",
            "spread": "0.04",
            "timestamp": "1787198402002",
        }
    )


def _public_read(
    payload: bytes,
    on_response_body_complete: Callable[[UtcTimestamp, int], None] | None,
) -> PublicReadResult:
    response_ns = time.monotonic_ns()
    response_at = UtcTimestamp.now()
    if on_response_body_complete is not None:
        on_response_body_complete(response_at, response_ns)
    return PublicReadResult(
        payload_bytes=payload,
        evidence=PublicReadEvidence(
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            attempts=1,
            started_monotonic_ns=response_ns - 1,
            response_received_wall_time=response_at,
            response_received_monotonic_ns=response_ns,
            completed_monotonic_ns=response_ns,
            status_code=200,
            cumulative_backoff_seconds=0.0,
            cumulative_rate_limit_wait_seconds=0.0,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        ),
    )


def _git(repository: Path, *arguments: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("test requires Git")
    completed = subprocess.run(  # noqa: S603 - fixed executable and test-owned repository
        [executable, *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _initialize_clean_source(repository: Path) -> str:
    _git(repository, "init", "--quiet")
    source_marker = repository / ".source-fixture"
    source_marker.write_text("clean source\n", encoding="utf-8")
    package_marker = repository / "src" / "multi_market_arbitrage" / "__init__.py"
    package_marker.parent.mkdir(parents=True)
    package_marker.write_text("# test-owned imported package root\n", encoding="utf-8")
    _git(
        repository,
        "add",
        source_marker.name,
        str(package_marker.relative_to(repository)),
    )
    _git(
        repository,
        "-c",
        "user.name=Audit Lifecycle Test",
        "-c",
        "user.email=audit-lifecycle@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )
    return _git(repository, "rev-parse", "HEAD")


@pytest.fixture(autouse=True)
def bind_imported_source_to_test_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    host_module = importlib.import_module("multi_market_arbitrage.observability.host")
    imported_package_root = tmp_path / "src" / "multi_market_arbitrage"
    monkeypatch.setattr(
        host_module,
        "_imported_package_root",
        lambda: str(imported_package_root),
    )


def _target() -> ResolvedPolymarketTarget:
    return ResolvedPolymarketTarget(
        mode=ObservationTargetMode.EXPLICIT_GATE1,
        requested_gamma_market_id="101",
        resolved_gamma_market_id="101",
        condition_id="0xcondition",
        event_id="501",
        yes_token_id="token-yes",  # noqa: S106 - public venue identifier
        no_token_id="token-no",  # noqa: S106 - public venue identifier
        reference_version_ids=("reference",),
        semantics_version_id="semantics",
        selection_rationale="explicit audit-lifecycle integration fixture",
    )


async def _bootstrap_fixture(
    service: PolymarketObservationService,
    *,
    client: object,
    capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
    processor: object,
    gamma_market_id: str,
    deadline: float | None = None,
) -> tuple[tuple[object, ...], tuple[str, ...], ResolvedPolymarketTarget]:
    del service, client, processor, gamma_market_id, deadline
    market_payload = _fixture("polymarket_gamma_market_keyset.json")
    market = parse_gamma_markets(market_payload).markets[0]
    for channel, payload in (
        (GAMMA_MARKETS_KEYSET, market_payload),
        (f"{CLOB_MARKET_PREFIX}0xcondition", _fixture("polymarket_clob_market_info.json")),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book_payload()),
    ):
        result = await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        )
        assert result.success
    return (market,), market.clob_token_ids, _target()


class _LifecycleSocket:
    instances: ClassVar[list[_LifecycleSocket]] = []
    first_replacement_processed: ClassVar[asyncio.Event | None] = None
    replacement_sibling_delay_seconds: ClassVar[float] = 0.0
    end_replacement_before_canonical_return: ClassVar[bool] = False
    allow_replacement_end: ClassVar[asyncio.Event | None] = None
    replacement_ended: ClassVar[asyncio.Event | None] = None
    replacement_ended_monotonic_ns: ClassVar[int | None] = None
    inject_sibling_bbo_overlap: ClassVar[bool] = False
    final_snapshot_preservation_started: ClassVar[asyncio.Event | None] = None
    sibling_bbo_admitted: ClassVar[asyncio.Event | None] = None
    sibling_transition_processed: ClassVar[asyncio.Event | None] = None
    first_generation_cancel_delay_seconds: ClassVar[float] = 0.0

    def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
        assert asset_ids == ("token-yes", "token-no")
        del kwargs
        self.generation = len(type(self).instances) + 1
        self.stopped = False
        self.subscription_semantics_sha256 = hashlib.sha256(
            f"audit-divergence-generation-{self.generation}".encode()
        ).hexdigest()
        type(self).instances.append(self)

    async def pump_received(
        self,
        admit: Callable[[ReceivedWebSocketMessage], None],
        *,
        ready: asyncio.Event,
    ) -> None:
        ready.set()
        try:
            if self.generation == 2:
                first_processed = type(self).first_replacement_processed
                if first_processed is None:
                    raise RuntimeError("replacement-generation test fence is unavailable")
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=_replacement_ws_book_payload("token-yes"),
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                await first_processed.wait()
                await asyncio.sleep(type(self).replacement_sibling_delay_seconds)
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=_replacement_ws_book_payload(
                            "token-no",
                            divergent_no=_LifecyclePublicClient.diverge_both,
                        ),
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                if type(self).inject_sibling_bbo_overlap:
                    final_snapshot_preservation_started = type(
                        self
                    ).final_snapshot_preservation_started
                    sibling_bbo_admitted = type(self).sibling_bbo_admitted
                    sibling_transition_processed = type(self).sibling_transition_processed
                    if (
                        final_snapshot_preservation_started is None
                        or sibling_bbo_admitted is None
                        or sibling_transition_processed is None
                    ):
                        raise RuntimeError("sibling transition test fences are unavailable")
                    await final_snapshot_preservation_started.wait()
                    admit(
                        ReceivedWebSocketMessage(
                            payload_bytes=_sibling_bbo_payload(),
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=time.monotonic_ns(),
                            message_type="TEXT",
                        )
                    )
                    sibling_bbo_admitted.set()
                    await sibling_transition_processed.wait()
                    admit(
                        ReceivedWebSocketMessage(
                            payload_bytes=_replacement_ws_book_payload(
                                "token-no",
                                divergent_no=_LifecyclePublicClient.diverge_both,
                            ),
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=time.monotonic_ns(),
                            message_type="TEXT",
                        )
                    )
                if type(self).end_replacement_before_canonical_return:
                    allow_replacement_end = type(self).allow_replacement_end
                    if allow_replacement_end is None:
                        raise RuntimeError("replacement-generation end fence is unavailable")
                    await allow_replacement_end.wait()
                    return
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if self.generation == 1:
                await asyncio.sleep(type(self).first_generation_cancel_delay_seconds)
            raise
        finally:
            self.stopped = True
            if self.generation == 2:
                type(self).replacement_ended_monotonic_ns = time.monotonic_ns()
                replacement_ended = type(self).replacement_ended
                if replacement_ended is not None:
                    replacement_ended.set()


class _LifecyclePublicClient:
    fail_resnapshot: ClassVar[bool] = False
    diverge_both: ClassVar[bool] = False
    latest: ClassVar[_LifecyclePublicClient | None] = None

    def __init__(self, **kwargs: object) -> None:
        del kwargs
        self.audit_calls: list[str] = []
        self.resnapshot_book_calls: list[str] = []
        self.first_generation_stopped_before_resnapshot = False
        type(self).latest = self

    async def __aenter__(self) -> _LifecyclePublicClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    def public_read_metrics_snapshot(self) -> PublicReadMetricsSnapshot:
        return PublicReadMetricsSnapshot((), 0, 0)

    async def fetch_order_book_raw_with_evidence(
        self,
        asset_id: str,
        *,
        deadline: float | None = None,
        on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
    ) -> PublicReadResult:
        del deadline
        self.audit_calls.append(asset_id)
        payload = (
            _divergent_yes_payload()
            if asset_id == "token-yes"
            else (_divergent_no_payload() if type(self).diverge_both else _no_book_payload())
        )
        return _public_read(payload, on_response_body_complete)

    async def fetch_gamma_market_raw(
        self,
        gamma_market_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        assert gamma_market_id == "101"
        return _direct_gamma_payload()

    async def fetch_gamma_event_raw(
        self,
        event_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        assert event_id == "501"
        return _canonical_bytes({"enableNegRisk": False, "id": 501, "negRiskAugmented": False})

    async def fetch_clob_market_raw(
        self,
        condition_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        assert condition_id == "0xcondition"
        return _fixture("polymarket_clob_market_info.json")

    async def fetch_order_book_raw(
        self,
        asset_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        self.first_generation_stopped_before_resnapshot = bool(
            _LifecycleSocket.instances and _LifecycleSocket.instances[0].stopped
        )
        self.resnapshot_book_calls.append(asset_id)
        if type(self).fail_resnapshot:
            raise PolymarketHttpTransportError(
                endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                category=PolymarketHttpFailureCategory.NETWORK,
                retryable=False,
                attempt=1,
                detail="injected authoritative resnapshot failure",
            )
        return (
            _divergent_yes_payload()
            if asset_id == "token-yes"
            else (_divergent_no_payload() if type(self).diverge_both else _no_book_payload())
        )


def _configured_service(
    tmp_path: Path,
    *,
    maximum_book_age_seconds: float | None = None,
    max_reconnect_attempts: int | None = None,
    audit_replacement_retry_delay_seconds: float | None = None,
    audit_replacement_hard_deadline_seconds: float | None = None,
) -> PolymarketObservationService:
    source_commit = _initialize_clean_source(tmp_path)
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            # Twenty hertz preserves several checkpoints in the shortest
            # causal path without overwhelming the bounded evidence writer on
            # NTFS-backed WSL temporary directories during longer regressions.
            "health": config.health.model_copy(update={"checkpoint_interval_seconds": 0.05}),
            "polymarket": config.polymarket.model_copy(
                update={
                    # Keep the audit launch guard large enough to dominate the
                    # fixture's real journal/staging latency.  A 10 ms guard
                    # made cutoff behavior depend on host load and could stage
                    # a synthetic RUN_ENDED pair immediately before shutdown.
                    "snapshot_audit_freshness_guard_seconds": 0.15,
                    "snapshot_audit_interval_seconds": 0.12,
                    "snapshot_audit_race_retry_quiet_seconds": 0.005,
                    "max_reconnect_attempts": (
                        config.polymarket.max_reconnect_attempts
                        if max_reconnect_attempts is None
                        else max_reconnect_attempts
                    ),
                    "audit_replacement_retry_delay_seconds": (
                        config.polymarket.audit_replacement_retry_delay_seconds
                        if audit_replacement_retry_delay_seconds is None
                        else audit_replacement_retry_delay_seconds
                    ),
                    "audit_replacement_hard_deadline_seconds": (
                        config.polymarket.audit_replacement_hard_deadline_seconds
                        if audit_replacement_hard_deadline_seconds is None
                        else audit_replacement_hard_deadline_seconds
                    ),
                }
            ),
            "scanner": (
                config.scanner
                if maximum_book_age_seconds is None
                else config.scanner.model_copy(
                    update={
                        "maximum_book_age_seconds": maximum_book_age_seconds,
                        "maximum_interleg_skew_seconds": min(
                            0.01,
                            maximum_book_age_seconds,
                        ),
                    }
                )
            ),
        }
    )
    return PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit=source_commit,
    )


def _prepare_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_resnapshot: bool,
    replacement_sibling_delay_seconds: float = 0.0,
) -> None:
    _LifecycleSocket.instances = []
    _LifecycleSocket.first_replacement_processed = asyncio.Event()
    _LifecycleSocket.replacement_sibling_delay_seconds = replacement_sibling_delay_seconds
    _LifecycleSocket.end_replacement_before_canonical_return = False
    _LifecycleSocket.allow_replacement_end = asyncio.Event()
    _LifecycleSocket.replacement_ended = asyncio.Event()
    _LifecycleSocket.replacement_ended_monotonic_ns = None
    _LifecycleSocket.inject_sibling_bbo_overlap = False
    _LifecycleSocket.final_snapshot_preservation_started = asyncio.Event()
    _LifecycleSocket.sibling_bbo_admitted = asyncio.Event()
    _LifecycleSocket.sibling_transition_processed = asyncio.Event()
    _LifecycleSocket.first_generation_cancel_delay_seconds = 0.0
    _LifecyclePublicClient.latest = None
    _LifecyclePublicClient.fail_resnapshot = fail_resnapshot
    _LifecyclePublicClient.diverge_both = False
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _LifecyclePublicClient)
    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _LifecycleSocket)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_bootstrap_explicit",
        _bootstrap_fixture,
    )


@pytest.mark.asyncio
async def test_resnapshot_wait_exhausts_replacement_hard_deadline_before_run_cutoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(
        tmp_path,
        audit_replacement_retry_delay_seconds=0.05,
        audit_replacement_hard_deadline_seconds=0.4,
    )
    _prepare_runtime(monkeypatch, fail_resnapshot=False)

    class _DeadlineBoundResnapshotClient(_LifecyclePublicClient):
        observed_deadline: ClassVar[float | None] = None
        raised_monotonic_ns: ClassVar[int | None] = None

        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            if asset_id == "token-yes":
                return await super().fetch_order_book_raw(asset_id, deadline=deadline)
            assert asset_id == "token-no"
            assert deadline is not None
            self.first_generation_stopped_before_resnapshot = bool(
                _LifecycleSocket.instances and _LifecycleSocket.instances[0].stopped
            )
            self.resnapshot_book_calls.append(asset_id)
            type(self).observed_deadline = deadline
            await asyncio.sleep(max(0.0, deadline - asyncio.get_running_loop().time()))
            type(self).raised_monotonic_ns = time.monotonic_ns()
            raise PolymarketHttpTransportError(
                endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                category=PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
                retryable=False,
                attempt=1,
                detail="injected resnapshot read reached its supervised deadline",
            )

    monkeypatch.setattr(
        observe_module,
        "PolymarketPublicClient",
        _DeadlineBoundResnapshotClient,
    )

    summary = await service.run(duration_seconds=2.0, gamma_market_id="101")

    assert _DeadlineBoundResnapshotClient.observed_deadline is not None
    assert _DeadlineBoundResnapshotClient.raised_monotonic_ns is not None
    assert len(_LifecycleSocket.instances) == 1
    assert _LifecycleSocket.instances[0].stopped
    client = _DeadlineBoundResnapshotClient.latest
    assert isinstance(client, _DeadlineBoundResnapshotClient)
    assert client.resnapshot_book_calls == ["token-yes", "token-no"]
    assert client.first_generation_stopped_before_resnapshot

    processor = summary.processor
    assert processor.audit_replacement_captures_pending == 1
    capture_authority = processor.audit_replacement_capture_authority[0]
    recovery = capture_authority.recovery_authority
    assert recovery is not None
    assert recovery.status.value == "HARD_DEADLINE_EXHAUSTED"
    assert recovery.last_result.value == "HARD_DEADLINE_EXHAUSTED"
    assert recovery.last_failure is not None
    assert recovery.last_failure.value == "HARD_DEADLINE_REACHED"
    assert recovery.attempt_count == 0
    assert recovery.unresolved
    assert recovery.hard_deadline.nanoseconds < recovery.run_deadline.nanoseconds
    observed_deadline = _DeadlineBoundResnapshotClient.observed_deadline
    raised_monotonic_ns = _DeadlineBoundResnapshotClient.raised_monotonic_ns
    assert observed_deadline is not None
    assert raised_monotonic_ns is not None
    assert abs(int(observed_deadline * 1e9) - recovery.hard_deadline.nanoseconds) <= 2_000_000
    assert (
        recovery.hard_deadline.nanoseconds
        <= raised_monotonic_ns
        < recovery.run_deadline.nanoseconds
    )

    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    lifecycle_records = tuple(
        (record, cast(dict[str, object], json.loads(record.payload_bytes)))
        for record in raw_records
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY
    )
    assert tuple(payload["action"] for _record, payload in lifecycle_records) == ("DEADLINE",)
    deadline_record, deadline_payload = lifecycle_records[0]
    assert deadline_payload["decision_monotonic_ns"] == (
        deadline_record.received_monotonic.nanoseconds
    )
    assert (
        recovery.hard_deadline.nanoseconds
        <= deadline_record.received_monotonic.nanoseconds
        < recovery.run_deadline.nanoseconds
    )
    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == processor
    assert replay.processor.terminal_hash == processor.terminal_hash


@pytest.mark.asyncio
async def test_pre_ready_wait_exhausts_replacement_hard_deadline_without_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(
        tmp_path,
        audit_replacement_retry_delay_seconds=0.05,
        audit_replacement_hard_deadline_seconds=0.4,
    )
    _prepare_runtime(monkeypatch, fail_resnapshot=False)

    class _NeverReadyReplacementSocket(_LifecycleSocket):
        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            if self.generation == 1:
                await super().pump_received(admit, ready=ready)
                return
            del admit, ready
            try:
                await asyncio.Event().wait()
            finally:
                self.stopped = True

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _NeverReadyReplacementSocket,
    )

    summary = await service.run(duration_seconds=1.5, gamma_market_id="101")

    assert len(_NeverReadyReplacementSocket.instances) == 2
    assert all(socket.stopped for socket in _NeverReadyReplacementSocket.instances)
    capture_authority = summary.processor.audit_replacement_capture_authority[0]
    recovery = capture_authority.recovery_authority
    assert recovery is not None
    assert recovery.status.value == "HARD_DEADLINE_EXHAUSTED"
    assert recovery.last_result.value == "HARD_DEADLINE_EXHAUSTED"
    assert recovery.last_failure is not None
    assert recovery.last_failure.value == "HARD_DEADLINE_REACHED"
    assert recovery.attempt_count == 0
    assert recovery.attempted_connection_ids == ()
    assert recovery.unresolved
    assert recovery.hard_deadline.nanoseconds < recovery.run_deadline.nanoseconds

    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    lifecycle_records = tuple(
        (record, cast(dict[str, object], json.loads(record.payload_bytes)))
        for record in raw_records
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY
    )
    assert tuple(payload["action"] for _record, payload in lifecycle_records) == ("DEADLINE",)
    deadline_record, deadline_payload = lifecycle_records[0]
    assert deadline_payload["decision_monotonic_ns"] == (
        deadline_record.received_monotonic.nanoseconds
    )
    assert (
        recovery.hard_deadline.nanoseconds
        <= deadline_record.received_monotonic.nanoseconds
        < recovery.run_deadline.nanoseconds
    )
    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == summary.processor
    assert replay.processor.terminal_hash == summary.processor.terminal_hash


@pytest.mark.asyncio
async def test_off_socket_cancellation_durably_terminalizes_replacement_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(
        tmp_path,
        audit_replacement_retry_delay_seconds=0.05,
        audit_replacement_hard_deadline_seconds=0.4,
    )
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    resnapshot_waiting = asyncio.Event()

    class _CancellableResnapshotClient(_LifecyclePublicClient):
        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            if asset_id == "token-yes":
                return await super().fetch_order_book_raw(asset_id, deadline=deadline)
            assert asset_id == "token-no"
            self.resnapshot_book_calls.append(asset_id)
            resnapshot_waiting.set()
            await asyncio.Event().wait()
            raise AssertionError("cancelled resnapshot unexpectedly resumed")

    monkeypatch.setattr(
        observe_module,
        "PolymarketPublicClient",
        _CancellableResnapshotClient,
    )
    retained_captures: list[observe_module._RawCaptureBoundary] = []  # pyright: ignore[reportPrivateUsage]
    original_capture_init = observe_module._RawCaptureBoundary.__init__  # pyright: ignore[reportPrivateUsage]

    def retain_capture(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        *,
        collector_run_id: CollectorRunId,
        writer: RawJournalWriter,
        processor: CanonicalProcessor,
        metrics: InMemoryMetrics,
        runtime_health: ObservationRuntimeHealth | None = None,
    ) -> None:
        original_capture_init(
            capture,
            collector_run_id=collector_run_id,
            writer=writer,
            processor=processor,
            metrics=metrics,
            runtime_health=runtime_health,
        )
        retained_captures.append(capture)

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "__init__",
        retain_capture,
    )

    observation = asyncio.create_task(
        service.run(duration_seconds=2.0, gamma_market_id="101"),
        name="off-socket-replacement-cancellation",
    )
    await asyncio.wait_for(resnapshot_waiting.wait(), timeout=5.0)
    observation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(observation, timeout=10.0)

    assert len(retained_captures) == 1
    capture = retained_captures[0]
    recovery = capture.processor.audit_replacement_recovery_authorities[0]
    assert recovery.status.value == "TERMINAL_UNRESOLVED"
    assert recovery.last_result.value == "TERMINAL_UNRESOLVED"
    assert recovery.last_failure is not None
    assert recovery.last_failure.value == "TERMINAL_CUTOFF"
    assert recovery.attempt_count == 0
    raw_records = JournalReader(
        capture.writer.config.root,
        capture.collector_run_id,
    ).read_all(include_partial=True)
    lifecycle_payloads = tuple(
        cast(dict[str, object], json.loads(record.payload_bytes))
        for record in raw_records
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY
    )
    assert tuple(payload["action"] for payload in lifecycle_payloads) == ("TERMINAL_CUTOFF",)


@pytest.mark.asyncio
async def test_ready_boundary_preceding_recovery_timer_starts_attempt_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(
        tmp_path,
        audit_replacement_retry_delay_seconds=0.05,
        audit_replacement_hard_deadline_seconds=0.4,
    )
    _prepare_runtime(monkeypatch, fail_resnapshot=False)

    class _ReadyBeforeDelayedTimerSocket(_LifecycleSocket):
        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            if self.generation == 1:
                await super().pump_received(admit, ready=ready)
                return
            ready.set()
            for asset_id in ("token-yes", "token-no"):
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=_replacement_ws_book_payload(asset_id),
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
            try:
                # Both full snapshots were synchronously admitted before the
                # hard boundary, but the timer owns release once the delayed
                # coordinator drains them after that boundary.
                time.sleep(0.5)  # noqa: ASYNC251 - force coordinator wakeup after the timer
                await asyncio.Event().wait()
            finally:
                self.stopped = True

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _ReadyBeforeDelayedTimerSocket,
    )

    # Keep the run cutoff independent of the deliberate 500 ms event-loop
    # stall below.  The assertion under test is readiness-versus-recovery
    # deadline ordering, not terminal run-cutoff behavior on a loaded host.
    summary = await service.run(duration_seconds=2.5, gamma_market_id="101")

    capture_authority = summary.processor.audit_replacement_capture_authority[0]
    recovery = capture_authority.recovery_authority
    assert recovery is not None
    assert recovery.status.value == "HARD_DEADLINE_EXHAUSTED"
    assert recovery.last_failure is not None
    assert recovery.last_failure.value == "HARD_DEADLINE_REACHED"
    assert recovery.attempt_count == 1
    assert len(recovery.attempted_connection_ids) == 1
    assert len(recovery.captured_snapshot_sources) == 2
    assert recovery.unresolved
    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    lifecycle_records = tuple(
        (record, cast(dict[str, object], json.loads(record.payload_bytes)))
        for record in raw_records
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY
    )
    assert tuple(payload["action"] for _record, payload in lifecycle_records) == (
        "ATTEMPT_START",
        "DEADLINE",
    )
    assert all(
        record.channel
        not in {
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
        }
        for record in raw_records
    )
    attempt_record, attempt_payload = lifecycle_records[0]
    deadline_record, _deadline_payload = lifecycle_records[1]
    assert attempt_payload["decision_monotonic_ns"] == (
        attempt_record.received_monotonic.nanoseconds
    )
    assert (
        attempt_record.received_monotonic.nanoseconds
        < recovery.hard_deadline.nanoseconds
        <= deadline_record.received_monotonic.nanoseconds
        < recovery.run_deadline.nanoseconds
    )


@pytest.mark.asyncio
async def test_resnapshot_result_returned_after_effective_deadline_is_not_preserved() -> None:
    class _ImmediateResultClient:
        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            assert asset_id == "token-yes"
            assert deadline is not None
            return _divergent_yes_payload()

    class _RejectingCapture:
        async def preserve_then_process(self, **kwargs: object) -> ProcessedEnvelope:
            del kwargs
            raise AssertionError("late resnapshot payload reached the preservation boundary")

    deadline = asyncio.get_running_loop().time() + 1.0
    deadline_monotonic_ns = time.monotonic_ns() - 1
    with pytest.raises(PolymarketHttpTransportError) as captured:
        await PolymarketObservationService._fetch_book(  # pyright: ignore[reportPrivateUsage]
            cast(PolymarketPublicClient, _ImmediateResultClient()),
            cast(observe_module._RawCaptureBoundary, _RejectingCapture()),  # pyright: ignore[reportPrivateUsage]
            "token-yes",
            audit=False,
            deadline=deadline,
            deadline_monotonic_ns=deadline_monotonic_ns,
        )

    assert captured.value.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED


@pytest.mark.asyncio
async def test_reference_result_after_exact_deadline_is_not_preserved(tmp_path: Path) -> None:
    class _ImmediateReferenceClient:
        async def fetch_gamma_market_raw(
            self,
            gamma_market_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            assert gamma_market_id == "101"
            assert deadline is not None
            return _direct_gamma_payload()

    class _RejectingCapture:
        async def preserve_then_process(self, **kwargs: object) -> ProcessedEnvelope:
            del kwargs
            raise AssertionError("late reference payload reached the preservation boundary")

    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )
    with pytest.raises(PolymarketHttpTransportError) as captured:
        await service._refresh_reference_by_id(  # pyright: ignore[reportPrivateUsage]
            cast(PolymarketPublicClient, _ImmediateReferenceClient()),
            cast(observe_module._RawCaptureBoundary, _RejectingCapture()),  # pyright: ignore[reportPrivateUsage]
            gamma_market_id="101",
            deadline=asyncio.get_running_loop().time() + 1.0,
            deadline_monotonic_ns=time.monotonic_ns() - 1,
        )

    assert captured.value.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED


@pytest.mark.asyncio
async def test_exact_recovery_deadline_precedes_run_deadline() -> None:
    async def _unused_process_due(
        moment: observe_module._CaptureCutoffMoment,  # pyright: ignore[reportPrivateUsage]
    ) -> None:
        del moment

    supervisor = observe_module._ReplacementDeadlineSupervisor(  # pyright: ignore[reportPrivateUsage]
        next_exhaustion_deadline_ns=lambda: 100,
        process_due_exhaustions=_unused_process_due,
    )

    assert (
        PolymarketObservationService._off_socket_operation_deadline_ns(  # pyright: ignore[reportPrivateUsage]
            200,
            supervisor,
        )
        == 100
    )


@pytest.mark.asyncio
async def test_late_authoritative_resnapshot_remains_terminally_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)

    class _RunDeadlineCrossingResnapshotClient(_LifecyclePublicClient):
        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            assert asset_id == "token-yes"
            assert deadline is not None
            self.first_generation_stopped_before_resnapshot = bool(
                _LifecycleSocket.instances and _LifecycleSocket.instances[0].stopped
            )
            self.resnapshot_book_calls.append(asset_id)
            await asyncio.sleep(max(0.0, deadline - asyncio.get_running_loop().time()) + 0.01)
            return _divergent_yes_payload()

    monkeypatch.setattr(
        observe_module,
        "PolymarketPublicClient",
        _RunDeadlineCrossingResnapshotClient,
    )

    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    client = _RunDeadlineCrossingResnapshotClient.latest
    assert isinstance(client, _RunDeadlineCrossingResnapshotClient)
    assert client.resnapshot_book_calls == ["token-yes"]
    assert client.first_generation_stopped_before_resnapshot
    processor = summary.processor
    assert processor.audit_divergences_observed == 1
    assert processor.audit_divergences_resolved_by_authoritative_resnapshot == 0
    assert processor.audit_divergences_pending == 1
    assert processor.audit_divergences_unresolved == 1
    assert processor.audit_replacement_captures_pending == 0
    assert not processor.audit_divergence_resolution_records
    assert summary.status is ComponentHealth.DEGRADED
    assert not summary.health_decision.research_eligible
    assert not summary.health_decision.gate1_eligible_so_far
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.gate1_sticky_reason_codes
    )
    assert summary.availability_evidence.open_at_shutdown_intervals == 1
    interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    assert interval["cause_type"] == AvailabilityCause.AUDIT_MISMATCH_RECOVERY
    assert interval["recovery_outcome"] == (AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN)
    assert interval["research_eligibility_restored"] is False
    assert interval["open_at_shutdown"] is True

    final_checkpoint = json.loads(
        summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1]
    )
    assert final_checkpoint["audit"]["divergences"]["pending"] == 1
    assert final_checkpoint["audit"]["divergences"]["unresolved"] == 1
    assert final_checkpoint["decisions"]["research_eligible"] is False
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in _record_decision_reasons(
        final_checkpoint,
        sticky=True,
    )
    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == processor
    assert replay.processor.terminal_hash == processor.terminal_hash


@pytest.mark.asyncio
async def test_paired_response_is_classified_before_slow_mismatch_socket_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mismatch recovery cannot delay an already-received sibling response."""

    maximum_book_age_seconds = 2.0
    service = _configured_service(
        tmp_path,
        maximum_book_age_seconds=maximum_book_age_seconds,
    )
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    # Preserve the discriminating condition: awaiting generation cleanup would
    # carry the already-received sibling response past its hard expiry.
    _LifecycleSocket.first_generation_cancel_delay_seconds = maximum_book_age_seconds + 0.1

    summary = await service.run(duration_seconds=5.0, gamma_market_id="101")

    first_generation_outcomes = tuple(
        outcome
        for outcome in summary.processor.audit_outcomes
        if outcome.connection_id.value == summary.processor.audit_outcomes[0].connection_id.value
    )
    assert len(first_generation_outcomes) == 2
    assert tuple(outcome.outcome.value for outcome in first_generation_outcomes) == (
        "MISMATCH",
        "MATCH",
    )
    assert all(outcome.reason is None for outcome in first_generation_outcomes)
    records = JournalReader(
        summary.journal_root,
        summary.collector_run_id,
    ).read_all()
    first_request_ids = {outcome.request_id for outcome in first_generation_outcomes}
    classifications = tuple(
        cast(dict[str, object], json.loads(record.payload_bytes))
        for record in records
        if record.channel == "mma.control.rest-audit-classify.v1"
        and cast(dict[str, object], json.loads(record.payload_bytes))["request_id"]
        in first_request_ids
    )
    assert len(classifications) == 2
    assert all(
        cast(int, item["classification_monotonic_ns"]) < cast(int, item["hard_expiry_monotonic_ns"])
        for item in classifications
    )
    assert summary.runtime_health.availability_contributions_emitted >= 1


@pytest.mark.asyncio
async def test_mismatch_waits_for_inflight_sibling_before_generation_rotation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mismatch-driven rotation cannot manufacture a permanent sibling fault."""

    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    allow_sibling_response = asyncio.Event()
    sibling_observed_live_generation: list[bool] = []
    resnapshot_started_after_sibling_response: list[bool] = []

    class _DelayedSiblingClient(_LifecyclePublicClient):
        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            if asset_id == "token-no" and not allow_sibling_response.is_set():
                await allow_sibling_response.wait()
                sibling_observed_live_generation.append(
                    bool(_LifecycleSocket.instances) and not _LifecycleSocket.instances[0].stopped
                )
            return await super().fetch_order_book_raw_with_evidence(
                asset_id,
                deadline=deadline,
                on_response_body_complete=on_response_body_complete,
            )

        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            resnapshot_started_after_sibling_response.append(allow_sibling_response.is_set())
            return await super().fetch_order_book_raw(asset_id, deadline=deadline)

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _DelayedSiblingClient)
    original_preserve = (
        observe_module._RawCaptureBoundary.preserve_then_process_with_evidence  # pyright: ignore[reportPrivateUsage]
    )

    async def release_sibling_after_mismatch(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
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
    ) -> object:
        evidence = await original_preserve(
            capture,
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=received_wall_time,
            received_monotonic_ns=received_monotonic_ns,
        )
        result = evidence.result
        if result.audit_outcomes and result.audit_outcomes[-1].outcome.value == "MISMATCH":
            allow_sibling_response.set()
        return evidence

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_then_process_with_evidence",
        release_sibling_after_mismatch,
    )
    first_replacement_processed = _LifecycleSocket.first_replacement_processed
    assert first_replacement_processed is not None
    first_replacement_processed.set()

    async with asyncio.timeout(30.0):
        summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    first_connection = summary.processor.audit_outcomes[0].connection_id
    first_generation_outcomes = tuple(
        outcome
        for outcome in summary.processor.audit_outcomes
        if outcome.connection_id == first_connection
    )
    assert tuple(outcome.outcome.value for outcome in first_generation_outcomes) == (
        "MISMATCH",
        "MATCH",
    )
    assert all(outcome.reason is None for outcome in first_generation_outcomes)
    assert sibling_observed_live_generation == [True]
    assert resnapshot_started_after_sibling_response == [True, True]
    assert summary.processor.audit_faults_observed == 0
    assert summary.processor.audit_faults_current == 0
    assert summary.processor.audit_faults_permanent == 0
    assert summary.processor.audit_replacement_captures_pending == 0
    assert summary.processor.audit_divergences_pending == 0
    assert summary.processor.audit_divergences_unresolved == 0
    assert summary.processor.audit_divergence_lifecycle_reconciled
    assert summary.health_decision.gate1_eligible_so_far

    records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    first_request_ids = {outcome.request_id for outcome in first_generation_outcomes}
    classification_indices = tuple(
        index
        for index, record in enumerate(records)
        if record.channel == CONTROL_REST_AUDIT_CLASSIFY
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("request_id")
        in first_request_ids
    )
    assert len(classification_indices) == 2
    first_resnapshot_index = next(
        index
        for index, record in enumerate(records)
        if index > max(classification_indices) and record.channel == REST_BOOK
    )
    assert max(classification_indices) < first_resnapshot_index
    assert not any(
        record.channel == CONTROL_REST_AUDIT_START
        for record in records[min(classification_indices) + 1 : first_resnapshot_index]
    )
    assert not any(
        record.channel == CONTROL_REST_AUDIT_FAILED
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("request_id")
        in first_request_ids
        for record in records
    )
    assert (
        sum(record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2 for record in records) == 1
    )
    assert not any(record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE for record in records)
    contribution_path = summary.availability_evidence.contribution_path
    assert contribution_path is not None
    contributions = tuple(
        cast(dict[str, object], json.loads(line))
        for line in contribution_path.read_text(encoding="utf-8").splitlines()
    )
    released_recovery = next(
        cast(dict[str, object], contribution["replacement_recovery"])
        for contribution in contributions
        if cast(dict[str, object], contribution["replacement_recovery"])["status"] == "RELEASED"
    )
    assert released_recovery["last_result"] == "RELEASED"
    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == summary.processor
    assert replay.processor.terminal_hash == summary.processor.terminal_hash


@pytest.mark.asyncio
async def test_socket_end_during_mismatch_sibling_drain_preserves_disconnect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real generation end still owns disconnect recovery during the drain."""

    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    first_no_started = asyncio.Event()
    first_no_cancelled = asyncio.Event()
    end_first_generation = asyncio.Event()

    class _DisconnectingLifecycleSocket(_LifecycleSocket):
        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            if self.generation != 1:
                await super().pump_received(admit, ready=ready)
                return
            del admit
            ready.set()
            try:
                await end_first_generation.wait()
                raise PolymarketWebSocketTransportError(
                    category=PolymarketWebSocketFailureCategory.CONNECTION_CLOSED,
                    retryable=True,
                    detail="fixture socket ended while mismatch sibling remained in flight",
                )
            finally:
                self.stopped = True

    class _BlockedSiblingClient(_LifecyclePublicClient):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self._block_first_no = True

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            if asset_id == "token-yes" and self._block_first_no:
                await first_no_started.wait()
            elif asset_id == "token-no" and self._block_first_no:
                self._block_first_no = False
                first_no_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    first_no_cancelled.set()
            return await super().fetch_order_book_raw_with_evidence(
                asset_id,
                deadline=deadline,
                on_response_body_complete=on_response_body_complete,
            )

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _DisconnectingLifecycleSocket,
    )
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BlockedSiblingClient)
    original_preserve = (
        observe_module._RawCaptureBoundary.preserve_then_process_with_evidence  # pyright: ignore[reportPrivateUsage]
    )

    async def end_socket_after_mismatch(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
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
    ) -> object:
        evidence = await original_preserve(
            capture,
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=received_wall_time,
            received_monotonic_ns=received_monotonic_ns,
        )
        if evidence.result.audit_outcomes and (
            evidence.result.audit_outcomes[-1].outcome.value == "MISMATCH"
        ):
            end_first_generation.set()
        return evidence

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_then_process_with_evidence",
        end_socket_after_mismatch,
    )
    first_replacement_processed = _LifecycleSocket.first_replacement_processed
    assert first_replacement_processed is not None
    first_replacement_processed.set()

    async with asyncio.timeout(30.0):
        summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    first_connection = summary.processor.audit_outcomes[0].connection_id
    first_generation_outcomes = tuple(
        outcome
        for outcome in summary.processor.audit_outcomes
        if outcome.connection_id == first_connection
    )
    assert tuple(outcome.outcome.value for outcome in first_generation_outcomes) == (
        "MISMATCH",
        "FAILED",
    )
    assert first_generation_outcomes[-1].reason is not None
    assert first_generation_outcomes[-1].reason.value == "SOCKET_GENERATION_CHANGED"
    assert first_no_cancelled.is_set()
    assert len(_LifecycleSocket.instances) >= 2
    assert summary.runtime_health.reconnects == 1
    assert summary.runtime_health.resyncs == 1

    counters = {key.name: value for key, value in summary.observability_metrics.counters}
    assert counters["mma_websocket_disconnects_total"] == 1
    assert counters["mma_websocket_reconnects_total"] == 1
    records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    assert sum(record.channel == CONTROL_BOOK_INVALIDATION for record in records) == 1


@pytest.mark.asyncio
async def test_paired_mismatches_create_two_direct_contributions_and_two_captures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both explicit binary legs can diverge in one cycle under one union interval."""

    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    _LifecyclePublicClient.diverge_both = True
    first_replacement_processed = _LifecycleSocket.first_replacement_processed
    assert first_replacement_processed is not None
    first_replacement_processed.set()
    capture_states: list[tuple[str, int, int]] = []
    original_preserve = observe_module._RawCaptureBoundary.preserve_then_process  # pyright: ignore[reportPrivateUsage]

    async def observe_two_capture_authorities(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
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
        recovery_active = bool(capture.processor.audit_divergence_pending_records)
        result = await original_preserve(
            capture,
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=received_wall_time,
            received_monotonic_ns=received_monotonic_ns,
        )
        if channel == REST_BOOK and recovery_active:
            native_asset_id = cast(dict[str, object], json.loads(payload))["asset_id"]
            assert isinstance(native_asset_id, str)
            capture_states.append(
                (
                    native_asset_id,
                    capture.processor.audit_replacement_captures_pending,
                    len(capture.processor.audit_divergence_pending_records),
                )
            )
        return result

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_then_process",
        observe_two_capture_authorities,
    )

    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    first_connection = summary.processor.audit_outcomes[0].connection_id
    first_generation_outcomes = tuple(
        outcome
        for outcome in summary.processor.audit_outcomes
        if outcome.connection_id == first_connection
    )
    assert tuple(outcome.outcome.value for outcome in first_generation_outcomes) == (
        "MISMATCH",
        "MISMATCH",
    )
    assert capture_states == [
        ("token-yes", 1, 1),
        ("token-no", 2, 0),
    ]
    assert summary.processor.audit_divergences_observed == 2
    assert summary.processor.audit_divergences_resolved_by_authoritative_resnapshot == 2
    assert summary.processor.audit_divergences_pending == 0
    assert summary.processor.audit_replacement_captures_pending == 0
    assert summary.processor.audit_divergence_lifecycle_reconciled
    assert summary.runtime_health.unavailable_intervals == 1
    assert summary.runtime_health.availability_contributions_emitted == 2
    contribution_path = summary.availability_evidence.contribution_path
    assert contribution_path is not None
    contributions = tuple(
        cast(dict[str, object], json.loads(line))
        for line in contribution_path.read_text(encoding="utf-8").splitlines()
    )
    assert len(contributions) == 2
    assert len({item["interval_id"] for item in contributions}) == 1
    assert len({item["request_id"] for item in contributions}) == 2
    assert len({item["divergence_id"] for item in contributions}) == 2
    assert {item["instrument_id"] for item in contributions} == {
        outcome.starting_book.instrument_id.value for outcome in first_generation_outcomes
    }
    assert sum(cast(bool, item["initiating_contributor"]) for item in contributions) == 1
    assert all(
        item["recovery_outcome"] == AvailabilityRecoveryOutcome.RESTORED for item in contributions
    )
    assert summary.health_decision.gate1_eligible_so_far


@pytest.mark.asyncio
async def test_observed_divergence_resnapshot_replacement_generation_restores_current_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(tmp_path)
    _prepare_runtime(
        monkeypatch,
        fail_resnapshot=False,
        # Hold replacement capture across several 120 ms audit periods so the
        # launch-suppression assertion is independent of journal/fsync speed.
        replacement_sibling_delay_seconds=0.4,
    )
    recovery_states: list[tuple[str, int, tuple[str, ...]]] = []
    replacement_states: list[tuple[str, int, tuple[str, ...]]] = []
    replacement_processing_completed_ns: list[int] = []
    recovery_observation_started = False
    original_preserve = observe_module._RawCaptureBoundary.preserve_then_process  # pyright: ignore[reportPrivateUsage]

    async def observe_recovery_books(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
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
        nonlocal recovery_observation_started
        if channel == REST_BOOK and capture.processor.audit_divergence_pending_records:
            recovery_observation_started = True
        result = await original_preserve(
            capture,
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=received_wall_time,
            received_monotonic_ns=received_monotonic_ns,
        )
        if channel == REST_BOOK and recovery_observation_started:
            native_asset_id = cast(dict[str, object], json.loads(payload))["asset_id"]
            assert isinstance(native_asset_id, str)
            recovery_states.append(
                (
                    native_asset_id,
                    result.scan_count,
                    tuple(
                        instrument.value
                        for instrument in capture.processor.audit_ineligible_instruments
                    ),
                )
            )
        return result

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_then_process",
        observe_recovery_books,
    )
    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )

    async def observe_replacement_snapshots(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if len(_LifecycleSocket.instances) >= 2:
            for item, result in zip(items, results, strict=True):
                raw = cast(dict[str, object], json.loads(item.message.payload_bytes))
                if raw.get("event_type") != "book":
                    continue
                native_asset_id = raw["asset_id"]
                assert isinstance(native_asset_id, str)
                replacement_states.append(
                    (
                        native_asset_id,
                        result.scan_count,
                        tuple(
                            instrument.value
                            for instrument in capture.processor.audit_ineligible_instruments
                        ),
                    )
                )
                if native_asset_id == "token-yes":
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
                else:
                    replacement_processing_completed_ns.append(time.monotonic_ns())
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        observe_replacement_snapshots,
    )

    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    client = _LifecyclePublicClient.latest
    assert client is not None
    assert client.audit_calls[:2] == ["token-yes", "token-no"]
    assert len(client.audit_calls) >= 2
    assert client.resnapshot_book_calls == ["token-yes", "token-no"]
    assert client.first_generation_stopped_before_resnapshot
    assert len(_LifecycleSocket.instances) == 2
    assert all(socket.stopped for socket in _LifecycleSocket.instances)
    assert tuple(state[0] for state in recovery_states) == ("token-yes", "token-no")
    assert recovery_states[0][1] == 0
    assert len(recovery_states[0][2]) == 1
    blocked_instrument_id = recovery_states[0][2][0]
    assert recovery_states[1][2] == (blocked_instrument_id,)
    assert tuple(state[0] for state in replacement_states) == ("token-yes", "token-no")
    assert replacement_states[0][1] == 0
    assert replacement_states[0][2] == (blocked_instrument_id,)
    assert replacement_states[1][1] == 0
    assert replacement_states[1][2] == (blocked_instrument_id,)

    processor = summary.processor
    assert processor.rest_audit_mismatches == 1
    assert processor.audit_divergences_observed == 1
    assert processor.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert processor.audit_divergences_resolved_same_generation == 0
    assert processor.audit_divergences_pending == 0
    assert processor.audit_divergences_unresolved == 0
    assert processor.audit_divergence_lifecycle_reconciled
    assert not processor.audit_divergence_pending_records
    assert len(processor.audit_divergence_resolution_records) == 1
    resolution = processor.audit_divergence_resolution_records[0]
    assert resolution.authoritative_snapshot_source.ingest_id != (
        resolution.mismatch_response_source.ingest_id
    )

    runtime = summary.runtime_health
    assert runtime.socket_generation == 2
    assert runtime.socket_connected
    assert runtime.resyncs == 1
    assert runtime.unavailable_intervals == 1
    assert runtime.availability_records_emitted == 1
    assert runtime.current_unavailable_started_monotonic_ns is None
    assert runtime.connection_id != (
        resolution.observed_divergence.audit_outcome.connection_id.value
    )

    interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    assert interval["cause_type"] == AvailabilityCause.AUDIT_MISMATCH_RECOVERY
    assert interval["reason_code"] == "REST_AUDIT_MISMATCH"
    assert interval["recovery_action"] == AvailabilityRecoveryAction.RESNAPSHOT
    assert interval["recovery_outcome"] == AvailabilityRecoveryOutcome.RESTORED
    assert interval["research_eligibility_restored"] is True
    assert interval["open_at_shutdown"] is False
    assert interval["socket_generation"] == 1
    assert interval["affected_scopes"] == [
        AvailabilityScope.AUDIT_ELIGIBILITY.value,
        AvailabilityScope.CANONICAL_BOOKS.value,
        AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY.value,
    ]
    assert interval["ended_monotonic_ns"] >= interval["started_monotonic_ns"]
    assert interval["ended_monotonic_ns"] >= replacement_processing_completed_ns[-1]

    final_checkpoint = json.loads(
        summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1]
    )
    assert final_checkpoint["audit"]["divergences"]["observed"] == 1
    assert final_checkpoint["audit"]["divergences"]["pending"] == 0
    assert final_checkpoint["audit"]["divergences"]["resolved_by_authoritative_resnapshot"] == 1
    assert final_checkpoint["audit"]["divergences"]["evidence_reconciled"] is True
    assert final_checkpoint["decisions"]["capture_healthy"] is True
    assert final_checkpoint["decisions"]["research_eligible"] is True
    assert final_checkpoint["decisions"]["gate1_eligible_so_far"] is True
    assert _record_decision_reasons(final_checkpoint) == ()
    assert summary.status is ComponentHealth.HEALTHY
    assert summary.health_decision.capture_healthy
    assert summary.health_decision.research_eligible
    assert summary.health_decision.gate1_eligible_so_far
    assert summary.health_decision.reason_codes == ()
    suppressed_launches = {
        (key.name, dict(key.labels).get("reason")): value
        for key, value in summary.observability_metrics.counters
    }
    assert (
        suppressed_launches[
            ("mma_rest_audit_launches_suppressed_total", "divergence_recovery_pending")
        ]
        >= 2
    )

    raw_records = JournalReader(
        summary.journal_root,
        summary.collector_run_id,
    ).read_all()
    audit_response_index = next(
        index for index, record in enumerate(raw_records) if record.channel == REST_BOOK_AUDIT
    )
    authoritative_index = next(
        index
        for index, record in enumerate(raw_records)
        if index > audit_response_index and record.channel == REST_BOOK
    )
    replacement_sibling_index = next(
        index
        for index, record in enumerate(raw_records)
        if index > authoritative_index
        and record.channel == WS_MARKET
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("event_type") == "book"
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("asset_id") == "token-no"
    )
    release_index = next(
        index
        for index, record in enumerate(raw_records)
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2
    )
    release_record = raw_records[release_index]
    release_payload = cast(dict[str, object], json.loads(release_record.payload_bytes))
    released_capture = cast(dict[str, object], release_payload["capture_record"])
    released_sources = cast(
        list[dict[str, object]],
        released_capture["captured_snapshot_sources"],
    )
    assert audit_response_index < authoritative_index
    assert authoritative_index < replacement_sibling_index < release_index
    assert release_payload["divergence_id"] == resolution.divergence_id
    assert (
        release_payload["capture_state_sha256"]
        == hashlib.sha256(_canonical_bytes(released_capture)).hexdigest()
    )
    assert len(released_sources) == 2
    assert all(
        "raw_source" in source and "received_monotonic" in source for source in released_sources
    )
    assert release_payload["decision_monotonic_ns"] == (
        release_record.received_monotonic.nanoseconds
    )
    assert interval["ended_monotonic_ns"] == release_payload["decision_monotonic_ns"]
    assert not any(
        record.channel == CONTROL_REST_AUDIT_START
        for record in raw_records[authoritative_index + 1 : replacement_sibling_index]
    )

    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == summary.processor
    assert replay.processor.terminal_hash == summary.processor.terminal_hash


@pytest.mark.asyncio
async def test_sibling_reported_top_blocks_release_until_authoritative_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    _LifecycleSocket.inject_sibling_bbo_overlap = True
    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )
    overlap_scan_counts: list[int] = []

    async def observe_sibling_overlap(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        preservation_started = _LifecycleSocket.final_snapshot_preservation_started
        if (
            len(_LifecycleSocket.instances) >= 2
            and preservation_started is not None
            and not preservation_started.is_set()
            and any(
                cast(dict[str, object], json.loads(item.message.payload_bytes)).get("event_type")
                == "book"
                and cast(
                    dict[str, object],
                    json.loads(item.message.payload_bytes),
                ).get("asset_id")
                == "token-no"
                for item in items
            )
        ):
            bbo_admitted = _LifecycleSocket.sibling_bbo_admitted
            assert bbo_admitted is not None
            preservation_started.set()
            # Hold the final snapshot inside its durable-preservation await
            # until a later same-generation frame has been admitted.  The
            # coordinator must drain that admitted prefix before it can sample
            # or journal a replacement-release decision.
            await bbo_admitted.wait()
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if len(_LifecycleSocket.instances) >= 2:
            for item, result in zip(items, results, strict=True):
                raw = cast(dict[str, object], json.loads(item.message.payload_bytes))
                event_type = raw.get("event_type")
                if event_type == "book" and raw.get("asset_id") == "token-yes":
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
                if event_type == "best_bid_ask":
                    overlap_scan_counts.append(result.scan_count)
                    assert capture.processor.audit_replacement_captures_pending == 1
                    assert capture.processor.reported_top_transitions
                    assert not capture.processor.audit_replacement_release_commands(
                        connection_id=connection_id,
                        decision_monotonic=MonotonicTimestamp(
                            capture.collector_run_id.value,
                            time.monotonic_ns(),
                        ),
                    )
                    sibling_processed = _LifecycleSocket.sibling_transition_processed
                    assert sibling_processed is not None
                    sibling_processed.set()
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        observe_sibling_overlap,
    )

    # Give the fixture's event-driven second generation enough acquisition
    # time even when the preceding tests leave the host under I/O pressure.
    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    assert overlap_scan_counts == [0]
    assert summary.processor.audit_replacement_captures_pending == 0
    assert summary.processor.audit_ineligible_books == 0
    assert summary.health_decision.research_eligible
    assert summary.availability_evidence.open_at_shutdown_intervals == 0
    interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    assert interval["recovery_outcome"] == AvailabilityRecoveryOutcome.RESTORED
    assert interval["research_eligibility_restored"] is True

    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    bbo_index = next(
        index
        for index, record in enumerate(raw_records)
        if record.channel == WS_MARKET
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("event_type")
        == "best_bid_ask"
    )
    final_sibling_index = next(
        index
        for index, record in enumerate(raw_records)
        if index > bbo_index
        and record.channel == WS_MARKET
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("event_type") == "book"
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("asset_id") == "token-no"
    )
    release_indices = tuple(
        index
        for index, record in enumerate(raw_records)
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2
    )
    assert len(release_indices) == 1
    initial_sibling_index = max(
        index
        for index, record in enumerate(raw_records[:bbo_index])
        if record.channel == WS_MARKET
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("event_type") == "book"
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("asset_id") == "token-no"
    )
    assert initial_sibling_index < bbo_index < final_sibling_index < release_indices[0]
    preservation_started = _LifecycleSocket.final_snapshot_preservation_started
    bbo_admitted = _LifecycleSocket.sibling_bbo_admitted
    assert preservation_started is not None
    assert preservation_started.is_set()
    assert bbo_admitted is not None
    assert bbo_admitted.is_set()

    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == summary.processor
    assert replay.processor.terminal_hash == summary.processor.terminal_hash


@pytest.mark.asyncio
async def test_stale_first_replacement_snapshot_cannot_close_recovery_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The same age bound governs the initial durable audit protocol and the
    # replacement pair. One second leaves deterministic NTFS/fsync headroom;
    # the event-gated sibling delay below still proves replacement staleness.
    maximum_book_age_seconds = 1.0
    service = _configured_service(
        tmp_path,
        maximum_book_age_seconds=maximum_book_age_seconds,
    )
    _prepare_runtime(
        monkeypatch,
        fail_resnapshot=False,
        replacement_sibling_delay_seconds=maximum_book_age_seconds + 0.04,
    )
    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )
    processed_replacement_assets: list[str] = []

    async def signal_first_replacement_processed(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if len(_LifecycleSocket.instances) >= 2:
            for item in items:
                raw = cast(dict[str, object], json.loads(item.message.payload_bytes))
                if raw.get("event_type") != "book":
                    continue
                native_asset_id = raw.get("asset_id")
                assert isinstance(native_asset_id, str)
                processed_replacement_assets.append(native_asset_id)
                if native_asset_id == "token-yes":
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        signal_first_replacement_processed,
    )

    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    # The idle recovery timer owns the first snapshot's freshness boundary.
    # It closes and rotates the attempt before the deliberately stale sibling
    # can be admitted; waiting for that sibling would make timer progress
    # dependent on another WebSocket frame.
    assert processed_replacement_assets == ["token-yes"]
    client = _LifecyclePublicClient.latest
    assert client is not None
    assert client.resnapshot_book_calls == ["token-yes", "token-no"]
    processor = summary.processor
    assert processor.audit_divergences_observed == 1
    assert processor.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert processor.audit_divergences_pending == 0
    assert processor.audit_replacement_captures_pending == 1
    assert processor.audit_ineligible_books == 1

    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    assert len(processor.audit_divergence_resolution_records) == 1
    resolution = processor.audit_divergence_resolution_records[0]
    authoritative_index = next(
        index
        for index, record in enumerate(raw_records)
        if record.ingest_id == resolution.authoritative_snapshot_source.ingest_id
    )
    replacement_records = tuple(
        (index, record, cast(dict[str, object], json.loads(record.payload_bytes)))
        for index, record in enumerate(raw_records)
        if record.channel == WS_MARKET
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("event_type") == "book"
    )
    assert tuple(item[2].get("asset_id") for item in replacement_records) == ("token-yes",)
    yes_index, yes_record, _yes_payload = replacement_records[0]
    authoritative_record = raw_records[authoritative_index]
    lifecycle_records = tuple(
        (index, record, cast(dict[str, object], json.loads(record.payload_bytes)))
        for index, record in enumerate(raw_records)
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY
    )
    assert tuple(item[2]["action"] for item in lifecycle_records) == (
        "ATTEMPT_START",
        "DEADLINE",
        "DEADLINE",
    )
    attempt_index = lifecycle_records[0][0]
    freshness_deadline_index, freshness_deadline_record, _deadline_payload = lifecycle_records[1]
    assert authoritative_index < attempt_index < yes_index < freshness_deadline_index
    assert (
        authoritative_record.received_monotonic.epoch_id
        == yes_record.received_monotonic.epoch_id
        == freshness_deadline_record.received_monotonic.epoch_id
    )
    assert (
        authoritative_record.received_monotonic.nanoseconds
        <= yes_record.received_monotonic.nanoseconds
    )
    assert (
        freshness_deadline_record.received_monotonic.nanoseconds
        - yes_record.received_monotonic.nanoseconds
        > int(maximum_book_age_seconds * 1_000_000_000)
    )
    capture_authority = processor.audit_replacement_capture_authority[0]
    recovery = capture_authority.recovery_authority
    assert recovery is not None
    assert recovery.status.value == "RUN_DEADLINE_EXHAUSTED"
    assert recovery.last_result.value == "RUN_DEADLINE_EXHAUSTED"
    assert recovery.last_failure is not None
    assert recovery.last_failure.value == "RUN_DEADLINE_REACHED"
    assert recovery.attempt_count == 1
    assert recovery.unresolved
    assert not summary.health_decision.research_eligible
    assert not summary.health_decision.gate1_eligible_so_far
    assert HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING in (
        summary.health_decision.reason_codes
    )
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.reason_codes
    )
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.gate1_sticky_reason_codes
    )

    runtime = summary.runtime_health
    assert runtime.unavailable_intervals == 1
    assert runtime.availability_records_emitted == 1
    assert summary.availability_evidence.open_at_shutdown_intervals == 1
    interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    assert interval["cause_type"] == AvailabilityCause.AUDIT_MISMATCH_RECOVERY
    assert interval["recovery_outcome"] == (AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN)
    assert interval["research_eligibility_restored"] is False
    assert interval["open_at_shutdown"] is True

    final_checkpoint = json.loads(
        summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1]
    )
    divergences = final_checkpoint["audit"]["divergences"]
    assert divergences["pending"] == 0
    assert divergences["replacement_captures_pending"] == 1
    assert divergences["replacement_captures_unresolved"] == 1
    assert final_checkpoint["audit"]["current_eligibility"]["eligible"] is False
    assert "AUDIT_DIVERGENCE_UNRESOLVED" in _record_decision_reasons(final_checkpoint, sticky=True)


@pytest.mark.asyncio
async def test_slow_canonical_return_cannot_scan_or_release_stale_replacement_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This age also bounds the initial audit protocol; retain ample durable-I/O
    # headroom and cross the bound only after both replacement frames process.
    maximum_book_age_seconds = 1.0
    service = _configured_service(
        tmp_path,
        maximum_book_age_seconds=maximum_book_age_seconds,
    )
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )
    replacement_scan_counts: list[int] = []
    processed_replacement_assets: list[str] = []

    async def delay_completed_replacement_batch(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        delay_return = False
        if len(_LifecycleSocket.instances) >= 2:
            for item, result in zip(items, results, strict=True):
                raw = cast(dict[str, object], json.loads(item.message.payload_bytes))
                if raw.get("event_type") != "book":
                    continue
                native_asset_id = raw.get("asset_id")
                assert isinstance(native_asset_id, str)
                processed_replacement_assets.append(native_asset_id)
                replacement_scan_counts.append(result.scan_count)
                if native_asset_id == "token-yes":
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
                elif native_asset_id == "token-no":
                    delay_return = True
        if delay_return:
            # Both raw snapshots are mutually fresh and already canonical, but
            # the coordinator cannot see their result until after the hard age
            # bound.  No scanner/research release may occur in that gap.
            await asyncio.sleep(maximum_book_age_seconds + 0.04)
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        delay_completed_replacement_batch,
    )

    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    assert processed_replacement_assets == ["token-yes", "token-no"]
    assert replacement_scan_counts == [0, 0]
    assert summary.processor.audit_divergences_observed == 1
    assert summary.processor.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert summary.processor.audit_divergences_pending == 0
    assert summary.processor.audit_replacement_captures_pending == 1
    assert summary.processor.audit_ineligible_books == 1
    assert not summary.health_decision.research_eligible
    assert HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING in (
        summary.health_decision.reason_codes
    )
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.gate1_sticky_reason_codes
    )

    raw_records = JournalReader(
        summary.journal_root,
        summary.collector_run_id,
    ).read_all()
    assert all(
        record.channel
        not in {
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
        }
        for record in raw_records
    )
    assert summary.runtime_health.unavailable_intervals == 1
    assert summary.availability_evidence.open_at_shutdown_intervals == 1
    interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    assert interval["recovery_outcome"] == (AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN)
    assert interval["research_eligibility_restored"] is False
    assert interval["open_at_shutdown"] is True


@pytest.mark.asyncio
async def test_ended_replacement_generation_cannot_release_after_canonical_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(tmp_path, max_reconnect_attempts=0)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    _LifecycleSocket.end_replacement_before_canonical_return = True
    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )
    replacement_scan_counts: list[int] = []
    processed_replacement_assets: list[str] = []
    canonical_return_monotonic_ns: list[int] = []

    async def end_acquisition_before_completed_replacement_return(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if len(_LifecycleSocket.instances) >= 2:
            for item, result in zip(items, results, strict=True):
                raw = cast(dict[str, object], json.loads(item.message.payload_bytes))
                if raw.get("event_type") != "book":
                    continue
                native_asset_id = raw.get("asset_id")
                assert isinstance(native_asset_id, str)
                processed_replacement_assets.append(native_asset_id)
                replacement_scan_counts.append(result.scan_count)
                if native_asset_id == "token-yes":
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
                elif native_asset_id == "token-no":
                    allow_replacement_end = _LifecycleSocket.allow_replacement_end
                    replacement_ended = _LifecycleSocket.replacement_ended
                    assert allow_replacement_end is not None
                    assert replacement_ended is not None
                    allow_replacement_end.set()
                    await asyncio.wait_for(replacement_ended.wait(), timeout=1.0)
                    canonical_return_monotonic_ns.append(time.monotonic_ns())
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        end_acquisition_before_completed_replacement_return,
    )

    # Generation two intentionally ends only after the exact canonical-return
    # fence below. With reconnects disabled for this test, that event completes
    # the observation immediately; five seconds is solely a failure deadline
    # for loaded CI hosts, not a normal-path sleep.
    summary = await service.run(duration_seconds=5.0, gamma_market_id="101")

    assert processed_replacement_assets == ["token-yes", "token-no"]
    assert replacement_scan_counts == [0, 0]
    assert len(canonical_return_monotonic_ns) == 1
    replacement_ended_ns = _LifecycleSocket.replacement_ended_monotonic_ns
    assert replacement_ended_ns is not None
    assert replacement_ended_ns < canonical_return_monotonic_ns[0]
    assert summary.processor.audit_divergences_observed == 1
    assert summary.processor.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert summary.processor.audit_divergences_pending == 0
    assert summary.processor.audit_replacement_captures_pending == 1
    assert summary.processor.audit_ineligible_books == 1
    assert not summary.health_decision.research_eligible
    assert HealthDecisionReason.AUDIT_REPLACEMENT_CAPTURE_PENDING in (
        summary.health_decision.reason_codes
    )
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.gate1_sticky_reason_codes
    )

    raw_records = JournalReader(
        summary.journal_root,
        summary.collector_run_id,
    ).read_all()
    assert all(
        record.channel
        not in {
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
        }
        for record in raw_records
    )
    assert summary.runtime_health.unavailable_intervals == 1
    assert summary.availability_evidence.open_at_shutdown_intervals == 1
    interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
    assert interval["recovery_outcome"] == (AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN)
    assert interval["research_eligibility_restored"] is False
    assert interval["open_at_shutdown"] is True


@pytest.mark.asyncio
async def test_generation_end_after_sampled_release_decision_does_not_retroactively_strand(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _configured_service(tmp_path, max_reconnect_attempts=0)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    _LifecycleSocket.end_replacement_before_canonical_return = True
    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )
    replacement_scan_counts: list[int] = []

    async def signal_first_replacement(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if len(_LifecycleSocket.instances) >= 2:
            for item, result in zip(items, results, strict=True):
                raw = cast(dict[str, object], json.loads(item.message.payload_bytes))
                if raw.get("event_type") != "book":
                    continue
                replacement_scan_counts.append(result.scan_count)
                if raw.get("asset_id") == "token-yes":
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        signal_first_replacement,
    )
    original_preserve = observe_module._RawCaptureBoundary.preserve_then_process  # pyright: ignore[reportPrivateUsage]
    release_chronology: list[tuple[int, int]] = []
    release_processor_states: list[tuple[int, int, int]] = []

    async def end_generation_during_release_durability(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
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
        if channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2:
            release = cast(dict[str, object], json.loads(payload))
            decision_ns = cast(int, release["decision_monotonic_ns"])
            assert capture.processor.audit_replacement_captures_pending == 1
            assert capture.processor.audit_ineligible_instruments
            allow_replacement_end = _LifecycleSocket.allow_replacement_end
            replacement_ended = _LifecycleSocket.replacement_ended
            assert allow_replacement_end is not None
            assert replacement_ended is not None
            allow_replacement_end.set()
            await asyncio.wait_for(replacement_ended.wait(), timeout=1.0)
            ended_ns = _LifecycleSocket.replacement_ended_monotonic_ns
            assert ended_ns is not None
            release_chronology.append((decision_ns, ended_ns))
            # Hold the release before raw durability/canonical application.
            # Complete replacement snapshots remain scan-ineligible throughout
            # this delay even though the decision has already been sampled.
            await asyncio.sleep(0.05)
            assert capture.processor.audit_replacement_captures_pending == 1
            assert capture.processor.audit_ineligible_instruments
        result = await original_preserve(
            capture,
            channel=channel,
            payload=payload,
            transport_type=transport_type,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
            transport_metadata=transport_metadata,
            received_wall_time=received_wall_time,
            received_monotonic_ns=received_monotonic_ns,
        )
        if channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2:
            release_processor_states.append(
                (
                    1,
                    capture.processor.audit_replacement_captures_pending,
                    result.scan_count,
                )
            )
        return result

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_then_process",
        end_generation_during_release_durability,
    )

    original_try_release = cast(
        Callable[..., Awaitable[bool]],
        PolymarketObservationService._try_release_audit_replacement_captures,  # pyright: ignore[reportPrivateUsage]
    )
    release_runtime_states: list[tuple[int, int, int, bool]] = []

    async def observe_runtime_release_reconciliation(
        release_service: PolymarketObservationService,
        **kwargs: object,
    ) -> bool:
        release_capture = cast(
            observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
            kwargs["capture"],
        )
        runtime_health = cast(ObservationRuntimeHealth, kwargs["runtime_health"])
        is_release = any(
            authority.status.value == "CAPTURE_COMPLETE"
            for authority in release_capture.processor.audit_replacement_recovery_authorities
        )
        before = runtime_health.snapshot() if is_release else None
        if is_release:
            contributors = runtime_health.active_availability_contributors
            assert len(contributors) == 1
            recovery = contributors[0].replacement_recovery
            assert recovery is not None
            assert recovery.status == "CAPTURE_COMPLETE"
            assert recovery.last_result == "CAPTURE_SUCCEEDED"
        deferred = await original_try_release(release_service, **kwargs)
        if is_release:
            assert before is not None
            after = runtime_health.snapshot()
            release_runtime_states.append(
                (
                    before.active_availability_contributors,
                    after.active_availability_contributors,
                    after.availability_contributions_emitted,
                    after.availability_ledger_healthy,
                )
            )
        return deferred

    monkeypatch.setattr(
        PolymarketObservationService,
        "_try_release_audit_replacement_captures",
        observe_runtime_release_reconciliation,
    )

    # The sampled-release fence intentionally ends generation two. Disabling
    # reconnects makes that event the successful completion path; five seconds
    # is only a loaded-host failure deadline.
    summary = await service.run(duration_seconds=5.0, gamma_market_id="101")

    assert len(release_chronology) == 1
    assert replacement_scan_counts == [0, 0]
    assert release_processor_states == [(1, 0, 1)]
    assert release_runtime_states == [(1, 0, 1, True)]
    decision_ns, ended_ns = release_chronology[0]
    assert decision_ns < ended_ns
    assert summary.processor.audit_replacement_captures_pending == 0
    assert summary.processor.audit_ineligible_books == 0
    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    assert (
        sum(record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2 for record in raw_records)
        == 1
    )
    intervals = tuple(
        cast(dict[str, object], json.loads(line))
        for line in summary.availability_evidence.path.read_text(encoding="utf-8").splitlines()
    )
    mismatch_interval = next(
        interval
        for interval in intervals
        if interval["cause_type"] == AvailabilityCause.AUDIT_MISMATCH_RECOVERY
    )
    assert mismatch_interval["ended_monotonic_ns"] == decision_ns
    assert mismatch_interval["research_eligibility_restored"] is True
    assert mismatch_interval["open_at_shutdown"] is False
    contribution_path = summary.availability_evidence.contribution_path
    assert contribution_path is not None
    contributions = tuple(
        cast(dict[str, object], json.loads(line))
        for line in contribution_path.read_text(encoding="utf-8").splitlines()
    )
    contribution = next(
        item
        for item in contributions
        if cast(dict[str, object], item["replacement_recovery"])["status"] == "RELEASED"
    )
    recovery_record = cast(dict[str, object], contribution["replacement_recovery"])
    assert recovery_record["status"] == "RELEASED"
    assert recovery_record["last_result"] == "RELEASED"

    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == summary.processor
    assert replay.processor.terminal_hash == summary.processor.terminal_hash


@pytest.mark.asyncio
@pytest.mark.parametrize("availability_writer_failure", [False, True])
async def test_resnapshot_failure_keeps_divergence_terminally_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    availability_writer_failure: bool,
) -> None:
    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=True)
    if availability_writer_failure:
        original_enqueue = SupervisedAvailabilityIntervalWriter.enqueue_contribution_nowait

        def fail_availability_admission(
            writer: SupervisedAvailabilityIntervalWriter,
            contribution: AvailabilityIntervalContributionV2,
        ) -> bool:
            writer.record_failure(OSError("injected availability writer failure while pending"))
            return original_enqueue(writer, contribution)

        monkeypatch.setattr(
            SupervisedAvailabilityIntervalWriter,
            "enqueue_contribution_nowait",
            fail_availability_admission,
        )

    # The injected non-retryable resnapshot failure terminates causally; this
    # bound protects only against a loaded host failing to reach that event.
    summary = await service.run(duration_seconds=5.0, gamma_market_id="101")

    client = _LifecyclePublicClient.latest
    assert client is not None
    assert client.audit_calls == ["token-yes", "token-no"]
    assert client.resnapshot_book_calls == ["token-yes"]
    assert client.first_generation_stopped_before_resnapshot
    assert len(_LifecycleSocket.instances) == 1
    assert _LifecycleSocket.instances[0].stopped

    processor = summary.processor
    assert processor.audit_divergences_observed == 1
    assert processor.audit_divergences_resolved_by_authoritative_resnapshot == 0
    assert processor.audit_divergences_pending == 1
    assert processor.audit_divergences_unresolved == 1
    assert processor.audit_divergence_lifecycle_reconciled
    assert len(processor.audit_divergence_pending_records) == 1
    assert not processor.audit_divergence_resolution_records
    assert summary.status is ComponentHealth.DEGRADED
    assert not summary.health_decision.research_eligible
    assert not summary.health_decision.gate1_eligible_so_far
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.reason_codes
    )
    assert HealthDecisionReason.SOCKET_DISCONNECTED in summary.health_decision.reason_codes
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in (
        summary.health_decision.gate1_sticky_reason_codes
    )

    runtime = summary.runtime_health
    assert runtime.socket_generation == 1
    assert not runtime.socket_connected
    assert runtime.unavailable_intervals == 1
    assert runtime.current_unavailable_started_monotonic_ns is None
    if availability_writer_failure:
        assert not runtime.availability_ledger_healthy
        assert runtime.availability_records_emitted == 0
        assert not summary.availability_evidence.gate1_evidence_eligible
        assert summary.availability_evidence.failure_type == "OSError"
        assert HealthDecisionReason.AVAILABILITY_EVIDENCE_UNHEALTHY in (
            summary.health_decision.reason_codes
        )
    else:
        assert runtime.availability_ledger_healthy
        assert runtime.availability_records_emitted == 1
        assert summary.availability_evidence.open_at_shutdown_intervals == 1
        interval = json.loads(summary.availability_evidence.path.read_text(encoding="utf-8"))
        assert interval["cause_type"] == (AvailabilityCause.AUDIT_MISMATCH_RECOVERY)
        assert interval["recovery_action"] == AvailabilityRecoveryAction.RESNAPSHOT
        assert interval["recovery_outcome"] == (
            AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
        )
        assert interval["research_eligibility_restored"] is False
        assert interval["open_at_shutdown"] is True

    final_checkpoint = json.loads(
        summary.health_evidence.path.read_text(encoding="utf-8").splitlines()[-1]
    )
    assert final_checkpoint["audit"]["divergences"]["observed"] == 1
    assert final_checkpoint["audit"]["divergences"]["pending"] == 1
    assert final_checkpoint["audit"]["divergences"]["unresolved"] == 1
    assert final_checkpoint["decisions"]["research_eligible"] is False
    assert HealthDecisionReason.AUDIT_DIVERGENCE_PENDING in _record_decision_reasons(
        final_checkpoint
    )
    assert HealthDecisionReason.AUDIT_DIVERGENCE_UNRESOLVED in _record_decision_reasons(
        final_checkpoint
    )
    assert "AUDIT_DIVERGENCE_UNRESOLVED" in _record_decision_reasons(final_checkpoint, sticky=True)


@pytest.mark.asyncio
async def test_contribution_writer_failure_does_not_stop_raw_ws_durability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A derived-evidence failure cannot become a raw-capture stop signal."""

    service = _configured_service(tmp_path)
    _prepare_runtime(monkeypatch, fail_resnapshot=False)
    writer_failed = asyncio.Event()
    post_failure_processed = asyncio.Event()
    failure_monotonic_ns: list[int] = []
    failed_contributions: list[AvailabilityIntervalContributionV2] = []
    post_failure_payloads = tuple(
        _canonical_bytes(
            {
                **cast(
                    dict[str, object],
                    json.loads(_replacement_ws_book_payload(asset_id)),
                ),
                "hash": f"post-contribution-writer-failure-{asset_id}",
                "timestamp": timestamp,
            }
        )
        for asset_id, timestamp in (
            ("token-yes", "1787198403001"),
            ("token-no", "1787198403002"),
        )
    )
    post_failure_hashes = {
        cast(dict[str, object], json.loads(payload))["hash"] for payload in post_failure_payloads
    }

    class _RawContinuesAfterContributionWriterFailureSocket(_LifecycleSocket):
        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            try:
                if self.generation == 2:
                    first_processed = type(self).first_replacement_processed
                    if first_processed is None:
                        raise RuntimeError("replacement-generation test fence is unavailable")
                    admit(
                        ReceivedWebSocketMessage(
                            payload_bytes=_replacement_ws_book_payload("token-yes"),
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=time.monotonic_ns(),
                            message_type="TEXT",
                        )
                    )
                    await first_processed.wait()
                    admit(
                        ReceivedWebSocketMessage(
                            payload_bytes=_replacement_ws_book_payload("token-no"),
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=time.monotonic_ns(),
                            message_type="TEXT",
                        )
                    )
                    # The replacement pair closes the mismatch contributor.
                    # Only its failed writer admission releases this fence, so
                    # both following frames are causally post-failure.
                    await writer_failed.wait()
                    for payload in post_failure_payloads:
                        admit(
                            ReceivedWebSocketMessage(
                                payload_bytes=payload,
                                received_wall_time=UtcTimestamp.now(),
                                received_monotonic_ns=time.monotonic_ns(),
                                message_type="TEXT",
                            )
                        )
                    await post_failure_processed.wait()
                await asyncio.Event().wait()
            finally:
                self.stopped = True

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _RawContinuesAfterContributionWriterFailureSocket,
    )

    original_enqueue = SupervisedAvailabilityIntervalWriter.enqueue_contribution_nowait

    def fail_mismatch_contribution_admission(
        writer: SupervisedAvailabilityIntervalWriter,
        contribution: AvailabilityIntervalContributionV2,
    ) -> bool:
        if (
            not writer_failed.is_set()
            and contribution.cause is AvailabilityCause.AUDIT_MISMATCH_RECOVERY
        ):
            failed_contributions.append(contribution)
            writer.record_failure(OSError("injected mid-run contribution writer failure"))
            failure_monotonic_ns.append(time.monotonic_ns())
            writer_failed.set()
        return original_enqueue(writer, contribution)

    monkeypatch.setattr(
        SupervisedAvailabilityIntervalWriter,
        "enqueue_contribution_nowait",
        fail_mismatch_contribution_admission,
    )

    original_preserve_websocket = (
        observe_module._RawCaptureBoundary.preserve_websocket_batch  # pyright: ignore[reportPrivateUsage]
    )
    processed_post_failure_hashes: list[str] = []

    async def observe_post_failure_durability(
        capture: observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        items: tuple[RawIngressItem, ...],
        *,
        semantics_hash: str,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> tuple[ProcessedEnvelope, ...]:
        results = await original_preserve_websocket(
            capture,
            items,
            semantics_hash=semantics_hash,
            connection_id=connection_id,
            subscription_id=subscription_id,
        )
        if len(_LifecycleSocket.instances) >= 2:
            for item in items:
                payload = cast(dict[str, object], json.loads(item.message.payload_bytes))
                if payload.get("asset_id") == "token-yes" and payload.get("hash") == (
                    "replacement-ws-token-yes"
                ):
                    first_processed = _LifecycleSocket.first_replacement_processed
                    assert first_processed is not None
                    first_processed.set()
                payload_hash = payload.get("hash")
                if isinstance(payload_hash, str) and payload_hash in post_failure_hashes:
                    processed_post_failure_hashes.append(payload_hash)
                    if len(processed_post_failure_hashes) == len(post_failure_payloads):
                        post_failure_processed.set()
        return results

    monkeypatch.setattr(
        observe_module._RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
        "preserve_websocket_batch",
        observe_post_failure_durability,
    )

    summary = await service.run(duration_seconds=3.0, gamma_market_id="101")

    assert writer_failed.is_set()
    assert post_failure_processed.is_set()
    assert len(failure_monotonic_ns) == 1
    assert len(failed_contributions) == 1
    assert failed_contributions[0].cause is AvailabilityCause.AUDIT_MISMATCH_RECOVERY
    assert set(processed_post_failure_hashes) == post_failure_hashes

    raw_records = JournalReader(summary.journal_root, summary.collector_run_id).read_all()
    raw_post_failure_records = tuple(
        record
        for record in raw_records
        if record.channel == WS_MARKET
        and cast(dict[str, object], json.loads(record.payload_bytes)).get("hash")
        in post_failure_hashes
    )
    assert len(raw_post_failure_records) == 2
    assert all(
        record.received_monotonic.nanoseconds > failure_monotonic_ns[0]
        for record in raw_post_failure_records
    )
    release_index = next(
        index
        for index, record in enumerate(raw_records)
        if record.channel == CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2
    )
    post_failure_indices = tuple(raw_records.index(record) for record in raw_post_failure_records)
    assert release_index < min(post_failure_indices)
    assert summary.journal_metrics.writer_failures == 0
    assert summary.journal_metrics.backpressure_failures == 0
    assert summary.journal_metrics.received == summary.journal_metrics.durable == len(raw_records)

    runtime = summary.runtime_health
    assert not runtime.availability_ledger_healthy
    assert runtime.completed_availability_contributors == 1
    assert runtime.availability_contribution_persistence_failures == 1
    assert runtime.availability_contributions_emitted == 0
    assert runtime.availability_records_emitted == 0
    assert summary.status is ComponentHealth.DEGRADED
    assert not summary.availability_evidence.completed
    assert not summary.availability_evidence.gate1_evidence_eligible
    assert summary.availability_evidence.failure_type == "OSError"
    assert summary.availability_evidence.contribution_failure_type == "OSError"
    # Raw capture and the derived analytical path remain independently
    # usable, but the unavailable contributor ledger cannot authorize Gate 1.
    assert summary.health_decision.capture_healthy
    assert summary.health_decision.research_eligible
    assert not summary.health_decision.gate1_eligible_so_far
    assert HealthDecisionReason.AVAILABILITY_EVIDENCE_UNHEALTHY in (
        summary.health_decision.reason_codes
    )

    replay = replay_journal(
        summary.journal_root,
        summary.collector_run_id,
        service._config,  # pyright: ignore[reportPrivateUsage]
    )
    assert replay.processor == summary.processor
    assert replay.processor.terminal_hash == summary.processor.terminal_hash
