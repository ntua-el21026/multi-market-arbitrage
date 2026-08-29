# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
from test_processor import (
    _audit_classification_attestation,
    _audit_classification_prepare,
    _audit_response,
    _audit_start,
    _bootstrap,
    _bootstrap_hard_expiry_ns,
    _envelope,
    _fixture,
    _no_book,
    _websocket_book_payload,
)

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId, ConnectionId, SubscriptionId
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.observability import StructuredLogger
from multi_market_arbitrage.pipeline import (
    AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
    REST_BOOK,
    WS_MARKET,
    AuditOutcome,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_REPLACEMENT_RELEASE_V1_SCHEMA,
    AUDIT_REPLACEMENT_RELEASE_V2_SCHEMA,
    AuditReplacementReleaseCommand,
)
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.replacement_recovery import (
    ReplacementRecoveryAuthority,
    ReplacementRecoveryFailure,
    ReplacementRecoveryLifecycleAction,
    ReplacementRecoveryResult,
    ReplacementRecoveryStatus,
)
from multi_market_arbitrage.venues.polymarket import PolymarketPublicClient

_RESOLUTION_MONOTONIC_NS = 300
_FIRST_REPLACEMENT_MONOTONIC_NS = 400
_RUN_EXTENSION_NS = 300_000_000_000


@dataclass(slots=True)
class _ResolvedMismatch:
    config: Phase1Config
    run_id: CollectorRunId
    processor: CanonicalProcessor
    envelopes: list[RawEventEnvelope]
    mismatched_yes: bytes
    run_deadline_ns: int


class _LifecycleCapture:
    def __init__(self, scenario: _ResolvedMismatch) -> None:
        self.collector_run_id = scenario.run_id
        self.processor = scenario.processor
        self._scenario = scenario

    async def preserve_then_process(self, **kwargs: object):
        channel = kwargs["channel"]
        payload = kwargs["payload"]
        connection_id = kwargs["connection_id"]
        received_monotonic_ns = kwargs["received_monotonic_ns"]
        assert isinstance(channel, str)
        assert isinstance(payload, bytes)
        assert isinstance(connection_id, ConnectionId)
        assert isinstance(received_monotonic_ns, int)
        envelope = _envelope(
            self.collector_run_id,
            len(self._scenario.envelopes) + 1,
            channel,
            payload,
            monotonic_ns=received_monotonic_ns,
            connection_id=connection_id,
        )
        self._scenario.envelopes.append(envelope)
        return self.processor.process_replayed(envelope)


class _LifecycleHealth:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.authorities: list[ReplacementRecoveryAuthority] = []

    def bind_contribution_replacement_recovery(
        self,
        *,
        divergence_id: str,
        authority: ReplacementRecoveryAuthority,
    ) -> None:
        self.authorities.append(authority)
        self.events.append(
            f"bind:{divergence_id}:{authority.status.value}:{authority.evidence_event_count}"
        )

    def finish_generation(self, **_kwargs: object) -> None:
        self.events.append("finish_generation")


class _LifecycleLogger:
    def info(self, *_args: object, **_kwargs: object) -> None:
        pass


def _current_config(*, hard_deadline_seconds: float | None = None) -> Phase1Config:
    config = load_phase1_config(Path("config/phase1.toml"))
    if hard_deadline_seconds is None:
        return config
    return config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={"audit_replacement_hard_deadline_seconds": hard_deadline_seconds}
            )
        }
    )


def _resolved_prepare_attest_mismatch(
    label: str,
    *,
    config: Phase1Config | None = None,
    source_journal_git_commit: str | None = None,
    expect_recovery_authority: bool = True,
) -> _ResolvedMismatch:
    effective_config = _current_config() if config is None else config
    run_id = CollectorRunId(f"replacement-recovery-processor-{label}")
    processor = CanonicalProcessor(
        effective_config,
        source_journal_git_commit=source_journal_git_commit,
    )
    envelopes: list[RawEventEnvelope] = []

    for envelope in _bootstrap(run_id):
        envelopes.append(envelope)
        assert processor.process_replayed(envelope).success

    hard_expiry_ns = _bootstrap_hard_expiry_ns(effective_config)
    run_deadline_ns = hard_expiry_ns + _RUN_EXTENSION_NS
    mismatched_yes = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"replacement-recovery-{label}",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=hard_expiry_ns,
        run_deadline_monotonic_ns=run_deadline_ns,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    assert intent.transport_metadata["classification_protocol"] == (
        AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL
    )
    envelopes.append(intent)
    assert processor.process_replayed(intent).success

    response = _audit_response(
        run_id,
        5,
        start,
        mismatched_yes,
        boundary=0,
        classification_deferred=True,
    )
    envelopes.append(response)
    staged = processor.process_replayed(response)
    assert staged.success
    assert staged.audit_outcomes == ()

    _, preparation = _audit_classification_prepare(
        run_id,
        6,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=200,
    )
    envelopes.append(preparation)
    prepared = processor.process_replayed(preparation)
    assert prepared.success
    assert prepared.audit_outcomes == ()

    _, attestation = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
    )
    envelopes.append(attestation)
    classified = processor.process_replayed(attestation)
    assert classified.success
    assert classified.needs_resync
    assert tuple(item.outcome for item in classified.audit_outcomes) == (AuditOutcome.MISMATCH,)

    resolution = _envelope(
        run_id,
        8,
        REST_BOOK,
        mismatched_yes,
        monotonic_ns=_RESOLUTION_MONOTONIC_NS,
    )
    envelopes.append(resolution)
    resolved = processor.process_replayed(resolution)
    assert resolved.success
    assert len(resolved.audit_divergences_resolved) == 1

    summary = processor.summary()
    assert summary.audit_divergences_pending == 0
    assert summary.audit_replacement_captures_pending == 1
    capture = summary.audit_replacement_capture_authority[0]
    recovery = capture.recovery_authority
    if not expect_recovery_authority:
        assert recovery is None
        return _ResolvedMismatch(
            config=effective_config,
            run_id=run_id,
            processor=processor,
            envelopes=envelopes,
            mismatched_yes=mismatched_yes,
            run_deadline_ns=run_deadline_ns,
        )
    assert recovery is not None
    assert recovery.status is ReplacementRecoveryStatus.READY
    assert recovery.attempt_count == 0
    assert recovery.resolution_ingest_seq == resolution.ingest_seq
    assert recovery.resolution_monotonic == resolution.received_monotonic
    assert recovery.required_instrument_ids == capture.required_instrument_ids

    return _ResolvedMismatch(
        config=effective_config,
        run_id=run_id,
        processor=processor,
        envelopes=envelopes,
        mismatched_yes=mismatched_yes,
        run_deadline_ns=run_deadline_ns,
    )


def _append_and_process(
    scenario: _ResolvedMismatch,
    envelope: RawEventEnvelope,
):
    scenario.envelopes.append(envelope)
    return scenario.processor.process_replayed(envelope)


def _current_recovery(scenario: _ResolvedMismatch):
    captures = scenario.processor.summary().audit_replacement_capture_authority
    assert len(captures) == 1
    recovery = captures[0].recovery_authority
    assert recovery is not None
    return recovery


def _start_attempt(
    scenario: _ResolvedMismatch,
    *,
    connection_id: ConnectionId,
    sequence: int,
    monotonic_ns: int = _FIRST_REPLACEMENT_MONOTONIC_NS,
) -> ReplacementRecoveryAuthority:
    decision = MonotonicTimestamp(scenario.run_id.value, monotonic_ns)
    commands = scenario.processor.audit_replacement_attempt_start_commands(
        connection_id=connection_id,
        decision_monotonic=decision,
    )
    assert len(commands) == 1
    result = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            sequence,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            commands[0].to_payload(),
            monotonic_ns=monotonic_ns,
            connection_id=connection_id,
        ),
    )
    assert result.success
    authority = _current_recovery(scenario)
    assert authority.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    return authority


def _assert_replay_equal(
    scenario: _ResolvedMismatch,
    *,
    expected_failures: int = 0,
) -> None:
    replay = CanonicalProcessor(scenario.config)
    replay_results = tuple(replay.process_replayed(envelope) for envelope in scenario.envelopes)

    assert sum(not result.success for result in replay_results) == expected_failures
    assert replay.summary() == scenario.processor.summary()
    assert replay.summary().terminal_hash == scenario.processor.summary().terminal_hash
    assert replay.snapshots() == scenario.processor.snapshots()


def test_prepare_attest_mismatch_releases_complete_first_connection_capture_and_replays() -> None:
    scenario = _resolved_prepare_attest_mismatch("first-connection-release")
    initial = _current_recovery(scenario)
    recovery_id = initial.recovery_id
    contributor_key = initial.stable_contributor_key
    connection_id = ConnectionId("replacement-first-permitted-connection")
    _start_attempt(scenario, connection_id=connection_id, sequence=9)

    first = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            10,
            WS_MARKET,
            _websocket_book_payload(scenario.mismatched_yes, label="replacement-yes"),
            monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": 1},
        ),
    )
    assert first.success
    assert first.scan_count == 0
    partial = _current_recovery(scenario)
    assert partial.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert partial.attempted_connection_ids == (connection_id,)
    assert partial.attempt_count == 1
    assert len(partial.captured_snapshot_sources) == 1
    assert partial.recovery_id == recovery_id
    assert partial.stable_contributor_key == contributor_key

    second = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            11,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="replacement-no"),
            monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + 1,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": 2},
        ),
    )
    assert second.success
    assert second.scan_count == 0
    complete = _current_recovery(scenario)
    assert complete.status is ReplacementRecoveryStatus.CAPTURE_COMPLETE
    assert complete.captured_instrument_ids == complete.required_instrument_ids
    assert len(complete.captured_snapshot_sources) == 2
    assert complete.recovery_id == recovery_id
    assert complete.stable_contributor_key == contributor_key

    decision_monotonic = MonotonicTimestamp(
        scenario.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS + 1,
    )
    commands = scenario.processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=decision_monotonic,
    )
    assert len(commands) == 1
    assert commands[0].schema == AUDIT_REPLACEMENT_RELEASE_V2_SCHEMA
    assert canonical_json_bytes(commands[0].capture_record["recovery_authority"]) == (
        canonical_json_bytes(complete.to_record())
    )
    release_envelope = _envelope(
        scenario.run_id,
        12,
        CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
        commands[0].to_payload(),
        monotonic_ns=decision_monotonic.nanoseconds,
        connection_id=connection_id,
    )
    released = _append_and_process(scenario, release_envelope)

    assert released.success
    assert len(released.audit_divergences_eligibility_restored) == 1
    restoration = released.audit_divergences_eligibility_restored[0]
    released_recovery = restoration.replacement_recovery_authority
    assert released_recovery is not None
    assert released_recovery.status is ReplacementRecoveryStatus.RELEASED
    assert released_recovery.recovery_id == recovery_id
    assert released_recovery.stable_contributor_key == contributor_key
    assert released_recovery.release_source == released.source
    assert released_recovery.release_ingest_seq == release_envelope.ingest_seq
    assert scenario.processor.summary().audit_replacement_captures_pending == 0
    assert scenario.processor.audit_ineligible_instruments == ()
    _assert_replay_equal(scenario)


@pytest.mark.parametrize(
    "source_commit",
    [
        "a382b93220011428703fe02483f7034f59c1f045",
        "ff7054ac2167026ce1ed14111b7e144cfd4fd865",
        "ee240c51184cccfc6e425906097758b5ca019a95",
        "6dc5dc43bbb673a35ff960bb7d84fed062aefda4",
    ],
)
def test_retained_pre_adr0056_source_replays_v1_replacement_release(
    source_commit: str,
) -> None:
    base = _resolved_prepare_attest_mismatch(
        f"retained-v1-{source_commit[:8]}",
        source_journal_git_commit=source_commit,
        expect_recovery_authority=False,
    )
    processor = CanonicalProcessor(
        base.config,
        source_journal_git_commit=source_commit,
    )
    envelopes = list(base.envelopes)
    assert all(processor.process_replayed(envelope).success for envelope in envelopes)

    captures = processor.summary().audit_replacement_capture_authority
    assert len(captures) == 1
    assert captures[0].recovery_authority is None

    connection_id = ConnectionId(f"retained-v1-{source_commit[:8]}-connection")
    for sequence, payload, ordinal in (
        (9, base.mismatched_yes, 1),
        (10, _no_book(), 2),
    ):
        envelope = _envelope(
            base.run_id,
            sequence,
            WS_MARKET,
            _websocket_book_payload(payload, label=f"retained-v1-{ordinal}"),
            monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + ordinal - 1,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": ordinal},
        )
        envelopes.append(envelope)
        assert processor.process_replayed(envelope).success

    decision = MonotonicTimestamp(
        base.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS + 1,
    )
    commands = processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=decision,
    )
    assert len(commands) == 1
    assert commands[0].schema == AUDIT_REPLACEMENT_RELEASE_V1_SCHEMA
    release = _envelope(
        base.run_id,
        11,
        CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
        commands[0].to_payload(),
        monotonic_ns=decision.nanoseconds,
        connection_id=connection_id,
    )
    envelopes.append(release)
    released = processor.process_replayed(release)

    assert released.success
    assert processor.summary().audit_replacement_captures_pending == 0
    replay = CanonicalProcessor(
        base.config,
        source_journal_git_commit=source_commit,
    )
    assert all(replay.process_replayed(envelope).success for envelope in envelopes)
    assert replay.summary() == processor.summary()
    assert replay.snapshots() == processor.snapshots()


def test_unlisted_source_cannot_select_historical_replacement_capture_protocol() -> None:
    base = _resolved_prepare_attest_mismatch("unlisted-source")
    processor = CanonicalProcessor(
        base.config,
        source_journal_git_commit="6dc5dc43bbb673a35ff960bb7d84fed062aefda40",
    )

    assert all(processor.process_replayed(envelope).success for envelope in base.envelopes)
    captures = processor.summary().audit_replacement_capture_authority
    assert len(captures) == 1
    assert captures[0].recovery_authority is not None


def test_release_v1_fixture_is_legacy_only_and_replays_as_typed_rejection() -> None:
    scenario = _resolved_prepare_attest_mismatch("immutable-v1-release-fixture")
    connection_id = ConnectionId("immutable-v1-release-fixture-connection")
    _start_attempt(scenario, connection_id=connection_id, sequence=9)
    for sequence, payload, ordinal in (
        (10, scenario.mismatched_yes, 1),
        (11, _no_book(), 2),
    ):
        assert _append_and_process(
            scenario,
            _envelope(
                scenario.run_id,
                sequence,
                WS_MARKET,
                _websocket_book_payload(payload, label=f"v1-fixture-{ordinal}"),
                monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + ordinal - 1,
                connection_id=connection_id,
                transport_metadata={"websocket_admission_ordinal": ordinal},
            ),
        ).success

    decision = MonotonicTimestamp(
        scenario.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS + 1,
    )
    current = scenario.processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=decision,
    )[0]
    legacy_capture_record = current.capture_record
    del legacy_capture_record["recovery_authority"]
    fixture = AuditReplacementReleaseCommand.create(
        divergence_id=current.divergence_id,
        instrument_id=current.instrument_id,
        market_id=current.market_id,
        replacement_connection_id=current.replacement_connection_id,
        capture_record=legacy_capture_record,
        monotonic_epoch_id=current.monotonic_epoch_id,
        decision_monotonic_ns=current.decision_monotonic_ns,
        schema=AUDIT_REPLACEMENT_RELEASE_V1_SCHEMA,
    )

    assert fixture.schema == AUDIT_REPLACEMENT_RELEASE_V1_SCHEMA
    assert frozenset(fixture.capture_record) == frozenset(
        {
            "captured_snapshot_sources",
            "divergence_id",
            "market_id",
            "mismatch_cause",
            "original_connection_id",
            "pre_open_connection_count",
            "pre_open_connection_ids_sha256",
            "replacement_connection_id",
            "resolution_ingest_seq",
            "resolution_monotonic",
            "resolution_source",
            "required_instrument_ids",
        }
    )
    with pytest.raises(ValueError, match="capture record shape is invalid"):
        AuditReplacementReleaseCommand.create(
            divergence_id=current.divergence_id,
            instrument_id=current.instrument_id,
            market_id=current.market_id,
            replacement_connection_id=current.replacement_connection_id,
            capture_record=current.capture_record,
            monotonic_epoch_id=current.monotonic_epoch_id,
            decision_monotonic_ns=current.decision_monotonic_ns,
            schema=AUDIT_REPLACEMENT_RELEASE_V1_SCHEMA,
        )
    with pytest.raises(ValueError, match="capture record shape is invalid"):
        AuditReplacementReleaseCommand.create(
            divergence_id=current.divergence_id,
            instrument_id=current.instrument_id,
            market_id=current.market_id,
            replacement_connection_id=current.replacement_connection_id,
            capture_record=legacy_capture_record,
            monotonic_epoch_id=current.monotonic_epoch_id,
            decision_monotonic_ns=current.decision_monotonic_ns,
            schema=AUDIT_REPLACEMENT_RELEASE_V2_SCHEMA,
        )

    rejected = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            12,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            fixture.to_payload(),
            monotonic_ns=decision.nanoseconds,
            connection_id=connection_id,
        ),
    )
    assert not rejected.success
    authority = _current_recovery(scenario)
    assert authority.last_result is ReplacementRecoveryResult.RELEASE_REJECTED
    assert authority.last_failure is ReplacementRecoveryFailure.RELEASE_CONTROL_REJECTED
    _assert_replay_equal(scenario, expected_failures=1)


def test_blocked_release_projection_is_typed_durable_and_later_releasable() -> None:
    scenario = _resolved_prepare_attest_mismatch("release-not-projected")
    connection_id = ConnectionId("replacement-release-not-projected-connection")
    _start_attempt(scenario, connection_id=connection_id, sequence=9)
    for sequence, payload, ordinal in (
        (10, scenario.mismatched_yes, 1),
        (11, _no_book(), 2),
    ):
        assert _append_and_process(
            scenario,
            _envelope(
                scenario.run_id,
                sequence,
                WS_MARKET,
                _websocket_book_payload(payload, label=f"not-projected-{ordinal}"),
                monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + ordinal - 1,
                connection_id=connection_id,
                transport_metadata={"websocket_admission_ordinal": ordinal},
            ),
        ).success

    blocked_at = MonotonicTimestamp(
        scenario.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS + 2,
    )
    transition = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            12,
            WS_MARKET,
            canonical_json_bytes(
                {
                    "asset_id": "token-no",
                    "best_ask": "0.35",
                    "best_bid": "0.31",
                    "event_type": "best_bid_ask",
                    "market": "0xcondition",
                    "spread": "0.04",
                    "timestamp": "1787198402002",
                }
            ),
            monotonic_ns=blocked_at.nanoseconds,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": 3},
        ),
    )
    assert transition.success
    assert scenario.processor.reported_top_transitions
    assert not scenario.processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=blocked_at,
    )
    verdicts = scenario.processor.audit_replacement_release_not_projected_commands(
        connection_id=connection_id,
        decision_monotonic=blocked_at,
    )
    assert len(verdicts) == 1
    assert verdicts[0].action is ReplacementRecoveryLifecycleAction.RELEASE_NOT_PROJECTED
    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            13,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            verdicts[0].to_payload(),
            monotonic_ns=blocked_at.nanoseconds,
            connection_id=connection_id,
        ),
    ).success
    blocked = _current_recovery(scenario)
    assert blocked.status is ReplacementRecoveryStatus.CAPTURE_COMPLETE
    assert blocked.last_result is ReplacementRecoveryResult.RELEASE_NOT_PROJECTED
    assert blocked.last_failure is ReplacementRecoveryFailure.RELEASE_CONTROL_NOT_PROJECTED
    assert not scenario.processor.audit_replacement_release_not_projected_commands(
        connection_id=connection_id,
        decision_monotonic=blocked_at,
    )
    _assert_replay_equal(scenario)

    released_at = MonotonicTimestamp(scenario.run_id.value, blocked_at.nanoseconds + 1)
    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            14,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="not-projected-reconciled"),
            monotonic_ns=released_at.nanoseconds,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": 4},
        ),
    ).success
    assert not scenario.processor.reported_top_transitions
    release_commands = scenario.processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=released_at,
    )
    assert len(release_commands) == 1
    assert release_commands[0].schema == AUDIT_REPLACEMENT_RELEASE_V2_SCHEMA
    released = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            15,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
            release_commands[0].to_payload(),
            monotonic_ns=released_at.nanoseconds,
            connection_id=connection_id,
        ),
    )
    assert released.success
    released_authority = released.audit_divergences_eligibility_restored[
        0
    ].replacement_recovery_authority
    assert released_authority is not None
    assert released_authority.status is ReplacementRecoveryStatus.RELEASED
    _assert_replay_equal(scenario)


@pytest.mark.parametrize(
    "cancelled_status",
    [
        ReplacementRecoveryStatus.ATTEMPT_ACTIVE,
        ReplacementRecoveryStatus.RETRY_SCHEDULED,
    ],
)
@pytest.mark.asyncio
async def test_observe_cancellation_shields_terminal_control_and_replays(
    cancelled_status: ReplacementRecoveryStatus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = _resolved_prepare_attest_mismatch(f"cancel-{cancelled_status.value.lower()}")
    capture = _LifecycleCapture(scenario)
    health = _LifecycleHealth()
    service = PolymarketObservationService(
        scenario.config,
        workspace_root=Path("."),
        code_commit="fixture",
    )
    generation_ready = asyncio.Event()
    terminal_cutoffs: list[observe_module._CaptureCutoffMoment] = []

    def freeze_terminal_cutoff(cutoff: observe_module._CaptureCutoffMoment) -> None:
        authority = _current_recovery(scenario)
        assert authority.terminal
        assert authority.unresolved
        assert scenario.envelopes[-1].received_monotonic.nanoseconds <= cutoff.monotonic_ns
        terminal_cutoffs.append(cutoff)
        health.events.append(f"cutoff:{authority.status.value}")

    async def cancellable_generation(
        generation_service: PolymarketObservationService,
        **kwargs: object,
    ) -> observe_module._GenerationResult:
        connection_id = cast(ConnectionId, kwargs["connection_id"])
        subscription_id = kwargs["subscription_id"]
        generation_capture = cast(observe_module._RawCaptureBoundary, kwargs["capture"])
        runtime_health = cast(ObservationRuntimeHealth, kwargs["runtime_health"])
        started = MonotonicTimestamp(scenario.run_id.value, 400)
        await generation_service._process_audit_replacement_recovery_commands(
            capture=generation_capture,
            commands=scenario.processor.audit_replacement_attempt_start_commands(
                connection_id=connection_id,
                decision_monotonic=started,
            ),
            decision_at=UtcTimestamp.now(),
            connection_id=connection_id,
            subscription_id=cast(SubscriptionId, subscription_id),
            runtime_health=runtime_health,
        )
        if cancelled_status is ReplacementRecoveryStatus.RETRY_SCHEDULED:
            abandoned = MonotonicTimestamp(scenario.run_id.value, 401)
            await generation_service._process_audit_replacement_recovery_commands(
                capture=generation_capture,
                commands=scenario.processor.audit_replacement_attempt_abandon_commands(
                    connection_id=connection_id,
                    decision_monotonic=abandoned,
                    failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
                ),
                decision_at=UtcTimestamp.now(),
                connection_id=connection_id,
                subscription_id=cast(SubscriptionId, subscription_id),
                runtime_health=runtime_health,
            )
        assert _current_recovery(scenario).status is cancelled_status
        generation_ready.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled generation unexpectedly returned")

    monkeypatch.setattr(observe_module.time, "monotonic_ns", lambda: 450)
    monkeypatch.setattr(
        PolymarketObservationService,
        "_run_generation",
        cancellable_generation,
    )
    task = asyncio.create_task(
        service._observe_websocket(
            client=cast(PolymarketPublicClient, object()),
            capture=cast(observe_module._RawCaptureBoundary, capture),
            selected=(),
            asset_ids=(),
            deadline=asyncio.get_running_loop().time() + 30.0,
            logger=cast(StructuredLogger, _LifecycleLogger()),
            runtime_health=cast(ObservationRuntimeHealth, health),
            on_capture_cutoff=freeze_terminal_cutoff,
        )
    )
    await asyncio.wait_for(generation_ready.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)

    terminal = _current_recovery(scenario)
    assert terminal.status is ReplacementRecoveryStatus.TERMINAL_UNRESOLVED
    assert terminal.last_result is ReplacementRecoveryResult.TERMINAL_UNRESOLVED
    assert terminal.last_failure is ReplacementRecoveryFailure.TERMINAL_CUTOFF
    assert len(terminal_cutoffs) == 1
    assert health.events[-3].split(":")[2] == terminal.status.value
    assert health.events[-2] == "finish_generation"
    assert health.events[-1] == f"cutoff:{terminal.status.value}"
    _assert_replay_equal(scenario)


def test_connection_change_retries_on_new_connection_without_multiplying_identity() -> None:
    scenario = _resolved_prepare_attest_mismatch("connection-change-retry")
    initial = _current_recovery(scenario)
    recovery_id = initial.recovery_id
    contributor_key = initial.stable_contributor_key
    first_connection = ConnectionId("replacement-interrupted-connection")
    retry_connection = ConnectionId("replacement-retry-connection")
    _start_attempt(scenario, connection_id=first_connection, sequence=9)

    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            10,
            WS_MARKET,
            _websocket_book_payload(scenario.mismatched_yes, label="partial-yes"),
            monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS,
            connection_id=first_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        ),
    ).success
    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            11,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            scenario.processor.audit_replacement_attempt_abandon_commands(
                connection_id=first_connection,
                decision_monotonic=MonotonicTimestamp(
                    scenario.run_id.value,
                    _FIRST_REPLACEMENT_MONOTONIC_NS + 1,
                ),
                failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
            )[0].to_payload(),
            monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + 1,
            connection_id=first_connection,
        ),
    ).success

    scheduled = _current_recovery(scenario)
    assert scheduled.status is ReplacementRecoveryStatus.RETRY_SCHEDULED
    assert scheduled.last_failure is ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED
    assert scheduled.attempted_connection_ids == (first_connection,)
    assert len(scheduled.captured_snapshot_sources) == 1
    assert scheduled.recovery_id == recovery_id
    assert scheduled.stable_contributor_key == contributor_key
    retry_at = scheduled.next_retry_monotonic
    assert retry_at is not None

    premature = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            12,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="retry-not-due"),
            monotonic_ns=retry_at.nanoseconds - 1,
            connection_id=retry_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        ),
    )
    assert premature.success
    assert premature.scan_count == 0
    assert _current_recovery(scenario) == scheduled

    start_commands = scenario.processor.audit_replacement_attempt_start_commands(
        connection_id=retry_connection,
        decision_monotonic=retry_at,
    )
    assert len(start_commands) == 1
    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            13,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            start_commands[0].to_payload(),
            monotonic_ns=retry_at.nanoseconds,
            connection_id=retry_connection,
        ),
    ).success
    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            14,
            WS_MARKET,
            _websocket_book_payload(scenario.mismatched_yes, label="retry-yes"),
            monotonic_ns=retry_at.nanoseconds,
            connection_id=retry_connection,
            transport_metadata={"websocket_admission_ordinal": 2},
        ),
    ).success
    retried = _current_recovery(scenario)
    assert retried.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert retried.attempt_count == 2
    assert retried.attempted_connection_ids == (first_connection, retry_connection)
    assert tuple(item.connection_id for item in retried.captured_snapshot_sources) == (
        retry_connection,
    )
    assert retried.recovery_id == recovery_id
    assert retried.stable_contributor_key == contributor_key

    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            15,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="retry-no"),
            monotonic_ns=retry_at.nanoseconds + 1,
            connection_id=retry_connection,
            transport_metadata={"websocket_admission_ordinal": 3},
        ),
    ).success
    complete = _current_recovery(scenario)
    assert complete.status is ReplacementRecoveryStatus.CAPTURE_COMPLETE
    assert complete.captured_instrument_ids == complete.required_instrument_ids
    assert complete.recovery_id == recovery_id
    assert complete.stable_contributor_key == contributor_key

    decision = MonotonicTimestamp(scenario.run_id.value, retry_at.nanoseconds + 1)
    commands = scenario.processor.audit_replacement_release_commands(
        connection_id=retry_connection,
        decision_monotonic=decision,
    )
    assert len(commands) == 1
    released = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            16,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
            commands[0].to_payload(),
            monotonic_ns=decision.nanoseconds,
            connection_id=retry_connection,
        ),
    )
    assert released.success
    released_recovery = released.audit_divergences_eligibility_restored[
        0
    ].replacement_recovery_authority
    assert released_recovery is not None
    assert released_recovery.status is ReplacementRecoveryStatus.RELEASED
    assert released_recovery.attempt_count == 2
    assert released_recovery.recovery_id == recovery_id
    assert released_recovery.stable_contributor_key == contributor_key
    _assert_replay_equal(scenario)


@pytest.mark.asyncio
async def test_hard_deadline_exhaustion_is_control_owned_and_runtime_mirrored() -> None:
    config = _current_config(hard_deadline_seconds=6.0)
    scenario = _resolved_prepare_attest_mismatch(
        "hard-deadline-exhaustion",
        config=config,
    )
    initial = _current_recovery(scenario)
    recovery_id = initial.recovery_id
    contributor_key = initial.stable_contributor_key
    hard_deadline_ns = initial.hard_deadline.nanoseconds
    connection_id = ConnectionId("replacement-after-hard-deadline")
    subscription_id = SubscriptionId("replacement-hard-deadline-supervisor")
    capture = _LifecycleCapture(scenario)
    health = _LifecycleHealth()
    service = PolymarketObservationService(
        scenario.config,
        workspace_root=Path("."),
        code_commit="fixture",
    )

    started = MonotonicTimestamp(scenario.run_id.value, hard_deadline_ns - 1)
    start_commands = scenario.processor.audit_replacement_attempt_start_commands(
        connection_id=connection_id,
        decision_monotonic=started,
    )
    assert len(start_commands) == 1
    await service._process_audit_replacement_recovery_commands(
        capture=cast(observe_module._RawCaptureBoundary, capture),
        commands=start_commands,
        decision_at=UtcTimestamp.now(),
        connection_id=connection_id,
        subscription_id=subscription_id,
        runtime_health=cast(ObservationRuntimeHealth, health),
    )
    active = _current_recovery(scenario)
    assert active.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert health.authorities[-1] == active

    at_deadline = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            11,
            WS_MARKET,
            _websocket_book_payload(scenario.mismatched_yes, label="at-hard-deadline"),
            monotonic_ns=hard_deadline_ns,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": 1},
        ),
    )
    assert at_deadline.success
    assert at_deadline.scan_count == 0
    rejected = _current_recovery(scenario)
    assert rejected.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert rejected.last_result is ReplacementRecoveryResult.SNAPSHOT_REJECTED
    assert rejected.last_failure is ReplacementRecoveryFailure.HARD_DEADLINE_REACHED
    assert not rejected.terminal
    service._bind_current_audit_replacement_recoveries(
        capture=cast(observe_module._RawCaptureBoundary, capture),
        runtime_health=cast(ObservationRuntimeHealth, health),
    )
    assert health.authorities[-1] == rejected

    retry_rotation = await service._process_due_audit_replacement_recoveries(
        capture=cast(observe_module._RawCaptureBoundary, capture),
        connection_id=connection_id,
        subscription_id=subscription_id,
        runtime_health=cast(ObservationRuntimeHealth, health),
        decision_at=UtcTimestamp.now(),
        decision_monotonic_ns=hard_deadline_ns,
    )
    assert not retry_rotation
    exhausted = _current_recovery(scenario)
    assert exhausted.status is ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED
    assert exhausted.last_failure is ReplacementRecoveryFailure.HARD_DEADLINE_REACHED
    assert exhausted.terminal
    assert exhausted.unresolved
    assert exhausted.attempt_count == 1
    assert exhausted.recovery_id == recovery_id
    assert exhausted.stable_contributor_key == contributor_key
    assert health.authorities[-1] == exhausted
    exhausted_record_hash = canonical_sha256(exhausted.to_record())

    after_deadline = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            13,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="after-hard-deadline"),
            monotonic_ns=hard_deadline_ns + 1,
            connection_id=connection_id,
            transport_metadata={"websocket_admission_ordinal": 2},
        ),
    )
    assert after_deadline.success
    assert after_deadline.scan_count == 0
    assert canonical_sha256(_current_recovery(scenario).to_record()) == exhausted_record_hash
    assert (
        scenario.processor.audit_replacement_release_commands(
            connection_id=connection_id,
            decision_monotonic=MonotonicTimestamp(
                scenario.run_id.value,
                hard_deadline_ns + 1,
            ),
        )
        == ()
    )
    _assert_replay_equal(scenario)

    audit_hard_expiry_ns = hard_deadline_ns + int(
        scenario.config.scanner.maximum_book_age_seconds * 1_000_000_000
    )
    _, blocked_audit = _audit_start(
        scenario.processor,
        scenario.run_id,
        14,
        request_id="audit-after-replacement-exhaustion",
        request_started_monotonic_ns=hard_deadline_ns + 2,
        hard_expiry_monotonic_ns=audit_hard_expiry_ns,
        response_deadline_monotonic_ns=min(
            audit_hard_expiry_ns,
            scenario.run_deadline_ns,
        ),
        run_deadline_monotonic_ns=scenario.run_deadline_ns,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    before_audit = scenario.processor.summary()
    blocked = scenario.processor.process_replayed(blocked_audit)
    after_audit = scenario.processor.summary()

    assert not blocked.success
    assert blocked.error_type == "NormalizationError"
    assert blocked.audit_outcomes == ()
    assert blocked.scan_count == 0
    assert after_audit.rest_audits == before_audit.rest_audits
    assert after_audit.pending_rest_audits == 0
    assert canonical_sha256(_current_recovery(scenario).to_record()) == exhausted_record_hash
    assert scenario.processor.summary().audit_replacement_captures_pending == 1
    assert scenario.processor.audit_ineligible_instruments


def test_idle_no_frame_deadline_control_exhausts_and_replays() -> None:
    scenario = _resolved_prepare_attest_mismatch(
        "idle-no-frame-deadline",
        config=_current_config(hard_deadline_seconds=6.0),
    )
    initial = _current_recovery(scenario)
    deadline = initial.hard_deadline
    commands = scenario.processor.audit_replacement_deadline_commands(decision_monotonic=deadline)

    assert len(commands) == 1
    exhausted = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            9,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            commands[0].to_payload(),
            monotonic_ns=deadline.nanoseconds,
            connection_id=ConnectionId("idle-supervisor-control"),
        ),
    )

    assert exhausted.success
    authority = _current_recovery(scenario)
    assert authority.status is ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED
    assert authority.last_failure is ReplacementRecoveryFailure.HARD_DEADLINE_REACHED
    assert authority.attempt_count == 0
    assert authority.attempted_connection_ids == ()
    assert authority.captured_snapshot_sources == ()
    _assert_replay_equal(scenario)


def test_terminal_cutoff_control_freezes_pending_authority_and_replays() -> None:
    scenario = _resolved_prepare_attest_mismatch("terminal-cutoff")
    cutoff = MonotonicTimestamp(
        scenario.run_id.value,
        _RESOLUTION_MONOTONIC_NS + 1,
    )
    commands = scenario.processor.audit_replacement_terminal_commands(decision_monotonic=cutoff)

    assert len(commands) == 1
    result = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            9,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            commands[0].to_payload(),
            monotonic_ns=cutoff.nanoseconds,
            connection_id=ConnectionId("terminal-cutoff-control"),
        ),
    )

    assert result.success
    authority = _current_recovery(scenario)
    assert authority.status is ReplacementRecoveryStatus.TERMINAL_UNRESOLVED
    assert authority.last_failure is ReplacementRecoveryFailure.TERMINAL_CUTOFF
    assert authority.unresolved
    assert authority.terminal
    assert (
        scenario.processor.audit_replacement_release_commands(
            connection_id=ConnectionId("terminal-cutoff-control"),
            decision_monotonic=cutoff,
        )
        == ()
    )
    _assert_replay_equal(scenario)


def test_cross_epoch_full_snapshot_is_typed_rejection_not_synthetic_terminal() -> None:
    scenario = _resolved_prepare_attest_mismatch("cross-epoch-source")
    connection_id = ConnectionId("cross-epoch-attempt")
    started = MonotonicTimestamp(
        scenario.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS,
    )
    commands = scenario.processor.audit_replacement_attempt_start_commands(
        connection_id=connection_id,
        decision_monotonic=started,
    )
    assert len(commands) == 1
    assert _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            9,
            CONTROL_REST_AUDIT_REPLACEMENT_RECOVERY,
            commands[0].to_payload(),
            monotonic_ns=started.nanoseconds,
            connection_id=connection_id,
        ),
    ).success

    cross_run = CollectorRunId("replacement-recovery-cross-epoch-input")
    cross_epoch = _envelope(
        cross_run,
        10,
        WS_MARKET,
        _websocket_book_payload(scenario.mismatched_yes, label="cross-epoch-full"),
        monotonic_ns=started.nanoseconds + 1,
        connection_id=connection_id,
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    result = _append_and_process(scenario, cross_epoch)

    assert result.success
    authority = _current_recovery(scenario)
    assert authority.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert authority.last_failure is ReplacementRecoveryFailure.CROSS_EPOCH_SOURCE
    assert authority.captured_snapshot_sources == ()
    assert not authority.terminal
    _assert_replay_equal(scenario)


def test_current_release_parser_rejects_forged_stable_recovery_identity() -> None:
    scenario = _resolved_prepare_attest_mismatch("forged-recovery-identity")
    connection_id = ConnectionId("replacement-forged-identity-connection")
    _start_attempt(scenario, connection_id=connection_id, sequence=9)
    for sequence, payload, ordinal in (
        (10, scenario.mismatched_yes, 1),
        (11, _no_book(), 2),
    ):
        result = _append_and_process(
            scenario,
            _envelope(
                scenario.run_id,
                sequence,
                WS_MARKET,
                _websocket_book_payload(payload, label=f"forgery-source-{ordinal}"),
                monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + ordinal - 1,
                connection_id=connection_id,
                transport_metadata={"websocket_admission_ordinal": ordinal},
            ),
        )
        assert result.success

    decision = MonotonicTimestamp(
        scenario.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS + 1,
    )
    commands = scenario.processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=decision,
    )
    assert len(commands) == 1
    valid = commands[0]
    forged_record = valid.capture_record
    recovery_record = forged_record["recovery_authority"]
    assert isinstance(recovery_record, dict)
    recovery_record["stable_contributor_key"] = "0" * 64

    with pytest.raises(
        ValueError,
        match="audit replacement release recovery authority is invalid",
    ):
        AuditReplacementReleaseCommand.create(
            divergence_id=valid.divergence_id,
            instrument_id=valid.instrument_id,
            market_id=valid.market_id,
            replacement_connection_id=valid.replacement_connection_id,
            capture_record=forged_record,
            monotonic_epoch_id=valid.monotonic_epoch_id,
            decision_monotonic_ns=valid.decision_monotonic_ns,
        )


def test_attributable_release_control_rejection_is_typed_and_replayable() -> None:
    scenario = _resolved_prepare_attest_mismatch("typed-release-rejection")
    connection_id = ConnectionId("replacement-release-rejection-connection")
    _start_attempt(scenario, connection_id=connection_id, sequence=9)
    for sequence, payload, ordinal in (
        (10, scenario.mismatched_yes, 1),
        (11, _no_book(), 2),
    ):
        assert _append_and_process(
            scenario,
            _envelope(
                scenario.run_id,
                sequence,
                WS_MARKET,
                _websocket_book_payload(payload, label=f"release-reject-{ordinal}"),
                monotonic_ns=_FIRST_REPLACEMENT_MONOTONIC_NS + ordinal - 1,
                connection_id=connection_id,
                transport_metadata={"websocket_admission_ordinal": ordinal},
            ),
        ).success

    decision = MonotonicTimestamp(
        scenario.run_id.value,
        _FIRST_REPLACEMENT_MONOTONIC_NS + 1,
    )
    commands = scenario.processor.audit_replacement_release_commands(
        connection_id=connection_id,
        decision_monotonic=decision,
    )
    assert len(commands) == 1
    rejected = _append_and_process(
        scenario,
        _envelope(
            scenario.run_id,
            12,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE_V2,
            commands[0].to_payload(),
            monotonic_ns=decision.nanoseconds,
            connection_id=ConnectionId("wrong-release-control-connection"),
        ),
    )

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.scan_count == 0
    authority = _current_recovery(scenario)
    assert authority.status is ReplacementRecoveryStatus.CAPTURE_COMPLETE
    assert authority.last_result is ReplacementRecoveryResult.RELEASE_REJECTED
    assert authority.last_failure is ReplacementRecoveryFailure.RELEASE_CONTROL_REJECTED
    assert authority.unresolved
    assert scenario.processor.audit_ineligible_instruments
    _assert_replay_equal(scenario, expected_failures=1)
