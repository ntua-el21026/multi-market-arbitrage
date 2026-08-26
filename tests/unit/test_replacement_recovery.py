from __future__ import annotations

from collections.abc import Callable

import pytest
from hypothesis import given
from hypothesis import strategies as st

from multi_market_arbitrage.core.ids import ConnectionId, IngestId, InstrumentId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp
from multi_market_arbitrage.pipeline.replacement_recovery import (
    MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS,
    MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS,
    MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS,
    MAXIMUM_REPLACEMENT_RECOVERY_NANOSECONDS,
    MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS,
    MAXIMUM_REPLACEMENT_RECOVERY_RETRY_DELAY_NS,
    ReplacementRecoveryAuthority,
    ReplacementRecoveryBounds,
    ReplacementRecoveryFailure,
    ReplacementRecoveryResult,
    ReplacementRecoveryStatus,
    ReplacementRecoveryTransitionError,
    ReplacementSnapshotCandidate,
)

EPOCH = "run-epoch"
YES = InstrumentId("instrument-yes")
NO = InstrumentId("instrument-no")
ORIGINAL = ConnectionId("connection-original")
EXCLUDED = ConnectionId("connection-before-resolution")
ATTEMPT_1 = ConnectionId("connection-attempt-1")
ATTEMPT_2 = ConnectionId("connection-attempt-2")
DIVERGENCE_ID = "d" * 64


def _mono(value: int, *, epoch: str = EPOCH) -> MonotonicTimestamp:
    return MonotonicTimestamp(epoch, value)


def _source(label: str) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=IngestId(f"ingest-{label}"),
        payload_sha256=canonical_sha256({"source": label}),
    )


def _authority(
    *,
    maximum_attempts: int = 3,
    retry_delay_ns: int = 10,
    maximum_snapshot_age_ns: int = 20,
    hard_deadline_ns: int = 1_000,
    run_deadline_ns: int = 2_000,
) -> ReplacementRecoveryAuthority:
    return ReplacementRecoveryAuthority.start(
        divergence_id=DIVERGENCE_ID,
        resolution_source=_source("resolution"),
        resolution_ingest_seq=10,
        resolution_monotonic=_mono(100),
        original_connection_id=ORIGINAL,
        excluded_connection_ids=(EXCLUDED,),
        required_instrument_ids=(NO, YES),
        bounds=ReplacementRecoveryBounds(
            maximum_attempts=maximum_attempts,
            retry_delay_ns=retry_delay_ns,
            maximum_snapshot_age_ns=maximum_snapshot_age_ns,
        ),
        hard_deadline=_mono(hard_deadline_ns),
        run_deadline=_mono(run_deadline_ns),
    )


def _active(**kwargs: int) -> ReplacementRecoveryAuthority:
    return _authority(**kwargs).start_attempt(
        connection_id=ATTEMPT_1,
        started_monotonic=_mono(110),
    )


def _candidate(
    *,
    connection_id: ConnectionId = ATTEMPT_1,
    instrument_ids: tuple[InstrumentId, ...] = (YES, NO),
    admission_ordinal: int | None = 1,
    ingest_seq: int = 11,
    received_ns: int = 111,
    epoch: str = EPOCH,
    label: str = "candidate",
) -> ReplacementSnapshotCandidate:
    return ReplacementSnapshotCandidate(
        connection_id=connection_id,
        instrument_ids=instrument_ids,
        admission_ordinal=admission_ordinal,
        ingest_seq=ingest_seq,
        received_monotonic=_mono(received_ns, epoch=epoch),
        raw_source=_source(label),
    )


def test_start_is_deterministic_and_does_not_silently_start_attempt() -> None:
    first = _authority()
    second = _authority()

    assert first == second
    assert first.status is ReplacementRecoveryStatus.READY
    assert first.attempt_count == 0
    assert first.current_connection_id is None
    assert first.first_attempt_monotonic is None
    assert first.next_retry_monotonic is None
    assert first.last_result is ReplacementRecoveryResult.RECOVERY_STARTED
    assert first.unresolved
    assert first.retry_allowed
    assert canonical_sha256(first.to_record()) == canonical_sha256(second.to_record())


def test_attempt_start_preserves_stable_recovery_and_contributor_identity() -> None:
    initial = _authority()
    active = initial.start_attempt(
        connection_id=ATTEMPT_1,
        started_monotonic=_mono(110),
    )

    assert active.recovery_id == initial.recovery_id
    assert active.stable_contributor_key == initial.stable_contributor_key
    assert active.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert active.attempt_count == 1
    assert active.attempted_connection_ids == (ATTEMPT_1,)
    assert active.first_attempt_monotonic == _mono(110)
    assert active.latest_attempt_monotonic == _mono(110)
    assert active.current_connection_id == ATTEMPT_1
    assert active.last_result is ReplacementRecoveryResult.ATTEMPT_STARTED
    assert active.evidence_chain_sha256 != initial.evidence_chain_sha256
    assert active.attempt_chain_sha256 != initial.attempt_chain_sha256


@pytest.mark.parametrize(
    ("candidate_factory", "observed", "expected_failure"),
    [
        (
            lambda: _candidate(ingest_seq=10),
            _mono(112),
            ReplacementRecoveryFailure.PRE_RESOLUTION_SOURCE,
        ),
        (
            lambda: _candidate(received_ns=99),
            _mono(112),
            ReplacementRecoveryFailure.PRE_RESOLUTION_SOURCE,
        ),
        (
            lambda: _candidate(received_ns=109),
            _mono(112),
            ReplacementRecoveryFailure.PRE_ATTEMPT_SOURCE,
        ),
        (
            lambda: _candidate(connection_id=ORIGINAL),
            _mono(112),
            ReplacementRecoveryFailure.ORIGINAL_CONNECTION_REUSED,
        ),
        (
            lambda: _candidate(connection_id=EXCLUDED),
            _mono(112),
            ReplacementRecoveryFailure.DISALLOWED_CONNECTION,
        ),
        (
            lambda: _candidate(connection_id=ConnectionId("another-new-connection")),
            _mono(112),
            ReplacementRecoveryFailure.DISALLOWED_CONNECTION,
        ),
        (
            lambda: _candidate(received_ns=111),
            _mono(132),
            ReplacementRecoveryFailure.STALE_SOURCE,
        ),
        (
            lambda: _candidate(received_ns=113),
            _mono(112),
            ReplacementRecoveryFailure.FUTURE_SOURCE,
        ),
        (
            lambda: _candidate(epoch="another-epoch"),
            _mono(112),
            ReplacementRecoveryFailure.CROSS_EPOCH_SOURCE,
        ),
        (
            lambda: _candidate(),
            _mono(112, epoch="another-epoch"),
            ReplacementRecoveryFailure.CROSS_EPOCH_SOURCE,
        ),
        (
            lambda: _candidate(admission_ordinal=None),
            _mono(112),
            ReplacementRecoveryFailure.MISSING_ADMISSION_ORDINAL,
        ),
        (
            lambda: _candidate(admission_ordinal=0),
            _mono(112),
            ReplacementRecoveryFailure.MISSING_ADMISSION_ORDINAL,
        ),
        (
            lambda: _candidate(instrument_ids=(InstrumentId("unrelated"),)),
            _mono(112),
            ReplacementRecoveryFailure.UNRELATED_INSTRUMENT,
        ),
    ],
)
def test_snapshot_rejection_vocabulary_is_typed_and_fail_closed(
    candidate_factory: Callable[[], ReplacementSnapshotCandidate],
    observed: MonotonicTimestamp,
    expected_failure: ReplacementRecoveryFailure,
) -> None:
    active = _active()
    rejected = active.observe_snapshot(
        candidate=candidate_factory(),
        observed_monotonic=observed,
    )

    assert rejected.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert rejected.last_result is ReplacementRecoveryResult.SNAPSHOT_REJECTED
    assert rejected.last_failure is expected_failure
    assert rejected.captured_snapshot_sources == ()
    assert rejected.attempt_count == active.attempt_count
    assert rejected.recovery_id == active.recovery_id
    assert rejected.stable_contributor_key == active.stable_contributor_key
    assert rejected.evidence_event_count == active.evidence_event_count + 1
    assert rejected.evidence_chain_sha256 != active.evidence_chain_sha256


def test_partial_coverage_cannot_succeed_or_release() -> None:
    active = _active()
    partial = active.observe_snapshot(
        candidate=_candidate(instrument_ids=(YES,)),
        observed_monotonic=_mono(112),
    )

    assert partial.last_result is ReplacementRecoveryResult.PARTIAL_COVERAGE
    assert partial.last_failure is ReplacementRecoveryFailure.PARTIAL_REQUIRED_SET
    assert partial.captured_instrument_ids == (YES,)
    assert not partial.coverage_complete

    rejected_success = partial.succeed_attempt(decided_monotonic=_mono(113))
    assert rejected_success.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert rejected_success.last_result is ReplacementRecoveryResult.CAPTURE_SUCCESS_REJECTED
    assert rejected_success.last_failure is ReplacementRecoveryFailure.PARTIAL_REQUIRED_SET
    with pytest.raises(ReplacementRecoveryTransitionError, match="successful complete capture"):
        rejected_success.release(
            release_source=_source("release"),
            release_ingest_seq=13,
            release_monotonic=_mono(114),
        )


def test_complete_binary_coverage_requires_explicit_success_and_durable_release() -> None:
    active = _active()
    covered = active.observe_snapshot(
        candidate=_candidate(),
        observed_monotonic=_mono(112),
    )

    assert covered.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert covered.coverage_complete
    assert covered.last_result is ReplacementRecoveryResult.SNAPSHOT_ACCEPTED
    with pytest.raises(ReplacementRecoveryTransitionError, match="successful complete capture"):
        covered.release(
            release_source=_source("release-too-early"),
            release_ingest_seq=12,
            release_monotonic=_mono(113),
        )

    succeeded = covered.succeed_attempt(decided_monotonic=_mono(113))
    assert succeeded.status is ReplacementRecoveryStatus.CAPTURE_COMPLETE
    assert succeeded.unresolved

    released = succeeded.release(
        release_source=_source("release"),
        release_ingest_seq=12,
        release_monotonic=_mono(114),
    )
    assert released.status is ReplacementRecoveryStatus.RELEASED
    assert released.terminal
    assert not released.unresolved
    assert not released.retry_allowed
    assert released.release_source == _source("release")
    assert released.release_ingest_seq == 12
    assert released.release_monotonic == _mono(114)
    with pytest.raises(ReplacementRecoveryTransitionError, match="terminal"):
        released.start_attempt(
            connection_id=ATTEMPT_2,
            started_monotonic=_mono(120),
        )


def test_stale_complete_capture_is_abandoned_instead_of_released() -> None:
    succeeded = (
        _active(maximum_snapshot_age_ns=20)
        .observe_snapshot(
            candidate=_candidate(received_ns=111),
            observed_monotonic=_mono(112),
        )
        .succeed_attempt(decided_monotonic=_mono(113))
    )
    abandoned = succeeded.release(
        release_source=_source("stale-release"),
        release_ingest_seq=12,
        release_monotonic=_mono(132),
    )

    assert abandoned.status is ReplacementRecoveryStatus.ATTEMPT_ABANDONED
    assert abandoned.last_failure is ReplacementRecoveryFailure.FRESHNESS_EXPIRED
    assert abandoned.release_source is None
    assert abandoned.unresolved


def test_non_monotonic_snapshot_cannot_replace_current_attempt_evidence() -> None:
    first = _active().observe_snapshot(
        candidate=_candidate(
            instrument_ids=(YES,),
            admission_ordinal=2,
            ingest_seq=12,
            received_ns=112,
            label="newer",
        ),
        observed_monotonic=_mono(113),
    )
    rejected = first.observe_snapshot(
        candidate=_candidate(
            instrument_ids=(YES,),
            admission_ordinal=1,
            ingest_seq=11,
            received_ns=111,
            label="older",
        ),
        observed_monotonic=_mono(114),
    )

    assert rejected.last_failure is ReplacementRecoveryFailure.NON_MONOTONIC_SOURCE
    assert rejected.captured_snapshot_sources == first.captured_snapshot_sources


def test_abandon_schedule_and_retry_clear_capture_but_preserve_identity() -> None:
    initial = _active()
    partial = initial.observe_snapshot(
        candidate=_candidate(instrument_ids=(YES,)),
        observed_monotonic=_mono(112),
    )
    abandoned = partial.abandon_attempt(
        failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
        abandoned_monotonic=_mono(120),
        source=_source("disconnect"),
    )
    scheduled = abandoned.schedule_retry(decided_monotonic=_mono(121))

    assert scheduled.status is ReplacementRecoveryStatus.RETRY_SCHEDULED
    assert scheduled.next_retry_monotonic == _mono(131)
    assert scheduled.next_action_deadline == _mono(131)
    with pytest.raises(ReplacementRecoveryTransitionError, match="scheduled retry"):
        scheduled.start_attempt(
            connection_id=ATTEMPT_2,
            started_monotonic=_mono(130),
        )

    retried = scheduled.start_attempt(
        connection_id=ATTEMPT_2,
        started_monotonic=_mono(131),
    )
    assert retried.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert retried.attempt_count == 2
    assert retried.attempted_connection_ids == (ATTEMPT_1, ATTEMPT_2)
    assert retried.first_attempt_monotonic == _mono(110)
    assert retried.latest_attempt_monotonic == _mono(131)
    assert retried.next_retry_monotonic is None
    assert retried.captured_snapshot_sources == ()
    assert retried.recovery_id == initial.recovery_id
    assert retried.stable_contributor_key == initial.stable_contributor_key


@pytest.mark.parametrize("connection_id", [ORIGINAL, EXCLUDED, ATTEMPT_1])
def test_retry_rejects_original_excluded_and_already_attempted_connections(
    connection_id: ConnectionId,
) -> None:
    scheduled = (
        _active()
        .abandon_attempt(
            failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
            abandoned_monotonic=_mono(120),
            source=None,
        )
        .schedule_retry(decided_monotonic=_mono(121))
    )
    with pytest.raises(ReplacementRecoveryTransitionError, match="new permitted connection"):
        scheduled.start_attempt(
            connection_id=connection_id,
            started_monotonic=_mono(131),
        )


def test_maximum_attempt_exhaustion_stops_retries_and_stays_unresolved() -> None:
    first = _active(maximum_attempts=2)
    second = (
        first.abandon_attempt(
            failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
            abandoned_monotonic=_mono(120),
            source=None,
        )
        .schedule_retry(decided_monotonic=_mono(121))
        .start_attempt(connection_id=ATTEMPT_2, started_monotonic=_mono(131))
    )
    exhausted = second.abandon_attempt(
        failure=ReplacementRecoveryFailure.FRESHNESS_EXPIRED,
        abandoned_monotonic=_mono(140),
        source=None,
    ).schedule_retry(decided_monotonic=_mono(141))

    assert exhausted.status is ReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED
    assert exhausted.last_result is ReplacementRecoveryResult.MAXIMUM_ATTEMPTS_EXHAUSTED
    assert exhausted.last_failure is ReplacementRecoveryFailure.MAXIMUM_ATTEMPTS_REACHED
    assert exhausted.attempt_count == 2
    assert exhausted.terminal
    assert exhausted.unresolved
    assert not exhausted.retry_allowed
    with pytest.raises(ReplacementRecoveryTransitionError, match="terminal"):
        exhausted.start_attempt(
            connection_id=ConnectionId("connection-attempt-3"),
            started_monotonic=_mono(150),
        )


def test_hard_deadline_snapshot_rejects_until_control_owned_exhaustion() -> None:
    active = _active(hard_deadline_ns=150, run_deadline_ns=200)
    rejected = active.observe_snapshot(
        candidate=_candidate(received_ns=149),
        observed_monotonic=_mono(150),
    )

    assert rejected.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert rejected.last_result is ReplacementRecoveryResult.SNAPSHOT_REJECTED
    assert rejected.last_failure is ReplacementRecoveryFailure.HARD_DEADLINE_REACHED
    assert not rejected.terminal
    exhausted = rejected.exhaust_hard_deadline(at=_mono(150))
    assert exhausted.status is ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED
    assert exhausted.last_failure is ReplacementRecoveryFailure.HARD_DEADLINE_REACHED
    assert exhausted.terminal
    assert exhausted.unresolved


def test_run_deadline_snapshot_rejects_until_control_owned_exhaustion() -> None:
    active = _active(hard_deadline_ns=200, run_deadline_ns=140)
    rejected = active.observe_snapshot(
        candidate=_candidate(received_ns=139),
        observed_monotonic=_mono(140),
    )

    assert rejected.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE
    assert rejected.last_result is ReplacementRecoveryResult.SNAPSHOT_REJECTED
    assert rejected.last_failure is ReplacementRecoveryFailure.RUN_DEADLINE_REACHED
    assert not rejected.terminal
    exhausted = rejected.exhaust_run_deadline(at=_mono(140))
    assert exhausted.status is ReplacementRecoveryStatus.RUN_DEADLINE_EXHAUSTED
    assert exhausted.last_failure is ReplacementRecoveryFailure.RUN_DEADLINE_REACHED
    assert exhausted.terminal
    assert exhausted.unresolved


def test_deadline_remains_next_action_when_retry_would_fall_after_it() -> None:
    scheduled = (
        _active(hard_deadline_ns=135, run_deadline_ns=200, retry_delay_ns=20)
        .abandon_attempt(
            failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
            abandoned_monotonic=_mono(120),
            source=None,
        )
        .schedule_retry(decided_monotonic=_mono(121))
    )

    assert scheduled.next_retry_monotonic == _mono(141)
    assert scheduled.next_action_deadline == _mono(135)
    exhausted = scheduled.exhaust_hard_deadline(at=_mono(135))
    assert exhausted.status is ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED
    assert exhausted.unresolved


def test_explicit_deadline_exhaustion_rejects_early_decisions() -> None:
    authority = _authority(hard_deadline_ns=150, run_deadline_ns=200)
    with pytest.raises(ReplacementRecoveryTransitionError, match="has not been reached"):
        authority.exhaust_hard_deadline(at=_mono(149))
    with pytest.raises(ReplacementRecoveryTransitionError, match="has not been reached"):
        authority.exhaust_run_deadline(at=_mono(199))

    assert (
        authority.exhaust_hard_deadline(at=_mono(150)).status
        is ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED
    )
    assert (
        authority.exhaust_run_deadline(at=_mono(200)).status
        is ReplacementRecoveryStatus.RUN_DEADLINE_EXHAUSTED
    )


def test_terminal_cutoff_preserves_unresolved_authority_and_stops_retry() -> None:
    scheduled = (
        _active()
        .abandon_attempt(
            failure=ReplacementRecoveryFailure.RELEASE_CONTROL_NOT_PROJECTED,
            abandoned_monotonic=_mono(120),
            source=None,
        )
        .schedule_retry(decided_monotonic=_mono(121))
    )
    terminal = scheduled.terminalize(cutoff_monotonic=_mono(125))

    assert terminal.status is ReplacementRecoveryStatus.TERMINAL_UNRESOLVED
    assert terminal.last_result is ReplacementRecoveryResult.TERMINAL_UNRESOLVED
    assert terminal.last_failure is ReplacementRecoveryFailure.TERMINAL_CUTOFF
    assert terminal.next_retry_monotonic is None
    assert terminal.terminal
    assert terminal.unresolved
    assert not terminal.retry_allowed


def test_bounds_have_no_runtime_defaults() -> None:
    with pytest.raises(TypeError):
        ReplacementRecoveryBounds()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"maximum_attempts": 0, "retry_delay_ns": 1, "maximum_snapshot_age_ns": 1},
        {
            "maximum_attempts": MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS + 1,
            "retry_delay_ns": 1,
            "maximum_snapshot_age_ns": 1,
        },
        {"maximum_attempts": 1, "retry_delay_ns": 0, "maximum_snapshot_age_ns": 1},
        {
            "maximum_attempts": 1,
            "retry_delay_ns": MAXIMUM_REPLACEMENT_RECOVERY_RETRY_DELAY_NS + 1,
            "maximum_snapshot_age_ns": 1,
        },
        {"maximum_attempts": 1, "retry_delay_ns": 1, "maximum_snapshot_age_ns": 0},
        {
            "maximum_attempts": 1,
            "retry_delay_ns": 1,
            "maximum_snapshot_age_ns": MAXIMUM_REPLACEMENT_RECOVERY_NANOSECONDS + 1,
        },
    ],
)
def test_runtime_bounds_cannot_exceed_hard_maxima(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="replacement recovery"):
        ReplacementRecoveryBounds(**kwargs)


def test_runtime_retry_and_hard_duration_accept_exact_implementation_maxima() -> None:
    bounds = ReplacementRecoveryBounds(
        maximum_attempts=MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS,
        retry_delay_ns=MAXIMUM_REPLACEMENT_RECOVERY_RETRY_DELAY_NS,
        maximum_snapshot_age_ns=1,
    )
    authority = ReplacementRecoveryAuthority.start(
        divergence_id=DIVERGENCE_ID,
        resolution_source=_source("resolution-hard-duration-boundary"),
        resolution_ingest_seq=10,
        resolution_monotonic=_mono(100),
        original_connection_id=ORIGINAL,
        excluded_connection_ids=(),
        required_instrument_ids=(YES,),
        bounds=bounds,
        hard_deadline=_mono(100 + MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS),
        run_deadline=_mono(101 + MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS),
    )

    assert authority.bounds == bounds


def test_runtime_hard_duration_cannot_exceed_implementation_maximum() -> None:
    with pytest.raises(ValueError, match="hard duration exceeds"):
        ReplacementRecoveryAuthority.start(
            divergence_id=DIVERGENCE_ID,
            resolution_source=_source("resolution-hard-duration-overflow"),
            resolution_ingest_seq=10,
            resolution_monotonic=_mono(100),
            original_connection_id=ORIGINAL,
            excluded_connection_ids=(),
            required_instrument_ids=(YES,),
            bounds=ReplacementRecoveryBounds(1, 1, 1),
            hard_deadline=_mono(101 + MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS),
            run_deadline=_mono(102 + MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS),
        )


def test_required_instrument_and_excluded_connection_hard_maxima() -> None:
    with pytest.raises(ValueError, match="instruments exceed"):
        ReplacementRecoveryAuthority.start(
            divergence_id=DIVERGENCE_ID,
            resolution_source=_source("resolution-many-instruments"),
            resolution_ingest_seq=10,
            resolution_monotonic=_mono(100),
            original_connection_id=ORIGINAL,
            excluded_connection_ids=(),
            required_instrument_ids=tuple(
                InstrumentId(f"instrument-{index:04d}")
                for index in range(MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS + 1)
            ),
            bounds=ReplacementRecoveryBounds(1, 1, 1),
            hard_deadline=_mono(200),
            run_deadline=_mono(300),
        )
    with pytest.raises(ValueError, match="excluded connections exceed"):
        ReplacementRecoveryAuthority.start(
            divergence_id=DIVERGENCE_ID,
            resolution_source=_source("resolution-many-connections"),
            resolution_ingest_seq=10,
            resolution_monotonic=_mono(100),
            original_connection_id=ORIGINAL,
            excluded_connection_ids=tuple(
                ConnectionId(f"connection-{index:05d}")
                for index in range(MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS + 1)
            ),
            required_instrument_ids=(YES, NO),
            bounds=ReplacementRecoveryBounds(1, 1, 1),
            hard_deadline=_mono(200),
            run_deadline=_mono(300),
        )


def test_duplicate_required_instruments_and_excluded_connections_are_rejected() -> None:
    def start(
        *,
        excluded_connection_ids: tuple[ConnectionId, ...],
        required_instrument_ids: tuple[InstrumentId, ...],
    ) -> ReplacementRecoveryAuthority:
        return ReplacementRecoveryAuthority.start(
            divergence_id=DIVERGENCE_ID,
            resolution_source=_source("resolution-duplicates"),
            resolution_ingest_seq=10,
            resolution_monotonic=_mono(100),
            original_connection_id=ORIGINAL,
            excluded_connection_ids=excluded_connection_ids,
            required_instrument_ids=required_instrument_ids,
            bounds=ReplacementRecoveryBounds(1, 1, 1),
            hard_deadline=_mono(200),
            run_deadline=_mono(300),
        )

    with pytest.raises(ValueError, match="instruments must be unique"):
        start(
            excluded_connection_ids=(),
            required_instrument_ids=(YES, YES),
        )
    with pytest.raises(ValueError, match="excluded connections must be unique"):
        start(
            excluded_connection_ids=(EXCLUDED, EXCLUDED),
            required_instrument_ids=(YES, NO),
        )


@given(
    maximum_attempts=st.integers(
        min_value=1,
        max_value=MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS,
    )
)
def test_generated_retry_sequences_never_multiply_identity(
    maximum_attempts: int,
) -> None:
    authority = _authority(
        maximum_attempts=maximum_attempts,
        retry_delay_ns=1,
        hard_deadline_ns=10_000,
        run_deadline_ns=20_000,
    )
    recovery_id = authority.recovery_id
    contributor_key = authority.stable_contributor_key
    now = 110

    for attempt_index in range(maximum_attempts):
        authority = authority.start_attempt(
            connection_id=ConnectionId(f"generated-connection-{attempt_index}"),
            started_monotonic=_mono(now),
        )
        assert authority.recovery_id == recovery_id
        assert authority.stable_contributor_key == contributor_key
        authority = authority.abandon_attempt(
            failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
            abandoned_monotonic=_mono(now + 1),
            source=None,
        )
        authority = authority.schedule_retry(decided_monotonic=_mono(now + 2))
        assert authority.recovery_id == recovery_id
        assert authority.stable_contributor_key == contributor_key
        now += 3

    assert authority.status is ReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED
    assert authority.attempt_count == maximum_attempts
    assert len(authority.attempted_connection_ids) == maximum_attempts
    assert len(set(authority.attempted_connection_ids)) == maximum_attempts
    assert authority.unresolved
    assert authority.terminal


def test_same_transition_sequence_produces_identical_attempt_and_evidence_chains() -> None:
    def run() -> ReplacementRecoveryAuthority:
        return (
            _active()
            .observe_snapshot(
                candidate=_candidate(instrument_ids=(YES,)),
                observed_monotonic=_mono(112),
            )
            .abandon_attempt(
                failure=ReplacementRecoveryFailure.TRANSPORT_INTERRUPTED,
                abandoned_monotonic=_mono(120),
                source=_source("deterministic-disconnect"),
            )
            .schedule_retry(decided_monotonic=_mono(121))
            .start_attempt(connection_id=ATTEMPT_2, started_monotonic=_mono(131))
            .observe_snapshot(
                candidate=_candidate(
                    connection_id=ATTEMPT_2,
                    ingest_seq=20,
                    received_ns=132,
                    label="deterministic-complete",
                ),
                observed_monotonic=_mono(133),
            )
            .succeed_attempt(decided_monotonic=_mono(134))
            .release(
                release_source=_source("deterministic-release"),
                release_ingest_seq=21,
                release_monotonic=_mono(135),
            )
        )

    first = run()
    second = run()
    assert first == second
    assert first.attempt_chain_sha256 == second.attempt_chain_sha256
    assert first.evidence_chain_sha256 == second.evidence_chain_sha256
    assert canonical_sha256(first.to_record()) == canonical_sha256(second.to_record())
