"""Deterministic bounded authority for one audit replacement recovery.

The state machine in this module is deliberately independent from acquisition,
journaling, availability ownership, and the canonical processor.  It models one
unresolved audit divergence across a bounded sequence of connection attempts.
Callers own timers and durable controls; every transition here is pure and
returns a new immutable authority value.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final, Self

from multi_market_arbitrage.core.ids import (
    ConnectionId,
    InstrumentId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp
from multi_market_arbitrage.observability.availability import (
    AvailabilityCausalFamily,
    availability_audit_contribution_id,
    availability_stable_causal_key,
)

REPLACEMENT_RECOVERY_SCHEMA_VERSION: Final = "gate1-audit-replacement-recovery-v1"
REPLACEMENT_RECOVERY_ID_SCHEMA: Final = "gate1-audit-replacement-recovery-id-v1"
REPLACEMENT_RECOVERY_CONTRIBUTOR_KEY_SCHEMA: Final = "gate1-audit-replacement-contributor-key-v1"
REPLACEMENT_RECOVERY_EVIDENCE_CHAIN_SCHEMA: Final = "gate1-audit-replacement-evidence-chain-v1"
REPLACEMENT_RECOVERY_ATTEMPT_CHAIN_SCHEMA: Final = "gate1-audit-replacement-attempt-chain-v1"
REPLACEMENT_RECOVERY_RESOLUTION_ID_SCHEMA: Final = "gate1-audit-replacement-resolution-id-v1"
REPLACEMENT_RECOVERY_LIFECYCLE_SCHEMA: Final = "mma-rest-audit-replacement-recovery-lifecycle-v1"

# These are implementation safety ceilings, not Phase-1 runtime policy.  The
# actual attempt, retry, and freshness bounds are mandatory caller inputs.
MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS: Final = 64
MAXIMUM_REPLACEMENT_RECOVERY_RETRY_DELAY_NS: Final = 60 * 1_000_000_000
MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS: Final = 300 * 1_000_000_000
MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS: Final = 32
MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS: Final = 4_096
MAXIMUM_REPLACEMENT_RECOVERY_NANOSECONDS: Final = (1 << 63) - 1


class ReplacementRecoveryTransitionError(RuntimeError):
    """A caller requested a transition that is invalid for the current state."""


class ReplacementRecoveryStatus(StrEnum):
    """Finite lifecycle states for one stable replacement recovery."""

    READY = "READY"
    ATTEMPT_ACTIVE = "ATTEMPT_ACTIVE"
    ATTEMPT_ABANDONED = "ATTEMPT_ABANDONED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    CAPTURE_COMPLETE = "CAPTURE_COMPLETE"
    RELEASED = "RELEASED"
    MAXIMUM_ATTEMPTS_EXHAUSTED = "MAXIMUM_ATTEMPTS_EXHAUSTED"
    HARD_DEADLINE_EXHAUSTED = "HARD_DEADLINE_EXHAUSTED"
    RUN_DEADLINE_EXHAUSTED = "RUN_DEADLINE_EXHAUSTED"
    TERMINAL_UNRESOLVED = "TERMINAL_UNRESOLVED"


class ReplacementRecoveryResult(StrEnum):
    """Typed result of the most recent deterministic transition."""

    RECOVERY_STARTED = "RECOVERY_STARTED"
    ATTEMPT_STARTED = "ATTEMPT_STARTED"
    SNAPSHOT_ACCEPTED = "SNAPSHOT_ACCEPTED"
    SNAPSHOT_REJECTED = "SNAPSHOT_REJECTED"
    PARTIAL_COVERAGE = "PARTIAL_COVERAGE"
    ATTEMPT_ABANDONED = "ATTEMPT_ABANDONED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    CAPTURE_SUCCEEDED = "CAPTURE_SUCCEEDED"
    CAPTURE_SUCCESS_REJECTED = "CAPTURE_SUCCESS_REJECTED"
    RELEASED = "RELEASED"
    RELEASE_NOT_PROJECTED = "RELEASE_NOT_PROJECTED"
    RELEASE_REJECTED = "RELEASE_REJECTED"
    MAXIMUM_ATTEMPTS_EXHAUSTED = "MAXIMUM_ATTEMPTS_EXHAUSTED"
    HARD_DEADLINE_EXHAUSTED = "HARD_DEADLINE_EXHAUSTED"
    RUN_DEADLINE_EXHAUSTED = "RUN_DEADLINE_EXHAUSTED"
    TERMINAL_UNRESOLVED = "TERMINAL_UNRESOLVED"


class ReplacementRecoveryFailure(StrEnum):
    """Finite fail-closed reasons retained by the recovery authority."""

    PRE_RESOLUTION_SOURCE = "PRE_RESOLUTION_SOURCE"
    PRE_ATTEMPT_SOURCE = "PRE_ATTEMPT_SOURCE"
    ORIGINAL_CONNECTION_REUSED = "ORIGINAL_CONNECTION_REUSED"
    DISALLOWED_CONNECTION = "DISALLOWED_CONNECTION"
    STALE_SOURCE = "STALE_SOURCE"
    FUTURE_SOURCE = "FUTURE_SOURCE"
    CROSS_EPOCH_SOURCE = "CROSS_EPOCH_SOURCE"
    MISSING_ADMISSION_ORDINAL = "MISSING_ADMISSION_ORDINAL"
    PARTIAL_REQUIRED_SET = "PARTIAL_REQUIRED_SET"
    UNRELATED_INSTRUMENT = "UNRELATED_INSTRUMENT"
    NON_MONOTONIC_SOURCE = "NON_MONOTONIC_SOURCE"
    TRANSPORT_INTERRUPTED = "TRANSPORT_INTERRUPTED"
    FRESHNESS_EXPIRED = "FRESHNESS_EXPIRED"
    RELEASE_CONTROL_NOT_PROJECTED = "RELEASE_CONTROL_NOT_PROJECTED"
    RELEASE_CONTROL_REJECTED = "RELEASE_CONTROL_REJECTED"
    MAXIMUM_ATTEMPTS_REACHED = "MAXIMUM_ATTEMPTS_REACHED"
    HARD_DEADLINE_REACHED = "HARD_DEADLINE_REACHED"
    RUN_DEADLINE_REACHED = "RUN_DEADLINE_REACHED"
    TERMINAL_CUTOFF = "TERMINAL_CUTOFF"


class ReplacementRecoveryLifecycleAction(StrEnum):
    """Durable coordinator decisions consumed by live and replay processing."""

    ATTEMPT_START = "ATTEMPT_START"
    ATTEMPT_ABANDON_RETRY = "ATTEMPT_ABANDON_RETRY"
    DEADLINE = "DEADLINE"
    RELEASE_NOT_PROJECTED = "RELEASE_NOT_PROJECTED"
    TERMINAL_CUTOFF = "TERMINAL_CUTOFF"


_TERMINAL_STATUSES: Final = frozenset(
    {
        ReplacementRecoveryStatus.RELEASED,
        ReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED,
        ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED,
        ReplacementRecoveryStatus.RUN_DEADLINE_EXHAUSTED,
        ReplacementRecoveryStatus.TERMINAL_UNRESOLVED,
    }
)


def _require_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"replacement recovery {label} must be an integer")
    return value


@dataclass(frozen=True, slots=True)
class ReplacementRecoveryLifecycleCommand:
    """One source-bound, replayable replacement-recovery supervisor decision."""

    action: ReplacementRecoveryLifecycleAction
    recovery_id: str
    divergence_id: str
    prior_authority_sha256: str
    monotonic_epoch_id: str
    decision_monotonic_ns: int
    connection_id: ConnectionId | None
    failure: ReplacementRecoveryFailure | None
    schema: str = REPLACEMENT_RECOVERY_LIFECYCLE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != REPLACEMENT_RECOVERY_LIFECYCLE_SCHEMA:
            raise ValueError("unsupported replacement recovery lifecycle schema")
        for label, value in (
            ("recovery ID", self.recovery_id),
            ("divergence ID", self.divergence_id),
            ("prior authority", self.prior_authority_sha256),
        ):
            if not is_sha256_hex(value):
                raise ValueError(f"replacement recovery lifecycle {label} must be SHA-256 hex")
        if not self.monotonic_epoch_id or len(self.monotonic_epoch_id.encode("utf-8")) > 256:
            raise ValueError("replacement recovery lifecycle epoch is invalid")
        _require_integer(self.decision_monotonic_ns, label="lifecycle decision")
        if self.decision_monotonic_ns < 0:
            raise ValueError("replacement recovery lifecycle decision cannot be negative")
        if self.action is ReplacementRecoveryLifecycleAction.ATTEMPT_START:
            if self.connection_id is None or self.failure is not None:
                raise ValueError("attempt-start lifecycle control requires only a connection")
        elif self.action is ReplacementRecoveryLifecycleAction.ATTEMPT_ABANDON_RETRY:
            if self.connection_id is None or self.failure is None:
                raise ValueError(
                    "attempt-abandon lifecycle control requires a connection and failure"
                )
            if self.failure in {
                ReplacementRecoveryFailure.MAXIMUM_ATTEMPTS_REACHED,
                ReplacementRecoveryFailure.HARD_DEADLINE_REACHED,
                ReplacementRecoveryFailure.RUN_DEADLINE_REACHED,
                ReplacementRecoveryFailure.TERMINAL_CUTOFF,
            }:
                raise ValueError("attempt-abandon lifecycle control cannot carry terminal failure")
        elif self.action is ReplacementRecoveryLifecycleAction.RELEASE_NOT_PROJECTED:
            if self.connection_id is None or self.failure is not None:
                raise ValueError(
                    "release-not-projected lifecycle control requires only a connection"
                )
        elif self.connection_id is not None or self.failure is not None:
            raise ValueError("deadline/terminal lifecycle control cannot carry attempt fields")

    @classmethod
    def create(
        cls,
        *,
        authority: ReplacementRecoveryAuthority,
        action: ReplacementRecoveryLifecycleAction,
        decision_monotonic: MonotonicTimestamp,
        connection_id: ConnectionId | None = None,
        failure: ReplacementRecoveryFailure | None = None,
    ) -> Self:
        return cls(
            action=action,
            recovery_id=authority.recovery_id,
            divergence_id=authority.divergence_id,
            prior_authority_sha256=canonical_sha256(authority.to_record()),
            monotonic_epoch_id=decision_monotonic.epoch_id,
            decision_monotonic_ns=decision_monotonic.nanoseconds,
            connection_id=connection_id,
            failure=failure,
        )

    @property
    def decision_monotonic(self) -> MonotonicTimestamp:
        return MonotonicTimestamp(self.monotonic_epoch_id, self.decision_monotonic_ns)

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "action": self.action,
                "connection_id": self.connection_id,
                "decision_monotonic_ns": self.decision_monotonic_ns,
                "divergence_id": self.divergence_id,
                "failure": self.failure,
                "monotonic_epoch_id": self.monotonic_epoch_id,
                "prior_authority_sha256": self.prior_authority_sha256,
                "recovery_id": self.recovery_id,
                "schema": self.schema,
            }
        )


@dataclass(frozen=True, slots=True)
class ReplacementRecoveryBounds:
    """Mandatory Phase-1 policy bounded by non-configurable safety ceilings."""

    maximum_attempts: int
    retry_delay_ns: int
    maximum_snapshot_age_ns: int

    def __post_init__(self) -> None:
        for name, value in (
            ("maximum_attempts", self.maximum_attempts),
            ("retry_delay_ns", self.retry_delay_ns),
            ("maximum_snapshot_age_ns", self.maximum_snapshot_age_ns),
        ):
            _require_integer(value, label=name)
            if value <= 0:
                raise ValueError(f"replacement recovery {name} must be positive")
        if self.maximum_attempts > MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS:
            raise ValueError("replacement recovery attempt bound exceeds its hard maximum")
        if self.retry_delay_ns > MAXIMUM_REPLACEMENT_RECOVERY_RETRY_DELAY_NS:
            raise ValueError("replacement recovery retry delay exceeds its hard maximum")
        if self.maximum_snapshot_age_ns > MAXIMUM_REPLACEMENT_RECOVERY_NANOSECONDS:
            raise ValueError("replacement recovery time bound exceeds its hard maximum")


@dataclass(frozen=True, slots=True)
class ReplacementSnapshotCandidate:
    """One raw full-snapshot candidate observed at a bounded decision point."""

    connection_id: ConnectionId
    instrument_ids: tuple[InstrumentId, ...]
    admission_ordinal: int | None
    ingest_seq: int
    received_monotonic: MonotonicTimestamp
    raw_source: RawSourceRef

    def __post_init__(self) -> None:
        _require_integer(self.ingest_seq, label="snapshot ingest sequence")
        if self.ingest_seq < 0:
            raise ValueError("replacement snapshot ingest sequence cannot be negative")
        if self.admission_ordinal is not None:
            _require_integer(self.admission_ordinal, label="snapshot admission ordinal")
        if not self.instrument_ids:
            raise ValueError("replacement snapshot candidate must contain an instrument")
        if len(self.instrument_ids) > MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS:
            raise ValueError("replacement snapshot candidate exceeds the instrument hard bound")
        normalized = tuple(sorted(set(self.instrument_ids), key=lambda item: item.value))
        if len(normalized) != len(self.instrument_ids):
            raise ValueError("replacement snapshot candidate repeats an instrument")
        object.__setattr__(self, "instrument_ids", normalized)


@dataclass(frozen=True, slots=True)
class CapturedReplacementSnapshot:
    """Accepted current-attempt source for one required instrument."""

    instrument_id: InstrumentId
    connection_id: ConnectionId
    admission_ordinal: int
    ingest_seq: int
    received_monotonic: MonotonicTimestamp
    raw_source: RawSourceRef

    def __post_init__(self) -> None:
        if min(self.admission_ordinal, self.ingest_seq) < 0:
            raise ValueError("captured replacement ordinals cannot be negative")
        if self.admission_ordinal == 0:
            raise ValueError("captured replacement admission ordinal must be positive")


def _chain_seed(*, schema: str, recovery_id: str) -> str:
    return canonical_sha256(
        {
            "recovery_id": recovery_id,
            "schema": schema,
            "seed": True,
        }
    )


def _chain_extend(*, schema: str, previous: str, event: str, payload: object) -> str:
    return canonical_sha256(
        {
            "event": event,
            "payload": payload,
            "previous_sha256": previous,
            "schema": schema,
        }
    )


@dataclass(frozen=True, slots=True)
class ReplacementRecoveryAuthority:
    """Immutable current authority for exactly one audit recovery lifecycle."""

    recovery_id: str
    resolution_id: str
    contribution_id: str
    stable_contributor_key: str
    divergence_id: str
    resolution_source: RawSourceRef
    resolution_ingest_seq: int
    resolution_monotonic: MonotonicTimestamp
    original_connection_id: ConnectionId
    excluded_connection_ids: tuple[ConnectionId, ...]
    required_instrument_ids: tuple[InstrumentId, ...]
    bounds: ReplacementRecoveryBounds
    hard_deadline: MonotonicTimestamp
    run_deadline: MonotonicTimestamp
    status: ReplacementRecoveryStatus
    attempt_count: int
    attempted_connection_ids: tuple[ConnectionId, ...]
    first_attempt_monotonic: MonotonicTimestamp | None
    latest_attempt_monotonic: MonotonicTimestamp | None
    next_retry_monotonic: MonotonicTimestamp | None
    current_connection_id: ConnectionId | None
    captured_snapshot_sources: tuple[CapturedReplacementSnapshot, ...]
    last_result: ReplacementRecoveryResult
    last_failure: ReplacementRecoveryFailure | None
    attempt_chain_sha256: str
    evidence_chain_sha256: str
    evidence_event_count: int
    release_source: RawSourceRef | None
    release_ingest_seq: int | None
    release_monotonic: MonotonicTimestamp | None
    schema_version: str

    def __post_init__(self) -> None:
        if self.schema_version != REPLACEMENT_RECOVERY_SCHEMA_VERSION:
            raise ValueError("unsupported replacement recovery schema version")
        for name, value in (
            ("recovery ID", self.recovery_id),
            ("resolution ID", self.resolution_id),
            ("contribution ID", self.contribution_id),
            ("divergence ID", self.divergence_id),
            ("attempt chain", self.attempt_chain_sha256),
            ("evidence chain", self.evidence_chain_sha256),
        ):
            if not is_sha256_hex(value):
                raise ValueError(f"replacement recovery {name} must be SHA-256 hex")
        expected_contributor_key = availability_stable_causal_key(
            causal_family=AvailabilityCausalFamily.AUDIT_DIVERGENCE,
            causal_lifecycle_id=f"divergence:{self.divergence_id}",
        )
        if self.stable_contributor_key != expected_contributor_key:
            raise ValueError("replacement recovery stable contributor key is not canonical")
        expected_contribution_id = availability_audit_contribution_id(
            stable_causal_key=self.stable_contributor_key
        )
        if self.contribution_id != expected_contribution_id:
            raise ValueError("replacement recovery contribution ID is not canonical")
        expected_resolution_id = canonical_sha256(
            {
                "divergence_id": self.divergence_id,
                "resolution_ingest_seq": self.resolution_ingest_seq,
                "resolution_monotonic": self.resolution_monotonic,
                "resolution_source": self.resolution_source,
                "schema": REPLACEMENT_RECOVERY_RESOLUTION_ID_SCHEMA,
            }
        )
        if self.resolution_id != expected_resolution_id:
            raise ValueError("replacement recovery resolution ID is not canonical")
        _require_integer(self.resolution_ingest_seq, label="resolution sequence")
        if self.resolution_ingest_seq < 0:
            raise ValueError("replacement recovery resolution sequence cannot be negative")
        epoch_id = self.resolution_monotonic.epoch_id
        if self.hard_deadline.epoch_id != epoch_id or self.run_deadline.epoch_id != epoch_id:
            raise ValueError("replacement recovery deadlines belong to another epoch")
        if (
            min(
                self.hard_deadline.nanoseconds - self.resolution_monotonic.nanoseconds,
                self.run_deadline.nanoseconds - self.resolution_monotonic.nanoseconds,
            )
            <= 0
        ):
            raise ValueError("replacement recovery deadlines must follow resolution")
        if (
            self.hard_deadline.nanoseconds - self.resolution_monotonic.nanoseconds
            > MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS
        ):
            raise ValueError("replacement recovery hard duration exceeds its hard maximum")
        if not self.required_instrument_ids:
            raise ValueError("replacement recovery requires at least one instrument")
        if len(self.required_instrument_ids) > (MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS):
            raise ValueError("replacement recovery instruments exceed their hard bound")
        expected_required = tuple(
            sorted(set(self.required_instrument_ids), key=lambda item: item.value)
        )
        if expected_required != self.required_instrument_ids:
            raise ValueError("replacement recovery instruments must be unique and sorted")
        if len(self.excluded_connection_ids) > (MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS):
            raise ValueError("replacement recovery excluded connections exceed their hard bound")
        expected_excluded = tuple(
            sorted(set(self.excluded_connection_ids), key=lambda item: item.value)
        )
        if expected_excluded != self.excluded_connection_ids:
            raise ValueError("replacement recovery excluded connections must be unique and sorted")
        if self.original_connection_id in self.attempted_connection_ids:
            raise ValueError("replacement recovery reused the original connection")
        if set(self.excluded_connection_ids).intersection(self.attempted_connection_ids):
            raise ValueError("replacement recovery used an excluded connection")
        if len(set(self.attempted_connection_ids)) != len(self.attempted_connection_ids):
            raise ValueError("replacement recovery attempted one connection more than once")
        if self.attempt_count != len(self.attempted_connection_ids):
            raise ValueError("replacement recovery attempt count does not reconcile")
        if not 0 <= self.attempt_count <= self.bounds.maximum_attempts:
            raise ValueError("replacement recovery attempt count exceeds its configured bound")
        has_attempt_times = (
            self.first_attempt_monotonic is not None and self.latest_attempt_monotonic is not None
        )
        if has_attempt_times != (self.attempt_count > 0):
            raise ValueError("replacement recovery attempt times do not reconcile")
        if self.attempt_count > 0:
            first = self.first_attempt_monotonic
            latest = self.latest_attempt_monotonic
            if first is None or latest is None:  # pragma: no cover - narrowed above
                raise ValueError("replacement recovery attempt times are absent")
            if first.epoch_id != epoch_id or latest.epoch_id != epoch_id:
                raise ValueError("replacement recovery attempt times belong to another epoch")
            if not (
                self.resolution_monotonic.nanoseconds <= first.nanoseconds <= latest.nanoseconds
            ):
                raise ValueError("replacement recovery attempt chronology is invalid")
        if (self.next_retry_monotonic is not None) != (
            self.status is ReplacementRecoveryStatus.RETRY_SCHEDULED
        ):
            raise ValueError("replacement recovery next-retry state is inconsistent")
        if self.next_retry_monotonic is not None:
            if self.next_retry_monotonic.epoch_id != epoch_id:
                raise ValueError("replacement recovery retry belongs to another epoch")
            latest = self.latest_attempt_monotonic
            if latest is None or self.next_retry_monotonic.nanoseconds <= latest.nanoseconds:
                raise ValueError("replacement recovery retry must follow the latest attempt")
        if (
            self.status
            in {
                ReplacementRecoveryStatus.READY,
            }
            and self.current_connection_id is not None
        ):
            raise ValueError("ready replacement recovery cannot have a current connection")
        if (
            self.status
            in {
                ReplacementRecoveryStatus.ATTEMPT_ACTIVE,
                ReplacementRecoveryStatus.CAPTURE_COMPLETE,
            }
            and self.current_connection_id is None
        ):
            raise ValueError("active replacement recovery requires a current connection")
        if self.current_connection_id is not None and (
            not self.attempted_connection_ids
            or self.current_connection_id != self.attempted_connection_ids[-1]
        ):
            raise ValueError("replacement recovery current connection is not its latest attempt")
        captured_ids = tuple(item.instrument_id for item in self.captured_snapshot_sources)
        if tuple(sorted(set(captured_ids), key=lambda item: item.value)) != captured_ids:
            raise ValueError("replacement recovery captured instruments must be unique and sorted")
        if not set(captured_ids).issubset(self.required_instrument_ids):
            raise ValueError("replacement recovery captured an unrelated instrument")
        if self.captured_snapshot_sources and self.current_connection_id is None:
            raise ValueError("replacement recovery captures require a current connection")
        for capture in self.captured_snapshot_sources:
            if (
                capture.connection_id != self.current_connection_id
                or capture.received_monotonic.epoch_id != epoch_id
                or capture.ingest_seq <= self.resolution_ingest_seq
                or capture.received_monotonic.nanoseconds < self.resolution_monotonic.nanoseconds
            ):
                raise ValueError("replacement recovery captured source is ineligible")
        complete = captured_ids == self.required_instrument_ids
        if (
            self.status
            in {
                ReplacementRecoveryStatus.CAPTURE_COMPLETE,
                ReplacementRecoveryStatus.RELEASED,
            }
            and not complete
        ):
            raise ValueError("completed replacement recovery lacks full coverage")
        release_values = (
            self.release_source,
            self.release_ingest_seq,
            self.release_monotonic,
        )
        if (self.status is ReplacementRecoveryStatus.RELEASED) != all(
            value is not None for value in release_values
        ):
            raise ValueError("replacement recovery release evidence is inconsistent")
        if self.status is not ReplacementRecoveryStatus.RELEASED and any(
            value is not None for value in release_values
        ):
            raise ValueError("unreleased replacement recovery cannot carry release evidence")
        if self.status is ReplacementRecoveryStatus.RELEASED:
            release_ingest_seq = self.release_ingest_seq
            release_monotonic = self.release_monotonic
            if release_ingest_seq is None or release_monotonic is None:
                raise ValueError("released replacement recovery lacks its boundary")
            if release_monotonic.epoch_id != epoch_id:
                raise ValueError("replacement release belongs to another epoch")
            if release_ingest_seq <= max(
                item.ingest_seq for item in self.captured_snapshot_sources
            ):
                raise ValueError("replacement release does not follow its captured sources")
            if release_monotonic.nanoseconds < max(
                item.received_monotonic.nanoseconds for item in self.captured_snapshot_sources
            ):
                raise ValueError("replacement release precedes its captured sources")
        if self.evidence_event_count <= 0:
            raise ValueError("replacement recovery evidence count must be positive")

    @classmethod
    def start(
        cls,
        *,
        divergence_id: str,
        resolution_source: RawSourceRef,
        resolution_ingest_seq: int,
        resolution_monotonic: MonotonicTimestamp,
        original_connection_id: ConnectionId,
        excluded_connection_ids: tuple[ConnectionId, ...],
        required_instrument_ids: tuple[InstrumentId, ...],
        bounds: ReplacementRecoveryBounds,
        hard_deadline: MonotonicTimestamp,
        run_deadline: MonotonicTimestamp,
    ) -> Self:
        """Create unresolved authority without silently starting an attempt."""

        if len(excluded_connection_ids) > MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS:
            raise ValueError("replacement recovery excluded connections exceed their hard bound")
        if len(set(excluded_connection_ids)) != len(excluded_connection_ids):
            raise ValueError("replacement recovery excluded connections must be unique")
        if len(required_instrument_ids) > MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS:
            raise ValueError("replacement recovery instruments exceed their hard bound")
        if len(set(required_instrument_ids)) != len(required_instrument_ids):
            raise ValueError("replacement recovery instruments must be unique")
        normalized_excluded = tuple(
            sorted(set(excluded_connection_ids), key=lambda item: item.value)
        )
        normalized_required = tuple(
            sorted(set(required_instrument_ids), key=lambda item: item.value)
        )
        recovery_id = canonical_sha256(
            {
                "divergence_id": divergence_id,
                "original_connection_id": original_connection_id,
                "required_instrument_ids": normalized_required,
                "resolution_ingest_seq": resolution_ingest_seq,
                "resolution_monotonic": resolution_monotonic,
                "resolution_source": resolution_source,
                "schema": REPLACEMENT_RECOVERY_ID_SCHEMA,
            }
        )
        stable_contributor_key = availability_stable_causal_key(
            causal_family=AvailabilityCausalFamily.AUDIT_DIVERGENCE,
            causal_lifecycle_id=f"divergence:{divergence_id}",
        )
        contribution_id = availability_audit_contribution_id(
            stable_causal_key=stable_contributor_key
        )
        resolution_id = canonical_sha256(
            {
                "divergence_id": divergence_id,
                "resolution_ingest_seq": resolution_ingest_seq,
                "resolution_monotonic": resolution_monotonic,
                "resolution_source": resolution_source,
                "schema": REPLACEMENT_RECOVERY_RESOLUTION_ID_SCHEMA,
            }
        )
        attempt_chain = _chain_seed(
            schema=REPLACEMENT_RECOVERY_ATTEMPT_CHAIN_SCHEMA,
            recovery_id=recovery_id,
        )
        evidence_seed = _chain_seed(
            schema=REPLACEMENT_RECOVERY_EVIDENCE_CHAIN_SCHEMA,
            recovery_id=recovery_id,
        )
        start_payload = {
            "bounds": bounds,
            "divergence_id": divergence_id,
            "excluded_connection_ids": normalized_excluded,
            "hard_deadline": hard_deadline,
            "original_connection_id": original_connection_id,
            "required_instrument_ids": normalized_required,
            "resolution_ingest_seq": resolution_ingest_seq,
            "resolution_monotonic": resolution_monotonic,
            "resolution_source": resolution_source,
            "run_deadline": run_deadline,
        }
        evidence_chain = _chain_extend(
            schema=REPLACEMENT_RECOVERY_EVIDENCE_CHAIN_SCHEMA,
            previous=evidence_seed,
            event=ReplacementRecoveryResult.RECOVERY_STARTED.value,
            payload=start_payload,
        )
        return cls(
            recovery_id=recovery_id,
            resolution_id=resolution_id,
            contribution_id=contribution_id,
            stable_contributor_key=stable_contributor_key,
            divergence_id=divergence_id,
            resolution_source=resolution_source,
            resolution_ingest_seq=resolution_ingest_seq,
            resolution_monotonic=resolution_monotonic,
            original_connection_id=original_connection_id,
            excluded_connection_ids=normalized_excluded,
            required_instrument_ids=normalized_required,
            bounds=bounds,
            hard_deadline=hard_deadline,
            run_deadline=run_deadline,
            status=ReplacementRecoveryStatus.READY,
            attempt_count=0,
            attempted_connection_ids=(),
            first_attempt_monotonic=None,
            latest_attempt_monotonic=None,
            next_retry_monotonic=None,
            current_connection_id=None,
            captured_snapshot_sources=(),
            last_result=ReplacementRecoveryResult.RECOVERY_STARTED,
            last_failure=None,
            attempt_chain_sha256=attempt_chain,
            evidence_chain_sha256=evidence_chain,
            evidence_event_count=1,
            release_source=None,
            release_ingest_seq=None,
            release_monotonic=None,
            schema_version=REPLACEMENT_RECOVERY_SCHEMA_VERSION,
        )

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_STATUSES

    @property
    def unresolved(self) -> bool:
        return self.status is not ReplacementRecoveryStatus.RELEASED

    @property
    def captured_instrument_ids(self) -> tuple[InstrumentId, ...]:
        return tuple(item.instrument_id for item in self.captured_snapshot_sources)

    @property
    def coverage_complete(self) -> bool:
        return self.captured_instrument_ids == self.required_instrument_ids

    @property
    def retry_allowed(self) -> bool:
        return (
            not self.terminal
            and self.status
            in {
                ReplacementRecoveryStatus.READY,
                ReplacementRecoveryStatus.ATTEMPT_ABANDONED,
                ReplacementRecoveryStatus.RETRY_SCHEDULED,
            }
            and self.attempt_count < self.bounds.maximum_attempts
        )

    @property
    def next_action_deadline(self) -> MonotonicTimestamp:
        candidates = [self.hard_deadline, self.run_deadline]
        if self.next_retry_monotonic is not None:
            candidates.append(self.next_retry_monotonic)
        if self.status is ReplacementRecoveryStatus.ATTEMPT_ACTIVE:
            latest_attempt = self.latest_attempt_monotonic
            if latest_attempt is None:  # pragma: no cover - constructor enforces this
                raise ReplacementRecoveryTransitionError(
                    "active replacement attempt lacks its start boundary"
                )
            freshness_origin = (
                min(
                    (source.received_monotonic for source in self.captured_snapshot_sources),
                    key=lambda item: item.nanoseconds,
                )
                if self.captured_snapshot_sources
                else latest_attempt
            )
            candidates.append(
                MonotonicTimestamp(
                    freshness_origin.epoch_id,
                    freshness_origin.nanoseconds + self.bounds.maximum_snapshot_age_ns + 1,
                )
            )
        elif self.status is ReplacementRecoveryStatus.CAPTURE_COMPLETE:
            candidates.append(
                MonotonicTimestamp(
                    self.resolution_monotonic.epoch_id,
                    min(
                        source.received_monotonic.nanoseconds
                        for source in self.captured_snapshot_sources
                    )
                    + self.bounds.maximum_snapshot_age_ns
                    + 1,
                )
            )
        return min(candidates, key=lambda item: item.nanoseconds)

    def _require_nonterminal(self) -> None:
        if self.terminal:
            raise ReplacementRecoveryTransitionError(
                f"replacement recovery is terminal: {self.status.value}"
            )

    def _require_epoch(self, value: MonotonicTimestamp, *, label: str) -> None:
        if value.epoch_id != self.resolution_monotonic.epoch_id:
            raise ReplacementRecoveryTransitionError(
                f"replacement recovery {label} belongs to another epoch"
            )

    def _with_event(
        self,
        *,
        event: ReplacementRecoveryResult,
        payload: object,
        failure: ReplacementRecoveryFailure | None,
        attempt_event: bool,
        **changes: object,
    ) -> Self:
        evidence_chain = _chain_extend(
            schema=REPLACEMENT_RECOVERY_EVIDENCE_CHAIN_SCHEMA,
            previous=self.evidence_chain_sha256,
            event=event.value,
            payload={"failure": failure, "value": payload},
        )
        attempt_chain = self.attempt_chain_sha256
        if attempt_event:
            attempt_chain = _chain_extend(
                schema=REPLACEMENT_RECOVERY_ATTEMPT_CHAIN_SCHEMA,
                previous=attempt_chain,
                event=event.value,
                payload={"attempt": self.attempt_count, "failure": failure, "value": payload},
            )
        return replace(
            self,
            last_result=event,
            last_failure=failure,
            evidence_chain_sha256=evidence_chain,
            evidence_event_count=self.evidence_event_count + 1,
            attempt_chain_sha256=attempt_chain,
            **changes,
        )

    def _deadline_failure(
        self,
        at: MonotonicTimestamp,
    ) -> ReplacementRecoveryFailure | None:
        self._require_epoch(at, label="decision")
        hard_reached = at.nanoseconds >= self.hard_deadline.nanoseconds
        run_reached = at.nanoseconds >= self.run_deadline.nanoseconds
        if not hard_reached and not run_reached:
            return None
        if run_reached and (
            not hard_reached or self.run_deadline.nanoseconds <= self.hard_deadline.nanoseconds
        ):
            return ReplacementRecoveryFailure.RUN_DEADLINE_REACHED
        return ReplacementRecoveryFailure.HARD_DEADLINE_REACHED

    def _deadline_exhaustion(self, at: MonotonicTimestamp) -> Self | None:
        failure = self._deadline_failure(at)
        if failure is ReplacementRecoveryFailure.RUN_DEADLINE_REACHED:
            return self.exhaust_run_deadline(at=at)
        if failure is ReplacementRecoveryFailure.HARD_DEADLINE_REACHED:
            return self.exhaust_hard_deadline(at=at)
        return None

    def start_attempt(
        self,
        *,
        connection_id: ConnectionId,
        started_monotonic: MonotonicTimestamp,
    ) -> Self:
        """Start the initial or a scheduled retry on one new permitted connection."""

        self._require_nonterminal()
        if self.status not in {
            ReplacementRecoveryStatus.READY,
            ReplacementRecoveryStatus.RETRY_SCHEDULED,
        }:
            raise ReplacementRecoveryTransitionError(
                "replacement attempt can start only from ready or retry-scheduled state"
            )
        self._require_epoch(started_monotonic, label="attempt start")
        exhausted = self._deadline_exhaustion(started_monotonic)
        if exhausted is not None:
            return exhausted
        if self.status is ReplacementRecoveryStatus.RETRY_SCHEDULED:
            next_retry = self.next_retry_monotonic
            if next_retry is None:  # pragma: no cover - constructor enforces this
                raise ReplacementRecoveryTransitionError("scheduled retry lacks its deadline")
            if started_monotonic.nanoseconds < next_retry.nanoseconds:
                raise ReplacementRecoveryTransitionError(
                    "replacement attempt precedes its scheduled retry boundary"
                )
        if self.attempt_count >= self.bounds.maximum_attempts:
            return self.exhaust_maximum_attempts(at=started_monotonic)
        disallowed = {
            self.original_connection_id,
            *self.excluded_connection_ids,
            *self.attempted_connection_ids,
        }
        if connection_id in disallowed:
            raise ReplacementRecoveryTransitionError(
                "replacement attempt requires a new permitted connection"
            )
        new_attempt_count = self.attempt_count + 1
        attempt_payload = {
            "attempt": new_attempt_count,
            "connection_id": connection_id,
            "started_monotonic": started_monotonic,
        }
        updated = replace(
            self,
            attempt_count=new_attempt_count,
            attempted_connection_ids=(*self.attempted_connection_ids, connection_id),
            first_attempt_monotonic=(
                started_monotonic
                if self.first_attempt_monotonic is None
                else self.first_attempt_monotonic
            ),
            latest_attempt_monotonic=started_monotonic,
            next_retry_monotonic=None,
            current_connection_id=connection_id,
            captured_snapshot_sources=(),
            status=ReplacementRecoveryStatus.ATTEMPT_ACTIVE,
        )
        return updated._with_event(
            event=ReplacementRecoveryResult.ATTEMPT_STARTED,
            payload=attempt_payload,
            failure=None,
            attempt_event=True,
        )

    def _reject_snapshot(
        self,
        *,
        candidate: ReplacementSnapshotCandidate,
        observed_monotonic: MonotonicTimestamp,
        failure: ReplacementRecoveryFailure,
    ) -> Self:
        return self._with_event(
            event=ReplacementRecoveryResult.SNAPSHOT_REJECTED,
            payload={"candidate": candidate, "observed_monotonic": observed_monotonic},
            failure=failure,
            attempt_event=False,
        )

    def observe_snapshot(
        self,
        *,
        candidate: ReplacementSnapshotCandidate,
        observed_monotonic: MonotonicTimestamp,
    ) -> Self:
        """Validate and retain one current-attempt full-snapshot candidate."""

        self._require_nonterminal()
        if self.status is not ReplacementRecoveryStatus.ATTEMPT_ACTIVE:
            raise ReplacementRecoveryTransitionError(
                "replacement snapshot requires one active attempt"
            )
        current_connection = self.current_connection_id
        if current_connection is None:  # pragma: no cover - constructor enforces this
            raise ReplacementRecoveryTransitionError("active replacement attempt lacks connection")
        epoch_id = self.resolution_monotonic.epoch_id
        if (
            candidate.received_monotonic.epoch_id != epoch_id
            or observed_monotonic.epoch_id != epoch_id
        ):
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.CROSS_EPOCH_SOURCE,
            )
        deadline_failure = self._deadline_failure(observed_monotonic)
        if deadline_failure is not None:
            # Raw market evidence may be retained/rejected, but only a durable
            # lifecycle control may move the supervisor into deadline
            # exhaustion.  The subsequent FIFO-drained timer control binds the
            # same updated prior-authority identity in live and replay.
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=deadline_failure,
            )
        if candidate.received_monotonic.nanoseconds > observed_monotonic.nanoseconds:
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.FUTURE_SOURCE,
            )
        if (
            candidate.ingest_seq <= self.resolution_ingest_seq
            or candidate.received_monotonic.nanoseconds < self.resolution_monotonic.nanoseconds
        ):
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.PRE_RESOLUTION_SOURCE,
            )
        latest_attempt = self.latest_attempt_monotonic
        if (
            latest_attempt is None
            or candidate.received_monotonic.nanoseconds < latest_attempt.nanoseconds
        ):
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.PRE_ATTEMPT_SOURCE,
            )
        if candidate.admission_ordinal is None or candidate.admission_ordinal <= 0:
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.MISSING_ADMISSION_ORDINAL,
            )
        if candidate.connection_id == self.original_connection_id:
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.ORIGINAL_CONNECTION_REUSED,
            )
        if (
            candidate.connection_id != current_connection
            or candidate.connection_id in self.excluded_connection_ids
        ):
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.DISALLOWED_CONNECTION,
            )
        age_ns = observed_monotonic.nanoseconds - candidate.received_monotonic.nanoseconds
        if age_ns > self.bounds.maximum_snapshot_age_ns:
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.STALE_SOURCE,
            )
        if not set(candidate.instrument_ids).issubset(self.required_instrument_ids):
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.UNRELATED_INSTRUMENT,
            )
        captured_by_instrument = {
            item.instrument_id: item for item in self.captured_snapshot_sources
        }
        if self.captured_snapshot_sources and (
            candidate.ingest_seq < max(item.ingest_seq for item in self.captured_snapshot_sources)
            or candidate.received_monotonic.nanoseconds
            < max(item.received_monotonic.nanoseconds for item in self.captured_snapshot_sources)
            or candidate.admission_ordinal
            < max(item.admission_ordinal for item in self.captured_snapshot_sources)
        ):
            return self._reject_snapshot(
                candidate=candidate,
                observed_monotonic=observed_monotonic,
                failure=ReplacementRecoveryFailure.NON_MONOTONIC_SOURCE,
            )
        for instrument_id in candidate.instrument_ids:
            prior = captured_by_instrument.get(instrument_id)
            if prior is not None and (
                candidate.ingest_seq < prior.ingest_seq
                or candidate.received_monotonic.nanoseconds < prior.received_monotonic.nanoseconds
                or candidate.admission_ordinal < prior.admission_ordinal
            ):
                return self._reject_snapshot(
                    candidate=candidate,
                    observed_monotonic=observed_monotonic,
                    failure=ReplacementRecoveryFailure.NON_MONOTONIC_SOURCE,
                )
        for instrument_id in candidate.instrument_ids:
            captured_by_instrument[instrument_id] = CapturedReplacementSnapshot(
                instrument_id=instrument_id,
                connection_id=candidate.connection_id,
                admission_ordinal=candidate.admission_ordinal,
                ingest_seq=candidate.ingest_seq,
                received_monotonic=candidate.received_monotonic,
                raw_source=candidate.raw_source,
            )
        captured = tuple(
            captured_by_instrument[key]
            for key in sorted(captured_by_instrument, key=lambda item: item.value)
        )
        complete = tuple(item.instrument_id for item in captured) == self.required_instrument_ids
        result = (
            ReplacementRecoveryResult.SNAPSHOT_ACCEPTED
            if complete
            else ReplacementRecoveryResult.PARTIAL_COVERAGE
        )
        return self._with_event(
            event=result,
            payload={"candidate": candidate, "observed_monotonic": observed_monotonic},
            failure=(None if complete else ReplacementRecoveryFailure.PARTIAL_REQUIRED_SET),
            attempt_event=False,
            captured_snapshot_sources=captured,
        )

    def succeed_attempt(self, *, decided_monotonic: MonotonicTimestamp) -> Self:
        """Freeze complete, fresh binary-leg coverage before durable release."""

        self._require_nonterminal()
        if self.status is not ReplacementRecoveryStatus.ATTEMPT_ACTIVE:
            raise ReplacementRecoveryTransitionError(
                "replacement capture can succeed only from an active attempt"
            )
        self._require_epoch(decided_monotonic, label="capture success")
        exhausted = self._deadline_exhaustion(decided_monotonic)
        if exhausted is not None:
            return exhausted
        failure: ReplacementRecoveryFailure | None = None
        if not self.coverage_complete:
            failure = ReplacementRecoveryFailure.PARTIAL_REQUIRED_SET
        elif any(
            source.received_monotonic.nanoseconds > decided_monotonic.nanoseconds
            for source in self.captured_snapshot_sources
        ):
            failure = ReplacementRecoveryFailure.FUTURE_SOURCE
        elif any(
            decided_monotonic.nanoseconds - source.received_monotonic.nanoseconds
            > self.bounds.maximum_snapshot_age_ns
            for source in self.captured_snapshot_sources
        ):
            failure = ReplacementRecoveryFailure.STALE_SOURCE
        if failure is not None:
            return self._with_event(
                event=ReplacementRecoveryResult.CAPTURE_SUCCESS_REJECTED,
                payload={"decided_monotonic": decided_monotonic},
                failure=failure,
                attempt_event=True,
            )
        return self._with_event(
            event=ReplacementRecoveryResult.CAPTURE_SUCCEEDED,
            payload={
                "captured_snapshot_sources": self.captured_snapshot_sources,
                "decided_monotonic": decided_monotonic,
            },
            failure=None,
            attempt_event=True,
            status=ReplacementRecoveryStatus.CAPTURE_COMPLETE,
        )

    def abandon_attempt(
        self,
        *,
        failure: ReplacementRecoveryFailure,
        abandoned_monotonic: MonotonicTimestamp,
        source: RawSourceRef | None,
    ) -> Self:
        """End the current attempt without changing recovery/contributor identity."""

        self._require_nonterminal()
        if self.status not in {
            ReplacementRecoveryStatus.ATTEMPT_ACTIVE,
            ReplacementRecoveryStatus.CAPTURE_COMPLETE,
        }:
            raise ReplacementRecoveryTransitionError(
                "replacement attempt can be abandoned only while active or capture-complete"
            )
        if failure in {
            ReplacementRecoveryFailure.MAXIMUM_ATTEMPTS_REACHED,
            ReplacementRecoveryFailure.HARD_DEADLINE_REACHED,
            ReplacementRecoveryFailure.RUN_DEADLINE_REACHED,
            ReplacementRecoveryFailure.TERMINAL_CUTOFF,
        }:
            raise ReplacementRecoveryTransitionError(
                "replacement attempt abandonment cannot substitute for terminal exhaustion"
            )
        self._require_epoch(abandoned_monotonic, label="attempt abandonment")
        exhausted = self._deadline_exhaustion(abandoned_monotonic)
        if exhausted is not None:
            return exhausted
        latest = self.latest_attempt_monotonic
        if latest is None or abandoned_monotonic.nanoseconds < latest.nanoseconds:
            raise ReplacementRecoveryTransitionError(
                "replacement attempt abandonment precedes its start"
            )
        return self._with_event(
            event=ReplacementRecoveryResult.ATTEMPT_ABANDONED,
            payload={"abandoned_monotonic": abandoned_monotonic, "source": source},
            failure=failure,
            attempt_event=True,
            status=ReplacementRecoveryStatus.ATTEMPT_ABANDONED,
        )

    def schedule_retry(self, *, decided_monotonic: MonotonicTimestamp) -> Self:
        """Schedule the exact configured retry delay after an abandoned attempt."""

        self._require_nonterminal()
        if self.status is not ReplacementRecoveryStatus.ATTEMPT_ABANDONED:
            raise ReplacementRecoveryTransitionError(
                "replacement retry can be scheduled only after attempt abandonment"
            )
        self._require_epoch(decided_monotonic, label="retry decision")
        exhausted = self._deadline_exhaustion(decided_monotonic)
        if exhausted is not None:
            return exhausted
        if self.attempt_count >= self.bounds.maximum_attempts:
            return self.exhaust_maximum_attempts(at=decided_monotonic)
        latest = self.latest_attempt_monotonic
        if latest is None or decided_monotonic.nanoseconds < latest.nanoseconds:
            raise ReplacementRecoveryTransitionError("replacement retry decision regressed")
        retry_ns = decided_monotonic.nanoseconds + self.bounds.retry_delay_ns
        if retry_ns > MAXIMUM_REPLACEMENT_RECOVERY_NANOSECONDS:
            raise ReplacementRecoveryTransitionError(
                "replacement retry deadline exceeds the monotonic hard maximum"
            )
        next_retry = MonotonicTimestamp(decided_monotonic.epoch_id, retry_ns)
        return self._with_event(
            event=ReplacementRecoveryResult.RETRY_SCHEDULED,
            payload={
                "decided_monotonic": decided_monotonic,
                "next_retry_monotonic": next_retry,
            },
            failure=self.last_failure,
            attempt_event=True,
            status=ReplacementRecoveryStatus.RETRY_SCHEDULED,
            next_retry_monotonic=next_retry,
        )

    def release(
        self,
        *,
        release_source: RawSourceRef,
        release_ingest_seq: int,
        release_monotonic: MonotonicTimestamp,
    ) -> Self:
        """Accept one durable release strictly after complete fresh coverage."""

        self._require_nonterminal()
        if self.status is not ReplacementRecoveryStatus.CAPTURE_COMPLETE:
            raise ReplacementRecoveryTransitionError(
                "replacement release requires successful complete capture"
            )
        _require_integer(release_ingest_seq, label="release sequence")
        self._require_epoch(release_monotonic, label="release")
        exhausted = self._deadline_exhaustion(release_monotonic)
        if exhausted is not None:
            return exhausted
        maximum_capture_seq = max(item.ingest_seq for item in self.captured_snapshot_sources)
        maximum_capture_ns = max(
            item.received_monotonic.nanoseconds for item in self.captured_snapshot_sources
        )
        if (
            release_ingest_seq <= maximum_capture_seq
            or release_monotonic.nanoseconds < maximum_capture_ns
        ):
            raise ReplacementRecoveryTransitionError(
                "replacement release must follow every captured source"
            )
        if any(
            release_monotonic.nanoseconds - item.received_monotonic.nanoseconds
            > self.bounds.maximum_snapshot_age_ns
            for item in self.captured_snapshot_sources
        ):
            return self.abandon_attempt(
                failure=ReplacementRecoveryFailure.FRESHNESS_EXPIRED,
                abandoned_monotonic=release_monotonic,
                source=release_source,
            )
        return self._with_event(
            event=ReplacementRecoveryResult.RELEASED,
            payload={
                "release_ingest_seq": release_ingest_seq,
                "release_monotonic": release_monotonic,
                "release_source": release_source,
            },
            failure=None,
            attempt_event=False,
            status=ReplacementRecoveryStatus.RELEASED,
            release_source=release_source,
            release_ingest_seq=release_ingest_seq,
            release_monotonic=release_monotonic,
        )

    def reject_release(
        self,
        *,
        rejection_source: RawSourceRef,
        rejected_monotonic: MonotonicTimestamp,
    ) -> Self:
        """Retain one attributable durable release-control rejection."""

        self._require_nonterminal()
        self._require_epoch(rejected_monotonic, label="release rejection")
        exhausted = self._deadline_exhaustion(rejected_monotonic)
        if exhausted is not None:
            return exhausted
        if rejected_monotonic.nanoseconds < self.resolution_monotonic.nanoseconds:
            raise ReplacementRecoveryTransitionError(
                "replacement release rejection precedes resolution"
            )
        return self._with_event(
            event=ReplacementRecoveryResult.RELEASE_REJECTED,
            payload={
                "rejected_monotonic": rejected_monotonic,
                "rejection_source": rejection_source,
            },
            failure=ReplacementRecoveryFailure.RELEASE_CONTROL_REJECTED,
            attempt_event=True,
        )

    def record_release_not_projected(
        self,
        *,
        decided_monotonic: MonotonicTimestamp,
    ) -> Self:
        """Retain an explicit durable verdict when no release can be projected."""

        self._require_nonterminal()
        if self.status is not ReplacementRecoveryStatus.CAPTURE_COMPLETE:
            raise ReplacementRecoveryTransitionError(
                "replacement release projection requires complete capture"
            )
        self._require_epoch(decided_monotonic, label="release projection")
        exhausted = self._deadline_exhaustion(decided_monotonic)
        if exhausted is not None:
            return exhausted
        if decided_monotonic.nanoseconds < max(
            source.received_monotonic.nanoseconds for source in self.captured_snapshot_sources
        ):
            raise ReplacementRecoveryTransitionError(
                "replacement release projection precedes captured evidence"
            )
        return self._with_event(
            event=ReplacementRecoveryResult.RELEASE_NOT_PROJECTED,
            payload={"decided_monotonic": decided_monotonic},
            failure=ReplacementRecoveryFailure.RELEASE_CONTROL_NOT_PROJECTED,
            attempt_event=False,
        )

    def exhaust_maximum_attempts(self, *, at: MonotonicTimestamp) -> Self:
        self._require_nonterminal()
        self._require_epoch(at, label="maximum-attempt exhaustion")
        if self.attempt_count < self.bounds.maximum_attempts:
            raise ReplacementRecoveryTransitionError(
                "replacement recovery has not reached its maximum attempts"
            )
        return self._with_event(
            event=ReplacementRecoveryResult.MAXIMUM_ATTEMPTS_EXHAUSTED,
            payload={"at": at, "attempt_count": self.attempt_count},
            failure=ReplacementRecoveryFailure.MAXIMUM_ATTEMPTS_REACHED,
            attempt_event=True,
            status=ReplacementRecoveryStatus.MAXIMUM_ATTEMPTS_EXHAUSTED,
            next_retry_monotonic=None,
        )

    def exhaust_hard_deadline(self, *, at: MonotonicTimestamp) -> Self:
        self._require_nonterminal()
        self._require_epoch(at, label="hard-deadline exhaustion")
        if at.nanoseconds < self.hard_deadline.nanoseconds:
            raise ReplacementRecoveryTransitionError(
                "replacement recovery hard deadline has not been reached"
            )
        return self._with_event(
            event=ReplacementRecoveryResult.HARD_DEADLINE_EXHAUSTED,
            payload={"at": at, "deadline": self.hard_deadline},
            failure=ReplacementRecoveryFailure.HARD_DEADLINE_REACHED,
            attempt_event=True,
            status=ReplacementRecoveryStatus.HARD_DEADLINE_EXHAUSTED,
            next_retry_monotonic=None,
        )

    def exhaust_run_deadline(self, *, at: MonotonicTimestamp) -> Self:
        self._require_nonterminal()
        self._require_epoch(at, label="run-deadline exhaustion")
        if at.nanoseconds < self.run_deadline.nanoseconds:
            raise ReplacementRecoveryTransitionError(
                "replacement recovery run deadline has not been reached"
            )
        return self._with_event(
            event=ReplacementRecoveryResult.RUN_DEADLINE_EXHAUSTED,
            payload={"at": at, "deadline": self.run_deadline},
            failure=ReplacementRecoveryFailure.RUN_DEADLINE_REACHED,
            attempt_event=True,
            status=ReplacementRecoveryStatus.RUN_DEADLINE_EXHAUSTED,
            next_retry_monotonic=None,
        )

    def terminalize(self, *, cutoff_monotonic: MonotonicTimestamp) -> Self:
        """Freeze unresolved terminal state without inventing a recovery."""

        self._require_nonterminal()
        self._require_epoch(cutoff_monotonic, label="terminal cutoff")
        if cutoff_monotonic.nanoseconds < self.resolution_monotonic.nanoseconds:
            raise ReplacementRecoveryTransitionError(
                "replacement terminal cutoff precedes resolution"
            )
        exhausted = self._deadline_exhaustion(cutoff_monotonic)
        if exhausted is not None:
            return exhausted
        return self._with_event(
            event=ReplacementRecoveryResult.TERMINAL_UNRESOLVED,
            payload={"cutoff_monotonic": cutoff_monotonic},
            failure=ReplacementRecoveryFailure.TERMINAL_CUTOFF,
            attempt_event=True,
            status=ReplacementRecoveryStatus.TERMINAL_UNRESOLVED,
            next_retry_monotonic=None,
        )

    def to_record(self) -> dict[str, object]:
        """Return deterministic current authority suitable for durable evidence."""

        return {
            "attempt_chain_sha256": self.attempt_chain_sha256,
            "attempt_count": self.attempt_count,
            "attempted_connection_ids": self.attempted_connection_ids,
            "bounds": self.bounds,
            "captured_snapshot_sources": self.captured_snapshot_sources,
            "contribution_id": self.contribution_id,
            "current_connection_id": self.current_connection_id,
            "divergence_id": self.divergence_id,
            "evidence_chain_sha256": self.evidence_chain_sha256,
            "evidence_event_count": self.evidence_event_count,
            "excluded_connection_ids": self.excluded_connection_ids,
            "first_attempt_monotonic": self.first_attempt_monotonic,
            "hard_deadline": self.hard_deadline,
            "last_failure": self.last_failure,
            "last_result": self.last_result,
            "latest_attempt_monotonic": self.latest_attempt_monotonic,
            "next_retry_monotonic": self.next_retry_monotonic,
            "original_connection_id": self.original_connection_id,
            "recovery_id": self.recovery_id,
            "release_ingest_seq": self.release_ingest_seq,
            "release_monotonic": self.release_monotonic,
            "release_source": self.release_source,
            "required_instrument_ids": self.required_instrument_ids,
            "resolution_ingest_seq": self.resolution_ingest_seq,
            "resolution_id": self.resolution_id,
            "resolution_monotonic": self.resolution_monotonic,
            "resolution_source": self.resolution_source,
            "run_deadline": self.run_deadline,
            "schema_version": self.schema_version,
            "stable_contributor_key": self.stable_contributor_key,
            "status": self.status,
            "unresolved": self.unresolved,
        }


__all__ = [
    "MAXIMUM_REPLACEMENT_RECOVERY_ATTEMPTS",
    "MAXIMUM_REPLACEMENT_RECOVERY_EXCLUDED_CONNECTIONS",
    "MAXIMUM_REPLACEMENT_RECOVERY_HARD_DURATION_NS",
    "MAXIMUM_REPLACEMENT_RECOVERY_NANOSECONDS",
    "MAXIMUM_REPLACEMENT_RECOVERY_REQUIRED_INSTRUMENTS",
    "MAXIMUM_REPLACEMENT_RECOVERY_RETRY_DELAY_NS",
    "REPLACEMENT_RECOVERY_LIFECYCLE_SCHEMA",
    "REPLACEMENT_RECOVERY_RESOLUTION_ID_SCHEMA",
    "REPLACEMENT_RECOVERY_SCHEMA_VERSION",
    "CapturedReplacementSnapshot",
    "ReplacementRecoveryAuthority",
    "ReplacementRecoveryBounds",
    "ReplacementRecoveryFailure",
    "ReplacementRecoveryLifecycleAction",
    "ReplacementRecoveryLifecycleCommand",
    "ReplacementRecoveryResult",
    "ReplacementRecoveryStatus",
    "ReplacementRecoveryTransitionError",
    "ReplacementSnapshotCandidate",
]
