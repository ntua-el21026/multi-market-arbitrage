from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityAuthoritySnapshot,
    AuditAvailabilityContributionChainSnapshot,
    audit_availability_chain_seed,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
)
from multi_market_arbitrage.core.ids import CollectorRunId, IngestId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION,
    AVAILABILITY_INTERVAL_SCHEMA_VERSION,
    AvailabilityCause,
    AvailabilityEvidenceSummary,
    AvailabilityInterval,
    AvailabilityIntervalContribution,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityScope,
    SupervisedAvailabilityIntervalWriter,
    reconcile_availability_contributions,
    reconcile_availability_intervals,
    reconcile_published_availability_evidence,
)


def _interval(
    run_id: CollectorRunId,
    *,
    interval_id: str = "interval-1",
    start_ns: int = 10,
    end_ns: int = 20,
    start_utc_ns: int = 1_000,
    end_utc_ns: int = 2_000,
    cause: AvailabilityCause = AvailabilityCause.EXTERNAL_CONNECTION_TRANSPORT,
    recovery_outcome: AvailabilityRecoveryOutcome = AvailabilityRecoveryOutcome.RESTORED,
    research_eligibility_restored: bool = True,
    open_at_shutdown: bool = False,
) -> AvailabilityInterval:
    return AvailabilityInterval(
        interval_id=interval_id,
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="connection-1",
        started_at=UtcTimestamp(start_utc_ns),
        started_monotonic=MonotonicTimestamp(run_id.value, start_ns),
        ended_at=UtcTimestamp(end_utc_ns),
        ended_monotonic=MonotonicTimestamp(run_id.value, end_ns),
        duration_nanoseconds=end_ns - start_ns,
        cause=cause,
        reason_code=f"TEST_{cause.value}",
        source_evidence_ids=("ingest-seq:1",),
        affected_instruments=("instrument-1",),
        affected_scopes=(
            AvailabilityScope.WEBSOCKET_CAPTURE,
            AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
        ),
        recovery_action=AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT,
        recovery_outcome=recovery_outcome,
        research_eligibility_restored=research_eligibility_restored,
        open_at_shutdown=open_at_shutdown,
    )


def _contribution(
    run_id: CollectorRunId,
    *,
    interval_id: str = "interval-1",
    contribution_id: str = "contribution-1",
    start_ns: int = 10,
    end_ns: int = 20,
    initiating: bool = True,
) -> AvailabilityIntervalContribution:
    def source(label: str) -> RawSourceRef:
        return RawSourceRef(
            ingest_id=IngestId(f"{contribution_id}-{label}"),
            payload_sha256=hashlib.sha256(f"{contribution_id}-{label}".encode()).hexdigest(),
        )

    return AvailabilityIntervalContribution(
        contribution_id=contribution_id,
        interval_id=interval_id,
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="connection-1",
        started_at=UtcTimestamp(1_000),
        started_monotonic=MonotonicTimestamp(run_id.value, start_ns),
        ended_at=UtcTimestamp(2_000),
        ended_monotonic=MonotonicTimestamp(run_id.value, end_ns),
        duration_nanoseconds=end_ns - start_ns,
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=(f"audit-request:{contribution_id}",),
        affected_instruments=("instrument-1",),
        affected_scopes=(AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        initiating_contributor=initiating,
        recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
        request_id=f"request-{contribution_id}",
        divergence_id=hashlib.sha256(contribution_id.encode()).hexdigest(),
        instrument_id="instrument-1",
        market_id="market-1",
        audit_start_source=source("start"),
        audit_response_source=source("response"),
        audit_classification_source=source("classification"),
        authoritative_resolution_source=source("resolution"),
        replacement_release_source=source("release"),
    )


def _audit_contribution_chain(
    contributions: tuple[AvailabilityIntervalContribution, ...],
) -> AuditAvailabilityContributionChainSnapshot:
    chain = audit_availability_contribution_chain_seed()
    for contribution in contributions:
        chain = audit_availability_contribution_chain_extend(
            chain,
            contribution.to_record(),
        )
    return AuditAvailabilityContributionChainSnapshot(
        count=len(contributions),
        chain_sha256=chain,
    )


def _interval_for_contributions(
    contributions: tuple[AvailabilityIntervalContribution, ...],
    *,
    recovery_outcome: AvailabilityRecoveryOutcome = AvailabilityRecoveryOutcome.RESTORED,
    research_eligibility_restored: bool = True,
    open_at_shutdown: bool = False,
) -> AvailabilityInterval:
    if not contributions:
        raise ValueError("test interval requires at least one contribution")
    opener = contributions[0]
    last = max(contributions, key=lambda item: item.ended_monotonic.nanoseconds)
    affected_scope_set = {
        scope for contribution in contributions for scope in contribution.affected_scopes
    }
    return AvailabilityInterval(
        interval_id=opener.interval_id,
        collector_run_id=opener.collector_run_id,
        socket_generation=opener.socket_generation,
        connection_id=opener.connection_id,
        started_at=opener.started_at,
        started_monotonic=opener.started_monotonic,
        ended_at=last.ended_at,
        ended_monotonic=last.ended_monotonic,
        duration_nanoseconds=(
            last.ended_monotonic.nanoseconds - opener.started_monotonic.nanoseconds
        ),
        cause=opener.cause,
        reason_code=opener.reason_code,
        source_evidence_ids=opener.source_evidence_ids,
        affected_instruments=tuple(
            sorted(
                {
                    instrument_id
                    for contribution in contributions
                    for instrument_id in contribution.affected_instruments
                }
            )
        ),
        affected_scopes=tuple(scope for scope in AvailabilityScope if scope in affected_scope_set),
        recovery_action=opener.recovery_action,
        recovery_outcome=recovery_outcome,
        research_eligibility_restored=research_eligibility_restored,
        open_at_shutdown=open_at_shutdown,
    )


def _non_audit_contribution(
    run_id: CollectorRunId,
    *,
    interval: AvailabilityInterval | None = None,
    contribution_id: str = "contribution-1",
    start_ns: int | None = None,
    end_ns: int | None = None,
    initiating: bool = True,
) -> AvailabilityIntervalContribution:
    bound_interval = interval or _interval(run_id)
    contribution_start_ns = (
        bound_interval.started_monotonic.nanoseconds if start_ns is None else start_ns
    )
    contribution_end_ns = bound_interval.ended_monotonic.nanoseconds if end_ns is None else end_ns
    return replace(
        _contribution(
            run_id,
            interval_id=bound_interval.interval_id,
            contribution_id=contribution_id,
            start_ns=contribution_start_ns,
            end_ns=contribution_end_ns,
            initiating=initiating,
        ),
        socket_generation=bound_interval.socket_generation,
        connection_id=bound_interval.connection_id,
        started_at=(
            bound_interval.started_at
            if contribution_start_ns == bound_interval.started_monotonic.nanoseconds
            else UtcTimestamp(
                bound_interval.started_at.nanoseconds_since_epoch
                + contribution_start_ns
                - bound_interval.started_monotonic.nanoseconds
            )
        ),
        ended_at=(
            bound_interval.ended_at
            if contribution_end_ns == bound_interval.ended_monotonic.nanoseconds
            else UtcTimestamp(
                bound_interval.started_at.nanoseconds_since_epoch
                + contribution_end_ns
                - bound_interval.started_monotonic.nanoseconds
            )
        ),
        cause=bound_interval.cause,
        reason_code=bound_interval.reason_code,
        source_evidence_ids=bound_interval.source_evidence_ids,
        affected_instruments=bound_interval.affected_instruments,
        affected_scopes=bound_interval.affected_scopes,
        recovery_action=bound_interval.recovery_action,
        recovery_outcome=bound_interval.recovery_outcome,
        request_id=None,
        divergence_id=None,
        instrument_id=None,
        market_id=None,
        audit_start_source=None,
        audit_response_source=None,
        audit_classification_source=None,
        authoritative_resolution_source=None,
        replacement_release_source=None,
    )


async def _publish_current_availability_bundle(
    root: Path,
    interval: AvailabilityInterval,
    contributions: tuple[AvailabilityIntervalContribution, ...],
) -> AvailabilityEvidenceSummary:
    writer = SupervisedAvailabilityIntervalWriter(
        root=root,
        collector_run_id=interval.collector_run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(interval, contributions=contributions)
    return await writer.close(completed=True)


def _rewrite_published_contributions(
    summary: AvailabilityEvidenceSummary,
    records: tuple[dict[str, object], ...],
) -> AvailabilityEvidenceSummary:
    if summary.contribution_path is None:
        raise ValueError("test summary lacks its contribution path")
    payload = b"".join(canonical_json_bytes(record) + b"\n" for record in records)
    summary.contribution_path.write_bytes(payload)
    return replace(
        summary,
        contributions_written=len(records),
        contribution_file_bytes=len(payload),
        contribution_content_sha256=hashlib.sha256(payload).hexdigest(),
    )


@pytest.mark.parametrize("cause", list(AvailabilityCause))
def test_availability_interval_serializes_every_typed_cause(
    cause: AvailabilityCause,
) -> None:
    run_id = CollectorRunId.new()

    record = _interval(run_id, cause=cause).to_record()

    assert record["schema_version"] == AVAILABILITY_INTERVAL_SCHEMA_VERSION
    assert record["cause_type"] == cause
    assert record["started_monotonic_epoch_id"] == run_id.value
    assert record["duration_ns"] == 10


def test_availability_interval_duration_uses_monotonic_when_utc_regresses() -> None:
    run_id = CollectorRunId.new()

    interval = _interval(
        run_id,
        start_utc_ns=2_000,
        end_utc_ns=1_000,
    )

    assert interval.duration_nanoseconds == 10


def test_availability_contribution_binds_exactly_one_union_interval() -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)

    record = contribution.to_record()

    assert record["schema_version"] == AVAILABILITY_INTERVAL_CONTRIBUTION_SCHEMA_VERSION
    assert record["interval_id"] == "interval-1"
    assert record["contribution_id"] == "contribution-1"
    assert record["source_evidence_ids"] == ("audit-request:contribution-1",)
    assert record["request_id"] == "request-contribution-1"
    assert record["joined_existing_interval"] is False
    with pytest.raises(ValueError, match="another run epoch"):
        replace(
            contribution,
            ended_monotonic=MonotonicTimestamp("another-run", 20),
        )


def test_non_audit_contribution_rejects_recovery_only_audit_provenance() -> None:
    run_id = CollectorRunId.new()
    audit_contribution = _contribution(run_id)
    source = audit_contribution.authoritative_resolution_source
    assert source is not None
    non_audit = _non_audit_contribution(run_id)

    with pytest.raises(ValueError, match="non-audit contribution"):
        replace(non_audit, authoritative_resolution_source=source)
    with pytest.raises(ValueError, match="non-audit contribution"):
        replace(non_audit, replacement_release_source=source)


def test_availability_interval_rejects_epoch_mismatch() -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)

    with pytest.raises(ValueError, match="another run epoch"):
        replace(
            interval,
            ended_monotonic=MonotonicTimestamp("another-run", 20),
        )


def test_availability_interval_shutdown_state_is_explicit() -> None:
    run_id = CollectorRunId.new()
    interval = _interval(
        run_id,
        recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
        research_eligibility_restored=False,
        open_at_shutdown=True,
    )

    assert interval.open_at_shutdown is True
    assert interval.recovery_outcome is AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN
    with pytest.raises(ValueError, match="cannot restore"):
        replace(interval, research_eligibility_restored=True)


def test_availability_reconciliation_is_exact_and_rejects_overlap() -> None:
    run_id = CollectorRunId.new()
    first = _interval(run_id, interval_id="first", start_ns=10, end_ns=20)
    second = _interval(run_id, interval_id="second", start_ns=20, end_ns=50)

    result = reconcile_availability_intervals(
        (first, second),
        aggregate_interval_count=2,
        aggregate_unavailable_duration_nanoseconds=40,
        aggregate_longest_interval_nanoseconds=30,
        aggregate_open_at_shutdown_intervals=0,
    )

    assert result.reconciled is True
    mismatched = reconcile_availability_intervals(
        (first, second),
        aggregate_interval_count=3,
        aggregate_unavailable_duration_nanoseconds=40,
        aggregate_longest_interval_nanoseconds=30,
        aggregate_open_at_shutdown_intervals=0,
    )
    assert mismatched.reconciled is False
    assert mismatched.discrepancies == ("INTERVAL_COUNT",)

    overlapping = _interval(
        run_id,
        interval_id="overlap",
        start_ns=19,
        end_ns=25,
    )
    with pytest.raises(ValueError, match="overlap"):
        reconcile_availability_intervals(
            (first, overlapping),
            aggregate_interval_count=2,
            aggregate_unavailable_duration_nanoseconds=16,
            aggregate_longest_interval_nanoseconds=10,
            aggregate_open_at_shutdown_intervals=0,
        )


def test_contribution_reconciliation_binds_each_mismatch_and_instrument_duration() -> None:
    run_id = CollectorRunId.new()
    first = _contribution(run_id)
    second = _contribution(
        run_id,
        contribution_id="contribution-2",
        start_ns=15,
        initiating=False,
    )
    interval = _interval_for_contributions((first, second))

    result = reconcile_availability_contributions(
        (interval,),
        (first, second),
        expected_mismatch_request_ids=(first.request_id or "", second.request_id or ""),
    )

    assert result.reconciled
    assert result.audit_mismatch_contribution_count == 2
    # Per-instrument research ineligibility is the interval union, never the
    # sum of overlapping contributors for the same instrument.
    assert result.per_instrument_duration_nanoseconds == (("instrument-1", 10),)
    missing = reconcile_availability_contributions(
        (interval,),
        (first,),
        expected_mismatch_request_ids=(first.request_id or "", second.request_id or ""),
    )
    assert not missing.reconciled
    assert missing.discrepancies == ("MISMATCH_BINDING_SET",)


@pytest.mark.parametrize("wrong_field", ["source", "instrument", "divergence"])
def test_contribution_reconciliation_rejects_wrong_exact_binding(wrong_field: str) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    interval = _interval_for_contributions((contribution,))
    expected = contribution.audit_availability_binding()
    assert expected is not None
    if wrong_field == "source":
        wrong = replace(
            contribution,
            audit_start_source=RawSourceRef(
                ingest_id=IngestId("wrong-start-source"),
                payload_sha256=hashlib.sha256(b"wrong-start-source").hexdigest(),
            ),
        )
    elif wrong_field == "instrument":
        wrong = replace(
            contribution,
            instrument_id="wrong-instrument",
            affected_instruments=("wrong-instrument",),
        )
    else:
        wrong = replace(
            contribution,
            divergence_id=hashlib.sha256(b"wrong-divergence").hexdigest(),
        )

    result = reconcile_availability_contributions(
        (interval,),
        (wrong,),
        expected_mismatch_bindings=(expected,),
    )

    assert not result.reconciled
    assert "MISMATCH_EXACT_BINDING_SET" in result.discrepancies


def test_explicit_empty_binding_authority_rejects_unexpected_mismatch() -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    interval = _interval_for_contributions((contribution,))

    result = reconcile_availability_contributions(
        (interval,),
        (contribution,),
        expected_mismatch_bindings=(),
    )

    assert not result.reconciled
    assert result.discrepancies == ("MISMATCH_EXACT_BINDING_SET",)


def test_contribution_reconciliation_rejects_interval_without_contributors() -> None:
    run_id = CollectorRunId.new()

    result = reconcile_availability_contributions((_interval(run_id),), ())

    assert not result.reconciled
    assert result.discrepancies == ("MISSING_INTERVAL_CONTRIBUTION",)


@pytest.mark.parametrize(
    ("mutation", "expected_discrepancy"),
    [
        ("no_opener", "INTERVAL_INITIATING_CONTRIBUTOR_COUNT"),
        ("two_openers", "INTERVAL_INITIATING_CONTRIBUTOR_COUNT"),
        ("gap", "INTERVAL_RANGE_UNION_COVERAGE"),
        ("partial", "INTERVAL_RANGE_UNION_COVERAGE"),
    ],
)
def test_contribution_reconciliation_rejects_noncanonical_interval_bundle(
    mutation: str,
    expected_discrepancy: str,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)
    first = _non_audit_contribution(
        run_id,
        interval=interval,
        contribution_id="first",
        end_ns=15,
    )
    second = _non_audit_contribution(
        run_id,
        interval=interval,
        contribution_id="second",
        start_ns=15,
        initiating=False,
    )
    if mutation == "no_opener":
        contributions = (replace(first, initiating_contributor=False), second)
    elif mutation == "two_openers":
        contributions = (first, replace(second, initiating_contributor=True))
    elif mutation == "gap":
        contributions = (
            first,
            replace(
                second,
                started_at=UtcTimestamp(1_006),
                started_monotonic=MonotonicTimestamp(run_id.value, 16),
                duration_nanoseconds=4,
            ),
        )
    else:
        contributions = (first,)

    result = reconcile_availability_contributions((interval,), contributions)

    assert not result.reconciled
    assert expected_discrepancy in result.discrepancies


@pytest.mark.parametrize(
    ("field", "expected_discrepancy"),
    [
        ("run", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("generation", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("connection", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("start", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("cause", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("reason", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("source", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("action", "INITIATING_CONTRIBUTOR_INTERVAL_CONFLICT"),
        ("instruments", "INTERVAL_INSTRUMENT_UNION"),
        ("scopes", "INTERVAL_SCOPE_UNION"),
    ],
)
def test_contribution_reconciliation_rejects_opener_interval_disagreement(
    field: str,
    expected_discrepancy: str,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)
    contribution = _non_audit_contribution(run_id, interval=interval)
    if field == "run":
        contribution = _non_audit_contribution(CollectorRunId.new(), interval=interval)
    elif field == "generation":
        contribution = replace(contribution, socket_generation=2)
    elif field == "connection":
        contribution = replace(contribution, connection_id="different-connection")
    elif field == "start":
        contribution = replace(contribution, started_at=UtcTimestamp(999))
    elif field == "cause":
        contribution = replace(contribution, cause=AvailabilityCause.UNKNOWN)
    elif field == "reason":
        contribution = replace(contribution, reason_code="DIFFERENT_REASON")
    elif field == "source":
        contribution = replace(contribution, source_evidence_ids=("different-source",))
    elif field == "action":
        contribution = replace(
            contribution,
            recovery_action=AvailabilityRecoveryAction.NONE,
        )
    elif field == "instruments":
        contribution = replace(contribution, affected_instruments=())
    else:
        contribution = replace(
            contribution,
            affected_scopes=(AvailabilityScope.WEBSOCKET_CAPTURE,),
        )

    result = reconcile_availability_contributions((interval,), (contribution,))

    assert not result.reconciled
    assert expected_discrepancy in result.discrepancies


def test_contribution_reconciliation_rejects_interval_id_reassignment() -> None:
    run_id = CollectorRunId.new()
    first_interval = _interval(
        run_id,
        interval_id="first",
        start_ns=10,
        end_ns=20,
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
    )
    second_interval = _interval(
        run_id,
        interval_id="second",
        start_ns=20,
        end_ns=30,
        cause=AvailabilityCause.AUDIT_MISMATCH_RESYNCHRONIZATION,
    )
    contribution = _contribution(
        run_id,
        interval_id="first",
        start_ns=20,
        end_ns=20,
    )
    binding = contribution.audit_availability_binding()
    assert binding is not None

    result = reconcile_availability_contributions(
        (first_interval, second_interval),
        (replace(contribution, interval_id="second"),),
        expected_mismatch_bindings=(binding,),
        expected_mismatch_contributions=_audit_contribution_chain((contribution,)),
    )

    assert not result.reconciled
    assert "MISMATCH_EXACT_CONTRIBUTION_CHAIN" in result.discrepancies


@given(
    segments=st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=100),
            st.integers(min_value=1, max_value=30),
        ),
        min_size=1,
        max_size=8,
    )
)
@settings(max_examples=75, deadline=None, derandomize=True)
def test_contribution_instrument_duration_is_exact_interval_union(
    segments: list[tuple[int, int]],
) -> None:
    run_id = CollectorRunId("availability-union-property")
    ranges = tuple(sorted((start, start + length) for start, length in segments))
    contributions = tuple(
        _contribution(
            run_id,
            contribution_id=f"property-{index}",
            start_ns=start,
            end_ns=end,
            initiating=index == 0,
        )
        for index, (start, end) in enumerate(ranges)
    )
    interval = _interval_for_contributions(contributions)
    bindings = tuple(
        binding
        for contribution in contributions
        if (binding := contribution.audit_availability_binding()) is not None
    )

    result = reconcile_availability_contributions(
        (interval,),
        contributions,
        expected_mismatch_bindings=bindings,
        expected_mismatch_contributions=_audit_contribution_chain(contributions),
    )

    expected_duration = len(
        {nanosecond for start, end in ranges for nanosecond in range(start, end)}
    )
    assert result.reconciled is (expected_duration == interval.duration_nanoseconds)
    if expected_duration != interval.duration_nanoseconds:
        assert "INTERVAL_RANGE_UNION_COVERAGE" in result.discrepancies
    assert result.per_instrument_duration_nanoseconds == (("instrument-1", expected_duration),)


def test_audit_availability_authority_enforces_open_release_invariant() -> None:
    with pytest.raises(ValueError, match="current binding count"):
        AuditAvailabilityAuthoritySnapshot(
            opened=1,
            resolved=0,
            released=0,
            legacy_unverifiable=0,
            open_chain_sha256=audit_availability_chain_seed("open"),
            resolution_chain_sha256=audit_availability_chain_seed("resolution"),
            release_chain_sha256=audit_availability_chain_seed("release"),
            current_bindings=(),
        )


def test_audit_availability_authority_enforces_exact_current_phases() -> None:
    run_id = CollectorRunId("availability-authority-phase-exact")
    released = _contribution(run_id).audit_availability_binding()
    assert released is not None
    unresolved = replace(
        released,
        authoritative_resolution_source=None,
        replacement_release_source=None,
    )
    resolved = replace(released, replacement_release_source=None)
    chains = {
        "open_chain_sha256": audit_availability_chain_seed("open"),
        "resolution_chain_sha256": audit_availability_chain_seed("resolution"),
        "release_chain_sha256": audit_availability_chain_seed("release"),
    }

    with pytest.raises(ValueError, match="unresolved binding count"):
        AuditAvailabilityAuthoritySnapshot(
            opened=1,
            resolved=1,
            released=0,
            legacy_unverifiable=0,
            current_bindings=(unresolved,),
            **chains,
        )
    with pytest.raises(ValueError, match="unresolved binding count"):
        AuditAvailabilityAuthoritySnapshot(
            opened=1,
            resolved=0,
            released=0,
            legacy_unverifiable=0,
            current_bindings=(resolved,),
            **chains,
        )
    with pytest.raises(ValueError, match="cannot already be released"):
        AuditAvailabilityAuthoritySnapshot(
            opened=1,
            resolved=1,
            released=0,
            legacy_unverifiable=0,
            current_bindings=(released,),
            **chains,
        )


@pytest.mark.asyncio
async def test_published_availability_reconciliation_checks_exact_audit_chain(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contribution = _contribution(run_id)
    interval = _interval_for_contributions((contribution,))
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(interval, contributions=(contribution,))
    summary = await writer.close(completed=True)

    result = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=summary.audit_contributions_written,
            chain_sha256=summary.audit_contribution_written_chain_sha256,
        ),
    )
    wrong_authority = reconcile_published_availability_evidence(
        summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=summary.audit_contribution_written_chain_sha256,
        ),
    )

    assert result.reconciled
    assert not wrong_authority.reconciled
    assert wrong_authority.discrepancies == ("AUDIT_CONTRIBUTION_COUNT",)


@pytest.mark.parametrize(
    "malformed_field",
    ["authoritative_resolution_source", "replacement_release_source"],
)
@pytest.mark.asyncio
async def test_published_non_audit_contribution_rejects_recovery_only_provenance(
    tmp_path: Path,
    malformed_field: str,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)
    audit_contribution = _contribution(run_id)
    source = audit_contribution.authoritative_resolution_source
    assert source is not None
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(
        interval,
        contributions=(_non_audit_contribution(run_id),),
    )
    summary = await writer.close(completed=True)
    assert summary.contribution_path is not None
    record = json.loads(summary.contribution_path.read_text(encoding="utf-8"))
    record[malformed_field] = {
        "ingest_id": source.ingest_id.value,
        "payload_sha256": source.payload_sha256,
    }
    malformed_payload = canonical_json_bytes(record) + b"\n"
    summary.contribution_path.write_bytes(malformed_payload)
    malformed_summary = replace(
        summary,
        contribution_file_bytes=len(malformed_payload),
        contribution_content_sha256=hashlib.sha256(malformed_payload).hexdigest(),
    )

    result = reconcile_published_availability_evidence(
        malformed_summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=summary.audit_contributions_written,
            chain_sha256=summary.audit_contribution_written_chain_sha256,
        ),
    )

    assert not result.reconciled
    assert result.discrepancies == ("PUBLISHED_LEDGER_INVALID:ValueError",)


@pytest.mark.parametrize(
    ("mutation", "expected_discrepancy"),
    [
        ("zero_contributions", "CONTRIBUTION_MISSING_INTERVAL_CONTRIBUTION"),
        ("no_opener", "CONTRIBUTION_INTERVAL_INITIATING_CONTRIBUTOR_COUNT"),
        ("two_openers", "CONTRIBUTION_INTERVAL_INITIATING_CONTRIBUTOR_COUNT"),
        ("gap", "CONTRIBUTION_INTERVAL_RANGE_UNION_COVERAGE"),
        ("partial", "CONTRIBUTION_INTERVAL_RANGE_UNION_COVERAGE"),
    ],
)
@pytest.mark.asyncio
async def test_published_reconciliation_rejects_noncanonical_contribution_bundle(
    tmp_path: Path,
    mutation: str,
    expected_discrepancy: str,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)
    full = _non_audit_contribution(run_id, interval=interval)
    first = _non_audit_contribution(
        run_id,
        interval=interval,
        contribution_id="first",
        end_ns=15,
    )
    second = _non_audit_contribution(
        run_id,
        interval=interval,
        contribution_id="second",
        start_ns=15,
        initiating=False,
    )
    valid_contributions = (first, second) if mutation in {"two_openers", "gap"} else (full,)
    summary = await _publish_current_availability_bundle(
        tmp_path,
        interval,
        valid_contributions,
    )
    assert summary.contribution_path is not None
    records: tuple[dict[str, object], ...] = tuple(
        json.loads(line)
        for line in summary.contribution_path.read_text(encoding="utf-8").splitlines()
    )
    if mutation == "zero_contributions":
        records = ()
    elif mutation == "no_opener":
        records[0]["initiating_contributor"] = False
        records[0]["joined_existing_interval"] = True
    elif mutation == "two_openers":
        records[1]["initiating_contributor"] = True
        records[1]["joined_existing_interval"] = False
    elif mutation == "gap":
        records[1]["started_at_utc_ns"] = 1_006
        records[1]["started_monotonic_ns"] = 16
        records[1]["duration_ns"] = 4
    else:
        records[0]["ended_at_utc_ns"] = 1_009
        records[0]["ended_monotonic_ns"] = 19
        records[0]["duration_ns"] = 9
    malformed_summary = _rewrite_published_contributions(summary, records)

    result = reconcile_published_availability_evidence(
        malformed_summary,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=0,
            chain_sha256=audit_availability_contribution_chain_seed(),
        ),
    )

    assert not result.reconciled
    assert expected_discrepancy in result.discrepancies


@pytest.mark.asyncio
async def test_availability_writer_publishes_empty_successful_ledger(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.written == 0
    assert summary.path.read_bytes() == b""
    assert summary.content_sha256 is not None


@pytest.mark.asyncio
async def test_availability_writer_publishes_and_reconciles_semantics(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(run_id)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert (
        writer.enqueue_nowait(
            interval,
            contributions=(_non_audit_contribution(run_id, interval=interval),),
        )
        is True
    )

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.accepted == summary.written == 1
    assert summary.unavailable_duration_nanoseconds == interval.duration_nanoseconds
    assert summary.longest_interval_nanoseconds == interval.duration_nanoseconds
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["interval_id"] == interval.interval_id


@pytest.mark.asyncio
async def test_availability_writer_publishes_companion_contribution_stream(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    contributions = (
        _contribution(run_id),
        _contribution(
            run_id,
            contribution_id="contribution-2",
            start_ns=15,
            initiating=False,
        ),
    )
    interval = _interval_for_contributions(contributions)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(interval, contributions=contributions) is True

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is True
    assert summary.contributions_accepted == summary.contributions_written == 2
    assert summary.contribution_content_sha256 is not None
    assert summary.contribution_path is not None
    records = tuple(
        json.loads(line)
        for line in summary.contribution_path.read_text(encoding="utf-8").splitlines()
    )
    assert tuple(record["interval_id"] for record in records) == (
        interval.interval_id,
        interval.interval_id,
    )
    assert tuple(record["initiating_contributor"] for record in records) == (True, False)


def test_availability_writer_rejects_cross_interval_contribution(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )

    with pytest.raises(ValueError, match="not bound"):
        writer.enqueue_nowait(
            _interval(run_id),
            contributions=(_contribution(run_id, interval_id="different"),),
        )


def test_availability_writer_rejects_interval_without_contributors(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )

    with pytest.raises(ValueError, match="requires at least one contribution"):
        writer.enqueue_nowait(_interval(run_id))


@pytest.mark.asyncio
async def test_incomplete_availability_writer_drains_accepted_terminal_evidence(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    interval = _interval(
        run_id,
        cause=AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN,
        recovery_outcome=AvailabilityRecoveryOutcome.NOT_RESTORED_AT_SHUTDOWN,
        research_eligibility_restored=False,
        open_at_shutdown=True,
    )
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert (
        writer.enqueue_nowait(
            interval,
            contributions=(_non_audit_contribution(run_id, interval=interval),),
        )
        is True
    )

    summary = await writer.close(completed=False)

    assert summary.completed is False
    assert summary.gate1_evidence_eligible is False
    assert summary.failure_type == "RuntimeError"
    assert summary.accepted == summary.written == 1
    assert summary.path.suffix == ".partial"
    record = json.loads(summary.path.read_text(encoding="utf-8"))
    assert record["cause_type"] == AvailabilityCause.PLANNED_OPERATOR_SHUTDOWN.value
    assert record["open_at_shutdown"] is True


@pytest.mark.asyncio
async def test_availability_writer_failure_is_isolated_and_fail_closed(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=1,
    )
    await writer.start()
    interval = _interval(run_id)
    assert (
        writer.enqueue_nowait(
            interval,
            contributions=(_non_audit_contribution(run_id, interval=interval),),
        )
        is True
    )

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is False
    assert summary.failure_type == "RuntimeError"


@pytest.mark.asyncio
async def test_availability_writer_rejects_overlapping_admission(tmp_path: Path) -> None:
    run_id = CollectorRunId.new()
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    first = _interval(run_id, interval_id="first", start_ns=10, end_ns=20)
    overlap = _interval(run_id, interval_id="overlap", start_ns=19, end_ns=30)
    assert (
        writer.enqueue_nowait(
            first,
            contributions=(
                _non_audit_contribution(
                    run_id,
                    interval=first,
                    contribution_id="first-contribution",
                ),
            ),
        )
        is True
    )
    assert (
        writer.enqueue_nowait(
            overlap,
            contributions=(
                _non_audit_contribution(
                    run_id,
                    interval=overlap,
                    contribution_id="overlap-contribution",
                ),
            ),
        )
        is False
    )
    await writer.start()

    summary = await writer.close(completed=True)

    assert summary.gate1_evidence_eligible is False
    assert summary.rejected >= 1
