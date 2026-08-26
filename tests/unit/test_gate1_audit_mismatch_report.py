from __future__ import annotations

import copy
import hashlib
import json
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityAuthoritySnapshot,
    AuditAvailabilityBinding,
    AuditAvailabilityContributionChainSnapshot,
    PublicReadFaultAvailabilityProjectionSnapshot,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
    audit_availability_contribution_chain_extend,
    audit_availability_contribution_chain_seed,
    empty_audit_availability_authority_snapshot,
)
from multi_market_arbitrage.core.ids import CollectorRunId, IngestId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import (
    canonical_json_bytes,
    canonical_sha256,
    canonicalize,
)
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.observability import (
    AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION,
    EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256,
    AvailabilityCausalFamily,
    AvailabilityCause,
    AvailabilityInterval,
    AvailabilityIntervalContribution,
    AvailabilityIntervalContributionV2,
    AvailabilityIntervalV2,
    AvailabilityRecoveryAction,
    AvailabilityRecoveryOutcome,
    AvailabilityReplacementRecoveryStatus,
    AvailabilityReplacementRecoveryV2,
    AvailabilityScope,
    SupervisedAvailabilityIntervalWriter,
    availability_active_period_union_sha256,
    availability_audit_contribution_id,
    availability_contribution_v2_chain_extend,
    availability_interval_reconciliation_id,
    availability_stable_causal_key,
    reconcile_published_availability_evidence,
)

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

from benchmarks.gate1_audit_mismatch_report import (  # noqa: E402
    AvailabilityReporterEvidenceError,
    _load_published_availability_authority,  # pyright: ignore[reportPrivateUsage]
    _ordered_published_bindings,  # pyright: ignore[reportPrivateUsage]
)

_TERMINAL_HASH = "a" * 64


def _nonempty_public_fault_projection() -> PublicReadFaultAvailabilityProjectionSnapshot:
    return PublicReadFaultAvailabilityProjectionSnapshot(
        opened=1,
        completed=0,
        open_chain_sha256="c" * 64,
        completion_chain_sha256="d" * 64,
    )


@dataclass(frozen=True, slots=True)
class _PublishedFixture:
    run_id: CollectorRunId
    summary_path: Path
    interval_path: Path
    contribution_path: Path
    replay_authority: AuditAvailabilityAuthoritySnapshot
    binding: AuditAvailabilityBinding
    document: dict[str, object]


def _source(label: str) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=IngestId(f"reporter-{label}"),
        payload_sha256=hashlib.sha256(label.encode()).hexdigest(),
    )


def _interval(run_id: CollectorRunId) -> AvailabilityInterval:
    return AvailabilityInterval(
        interval_id="reporter-interval",
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="reporter-connection",
        started_at=UtcTimestamp(1_000),
        started_monotonic=MonotonicTimestamp(run_id.value, 10),
        ended_at=UtcTimestamp(2_000),
        ended_monotonic=MonotonicTimestamp(run_id.value, 20),
        duration_nanoseconds=10,
        cause=AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-request:reporter-request",),
        affected_instruments=("reporter-instrument",),
        affected_scopes=(AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
        research_eligibility_restored=True,
        open_at_shutdown=False,
    )


def _contribution(run_id: CollectorRunId) -> AvailabilityIntervalContribution:
    return AvailabilityIntervalContribution(
        contribution_id="reporter-contribution",
        interval_id="reporter-interval",
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="reporter-connection",
        started_at=UtcTimestamp(1_000),
        started_monotonic=MonotonicTimestamp(run_id.value, 10),
        ended_at=UtcTimestamp(2_000),
        ended_monotonic=MonotonicTimestamp(run_id.value, 20),
        duration_nanoseconds=10,
        cause=AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=("audit-request:reporter-request",),
        affected_instruments=("reporter-instrument",),
        affected_scopes=(AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        initiating_contributor=True,
        recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
        request_id="reporter-request",
        divergence_id=hashlib.sha256(b"reporter-divergence").hexdigest(),
        instrument_id="reporter-instrument",
        market_id="reporter-market",
        audit_start_source=_source("start"),
        audit_response_source=_source("response"),
        audit_classification_source=_source("classification"),
        authoritative_resolution_source=_source("resolution"),
        replacement_release_source=_source("release"),
    )


def _v2_transport_contribution(
    run_id: CollectorRunId,
) -> AvailabilityIntervalContributionV2:
    stable_key = canonical_sha256(
        {
            "fault_lifecycle_id": "reporter-v2-transport",
            "schema": "reporter-v2-stable-key",
        }
    )
    contribution_id = canonical_sha256(
        {
            "stable_causal_key": stable_key,
            "schema": "reporter-v2-contribution",
        }
    )
    return AvailabilityIntervalContributionV2(
        contribution_id=contribution_id,
        interval_id="reporter-v2-interval",
        stable_causal_key=stable_key,
        causal_family=AvailabilityCausalFamily.TRANSPORT_INTERRUPTION,
        collector_run_id=run_id,
        socket_generation=1,
        connection_id="reporter-v2-connection",
        started_at=UtcTimestamp(1_000),
        started_monotonic=MonotonicTimestamp(run_id.value, 10),
        latest_observed_at=UtcTimestamp(2_000),
        latest_observed_monotonic=MonotonicTimestamp(run_id.value, 20),
        ended_at=UtcTimestamp(2_000),
        ended_monotonic=MonotonicTimestamp(run_id.value, 20),
        duration_nanoseconds=10,
        cause=AvailabilityCause.TRANSPORT_DISCONNECT,
        reason_code="REPORTER_V2_TRANSPORT",
        source_evidence_ids=("connection:reporter-v2-transport",),
        observation_count=2,
        observation_chain_sha256=hashlib.sha256(b"reporter-v2-observation").hexdigest(),
        affected_instruments=("reporter-v2-instrument",),
        affected_scopes=(AvailabilityScope.WEBSOCKET_CAPTURE,),
        recovery_action=AvailabilityRecoveryAction.RECONNECT_AND_RESNAPSHOT,
        initiating_contributor=True,
        recovery_outcome=AvailabilityRecoveryOutcome.RESTORED,
        completion_sequence=1,
        open_at_shutdown=False,
        closure_reason_code="CONNECTED_GENERATION_RESTORED",
        closure_source_evidence_ids=("connection:reporter-v2-restored",),
        fault_lifecycle_id="reporter-v2-transport",
    )


def _v2_released_audit_contribution(
    run_id: CollectorRunId,
) -> AvailabilityIntervalContributionV2:
    divergence_id = canonical_sha256(
        {
            "collector_run_id": run_id.value,
            "schema": "reporter-v2-audit-divergence",
        }
    )
    stable_key = availability_stable_causal_key(
        causal_family=AvailabilityCausalFamily.AUDIT_DIVERGENCE,
        causal_lifecycle_id=f"divergence:{divergence_id}",
    )
    contribution_id = availability_audit_contribution_id(
        stable_causal_key=stable_key,
    )
    recovery_id = canonical_sha256(
        {
            "divergence_id": divergence_id,
            "schema": "reporter-v2-replacement-recovery",
        }
    )
    recovery = AvailabilityReplacementRecoveryV2(
        recovery_id=recovery_id,
        resolution_id=canonical_sha256(
            {
                "divergence_id": divergence_id,
                "schema": "reporter-v2-authoritative-resolution",
            }
        ),
        contribution_id=contribution_id,
        stable_contributor_key=stable_key,
        divergence_id=divergence_id,
        status=AvailabilityReplacementRecoveryStatus.RELEASED,
        attempt_count=1,
        maximum_attempts=9,
        original_connection_id="reporter-v2-original-connection",
        excluded_connection_count=1,
        excluded_connection_ids_sha256=hashlib.sha256(
            b"reporter-v2-excluded-connections"
        ).hexdigest(),
        attempted_connection_ids=("reporter-v2-replacement-connection",),
        current_connection_id="reporter-v2-replacement-connection",
        first_attempt_monotonic=MonotonicTimestamp(run_id.value, 12),
        latest_attempt_monotonic=MonotonicTimestamp(run_id.value, 12),
        next_retry_monotonic=None,
        hard_deadline=MonotonicTimestamp(run_id.value, 120),
        run_deadline=MonotonicTimestamp(run_id.value, 240),
        required_instrument_ids=("reporter-v2-instrument",),
        captured_instrument_ids=("reporter-v2-instrument",),
        last_result="RELEASED",
        last_failure=None,
        attempt_chain_sha256=hashlib.sha256(b"reporter-v2-attempt-chain").hexdigest(),
        evidence_chain_sha256=hashlib.sha256(b"reporter-v2-evidence-chain").hexdigest(),
        evidence_event_count=5,
    )
    return replace(
        _v2_transport_contribution(run_id),
        contribution_id=contribution_id,
        stable_causal_key=stable_key,
        causal_family=AvailabilityCausalFamily.AUDIT_DIVERGENCE,
        connection_id="reporter-v2-original-connection",
        cause=AvailabilityCause.AUDIT_MISMATCH_RECOVERY,
        reason_code="REST_AUDIT_MISMATCH",
        source_evidence_ids=(f"audit-divergence:{divergence_id}",),
        affected_instruments=("reporter-v2-instrument",),
        affected_scopes=(
            AvailabilityScope.AUDIT_ELIGIBILITY,
            AvailabilityScope.CANONICAL_BOOKS,
            AvailabilityScope.SCANNER_RESEARCH_ELIGIBILITY,
        ),
        recovery_action=AvailabilityRecoveryAction.RESNAPSHOT,
        closure_reason_code="AUDIT_REPLACEMENT_RELEASED",
        closure_source_evidence_ids=(f"replacement-recovery:{recovery_id}",),
        request_id="reporter-v2-request",
        divergence_id=divergence_id,
        fault_lifecycle_id=None,
        recovery_id=recovery_id,
        instrument_id="reporter-v2-instrument",
        market_id="reporter-v2-market",
        audit_start_source=_source("v2-audit-start"),
        audit_response_source=_source("v2-audit-response"),
        audit_classification_source=_source("v2-audit-classification"),
        authoritative_resolution_source=_source("v2-authoritative-resolution"),
        replacement_release_source=_source("v2-replacement-release"),
        replacement_recovery=recovery,
    )


def _v2_interval(
    contribution: AvailabilityIntervalContributionV2,
) -> AvailabilityIntervalV2:
    contributor_chain = availability_contribution_v2_chain_extend(
        EMPTY_AVAILABILITY_CONTRIBUTION_V2_CHAIN_SHA256,
        contribution.to_record(),
    )
    active_union = availability_active_period_union_sha256(
        collector_run_id=contribution.collector_run_id,
        started_monotonic=contribution.started_monotonic,
        ended_monotonic=contribution.ended_monotonic,
    )
    reconciliation_id = availability_interval_reconciliation_id(
        interval_id=contribution.interval_id,
        active_period_union_sha256=active_union,
        total_contributor_count=1,
        completed_contributor_count=1,
        terminal_open_contributor_count=0,
        contributor_chain_sha256=contributor_chain,
    )
    return AvailabilityIntervalV2(
        interval_id=contribution.interval_id,
        collector_run_id=contribution.collector_run_id,
        socket_generation=contribution.socket_generation,
        connection_id=contribution.connection_id,
        started_at=contribution.started_at,
        started_monotonic=contribution.started_monotonic,
        ended_at=contribution.ended_at,
        ended_monotonic=contribution.ended_monotonic,
        duration_nanoseconds=contribution.duration_nanoseconds,
        cause=contribution.cause,
        reason_code=contribution.reason_code,
        source_evidence_ids=contribution.source_evidence_ids,
        affected_instruments=contribution.affected_instruments,
        affected_scopes=contribution.affected_scopes,
        recovery_action=contribution.recovery_action,
        recovery_outcome=contribution.recovery_outcome,
        research_eligibility_restored=True,
        open_at_shutdown=False,
        active_period_union_sha256=active_union,
        total_contributor_count=1,
        completed_contributor_count=1,
        terminal_open_contributor_count=0,
        contributor_chain_sha256=contributor_chain,
        per_cause_counts=((contribution.cause, 1),),
        per_scope_counts=tuple((scope, 1) for scope in contribution.affected_scopes),
        reconciliation_id=reconciliation_id,
    )


def _released_authority(binding: AuditAvailabilityBinding) -> AuditAvailabilityAuthoritySnapshot:
    return _released_authority_for_bindings((binding,))


def _released_authority_for_bindings(
    bindings: tuple[AuditAvailabilityBinding, ...],
) -> AuditAvailabilityAuthoritySnapshot:
    open_chain = audit_availability_chain_seed("open")
    resolution_chain = audit_availability_chain_seed("resolution")
    release_chain = audit_availability_chain_seed("release")
    for binding in bindings:
        open_chain = audit_availability_chain_extend(
            open_chain,
            "open",
            replace(
                binding,
                authoritative_resolution_source=None,
                replacement_release_source=None,
            ),
        )
        resolution_chain = audit_availability_chain_extend(
            resolution_chain,
            "resolution",
            replace(binding, replacement_release_source=None),
        )
        release_chain = audit_availability_chain_extend(
            release_chain,
            "release",
            binding,
        )
    return AuditAvailabilityAuthoritySnapshot(
        opened=len(bindings),
        resolved=len(bindings),
        released=len(bindings),
        legacy_unverifiable=0,
        open_chain_sha256=open_chain,
        resolution_chain_sha256=resolution_chain,
        release_chain_sha256=release_chain,
        current_bindings=(),
    )


def _record(value: object) -> dict[str, object]:
    encoded = canonicalize(value, allow_floats=False)
    assert isinstance(encoded, dict)
    return cast(dict[str, object], encoded)


async def _published_fixture(tmp_path: Path) -> _PublishedFixture:
    run_id = CollectorRunId("authority-aware-reporter")
    contribution = _contribution(run_id)
    binding = contribution.audit_availability_binding()
    assert binding is not None
    authority = _released_authority(binding)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_nowait(_interval(run_id), contributions=(contribution,))
    evidence = await writer.close(completed=True)
    contribution_path = evidence.contribution_path
    assert contribution_path is not None
    relocated = replace(
        evidence,
        path=Path("/stale-host/availability-intervals.jsonl"),
        contribution_path=Path("/stale-host/availability-contributions.jsonl"),
    )
    document: dict[str, object] = {
        "collector_run_id": run_id.value,
        "availability_evidence": _record(relocated),
        "processor": {
            "terminal_hash": _TERMINAL_HASH,
            "rest_audit_mismatches": 1,
            "audit_availability_authority": _record(authority),
        },
        "runtime_health": {
            "availability_ledger_healthy": True,
            "availability_records_emitted": 1,
            "availability_contributions_emitted": 1,
            "availability_open_at_shutdown_intervals": 0,
            "unavailable_intervals": 1,
            "unavailable_total_nanoseconds": 10,
            "longest_unavailable_nanoseconds": 10,
            "current_unavailable_started_monotonic_ns": None,
            "audit_availability_authority": _record(authority),
            "audit_availability_contributions_emitted": {
                "count": evidence.audit_contributions_written,
                "chain_sha256": evidence.audit_contribution_written_chain_sha256,
            },
        },
    }
    summary_path = tmp_path / "observation-summary.json"
    summary_path.write_bytes(canonical_json_bytes(document) + b"\n")
    return _PublishedFixture(
        run_id=run_id,
        summary_path=summary_path,
        interval_path=evidence.path,
        contribution_path=contribution_path,
        replay_authority=authority,
        binding=binding,
        document=document,
    )


def _load(fixture: _PublishedFixture):
    return _load_published_availability_authority(
        fixture.summary_path,
        fixture.interval_path,
        fixture.contribution_path,
        collector_run_id=fixture.run_id,
        replay_authority=fixture.replay_authority,
        ordered_replay_release_bindings=(fixture.binding,),
        replay_rest_audit_mismatches=1,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=64 * 1024,
    )


@pytest.mark.asyncio
async def test_reporter_reconciles_relocated_full_contribution_record(tmp_path: Path) -> None:
    fixture = await _published_fixture(tmp_path)

    published = _load(fixture)

    assert published.reconciliation is not None
    assert published.reconciliation.reconciled
    assert published.bindings == (fixture.binding,)


@pytest.mark.asyncio
async def test_reporter_reconciles_relocated_current_v2_evidence(tmp_path: Path) -> None:
    run_id = CollectorRunId("authority-aware-reporter-v2")
    contribution = _v2_transport_contribution(run_id)
    interval = _v2_interval(contribution)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(interval)
    evidence = await writer.close(completed=True, terminal_active_causal_keys=())
    assert evidence.schema_version == AVAILABILITY_EVIDENCE_SUMMARY_V2_VERSION
    assert evidence.contribution_path is not None
    assert evidence.completion_path is not None
    assert evidence.contribution_file_bytes > evidence.file_bytes
    relocated = replace(
        evidence,
        path=Path("/stale-host/availability-intervals-v2.jsonl"),
        contribution_path=Path("/stale-host/availability-contributions-v2.jsonl"),
        completion_path=Path("/stale-host") / evidence.completion_path.name,
    )
    authority = empty_audit_availability_authority_snapshot()
    document = {
        "collector_run_id": run_id.value,
        "availability_evidence": _record(relocated),
        "processor": {
            "terminal_hash": _TERMINAL_HASH,
            "rest_audit_mismatches": 0,
            "audit_availability_authority": _record(authority),
            "public_read_fault_availability_projection": _record(
                evidence.public_read_fault_projection_written
            ),
        },
        "runtime_health": {
            "availability_ledger_healthy": True,
            "availability_records_emitted": 1,
            "availability_contributions_emitted": 1,
            "availability_open_at_shutdown_intervals": 0,
            "unavailable_intervals": 1,
            "unavailable_total_nanoseconds": 10,
            "longest_unavailable_nanoseconds": 10,
            "current_unavailable_cause": None,
            "current_unavailable_reason_code": None,
            "current_unavailable_started_monotonic_ns": None,
            "audit_availability_authority": _record(authority),
            "audit_availability_contributions_emitted": {
                "count": 0,
                "chain_sha256": evidence.audit_contribution_written_chain_sha256,
            },
            "public_read_fault_availability_projection": _record(
                evidence.public_read_fault_projection_written
            ),
        },
    }
    summary_path = tmp_path / "observation-summary-v2.json"
    summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    published = _load_published_availability_authority(
        summary_path,
        evidence.path,
        evidence.contribution_path,
        collector_run_id=run_id,
        replay_authority=authority,
        ordered_replay_release_bindings=(),
        replay_rest_audit_mismatches=0,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=evidence.file_bytes,
        maximum_contribution_ledger_bytes=evidence.contribution_file_bytes,
    )

    assert published.reconciliation is not None
    assert published.reconciliation.reconciled
    assert published.reconciliation.interval_reconciliation is None
    assert published.reconciliation.contribution_reconciliation is None
    assert published.bindings == ()

    for owner in ("processor", "runtime_health"):
        tampered = copy.deepcopy(document)
        owner_record = cast(dict[str, object], tampered[owner])
        projection = cast(
            dict[str, object],
            owner_record["public_read_fault_availability_projection"],
        )
        projection["opened"] = 1
        projection["completed"] = 1
        summary_path.write_bytes(canonical_json_bytes(tampered) + b"\n")
        with pytest.raises(
            AvailabilityReporterEvidenceError,
            match="public-read fault availability projections do not reconcile",
        ):
            _load_published_availability_authority(
                summary_path,
                evidence.path,
                evidence.contribution_path,
                collector_run_id=run_id,
                replay_authority=authority,
                ordered_replay_release_bindings=(),
                replay_rest_audit_mismatches=0,
                expected_terminal_hash=_TERMINAL_HASH,
                maximum_ledger_bytes=64 * 1024,
            )

    tampered = copy.deepcopy(document)
    availability = cast(dict[str, object], tampered["availability_evidence"])
    availability["public_read_fault_completion_expected_written_chain_sha256"] = "f" * 64
    summary_path.write_bytes(canonical_json_bytes(tampered) + b"\n")
    with pytest.raises(
        AvailabilityReporterEvidenceError,
        match="public-read fault completion chain differs from written projection",
    ):
        _load_published_availability_authority(
            summary_path,
            evidence.path,
            evidence.contribution_path,
            collector_run_id=run_id,
            replay_authority=authority,
            ordered_replay_release_bindings=(),
            replay_rest_audit_mismatches=0,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
        )


@pytest.mark.asyncio
async def test_reporter_loads_released_v2_audit_binding_and_rejects_recovery_tamper(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("authority-aware-reporter-v2-audit")
    contribution = _v2_released_audit_contribution(run_id)
    interval = _v2_interval(contribution)
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        contribution_queue_capacity=2,
        maximum_intervals=2,
        maximum_contributions=2,
        maximum_file_bytes=64 * 1024,
        maximum_contribution_file_bytes=64 * 1024,
    )
    await writer.start()
    assert writer.enqueue_contribution_nowait(contribution)
    assert writer.enqueue_interval_nowait(interval)
    evidence = await writer.close(completed=True, terminal_active_causal_keys=())
    expected_chain = audit_availability_contribution_chain_extend(
        audit_availability_contribution_chain_seed(),
        contribution.to_record(),
    )
    reconciliation = reconcile_published_availability_evidence(
        evidence,
        expected_audit_contributions=AuditAvailabilityContributionChainSnapshot(
            count=1,
            chain_sha256=expected_chain,
        ),
    )

    assert reconciliation.reconciled
    assert reconciliation.discrepancies == ()
    assert evidence.audit_contributions_written == 1
    binding = contribution.audit_availability_binding()
    assert binding is not None
    loaded = _ordered_published_bindings(
        evidence,
        reconciliation,
        maximum_ledger_bytes=64 * 1024,
    )
    assert loaded == (binding,)
    assert loaded[0].divergence_id == contribution.divergence_id
    assert loaded[0].request_id == contribution.request_id
    assert loaded[0].instrument_id == contribution.instrument_id
    assert loaded[0].market_id == contribution.market_id
    assert loaded[0].authoritative_resolution_source == (
        contribution.authoritative_resolution_source
    )
    assert loaded[0].replacement_release_source == contribution.replacement_release_source

    contribution_path = evidence.contribution_path
    assert contribution_path is not None
    decoded: object = json.loads(contribution_path.read_bytes())
    assert isinstance(decoded, dict)
    record = cast(dict[str, object], decoded)
    recovery_value = record["replacement_recovery"]
    assert isinstance(recovery_value, dict)
    recovery = cast(dict[str, object], recovery_value)
    assert recovery["status"] == "RELEASED"
    assert recovery["unresolved"] is False
    recovery["unresolved"] = True
    tampered_payload = canonical_json_bytes(record) + b"\n"
    contribution_path.write_bytes(tampered_payload)
    tampered_evidence = replace(
        evidence,
        contribution_file_bytes=len(tampered_payload),
        contribution_content_sha256=hashlib.sha256(tampered_payload).hexdigest(),
    )

    with pytest.raises(
        AvailabilityReporterEvidenceError,
        match=r"availability contribution\[0\] is not a valid",
    ) as captured:
        _ordered_published_bindings(
            tampered_evidence,
            reconciliation,
            maximum_ledger_bytes=64 * 1024,
        )
    assert isinstance(captured.value.__cause__, ValueError)
    assert "unresolved flag conflicts with status" in str(captured.value.__cause__)


@pytest.mark.asyncio
async def test_reporter_keeps_contribution_and_release_ordering_domains_distinct(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("two-ordering-domains")
    first = _contribution(run_id)
    second = replace(
        first,
        contribution_id="reporter-contribution-second",
        initiating_contributor=False,
        source_evidence_ids=("audit-request:reporter-request-second",),
        affected_instruments=("reporter-instrument-second",),
        request_id="reporter-request-second",
        divergence_id=hashlib.sha256(b"reporter-divergence-second").hexdigest(),
        instrument_id="reporter-instrument-second",
        market_id="reporter-market-second",
        audit_start_source=_source("second-start"),
        audit_response_source=_source("second-response"),
        audit_classification_source=_source("second-classification"),
        authoritative_resolution_source=_source("second-resolution"),
        replacement_release_source=_source("second-release"),
    )
    first_binding = first.audit_availability_binding()
    second_binding = second.audit_availability_binding()
    assert first_binding is not None
    assert second_binding is not None
    writer = SupervisedAvailabilityIntervalWriter(
        root=tmp_path,
        collector_run_id=run_id,
        queue_capacity=2,
        maximum_intervals=2,
        maximum_file_bytes=64 * 1024,
    )
    await writer.start()
    interval = replace(
        _interval(run_id),
        affected_instruments=("reporter-instrument", "reporter-instrument-second"),
    )
    assert writer.enqueue_nowait(interval, contributions=(first, second))
    evidence = await writer.close(completed=True)
    contribution_path = evidence.contribution_path
    assert contribution_path is not None

    release_order = (second_binding, first_binding)
    replay_authority = _released_authority_for_bindings(release_order)
    document = {
        "collector_run_id": run_id.value,
        "availability_evidence": _record(evidence),
        "processor": {
            "terminal_hash": _TERMINAL_HASH,
            "rest_audit_mismatches": 2,
            "audit_availability_authority": _record(replay_authority),
        },
        "runtime_health": {
            "availability_ledger_healthy": True,
            "availability_records_emitted": 1,
            "availability_contributions_emitted": 2,
            "availability_open_at_shutdown_intervals": 0,
            "unavailable_intervals": 1,
            "unavailable_total_nanoseconds": 10,
            "longest_unavailable_nanoseconds": 10,
            "current_unavailable_cause": None,
            "current_unavailable_reason_code": None,
            "current_unavailable_started_monotonic_ns": None,
            "audit_availability_authority": _record(replay_authority),
            "audit_availability_contributions_emitted": {
                "count": evidence.audit_contributions_written,
                "chain_sha256": evidence.audit_contribution_written_chain_sha256,
            },
        },
    }
    summary_path = tmp_path / "two-ordering-summary.json"
    summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    published = _load_published_availability_authority(
        summary_path,
        evidence.path,
        contribution_path,
        collector_run_id=run_id,
        replay_authority=replay_authority,
        ordered_replay_release_bindings=release_order,
        replay_rest_audit_mismatches=2,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=64 * 1024,
    )

    assert published.reconciliation is not None
    assert published.reconciliation.reconciled
    assert published.bindings == (first_binding, second_binding)


def test_reporter_without_availability_evidence_preserves_historical_mode() -> None:
    published = _load_published_availability_authority(
        None,
        None,
        None,
        collector_run_id=CollectorRunId("historical-reporter"),
        replay_authority=empty_audit_availability_authority_snapshot(),
        ordered_replay_release_bindings=(),
        replay_rest_audit_mismatches=0,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=64 * 1024,
    )

    assert published.reconciliation is None
    assert published.bindings == ()


def test_reporter_without_availability_evidence_accepts_exact_legacy_mismatches() -> None:
    authority = replace(
        empty_audit_availability_authority_snapshot(),
        legacy_unverifiable=1,
    )

    published = _load_published_availability_authority(
        None,
        None,
        None,
        collector_run_id=CollectorRunId("legacy-v1-reporter"),
        replay_authority=authority,
        ordered_replay_release_bindings=(),
        replay_rest_audit_mismatches=1,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=64 * 1024,
    )

    assert published.reconciliation is None
    assert published.bindings == ()


def test_reporter_without_availability_evidence_rejects_public_fault_authority() -> None:
    with pytest.raises(
        AvailabilityReporterEvidenceError,
        match="cannot bind public-read fault authority",
    ):
        _load_published_availability_authority(
            None,
            None,
            None,
            collector_run_id=CollectorRunId("legacy-public-fault-reporter"),
            replay_authority=empty_audit_availability_authority_snapshot(),
            ordered_replay_release_bindings=(),
            replay_rest_audit_mismatches=0,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
            expected_public_read_fault_projection=_nonempty_public_fault_projection(),
        )


@pytest.mark.asyncio
async def test_reporter_rejects_omitted_evidence_for_released_v2_authority(
    tmp_path: Path,
) -> None:
    fixture = await _published_fixture(tmp_path)

    with pytest.raises(AvailabilityReporterEvidenceError, match="may be omitted only"):
        _load_published_availability_authority(
            None,
            None,
            None,
            collector_run_id=fixture.run_id,
            replay_authority=fixture.replay_authority,
            ordered_replay_release_bindings=(fixture.binding,),
            replay_rest_audit_mismatches=1,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
        )


def test_reporter_rejects_partial_availability_arguments() -> None:
    with pytest.raises(AvailabilityReporterEvidenceError, match="requires summary"):
        _load_published_availability_authority(
            Path("summary.json"),
            None,
            None,
            collector_run_id=CollectorRunId("partial-reporter"),
            replay_authority=empty_audit_availability_authority_snapshot(),
            ordered_replay_release_bindings=(),
            replay_rest_audit_mismatches=0,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
        )


def _historical_v1_document(fixture: _PublishedFixture) -> dict[str, object]:
    current_evidence = cast(dict[str, object], fixture.document["availability_evidence"])
    historical_evidence_keys = {
        "accepted",
        "collector_run_id",
        "completed",
        "content_sha256",
        "failure_type",
        "file_bytes",
        "gate1_evidence_eligible",
        "longest_interval_nanoseconds",
        "open_at_shutdown_intervals",
        "path",
        "queue_high_water",
        "rejected",
        "schema_version",
        "unavailable_duration_nanoseconds",
        "written",
    }
    evidence = {
        key: value for key, value in current_evidence.items() if key in historical_evidence_keys
    }
    return {
        "collector_run_id": fixture.run_id.value,
        "availability_evidence": evidence,
        "processor": {
            "terminal_hash": "b" * 64,
            "rest_audit_mismatches": 1,
        },
        "runtime_health": {
            "availability_ledger_healthy": True,
            "availability_open_at_shutdown_intervals": 0,
            "availability_records_emitted": 1,
            "connection_id": "historical-connection",
            "current_unavailable_cause": None,
            "current_unavailable_reason_code": None,
            "current_unavailable_started_monotonic_ns": None,
            "data_loss_count": 0,
            "ingress": {},
            "last_frame_received_monotonic_ns": 20,
            "longest_unavailable_nanoseconds": 10,
            "protocol_deviations": 0,
            "reconnects": 0,
            "resyncs": 1,
            "socket_connected": True,
            "socket_generation": 1,
            "unavailable_intervals": 1,
            "unavailable_total_nanoseconds": 10,
        },
    }


@pytest.mark.asyncio
async def test_reporter_validates_historical_v1_interval_only_evidence(tmp_path: Path) -> None:
    fixture = await _published_fixture(tmp_path)
    document = _historical_v1_document(fixture)
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    published = _load_published_availability_authority(
        fixture.summary_path,
        fixture.interval_path,
        None,
        collector_run_id=fixture.run_id,
        replay_authority=fixture.replay_authority,
        ordered_replay_release_bindings=(fixture.binding,),
        replay_rest_audit_mismatches=1,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=64 * 1024,
    )

    assert published.reconciliation is None
    assert published.bindings == ()

    with pytest.raises(
        AvailabilityReporterEvidenceError,
        match="cannot bind public-read fault authority",
    ):
        _load_published_availability_authority(
            fixture.summary_path,
            fixture.interval_path,
            None,
            collector_run_id=fixture.run_id,
            replay_authority=fixture.replay_authority,
            ordered_replay_release_bindings=(fixture.binding,),
            replay_rest_audit_mismatches=1,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
            expected_public_read_fault_projection=_nonempty_public_fault_projection(),
        )


@pytest.mark.asyncio
async def test_reporter_validates_v1_interval_evidence_without_inventing_legacy_bindings(
    tmp_path: Path,
) -> None:
    fixture = await _published_fixture(tmp_path)
    document = _historical_v1_document(fixture)
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")
    legacy_authority = replace(
        empty_audit_availability_authority_snapshot(),
        legacy_unverifiable=1,
    )

    published = _load_published_availability_authority(
        fixture.summary_path,
        fixture.interval_path,
        None,
        collector_run_id=fixture.run_id,
        replay_authority=legacy_authority,
        ordered_replay_release_bindings=(),
        replay_rest_audit_mismatches=1,
        expected_terminal_hash=_TERMINAL_HASH,
        maximum_ledger_bytes=64 * 1024,
    )

    assert published.reconciliation is None
    assert published.bindings == ()


@pytest.mark.parametrize(
    "tamper",
    ["schema", "counter", "hash", "terminal", "authority"],
)
@pytest.mark.asyncio
async def test_reporter_rejects_tampered_historical_v1_evidence(
    tmp_path: Path,
    tamper: str,
) -> None:
    fixture = await _published_fixture(tmp_path)
    document = _historical_v1_document(fixture)
    evidence = cast(dict[str, object], document["availability_evidence"])
    runtime = cast(dict[str, object], document["runtime_health"])
    processor = cast(dict[str, object], document["processor"])
    if tamper == "schema":
        evidence["schema_version"] = "gate1-availability-evidence-summary-v0"
    elif tamper == "counter":
        runtime["unavailable_total_nanoseconds"] = 11
    elif tamper == "hash":
        evidence["content_sha256"] = "c" * 64
    elif tamper == "terminal":
        processor["terminal_hash"] = "not-a-sha256"
    else:
        processor["audit_availability_authority"] = _record(fixture.replay_authority)
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    with pytest.raises(AvailabilityReporterEvidenceError):
        _load_published_availability_authority(
            fixture.summary_path,
            fixture.interval_path,
            None,
            collector_run_id=fixture.run_id,
            replay_authority=fixture.replay_authority,
            ordered_replay_release_bindings=(fixture.binding,),
            replay_rest_audit_mismatches=1,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
        )


@pytest.mark.asyncio
async def test_reporter_recomputes_release_chain_from_ordered_replay_restorations(
    tmp_path: Path,
) -> None:
    fixture = await _published_fixture(tmp_path)
    wrong_authority = replace(fixture.replay_authority, release_chain_sha256="c" * 64)
    document = copy.deepcopy(fixture.document)
    processor = cast(dict[str, object], document["processor"])
    runtime = cast(dict[str, object], document["runtime_health"])
    processor["audit_availability_authority"] = _record(wrong_authority)
    runtime["audit_availability_authority"] = _record(wrong_authority)
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    with pytest.raises(AvailabilityReporterEvidenceError, match="ordered replay restorations"):
        _load_published_availability_authority(
            fixture.summary_path,
            fixture.interval_path,
            fixture.contribution_path,
            collector_run_id=fixture.run_id,
            replay_authority=wrong_authority,
            ordered_replay_release_bindings=(fixture.binding,),
            replay_rest_audit_mismatches=1,
            expected_terminal_hash=_TERMINAL_HASH,
            maximum_ledger_bytes=64 * 1024,
        )


@pytest.mark.asyncio
async def test_reporter_rejects_duplicate_keys_in_current_v2_interval_ledger(
    tmp_path: Path,
) -> None:
    fixture = await _published_fixture(tmp_path)
    payload = fixture.interval_path.read_bytes()
    needle = b'"socket_generation":1'
    assert payload.count(needle) == 1
    tampered = payload.replace(
        needle,
        needle + b',"socket_generation":1',
        1,
    )
    fixture.interval_path.write_bytes(tampered)
    document = copy.deepcopy(fixture.document)
    evidence = cast(dict[str, object], document["availability_evidence"])
    evidence["file_bytes"] = len(tampered)
    evidence["content_sha256"] = hashlib.sha256(tampered).hexdigest()
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    with pytest.raises(AvailabilityReporterEvidenceError, match="repeats key"):
        _load(fixture)


@pytest.mark.asyncio
async def test_reporter_rejects_duplicate_keys_in_current_v2_contribution_ledger(
    tmp_path: Path,
) -> None:
    fixture = await _published_fixture(tmp_path)
    payload = fixture.contribution_path.read_bytes()
    needle = b'"socket_generation":1'
    assert payload.count(needle) == 1
    tampered = payload.replace(
        needle,
        needle + b',"socket_generation":1',
        1,
    )
    fixture.contribution_path.write_bytes(tampered)
    document = copy.deepcopy(fixture.document)
    evidence = cast(dict[str, object], document["availability_evidence"])
    evidence["contribution_file_bytes"] = len(tampered)
    evidence["contribution_content_sha256"] = hashlib.sha256(tampered).hexdigest()
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    with pytest.raises(AvailabilityReporterEvidenceError, match="repeats key"):
        _load(fixture)


@pytest.mark.parametrize(
    "tamper",
    [
        "processor_terminal",
        "processor_authority",
        "runtime_authority",
        "runtime_duration",
        "runtime_current_cause",
        "contribution_ledger",
    ],
)
@pytest.mark.asyncio
async def test_reporter_fails_closed_on_summary_or_ledger_disagreement(
    tmp_path: Path,
    tamper: str,
) -> None:
    fixture = await _published_fixture(tmp_path)
    document = copy.deepcopy(fixture.document)
    processor = cast(dict[str, object], document["processor"])
    runtime = cast(dict[str, object], document["runtime_health"])
    if tamper == "processor_terminal":
        processor["terminal_hash"] = "b" * 64
    elif tamper == "processor_authority":
        authority = cast(dict[str, object], processor["audit_availability_authority"])
        authority["release_chain_sha256"] = "b" * 64
    elif tamper == "runtime_authority":
        authority = cast(dict[str, object], runtime["audit_availability_authority"])
        authority["release_chain_sha256"] = "b" * 64
    elif tamper == "runtime_duration":
        runtime["unavailable_total_nanoseconds"] = 11
    elif tamper == "runtime_current_cause":
        runtime["current_unavailable_cause"] = "AUDIT_MISMATCH_RECOVERY"
    else:
        fixture.contribution_path.write_bytes(fixture.contribution_path.read_bytes() + b"x")
    fixture.summary_path.write_bytes(canonical_json_bytes(document) + b"\n")

    with pytest.raises(AvailabilityReporterEvidenceError):
        _load(fixture)
