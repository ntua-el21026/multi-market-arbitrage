# pyright: reportPrivateUsage=false

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

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
    _receipt,
    _stage_prepare_attest_audit,
)

import multi_market_arbitrage.pipeline.processor as processor_module
from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import CollectorRunId
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.pipeline import (
    AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    AuditFailureReason,
    AuditFaultOrigin,
    AuditFaultStatus,
    AuditOutcome,
    CanonicalProcessor,
    ProcessedEnvelope,
)
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_CLASSIFICATION_V1_SCHEMA,
    AUDIT_CLASSIFICATION_V2_SCHEMA,
    AuditFaultFailureEvidenceKind,
)
from multi_market_arbitrage.pipeline.channels import (
    CONTROL_BOOK_INVALIDATION,
    WS_MARKET,
)
from multi_market_arbitrage.state.order_books import InvalidationReason


def test_final_attestation_commits_frozen_match_without_reparsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-frozen-no-reparse")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-frozen-no-reparse",
    )

    def forbidden_reparse(_payload: bytes) -> None:
        raise AssertionError("final attestation reparsed the REST response")

    monkeypatch.setattr(processor_module, "parse_rest_book", forbidden_reparse)
    _, final = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
    )

    result = processor.process_replayed(final)

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.MATCH,)
    assert processor.summary().pending_rest_audits == 0


def test_attestation_after_expiry_records_frozen_match_without_current_promotion() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-late-final-no-promotion")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-late-final-no-promotion",
    )
    snapshots_before = processor.snapshots()
    _, final = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
        attestation_monotonic_ns=hard_expiry_ns + 1,
    )

    result = processor.process_replayed(final)

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.MATCH,)
    assert result.book_freshness_updates == ()
    assert result.scan_count == 0
    assert result.scan_triggers == ()
    assert processor.snapshots() == snapshots_before
    assert processor.summary().pending_rest_audits == 0


def test_mismatched_prepare_hash_is_permanent_internal_expiry_failure() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-mismatched-prepare-hash")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-mismatched-prepare-hash",
    )
    _, final = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
    )
    metadata = final.transport_metadata
    metadata["classification_prepare_payload_sha256"] = "0" * 64
    mismatched = replace(
        final,
        transport_metadata_allowlist=tuple(sorted(metadata.items())),
    )

    result = processor.process_replayed(mismatched)
    summary = processor.summary()

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert result.scan_count == 0
    assert summary.pending_rest_audits == 0
    lifecycle = summary.audit_fault_lifecycle_records[-1]
    assert lifecycle.origin is AuditFaultOrigin.INTERNAL
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED


def test_mismatched_prepare_ingest_id_is_permanent_internal_expiry_failure() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-mismatched-prepare-ingest-id")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-mismatched-prepare-ingest-id",
    )
    _, final = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
    )
    metadata = final.transport_metadata
    metadata["classification_prepare_ingest_id"] = "wrong-prepare-ingest-id"
    mismatched = replace(
        final,
        transport_metadata_allowlist=tuple(sorted(metadata.items())),
    )

    result = processor.process_replayed(mismatched)

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert result.scan_count == 0
    assert processor.summary().pending_rest_audits == 0


@pytest.mark.parametrize("equality", ["durable", "processed"])
def test_prepare_durability_or_processing_at_expiry_equality_fails_closed(
    equality: str,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"prepare-attest-expiry-equality-{equality}")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id=f"prepare-attest-expiry-equality-{equality}",
    )
    durable_ns = hard_expiry_ns if equality == "durable" else hard_expiry_ns - 1
    _, final = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=durable_ns,
        processed_monotonic_ns=hard_expiry_ns,
    )

    result = processor.process_replayed(final)

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert result.book_freshness_updates == ()
    assert result.scan_count == 0
    lifecycle = processor.summary().audit_fault_lifecycle_records[-1]
    assert lifecycle.origin is AuditFaultOrigin.INTERNAL
    assert lifecycle.status is AuditFaultStatus.PERMANENT


@pytest.mark.parametrize(
    ("case", "expected_observed_schema"),
    [
        ("v1", AUDIT_CLASSIFICATION_V1_SCHEMA),
        ("v2-mismatch", AUDIT_CLASSIFICATION_V2_SCHEMA),
        ("unknown", "mma-rest-audit-classification-v999"),
        ("missing", None),
        ("malformed", None),
    ],
)
def test_rejected_attestation_preserves_observed_schema_and_replay_identity(
    case: str,
    expected_observed_schema: str | None,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"prepare-attest-observed-schema-{case}")
    live = CanonicalProcessor(config)
    replay = CanonicalProcessor(config)

    def process_both(
        envelope: RawEventEnvelope,
    ) -> tuple[ProcessedEnvelope, ProcessedEnvelope]:
        live_result = live.process_durable(envelope, _receipt(envelope))
        replay_result = replay.process_replayed(envelope)
        assert live_result == replay_result
        return live_result, replay_result

    for envelope in _bootstrap(run_id):
        process_both(envelope)
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, start_envelope = _audit_start(
        replay,
        run_id,
        4,
        request_id=f"prepare-attest-observed-schema-{case}",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    process_both(start_envelope)
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    process_both(response)
    _, preparation = _audit_classification_prepare(
        run_id,
        6,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=200,
    )
    process_both(preparation)
    _, baseline = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
        schema=(AUDIT_CLASSIFICATION_V1_SCHEMA if case == "v1" else AUDIT_CLASSIFICATION_V2_SCHEMA),
    )
    if case == "v2-mismatch":
        metadata = baseline.transport_metadata
        metadata["classification_prepare_payload_sha256"] = "0" * 64
        rejected = replace(
            baseline,
            transport_metadata_allowlist=tuple(sorted(metadata.items())),
        )
    elif case in {"unknown", "missing", "malformed"}:
        if case == "malformed":
            payload = b"{"
        else:
            document: dict[str, object] = {
                "classification_monotonic_ns": 250,
                "hard_expiry_monotonic_ns": hard_expiry_ns,
                "request_id": start.request_id,
                "response_ingest_seq": response.ingest_seq,
            }
            if case == "unknown":
                document["schema"] = expected_observed_schema
            payload = canonical_json_bytes(document)
        rejected = _envelope(
            run_id,
            7,
            baseline.channel,
            payload,
            monotonic_ns=baseline.received_monotonic.nanoseconds,
            connection_id=start.connection_id,
            transport_metadata=baseline.transport_metadata,
        )
    else:
        rejected = baseline

    live_result, replay_result = process_both(rejected)
    live_summary = live.summary()
    replay_summary = replay.summary()

    assert live_result == replay_result
    assert tuple(item.outcome for item in live_result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert live_summary == replay_summary
    lifecycle = live_summary.audit_fault_lifecycle_records[-1]
    assert lifecycle.failure_evidence_kind is (
        AuditFaultFailureEvidenceKind.AUDIT_ATTESTATION_VALIDATION_FAILURE
    )
    assert lifecycle.failure_schema == expected_observed_schema
    assert lifecycle.failure_source.ingest_id == rejected.ingest_id
    assert lifecycle.failure_source.payload_sha256 == rejected.payload_sha256
    assert lifecycle.blocks_current_scope


def test_later_book_state_cannot_recompute_prepared_outcome() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-later-book-state")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-later-book-state",
    )
    update = _envelope(
        run_id,
        7,
        WS_MARKET,
        _fixture("polymarket_ws_price_change.json"),
        monotonic_ns=220,
        connection_id=start.connection_id,
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    assert processor.process_replayed(update).success
    snapshots_after_update = processor.snapshots()
    freshness_after_update = dict(processor._last_accepted_l2_monotonic)
    _, final = _audit_classification_attestation(
        run_id,
        8,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
        attestation_monotonic_ns=251,
    )

    result = processor.process_replayed(final)

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert result.book_freshness_updates == ()
    assert result.scan_count == 0
    assert processor.snapshots() == snapshots_after_update
    assert processor._last_accepted_l2_monotonic == freshness_after_update
    summary = processor.summary()
    assert summary.pending_rest_audits == 0
    lifecycle = summary.audit_fault_lifecycle_records[-1]
    assert lifecycle.origin is AuditFaultOrigin.INTERNAL
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED


def test_intervening_book_state_rejects_prepared_mismatch_without_divergence() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-later-book-state-mismatch")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, start_envelope = _audit_start(
        processor,
        run_id,
        4,
        request_id="prepare-attest-later-book-state-mismatch",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    assert processor.process_replayed(start_envelope).success
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    response = _audit_response(
        run_id,
        5,
        start,
        mismatched_payload,
        boundary=0,
        classification_deferred=True,
    )
    assert processor.process_replayed(response).success
    _, preparation = _audit_classification_prepare(
        run_id,
        6,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=200,
    )
    assert processor.process_replayed(preparation).success
    prepared = processor._pending_rest_audit_classification_preparations[start.request_id]
    assert prepared.plan.outcome is AuditOutcome.MISMATCH
    update = _envelope(
        run_id,
        7,
        WS_MARKET,
        _fixture("polymarket_ws_price_change.json"),
        monotonic_ns=220,
        connection_id=start.connection_id,
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    assert processor.process_replayed(update).success
    snapshots_after_update = processor.snapshots()
    freshness_after_update = dict(processor._last_accepted_l2_monotonic)
    _, final = _audit_classification_attestation(
        run_id,
        8,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
        attestation_monotonic_ns=251,
    )

    result = processor.process_replayed(final)

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert result.audit_divergences_opened == ()
    assert result.book_freshness_updates == ()
    assert result.scan_count == 0
    assert not result.needs_resync
    assert processor.snapshots() == snapshots_after_update
    assert processor._last_accepted_l2_monotonic == freshness_after_update
    summary = processor.summary()
    assert summary.audit_divergences_pending == 0
    assert summary.pending_rest_audits == 0


def test_unreconciled_attestation_has_live_replay_parity() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("prepare-attest-unreconciled-live-replay")
    live = CanonicalProcessor(config)
    replay = CanonicalProcessor(config)

    def process_both(
        envelope: RawEventEnvelope,
    ) -> tuple[ProcessedEnvelope, ProcessedEnvelope]:
        live_result = live.process_durable(envelope, _receipt(envelope))
        replay_result = replay.process_replayed(envelope)
        assert live_result == replay_result
        return live_result, replay_result

    for envelope in _bootstrap(run_id):
        process_both(envelope)
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, start_envelope = _audit_start(
        replay,
        run_id,
        4,
        request_id="prepare-attest-unreconciled-live-replay",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    process_both(start_envelope)
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    process_both(response)
    _, preparation = _audit_classification_prepare(
        run_id,
        6,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=200,
    )
    process_both(preparation)
    update = _envelope(
        run_id,
        7,
        WS_MARKET,
        _fixture("polymarket_ws_price_change.json"),
        monotonic_ns=220,
        connection_id=start.connection_id,
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    process_both(update)
    _, final = _audit_classification_attestation(
        run_id,
        8,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
        attestation_monotonic_ns=251,
    )

    live_result, replay_result = process_both(final)

    assert live_result == replay_result
    assert live.summary() == replay.summary()
    assert live.snapshots() == replay.snapshots()


@pytest.mark.parametrize("with_prepare", [False, True], ids=["body-only", "missing-final"])
def test_invalidation_closes_unattested_body_as_permanent_internal_expiry(
    with_prepare: bool,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"prepare-attest-unattested-{with_prepare}")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, start_envelope = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"prepare-attest-unattested-{with_prepare}",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    assert processor.process_replayed(start_envelope).success
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert processor.process_replayed(response).success
    next_sequence = 6
    if with_prepare:
        _, prepare = _audit_classification_prepare(
            run_id,
            next_sequence,
            start,
            response,
            hard_expiry_monotonic_ns=hard_expiry_ns,
            monotonic_ns=200,
        )
        assert processor.process_replayed(prepare).success
        next_sequence += 1
    invalidation = _envelope(
        run_id,
        next_sequence,
        CONTROL_BOOK_INVALIDATION,
        canonical_json_bytes({"reason": InvalidationReason.DISCONNECT.value}),
    )

    result = processor.process_replayed(invalidation)
    summary = processor.summary()

    assert result.success
    assert tuple(item.outcome for item in result.audit_outcomes) == (AuditOutcome.FAILED,)
    assert result.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert summary.pending_rest_audits == 0
    lifecycle = summary.audit_fault_lifecycle_records[-1]
    assert lifecycle.origin is AuditFaultOrigin.INTERNAL
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
