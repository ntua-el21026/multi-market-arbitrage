from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from multi_market_arbitrage.core.exact import Price, Quantity
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    SubscriptionId,
    VenueId,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.pipeline.audit import (
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditMismatchCause,
    AuditStartEvidence,
    WebSocketAdmissionWatermark,
    compare_audit_exact_depth,
)
from multi_market_arbitrage.pipeline.channels import (
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_START,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
)
from multi_market_arbitrage.research.audit_mismatch import (
    AUDIT_MISMATCH_LINEAGE_REPORT_SCHEMA,
    AuditEqualityProof,
    AuditMatchingStateRepresentation,
    AuditMismatchCausalRelation,
    AuditMismatchLineageReason,
    AuditMismatchLineageReport,
    AuditMismatchObservation,
    AuditMismatchReportError,
    AuditMismatchReportLimits,
    AuditRecoveryEvidenceStatus,
    AuditVenueStateRelation,
    build_audit_mismatch_lineage_report,
)
from multi_market_arbitrage.state.order_books.models import BookLevel

_RUN = CollectorRunId("audit-mismatch-v2-run")
_PRIMARY_CONNECTION = ConnectionId("audit-mismatch-v2-primary")
_RECOVERY_CONNECTION = ConnectionId("audit-mismatch-v2-recovery")
_SUBSCRIPTION = SubscriptionId("audit-mismatch-v2-subscription")
_INSTRUMENT = InstrumentId("audit-mismatch-v2-instrument")
_ASSET = "audit-mismatch-v2-asset"
_REQUEST = "audit-mismatch-v2-request"
_SEMANTICS = hashlib.sha256(b"audit-mismatch-v2-semantics").hexdigest()


def _level(price: str, quantity: str) -> BookLevel:
    return BookLevel(price=Price.parse(price), quantity=Quantity.parse(quantity))


def _envelope(
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    connection_id: ConnectionId = _PRIMARY_CONNECTION,
    metadata: dict[str, str | int | bool] | None = None,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=_RUN,
        ingest_seq=sequence,
        ingest_id=IngestId(f"audit-mismatch-v2-ingest-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=connection_id,
        subscription_id=_SUBSCRIPTION,
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "CONTROL",
        subscription_semantics_hash=_SEMANTICS,
        received_wall_time=UtcTimestamp(1_700_000_000_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(_RUN.value, 1_000 + sequence * 100),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata,
    )


def _ws_book(
    sequence: int,
    *,
    bid_quantity: str,
    venue_hash: str,
    timestamp: str,
    admission_ordinal: int | None,
    connection_id: ConnectionId = _PRIMARY_CONNECTION,
    ask_quantity: str = "10",
) -> RawEventEnvelope:
    metadata: dict[str, str | int | bool] | None = (
        None if admission_ordinal is None else {"websocket_admission_ordinal": admission_ordinal}
    )
    return _envelope(
        sequence,
        WS_MARKET,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": ask_quantity}],
                "asset_id": _ASSET,
                "bids": [{"price": "0.4", "size": bid_quantity}],
                "event_type": "book",
                "hash": venue_hash,
                "market": "condition-v2",
                "timestamp": timestamp,
            }
        ),
        connection_id=connection_id,
        metadata=metadata,
    )


def _ws_change(
    sequence: int,
    *,
    bid_quantity: str,
    venue_hash: str,
    timestamp: str,
    admission_ordinal: int | None,
    connection_id: ConnectionId = _PRIMARY_CONNECTION,
) -> RawEventEnvelope:
    metadata: dict[str, str | int | bool] | None = (
        None if admission_ordinal is None else {"websocket_admission_ordinal": admission_ordinal}
    )
    return _envelope(
        sequence,
        WS_MARKET,
        canonical_json_bytes(
            {
                "event_type": "price_change",
                "market": "condition-v2",
                "price_changes": [
                    {
                        "asset_id": _ASSET,
                        "best_ask": "0.6",
                        "best_bid": "0.4",
                        "hash": venue_hash,
                        "price": "0.4",
                        "side": "BUY",
                        "size": bid_quantity,
                    }
                ],
                "timestamp": timestamp,
            }
        ),
        connection_id=connection_id,
        metadata=metadata,
    )


def _ws_multi_change(
    sequence: int,
    *,
    bid_quantities: tuple[str, ...],
    admission_ordinal: int,
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        WS_MARKET,
        canonical_json_bytes(
            {
                "event_type": "price_change",
                "market": "condition-v2",
                "price_changes": [
                    {
                        "asset_id": _ASSET,
                        "best_ask": "0.6",
                        "best_bid": "0.4",
                        "hash": f"multi-change-{subordinal}",
                        "price": "0.4",
                        "side": "BUY",
                        "size": quantity,
                    }
                    for subordinal, quantity in enumerate(bid_quantities)
                ],
                "timestamp": "1001",
            }
        ),
        metadata={"websocket_admission_ordinal": admission_ordinal},
    )


def _start(sequence: int, *, starting_admission_ordinal: int = 10) -> RawEventEnvelope:
    start = AuditStartEvidence(
        request_id=_REQUEST,
        connection_id=_PRIMARY_CONNECTION,
        native_asset_id=_ASSET,
        attempt=1,
        request_started_monotonic_ns=1_000 + sequence * 100,
        starting_admission_watermark=WebSocketAdmissionWatermark(
            _PRIMARY_CONNECTION,
            starting_admission_ordinal,
        ),
        starting_processed_watermark=WebSocketAdmissionWatermark(
            _PRIMARY_CONNECTION,
            starting_admission_ordinal,
        ),
        starting_book=AuditBookIdentity(
            instrument_id=_INSTRUMENT,
            generation=7,
            economic_content_sha256="a" * 64,
        ),
    )
    return _envelope(
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        metadata={"request_id": _REQUEST},
    )


def _rest_response(
    sequence: int,
    *,
    response_boundary_ordinal: int | None = 20,
) -> RawEventEnvelope:
    metadata: dict[str, str | int | bool] = {
        "audit_attempt": 1,
        "request_id": _REQUEST,
    }
    if response_boundary_ordinal is not None:
        metadata.update(
            {
                "barrier_websocket_ordinal": response_boundary_ordinal,
                "response_boundary_websocket_ordinal": response_boundary_ordinal,
            }
        )
    return _envelope(
        sequence,
        REST_BOOK_AUDIT,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": "10"}],
                "asset_id": _ASSET,
                "bids": [{"price": "0.4", "size": "12"}],
                "hash": "rest-hash",
                "last_trade_price": None,
                "market": "condition-v2",
                "min_order_size": "1",
                "neg_risk": False,
                "tick_size": "0.01",
                "timestamp": "1000",
            }
        ),
        metadata=metadata,
    )


def _classification(
    sequence: int,
    *,
    response_sequence: int,
    classification_monotonic_ns: int | None = None,
    hard_expiry_monotonic_ns: int = 100_000,
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        CONTROL_REST_AUDIT_CLASSIFY,
        AuditClassificationCommand(
            request_id=_REQUEST,
            response_ingest_seq=response_sequence,
            classification_monotonic_ns=(
                1_000 + sequence * 100
                if classification_monotonic_ns is None
                else classification_monotonic_ns
            ),
            hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
        ).to_payload(),
    )


def _rest_resnapshot(sequence: int) -> RawEventEnvelope:
    return _envelope(
        sequence,
        REST_BOOK,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": "10"}],
                "asset_id": _ASSET,
                "bids": [{"price": "0.4", "size": "12"}],
                "hash": "recovery-rest-hash",
                "last_trade_price": None,
                "market": "condition-v2",
                "min_order_size": "1",
                "neg_risk": False,
                "tick_size": "0.01",
                "timestamp": "1002",
            }
        ),
    )


def _observation(*, classification_sequence: int = 3) -> AuditMismatchObservation:
    local_bids = (_level("0.4", "10"),)
    rest_bids = (_level("0.4", "12"),)
    asks = (_level("0.6", "10"),)
    return AuditMismatchObservation.from_exact(
        classification_ingest_seq=classification_sequence,
        instrument_id=_INSTRUMENT,
        native_asset_id=_ASSET,
        exact_depth_difference=compare_audit_exact_depth(
            local_bids=local_bids,
            local_asks=asks,
            rest_bids=rest_bids,
            rest_asks=asks,
        ),
        rest_venue_hash="rest-hash",
        rest_timestamp_raw="1000",
        top_changed=False,
    )


def _prefix(*, response_boundary_ordinal: int | None = 20) -> tuple[RawEventEnvelope, ...]:
    return (
        _ws_book(
            0,
            bid_quantity="10",
            venue_hash="baseline-hash",
            timestamp="900",
            admission_ordinal=10,
        ),
        _start(1),
        _rest_response(2, response_boundary_ordinal=response_boundary_ordinal),
        _classification(3, response_sequence=2),
    )


def _build(
    envelopes: tuple[RawEventEnvelope, ...],
    *,
    limits: AuditMismatchReportLimits | None = None,
    classification_sequence: int = 3,
) -> AuditMismatchLineageReport:
    return build_audit_mismatch_lineage_report(
        lambda: iter(envelopes),
        _RUN,
        (_observation(classification_sequence=classification_sequence),),
        limits=limits,
    )


def test_same_generation_full_depth_match_is_authoritative_over_venue_diagnostics() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="12",
            venue_hash="different-opaque-hash",
            timestamp="3000",
            admission_ordinal=21,
        ),
    )

    first = _build(envelopes)
    second = _build(envelopes)

    assert first == second
    assert first.report_sha256 == second.report_sha256
    assert first.schema_version == AUDIT_MISMATCH_LINEAGE_REPORT_SCHEMA
    result = first.results[0]
    assert result.causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
    )
    assert result.cause is AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_EXACT_CONVERGENCE
    assert result.venue_state_relation is AuditVenueStateRelation.FUTURE
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert result.exact_reconciliation_proven
    assert result.matching_state_representation is AuditMatchingStateRepresentation.FULL_SNAPSHOT
    assert result.matching_connection_id == _PRIMARY_CONNECTION
    assert result.start_socket_generation == 1
    assert result.matching_socket_generation == 1
    assert result.matching_admission_ordinal == 21
    assert result.matching_websocket_subordinal == 0
    assert result.matching_event_type == "book"
    assert result.matching_exact_depth_identity == result.normalized_rest_exact_depth_identity
    assert result.matching_exact_frame_count == 1
    assert result.matching_source_chain.source_count == 1
    assert result.venue_hash_diagnostic_equal is False
    assert result.venue_timestamp_diagnostic_equal is False
    assert result.response_to_match_monotonic_ns == 200
    assert result.classification_to_match_monotonic_ns == 100


def test_same_generation_incremental_reconstruction_can_prove_exact_match() -> None:
    envelopes = (
        *_prefix(),
        _ws_change(
            4,
            bid_quantity="12",
            venue_hash="incremental-hash",
            timestamp="1001",
            admission_ordinal=21,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
    )
    assert result.matching_state_representation is (
        AuditMatchingStateRepresentation.RECONSTRUCTED_INCREMENTALS
    )
    assert result.matching_websocket_ingest_seq == 4
    assert result.matching_websocket_subordinal == 0
    assert result.matching_event_type == "price_change"
    assert result.matching_exact_frame_count == 2
    assert result.matching_source_chain.source_count == 2
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert result.exact_reconciliation_proven


def test_same_generation_conflict_before_incremental_convergence_is_not_natural() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="11",
            venue_hash="conflicting-snapshot",
            timestamp="1001",
            admission_ordinal=21,
        ),
        _ws_change(
            5,
            bid_quantity="12",
            venue_hash="converged-incremental",
            timestamp="1002",
            admission_ordinal=22,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE
    )
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_CONFLICT
    assert result.matching_websocket_ingest_seq == 5
    assert result.matching_state_representation is (
        AuditMatchingStateRepresentation.RECONSTRUCTED_INCREMENTALS
    )
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert not result.exact_reconciliation_proven


def test_same_generation_incremental_conflict_before_convergence_is_not_natural() -> None:
    envelopes = (
        *_prefix(),
        _ws_change(
            4,
            bid_quantity="11",
            venue_hash="conflicting-incremental",
            timestamp="1001",
            admission_ordinal=21,
        ),
        _ws_change(
            5,
            bid_quantity="12",
            venue_hash="converged-incremental",
            timestamp="1002",
            admission_ordinal=22,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE
    )
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_CONFLICT
    assert result.matching_websocket_ingest_seq == 5
    assert result.matching_state_representation is (
        AuditMatchingStateRepresentation.RECONSTRUCTED_INCREMENTALS
    )
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert not result.exact_reconciliation_proven


def test_later_connection_exact_match_is_classified_as_recovery_generation() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="12",
            venue_hash="replacement-hash",
            timestamp="1003",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.RECOVERY_GENERATION_MATCH
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.RECOVERY_GENERATION_EXACT_MATCH
    assert result.start_connection_id == _PRIMARY_CONNECTION
    assert result.start_socket_generation == 1
    assert result.matching_connection_id == _RECOVERY_CONNECTION
    assert result.matching_subscription_id == _SUBSCRIPTION
    assert result.matching_socket_generation == 2
    assert result.matching_state_representation is AuditMatchingStateRepresentation.FULL_SNAPSHOT
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    chronology = result.recovery_chronology
    assert chronology.generation_termination.status is (
        AuditRecoveryEvidenceStatus.RAW_JOURNAL_INFERENCE
    )
    assert chronology.replacement_websocket_generation.status is (
        AuditRecoveryEvidenceStatus.RAW_JOURNAL
    )
    assert chronology.replacement_websocket_generation.connection_id == _RECOVERY_CONNECTION
    assert chronology.replacement_websocket_generation.subscription_id == _SUBSCRIPTION
    assert chronology.replacement_websocket_generation.socket_generation == 2


def test_raw_rest_candidate_cannot_claim_recovery_or_natural_convergence() -> None:
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="same-socket-after-rest",
            timestamp="1003",
            admission_ordinal=21,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.AMBIGUOUS
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.CAUSAL_GENERATION_AMBIGUOUS
    assert result.matching_connection_id is None
    assert result.equality_proof is AuditEqualityProof.NOT_PROVEN
    chronology = result.recovery_chronology
    assert chronology.rest_resnapshot.status is AuditRecoveryEvidenceStatus.RAW_JOURNAL
    assert chronology.rest_resnapshot.ingest_seq == 4
    assert (
        chronology.rest_resnapshot.exact_depth_identity
        == result.normalized_rest_exact_depth_identity
    )
    assert "candidate" in chronology.rest_resnapshot.reason
    assert chronology.recovery_completion.status is AuditRecoveryEvidenceStatus.NOT_RETAINED
    assert "does not prove successful canonical application" in (
        chronology.recovery_completion.reason
    )
    assert chronology.research_eligibility_restoration.status is (
        AuditRecoveryEvidenceStatus.NOT_RETAINED
    )


def test_top_only_same_generation_equality_does_not_prove_full_depth_match() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="11",
            venue_hash="persistent-conflict",
            timestamp="1001",
            admission_ordinal=21,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.PERSISTENT_DIVERGENCE
    assert result.cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
    assert result.reason is AuditMismatchLineageReason.PERSISTENT_EXACT_DIVERGENCE
    assert result.venue_state_relation is AuditVenueStateRelation.NONE
    assert result.matching_websocket_source is None
    assert result.equality_proof is AuditEqualityProof.NOT_PROVEN
    assert result.observed_difference.top_changed is False


def test_missing_response_boundary_evidence_fails_closed_as_ambiguous() -> None:
    envelopes = (
        *_prefix(response_boundary_ordinal=None),
        _ws_book(
            4,
            bid_quantity="12",
            venue_hash="otherwise-matching",
            timestamp="1000",
            admission_ordinal=21,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.AMBIGUOUS
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.CAUSAL_GENERATION_AMBIGUOUS
    assert result.venue_state_relation is AuditVenueStateRelation.AMBIGUOUS
    assert result.matching_websocket_source is None
    assert result.equality_proof is AuditEqualityProof.NOT_PROVEN
    assert "RESPONSE_BOUNDARY_ADMISSION_ORDINAL_NOT_RETAINED" in result.unresolved_evidence
    assert "BARRIER_PROCESSED_ORDINAL_NOT_RETAINED" in result.unresolved_evidence
    assert "CAUSAL_ORDER_OR_GENERATION_AMBIGUOUS" in result.unresolved_evidence


def test_missing_matching_admission_ordinal_is_explicit_and_ambiguous() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="12",
            venue_hash="otherwise-matching",
            timestamp="1000",
            admission_ordinal=None,
        ),
    )

    result = _build(envelopes).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.AMBIGUOUS
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.CAUSAL_GENERATION_AMBIGUOUS
    assert result.equality_proof is AuditEqualityProof.NOT_PROVEN
    assert "MATCHING_ADMISSION_ORDINAL_NOT_RETAINED" in result.unresolved_evidence
    assert "RESPONSE_BOUNDARY_ADMISSION_ORDINAL_NOT_RETAINED" not in (result.unresolved_evidence)


def test_pass_identity_rejects_metadata_only_envelope_mutation() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="12",
            venue_hash="matching-hash",
            timestamp="1000",
            admission_ordinal=21,
        ),
    )
    mutated_first = replace(
        envelopes[0],
        transport_metadata_allowlist=(("websocket_admission_ordinal", 9),),
    )
    calls = 0

    def mutable_factory() -> tuple[RawEventEnvelope, ...]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return envelopes
        return (mutated_first, *envelopes[1:])

    with pytest.raises(
        AuditMismatchReportError,
        match="source journal identity changed between analysis passes",
    ):
        build_audit_mismatch_lineage_report(
            mutable_factory,
            _RUN,
            (_observation(),),
        )

    assert mutated_first.payload_bytes == envelopes[0].payload_bytes
    assert mutated_first.payload_sha256 == envelopes[0].payload_sha256
    assert mutated_first.transport_metadata != envelopes[0].transport_metadata


def test_causal_frame_bound_cannot_be_reset_by_full_snapshots() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="11",
            venue_hash="bounded-conflict",
            timestamp="1001",
            admission_ordinal=21,
        ),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="bounded-match",
            timestamp="1002",
            admission_ordinal=22,
        ),
    )
    one_frame = replace(
        AuditMismatchReportLimits(),
        maximum_exact_frames_per_observation=1,
    )

    result = _build(envelopes, limits=one_frame).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.AMBIGUOUS
    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.EXACT_LINEAGE_BOUND_EXCEEDED
    assert result.equality_proof is AuditEqualityProof.NOT_PROVEN
    assert result.matching_websocket_source is None
    assert "EXACT_FRAME_BOUND_EXCEEDED" in result.unresolved_evidence
    assert "CAUSAL_ORDER_OR_GENERATION_AMBIGUOUS" not in result.unresolved_evidence

    two_frames = replace(
        AuditMismatchReportLimits(),
        maximum_exact_frames_per_observation=2,
    )
    assert _build(envelopes, limits=two_frames).results[0].causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE
    )


def test_causal_frame_bound_counts_a_multi_action_source_once() -> None:
    envelopes = (
        *_prefix(),
        _ws_multi_change(
            4,
            bid_quantities=("11", "12"),
            admission_ordinal=21,
        ),
    )
    one_frame = replace(
        AuditMismatchReportLimits(),
        maximum_exact_frames_per_observation=1,
    )

    result = _build(envelopes, limits=one_frame).results[0]

    assert result.causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_CONFLICT_BEFORE_CONVERGENCE
    )
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_CONFLICT
    assert result.matching_websocket_ingest_seq == 4
    assert result.matching_websocket_subordinal == 1
    assert result.matching_exact_frame_count == 2
    assert "EXACT_FRAME_BOUND_EXCEEDED" not in result.unresolved_evidence


def test_causal_frame_bound_starts_at_response_admission_boundary() -> None:
    prefix = _prefix()
    envelopes = (
        *prefix[:3],
        _ws_book(
            3,
            bid_quantity="11",
            venue_hash="pre-classification-conflict",
            timestamp="1001",
            admission_ordinal=21,
        ),
        _classification(4, response_sequence=2),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="post-classification-match",
            timestamp="1002",
            admission_ordinal=22,
        ),
    )
    limits = replace(
        AuditMismatchReportLimits(),
        maximum_exact_frames_per_observation=1,
    )

    result = _build(
        envelopes,
        limits=limits,
        classification_sequence=4,
    ).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.AMBIGUOUS
    assert result.reason is AuditMismatchLineageReason.EXACT_LINEAGE_BOUND_EXCEEDED
    assert "EXACT_FRAME_BOUND_EXCEEDED" in result.unresolved_evidence


def test_causal_frame_bound_cannot_be_reset_by_reconnect() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="11",
            venue_hash="pre-reconnect-conflict",
            timestamp="1001",
            admission_ordinal=21,
        ),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="replacement-match",
            timestamp="1002",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
    )
    limits = replace(
        AuditMismatchReportLimits(),
        maximum_exact_frames_per_observation=1,
    )

    result = _build(envelopes, limits=limits).results[0]

    assert result.causal_relation is AuditMismatchCausalRelation.AMBIGUOUS
    assert result.reason is AuditMismatchLineageReason.EXACT_LINEAGE_BOUND_EXCEEDED
    assert result.equality_proof is AuditEqualityProof.NOT_PROVEN
    assert result.recovery_chronology.replacement_websocket_generation.connection_id == (
        _RECOVERY_CONNECTION
    )


def test_causal_search_stops_after_first_decisive_exact_match() -> None:
    envelopes = (
        *_prefix(),
        _ws_book(
            4,
            bid_quantity="12",
            venue_hash="first-decisive-match",
            timestamp="1001",
            admission_ordinal=21,
        ),
        _ws_book(
            5,
            bid_quantity="11",
            venue_hash="irrelevant-later-conflict",
            timestamp="1002",
            admission_ordinal=22,
        ),
    )
    limits = replace(
        AuditMismatchReportLimits(),
        maximum_exact_frames_per_observation=1,
    )

    result = _build(envelopes, limits=limits).results[0]

    assert result.causal_relation is (
        AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
    )
    assert result.matching_websocket_ingest_seq == 4
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_EXACT_CONVERGENCE


def test_classification_control_time_must_equal_raw_receive_time() -> None:
    envelopes = (
        *_prefix()[:3],
        _classification(
            3,
            response_sequence=2,
            classification_monotonic_ns=1_299,
        ),
    )

    with pytest.raises(
        AuditMismatchReportError,
        match="classification timestamp differs from raw receive evidence",
    ):
        _build(envelopes)


def test_mismatch_classification_must_precede_hard_expiry() -> None:
    envelopes = (
        *_prefix()[:3],
        _classification(
            3,
            response_sequence=2,
            hard_expiry_monotonic_ns=1_300,
        ),
    )

    with pytest.raises(
        AuditMismatchReportError,
        match="MISMATCH classification reached or exceeded its hard expiry",
    ):
        _build(envelopes)
