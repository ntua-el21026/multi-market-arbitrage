from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    SubscriptionId,
    VenueId,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
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
    REST_BOOK_AUDIT,
    WS_MARKET,
)
from multi_market_arbitrage.pipeline.transitions import (
    ReportedTopTransition,
    ReportedTopTransitionEntrySource,
    ReportedTopTransitionReason,
    ReportedTopTransitionStatus,
)
from multi_market_arbitrage.research.audit_mismatch import (
    AuditEqualityProof,
    AuditMatchingStateRepresentation,
    AuditMismatchCausalRelation,
    AuditMismatchLineageReason,
    AuditMismatchObservation,
    AuditMismatchReportError,
    AuditMismatchReportLimits,
    AuditMismatchReportResourceError,
    AuditVenueStateRelation,
    build_audit_mismatch_lineage_report,
    load_instrumented_replay_mismatch_observations,
)
from multi_market_arbitrage.state.order_books.models import BookLevel

_RUN = CollectorRunId("audit-mismatch-report-run")
_CONNECTION = ConnectionId("audit-mismatch-report-connection")
_SUBSCRIPTION = SubscriptionId("audit-mismatch-report-subscription")
_INSTRUMENT = InstrumentId("audit-mismatch-report-instrument")
_ASSET = "native-asset"
_REQUEST = "request-1"
_SEMANTICS = hashlib.sha256(b"audit-mismatch-report").hexdigest()


def _level(price: str, quantity: str) -> BookLevel:
    return BookLevel(price=Price.parse(price), quantity=Quantity.parse(quantity))


def _envelope(
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    metadata: dict[str, str | int | bool] | None = None,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=_RUN,
        ingest_seq=sequence,
        ingest_id=IngestId(f"audit-mismatch-ingest-{sequence}"),
        venue_id=VenueId("polymarket"),
        connection_id=_CONNECTION,
        subscription_id=_SUBSCRIPTION,
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "CONTROL",
        subscription_semantics_hash=_SEMANTICS,
        received_wall_time=UtcTimestamp(1_700_000_000_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(_RUN.value, sequence + 100),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata,
    )


def _ws_book(
    sequence: int,
    *,
    venue_hash: str,
    bid_quantity: str,
    ask_quantity: str = "10",
    timestamp: str = "1000",
) -> RawEventEnvelope:
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
                "market": "condition-1",
                "timestamp": timestamp,
            }
        ),
        metadata={"websocket_admission_ordinal": sequence + 1},
    )


def _ws_change(
    sequence: int,
    *,
    venue_hash: str,
    bid_quantity: str,
    timestamp: str = "1001",
    price: str = "0.4",
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        WS_MARKET,
        canonical_json_bytes(
            {
                "event_type": "price_change",
                "market": "condition-1",
                "price_changes": [
                    {
                        "asset_id": _ASSET,
                        "best_ask": "0.6",
                        "best_bid": "0.4",
                        "hash": venue_hash,
                        "price": price,
                        "side": "BUY",
                        "size": bid_quantity,
                    }
                ],
                "timestamp": timestamp,
            }
        ),
        metadata={"websocket_admission_ordinal": sequence + 1},
    )


def _ws_best_bid_ask(
    sequence: int,
    *,
    best_bid: str,
    best_ask: str,
    timestamp: str = "1000",
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        WS_MARKET,
        canonical_json_bytes(
            {
                "asset_id": _ASSET,
                "best_ask": best_ask,
                "best_bid": best_bid,
                "event_type": "best_bid_ask",
                "market": "condition-1",
                "spread": str(ExactDecimal.parse(best_ask) - ExactDecimal.parse(best_bid)),
                "timestamp": timestamp,
            }
        ),
        metadata={"websocket_admission_ordinal": sequence + 1},
    )


def _ws_tick_change(
    sequence: int,
    *,
    old_tick_size: str,
    new_tick_size: str,
    timestamp: str = "1001",
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        WS_MARKET,
        canonical_json_bytes(
            {
                "asset_id": _ASSET,
                "event_type": "tick_size_change",
                "market": "condition-1",
                "new_tick_size": new_tick_size,
                "old_tick_size": old_tick_size,
                "timestamp": timestamp,
            }
        ),
        metadata={"websocket_admission_ordinal": sequence + 1},
    )


def _start(
    sequence: int,
    *,
    response_sequence: int,
    request_id: str = _REQUEST,
) -> RawEventEnvelope:
    del response_sequence
    start = AuditStartEvidence(
        request_id=request_id,
        connection_id=_CONNECTION,
        native_asset_id=_ASSET,
        attempt=1,
        request_started_monotonic_ns=sequence + 100,
        starting_admission_watermark=WebSocketAdmissionWatermark(_CONNECTION, sequence),
        starting_processed_watermark=WebSocketAdmissionWatermark(_CONNECTION, sequence),
        starting_book=AuditBookIdentity(
            instrument_id=_INSTRUMENT,
            generation=1,
            economic_content_sha256="a" * 64,
        ),
    )
    return _envelope(
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        metadata={"request_id": request_id},
    )


def _rest_response(
    sequence: int,
    *,
    venue_hash: str,
    timestamp: str,
    bid_quantity: str,
    request_id: str = _REQUEST,
    tick_size: str = "0.01",
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        REST_BOOK_AUDIT,
        canonical_json_bytes(
            {
                "asks": [{"price": "0.6", "size": "10"}],
                "asset_id": _ASSET,
                "bids": [{"price": "0.4", "size": bid_quantity}],
                "hash": venue_hash,
                "last_trade_price": None,
                "market": "condition-1",
                "min_order_size": "1",
                "neg_risk": False,
                "tick_size": tick_size,
                "timestamp": timestamp,
            }
        ),
        metadata={
            "audit_attempt": 1,
            "barrier_websocket_ordinal": sequence - 1,
            "request_id": request_id,
            "response_boundary_websocket_ordinal": sequence - 1,
        },
    )


def _classification(
    sequence: int,
    *,
    response_sequence: int,
    request_id: str = _REQUEST,
) -> RawEventEnvelope:
    return _envelope(
        sequence,
        CONTROL_REST_AUDIT_CLASSIFY,
        AuditClassificationCommand(
            request_id=request_id,
            response_ingest_seq=response_sequence,
            classification_monotonic_ns=sequence + 100,
            hard_expiry_monotonic_ns=10_000,
        ).to_payload(),
    )


def _observation(
    *,
    classification_sequence: int,
    local_bids: tuple[BookLevel, ...],
    rest_bids: tuple[BookLevel, ...],
    local_asks: tuple[BookLevel, ...] | None = None,
    rest_asks: tuple[BookLevel, ...] | None = None,
    top_changed: bool | None = False,
    local_tick_size: ExactDecimal | None = None,
    reported_top_transition: ReportedTopTransition | None = None,
) -> AuditMismatchObservation:
    actual_local_asks = (_level("0.6", "10"),) if local_asks is None else local_asks
    actual_rest_asks = (_level("0.6", "10"),) if rest_asks is None else rest_asks
    difference = compare_audit_exact_depth(
        local_bids=local_bids,
        local_asks=actual_local_asks,
        rest_bids=rest_bids,
        rest_asks=actual_rest_asks,
    )
    return AuditMismatchObservation.from_exact(
        classification_ingest_seq=classification_sequence,
        instrument_id=_INSTRUMENT,
        native_asset_id=_ASSET,
        exact_depth_difference=difference,
        rest_venue_hash="h0",
        rest_timestamp_raw="1000",
        top_changed=top_changed,
        local_tick_size=local_tick_size,
        reported_top_transition=reported_top_transition,
    )


def _reported_transition(source: RawEventEnvelope) -> ReportedTopTransition:
    source_ref = RawSourceRef(
        ingest_id=source.ingest_id,
        payload_sha256=source.payload_sha256,
    )
    return ReportedTopTransition(
        transition_id="d" * 64,
        instrument_id=_INSTRUMENT,
        market_id=MarketId("reported-transition-market"),
        target_best_bid=Price.parse("0.4"),
        target_best_ask=Price.parse("0.6"),
        starting_best_bid=Price.parse("0.3"),
        starting_best_ask=Price.parse("0.6"),
        starting_generation=1,
        starting_economic_sha256="e" * 64,
        first_source_raw_refs=(source_ref,),
        latest_source_raw_refs=(source_ref,),
        first_source_canonical_event_id=CanonicalEventId("reported-transition-first"),
        latest_source_canonical_event_id=CanonicalEventId("reported-transition-latest"),
        connection_id=_CONNECTION,
        monotonic_epoch_id=_RUN.value,
        first_admission_ordinal=1,
        latest_admission_ordinal=1,
        first_ingest_seq=source.ingest_seq,
        latest_ingest_seq=source.ingest_seq,
        first_received_at=source.received_wall_time,
        latest_received_at=source.received_wall_time,
        first_received_monotonic_ns=source.received_monotonic.nanoseconds,
        latest_received_monotonic_ns=source.received_monotonic.nanoseconds,
        accepted_frame_count=1,
        maximum_frames=8,
        maximum_duration_ns=2_000_000_000,
        entry_source=ReportedTopTransitionEntrySource.BEST_BID_ASK,
        baseline_evidence=None,
        status=ReportedTopTransitionStatus.PENDING,
        reason=ReportedTopTransitionReason.BBO_AHEAD_OF_EXACT_L2,
    )


def _build(
    envelopes: tuple[RawEventEnvelope, ...],
    observation: AuditMismatchObservation,
):
    return build_audit_mismatch_lineage_report(
        lambda: iter(envelopes),
        _RUN,
        (observation,),
    )


def test_prior_exact_mutation_lineage_proves_freshness_boundary() -> None:
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _ws_change(1, venue_hash="h1", bid_quantity="12"),
        _start(2, response_sequence=3),
        _rest_response(3, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(4, response_sequence=3),
    )
    observation = _observation(
        classification_sequence=4,
        local_bids=(_level("0.4", "12"),),
        rest_bids=(_level("0.4", "10"),),
    )

    first = _build(envelopes, observation)
    second = _build(envelopes, observation)

    assert first == second
    assert first.report_sha256 == second.report_sha256
    result = first.results[0]
    assert result.cause is AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
    assert result.reason is AuditMismatchLineageReason.PRIOR_STATE_EXACTLY_RECONCILED
    assert result.venue_state_relation is AuditVenueStateRelation.PRIOR
    assert result.causal_relation is AuditMismatchCausalRelation.PRIOR_STATE_MATCH
    assert result.response_to_match_monotonic_ns == -3
    assert result.classification_to_match_monotonic_ns == -4
    assert result.exact_reconciliation_proven
    assert result.subsequent_exact_frame_count == 1
    assert result.subsequent_raw_sources.source_count == 1


def test_contradictory_prior_snapshot_is_not_seeded_from_rest_depth() -> None:
    envelopes = (
        # Hash/timestamp match REST, but the complete ask depth contradicts it.
        _ws_book(0, venue_hash="h0", bid_quantity="10", ask_quantity="11"),
        _ws_change(1, venue_hash="h1", bid_quantity="12"),
        _start(2, response_sequence=3),
        _rest_response(3, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(4, response_sequence=3),
    )
    observation = _observation(
        classification_sequence=4,
        local_bids=(_level("0.4", "12"),),
        rest_bids=(_level("0.4", "10"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.VENUE_STATE_NOT_FOUND
    assert result.causal_relation is AuditMismatchCausalRelation.NOT_FOUND
    assert result.venue_state_relation is AuditVenueStateRelation.NONE
    assert result.matching_websocket_source is None
    assert result.subsequent_exact_frame_count == 0
    assert not result.exact_reconciliation_proven


def test_reconstructed_crossed_depth_proves_local_book_defect() -> None:
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _ws_change(1, venue_hash="h1", bid_quantity="1", price="0.7"),
        _start(2, response_sequence=3),
        _rest_response(3, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(4, response_sequence=3),
    )
    observation = _observation(
        classification_sequence=4,
        local_bids=(_level("0.7", "1"), _level("0.4", "10")),
        rest_bids=(_level("0.4", "10"),),
        top_changed=True,
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.LOCAL_BOOK_DEFECT
    assert result.reason is AuditMismatchLineageReason.RECONSTRUCTED_LOCAL_BOOK_DEFECT
    assert result.causal_relation is AuditMismatchCausalRelation.LOCAL_BOOK_DEFECT
    assert result.venue_state_relation is AuditVenueStateRelation.PRIOR
    assert not result.exact_reconciliation_proven


def test_residual_exact_difference_is_not_hidden_by_prior_boundary() -> None:
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _ws_change(1, venue_hash="h1", bid_quantity="12"),
        _start(2, response_sequence=3),
        _rest_response(3, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(4, response_sequence=3),
    )
    observation = _observation(
        classification_sequence=4,
        local_bids=(_level("0.4", "12"), _level("0.3", "1")),
        rest_bids=(_level("0.4", "10"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
    assert result.reason is AuditMismatchLineageReason.PRIOR_STATE_RESIDUAL_DIVERGENCE
    assert not result.exact_reconciliation_proven


def test_future_matching_venue_state_proves_response_window_boundary() -> None:
    envelopes = (
        _ws_book(0, venue_hash="old", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="12"),
        _classification(3, response_sequence=2),
        _ws_book(4, venue_hash="h0", bid_quantity="12"),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "12"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_EXACT_CONVERGENCE
    assert result.causal_relation is AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
    assert result.venue_state_relation is AuditVenueStateRelation.FUTURE
    assert result.matching_websocket_ingest_seq == 4
    assert result.matching_websocket_subordinal == 0
    assert result.matching_state_representation is AuditMatchingStateRepresentation.FULL_SNAPSHOT
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert result.response_to_match_monotonic_ns == 2
    assert result.classification_to_match_monotonic_ns == 1


def test_raw_bound_pending_transition_proves_reported_top_cause() -> None:
    reported = _ws_best_bid_ask(0, best_bid="0.4", best_ask="0.6")
    transition = _reported_transition(reported)
    envelopes = (
        reported,
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(3, response_sequence=2),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.3", "10"),),
        rest_bids=(_level("0.4", "10"),),
        top_changed=True,
        reported_top_transition=transition,
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.REPORTED_TOP_TRANSITION
    assert result.reason is AuditMismatchLineageReason.ACTIVE_REPORTED_TOP_TRANSITION
    assert result.causal_relation is AuditMismatchCausalRelation.REFERENCE_TRANSITION
    assert result.matching_websocket_source == transition.latest_source_raw_refs[0]
    assert result.matching_websocket_ingest_seq == transition.latest_ingest_seq
    assert not result.exact_reconciliation_proven


def test_equal_depth_future_tick_transition_proves_reference_precision_cause() -> None:
    envelopes = (
        _ws_book(0, venue_hash="old", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(
            2,
            venue_hash="h0",
            timestamp="1000",
            bid_quantity="10",
            tick_size="0.001",
        ),
        _classification(3, response_sequence=2),
        _ws_tick_change(4, old_tick_size="0.01", new_tick_size="0.001"),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "10"),),
        local_tick_size=ExactDecimal.parse("0.01"),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.REFERENCE_PRECISION_TRANSITION
    assert result.reason is (AuditMismatchLineageReason.FUTURE_REFERENCE_PRECISION_TRANSITION)
    assert result.causal_relation is AuditMismatchCausalRelation.REFERENCE_TRANSITION
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert result.venue_state_relation is AuditVenueStateRelation.FUTURE
    assert result.matching_websocket_ingest_seq == 4
    assert not result.exact_reconciliation_proven


def test_unbound_equal_depth_scalar_mismatch_remains_unknown() -> None:
    envelopes = (
        _ws_book(0, venue_hash="old", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(
            2,
            venue_hash="h0",
            timestamp="1000",
            bid_quantity="10",
            tick_size="0.001",
        ),
        _classification(3, response_sequence=2),
        _ws_tick_change(4, old_tick_size="0.01", new_tick_size="0.005"),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "10"),),
        local_tick_size=ExactDecimal.parse("0.01"),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.UNKNOWN
    assert result.reason is AuditMismatchLineageReason.SCALAR_MISMATCH_CAUSE_UNPROVEN
    assert result.causal_relation is AuditMismatchCausalRelation.NOT_FOUND
    assert result.equality_proof is AuditEqualityProof.FULL_DEPTH_EXACT
    assert result.venue_state_relation is AuditVenueStateRelation.NONE
    assert result.matching_websocket_source is None


def test_contradictory_full_snapshot_depth_cannot_prove_future_venue_state() -> None:
    envelopes = (
        _ws_book(0, venue_hash="old", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="12"),
        _classification(3, response_sequence=2),
        # The venue identity agrees, but one complete ask level does not.
        _ws_book(4, venue_hash="h0", bid_quantity="12", ask_quantity="11"),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "12"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
    assert result.reason is AuditMismatchLineageReason.PERSISTENT_EXACT_DIVERGENCE
    assert result.causal_relation is AuditMismatchCausalRelation.PERSISTENT_DIVERGENCE
    assert result.venue_state_relation is AuditVenueStateRelation.NONE
    assert result.matching_websocket_source is None
    assert not result.exact_reconciliation_proven


def test_price_change_reconstructs_exact_same_generation_state() -> None:
    envelopes = (
        _ws_book(0, venue_hash="old", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="12"),
        _classification(3, response_sequence=2),
        # The hash is diagnostic only; the retained snapshot plus ordered
        # incremental establishes the complete project-owned depth.
        _ws_change(4, venue_hash="h0", bid_quantity="12", timestamp="1000"),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "12"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_EXACT_CONVERGENCE
    assert result.causal_relation is AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
    assert result.venue_state_relation is AuditVenueStateRelation.FUTURE
    assert (
        result.matching_state_representation
        is AuditMatchingStateRepresentation.RECONSTRUCTED_INCREMENTALS
    )
    assert result.matching_exact_frame_count == 2
    assert result.matching_source_chain.source_count == 2
    assert result.exact_reconciliation_proven


def test_exact_depth_is_authoritative_and_venue_identity_is_diagnostic_only() -> None:
    envelopes = (
        _ws_book(0, venue_hash="old", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="12"),
        _classification(3, response_sequence=2),
        _ws_book(4, venue_hash="different", bid_quantity="12"),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "12"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.FRESHNESS_RESPONSE_WINDOW_BOUNDARY
    assert result.reason is AuditMismatchLineageReason.SAME_GENERATION_EXACT_CONVERGENCE
    assert result.causal_relation is AuditMismatchCausalRelation.SAME_GENERATION_NATURAL_CONVERGENCE
    assert result.matching_websocket_source is not None
    assert result.venue_hash_diagnostic_equal is False
    assert result.venue_timestamp_diagnostic_equal is True
    assert result.exact_reconciliation_proven


def test_latest_prior_state_remains_true_exact_depth_divergence() -> None:
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(3, response_sequence=2),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "11"),),
        rest_bids=(_level("0.4", "10"),),
    )

    result = _build(envelopes, observation).results[0]

    assert result.cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
    assert result.reason is AuditMismatchLineageReason.LATEST_PRIOR_STATE_DIVERGENCE
    assert result.subsequent_exact_frame_count == 0


def test_missing_venue_state_is_retained_as_unknown() -> None:
    envelopes = (
        _ws_book(0, venue_hash="unrelated", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="12"),
        _classification(3, response_sequence=2),
    )
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "10"),),
        rest_bids=(_level("0.4", "12"),),
    )

    report = _build(envelopes, observation)

    assert report.results[0].cause is AuditMismatchCause.UNKNOWN
    assert report.results[0].reason is AuditMismatchLineageReason.VENUE_STATE_NOT_FOUND
    assert report.results[0].causal_relation is AuditMismatchCausalRelation.NOT_FOUND
    assert sum(item.count for item in report.cause_counts) == 1
    assert (
        next(item.count for item in report.cause_counts if item.cause is AuditMismatchCause.UNKNOWN)
        == 1
    )


def test_exact_frame_bound_fails_closed_to_unknown() -> None:
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _ws_change(1, venue_hash="h1", bid_quantity="11"),
        _ws_change(2, venue_hash="h2", bid_quantity="12"),
        _start(3, response_sequence=4),
        _rest_response(4, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(5, response_sequence=4),
    )
    observation = _observation(
        classification_sequence=5,
        local_bids=(_level("0.4", "12"),),
        rest_bids=(_level("0.4", "10"),),
    )

    report = build_audit_mismatch_lineage_report(
        lambda: iter(envelopes),
        _RUN,
        (observation,),
        limits=AuditMismatchReportLimits(maximum_exact_frames_per_observation=1),
    )

    assert report.results[0].cause is AuditMismatchCause.UNKNOWN
    assert report.results[0].reason is AuditMismatchLineageReason.EXACT_LINEAGE_BOUND_EXCEEDED


def test_global_comparison_bound_rejects_many_actions_before_partial_report() -> None:
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _ws_change(1, venue_hash="h1", bid_quantity="11"),
        _ws_change(2, venue_hash="h2", bid_quantity="12"),
        _start(3, response_sequence=4),
        _rest_response(4, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(5, response_sequence=4),
    )
    observation = _observation(
        classification_sequence=5,
        local_bids=(_level("0.4", "12"),),
        rest_bids=(_level("0.4", "10"),),
    )

    with pytest.raises(
        AuditMismatchReportResourceError,
        match="action-to-observation comparisons",
    ):
        build_audit_mismatch_lineage_report(
            lambda: iter(envelopes),
            _RUN,
            (observation,),
            limits=AuditMismatchReportLimits(maximum_exact_action_comparisons=2),
        )


def test_global_comparison_bound_rejects_same_asset_observation_fanout() -> None:
    second_request = "request-2"
    envelopes = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(3, response_sequence=2),
        _start(4, response_sequence=5, request_id=second_request),
        _rest_response(
            5,
            venue_hash="h0",
            timestamp="1000",
            bid_quantity="10",
            request_id=second_request,
        ),
        _classification(6, response_sequence=5, request_id=second_request),
    )
    first = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "11"),),
        rest_bids=(_level("0.4", "10"),),
    )
    second = _observation(
        classification_sequence=6,
        local_bids=(_level("0.4", "11"),),
        rest_bids=(_level("0.4", "10"),),
    )

    with pytest.raises(
        AuditMismatchReportResourceError,
        match="action-to-observation comparisons",
    ):
        build_audit_mismatch_lineage_report(
            lambda: iter(envelopes),
            _RUN,
            (first, second),
            limits=AuditMismatchReportLimits(maximum_exact_action_comparisons=1),
        )


def test_changed_journal_between_passes_is_rejected() -> None:
    original = (
        _ws_book(0, venue_hash="h0", bid_quantity="10"),
        _start(1, response_sequence=2),
        _rest_response(2, venue_hash="h0", timestamp="1000", bid_quantity="10"),
        _classification(3, response_sequence=2),
    )
    changed = (
        _ws_book(0, venue_hash="changed", bid_quantity="10"),
        *original[1:],
    )
    calls = 0

    def source():
        nonlocal calls
        calls += 1
        return iter(original if calls == 1 else changed)

    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "11"),),
        rest_bids=(_level("0.4", "10"),),
    )

    with pytest.raises(AuditMismatchReportError, match="changed between analysis passes"):
        build_audit_mismatch_lineage_report(source, _RUN, (observation,))


def test_observation_limit_is_enforced_before_scanning() -> None:
    observation = _observation(
        classification_sequence=3,
        local_bids=(_level("0.4", "11"),),
        rest_bids=(_level("0.4", "10"),),
    )

    with pytest.raises(AuditMismatchReportResourceError, match="observation count"):
        build_audit_mismatch_lineage_report(
            lambda: (),
            _RUN,
            (observation, observation),
            limits=AuditMismatchReportLimits(maximum_observations=1),
        )


def test_failed_run_artifact_loader_preserves_coarse_evidence_and_confinement(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "instrumented.json"
    artifact.write_text(
        json.dumps(
            {
                "schema_version": "gate1-instrumented-protocol-replay-v1",
                "outcomes": {
                    "counts": {"MISMATCH": 1},
                    "mismatch_proofs": {
                        "barrier_processed_equals_boundary": 1,
                        "requires_resync": 1,
                        "stable_start_to_barrier_identity": 1,
                        "start_admission_not_after_boundary": 1,
                        "total": 1,
                    },
                },
                "mismatch_differences": {
                    "count": 1,
                    "details": [
                        {
                            "ask": {
                                "local_only": 0,
                                "quantity_differences": 0,
                                "rest_only": 0,
                            },
                            "bid": {
                                "local_only": 0,
                                "quantity_differences": 1,
                                "rest_only": 0,
                            },
                            "ingest_seq": 3,
                            "instrument_id": _INSTRUMENT.value,
                            "top_changed": False,
                            "venue_hash": "h0",
                            "venue_timestamp": "1000",
                        }
                    ],
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    observations = load_instrumented_replay_mismatch_observations(
        artifact,
        allowed_root=tmp_path,
    )

    assert len(observations) == 1
    assert observations[0].exact_depth_difference is None
    assert observations[0].difference_counts.quantity_difference_count == 1
    with pytest.raises(AuditMismatchReportError, match="escapes"):
        load_instrumented_replay_mismatch_observations(
            artifact,
            allowed_root=tmp_path / "elsewhere",
        )
