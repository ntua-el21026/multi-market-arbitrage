from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from multi_market_arbitrage.availability_authority import (
    AuditAvailabilityBinding,
    audit_availability_chain_extend,
    audit_availability_chain_seed,
)
from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
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
from multi_market_arbitrage.observability.availability import (
    AvailabilityContributionReconciliation,
    AvailabilityLedgerReconciliation,
    PublishedAvailabilityEvidenceReconciliation,
)
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    AUDIT_CLASSIFICATION_V2_SCHEMA,
    AUDIT_START_V1_SCHEMA,
    AUDIT_START_V2_SCHEMA,
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditClassificationPrepareCommand,
    AuditDivergencePending,
    AuditDivergenceResolution,
    AuditMismatchCause,
    AuditOutcome,
    AuditOutcomeEvidence,
    AuditStartEvidence,
    AuditTriggerReason,
    WebSocketAdmissionWatermark,
    audit_exact_depth_identity,
    build_audit_mismatch_diagnostic,
    compare_audit_exact_depth,
)
from multi_market_arbitrage.pipeline.channels import (
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_START,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
)
from multi_market_arbitrage.pipeline.processor import (
    AuditDivergenceEligibilityRestoration,
    ProcessedEnvelope,
)
from multi_market_arbitrage.research.audit_mismatch import (
    AUDIT_MISMATCH_LINEAGE_REPORT_SCHEMA,
    AuditEqualityProof,
    AuditExternalCausalAttribution,
    AuditMatchingStateRepresentation,
    AuditMismatchCausalRelation,
    AuditMismatchLineageReason,
    AuditMismatchLineageReport,
    AuditMismatchObservation,
    AuditMismatchReplayRecoveryAuthority,
    AuditMismatchReplayRecoveryCollector,
    AuditMismatchReportError,
    AuditMismatchReportLimits,
    AuditObservationFact,
    AuditReceivedPrefixIntegrity,
    AuditRecoveryEvidenceStatus,
    AuditRecoveryStageEvidence,
    AuditRecoveryState,
    AuditVenueStateRelation,
    _reconciled_published_binding_map,  # pyright: ignore[reportPrivateUsage]
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


def _start(
    sequence: int,
    *,
    starting_admission_ordinal: int = 10,
    schema: str = AUDIT_START_V2_SCHEMA,
) -> RawEventEnvelope:
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
        trigger_reason=(
            AuditTriggerReason.UNKNOWN_LEGACY
            if schema == AUDIT_START_V1_SCHEMA
            else AuditTriggerReason.OTHER_TYPED_REASON
        ),
        schema=schema,
    )
    return _envelope(
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        metadata={"request_id": _REQUEST},
    )


def _prepare_attest_start(
    sequence: int,
    *,
    hard_expiry_monotonic_ns: int = 100_000,
    run_deadline_monotonic_ns: int = 200_000,
) -> RawEventEnvelope:
    start = _start(sequence)
    return _envelope(
        sequence,
        CONTROL_REST_AUDIT_START,
        start.payload_bytes,
        metadata={
            "audit_attempt": 1,
            "classification_protocol": AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
            "hard_expiry_monotonic_ns": hard_expiry_monotonic_ns,
            "request_id": _REQUEST,
            "response_deadline_monotonic_ns": min(
                hard_expiry_monotonic_ns,
                run_deadline_monotonic_ns,
            ),
            "run_deadline_monotonic_ns": run_deadline_monotonic_ns,
        },
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


def _prepared_classification(
    *,
    prepare_sequence: int,
    classification_sequence: int,
    response_sequence: int,
    durable_monotonic_ns: int,
    processed_monotonic_ns: int,
    hard_expiry_monotonic_ns: int = 100_000,
) -> tuple[RawEventEnvelope, RawEventEnvelope]:
    prepare = _envelope(
        prepare_sequence,
        CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
        AuditClassificationPrepareCommand(
            request_id=_REQUEST,
            response_ingest_seq=response_sequence,
            hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
        ).to_payload(),
        metadata={"request_id": _REQUEST},
    )
    classification = _envelope(
        classification_sequence,
        CONTROL_REST_AUDIT_CLASSIFY,
        AuditClassificationCommand(
            request_id=_REQUEST,
            response_ingest_seq=response_sequence,
            classification_monotonic_ns=processed_monotonic_ns,
            hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
            schema=AUDIT_CLASSIFICATION_V2_SCHEMA,
        ).to_payload(),
        metadata={
            "classification_prepare_durable_monotonic_ns": durable_monotonic_ns,
            "classification_prepare_ingest_id": prepare.ingest_id.value,
            "classification_prepare_payload_sha256": prepare.payload_sha256,
            "classification_prepare_processed_monotonic_ns": processed_monotonic_ns,
            "request_id": _REQUEST,
        },
    )
    return prepare, classification


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


def _observation(
    *,
    classification_sequence: int = 3,
    local_bid_quantity: str = "10",
) -> AuditMismatchObservation:
    local_bids = (_level("0.4", local_bid_quantity),)
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


def _prefix(
    *,
    response_boundary_ordinal: int | None = 20,
    start_schema: str = AUDIT_START_V2_SCHEMA,
) -> tuple[RawEventEnvelope, ...]:
    return (
        _ws_book(
            0,
            bid_quantity="10",
            venue_hash="baseline-hash",
            timestamp="900",
            admission_ordinal=10,
        ),
        _start(1, schema=start_schema),
        _rest_response(2, response_boundary_ordinal=response_boundary_ordinal),
        _classification(3, response_sequence=2),
    )


def _build(
    envelopes: tuple[RawEventEnvelope, ...],
    *,
    limits: AuditMismatchReportLimits | None = None,
    classification_sequence: int = 3,
    observation: AuditMismatchObservation | None = None,
    replay_recovery_authorities: tuple[AuditMismatchReplayRecoveryAuthority, ...] | None = None,
    published_availability_reconciliation: (
        PublishedAvailabilityEvidenceReconciliation | None
    ) = None,
    reconciled_published_availability_bindings: tuple[AuditAvailabilityBinding, ...] | None = None,
) -> AuditMismatchLineageReport:
    selected_observation = observation or _observation(
        classification_sequence=classification_sequence
    )
    return build_audit_mismatch_lineage_report(
        lambda: iter(envelopes),
        _RUN,
        (selected_observation,),
        limits=limits,
        replay_recovery_authorities=replay_recovery_authorities,
        published_availability_reconciliation=published_availability_reconciliation,
        reconciled_published_availability_bindings=(reconciled_published_availability_bindings),
    )


def _raw_source(envelope: RawEventEnvelope) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=envelope.ingest_id,
        payload_sha256=envelope.payload_sha256,
    )


def _typed_recovery_authority(
    envelopes: tuple[RawEventEnvelope, ...],
    *,
    start_schema: str,
    include_resolution: bool = True,
    include_release: bool = False,
) -> AuditMismatchReplayRecoveryAuthority:
    start, response, classification = envelopes[1:4]
    start_source = _raw_source(start)
    response_source = _raw_source(response)
    classification_source = _raw_source(classification)
    starting_book = AuditBookIdentity(
        instrument_id=_INSTRUMENT,
        generation=7,
        economic_content_sha256="a" * 64,
    )
    local_bids = (_level("0.4", "10"),)
    rest_bids = (_level("0.4", "12"),)
    asks = (_level("0.6", "10"),)
    market_id = MarketId.from_native(VenueId("polymarket"), "condition-v2")
    diagnostic = build_audit_mismatch_diagnostic(
        cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
        local_book=starting_book,
        local_bids=local_bids,
        local_asks=asks,
        local_raw_sources=(_raw_source(envelopes[0]),),
        rest_instrument_id=_INSTRUMENT,
        rest_market_id=market_id,
        rest_bids=rest_bids,
        rest_asks=asks,
        rest_tick_size=ExactDecimal.parse("0.01"),
        rest_minimum_order_size=Quantity.parse("1"),
        rest_neg_risk=False,
        rest_raw_sources=(response_source,),
        response_source=response_source,
    )
    outcome = AuditOutcomeEvidence(
        request_id=_REQUEST,
        connection_id=_PRIMARY_CONNECTION,
        native_asset_id=_ASSET,
        attempt=1,
        request_started_monotonic_ns=start.received_monotonic.nanoseconds,
        starting_admission_ordinal=10,
        starting_processed_ordinal=10,
        starting_book=starting_book,
        outcome=AuditOutcome.MISMATCH,
        reason=None,
        response_ingest_id=response.ingest_id,
        raw_response_sha256=response.payload_sha256,
        response_received_wall_time=response.received_wall_time,
        response_received_monotonic_ns=response.received_monotonic.nanoseconds,
        response_boundary_ordinal=20,
        barrier_processed_ordinal=20,
        barrier_book=starting_book,
        requires_resync=True,
        mismatch_diagnostic=diagnostic,
        audit_start_schema=start_schema,
        trigger_reason=(
            AuditTriggerReason.UNKNOWN_LEGACY
            if start_schema == AUDIT_START_V1_SCHEMA
            else AuditTriggerReason.OTHER_TYPED_REASON
        ),
        audit_start_source=start_source,
        classification_source=classification_source,
    )
    pending = AuditDivergencePending.create(
        market_id=market_id,
        audit_outcome=outcome,
        audit_start_control_source=start_source,
        classification_source=classification_source,
        classification_control_source=classification_source,
        classification_received_wall_time=classification.received_wall_time,
        classification_received_monotonic_ns=(classification.received_monotonic.nanoseconds),
    )
    if not include_resolution:
        return AuditMismatchReplayRecoveryAuthority(
            collector_run_id=_RUN,
            classification_ingest_seq=classification.ingest_seq,
            observed_divergence=pending,
            recorded_starting_book=starting_book,
            replay_starting_book_generation_rebased=False,
        )
    resnapshot = envelopes[4]
    resolution_source = _raw_source(resnapshot)
    resolution = AuditDivergenceResolution(
        divergence_id=pending.divergence_id,
        request_id=pending.request_id,
        instrument_id=pending.instrument_id,
        market_id=pending.market_id,
        audit_start_schema=start_schema,
        trigger_reason=outcome.trigger_reason,
        mismatch_response_source=response_source,
        authoritative_snapshot_source=resolution_source,
        authoritative_rest_book=diagnostic.rest_book,
        recovered_book=AuditBookIdentity(
            instrument_id=_INSTRUMENT,
            generation=8,
            economic_content_sha256="b" * 64,
        ),
        observed_divergence=pending,
        resolved_at=resnapshot.received_wall_time,
        resolved_monotonic_ns=resnapshot.received_monotonic.nanoseconds,
    )
    resolution_stage = AuditRecoveryStageEvidence(
        status=AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY,
        reason="test exact accepted authoritative REST resnapshot",
        ingest_seq=resnapshot.ingest_seq,
        source=resolution_source,
        connection_id=resnapshot.connection_id,
        subscription_id=resnapshot.subscription_id,
        received_monotonic_ns=resnapshot.received_monotonic.nanoseconds,
        event_type="authoritative_rest_resnapshot",
        exact_depth_identity=diagnostic.rest_book.depth,
    )
    if not include_release:
        return AuditMismatchReplayRecoveryAuthority(
            collector_run_id=_RUN,
            classification_ingest_seq=classification.ingest_seq,
            observed_divergence=pending,
            recorded_starting_book=starting_book,
            replay_starting_book_generation_rebased=False,
            authoritative_resolution=resolution,
            resolution_stage=resolution_stage,
        )
    release = envelopes[-1]
    release_source = _raw_source(release)
    binding = AuditAvailabilityBinding(
        request_id=pending.request_id,
        divergence_id=pending.divergence_id,
        instrument_id=pending.instrument_id.value,
        market_id=pending.market_id.value,
        audit_start_source=start_source,
        audit_response_source=response_source,
        audit_classification_source=classification_source,
        authoritative_resolution_source=resolution_source,
        replacement_release_source=release_source,
    )
    restoration = AuditDivergenceEligibilityRestoration(
        divergence_id=pending.divergence_id,
        request_id=pending.request_id,
        instrument_id=pending.instrument_id,
        market_id=pending.market_id,
        audit_start_schema=start_schema,
        authoritative_snapshot_source=resolution_source,
        replacement_release_source=release_source,
        replacement_connection_id=release.connection_id,
        capture_state_sha256="c" * 64,
        availability_binding=binding,
        restored_ingest_seq=release.ingest_seq,
        restored_at=release.received_wall_time,
        restored_monotonic=release.received_monotonic,
    )
    release_stage = AuditRecoveryStageEvidence(
        status=AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY,
        reason="test exact accepted v2 replacement release",
        ingest_seq=release.ingest_seq,
        source=release_source,
        connection_id=release.connection_id,
        subscription_id=release.subscription_id,
        received_monotonic_ns=release.received_monotonic.nanoseconds,
        event_type="audit_replacement_release",
    )
    return AuditMismatchReplayRecoveryAuthority(
        collector_run_id=_RUN,
        classification_ingest_seq=classification.ingest_seq,
        observed_divergence=pending,
        recorded_starting_book=starting_book,
        replay_starting_book_generation_rebased=False,
        authoritative_resolution=resolution,
        resolution_stage=resolution_stage,
        eligibility_restoration=restoration,
        replacement_release_stage=release_stage,
    )


def _published_reconciliation(
    binding: AuditAvailabilityBinding,
) -> PublishedAvailabilityEvidenceReconciliation:
    return _published_reconciliation_for_bindings((binding,))


def _published_reconciliation_for_bindings(
    bindings: tuple[AuditAvailabilityBinding, ...],
) -> PublishedAvailabilityEvidenceReconciliation:
    chain = audit_availability_chain_seed("contribution")
    for binding in bindings:
        chain = audit_availability_chain_extend(
            chain,
            "contribution",
            binding,
        )
    return PublishedAvailabilityEvidenceReconciliation(
        interval_reconciliation=AvailabilityLedgerReconciliation(
            collector_run_id=_RUN,
            interval_count=1,
            unavailable_duration_nanoseconds=1,
            longest_interval_nanoseconds=1,
            open_at_shutdown_intervals=0,
            reconciled=True,
            discrepancies=(),
        ),
        contribution_reconciliation=AvailabilityContributionReconciliation(
            interval_count=1,
            contribution_count=len(bindings),
            audit_mismatch_contribution_count=len(bindings),
            audit_mismatch_binding_chain_sha256=chain,
            audit_mismatch_contribution_chain_sha256="d" * 64,
            per_instrument_duration_nanoseconds=tuple(
                (binding.instrument_id, 1) for binding in bindings
            ),
            reconciled=True,
            discrepancies=(),
        ),
        reconciled=True,
        discrepancies=(),
    )


def _synthetic_published_binding(label: str) -> AuditAvailabilityBinding:
    def source(stage: str) -> RawSourceRef:
        identity = f"published-{label}-{stage}"
        return RawSourceRef(
            ingest_id=IngestId(identity),
            payload_sha256=hashlib.sha256(identity.encode()).hexdigest(),
        )

    return AuditAvailabilityBinding(
        request_id=f"published-request-{label}",
        divergence_id=hashlib.sha256(f"published-divergence-{label}".encode()).hexdigest(),
        instrument_id=f"published-instrument-{label}",
        market_id=f"published-market-{label}",
        audit_start_source=source("start"),
        audit_response_source=source("response"),
        audit_classification_source=source("classification"),
        authoritative_resolution_source=source("resolution"),
        replacement_release_source=source("release"),
    )


@pytest.mark.parametrize("case", ["wrong", "missing", "extra", "duplicate"])
def test_published_bindings_must_exactly_cover_two_replay_restorations(case: str) -> None:
    first = _synthetic_published_binding("first")
    second = _synthetic_published_binding("second")
    expected = (first, second)
    if case == "wrong":
        supplied = (first, replace(second, market_id="cross-bound-market"))
    elif case == "missing":
        supplied = (first,)
    elif case == "extra":
        supplied = (*expected, _synthetic_published_binding("extra"))
    else:
        supplied = (first, first)
    reconciliation = _published_reconciliation_for_bindings(supplied)

    with pytest.raises(AuditMismatchReportError):
        _reconciled_published_binding_map(reconciliation, supplied, expected)


def test_v2_reconciliation_marker_requires_exact_replay_binding_equality() -> None:
    binding = _synthetic_published_binding("v2")
    reconciliation = PublishedAvailabilityEvidenceReconciliation(
        interval_reconciliation=None,
        contribution_reconciliation=None,
        reconciled=True,
        discrepancies=(),
    )

    assert _reconciled_published_binding_map(
        reconciliation,
        (binding,),
        (binding,),
    ) == {binding.divergence_id: binding}

    with pytest.raises(AuditMismatchReportError, match="differ from replay"):
        _reconciled_published_binding_map(
            reconciliation,
            (replace(binding, market_id="tampered"),),
            (binding,),
        )

    inconsistent = replace(
        reconciliation,
        discrepancies=("SYNTHETIC_UNRESOLVED_DISCREPANCY",),
    )
    with pytest.raises(AuditMismatchReportError, match="not successfully reconciled"):
        _reconciled_published_binding_map(
            inconsistent,
            (binding,),
            (binding,),
        )


def _processed(
    sequence: int,
    *,
    opened: tuple[AuditDivergencePending, ...] = (),
    resolved: tuple[AuditDivergenceResolution, ...] = (),
    restored: tuple[AuditDivergenceEligibilityRestoration, ...] = (),
    outcomes: tuple[AuditOutcomeEvidence, ...] = (),
) -> ProcessedEnvelope:
    return ProcessedEnvelope(
        ingest_seq=sequence,
        success=True,
        canonical_event_count=0,
        scan_count=0,
        needs_resync=bool(opened),
        affected_instrument_ids=(),
        reference_refresh_requests=(),
        book_freshness_updates=(),
        scan_triggers=(),
        error_type=None,
        audit_outcomes=outcomes,
        audit_divergences_opened=opened,
        audit_divergences_resolved=resolved,
        audit_divergences_eligibility_restored=restored,
    )


def _released_v2_authority_fixture() -> tuple[
    tuple[RawEventEnvelope, ...],
    AuditMismatchReplayRecoveryAuthority,
]:
    release = _envelope(
        6,
        CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
        canonical_json_bytes({"schema": "test-streamed-replacement-release"}),
        connection_id=_RECOVERY_CONNECTION,
    )
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="streamed-v2-replacement",
            timestamp="1003",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
        release,
    )
    return (
        envelopes,
        _typed_recovery_authority(
            envelopes,
            start_schema=AUDIT_START_V2_SCHEMA,
            include_release=True,
        ),
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


def test_independent_received_prefix_oracle_detects_canonical_mutation_defect() -> None:
    envelopes = (
        _ws_book(
            0,
            bid_quantity="10",
            venue_hash="baseline-hash",
            timestamp="900",
            admission_ordinal=10,
        ),
        _start(1),
        _ws_change(
            2,
            bid_quantity="11",
            venue_hash="received-prefix-change",
            timestamp="901",
            admission_ordinal=20,
        ),
        _rest_response(3, response_boundary_ordinal=20),
        _classification(4, response_sequence=3),
    )

    result = _build(envelopes, classification_sequence=4).results[0]
    dimensions = result.evidence_dimensions
    oracle = dimensions.received_prefix_oracle
    expected_oracle_identity = audit_exact_depth_identity(
        (_level("0.4", "11"),),
        (_level("0.6", "10"),),
    )

    assert dimensions.observation_fact is AuditObservationFact.EXACT_FULL_DEPTH_DIVERGENCE
    assert dimensions.internal_received_prefix_integrity is (
        AuditReceivedPrefixIntegrity.INTERNAL_RECONSTRUCTION_DEFECT
    )
    assert oracle.production_local_depth_identity == result.barrier_exact_depth_identity
    assert oracle.reconstructed_depth_identity == expected_oracle_identity
    assert oracle.reconstructed_depth_identity != oracle.production_local_depth_identity
    assert oracle.response_boundary_admission_ordinal == 20
    assert oracle.last_applied_admission_ordinal == 20
    assert oracle.exact_frame_count == 2
    assert oracle.source_chain.source_count == 2
    assert oracle.unresolved_evidence == ()
    assert dimensions.external_causal_attribution is AuditExternalCausalAttribution.NOT_FOUND


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


def test_proven_received_prefix_does_not_invent_an_external_recovery_cause() -> None:
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
    dimensions = result.evidence_dimensions
    oracle = dimensions.received_prefix_oracle

    assert dimensions.observation_fact is AuditObservationFact.EXACT_FULL_DEPTH_DIVERGENCE
    assert dimensions.internal_received_prefix_integrity is (
        AuditReceivedPrefixIntegrity.PROVEN_RECEIVED_PREFIX_RECONSTRUCTION
    )
    assert oracle.reconstructed_depth_identity == oracle.production_local_depth_identity
    assert oracle.reconstructed_depth_identity == result.barrier_exact_depth_identity
    assert oracle.last_applied_admission_ordinal == 10
    assert oracle.exact_frame_count == 1
    assert oracle.unresolved_evidence == ()
    assert dimensions.external_evidence_relation is (
        AuditExternalCausalAttribution.RECOVERY_GENERATION_MATCH
    )
    assert dimensions.external_causal_attribution is (
        AuditExternalCausalAttribution.EXTERNAL_CAUSE_UNATTRIBUTED
    )
    assert dimensions.recovery_state is AuditRecoveryState.UNRESOLVED


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


def test_v1_exact_replay_resnapshot_resolves_without_synthesizing_release() -> None:
    envelopes = (
        *_prefix(start_schema=AUDIT_START_V1_SCHEMA),
        _rest_resnapshot(4),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="v1-replacement",
            timestamp="1003",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
    )
    authority = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V1_SCHEMA,
    )

    result = _build(
        envelopes,
        replay_recovery_authorities=(authority,),
    ).results[0]

    chronology = result.recovery_chronology
    assert result.evidence_dimensions.recovery_state is (
        AuditRecoveryState.RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
    )
    assert chronology.rest_resnapshot.status is (AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY)
    assert chronology.recovery_completion.status is (
        AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY
    )
    assert chronology.replacement_release.status is (AuditRecoveryEvidenceStatus.NOT_RETAINED)
    assert "historical v1" in chronology.replacement_release.reason
    assert chronology.research_eligibility_restoration.status is (
        AuditRecoveryEvidenceStatus.NOT_RETAINED
    )
    assert "legacy-unverifiable" in chronology.research_eligibility_restoration.reason


def test_v2_exact_release_requires_separate_published_ledger_reconciliation() -> None:
    release = _envelope(
        6,
        CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
        canonical_json_bytes({"schema": "test-exact-replacement-release"}),
        connection_id=_RECOVERY_CONNECTION,
    )
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="v2-replacement",
            timestamp="1003",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
        release,
    )
    authority = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V2_SCHEMA,
        include_release=True,
    )
    restoration = authority.eligibility_restoration
    assert restoration is not None

    replay_only = _build(
        envelopes,
        replay_recovery_authorities=(authority,),
    ).results[0]
    assert replay_only.recovery_chronology.replacement_release.status is (
        AuditRecoveryEvidenceStatus.DETERMINISTIC_REPLAY
    )
    assert replay_only.recovery_chronology.research_eligibility_restoration.status is (
        AuditRecoveryEvidenceStatus.NOT_RETAINED
    )
    assert replay_only.evidence_dimensions.recovery_state is (
        AuditRecoveryState.RESOLVED_BY_AUTHORITATIVE_RESNAPSHOT
    )

    reconciliation = _published_reconciliation(restoration.availability_binding)
    first = _build(
        envelopes,
        replay_recovery_authorities=(authority,),
        published_availability_reconciliation=reconciliation,
        reconciled_published_availability_bindings=(restoration.availability_binding,),
    )
    second = _build(
        envelopes,
        replay_recovery_authorities=(authority,),
        published_availability_reconciliation=reconciliation,
        reconciled_published_availability_bindings=(restoration.availability_binding,),
    )

    assert first == second
    assert first.report_sha256 == second.report_sha256
    restored = first.results[0]
    assert restored.evidence_dimensions.recovery_state is (AuditRecoveryState.ELIGIBILITY_RESTORED)
    assert restored.recovery_chronology.research_eligibility_restoration.status is (
        AuditRecoveryEvidenceStatus.PUBLISHED_LEDGER_RECONCILIATION
    )


@pytest.mark.parametrize(
    "field",
    [
        "audit_start_source",
        "audit_response_source",
        "audit_classification_source",
    ],
)
def test_replay_restoration_independently_cross_binds_every_audit_raw_source(
    field: str,
) -> None:
    release = _envelope(
        6,
        CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
        canonical_json_bytes({"schema": "test-exact-replacement-release"}),
        connection_id=_RECOVERY_CONNECTION,
    )
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="v2-replacement",
            timestamp="1003",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
        release,
    )
    authority = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V2_SCHEMA,
        include_release=True,
    )
    restoration = authority.eligibility_restoration
    assert restoration is not None
    tampered_binding = replace(
        restoration.availability_binding,
        **{field: _raw_source(envelopes[0])},
    )
    tampered_restoration = replace(
        restoration,
        availability_binding=tampered_binding,
    )

    with pytest.raises(AuditMismatchReportError, match="exact divergence authority"):
        replace(authority, eligibility_restoration=tampered_restoration)


def test_rejected_rest_and_raw_generation_remain_unresolved_with_replay_authority() -> None:
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="raw-generation-only",
            timestamp="1003",
            admission_ordinal=1,
            connection_id=_RECOVERY_CONNECTION,
        ),
    )
    authority = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V2_SCHEMA,
        include_resolution=False,
    )

    result = _build(
        envelopes,
        replay_recovery_authorities=(authority,),
    ).results[0]

    assert result.evidence_dimensions.recovery_state is AuditRecoveryState.UNRESOLVED
    assert result.recovery_chronology.rest_resnapshot.status is (
        AuditRecoveryEvidenceStatus.RAW_JOURNAL
    )
    assert result.recovery_chronology.recovery_completion.status is (
        AuditRecoveryEvidenceStatus.NOT_RETAINED
    )


def test_replay_recovery_authority_rejects_cross_bound_exact_difference() -> None:
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
    )
    authority = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V2_SCHEMA,
    )

    with pytest.raises(AuditMismatchReportError, match="cross-bound"):
        _build(
            envelopes,
            observation=_observation(local_bid_quantity="9"),
            replay_recovery_authorities=(authority,),
        )


def test_v1_replay_generation_rebase_cross_binds_recorded_and_effective_book() -> None:
    envelopes = (
        *_prefix(start_schema=AUDIT_START_V1_SCHEMA),
        _rest_resnapshot(4),
    )
    base = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V1_SCHEMA,
        include_resolution=False,
    )
    base_outcome = base.observed_divergence.audit_outcome
    effective_book = replace(
        base_outcome.starting_book,
        generation=base_outcome.starting_book.generation + 1,
    )
    assert base_outcome.mismatch_diagnostic is not None
    outcome = replace(
        base_outcome,
        starting_book=effective_book,
        barrier_book=effective_book,
        mismatch_diagnostic=replace(
            base_outcome.mismatch_diagnostic,
            local_book=effective_book,
        ),
    )
    pending = AuditDivergencePending.create(
        market_id=base.observed_divergence.market_id,
        audit_outcome=outcome,
        audit_start_control_source=base.observed_divergence.audit_start_control_source,
        classification_source=base.observed_divergence.classification_source,
        classification_control_source=(base.observed_divergence.classification_control_source),
        classification_received_wall_time=(
            base.observed_divergence.classification_received_wall_time
        ),
        classification_received_monotonic_ns=(
            base.observed_divergence.classification_received_monotonic_ns
        ),
    )
    collector = AuditMismatchReplayRecoveryCollector(_RUN)
    collector.observe_replayed_envelope(
        envelopes[1],
        _processed(envelopes[1].ingest_seq),
    )
    collector.observe_replayed_envelope(
        envelopes[3],
        _processed(
            envelopes[3].ingest_seq,
            opened=(pending,),
            outcomes=(outcome,),
        ),
    )
    (authority,) = collector.authorities
    assert authority.recorded_starting_book == base.recorded_starting_book
    assert authority.replay_starting_book_generation_rebased

    result = _build(
        envelopes,
        replay_recovery_authorities=(authority,),
    ).results[0]

    assert result.starting_book == base.recorded_starting_book
    assert result.replay_effective_starting_book == effective_book
    assert result.replay_starting_book_generation_rebased is True


def test_replay_generation_rebase_rejects_v2_or_economic_change() -> None:
    envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
    )
    base = _typed_recovery_authority(
        envelopes,
        start_schema=AUDIT_START_V2_SCHEMA,
        include_resolution=False,
    )

    with pytest.raises(AuditMismatchReportError, match="exact legacy authority"):
        AuditMismatchReplayRecoveryAuthority(
            collector_run_id=_RUN,
            classification_ingest_seq=base.classification_ingest_seq,
            observed_divergence=base.observed_divergence,
            recorded_starting_book=replace(
                base.recorded_starting_book,
                generation=base.recorded_starting_book.generation + 1,
            ),
            replay_starting_book_generation_rebased=True,
        )
    legacy_envelopes = (
        *_prefix(start_schema=AUDIT_START_V1_SCHEMA),
        _rest_resnapshot(4),
    )
    legacy = _typed_recovery_authority(
        legacy_envelopes,
        start_schema=AUDIT_START_V1_SCHEMA,
        include_resolution=False,
    )
    with pytest.raises(AuditMismatchReportError, match="exact legacy authority"):
        AuditMismatchReplayRecoveryAuthority(
            collector_run_id=_RUN,
            classification_ingest_seq=legacy.classification_ingest_seq,
            observed_divergence=legacy.observed_divergence,
            recorded_starting_book=replace(
                legacy.recorded_starting_book,
                generation=legacy.recorded_starting_book.generation + 1,
                economic_content_sha256="f" * 64,
            ),
            replay_starting_book_generation_rebased=True,
        )


def test_replay_recovery_collector_streams_exact_v2_restoration_authority() -> None:
    envelopes, expected = _released_v2_authority_fixture()
    resolution = expected.authoritative_resolution
    restoration = expected.eligibility_restoration
    assert resolution is not None
    assert restoration is not None
    collector = AuditMismatchReplayRecoveryCollector(_RUN)

    collector.observe_replayed_envelope(envelopes[1], _processed(envelopes[1].ingest_seq))
    collector.observe_replayed_envelope(
        envelopes[3],
        _processed(
            envelopes[3].ingest_seq,
            opened=(expected.observed_divergence,),
            outcomes=(expected.observed_divergence.audit_outcome,),
        ),
    )
    collector.observe_replayed_envelope(
        envelopes[4],
        _processed(envelopes[4].ingest_seq, resolved=(resolution,)),
    )
    collector.observe_replayed_envelope(
        envelopes[6],
        _processed(envelopes[6].ingest_seq, restored=(restoration,)),
    )

    (actual,) = collector.authorities
    assert actual.observed_divergence == expected.observed_divergence
    assert actual.recorded_starting_book == expected.recorded_starting_book
    assert actual.authoritative_resolution == expected.authoritative_resolution
    assert actual.eligibility_restoration == expected.eligibility_restoration
    assert actual.replacement_release_stage is not None
    assert actual.replacement_release_stage.ingest_seq == envelopes[6].ingest_seq
    assert actual.replacement_release_stage.source == _raw_source(envelopes[6])
    assert actual.eligibility_restoration is not None
    assert actual.eligibility_restoration.restored_ingest_seq == envelopes[6].ingest_seq
    assert actual.eligibility_restoration.replacement_release_source == _raw_source(envelopes[6])
    assert len(collector.observations) == 1


def test_replay_recovery_collector_rejects_release_before_resolution() -> None:
    envelopes, expected = _released_v2_authority_fixture()
    restoration = expected.eligibility_restoration
    assert restoration is not None
    collector = AuditMismatchReplayRecoveryCollector(_RUN)
    collector.observe_replayed_envelope(envelopes[1], _processed(envelopes[1].ingest_seq))
    collector.observe_replayed_envelope(
        envelopes[3],
        _processed(
            envelopes[3].ingest_seq,
            opened=(expected.observed_divergence,),
            outcomes=(expected.observed_divergence.audit_outcome,),
        ),
    )
    collector.observe_replayed_envelope(
        envelopes[6],
        _processed(envelopes[6].ingest_seq, restored=(restoration,)),
    )

    with pytest.raises(AuditMismatchReportError, match="requires authoritative resolution"):
        _ = collector.authorities


def test_replay_recovery_collector_rejects_tampered_restoration_raw_sequence() -> None:
    envelopes, expected = _released_v2_authority_fixture()
    resolution = expected.authoritative_resolution
    restoration = expected.eligibility_restoration
    assert resolution is not None
    assert restoration is not None
    collector = AuditMismatchReplayRecoveryCollector(_RUN)
    collector.observe_replayed_envelope(envelopes[1], _processed(envelopes[1].ingest_seq))
    collector.observe_replayed_envelope(
        envelopes[3],
        _processed(
            envelopes[3].ingest_seq,
            opened=(expected.observed_divergence,),
            outcomes=(expected.observed_divergence.audit_outcome,),
        ),
    )
    collector.observe_replayed_envelope(
        envelopes[4],
        _processed(envelopes[4].ingest_seq, resolved=(resolution,)),
    )

    with pytest.raises(AuditMismatchReportError, match="differs from its raw envelope"):
        collector.observe_replayed_envelope(
            envelopes[6],
            _processed(
                envelopes[6].ingest_seq,
                restored=(
                    replace(
                        restoration,
                        restored_ingest_seq=restoration.restored_ingest_seq + 1,
                    ),
                ),
            ),
        )


def test_replay_recovery_collector_streams_all_262_resolutions_deterministically() -> None:
    base_envelopes = (
        *_prefix(),
        _rest_resnapshot(4),
    )
    base = _typed_recovery_authority(
        base_envelopes,
        start_schema=AUDIT_START_V2_SCHEMA,
    )
    base_resolution = base.authoritative_resolution
    assert base_resolution is not None
    events: list[tuple[RawEventEnvelope, ProcessedEnvelope]] = []
    observations: list[AuditMismatchObservation] = []
    for index in range(262):
        classification_sequence = 100 + index
        request_id = f"streamed-recovery-{index}"
        start = _envelope(
            10_000 + index,
            CONTROL_REST_AUDIT_START,
            AuditStartEvidence(
                request_id=request_id,
                connection_id=_PRIMARY_CONNECTION,
                native_asset_id=_ASSET,
                attempt=1,
                request_started_monotonic_ns=1_100,
                starting_admission_watermark=WebSocketAdmissionWatermark(
                    connection_id=_PRIMARY_CONNECTION,
                    ordinal=10,
                ),
                starting_processed_watermark=WebSocketAdmissionWatermark(
                    connection_id=_PRIMARY_CONNECTION,
                    ordinal=10,
                ),
                starting_book=base.recorded_starting_book,
                trigger_reason=AuditTriggerReason.OTHER_TYPED_REASON,
                schema=AUDIT_START_V2_SCHEMA,
            ).to_payload(),
        )
        events.append((start, _processed(start.ingest_seq)))
        classification = _envelope(
            classification_sequence,
            CONTROL_REST_AUDIT_CLASSIFY,
            canonical_json_bytes({"streamed_classification": index}),
        )
        classification_source = _raw_source(classification)
        outcome = replace(
            base.observed_divergence.audit_outcome,
            request_id=request_id,
            audit_start_source=_raw_source(start),
            classification_source=classification_source,
        )
        pending = AuditDivergencePending.create(
            market_id=base.observed_divergence.market_id,
            audit_outcome=outcome,
            audit_start_control_source=_raw_source(start),
            classification_source=classification_source,
            classification_control_source=classification_source,
            classification_received_wall_time=classification.received_wall_time,
            classification_received_monotonic_ns=(classification.received_monotonic.nanoseconds),
        )
        observations.append(
            replace(
                _observation(classification_sequence=classification_sequence),
                classification_source=classification_source,
            )
        )
        events.append(
            (
                classification,
                _processed(
                    classification_sequence,
                    opened=(pending,),
                    outcomes=(outcome,),
                ),
            )
        )
        resolution_sequence = 1_000 + index
        resolution_envelope = _envelope(
            resolution_sequence,
            REST_BOOK,
            canonical_json_bytes({"streamed_resolution": index}),
        )
        resolution = replace(
            base_resolution,
            divergence_id=pending.divergence_id,
            request_id=request_id,
            authoritative_snapshot_source=_raw_source(resolution_envelope),
            observed_divergence=pending,
            resolved_at=resolution_envelope.received_wall_time,
            resolved_monotonic_ns=(resolution_envelope.received_monotonic.nanoseconds),
        )
        events.append(
            (
                resolution_envelope,
                _processed(resolution_sequence, resolved=(resolution,)),
            )
        )

    first = AuditMismatchReplayRecoveryCollector(_RUN, tuple(observations))
    second = AuditMismatchReplayRecoveryCollector(_RUN, tuple(observations))
    discovered = AuditMismatchReplayRecoveryCollector(_RUN)
    for envelope, processed in events:
        first.observe_replayed_envelope(envelope, processed)
        second.observe_replayed_envelope(envelope, processed)
        discovered.observe_replayed_envelope(envelope, processed)

    assert first.authorities == second.authorities
    assert discovered.authorities == first.authorities
    assert len(first.authorities) == 262
    assert len(discovered.observations) == 262
    assert len({item.observed_divergence.divergence_id for item in first.authorities}) == 262
    assert all(item.authoritative_resolution is not None for item in first.authorities)
    assert first.authorities[64].classification_ingest_seq == 164


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
    dimensions = result.evidence_dimensions
    assert dimensions.internal_received_prefix_integrity is (
        AuditReceivedPrefixIntegrity.INTERNAL_RECONSTRUCTION_INDETERMINATE
    )
    assert dimensions.external_evidence_relation is AuditExternalCausalAttribution.NOT_FOUND
    assert dimensions.external_causal_attribution is AuditExternalCausalAttribution.NOT_FOUND
    assert dimensions.recovery_state is AuditRecoveryState.PENDING
    assert "ORACLE_RESPONSE_BOUNDARY_NOT_RETAINED" in (
        dimensions.received_prefix_oracle.unresolved_evidence
    )


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


def test_audit_start_time_must_equal_raw_receive_time() -> None:
    original = _start(1)
    payload = json.loads(original.payload_bytes)
    payload["request_started_monotonic_ns"] = original.received_monotonic.nanoseconds + 1
    tampered = _envelope(
        1,
        CONTROL_REST_AUDIT_START,
        canonical_json_bytes(payload),
        metadata={"request_id": _REQUEST},
    )
    envelopes = (_prefix()[0], tampered, *_prefix()[2:])

    with pytest.raises(
        AuditMismatchReportError,
        match="audit start timestamp differs from raw receive evidence",
    ):
        _build(envelopes)


def test_current_prepare_attest_timing_binds_decision_before_final_receive() -> None:
    prepare, classification = _prepared_classification(
        prepare_sequence=3,
        classification_sequence=4,
        response_sequence=2,
        durable_monotonic_ns=1_350,
        processed_monotonic_ns=1_380,
    )
    envelopes = (
        _prefix()[0],
        _prepare_attest_start(1),
        _rest_response(2),
        prepare,
        classification,
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="prepared-replacement",
            timestamp="1003",
            admission_ordinal=21,
        ),
    )

    result = _build(envelopes, classification_sequence=4).results[0]

    assert result.classification_monotonic_ns == 1_380
    assert result.classification_received_monotonic_ns == 1_400
    assert result.classification_monotonic_ns < result.classification_received_monotonic_ns


def test_current_prepare_attest_allows_final_receive_after_decision_deadline() -> None:
    prepare, classification = _prepared_classification(
        prepare_sequence=3,
        classification_sequence=4,
        response_sequence=2,
        durable_monotonic_ns=1_350,
        processed_monotonic_ns=1_380,
        hard_expiry_monotonic_ns=1_390,
    )
    envelopes = (
        _prefix()[0],
        _prepare_attest_start(1, hard_expiry_monotonic_ns=1_390),
        _rest_response(2),
        prepare,
        classification,
        _ws_book(
            5,
            bid_quantity="12",
            venue_hash="prepared-replacement-after-deadline",
            timestamp="1003",
            admission_ordinal=21,
        ),
    )

    result = _build(envelopes, classification_sequence=4).results[0]

    assert result.classification_monotonic_ns == 1_380
    assert result.classification_received_monotonic_ns == 1_400
    assert result.classification_monotonic_ns < 1_390 < result.classification_received_monotonic_ns


def test_unmatched_prepare_history_does_not_consume_target_retention() -> None:
    foreign_preparations = tuple(
        _envelope(
            sequence,
            CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
            AuditClassificationPrepareCommand(
                request_id=f"foreign-request-{sequence}",
                response_ingest_seq=2,
                hard_expiry_monotonic_ns=100_000,
            ).to_payload(),
            metadata={"request_id": f"foreign-request-{sequence}"},
        )
        for sequence in range(3, 131)
    )
    prepare, classification = _prepared_classification(
        prepare_sequence=131,
        classification_sequence=132,
        response_sequence=2,
        durable_monotonic_ns=14_150,
        processed_monotonic_ns=14_180,
    )
    envelopes = (
        _prefix()[0],
        _prepare_attest_start(1),
        _rest_response(2),
        *foreign_preparations,
        prepare,
        classification,
        _ws_book(
            133,
            bid_quantity="12",
            venue_hash="prepared-replacement-after-unmatched-history",
            timestamp="1003",
            admission_ordinal=21,
        ),
    )
    limits = replace(AuditMismatchReportLimits(), maximum_observations=1)

    report = _build(
        envelopes,
        limits=limits,
        classification_sequence=132,
    )

    assert report.observation_count == 1
    assert report.results[0].classification_ingest_seq == 132


@pytest.mark.parametrize("response_deadline_monotonic_ns", [1_150, 1_200])
def test_current_prepare_attest_response_must_precede_deadline(
    response_deadline_monotonic_ns: int,
) -> None:
    prepare, classification = _prepared_classification(
        prepare_sequence=3,
        classification_sequence=4,
        response_sequence=2,
        durable_monotonic_ns=1_350,
        processed_monotonic_ns=1_380,
    )
    envelopes = (
        _prefix()[0],
        _prepare_attest_start(
            1,
            run_deadline_monotonic_ns=response_deadline_monotonic_ns,
        ),
        _rest_response(2),
        prepare,
        classification,
    )

    with pytest.raises(
        AuditMismatchReportError,
        match="response receipt reached or exceeded its response deadline",
    ):
        _build(envelopes, classification_sequence=4)


@pytest.mark.parametrize(
    "tamper",
    [
        "durability_before_prepare",
        "processed_mismatch",
        "prepare_source",
        "missing_prepare",
    ],
)
def test_current_prepare_attest_timing_fails_closed(tamper: str) -> None:
    prepare, classification = _prepared_classification(
        prepare_sequence=3,
        classification_sequence=4,
        response_sequence=2,
        durable_monotonic_ns=1_350,
        processed_monotonic_ns=1_380,
    )
    metadata = dict(classification.transport_metadata)
    envelopes: tuple[RawEventEnvelope, ...]
    if tamper == "durability_before_prepare":
        metadata["classification_prepare_durable_monotonic_ns"] = 1_250
    elif tamper == "processed_mismatch":
        metadata["classification_prepare_processed_monotonic_ns"] = 1_379
    elif tamper == "prepare_source":
        metadata["classification_prepare_payload_sha256"] = "f" * 64
    else:
        envelopes = (
            _prefix()[0],
            _prepare_attest_start(1),
            _rest_response(2),
            _ws_book(
                3,
                bid_quantity="10",
                venue_hash="missing-preparation-placeholder",
                timestamp="1001",
                admission_ordinal=11,
            ),
            classification,
        )
        with pytest.raises(AuditMismatchReportError, match="lacks its durable preparation"):
            _build(envelopes, classification_sequence=4)
        return
    tampered = RawEventEnvelope.capture(
        collector_run_id=classification.collector_run_id,
        ingest_seq=classification.ingest_seq,
        ingest_id=classification.ingest_id,
        venue_id=classification.venue_id,
        connection_id=classification.connection_id,
        subscription_id=classification.subscription_id,
        channel=classification.channel,
        transport_type=classification.transport_type,
        subscription_semantics_hash=classification.subscription_semantics_hash,
        received_wall_time=classification.received_wall_time,
        received_monotonic=classification.received_monotonic,
        payload_bytes=classification.payload_bytes,
        payload_encoding=classification.payload_encoding,
        transport_metadata=metadata,
    )
    envelopes = (
        _prefix()[0],
        _prepare_attest_start(1),
        _rest_response(2),
        prepare,
        tampered,
    )

    with pytest.raises(AuditMismatchReportError, match="PREPARE/ATTEST"):
        _build(envelopes, classification_sequence=4)


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
