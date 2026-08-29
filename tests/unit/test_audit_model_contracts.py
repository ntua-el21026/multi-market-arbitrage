from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import cast

import pytest

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import ConnectionId, IngestId, InstrumentId, MarketId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.pipeline.audit import (
    AuditBookIdentity,
    AuditClassificationCommand,
    AuditClassificationPrepareCommand,
    AuditDivergencePending,
    AuditMismatchCause,
    AuditOutcome,
    AuditOutcomeEvidence,
    build_audit_mismatch_diagnostic,
)
from multi_market_arbitrage.state.order_books.models import BookLevel


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _source(label: str) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=IngestId(f"ingest-{label}"),
        payload_sha256=_sha256(f"payload-{label}"),
    )


def _level(price: str, quantity: str) -> BookLevel:
    return BookLevel(price=Price.parse(price), quantity=Quantity.parse(quantity))


def _pending_divergence() -> AuditDivergencePending:
    market_id = MarketId("market-1")
    local_book = AuditBookIdentity(
        instrument_id=InstrumentId("instrument-1"),
        generation=3,
        economic_content_sha256=_sha256("local-book"),
    )
    audit_start_source = _source("audit-start")
    response_source = _source("response")
    diagnostic = build_audit_mismatch_diagnostic(
        cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
        local_book=local_book,
        local_bids=(_level("0.4", "10"),),
        local_asks=(_level("0.6", "10"),),
        local_raw_sources=(_source("local-book"),),
        rest_instrument_id=local_book.instrument_id,
        rest_market_id=market_id,
        rest_bids=(_level("0.4", "11"),),
        rest_asks=(_level("0.6", "10"),),
        rest_tick_size=ExactDecimal.parse("0.01"),
        rest_minimum_order_size=Quantity.parse("1"),
        rest_neg_risk=False,
        rest_raw_sources=(response_source,),
        response_source=response_source,
    )
    response_wall_time = UtcTimestamp(1_000_000_000)
    response_monotonic_ns = 200
    outcome = AuditOutcomeEvidence(
        request_id="audit-request-1",
        connection_id=ConnectionId("connection-1"),
        native_asset_id="native-1",
        attempt=1,
        request_started_monotonic_ns=100,
        starting_admission_ordinal=5,
        starting_processed_ordinal=5,
        starting_book=local_book,
        outcome=AuditOutcome.MISMATCH,
        reason=None,
        response_ingest_id=response_source.ingest_id,
        raw_response_sha256=response_source.payload_sha256,
        response_received_wall_time=response_wall_time,
        response_received_monotonic_ns=response_monotonic_ns,
        response_boundary_ordinal=6,
        barrier_processed_ordinal=6,
        barrier_book=local_book,
        requires_resync=True,
        mismatch_diagnostic=diagnostic,
        audit_start_source=audit_start_source,
        classification_source=response_source,
    )
    return AuditDivergencePending.create(
        market_id=market_id,
        audit_outcome=outcome,
        audit_start_control_source=audit_start_source,
        classification_source=response_source,
        classification_control_source=None,
        classification_received_wall_time=response_wall_time,
        classification_received_monotonic_ns=response_monotonic_ns,
    )


def _recreate_pending(
    pending: AuditDivergencePending,
    outcome: AuditOutcomeEvidence,
) -> AuditDivergencePending:
    return AuditDivergencePending.create(
        market_id=pending.market_id,
        audit_outcome=outcome,
        audit_start_control_source=pending.audit_start_control_source,
        classification_source=pending.classification_source,
        classification_control_source=pending.classification_control_source,
        classification_received_wall_time=pending.classification_received_wall_time,
        classification_received_monotonic_ns=pending.classification_received_monotonic_ns,
    )


@pytest.mark.parametrize(
    "field_name",
    ["response_ingest_seq", "classification_monotonic_ns", "hard_expiry_monotonic_ns"],
)
def test_audit_classification_command_rejects_boolean_integer_fields(field_name: str) -> None:
    command = AuditClassificationCommand(
        request_id="audit-request-1",
        response_ingest_seq=1,
        classification_monotonic_ns=2,
        hard_expiry_monotonic_ns=3,
    )

    with pytest.raises(ValueError, match="must be an integer"):
        replace(command, **{field_name: cast(int, True)})


@pytest.mark.parametrize("field_name", ["response_ingest_seq", "hard_expiry_monotonic_ns"])
def test_audit_classification_prepare_rejects_boolean_integer_fields(field_name: str) -> None:
    command = AuditClassificationPrepareCommand(
        request_id="audit-request-1",
        response_ingest_seq=1,
        hard_expiry_monotonic_ns=3,
    )

    with pytest.raises(ValueError, match="must be an integer"):
        replace(command, **{field_name: cast(int, True)})


def test_divergence_rejects_conflicting_nested_audit_start_source() -> None:
    pending = _pending_divergence()
    conflicting = replace(pending.audit_outcome, audit_start_source=_source("other-start"))

    with pytest.raises(ValueError, match="start source conflicts"):
        _recreate_pending(pending, conflicting)


def test_divergence_rejects_conflicting_nested_classification_source() -> None:
    pending = _pending_divergence()
    conflicting = replace(
        pending.audit_outcome,
        classification_source=_source("other-classification"),
    )

    with pytest.raises(ValueError, match="classification source conflicts"):
        _recreate_pending(pending, conflicting)


def test_divergence_preserves_historical_missing_nested_source_compatibility() -> None:
    enriched = _pending_divergence()
    historical_outcome = replace(
        enriched.audit_outcome,
        audit_start_source=None,
        classification_source=None,
    )

    historical = _recreate_pending(enriched, historical_outcome)

    assert historical.divergence_id == enriched.divergence_id
    assert historical.audit_outcome.audit_start_source is None
    assert historical.audit_outcome.classification_source is None
