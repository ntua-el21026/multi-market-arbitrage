from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace

import pytest

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import ConnectionId, IngestId, InstrumentId, MarketId
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE,
    AUDIT_MISMATCH_MAX_RAW_SOURCES,
    AUDIT_MISMATCH_MAX_REPRESENTATIVE_DIFFERENCES,
    AUDIT_MISMATCH_MAX_REPRESENTATIVE_RAW_SOURCES,
    AuditBookIdentity,
    AuditBookSide,
    AuditMismatchCause,
    AuditOutcome,
    AuditOutcomeEvidence,
    build_audit_mismatch_diagnostic,
    compare_audit_exact_depth,
    summarize_audit_raw_sources,
)
from multi_market_arbitrage.state.order_books.models import BookLevel


def _sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _level(price: str, quantity: str) -> BookLevel:
    return BookLevel(price=Price.parse(price), quantity=Quantity.parse(quantity))


def _source(label: str) -> RawSourceRef:
    return RawSourceRef(
        ingest_id=IngestId(f"ingest-{label}"),
        payload_sha256=_sha256(f"payload-{label}"),
    )


def _diagnostic(*, cause: AuditMismatchCause = AuditMismatchCause.UNKNOWN):
    local_book = AuditBookIdentity(
        instrument_id=InstrumentId("instrument-yes"),
        generation=7,
        economic_content_sha256=_sha256("local-economic-book"),
    )
    response_source = _source("rest-response")
    return build_audit_mismatch_diagnostic(
        cause=cause,
        local_book=local_book,
        local_bids=(
            _level("0.40", "2"),
            _level("0.30", "1"),
        ),
        local_asks=(
            _level("0.60", "4"),
            _level("0.70", "3"),
        ),
        local_raw_sources=(_source("local-book"), _source("local-delta")),
        rest_instrument_id=local_book.instrument_id,
        rest_market_id=MarketId("market-1"),
        rest_bids=(
            _level("0.20", "5"),
            _level("0.40", "9"),
        ),
        rest_asks=(
            _level("0.60", "4"),
            _level("0.80", "6"),
        ),
        rest_tick_size=ExactDecimal.parse("0.01"),
        rest_minimum_order_size=Quantity.parse("1"),
        rest_neg_risk=False,
        rest_raw_sources=(response_source,),
        response_source=response_source,
        context_evidence_sha256s=(_sha256("context-b"), _sha256("context-a")),
    )


def _mismatch_outcome():
    diagnostic = _diagnostic()
    return AuditOutcomeEvidence(
        request_id="audit-request-1",
        connection_id=ConnectionId("connection-1"),
        native_asset_id="native-yes",
        attempt=1,
        request_started_monotonic_ns=100,
        starting_admission_ordinal=5,
        starting_processed_ordinal=5,
        starting_book=diagnostic.local_book,
        outcome=AuditOutcome.MISMATCH,
        reason=None,
        response_ingest_id=diagnostic.response_source.ingest_id,
        raw_response_sha256=diagnostic.response_source.payload_sha256,
        response_received_wall_time=UtcTimestamp(1_000_000_000),
        response_received_monotonic_ns=200,
        response_boundary_ordinal=6,
        barrier_processed_ordinal=6,
        barrier_book=diagnostic.local_book,
        requires_resync=True,
        mismatch_diagnostic=diagnostic,
    )


def test_mismatch_cause_vocabulary_is_finite_and_metric_safe() -> None:
    assert {cause.value for cause in AuditMismatchCause} == {
        "REPORTED_TOP_TRANSITION",
        "TRUE_EXACT_DEPTH_DIVERGENCE",
        "FRESHNESS_RESPONSE_WINDOW_BOUNDARY",
        "REFERENCE_PRECISION_TRANSITION",
        "LOCAL_BOOK_DEFECT",
        "UNKNOWN",
    }


def test_exact_depth_comparison_is_deterministic_complete_and_bounded() -> None:
    local_bids = (_level("0.40", "2"), _level("0.30", "1"))
    local_asks = (_level("0.60", "4"), _level("0.70", "3"))
    rest_bids = (_level("0.20", "5"), _level("0.40", "9"))
    rest_asks = (_level("0.60", "4"), _level("0.80", "6"))

    comparison = compare_audit_exact_depth(
        local_bids=local_bids,
        local_asks=local_asks,
        rest_bids=rest_bids,
        rest_asks=rest_asks,
    )
    reordered = compare_audit_exact_depth(
        local_bids=tuple(reversed(local_bids)),
        local_asks=tuple(reversed(local_asks)),
        rest_bids=tuple(reversed(rest_bids)),
        rest_asks=tuple(reversed(rest_asks)),
    )

    assert comparison == reordered
    assert comparison.bid_difference_count == 3
    assert comparison.ask_difference_count == 2
    assert comparison.local_only_count == 2
    assert comparison.rest_only_count == 2
    assert comparison.quantity_difference_count == 1
    assert comparison.difference_count == 5
    assert comparison.representatives_truncated is False
    assert tuple(item.side for item in comparison.representatives) == (
        AuditBookSide.BID,
        AuditBookSide.BID,
        AuditBookSide.BID,
        AuditBookSide.ASK,
        AuditBookSide.ASK,
    )

    changed = compare_audit_exact_depth(
        local_bids=local_bids,
        local_asks=local_asks,
        rest_bids=(_level("0.20", "5"), _level("0.40", "10")),
        rest_asks=rest_asks,
    )
    assert changed.difference_sha256 != comparison.difference_sha256
    assert changed.rest_depth.content_sha256 != comparison.rest_depth.content_sha256


def test_depth_and_raw_representatives_are_strictly_bounded() -> None:
    local_bids = tuple(_level(str(index), "1") for index in range(20))
    comparison = compare_audit_exact_depth(
        local_bids=local_bids,
        local_asks=(),
        rest_bids=(),
        rest_asks=(),
    )
    assert comparison.difference_count == 20
    assert len(comparison.representatives) == AUDIT_MISMATCH_MAX_REPRESENTATIVE_DIFFERENCES
    assert comparison.representatives_truncated is True

    sources = tuple(_source(str(index)) for index in range(20))
    summary = summarize_audit_raw_sources(sources)
    assert summary.source_count == 20
    assert len(summary.representatives) == AUDIT_MISMATCH_MAX_REPRESENTATIVE_RAW_SOURCES
    assert summary.representatives_truncated is True
    assert summary == summarize_audit_raw_sources(iter(sources))


def test_diagnostic_builders_reject_unbounded_or_ambiguous_inputs() -> None:
    too_many_levels = tuple(
        _level(str(index), "1") for index in range(AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE + 1)
    )
    with pytest.raises(ValueError, match="exceeds 4096"):
        compare_audit_exact_depth(
            local_bids=too_many_levels,
            local_asks=(),
            rest_bids=(),
            rest_asks=(),
        )

    duplicate = _level("0.4", "1")
    with pytest.raises(ValueError, match="duplicate exact price"):
        compare_audit_exact_depth(
            local_bids=(duplicate, duplicate),
            local_asks=(),
            rest_bids=(),
            rest_asks=(),
        )

    with pytest.raises(ValueError, match="exceed 8193"):
        summarize_audit_raw_sources(
            _source(str(index)) for index in range(AUDIT_MISMATCH_MAX_RAW_SOURCES + 1)
        )
    with pytest.raises(ValueError, match="duplicate ingest"):
        summarize_audit_raw_sources((_source("same"), _source("same")))

    diagnostic = _diagnostic()
    with pytest.raises(ValueError, match="true exact-depth divergence"):
        build_audit_mismatch_diagnostic(
            cause=AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE,
            local_book=diagnostic.local_book,
            local_bids=(_level("0.4", "1"),),
            local_asks=(),
            local_raw_sources=(_source("local"),),
            rest_instrument_id=diagnostic.local_book.instrument_id,
            rest_market_id=MarketId("market-1"),
            rest_bids=(_level("0.4", "1"),),
            rest_asks=(),
            rest_tick_size=ExactDecimal.parse("0.01"),
            rest_minimum_order_size=Quantity.parse("1"),
            rest_neg_risk=False,
            rest_raw_sources=(_source("response"),),
            response_source=_source("response"),
        )


def test_mismatch_outcome_binds_immutable_diagnostic_and_preserves_it_in_history() -> None:
    mismatch = _mismatch_outcome()
    diagnostic = mismatch.mismatch_diagnostic
    assert diagnostic is not None
    assert diagnostic.local_book == mismatch.barrier_book
    assert diagnostic.exact_depth_difference.difference_count == 5
    assert diagnostic.context_evidence_sha256s == tuple(
        sorted((_sha256("context-a"), _sha256("context-b")))
    )
    assert b"UNKNOWN" in canonical_json_bytes(mismatch)

    with pytest.raises(FrozenInstanceError):
        mismatch.__setattr__("mismatch_diagnostic", None)

    successful_resnapshot = replace(
        mismatch,
        request_id="audit-request-2",
        outcome=AuditOutcome.MATCH,
        requires_resync=False,
        mismatch_diagnostic=None,
    )
    history = (mismatch, successful_resnapshot)
    assert history[0].mismatch_diagnostic is diagnostic
    assert history[1].mismatch_diagnostic is None


def test_outcome_rejects_unbound_or_non_mismatch_diagnostics_but_accepts_legacy_shape() -> None:
    mismatch = _mismatch_outcome()
    diagnostic = mismatch.mismatch_diagnostic
    assert diagnostic is not None

    with pytest.raises(ValueError, match="only a MISMATCH"):
        replace(
            mismatch,
            outcome=AuditOutcome.MATCH,
            requires_resync=False,
        )
    with pytest.raises(ValueError, match="another response ingest"):
        replace(mismatch, response_ingest_id=IngestId("other-response"))
    with pytest.raises(ValueError, match="barrier book identity"):
        replace(
            mismatch,
            barrier_book=replace(
                diagnostic.local_book,
                generation=diagnostic.local_book.generation + 1,
            ),
        )

    legacy = replace(mismatch, mismatch_diagnostic=None)
    assert legacy.outcome is AuditOutcome.MISMATCH
    assert legacy.mismatch_diagnostic is None
