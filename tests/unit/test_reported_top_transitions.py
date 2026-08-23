from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.exact import Price, Quantity
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    NormalizerVersion,
    SchemaVersion,
    SubscriptionId,
)
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.normalization import (
    BookSide,
    CanonicalEvent,
    CanonicalEventType,
    PriceChangeEvent,
    PriceLevelChange,
)
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_REPORTED_TOP_TRANSITION_EXPIRED,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditFailureReason,
    AuditOutcome,
    AuditStartEvidence,
    CanonicalProcessor,
    ReportedTopTransition,
    ReportedTopTransitionBaselineEvidence,
    ReportedTopTransitionEntrySource,
    ReportedTopTransitionExpiryCommand,
    ReportedTopTransitionOutcome,
    ReportedTopTransitionReason,
    ReportedTopTransitionStatus,
    WebSocketAdmissionWatermark,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.state.order_books import (
    BookStateError,
    CanonicalL2BookOwner,
    InvalidationReason,
)

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"
GATE1_FIXTURES = FIXTURES / "gate1"

CONDITION_ID = "0x39ea1503427df7f1439e7dd6567aad5ed72284ed49c58d09e5b917a24389fab7"
YES_ASSET_ID = "18258395755041392118198940898057846234774760894152262988568951221464458542850"
NO_ASSET_ID = "114727316628172873125190766157418606846347212023742363393254518987604640648589"


@dataclass(frozen=True, slots=True)
class _CatchUpCase:
    ingest_seq: int
    fixture: str
    payload_sha256: str
    payload_bytes: int
    connection_id: str
    target_received_monotonic_ns: int
    target_received_wall_time_ns: int
    snapshot_ingest_seq: int
    snapshot_payload_sha256: str
    snapshot_to_target_ns: int
    target_to_resnapshot_ns: int
    yes_baseline: tuple[str, str]
    no_baseline: tuple[str, str]
    yes_target: tuple[str, str]
    no_target: tuple[str, str]


_PRIMARY_PAYLOADS = (
    (
        20066,
        "primary-20066-price-change.json.b64",
        625,
        "67733552fefa017753557840caee222a86ae2f1a538cc86ce0437d00c7f383dc",
    ),
    (
        26146,
        "primary-26146-price-change.json.b64",
        617,
        "0e47341a1638f0f1058b9825cedc2f2e3eb43f18a35d2ada20816e3536c621ed",
    ),
    (
        57885,
        "primary-57885-price-change.json.b64",
        617,
        "f365d00574684efd35154f018e5a5cd34579747f8dea7e2e9498c2bf33523c3f",
    ),
    (
        84229,
        "primary-84229-price-change.json.b64",
        625,
        "91972e74475b9c9ce4f679d9e51203001948a6f05badbfe58594399557a447db",
    ),
)

_CATCH_UP_CASES = (
    _CatchUpCase(
        ingest_seq=26146,
        fixture="primary-26146-price-change.json.b64",
        payload_sha256="0e47341a1638f0f1058b9825cedc2f2e3eb43f18a35d2ada20816e3536c621ed",
        payload_bytes=617,
        connection_id="01a02bf4-cfe1-7f0c-8cea-890fc7a9bd41",
        target_received_monotonic_ns=5_007_965_082_008,
        target_received_wall_time_ns=1_787_443_863_715_496_858,
        snapshot_ingest_seq=26127,
        snapshot_payload_sha256=(
            "92f6cc94fde8c96df2d6a6da001e19a2840a63e744baeb1708c5567508640031"
        ),
        snapshot_to_target_ns=3_811_212_319,
        target_to_resnapshot_ns=936_433_594,
        yes_baseline=("0.016", "0.018"),
        no_baseline=("0.982", "0.984"),
        yes_target=("0.017", "0.018"),
        no_target=("0.982", "0.983"),
    ),
    _CatchUpCase(
        ingest_seq=57885,
        fixture="primary-57885-price-change.json.b64",
        payload_sha256="f365d00574684efd35154f018e5a5cd34579747f8dea7e2e9498c2bf33523c3f",
        payload_bytes=617,
        connection_id="01a02c50-be53-7cae-9252-4141a7c32e56",
        target_received_monotonic_ns=10_680_077_288_091,
        target_received_wall_time_ns=1_787_449_892_437_400_596,
        snapshot_ingest_seq=57858,
        snapshot_payload_sha256=(
            "25b7ba3a28ae026c8a900088633609901431ea91fb9f753360535b9fdcf49611"
        ),
        snapshot_to_target_ns=5_688_293_772,
        target_to_resnapshot_ns=1_089_442_261,
        yes_baseline=("0.016", "0.018"),
        no_baseline=("0.982", "0.984"),
        yes_target=("0.017", "0.018"),
        no_target=("0.982", "0.983"),
    ),
    _CatchUpCase(
        ingest_seq=84229,
        fixture="primary-84229-price-change.json.b64",
        payload_sha256="91972e74475b9c9ce4f679d9e51203001948a6f05badbfe58594399557a447db",
        payload_bytes=625,
        connection_id="01a02c9e-0774-7ae4-a378-4ec56fc0da58",
        target_received_monotonic_ns=15_422_441_176_267,
        target_received_wall_time_ns=1_787_454_952_098_969_571,
        snapshot_ingest_seq=84216,
        snapshot_payload_sha256=(
            "b68f7628ea3d5c4a1d582fa6a0b3e51bbbcfc96a7222c87d56a0e236368a8a7f"
        ),
        snapshot_to_target_ns=2_501_707_009,
        target_to_resnapshot_ns=1_030_001_677,
        yes_baseline=("0.016", "0.017"),
        no_baseline=("0.983", "0.984"),
        yes_target=("0.016", "0.018"),
        no_target=("0.982", "0.984"),
    ),
)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _gate1_payload(sequence: int) -> bytes:
    _, fixture, expected_bytes, expected_sha256 = next(
        item for item in _PRIMARY_PAYLOADS if item[0] == sequence
    )
    encoded = (GATE1_FIXTURES / fixture).read_text(encoding="ascii").strip()
    payload = base64.b64decode(encoded, validate=True)
    assert len(payload) == expected_bytes
    assert hashlib.sha256(payload).hexdigest() == expected_sha256
    return payload


def _gamma_payload() -> bytes:
    return (
        _fixture("polymarket_gamma_market_keyset.json")
        .replace(b"0xcondition", CONDITION_ID.encode())
        .replace(b"token-yes", YES_ASSET_ID.encode())
        .replace(b"token-no", NO_ASSET_ID.encode())
        .replace(b"0.0100", b"0.0010")
    )


def _clob_payload() -> bytes:
    return (
        _fixture("polymarket_clob_market_info.json")
        .replace(b"token-yes", YES_ASSET_ID.encode())
        .replace(b"token-no", NO_ASSET_ID.encode())
        .replace(b'"mts": 0.01', b'"mts": 0.001')
    )


def _book_event(
    asset_id: str,
    top: tuple[str, str],
    *,
    timestamp_ms: int,
    suffix: str,
) -> dict[str, object]:
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "market": CONDITION_ID,
        "bids": [{"price": top[0], "size": "10"}],
        "asks": [{"price": top[1], "size": "10"}],
        "timestamp": str(timestamp_ms),
        "hash": f"sanitized-{suffix}",
    }


def _two_book_snapshot(
    *,
    yes_top: tuple[str, str],
    no_top: tuple[str, str],
    timestamp_ms: int,
    suffix: str,
) -> bytes:
    return json.dumps(
        [
            _book_event(YES_ASSET_ID, yes_top, timestamp_ms=timestamp_ms, suffix=f"{suffix}-yes"),
            _book_event(NO_ASSET_ID, no_top, timestamp_ms=timestamp_ms, suffix=f"{suffix}-no"),
        ],
        separators=(",", ":"),
    ).encode()


def _best_bid_ask_payload(
    asset_id: str,
    top: tuple[str, str],
    *,
    timestamp_ms: int,
) -> bytes:
    spread = Price.parse(top[1]).value - Price.parse(top[0]).value
    return json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": asset_id,
            "market": CONDITION_ID,
            "best_bid": top[0],
            "best_ask": top[1],
            "spread": str(spread),
            "timestamp": str(timestamp_ms),
        },
        separators=(",", ":"),
    ).encode()


def _paired_best_bid_ask_payload(
    *,
    yes_top: tuple[str, str],
    no_top: tuple[str, str],
    timestamp_ms: int,
) -> bytes:
    return (
        b"["
        + b",".join(
            (
                _best_bid_ask_payload(YES_ASSET_ID, yes_top, timestamp_ms=timestamp_ms),
                _best_bid_ask_payload(NO_ASSET_ID, no_top, timestamp_ms=timestamp_ms),
            )
        )
        + b"]"
    )


def _single_price_change_payload(
    *,
    asset_id: str,
    side: str,
    price: str,
    size: str,
    reported_top: tuple[str, str],
    timestamp_ms: int,
) -> bytes:
    return json.dumps(
        {
            "event_type": "price_change",
            "market": CONDITION_ID,
            "price_changes": [
                {
                    "asset_id": asset_id,
                    "price": price,
                    "size": size,
                    "side": side,
                    "hash": f"synthetic-{timestamp_ms}",
                    "best_bid": reported_top[0],
                    "best_ask": reported_top[1],
                }
            ],
            "timestamp": str(timestamp_ms),
        },
        separators=(",", ":"),
    ).encode()


def _single_unreported_price_change_payload(
    *,
    asset_id: str,
    side: str,
    price: str,
    size: str,
    timestamp_ms: int,
) -> bytes:
    return json.dumps(
        {
            "event_type": "price_change",
            "market": CONDITION_ID,
            "price_changes": [
                {
                    "asset_id": asset_id,
                    "price": price,
                    "size": size,
                    "side": side,
                    "hash": f"synthetic-unreported-{timestamp_ms}",
                }
            ],
            "timestamp": str(timestamp_ms),
        },
        separators=(",", ":"),
    ).encode()


def _last_trade_payload(*, asset_id: str, price: str, timestamp_ms: int) -> bytes:
    return json.dumps(
        {
            "event_type": "last_trade_price",
            "asset_id": asset_id,
            "market": CONDITION_ID,
            "price": price,
            "size": "2.5",
            "side": "BUY",
            "fee_rate_bps": "700",
            "transaction_hash": "0xreportedtopdiagnostic",
            "timestamp": str(timestamp_ms),
        },
        separators=(",", ":"),
    ).encode()


def _tick_size_change_payload(
    *,
    asset_id: str,
    old_tick_size: str,
    new_tick_size: str,
    timestamp_ms: int,
) -> bytes:
    return json.dumps(
        {
            "event_type": "tick_size_change",
            "asset_id": asset_id,
            "market": CONDITION_ID,
            "old_tick_size": old_tick_size,
            "new_tick_size": new_tick_size,
            "timestamp": str(timestamp_ms),
        },
        separators=(",", ":"),
    ).encode()


def _same_target_continuation(payload: bytes, *, size: str, ordinal: int) -> bytes:
    value = json.loads(payload)
    value["timestamp"] = str(int(value["timestamp"]) + ordinal)
    for index, change in enumerate(value["price_changes"]):
        change["size"] = size
        change["hash"] = f"same-target-{ordinal}-{index}"
    return json.dumps(value, separators=(",", ":")).encode()


def _rest_book_payload(
    *,
    asset_id: str,
    top: tuple[str, str],
    timestamp_ms: int,
) -> bytes:
    return json.dumps(
        {
            "market": CONDITION_ID,
            "asset_id": asset_id,
            "timestamp": str(timestamp_ms),
            "hash": f"sanitized-rest-{timestamp_ms}",
            "bids": [{"price": top[0], "size": "10"}],
            "asks": [{"price": top[1], "size": "10"}],
            "min_order_size": "5",
            "tick_size": "0.001",
            "neg_risk": False,
            "last_trade_price": top[0],
        },
        separators=(",", ":"),
    ).encode()


def _envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    connection_id: ConnectionId,
    monotonic_ns: int,
    wall_time_ns: int,
    ordinal: int | None = None,
    transport_metadata: dict[str, str | int | bool] | None = None,
) -> RawEventEnvelope:
    metadata = {} if transport_metadata is None else transport_metadata.copy()
    if ordinal is not None:
        metadata.update({"websocket_admission_ordinal": ordinal, "websocket_message_type": "TEXT"})
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=connection_id,
        subscription_id=SubscriptionId("gate1-transition-fixture-subscription"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(wall_time_ns),
        received_monotonic=MonotonicTimestamp(run_id.value, monotonic_ns),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=metadata or None,
        ingest_id=IngestId(f"fixture-{sequence}-{hashlib.sha256(payload).hexdigest()[:16]}"),
    )


def _config() -> Phase1Config:
    return load_phase1_config(Path("config/phase1.toml"))


def _bootstrap_reference(
    processor: CanonicalProcessor,
    run_id: CollectorRunId,
    *,
    before_wall_time_ns: int,
) -> None:
    reference_connection = ConnectionId(f"reference-{run_id.value}")
    inputs = (
        _envelope(
            run_id,
            0,
            GAMMA_MARKETS_KEYSET,
            _gamma_payload(),
            connection_id=reference_connection,
            monotonic_ns=1,
            wall_time_ns=before_wall_time_ns - 2,
        ),
        _envelope(
            run_id,
            1,
            f"{CLOB_MARKET_PREFIX}{CONDITION_ID}",
            _clob_payload(),
            connection_id=reference_connection,
            monotonic_ns=2,
            wall_time_ns=before_wall_time_ns - 1,
        ),
    )
    assert all(processor.process_replayed(envelope).success for envelope in inputs)


def _refresh_seq57885_freshness_with_matching_audits(
    processor: CanonicalProcessor,
    run_id: CollectorRunId,
    case: _CatchUpCase,
    *,
    connection_id: ConnectionId,
) -> None:
    """Reproduce the two MATCH bodies immediately preceding source sequence 57885."""

    watermark = processor.processed_websocket_watermark(connection_id)
    assert watermark.ordinal == 1
    target_timestamp_ms = int(json.loads(_gate1_payload(case.ingest_seq))["timestamp"])
    refreshes = (
        # Authoritative response receive times were 10678970511956 (NO) and
        # 10678979985488 (YES), respectively 1.106776135s and 1.097302603s
        # before the ordinal-2 target.
        (NO_ASSET_ID, case.no_baseline, 1_106_776_135),
        (YES_ASSET_ID, case.yes_baseline, 1_097_302_603),
    )
    for index, (asset_id, top, response_offset_ns) in enumerate(refreshes):
        response_monotonic_ns = case.target_received_monotonic_ns - response_offset_ns
        response_wall_time_ns = case.target_received_wall_time_ns - response_offset_ns
        request_id = f"source-57885-match-{index}"
        start = AuditStartEvidence(
            request_id=request_id,
            connection_id=connection_id,
            native_asset_id=asset_id,
            attempt=1,
            request_started_monotonic_ns=response_monotonic_ns - 100,
            starting_admission_watermark=watermark,
            starting_processed_watermark=watermark,
            starting_book=processor.audit_book_identity(asset_id),
        )
        intent = _envelope(
            run_id,
            case.ingest_seq - 8 + index * 2,
            CONTROL_REST_AUDIT_START,
            start.to_payload(),
            connection_id=connection_id,
            monotonic_ns=start.request_started_monotonic_ns,
            wall_time_ns=response_wall_time_ns - 100,
        )
        assert processor.process_replayed(intent).success
        response = _envelope(
            run_id,
            case.ingest_seq - 7 + index * 2,
            REST_BOOK_AUDIT,
            _rest_book_payload(
                asset_id=asset_id,
                top=top,
                timestamp_ms=target_timestamp_ms - 1_000,
            ),
            connection_id=connection_id,
            monotonic_ns=response_monotonic_ns,
            wall_time_ns=response_wall_time_ns,
            transport_metadata={
                "request_id": request_id,
                "audit_attempt": 1,
                "response_boundary_websocket_ordinal": 1,
                "barrier_websocket_ordinal": 1,
            },
        )
        result = processor.process_replayed(response)
        assert result.success
        assert len(result.audit_outcomes) == 1
        assert result.audit_outcomes[0].outcome is AuditOutcome.MATCH


def _bootstrap_catch_up_case(
    case: _CatchUpCase,
    *,
    config: Phase1Config | None = None,
) -> tuple[CanonicalProcessor, CollectorRunId, RawEventEnvelope]:
    run_id = CollectorRunId(f"gate1-catch-up-{case.ingest_seq}")
    effective_config = _config() if config is None else config
    processor = CanonicalProcessor(effective_config)
    snapshot_monotonic_ns = case.target_received_monotonic_ns - case.snapshot_to_target_ns
    snapshot_wall_time_ns = case.target_received_wall_time_ns - case.snapshot_to_target_ns
    _bootstrap_reference(processor, run_id, before_wall_time_ns=snapshot_wall_time_ns)
    connection_id = ConnectionId(case.connection_id)
    target_payload = _gate1_payload(case.ingest_seq)
    target_timestamp_ms = int(json.loads(target_payload)["timestamp"])
    snapshot = _envelope(
        run_id,
        case.snapshot_ingest_seq,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=case.yes_baseline,
            no_top=case.no_baseline,
            timestamp_ms=target_timestamp_ms - 10_000,
            suffix=f"baseline-{case.ingest_seq}",
        ),
        connection_id=connection_id,
        monotonic_ns=snapshot_monotonic_ns,
        wall_time_ns=snapshot_wall_time_ns,
        ordinal=1,
    )
    assert processor.process_replayed(snapshot).success
    maximum_book_age_ns = int(effective_config.scanner.maximum_book_age_seconds * 1_000_000_000)
    if case.snapshot_to_target_ns > maximum_book_age_ns:
        assert case.ingest_seq == 57885
        _refresh_seq57885_freshness_with_matching_audits(
            processor,
            run_id,
            case,
            connection_id=connection_id,
        )
    target = _envelope(
        run_id,
        case.ingest_seq,
        WS_MARKET,
        target_payload,
        connection_id=connection_id,
        monotonic_ns=case.target_received_monotonic_ns,
        wall_time_ns=case.target_received_wall_time_ns,
        ordinal=2,
    )
    return processor, run_id, target


def _bootstrap_snapshot(
    *,
    run_name: str,
    yes_top: tuple[str, str],
    no_top: tuple[str, str],
    config: Phase1Config | None = None,
) -> tuple[CanonicalProcessor, CollectorRunId, ConnectionId]:
    run_id = CollectorRunId(run_name)
    processor = CanonicalProcessor(_config() if config is None else config)
    wall_time_ns = 1_787_500_000_000_000_000
    _bootstrap_reference(processor, run_id, before_wall_time_ns=wall_time_ns)
    connection_id = ConnectionId(f"connection-{run_name}")
    snapshot = _envelope(
        run_id,
        2,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=yes_top,
            no_top=no_top,
            timestamp_ms=1_787_499_999_000,
            suffix=run_name,
        ),
        connection_id=connection_id,
        monotonic_ns=100,
        wall_time_ns=wall_time_ns,
        ordinal=1,
    )
    assert processor.process_replayed(snapshot).success
    return processor, run_id, connection_id


def _instrument_id(processor: CanonicalProcessor, native_asset_id: str) -> InstrumentId:
    return processor.current_bundles[0].instrument_for_token(native_asset_id).instrument_id


def _owner(processor: CanonicalProcessor, native_asset_id: str) -> CanonicalL2BookOwner:
    instrument_id = _instrument_id(processor, native_asset_id)
    return next(owner for owner in processor.owners if owner.instrument_id == instrument_id)


def _bootstrap_yes_bbo_transition(
    run_name: str,
) -> tuple[
    CanonicalProcessor,
    CollectorRunId,
    ConnectionId,
    RawEventEnvelope,
    ReportedTopTransition,
]:
    processor, run_id, connection_id = _bootstrap_snapshot(
        run_name=run_name,
        yes_top=("0.014", "0.017"),
        no_top=("0.983", "0.986"),
    )
    bbo = _envelope(
        run_id,
        3,
        WS_MARKET,
        _best_bid_ask_payload(
            YES_ASSET_ID,
            ("0.014", "0.016"),
            timestamp_ms=1_787_499_999_100,
        ),
        connection_id=connection_id,
        monotonic_ns=200,
        wall_time_ns=1_787_500_000_000_000_100,
        ordinal=2,
    )
    result = processor.process_replayed(bbo)
    assert result.success
    transition = processor.reported_top_transition_for(_instrument_id(processor, YES_ASSET_ID))
    assert transition is not None
    return processor, run_id, connection_id, bbo, transition


def _assert_pending_transition_is_fail_closed(
    processor: CanonicalProcessor,
    native_asset_id: str,
) -> ReportedTopTransition:
    instrument_id = _instrument_id(processor, native_asset_id)
    transition = processor.reported_top_transition_for(instrument_id)
    assert transition is not None
    assert instrument_id in processor.audit_ineligible_instruments
    summary = processor.summary()
    assert summary.pending_reported_top_transitions >= 1
    assert summary.audit_ineligible_books >= 1
    assert summary.validated_opportunities == 0
    return transition


def _model_event(*, suffix: str, received_at_ns: int) -> CanonicalEvent:
    instrument_id = InstrumentId("transition-model-instrument")
    market_id = MarketId("transition-model-market")
    raw = f"raw-{suffix}".encode()
    return CanonicalEvent(
        canonical_event_id=CanonicalEventId(f"transition-model-event-{suffix}"),
        schema_version=SchemaVersion("transition-model-schema"),
        normalizer_version=NormalizerVersion("transition-model-normalizer"),
        event_type=CanonicalEventType.PRICE_CHANGE,
        venue_id=POLYMARKET_VENUE_ID,
        market_id=market_id,
        instrument_id=instrument_id,
        source_ingest_ids=(IngestId(f"transition-model-ingest-{suffix}"),),
        source_payload_hashes=(hashlib.sha256(raw).hexdigest(),),
        reference_version_ids=(),
        venue_timestamp=UtcTimestamp(received_at_ns - 1),
        receive_timestamp=UtcTimestamp(received_at_ns),
        deduplication_key=None,
        payload=PriceChangeEvent(
            instrument_id=instrument_id,
            market_id=market_id,
            changes=(
                PriceLevelChange(
                    side=BookSide.BID,
                    price=Price.parse("0.4"),
                    new_quantity=Quantity.parse("1"),
                    venue_hash=f"venue-{suffix}",
                    reported_best_bid=Price.parse("0.4"),
                    reported_best_ask=Price.parse("0.6"),
                ),
            ),
        ),
    )


def _start_model_transition(event: CanonicalEvent) -> ReportedTopTransition:
    baseline = ReportedTopTransitionBaselineEvidence(
        connection_id=ConnectionId("transition-model-connection"),
        monotonic_epoch_id="transition-model-epoch",
        admission_ordinal=1,
        ingest_seq=9,
        received_at=UtcTimestamp(999),
        received_monotonic_ns=90,
        source_raw_refs=event.raw_source_refs,
        source_canonical_event_ids=(CanonicalEventId("transition-model-baseline"),),
        instrument_states=(
            (
                InstrumentId("transition-model-instrument"),
                7,
                "b" * 64,
                Price.parse("0.39"),
                Price.parse("0.61"),
            ),
            (
                InstrumentId("transition-model-paired-instrument"),
                4,
                "c" * 64,
                Price.parse("0.39"),
                Price.parse("0.61"),
            ),
        ),
    )
    return ReportedTopTransition.start(
        instrument_id=InstrumentId("transition-model-instrument"),
        market_id=MarketId("transition-model-market"),
        target_best_bid=Price.parse("0.4"),
        target_best_ask=Price.parse("0.6"),
        starting_best_bid=Price.parse("0.39"),
        starting_best_ask=Price.parse("0.61"),
        starting_generation=7,
        starting_economic_sha256="a" * 64,
        event=event,
        connection_id=ConnectionId("transition-model-connection"),
        admission_ordinal=2,
        ingest_seq=10,
        received_monotonic=MonotonicTimestamp("transition-model-epoch", 100),
        maximum_frames=8,
        maximum_duration_ns=2_000_000_000,
        entry_source=ReportedTopTransitionEntrySource.POST_SNAPSHOT_CATCH_UP,
        reason=ReportedTopTransitionReason.POST_SNAPSHOT_PAIRED_CATCH_UP,
        baseline_evidence=baseline,
    )


def test_transition_model_has_deterministic_identity_validation_and_health_record() -> None:
    event = _model_event(suffix="first", received_at_ns=1_000)
    first = _start_model_transition(event)
    second = _start_model_transition(event)

    assert first == second
    assert len(first.transition_id) == 64
    assert first.deadline_monotonic_ns == 2_000_000_100
    assert first.health_record(now=MonotonicTimestamp("transition-model-epoch", 125)) == {
        "accepted_frame_count": 1,
        "age_nanoseconds": 25,
        "baseline_evidence_sha256": canonical_sha256(first.baseline_evidence),
        "deadline_monotonic_ns": 2_000_000_100,
        "entry_source": "POST_SNAPSHOT_CATCH_UP",
        "first_admission_ordinal": 2,
        "first_ingest_seq": 10,
        "frames_remaining": 7,
        "latest_admission_ordinal": 2,
        "latest_ingest_seq": 10,
        "maximum_duration_ns": 2_000_000_000,
        "maximum_frames": 8,
        "monotonic_epoch_id": "transition-model-epoch",
        "reason": "POST_SNAPSHOT_PAIRED_CATCH_UP",
        "status": "PENDING",
        "target_best_ask": "0.6",
        "target_best_bid": "0.4",
        "transition_id": first.transition_id,
    }
    assert (
        first.health_record(now=MonotonicTimestamp("another-epoch", 125))["age_nanoseconds"] is None
    )

    advanced = first.advance(
        event=_model_event(suffix="second", received_at_ns=1_001),
        admission_ordinal=3,
        ingest_seq=11,
        received_monotonic=MonotonicTimestamp("transition-model-epoch", 150),
        reason=ReportedTopTransitionReason.SAME_TARGET_EXACT_FRAME,
    )
    outcome = ReportedTopTransitionOutcome.close(
        advanced,
        status=ReportedTopTransitionStatus.CONVERGED,
        reason=ReportedTopTransitionReason.EXACT_L2_CONVERGED,
        ended_monotonic_ns=175,
        research_eligibility_restored=True,
    )
    assert advanced.accepted_frame_count == 2
    assert advanced.latest_ingest_seq == 11
    assert outcome.duration_ns == 75
    assert outcome.transition == advanced
    assert outcome.close_source is None
    assert outcome.first_source_raw_refs == first.first_source_raw_refs
    assert outcome.latest_source_raw_refs == advanced.latest_source_raw_refs

    with pytest.raises(ValueError, match="non-crossed complete top"):
        replace(first, target_best_bid=Price.parse("0.6"))
    with pytest.raises(ValueError, match="admission ordinal regressed"):
        first.advance(
            event=_model_event(suffix="regressed", received_at_ns=1_002),
            admission_ordinal=1,
            ingest_seq=12,
            received_monotonic=MonotonicTimestamp("transition-model-epoch", 151),
            reason=ReportedTopTransitionReason.SAME_TARGET_EXACT_FRAME,
        )


@pytest.mark.parametrize(
    ("sequence", "fixture", "expected_bytes", "expected_sha256"),
    _PRIMARY_PAYLOADS,
)
def test_primary_gate1_payload_fixtures_are_byte_exact(
    sequence: int,
    fixture: str,
    expected_bytes: int,
    expected_sha256: str,
) -> None:
    encoded = (GATE1_FIXTURES / fixture).read_text(encoding="ascii").strip()
    payload = base64.b64decode(encoded, validate=True)

    assert len(payload) == expected_bytes
    assert hashlib.sha256(payload).hexdigest() == expected_sha256
    assert json.loads(payload)["event_type"] == "price_change"
    metadata = json.loads((GATE1_FIXTURES / "corpus-metadata.json").read_bytes())
    bound = next(item for item in metadata["cases"] if item["ingest_seq"] == sequence)
    assert bound["fixture"] == fixture
    assert bound["decoded_sha256"] == expected_sha256


def test_corpus_metadata_binds_all_no_pending_cases_and_hard_bounds() -> None:
    metadata = json.loads((GATE1_FIXTURES / "corpus-metadata.json").read_bytes())
    bound_cases = {item["ingest_seq"]: item for item in metadata["cases"]}

    assert metadata["evidence_backed_bounds"] == {
        "interpretation": (
            "Regression bounds for a typed post-snapshot catch-up state, "
            "not a venue-delivery guarantee."
        ),
        "max_accepted_transition_frames": 8,
        "max_elapsed_from_first_uncertain_delta_ns": 2_000_000_000,
        "expire_when_elapsed_ns_greater_than_or_equal_to_bound": True,
        "same_connection_and_socket_generation_required": True,
        "max_observed_transition_frames_including_target": 6,
        "max_observed_target_to_authoritative_resnapshot_ns_across_primary_cases": (1_933_511_686),
        "all_primary_cases_covered": True,
    }
    for case in _CATCH_UP_CASES:
        bound = bound_cases[case.ingest_seq]
        snapshot = bound["complete_snapshot_before"]
        resnapshot = bound["authoritative_resnapshot"]
        assert bound["fixture"] == case.fixture
        assert bound["decoded_bytes"] == case.payload_bytes
        assert bound["decoded_sha256"] == case.payload_sha256
        assert bound["connection_id"] == case.connection_id
        assert bound["websocket_admission_ordinal"] == 2
        assert bound["pending_bbo_present"] is False
        assert bound["received_monotonic_ns"] == case.target_received_monotonic_ns
        assert bound["received_wall_time_utc_ns"] == case.target_received_wall_time_ns
        assert snapshot["ingest_seq"] == case.snapshot_ingest_seq
        assert snapshot["payload_sha256"] == case.snapshot_payload_sha256
        assert snapshot["websocket_admission_ordinal"] == 1
        assert snapshot["same_connection"] is True
        assert snapshot["snapshot_to_target_ns"] == case.snapshot_to_target_ns
        assert resnapshot["target_to_resnapshot_ns"] == case.target_to_resnapshot_ns
        assert case.target_to_resnapshot_ns < 2_000_000_000


@pytest.mark.parametrize("case", _CATCH_UP_CASES, ids=lambda case: str(case.ingest_seq))
def test_exact_post_snapshot_corpus_payload_enters_bounded_typed_uncertainty(
    case: _CatchUpCase,
) -> None:
    processor, _, target = _bootstrap_catch_up_case(case)

    result = processor.process_replayed(target)

    assert result.success
    assert not result.needs_resync
    summary = processor.summary()
    assert summary.reported_top_no_pending_recognized == 2
    assert summary.reported_top_no_pending_rejected == 0
    assert summary.reported_top_transitions_started == 2
    assert summary.pending_reported_top_transitions == 2
    assert summary.reported_top_transition_frames == 2
    assert summary.healthy_books == 2
    assert set(processor.audit_ineligible_instruments) == {
        _instrument_id(processor, YES_ASSET_ID),
        _instrument_id(processor, NO_ASSET_ID),
    }
    assert {transition.entry_source for transition in processor.reported_top_transitions} == {
        ReportedTopTransitionEntrySource.POST_SNAPSHOT_CATCH_UP
    }
    admission_ordinals = {
        transition.first_admission_ordinal for transition in processor.reported_top_transitions
    }
    assert admission_ordinals == {2}
    assert {transition.first_ingest_seq for transition in processor.reported_top_transitions} == {
        case.ingest_seq
    }
    assert all(transition.maximum_frames == 8 for transition in processor.reported_top_transitions)
    assert all(
        transition.maximum_duration_ns == 2_000_000_000
        for transition in processor.reported_top_transitions
    )

    yes_snapshot = _owner(processor, YES_ASSET_ID).snapshot()
    no_snapshot = _owner(processor, NO_ASSET_ID).snapshot()
    assert yes_snapshot.health is BookHealth.HEALTHY
    assert no_snapshot.health is BookHealth.HEALTHY
    assert yes_snapshot.best_bid is not None
    assert yes_snapshot.best_ask is not None
    assert no_snapshot.best_bid is not None
    assert no_snapshot.best_ask is not None
    assert (str(yes_snapshot.best_bid.price), str(yes_snapshot.best_ask.price)) == case.yes_baseline
    assert (str(no_snapshot.best_bid.price), str(no_snapshot.best_ask.price)) == case.no_baseline


@pytest.mark.parametrize("case", _CATCH_UP_CASES, ids=lambda case: str(case.ingest_seq))
def test_authoritative_resnapshot_clears_exact_post_snapshot_transition(
    case: _CatchUpCase,
) -> None:
    processor, run_id, target = _bootstrap_catch_up_case(case)
    assert processor.process_replayed(target).success
    target_timestamp_ms = int(json.loads(target.payload_bytes)["timestamp"])
    resnapshot = _envelope(
        run_id,
        case.ingest_seq + 100,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=case.yes_target,
            no_top=case.no_target,
            timestamp_ms=target_timestamp_ms + 1_000,
            suffix=f"resnapshot-{case.ingest_seq}",
        ),
        connection_id=ConnectionId(f"resnapshot-{case.connection_id}"),
        monotonic_ns=case.target_received_monotonic_ns + case.target_to_resnapshot_ns,
        wall_time_ns=case.target_received_wall_time_ns + case.target_to_resnapshot_ns,
        ordinal=1,
    )

    result = processor.process_replayed(resnapshot)

    assert result.success
    assert not result.needs_resync
    summary = processor.summary()
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_resnapshotted == 2
    assert summary.healthy_books == 2
    assert processor.audit_ineligible_instruments == ()
    assert {outcome.status for outcome in summary.reported_top_transition_outcomes} == {
        ReportedTopTransitionStatus.AUTHORITATIVE_SNAPSHOT
    }
    assert {outcome.reason for outcome in summary.reported_top_transition_outcomes} == {
        ReportedTopTransitionReason.AUTHORITATIVE_FULL_SNAPSHOT
    }


def test_best_bid_ask_starts_without_mutating_l2_and_exact_change_converges() -> None:
    processor, run_id, connection_id = _bootstrap_snapshot(
        run_name="reported-top-bbo-convergence",
        yes_top=("0.014", "0.017"),
        no_top=("0.983", "0.986"),
    )
    reported_top = ("0.014", "0.016")
    bbo = _envelope(
        run_id,
        3,
        WS_MARKET,
        _best_bid_ask_payload(
            YES_ASSET_ID,
            reported_top,
            timestamp_ms=1_787_499_999_100,
        ),
        connection_id=connection_id,
        monotonic_ns=200,
        wall_time_ns=1_787_500_000_000_000_100,
        ordinal=2,
    )

    bbo_result = processor.process_replayed(bbo)

    assert bbo_result.success
    yes_before_exact = _owner(processor, YES_ASSET_ID).snapshot()
    assert yes_before_exact.best_bid is not None
    assert yes_before_exact.best_ask is not None
    assert (
        str(yes_before_exact.best_bid.price),
        str(yes_before_exact.best_ask.price),
    ) == ("0.014", "0.017")
    transition = processor.reported_top_transition_for(_instrument_id(processor, YES_ASSET_ID))
    assert transition is not None
    assert transition.entry_source is ReportedTopTransitionEntrySource.BEST_BID_ASK
    assert (str(transition.target_best_bid), str(transition.target_best_ask)) == reported_top
    assert processor.audit_ineligible_instruments == (_instrument_id(processor, YES_ASSET_ID),)

    exact = _envelope(
        run_id,
        4,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.016",
            size="5",
            reported_top=reported_top,
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )
    exact_result = processor.process_replayed(exact)

    assert exact_result.success
    assert not exact_result.needs_resync
    assert processor.reported_top_transitions == ()
    assert processor.audit_ineligible_instruments == ()
    summary = processor.summary()
    assert summary.reported_top_transitions_started == 1
    assert summary.reported_top_transitions_converged == 1
    assert summary.reported_top_transition_frames == 2
    assert summary.reported_top_transitions_invalidated == 0
    assert summary.reported_top_transition_outcomes[-1].status is (
        ReportedTopTransitionStatus.CONVERGED
    )
    assert summary.reported_top_transition_outcomes[-1].reason is (
        ReportedTopTransitionReason.EXACT_L2_CONVERGED
    )
    yes_after_exact = _owner(processor, YES_ASSET_ID).snapshot()
    assert yes_after_exact.best_bid is not None
    assert yes_after_exact.best_ask is not None
    assert (
        str(yes_after_exact.best_bid.price),
        str(yes_after_exact.best_ask.price),
    ) == reported_top
    assert yes_after_exact.last_source_provenance is not None
    assert yes_after_exact.last_source_provenance.source_payload_hashes == (exact.payload_sha256,)


def test_exact_20066_payload_crosses_pending_yes_transition_and_invalidates_typed() -> None:
    processor, run_id, connection_id = _bootstrap_snapshot(
        run_name="reported-top-primary-20066",
        yes_top=("0.014", "0.016"),
        no_top=("0.983", "0.985"),
    )
    payload = _gate1_payload(20066)
    timestamp_ms = int(json.loads(payload)["timestamp"])
    bbo = _envelope(
        run_id,
        20_065,
        WS_MARKET,
        _paired_best_bid_ask_payload(
            yes_top=("0.016", "0.017"),
            no_top=("0.983", "0.984"),
            timestamp_ms=timestamp_ms - 1,
        ),
        connection_id=connection_id,
        monotonic_ns=200,
        wall_time_ns=1_787_500_000_000_000_100,
        ordinal=2,
    )
    assert processor.process_replayed(bbo).success
    assert len(processor.reported_top_transitions) == 2
    exact = _envelope(
        run_id,
        20_066,
        WS_MARKET,
        payload,
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )

    result = processor.process_replayed(exact)

    assert result.success
    assert result.needs_resync
    yes_id = _instrument_id(processor, YES_ASSET_ID)
    no_id = _instrument_id(processor, NO_ASSET_ID)
    assert result.affected_instrument_ids == (yes_id,)
    yes_snapshot = _owner(processor, YES_ASSET_ID).snapshot()
    no_snapshot = _owner(processor, NO_ASSET_ID).snapshot()
    assert yes_snapshot.health is BookHealth.INVALID
    assert yes_snapshot.invalidation_reason is InvalidationReason.IMPOSSIBLE_STATE
    assert no_snapshot.health is BookHealth.HEALTHY
    assert no_snapshot.best_bid is not None
    assert no_snapshot.best_ask is not None
    assert (str(no_snapshot.best_bid.price), str(no_snapshot.best_ask.price)) == (
        "0.983",
        "0.984",
    )
    transition_outcomes = processor.summary().reported_top_transition_outcomes
    outcomes = {outcome.instrument_id: outcome for outcome in transition_outcomes}
    assert outcomes[yes_id].status is ReportedTopTransitionStatus.INVALIDATED
    assert outcomes[yes_id].reason is ReportedTopTransitionReason.CROSSED_EXACT_BOOK
    assert outcomes[yes_id].research_eligibility_restored is False
    assert outcomes[no_id].status is ReportedTopTransitionStatus.CONVERGED
    assert outcomes[no_id].reason is ReportedTopTransitionReason.EXACT_L2_CONVERGED
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.reported_top_transitions_invalidated == 1
    assert summary.reported_top_transitions_converged == 1
    assert summary.pending_reported_top_transitions == 0


def test_post_snapshot_recognizer_rejects_wrong_connection_ordinal_pattern() -> None:
    processor, run_id, _ = _bootstrap_snapshot(
        run_name="reported-top-wrong-ordinal",
        yes_top=("0.016", "0.017"),
        no_top=("0.983", "0.984"),
    )
    payload = _gate1_payload(84229)
    target = _envelope(
        run_id,
        84_229,
        WS_MARKET,
        payload,
        connection_id=ConnectionId("wrong-ordinal-new-connection"),
        monotonic_ns=200,
        wall_time_ns=1_787_500_000_000_000_100,
        ordinal=1,
    )

    result = processor.process_replayed(target)

    assert result.success
    assert result.needs_resync
    summary = processor.summary()
    assert summary.reported_top_no_pending_recognized == 0
    assert summary.reported_top_no_pending_rejected == 2
    assert summary.reported_top_transitions_started == 0
    assert summary.healthy_books == 0
    assert len(result.affected_instrument_ids) == 2


def test_post_snapshot_recognizer_rejects_unequal_paired_quantities() -> None:
    processor, run_id, connection_id = _bootstrap_snapshot(
        run_name="reported-top-unpaired-quantity",
        yes_top=("0.016", "0.017"),
        no_top=("0.983", "0.984"),
    )
    value = json.loads(_gate1_payload(84229))
    value["price_changes"][1]["size"] = "2524.09"
    unpaired = json.dumps(value, separators=(",", ":")).encode()
    target = _envelope(
        run_id,
        84_229,
        WS_MARKET,
        unpaired,
        connection_id=connection_id,
        monotonic_ns=200,
        wall_time_ns=1_787_500_000_000_000_100,
        ordinal=2,
    )

    result = processor.process_replayed(target)

    assert result.success
    assert result.needs_resync
    summary = processor.summary()
    assert summary.reported_top_no_pending_recognized == 0
    assert summary.reported_top_no_pending_rejected == 2
    assert summary.reported_top_transitions_started == 0
    assert summary.healthy_books == 0


def test_84229_accepts_all_source_observed_same_target_frames_within_eight_frame_bound() -> None:
    case = _CATCH_UP_CASES[2]
    processor, run_id, target = _bootstrap_catch_up_case(case)
    assert processor.process_replayed(target).success
    continuations = (
        (84_230, 3, "2593.08", 15_422_442_830_173),
        (84_231, 4, "2697.08", 15_422_442_908_030),
        (84_232, 5, "2803.08", 15_422_442_943_951),
        (84_233, 6, "2912.08", 15_422_442_972_820),
        (84_234, 7, "3016.08", 15_422_446_845_950),
    )
    for sequence, ordinal, size, monotonic_ns in continuations:
        continuation = _envelope(
            run_id,
            sequence,
            WS_MARKET,
            _same_target_continuation(target.payload_bytes, size=size, ordinal=ordinal),
            connection_id=target.connection_id,
            monotonic_ns=monotonic_ns,
            wall_time_ns=(
                case.target_received_wall_time_ns + monotonic_ns - case.target_received_monotonic_ns
            ),
            ordinal=ordinal,
        )
        result = processor.process_replayed(continuation)
        assert result.success
        assert not result.needs_resync

    pending = processor.reported_top_transitions
    assert len(pending) == 2
    assert {transition.accepted_frame_count for transition in pending} == {6}
    assert {transition.maximum_frames for transition in pending} == {8}
    assert processor.summary().reported_top_transition_frames == 12
    assert processor.summary().reported_top_transitions_expired == 0

    resnapshot = _envelope(
        run_id,
        84_241,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=case.yes_target,
            no_top=case.no_target,
            timestamp_ms=int(json.loads(target.payload_bytes)["timestamp"]) + 1_000,
            suffix="84229-after-six-frames",
        ),
        connection_id=ConnectionId("84229-authoritative-resnapshot"),
        monotonic_ns=case.target_received_monotonic_ns + case.target_to_resnapshot_ns,
        wall_time_ns=case.target_received_wall_time_ns + case.target_to_resnapshot_ns,
        ordinal=1,
    )
    result = processor.process_replayed(resnapshot)

    assert result.success
    assert not result.needs_resync
    summary = processor.summary()
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_resnapshotted == 2
    accepted_frame_counts = {
        outcome.accepted_frame_count for outcome in summary.reported_top_transition_outcomes
    }
    assert accepted_frame_counts == {6}


def test_same_target_transition_expires_at_frame_bound() -> None:
    base_config = _config()
    bounded_polymarket = base_config.polymarket.model_copy(
        update={"reported_top_transition_max_frames": 3}
    )
    config = base_config.model_copy(update={"polymarket": bounded_polymarket})
    case = _CATCH_UP_CASES[2]
    processor, run_id, target = _bootstrap_catch_up_case(case, config=config)
    assert processor.process_replayed(target).success

    final_result = None
    for ordinal, size in ((3, "2593.08"), (4, "2697.08"), (5, "2803.08")):
        continuation = _envelope(
            run_id,
            case.ingest_seq + ordinal,
            WS_MARKET,
            _same_target_continuation(target.payload_bytes, size=size, ordinal=ordinal),
            connection_id=target.connection_id,
            monotonic_ns=case.target_received_monotonic_ns + ordinal * 1_000_000,
            wall_time_ns=case.target_received_wall_time_ns + ordinal * 1_000_000,
            ordinal=ordinal,
        )
        final_result = processor.process_replayed(continuation)
        assert final_result.success

    assert final_result is not None
    assert final_result.needs_resync
    summary = processor.summary()
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_expired == 2
    assert summary.reported_top_transition_frames == 6
    assert {outcome.status for outcome in summary.reported_top_transition_outcomes} == {
        ReportedTopTransitionStatus.EXPIRED
    }
    assert {outcome.reason for outcome in summary.reported_top_transition_outcomes} == {
        ReportedTopTransitionReason.FRAME_BOUND_EXCEEDED
    }
    accepted_frame_counts = {
        outcome.accepted_frame_count for outcome in summary.reported_top_transition_outcomes
    }
    assert accepted_frame_counts == {3}


def test_transition_expires_at_exact_monotonic_deadline() -> None:
    case = _CATCH_UP_CASES[2]
    processor, run_id, target = _bootstrap_catch_up_case(case)
    assert processor.process_replayed(target).success
    deadline = case.target_received_monotonic_ns + 2_000_000_000
    at_deadline = _envelope(
        run_id,
        case.ingest_seq + 3,
        WS_MARKET,
        _same_target_continuation(target.payload_bytes, size="2593.08", ordinal=3),
        connection_id=target.connection_id,
        monotonic_ns=deadline,
        wall_time_ns=case.target_received_wall_time_ns + 2_000_000_000,
        ordinal=3,
    )

    result = processor.process_replayed(at_deadline)

    assert result.success
    assert result.needs_resync
    summary = processor.summary()
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_expired == 2
    assert {outcome.reason for outcome in summary.reported_top_transition_outcomes} == {
        ReportedTopTransitionReason.TIME_BOUND_EXPIRED
    }
    assert {outcome.duration_ns for outcome in summary.reported_top_transition_outcomes} == {
        2_000_000_000
    }


def test_non_websocket_envelope_cannot_overtake_transition_expiry_supervisor() -> None:
    case = _CATCH_UP_CASES[2]
    processor, run_id, target = _bootstrap_catch_up_case(case)
    assert processor.process_replayed(target).success
    at_deadline = _envelope(
        run_id,
        case.ingest_seq + 50,
        GAMMA_MARKETS_KEYSET,
        _gamma_payload(),
        connection_id=ConnectionId("non-websocket-deadline-control"),
        monotonic_ns=case.target_received_monotonic_ns + 2_000_000_000,
        wall_time_ns=case.target_received_wall_time_ns + 2_000_000_000,
    )

    result = processor.process_replayed(at_deadline)

    assert result.success
    assert result.error_type is None
    assert not result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.pending_reported_top_transitions == 2
    assert summary.reported_top_transitions_expired == 0
    assert summary.reported_top_transitions_invalidated == 0
    assert not summary.reported_top_transition_outcomes


def test_rest_audit_cannot_match_or_clear_a_pending_transition() -> None:
    case = _CATCH_UP_CASES[0]
    processor, run_id, target = _bootstrap_catch_up_case(case)
    assert processor.process_replayed(target).success
    watermark = processor.processed_websocket_watermark(target.connection_id)
    yes_id = _instrument_id(processor, YES_ASSET_ID)
    start = AuditStartEvidence(
        request_id="reported-top-pending-audit",
        connection_id=target.connection_id,
        native_asset_id=YES_ASSET_ID,
        attempt=1,
        request_started_monotonic_ns=case.target_received_monotonic_ns + 1,
        starting_admission_watermark=WebSocketAdmissionWatermark(
            connection_id=target.connection_id,
            ordinal=watermark.ordinal,
        ),
        starting_processed_watermark=watermark,
        starting_book=processor.audit_book_identity(YES_ASSET_ID),
    )
    intent = _envelope(
        run_id,
        case.ingest_seq + 1,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        connection_id=target.connection_id,
        monotonic_ns=start.request_started_monotonic_ns,
        wall_time_ns=case.target_received_wall_time_ns + 1,
    )
    assert processor.process_replayed(intent).success
    response = _envelope(
        run_id,
        case.ingest_seq + 2,
        REST_BOOK_AUDIT,
        _rest_book_payload(
            asset_id=YES_ASSET_ID,
            top=case.yes_baseline,
            timestamp_ms=int(json.loads(target.payload_bytes)["timestamp"]),
        ),
        connection_id=target.connection_id,
        monotonic_ns=case.target_received_monotonic_ns + 2,
        wall_time_ns=case.target_received_wall_time_ns + 2,
        transport_metadata={
            "request_id": start.request_id,
            "audit_attempt": start.attempt,
            "response_boundary_websocket_ordinal": watermark.ordinal,
            "barrier_websocket_ordinal": watermark.ordinal,
        },
    )

    result = processor.process_replayed(response)

    assert result.success
    assert not result.needs_resync
    assert len(result.audit_outcomes) == 1
    outcome = result.audit_outcomes[0]
    assert outcome.outcome is AuditOutcome.FAILED
    assert outcome.reason is AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE
    assert outcome.requires_resync is False
    assert processor.reported_top_transition_for(yes_id) is not None
    summary = processor.summary()
    assert summary.rest_audits == 1
    assert summary.rest_audit_matches == 0
    assert summary.rest_audit_failed == 1
    assert summary.pending_reported_top_transitions == 2


def test_exact_corpus_replay_has_deterministic_transition_and_terminal_hashes() -> None:
    case = _CATCH_UP_CASES[0]

    def replay_once() -> tuple[str, str, tuple[ReportedTopTransitionOutcome, ...]]:
        processor, run_id, target = _bootstrap_catch_up_case(case)
        assert processor.process_replayed(target).success
        resnapshot = _envelope(
            run_id,
            case.ingest_seq + 100,
            WS_MARKET,
            _two_book_snapshot(
                yes_top=case.yes_target,
                no_top=case.no_target,
                timestamp_ms=int(json.loads(target.payload_bytes)["timestamp"]) + 1_000,
                suffix="deterministic-resnapshot",
            ),
            connection_id=ConnectionId(f"resnapshot-{case.connection_id}"),
            monotonic_ns=case.target_received_monotonic_ns + case.target_to_resnapshot_ns,
            wall_time_ns=case.target_received_wall_time_ns + case.target_to_resnapshot_ns,
            ordinal=1,
        )
        assert processor.process_replayed(resnapshot).success
        summary = processor.summary()
        return (
            summary.terminal_hash,
            summary.reported_top_transition_chain_sha256,
            summary.reported_top_transition_outcomes,
        )

    first = replay_once()
    second = replay_once()

    assert first == second
    assert len(first[0]) == 64
    assert len(first[1]) == 64


def test_post_snapshot_outcome_retains_exact_baseline_and_resnapshot_close_source() -> None:
    case = _CATCH_UP_CASES[0]
    processor, run_id, target = _bootstrap_catch_up_case(case)
    target_timestamp_ms = int(json.loads(target.payload_bytes)["timestamp"])
    baseline_payload = _two_book_snapshot(
        yes_top=case.yes_baseline,
        no_top=case.no_baseline,
        timestamp_ms=target_timestamp_ms - 10_000,
        suffix=f"baseline-{case.ingest_seq}",
    )
    baseline_payload_sha256 = hashlib.sha256(baseline_payload).hexdigest()
    expected_baseline_states_list: list[tuple[InstrumentId, int, str, Price, Price]] = []
    for owner in processor.owners:
        snapshot = owner.snapshot()
        assert snapshot.best_bid is not None
        assert snapshot.best_ask is not None
        expected_baseline_states_list.append(
            (
                owner.instrument_id,
                owner.generation,
                owner.audit_comparison_sha256(),
                snapshot.best_bid.price,
                snapshot.best_ask.price,
            )
        )
    expected_baseline_states = tuple(
        sorted(expected_baseline_states_list, key=lambda item: item[0].value)
    )
    expected_baseline_event_ids = {
        owner.snapshot().last_source_event_id for owner in processor.owners
    }

    assert processor.process_replayed(target).success
    transitions = {
        transition.instrument_id: transition for transition in processor.reported_top_transitions
    }
    assert len(transitions) == 2
    baselines = tuple(transition.baseline_evidence for transition in transitions.values())
    assert baselines[0] is not None
    assert baselines[1] == baselines[0]
    baseline = baselines[0]
    assert baseline.connection_id == target.connection_id
    assert baseline.monotonic_epoch_id == target.received_monotonic.epoch_id
    assert baseline.admission_ordinal == 1
    assert baseline.ingest_seq == case.snapshot_ingest_seq
    assert baseline.received_monotonic_ns == (
        case.target_received_monotonic_ns - case.snapshot_to_target_ns
    )
    assert baseline.received_at.nanoseconds_since_epoch == (
        case.target_received_wall_time_ns - case.snapshot_to_target_ns
    )
    assert len(baseline.source_raw_refs) == 1
    assert baseline.source_raw_refs[0].payload_sha256 == baseline_payload_sha256
    assert baseline.source_raw_refs[0].ingest_id == IngestId(
        f"fixture-{case.snapshot_ingest_seq}-{baseline_payload_sha256[:16]}"
    )
    assert set(baseline.source_canonical_event_ids) == expected_baseline_event_ids
    assert baseline.instrument_states == expected_baseline_states

    resnapshot = _envelope(
        run_id,
        case.ingest_seq + 100,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=case.yes_target,
            no_top=case.no_target,
            timestamp_ms=target_timestamp_ms + 1_000,
            suffix="baseline-close-source",
        ),
        connection_id=ConnectionId("baseline-close-source-resnapshot"),
        monotonic_ns=case.target_received_monotonic_ns + case.target_to_resnapshot_ns,
        wall_time_ns=case.target_received_wall_time_ns + case.target_to_resnapshot_ns,
        ordinal=1,
    )
    result = processor.process_replayed(resnapshot)

    assert result.success
    outcomes = {
        outcome.instrument_id: outcome
        for outcome in processor.summary().reported_top_transition_outcomes
    }
    assert set(outcomes) == set(transitions)
    for instrument_id, outcome in outcomes.items():
        assert outcome.transition == transitions[instrument_id]
        assert outcome.transition.baseline_evidence == baseline
        assert outcome.status is ReportedTopTransitionStatus.AUTHORITATIVE_SNAPSHOT
        assert outcome.reason is ReportedTopTransitionReason.AUTHORITATIVE_FULL_SNAPSHOT
        assert outcome.close_source is not None
        assert outcome.close_source.raw_source.ingest_id == resnapshot.ingest_id
        assert outcome.close_source.raw_source.payload_sha256 == resnapshot.payload_sha256
        assert outcome.close_source.ingest_seq == resnapshot.ingest_seq
        assert outcome.close_source.channel == WS_MARKET
        assert outcome.close_source.connection_id == resnapshot.connection_id
        assert outcome.close_source.received_at == resnapshot.received_wall_time
        assert outcome.close_source.received_monotonic == resnapshot.received_monotonic
        assert outcome.close_source.admission_ordinal == 1
        assert outcome.close_source.canonical_event_id == next(
            owner.snapshot().last_source_event_id
            for owner in processor.owners
            if owner.instrument_id == instrument_id
        )


def test_post_snapshot_recognizer_rejects_complementary_quantity_economic_noop_shape() -> None:
    processor, run_id, connection_id = _bootstrap_snapshot(
        run_name="reported-top-complementary-noop",
        yes_top=("0.016", "0.018"),
        no_top=("0.982", "0.984"),
    )
    payload = json.dumps(
        {
            "event_type": "price_change",
            "market": CONDITION_ID,
            "price_changes": [
                {
                    "asset_id": YES_ASSET_ID,
                    "price": "0.016",
                    "size": "10",
                    "side": "BUY",
                    "hash": "complementary-noop-yes",
                    "best_bid": "0.017",
                    "best_ask": "0.018",
                },
                {
                    "asset_id": NO_ASSET_ID,
                    "price": "0.984",
                    "size": "10",
                    "side": "SELL",
                    "hash": "complementary-noop-no",
                    "best_bid": "0.982",
                    "best_ask": "0.983",
                },
            ],
            "timestamp": "1787499999100",
        },
        separators=(",", ":"),
    ).encode()
    target = _envelope(
        run_id,
        3,
        WS_MARKET,
        payload,
        connection_id=connection_id,
        monotonic_ns=200,
        wall_time_ns=1_787_500_000_000_000_100,
        ordinal=2,
    )

    result = processor.process_replayed(target)

    assert result.success
    assert result.needs_resync
    assert result.canonical_event_count == 2
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.reported_top_no_pending_recognized == 0
    assert summary.reported_top_no_pending_rejected == 2
    assert summary.reported_top_transitions_started == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.healthy_books == 0


def test_duplicate_same_side_price_is_rejected_by_preview_and_apply_without_partial_levels() -> (
    None
):
    processor, _, _ = _bootstrap_snapshot(
        run_name="reported-top-duplicate-side-price",
        yes_top=("0.014", "0.017"),
        no_top=("0.983", "0.986"),
    )
    owner = _owner(processor, YES_ASSET_ID)
    raw = b"duplicate-same-side-price"
    payload = PriceChangeEvent(
        instrument_id=owner.instrument_id,
        market_id=owner.market_id,
        changes=(
            PriceLevelChange(
                side=BookSide.BID,
                price=Price.parse("0.014"),
                new_quantity=Quantity.parse("11"),
                venue_hash="duplicate-first",
                reported_best_bid=Price.parse("0.014"),
                reported_best_ask=Price.parse("0.017"),
            ),
            PriceLevelChange(
                side=BookSide.BID,
                price=Price.parse("0.014"),
                new_quantity=Quantity.parse("12"),
                venue_hash="duplicate-second",
                reported_best_bid=Price.parse("0.014"),
                reported_best_ask=Price.parse("0.017"),
            ),
        ),
    )
    event = CanonicalEvent(
        canonical_event_id=CanonicalEventId("duplicate-same-side-price-event"),
        schema_version=SchemaVersion("duplicate-test-schema"),
        normalizer_version=NormalizerVersion("duplicate-test-normalizer"),
        event_type=CanonicalEventType.PRICE_CHANGE,
        venue_id=POLYMARKET_VENUE_ID,
        market_id=owner.market_id,
        instrument_id=owner.instrument_id,
        source_ingest_ids=(IngestId("duplicate-same-side-price-ingest"),),
        source_payload_hashes=(hashlib.sha256(raw).hexdigest(),),
        reference_version_ids=(),
        venue_timestamp=UtcTimestamp(1_787_499_999_100_000_000),
        receive_timestamp=UtcTimestamp(1_787_500_000_000_000_100),
        deduplication_key=None,
        payload=payload,
    )
    before = owner.snapshot()

    with pytest.raises(BookStateError, match="duplicate side/price update"):
        owner.preview_price_change(payload, event=event)
    assert owner.snapshot() == before

    with pytest.raises(BookStateError, match="duplicate side/price update"):
        owner.apply_price_change(payload, event=event)
    after = owner.snapshot()
    assert after.health is BookHealth.INVALID
    assert after.invalidation_reason is InvalidationReason.PROTOCOL_VIOLATION
    assert after.generation == before.generation
    # Application is fail-closed: the duplicate never wins, and invalidation
    # deliberately clears the former exact image until an authoritative reset.
    assert after.bids == ()
    assert after.asks == ()


def test_stale_gap_incremental_invalidates_but_authoritative_ws_snapshot_recovers() -> None:
    config = _config()
    processor, run_id, connection_id = _bootstrap_snapshot(
        run_name="reported-top-stale-gap-resnapshot",
        yes_top=("0.014", "0.017"),
        no_top=("0.983", "0.986"),
        config=config,
    )
    gap_monotonic_ns = 100 + int(config.scanner.maximum_book_age_seconds * 1_000_000_000) + 1
    stale_delta = _envelope(
        run_id,
        3,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.016",
            size="5",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_100,
        ),
        connection_id=connection_id,
        monotonic_ns=gap_monotonic_ns,
        wall_time_ns=1_787_500_000_000_000_000 + gap_monotonic_ns,
        ordinal=2,
    )
    stale_result = processor.process_replayed(stale_delta)
    assert stale_result.success
    assert stale_result.needs_resync
    stale_yes = _owner(processor, YES_ASSET_ID).snapshot()
    assert stale_yes.health is BookHealth.INVALID
    assert stale_yes.invalidation_reason is InvalidationReason.STALE_SNAPSHOT

    resnapshot = _envelope(
        run_id,
        4,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=("0.013", "0.016"),
            no_top=("0.984", "0.987"),
            timestamp_ms=1_787_499_999_101,
            suffix="stale-gap-recovery",
        ),
        connection_id=connection_id,
        monotonic_ns=gap_monotonic_ns + 1,
        wall_time_ns=1_787_500_000_000_000_000 + gap_monotonic_ns + 1,
        ordinal=3,
    )
    recovered = processor.process_replayed(resnapshot)

    assert recovered.success
    assert not recovered.needs_resync
    assert recovered.canonical_event_count == 2
    assert processor.summary().parse_failures == 0
    for asset_id, expected_top in (
        (YES_ASSET_ID, ("0.013", "0.016")),
        (NO_ASSET_ID, ("0.984", "0.987")),
    ):
        snapshot = _owner(processor, asset_id).snapshot()
        assert snapshot.health is BookHealth.HEALTHY
        assert snapshot.best_bid is not None
        assert snapshot.best_ask is not None
        assert (str(snapshot.best_bid.price), str(snapshot.best_ask.price)) == expected_top


def test_tick_reference_change_invalidates_pending_transition_with_typed_close_source() -> None:
    processor, run_id, connection_id, _, transition = _bootstrap_yes_bbo_transition(
        "reported-top-tick-reference-change"
    )
    tick_change = _envelope(
        run_id,
        4,
        WS_MARKET,
        _tick_size_change_payload(
            asset_id=YES_ASSET_ID,
            old_tick_size="0.001",
            new_tick_size="0.0001",
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )

    result = processor.process_replayed(tick_change)

    assert result.success
    assert result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_invalidated == 1
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition == transition
    assert outcome.status is ReportedTopTransitionStatus.INVALIDATED
    assert outcome.reason is ReportedTopTransitionReason.REFERENCE_PRECISION_CHANGED
    assert outcome.research_eligibility_restored is False
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == tick_change.ingest_id
    assert outcome.close_source.raw_source.payload_sha256 == tick_change.payload_sha256
    assert outcome.close_source.ingest_seq == tick_change.ingest_seq
    assert outcome.close_source.channel == WS_MARKET
    assert outcome.close_source.connection_id == connection_id
    assert outcome.close_source.admission_ordinal == 3
    assert outcome.close_source.canonical_event_id is not None
    yes_snapshot = _owner(processor, YES_ASSET_ID).snapshot()
    assert yes_snapshot.health is BookHealth.INVALID
    assert yes_snapshot.invalidation_reason is InvalidationReason.UNKNOWN_PRECISION


def test_durable_targeted_expiry_closes_exact_transition_and_binds_control_source() -> None:
    processor, run_id, _, bbo, transition = _bootstrap_yes_bbo_transition(
        "reported-top-durable-expiry"
    )
    command = ReportedTopTransitionExpiryCommand(
        transition_id=transition.transition_id,
        instrument_id=transition.instrument_id,
        monotonic_epoch_id=transition.monotonic_epoch_id,
        deadline_monotonic_ns=transition.deadline_monotonic_ns,
        decision_monotonic_ns=transition.deadline_monotonic_ns,
    )
    control = _envelope(
        run_id,
        4,
        CONTROL_REPORTED_TOP_TRANSITION_EXPIRED,
        command.to_payload(),
        connection_id=ConnectionId("reported-top-expiry-controller"),
        monotonic_ns=command.decision_monotonic_ns,
        wall_time_ns=(
            bbo.received_wall_time.nanoseconds_since_epoch + transition.maximum_duration_ns
        ),
    )

    result = processor.process_replayed(control)

    assert result.success
    assert result.error_type is None
    assert result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_expired == 1
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition == transition
    assert outcome.status is ReportedTopTransitionStatus.EXPIRED
    assert outcome.reason is ReportedTopTransitionReason.TIME_BOUND_EXPIRED
    assert outcome.duration_ns == transition.maximum_duration_ns
    assert outcome.ended_monotonic_ns == command.decision_monotonic_ns
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == control.ingest_id
    assert outcome.close_source.raw_source.payload_sha256 == control.payload_sha256
    assert outcome.close_source.ingest_seq == control.ingest_seq
    assert outcome.close_source.channel == CONTROL_REPORTED_TOP_TRANSITION_EXPIRED
    assert outcome.close_source.connection_id == control.connection_id
    assert outcome.close_source.received_at == control.received_wall_time
    assert outcome.close_source.received_monotonic == control.received_monotonic
    assert outcome.close_source.admission_ordinal is None
    assert outcome.close_source.canonical_event_id is None


@pytest.mark.parametrize("invalid_kind", ["early", "wrong-identity"])
def test_durable_expiry_rejects_early_or_wrong_identity_and_fails_closed(
    invalid_kind: str,
) -> None:
    processor, run_id, _, bbo, transition = _bootstrap_yes_bbo_transition(
        f"reported-top-expiry-{invalid_kind}"
    )
    command = ReportedTopTransitionExpiryCommand(
        transition_id=transition.transition_id,
        instrument_id=transition.instrument_id,
        monotonic_epoch_id=transition.monotonic_epoch_id,
        deadline_monotonic_ns=transition.deadline_monotonic_ns,
        decision_monotonic_ns=transition.deadline_monotonic_ns,
    )
    control_object = json.loads(command.to_payload())
    if invalid_kind == "early":
        decision_monotonic_ns = transition.deadline_monotonic_ns - 1
        control_object["decision_monotonic_ns"] = decision_monotonic_ns
    else:
        decision_monotonic_ns = transition.deadline_monotonic_ns
        wrong_transition_id = "f" * 64
        assert wrong_transition_id != transition.transition_id
        control_object["transition_id"] = wrong_transition_id
    control = _envelope(
        run_id,
        4,
        CONTROL_REPORTED_TOP_TRANSITION_EXPIRED,
        canonical_json_bytes(control_object),
        connection_id=ConnectionId(f"reported-top-expiry-{invalid_kind}-controller"),
        monotonic_ns=decision_monotonic_ns,
        wall_time_ns=(
            bbo.received_wall_time.nanoseconds_since_epoch
            + decision_monotonic_ns
            - transition.first_received_monotonic_ns
        ),
    )

    result = processor.process_replayed(control)

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 1
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_expired == 0
    assert summary.reported_top_transitions_invalidated == 1
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition == transition
    assert outcome.status is ReportedTopTransitionStatus.INVALIDATED
    assert outcome.reason is ReportedTopTransitionReason.INVALID_EXACT_FRAME
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == control.ingest_id
    assert outcome.close_source.raw_source.payload_sha256 == control.payload_sha256
    assert outcome.close_source.channel == CONTROL_REPORTED_TOP_TRANSITION_EXPIRED
    assert outcome.close_source.canonical_event_id is None


def test_durable_expiry_rejects_stale_command_after_exact_convergence() -> None:
    processor, run_id, connection_id, bbo, transition = _bootstrap_yes_bbo_transition(
        "reported-top-expiry-stale"
    )
    stale_command = ReportedTopTransitionExpiryCommand(
        transition_id=transition.transition_id,
        instrument_id=transition.instrument_id,
        monotonic_epoch_id=transition.monotonic_epoch_id,
        deadline_monotonic_ns=transition.deadline_monotonic_ns,
        decision_monotonic_ns=transition.deadline_monotonic_ns,
    )
    exact = _envelope(
        run_id,
        4,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.016",
            size="5",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )
    assert processor.process_replayed(exact).success
    convergence = processor.summary().reported_top_transition_outcomes[-1]
    assert convergence.status is ReportedTopTransitionStatus.CONVERGED

    control = _envelope(
        run_id,
        5,
        CONTROL_REPORTED_TOP_TRANSITION_EXPIRED,
        stale_command.to_payload(),
        connection_id=ConnectionId("reported-top-expiry-stale-controller"),
        monotonic_ns=stale_command.decision_monotonic_ns,
        wall_time_ns=(
            bbo.received_wall_time.nanoseconds_since_epoch + transition.maximum_duration_ns
        ),
    )
    result = processor.process_replayed(control)

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 1
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_expired == 0
    assert summary.reported_top_transitions_converged == 1
    assert summary.reported_top_transition_outcomes == (convergence,)


def test_remove_old_level_before_add_new_level_stays_pending_then_converges() -> None:
    processor, run_id, connection_id, _, started = _bootstrap_yes_bbo_transition(
        "reported-top-remove-before-add"
    )
    removal = _envelope(
        run_id,
        4,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.017",
            size="0",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )

    removed = processor.process_replayed(removal)

    assert removed.success
    assert not removed.needs_resync
    after_removal = _owner(processor, YES_ASSET_ID).snapshot()
    assert after_removal.health is BookHealth.HEALTHY
    assert after_removal.best_bid is not None
    assert after_removal.best_ask is None
    pending = _assert_pending_transition_is_fail_closed(processor, YES_ASSET_ID)
    assert pending.transition_id == started.transition_id
    assert pending.accepted_frame_count == 2
    assert processor.summary().reported_top_transition_outcomes == ()

    addition = _envelope(
        run_id,
        5,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.016",
            size="5",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_102,
        ),
        connection_id=connection_id,
        monotonic_ns=400,
        wall_time_ns=1_787_500_000_000_000_300,
        ordinal=4,
    )
    converged = processor.process_replayed(addition)

    assert converged.success
    assert not converged.needs_resync
    assert processor.audit_ineligible_instruments == ()
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_converged == 1
    assert summary.validated_opportunities == 0
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition_id == started.transition_id
    assert outcome.accepted_frame_count == 3
    assert outcome.status is ReportedTopTransitionStatus.CONVERGED
    assert outcome.reason is ReportedTopTransitionReason.EXACT_L2_CONVERGED
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == addition.ingest_id
    assert outcome.close_source.canonical_event_id is not None


def test_add_new_level_before_remove_old_level_converges_then_stays_healthy() -> None:
    processor, run_id, connection_id, _, started = _bootstrap_yes_bbo_transition(
        "reported-top-add-before-remove"
    )
    addition = _envelope(
        run_id,
        4,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.016",
            size="5",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )

    converged = processor.process_replayed(addition)

    assert converged.success
    assert not converged.needs_resync
    after_addition = _owner(processor, YES_ASSET_ID).snapshot()
    assert after_addition.health is BookHealth.HEALTHY
    assert after_addition.best_ask is not None
    assert str(after_addition.best_ask.price) == "0.016"
    assert {str(level.price) for level in after_addition.asks} >= {"0.016", "0.017"}
    assert processor.reported_top_transitions == ()
    assert processor.audit_ineligible_instruments == ()
    converged_summary = processor.summary()
    assert converged_summary.reported_top_transitions_converged == 1
    outcome = converged_summary.reported_top_transition_outcomes[-1]
    assert outcome.transition_id == started.transition_id
    assert outcome.status is ReportedTopTransitionStatus.CONVERGED
    assert outcome.reason is ReportedTopTransitionReason.EXACT_L2_CONVERGED
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == addition.ingest_id

    removal = _envelope(
        run_id,
        5,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.017",
            size="0",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_102,
        ),
        connection_id=connection_id,
        monotonic_ns=400,
        wall_time_ns=1_787_500_000_000_000_300,
        ordinal=4,
    )

    removed = processor.process_replayed(removal)

    assert removed.success
    assert not removed.needs_resync
    after_removal = _owner(processor, YES_ASSET_ID).snapshot()
    assert after_removal.health is BookHealth.HEALTHY
    assert after_removal.best_ask is not None
    assert str(after_removal.best_ask.price) == "0.016"
    assert "0.017" not in {str(level.price) for level in after_removal.asks}
    assert processor.reported_top_transitions == ()
    assert processor.audit_ineligible_instruments == ()
    final_summary = processor.summary()
    assert final_summary.reported_top_transitions_converged == 1
    assert final_summary.reported_top_transition_outcomes == (outcome,)


def test_repeated_same_target_then_complete_exact_top_supersedes_successfully() -> None:
    processor, run_id, connection_id, _, started = _bootstrap_yes_bbo_transition(
        "reported-top-repeat-then-supersede"
    )
    repeated = _envelope(
        run_id,
        4,
        WS_MARKET,
        _best_bid_ask_payload(
            YES_ASSET_ID,
            ("0.014", "0.016"),
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )
    repeated_result = processor.process_replayed(repeated)
    assert repeated_result.success
    repeated_transition = _assert_pending_transition_is_fail_closed(processor, YES_ASSET_ID)
    assert repeated_transition.transition_id == started.transition_id
    assert repeated_transition.accepted_frame_count == 2

    exact_complete_top = _envelope(
        run_id,
        5,
        WS_MARKET,
        _single_unreported_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="BUY",
            price="0.015",
            size="5",
            timestamp_ms=1_787_499_999_102,
        ),
        connection_id=connection_id,
        monotonic_ns=400,
        wall_time_ns=1_787_500_000_000_000_300,
        ordinal=4,
    )
    exact_result = processor.process_replayed(exact_complete_top)
    assert exact_result.success
    pending = _assert_pending_transition_is_fail_closed(processor, YES_ASSET_ID)
    assert pending.accepted_frame_count == 3
    owner = _owner(processor, YES_ASSET_ID)
    exact_snapshot = owner.snapshot()
    assert exact_snapshot.best_bid is not None
    assert exact_snapshot.best_ask is not None
    assert (str(exact_snapshot.best_bid.price), str(exact_snapshot.best_ask.price)) == (
        "0.015",
        "0.017",
    )

    complete_bbo = _envelope(
        run_id,
        6,
        WS_MARKET,
        _best_bid_ask_payload(
            YES_ASSET_ID,
            ("0.015", "0.017"),
            timestamp_ms=1_787_499_999_103,
        ),
        connection_id=connection_id,
        monotonic_ns=500,
        wall_time_ns=1_787_500_000_000_000_400,
        ordinal=5,
    )
    superseded = processor.process_replayed(complete_bbo)

    assert superseded.success
    assert not superseded.needs_resync
    assert processor.audit_ineligible_instruments == ()
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_superseded == 1
    assert summary.validated_opportunities == 0
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition_id == started.transition_id
    assert outcome.accepted_frame_count == 3
    assert outcome.status is ReportedTopTransitionStatus.SUPERSEDED
    assert outcome.reason is ReportedTopTransitionReason.COMPLETE_REPORTED_TOP_SUPERSEDED
    assert outcome.research_eligibility_restored
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == complete_bbo.ingest_id
    assert outcome.close_source.canonical_event_id is not None


def test_irrelevant_last_trade_does_not_advance_or_clear_pending_transition() -> None:
    processor, run_id, connection_id, _, started = _bootstrap_yes_bbo_transition(
        "reported-top-last-trade-pending"
    )
    owner = _owner(processor, YES_ASSET_ID)
    exact_hash_before = owner.economic_content_sha256()
    trade = _envelope(
        run_id,
        4,
        WS_MARKET,
        _last_trade_payload(
            asset_id=YES_ASSET_ID,
            price="0.015",
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )

    result = processor.process_replayed(trade)

    assert result.success
    assert not result.needs_resync
    assert result.canonical_event_count == 1
    assert result.scan_count == 0
    pending = _assert_pending_transition_is_fail_closed(processor, YES_ASSET_ID)
    assert pending == started
    assert pending.accepted_frame_count == 1
    assert owner.economic_content_sha256() == exact_hash_before
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.reported_top_transition_frames == 1
    assert summary.reported_top_transition_outcomes == ()


def test_disconnect_closes_typed_then_new_generation_authoritative_snapshot_recovers() -> None:
    processor, run_id, _, _, started = _bootstrap_yes_bbo_transition(
        "reported-top-disconnect-resnapshot"
    )
    yes_id = _instrument_id(processor, YES_ASSET_ID)
    no_id = _instrument_id(processor, NO_ASSET_ID)

    affected = processor.on_disconnect()

    assert set(affected) == {yes_id, no_id}
    disconnected_summary = processor.summary()
    assert disconnected_summary.healthy_books == 0
    assert disconnected_summary.pending_reported_top_transitions == 0
    assert disconnected_summary.reported_top_transitions_invalidated == 1
    assert disconnected_summary.validated_opportunities == 0
    assert all(owner.snapshot().health is BookHealth.INVALID for owner in processor.owners)
    disconnect_outcome = disconnected_summary.reported_top_transition_outcomes[-1]
    assert disconnect_outcome.transition == started
    assert disconnect_outcome.status is ReportedTopTransitionStatus.INVALIDATED
    assert disconnect_outcome.reason is ReportedTopTransitionReason.DISCONNECT
    assert disconnect_outcome.close_source is None
    assert disconnect_outcome.duration_is_lower_bound
    assert not disconnect_outcome.research_eligibility_restored

    resnapshot = _envelope(
        run_id,
        4,
        WS_MARKET,
        _two_book_snapshot(
            yes_top=("0.014", "0.016"),
            no_top=("0.984", "0.986"),
            timestamp_ms=1_787_499_999_101,
            suffix="disconnect-authoritative-recovery",
        ),
        connection_id=ConnectionId("reported-top-post-disconnect-generation"),
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=1,
    )
    recovered = processor.process_replayed(resnapshot)

    assert recovered.success
    assert not recovered.needs_resync
    assert recovered.canonical_event_count == 2
    assert processor.audit_ineligible_instruments == ()
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.healthy_books == 2
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transition_outcomes == (disconnect_outcome,)
    assert summary.validated_opportunities == 0


def test_invalid_quantity_while_pending_fails_closed_with_deterministic_outcome() -> None:
    processor, run_id, connection_id, _, started = _bootstrap_yes_bbo_transition(
        "reported-top-invalid-quantity"
    )
    invalid = _envelope(
        run_id,
        4,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="SELL",
            price="0.016",
            size="-1",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )

    result = processor.process_replayed(invalid)

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 1
    assert summary.healthy_books == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_invalidated == 1
    assert summary.validated_opportunities == 0
    assert all(owner.snapshot().health is BookHealth.INVALID for owner in processor.owners)
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition == started
    assert outcome.status is ReportedTopTransitionStatus.INVALIDATED
    assert outcome.reason is ReportedTopTransitionReason.INVALID_EXACT_FRAME
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == invalid.ingest_id
    assert outcome.close_source.raw_source.payload_sha256 == invalid.payload_sha256
    assert outcome.close_source.canonical_event_id is None


def test_conflicting_raw_provenance_while_pending_invalidates_typed_without_parse_failure() -> None:
    run_name = "reported-top-conflicting-provenance"
    processor, run_id, connection_id, _, started = _bootstrap_yes_bbo_transition(run_name)
    baseline_payload = _two_book_snapshot(
        yes_top=("0.014", "0.017"),
        no_top=("0.983", "0.986"),
        timestamp_ms=1_787_499_999_000,
        suffix=run_name,
    )
    baseline_payload_sha256 = hashlib.sha256(baseline_payload).hexdigest()
    baseline_ingest_id = IngestId(f"fixture-2-{baseline_payload_sha256[:16]}")
    distinct_delta = _envelope(
        run_id,
        4,
        WS_MARKET,
        _single_price_change_payload(
            asset_id=YES_ASSET_ID,
            side="BUY",
            price="0.013",
            size="5",
            reported_top=("0.014", "0.016"),
            timestamp_ms=1_787_499_999_101,
        ),
        connection_id=connection_id,
        monotonic_ns=300,
        wall_time_ns=1_787_500_000_000_000_200,
        ordinal=3,
    )
    conflicting = replace(distinct_delta, ingest_id=baseline_ingest_id)
    assert conflicting.payload_sha256 != baseline_payload_sha256

    result = processor.process_replayed(conflicting)

    assert result.success
    assert result.error_type is None
    assert result.needs_resync
    summary = processor.summary()
    assert summary.parse_failures == 0
    assert summary.pending_reported_top_transitions == 0
    assert summary.reported_top_transitions_invalidated == 1
    assert summary.validated_opportunities == 0
    outcome = summary.reported_top_transition_outcomes[-1]
    assert outcome.transition == started
    assert outcome.status is ReportedTopTransitionStatus.INVALIDATED
    assert outcome.reason is ReportedTopTransitionReason.INVALID_EXACT_FRAME
    assert outcome.close_source is not None
    assert outcome.close_source.raw_source.ingest_id == baseline_ingest_id
    assert outcome.close_source.raw_source.payload_sha256 == conflicting.payload_sha256
    assert outcome.close_source.canonical_event_id is not None
    invalid = _owner(processor, YES_ASSET_ID).snapshot()
    assert invalid.health is BookHealth.INVALID
    assert invalid.invalidation_reason is InvalidationReason.PROTOCOL_VIOLATION

    watermark = processor.processed_websocket_watermark(connection_id)
    audit_start = AuditStartEvidence(
        request_id="conflicting-provenance-invalid-audit",
        connection_id=connection_id,
        native_asset_id=YES_ASSET_ID,
        attempt=1,
        request_started_monotonic_ns=301,
        starting_admission_watermark=watermark,
        starting_processed_watermark=watermark,
        starting_book=processor.audit_book_identity(YES_ASSET_ID),
    )
    intent = _envelope(
        run_id,
        5,
        CONTROL_REST_AUDIT_START,
        audit_start.to_payload(),
        connection_id=connection_id,
        monotonic_ns=301,
        wall_time_ns=1_787_500_000_000_000_201,
    )
    assert processor.process_replayed(intent).success
    response = _envelope(
        run_id,
        6,
        REST_BOOK_AUDIT,
        _rest_book_payload(
            asset_id=YES_ASSET_ID,
            top=("0.014", "0.017"),
            timestamp_ms=1_787_499_999_102,
        ),
        connection_id=connection_id,
        monotonic_ns=302,
        wall_time_ns=1_787_500_000_000_000_202,
        transport_metadata={
            "request_id": audit_start.request_id,
            "audit_attempt": audit_start.attempt,
            "response_boundary_websocket_ordinal": watermark.ordinal,
            "barrier_websocket_ordinal": watermark.ordinal,
        },
    )
    audited = processor.process_replayed(response)
    assert audited.success
    assert audited.needs_resync
    assert len(audited.audit_outcomes) == 1
    audit_outcome = audited.audit_outcomes[0]
    assert audit_outcome.outcome is AuditOutcome.FAILED
    assert audit_outcome.reason is AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE
    assert not audit_outcome.requires_resync
    assert _instrument_id(processor, YES_ASSET_ID) in processor.audit_ineligible_instruments
