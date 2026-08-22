from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.errors import ProgrammingInvariantViolation, StorageFault
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    SubscriptionId,
)
from multi_market_arbitrage.core.provenance import Provenance
from multi_market_arbitrage.core.serialization import canonical_json_bytes
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalDurabilityState, JournalReceipt
from multi_market_arbitrage.normalization import CanonicalEvent, CanonicalEventType
from multi_market_arbitrage.normalization import polymarket as normalization_module
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_START,
    GAMMA_EVENT_PREFIX,
    GAMMA_MARKET_PREFIX,
    GAMMA_MARKET_RELATIONSHIP_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditFailureCommand,
    AuditFailureReason,
    AuditOutcome,
    AuditStartEvidence,
    CanonicalProcessor,
    ScanTriggerReason,
    WebSocketAdmissionWatermark,
)
from multi_market_arbitrage.pipeline import processor as processor_module
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID, ReferenceBundle
from multi_market_arbitrage.research import Phase1AnalyticalOutputs
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScanResult,
    OpportunityLifecycle,
    ScanDisposition,
)
from multi_market_arbitrage.state import InvalidationReason

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"
_BASE_TIME_NS = 1_787_198_410_000_000_000


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book() -> bytes:
    return b"""{
      "market":"0xcondition",
      "asset_id":"token-no",
      "timestamp":"1787198400124",
      "hash":"opaque-rest-hash-no",
      "bids":[{"price":"0.30","size":"20"}],
      "asks":[{"price":"0.35","size":"20"}],
      "min_order_size":"5",
      "tick_size":"0.01",
      "neg_risk":false,
      "last_trade_price":"0.34"
    }"""


def _direct_gamma_without_events(*, gamma_market_id: int = 3257332) -> bytes:
    page = json.loads(_fixture("polymarket_gamma_market_keyset.json"))
    market = page["markets"][0]
    market["id"] = gamma_market_id
    market.pop("events", None)
    return json.dumps(market, separators=(",", ":")).encode()


def _gamma_relationship(
    direct: bytes,
    *,
    event_ids: tuple[int, ...] = (780132,),
    **changes: object,
) -> bytes:
    market = json.loads(direct)
    market.update(changes)
    market["events"] = [{"id": event_id} for event_id in event_ids]
    return json.dumps([market], separators=(",", ":")).encode()


def _price_change_payload(
    *,
    bid_size: str,
    ask_size: str | None = None,
    timestamp: str,
) -> bytes:
    changes: list[dict[str, str]] = [
        {
            "asset_id": "token-yes",
            "best_ask": "0.55",
            "best_bid": "0.45",
            "hash": f"bid-{timestamp}",
            "price": "0.45",
            "side": "BUY",
            "size": bid_size,
        }
    ]
    if ask_size is not None:
        changes.append(
            {
                "asset_id": "token-yes",
                "best_ask": "0.55",
                "best_bid": "0.45",
                "hash": f"ask-{timestamp}",
                "price": "0.55",
                "side": "SELL",
                "size": ask_size,
            }
        )
    return canonical_json_bytes(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": changes,
            "timestamp": timestamp,
        }
    )


def _new_market_payload(market_id: str = "99") -> bytes:
    return canonical_json_bytes(
        {
            "assets_ids": ["new-yes", "new-no"],
            "event_type": "new_market",
            "id": market_id,
            "market": f"new-condition-{market_id}",
            "timestamp": "1787198406000",
        }
    )


def _envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    monotonic_ns: int | None = None,
    connection_id: ConnectionId | None = None,
    transport_metadata: dict[str, str | int | bool] | None = None,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=connection_id or ConnectionId("processor-test-connection"),
        subscription_id=SubscriptionId("processor-test-subscription"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(_BASE_TIME_NS + sequence * 1_000_000_000),
        received_monotonic=MonotonicTimestamp(
            run_id.value,
            100 + sequence if monotonic_ns is None else monotonic_ns,
        ),
        payload_bytes=payload,
        payload_encoding="utf-8",
        transport_metadata=transport_metadata,
    )


def _audit_start(
    processor: CanonicalProcessor,
    run_id: CollectorRunId,
    sequence: int,
    *,
    request_id: str,
    native_asset_id: str = "token-yes",
    attempt: int = 1,
    admitted_ordinal: int | None = None,
) -> tuple[AuditStartEvidence, RawEventEnvelope]:
    connection_id = ConnectionId("processor-test-connection")
    processed = processor.processed_websocket_watermark(connection_id)
    start = AuditStartEvidence(
        request_id=request_id,
        connection_id=connection_id,
        native_asset_id=native_asset_id,
        attempt=attempt,
        request_started_monotonic_ns=100 + sequence,
        starting_admission_watermark=WebSocketAdmissionWatermark(
            connection_id=connection_id,
            ordinal=processed.ordinal if admitted_ordinal is None else admitted_ordinal,
        ),
        starting_processed_watermark=processed,
        starting_book=processor.audit_book_identity(native_asset_id),
    )
    return start, _envelope(
        run_id,
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        connection_id=connection_id,
        monotonic_ns=start.request_started_monotonic_ns,
    )


def _audit_response(
    run_id: CollectorRunId,
    sequence: int,
    start: AuditStartEvidence,
    payload: bytes,
    *,
    boundary: int,
    barrier: int | None = None,
) -> RawEventEnvelope:
    return _envelope(
        run_id,
        sequence,
        REST_BOOK_AUDIT,
        payload,
        connection_id=start.connection_id,
        transport_metadata={
            "audit_attempt": start.attempt,
            "barrier_websocket_ordinal": boundary if barrier is None else barrier,
            "request_id": start.request_id,
            "response_boundary_websocket_ordinal": boundary,
        },
    )


def _receipt(envelope: RawEventEnvelope) -> JournalReceipt:
    return JournalReceipt(
        ingest_id=envelope.ingest_id,
        collector_run_id=envelope.collector_run_id,
        ingest_seq=envelope.ingest_seq,
        state=JournalDurabilityState.DURABLE,
    )


def _bootstrap(run_id: CollectorRunId) -> tuple[RawEventEnvelope, ...]:
    return (
        _envelope(
            run_id,
            0,
            GAMMA_MARKETS_KEYSET,
            _fixture("polymarket_gamma_market_keyset.json"),
        ),
        _envelope(
            run_id,
            1,
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        _envelope(run_id, 2, REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        _envelope(run_id, 3, REST_BOOK, _no_book()),
    )


class _RecordingOutputs(Phase1AnalyticalOutputs):
    def __init__(self) -> None:
        self.references: list[tuple[ReferenceBundle, UtcTimestamp, Provenance]] = []
        self.canonical_events: list[CanonicalEvent] = []
        self.scans: list[CompleteSetScanResult] = []

    def append_reference(
        self,
        bundle: ReferenceBundle,
        observed_at: UtcTimestamp,
        provenance: Provenance,
    ) -> None:
        self.references.append((bundle, observed_at, provenance))

    def append_canonical(self, event: CanonicalEvent) -> None:
        self.canonical_events.append(event)

    def append_scan(
        self,
        result: CompleteSetScanResult,
        *,
        lifecycle: OpportunityLifecycle | None = None,
    ) -> None:
        del lifecycle
        self.scans.append(result)


class _FailingCanonicalOutputs(_RecordingOutputs):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self._error = error

    def append_canonical(self, event: CanonicalEvent) -> None:
        del event
        raise self._error


def test_exact_id_parent_relationship_is_replayable_and_preserves_all_raw_provenance() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    outputs = _RecordingOutputs()
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    run_id = CollectorRunId("processor-gamma-parent-relationship")
    direct = _direct_gamma_without_events()
    inputs = (
        _envelope(run_id, 0, f"{GAMMA_MARKET_PREFIX}3257332", direct),
        _envelope(
            run_id,
            1,
            f"{GAMMA_MARKET_RELATIONSHIP_PREFIX}3257332",
            _gamma_relationship(direct),
        ),
        _envelope(
            run_id,
            2,
            f"{GAMMA_EVENT_PREFIX}780132",
            b'{"id":780132,"enableNegRisk":false,"negRiskAugmented":false}',
        ),
        _envelope(
            run_id,
            3,
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        _envelope(run_id, 4, REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        _envelope(run_id, 5, REST_BOOK, _no_book()),
    )

    assert all(processor.process_replayed(envelope).success for envelope in inputs)

    bundle = processor.current_bundles[0]
    provenance = processor.reference_provenance(bundle.market.market_id)
    assert bundle.market.gamma_market_id == "3257332"
    assert bundle.market.gamma_event_id == "780132"
    assert provenance is not None
    assert inputs[0].ingest_id in provenance.source_ingest_ids
    assert inputs[1].ingest_id in provenance.source_ingest_ids
    assert inputs[2].ingest_id in provenance.source_ingest_ids
    assert dict(provenance.attributes)["reference_raw_surfaces"] == (
        "gamma-market,gamma-market-relationship,gamma-parent-event,clob-market-info,clob-rest-book"
    )
    assert outputs.references[-1][2] == provenance


def test_exact_id_parent_relationship_identity_mismatch_is_expected_data_failure() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-gamma-parent-mismatch")
    direct = _direct_gamma_without_events()
    direct_envelope = _envelope(
        run_id,
        0,
        f"{GAMMA_MARKET_PREFIX}3257332",
        direct,
    )
    mismatch_envelope = _envelope(
        run_id,
        1,
        f"{GAMMA_MARKET_RELATIONSHIP_PREFIX}3257332",
        _gamma_relationship(direct, conditionId="different-condition"),
    )

    assert processor.process_replayed(direct_envelope).success
    mismatch = processor.process_replayed(mismatch_envelope)

    assert not mismatch.success
    assert mismatch.error_type == "NormalizationError"
    assert processor.discovered_markets[0].condition_id == "0xcondition"
    assert processor.discovered_markets[0].event_id is None


def test_direct_refresh_retains_only_deliberately_carried_relationship_provenance() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-gamma-parent-refresh")
    direct = _direct_gamma_without_events()
    relationship = _envelope(
        run_id,
        1,
        f"{GAMMA_MARKET_RELATIONSHIP_PREFIX}3257332",
        _gamma_relationship(direct),
    )
    bootstrap = (
        _envelope(run_id, 0, f"{GAMMA_MARKET_PREFIX}3257332", direct),
        relationship,
        _envelope(
            run_id,
            2,
            f"{GAMMA_EVENT_PREFIX}780132",
            b'{"id":780132,"enableNegRisk":false,"negRiskAugmented":false}',
        ),
        _envelope(
            run_id,
            3,
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        _envelope(run_id, 4, REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
    )
    assert all(processor.process_replayed(envelope).success for envelope in bootstrap)

    omitted_refresh = _envelope(run_id, 5, f"{GAMMA_MARKET_PREFIX}3257332", direct)
    assert processor.process_replayed(omitted_refresh).success
    carried = processor.reference_provenance(processor.current_bundles[0].market.market_id)
    assert carried is not None
    assert relationship.ingest_id in carried.source_ingest_ids

    changed_relationship = _envelope(
        run_id,
        6,
        f"{GAMMA_MARKET_RELATIONSHIP_PREFIX}3257332",
        _gamma_relationship(direct, event_ids=()),
    )
    changed_result = processor.process_replayed(changed_relationship)
    assert not changed_result.success
    assert changed_result.error_type == "NormalizationError"
    assert processor.discovered_markets[0].event_id == "780132"
    unchanged = processor.reference_provenance(processor.current_bundles[0].market.market_id)
    assert unchanged is not None
    assert relationship.ingest_id in unchanged.source_ingest_ids

    embedded_value = json.loads(direct)
    embedded_value["events"] = [{"id": 780132}]
    embedded_refresh = _envelope(
        run_id,
        7,
        f"{GAMMA_MARKET_PREFIX}3257332",
        json.dumps(embedded_value, separators=(",", ":")).encode(),
    )
    assert processor.process_replayed(embedded_refresh).success
    direct_provenance = processor.reference_provenance(
        processor.current_bundles[0].market.market_id
    )
    assert direct_provenance is not None
    assert relationship.ingest_id not in direct_provenance.source_ingest_ids
    assert changed_relationship.ingest_id not in direct_provenance.source_ingest_ids
    assert "gamma-market-relationship" not in dict(direct_provenance.attributes)[
        "reference_raw_surfaces"
    ].split(",")


@pytest.mark.parametrize(
    "identity_change",
    [
        {"conditionId": "changed-condition"},
        {"clobTokenIds": '["changed-yes","changed-no"]'},
        {"events": [{"id": 780133}]},
    ],
)
def test_direct_refresh_cannot_change_frozen_target_identity(
    identity_change: dict[str, object],
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-frozen-target-refresh")
    direct = _direct_gamma_without_events()
    relationship = _envelope(
        run_id,
        1,
        f"{GAMMA_MARKET_RELATIONSHIP_PREFIX}3257332",
        _gamma_relationship(direct),
    )
    assert processor.process_replayed(
        _envelope(run_id, 0, f"{GAMMA_MARKET_PREFIX}3257332", direct)
    ).success
    assert processor.process_replayed(relationship).success

    changed_value = json.loads(direct)
    changed_value.update(identity_change)
    changed = processor.process_replayed(
        _envelope(
            run_id,
            2,
            f"{GAMMA_MARKET_PREFIX}3257332",
            json.dumps(changed_value, separators=(",", ":")).encode(),
        )
    )

    assert not changed.success
    assert changed.error_type == "NormalizationError"
    assert len(processor.discovered_markets) == 1
    frozen = processor.discovered_markets[0]
    assert frozen.gamma_market_id == "3257332"
    assert frozen.condition_id == "0xcondition"
    assert frozen.clob_token_ids == ("token-yes", "token-no")
    assert frozen.event_id == "780132"


def test_new_market_without_reference_emits_exact_deterministic_refresh_request() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-new-market")
    envelope = _envelope(run_id, 0, WS_MARKET, _new_market_payload())
    live = CanonicalProcessor(config)
    replay = CanonicalProcessor(config)

    live_result = live.process_durable(envelope, _receipt(envelope))
    replay_result = replay.process_replayed(envelope)

    assert live_result == replay_result
    assert live_result.success
    assert live_result.canonical_event_count == 1
    assert len(live_result.reference_refresh_requests) == 1
    request = live_result.reference_refresh_requests[0]
    assert request.event_type is CanonicalEventType.NEW_MARKET
    assert request.market_id is None
    assert request.native_market_id == "99"
    assert request.native_condition_id == "new-condition-99"
    assert request.native_asset_ids == ("new-yes", "new-no")
    assert live.reference_refresh_requests == live_result.reference_refresh_requests
    assert live.summary() == replay.summary()


def test_websocket_payload_is_strictly_parsed_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-single-ws-parse")
    envelope = _envelope(run_id, 0, WS_MARKET, _new_market_payload())
    original = processor_module.parse_market_ws_message
    calls = 0

    def counted(payload: bytes) -> tuple[object, ...]:
        nonlocal calls
        calls += 1
        return original(payload)

    monkeypatch.setattr(processor_module, "parse_market_ws_message", counted)
    monkeypatch.setattr(normalization_module, "parse_market_ws_message", counted)

    result = CanonicalProcessor(config).process_replayed(envelope)

    assert result.success
    assert calls == 1


def test_scan_latency_observer_receives_one_sample_per_dirty_market_scan() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-scan-latency-observer")
    samples: list[int] = []
    processor = CanonicalProcessor(config, scan_latency_observer=samples.append)
    scan_count = 0

    for envelope in _bootstrap(run_id):
        scan_count += processor.process_replayed(envelope).scan_count

    assert scan_count > 0
    assert len(samples) == scan_count
    assert all(sample >= 0 for sample in samples)


@pytest.mark.parametrize(
    "error",
    [
        StorageFault("fixture storage failure", code="TEST_STORAGE_FAILURE"),
        ProgrammingInvariantViolation("fixture programming failure"),
    ],
)
def test_processor_propagates_storage_and_programming_faults(error: Exception) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"processor-propagation-{type(error).__name__}")
    envelope = _envelope(run_id, 0, WS_MARKET, _new_market_payload())
    processor = CanonicalProcessor(
        config,
        analytical_outputs=_FailingCanonicalOutputs(error),
    )

    with pytest.raises(type(error)):
        processor.process_replayed(envelope)


@pytest.mark.parametrize(
    ("channel", "payload", "error_type"),
    [
        (REST_BOOK_AUDIT, b"{}", "NormalizationError"),
        (CONTROL_BOOK_INVALIDATION, b"{}", "NormalizationError"),
        (
            CONTROL_BOOK_INVALIDATION,
            b'{ "reason": "DISCONNECT" }',
            "NormalizationError",
        ),
        (
            CONTROL_BOOK_INVALIDATION,
            b'{"reason":"NOT_A_REASON"}',
            "NormalizationError",
        ),
    ],
)
def test_malformed_audit_and_control_payloads_are_expected_data_faults(
    channel: str,
    payload: bytes,
    error_type: str,
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    envelope = _envelope(CollectorRunId("processor-malformed-control"), 0, channel, payload)

    result = processor.process_replayed(envelope)

    assert not result.success
    assert result.error_type == error_type
    assert processor.summary().parse_failures == 1


def test_rest_audit_counts_matches_and_invalidates_on_exact_mismatch() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-rest-audit")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    yes_instrument = processor.current_bundles[0].instrument_for_token("token-yes").instrument_id
    owner = next(item for item in processor.owners if item.instrument_id == yes_instrument)
    before = owner.snapshot()
    scans_before = processor.summary().scans

    matching_start, matching_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="matching-audit",
    )
    assert processor.process_replayed(matching_intent).success
    matching = _audit_response(
        run_id,
        5,
        matching_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
    )
    matching_result = processor.process_replayed(matching)
    matched = owner.snapshot()

    assert matching_result.success
    assert not matching_result.needs_resync
    assert len(matching_result.audit_outcomes) == 1
    assert matching_result.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert matched.generation == before.generation
    assert matched.bids == before.bids
    assert matched.asks == before.asks
    assert matching_result.scan_count == 1
    assert len(matching_result.book_freshness_updates) == 1
    assert matching_result.book_freshness_updates[0].instrument_id == yes_instrument
    assert matching_result.book_freshness_updates[0].received_monotonic == (
        matching.received_monotonic
    )
    assert matching_result.scan_triggers[0].reasons == (ScanTriggerReason.REST_AUDIT,)
    assert processor.summary().scans == scans_before + 1

    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    mismatch_start, mismatch_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id="mismatching-audit",
    )
    assert processor.process_replayed(mismatch_intent).success
    mismatching = _audit_response(
        run_id,
        7,
        mismatch_start,
        mismatched_payload,
        boundary=0,
    )
    mismatch_result = processor.process_replayed(mismatching)
    summary = processor.summary()

    assert mismatch_result.success
    assert mismatch_result.needs_resync
    assert mismatch_result.audit_outcomes[0].outcome is AuditOutcome.MISMATCH
    assert mismatch_result.scan_count == 1
    assert not mismatch_result.book_freshness_updates
    assert mismatch_result.scan_triggers[0].reasons == (ScanTriggerReason.REST_AUDIT,)
    assert owner.snapshot().health is BookHealth.INVALID
    assert owner.snapshot().invalidation_reason is InvalidationReason.REST_AUDIT_MISMATCH
    assert summary.rest_audits == 2
    assert summary.rest_audit_matches == 1
    assert summary.rest_audit_mismatches == 1
    assert summary.rest_audit_raced == 0
    assert summary.rest_audit_failed == 0
    assert summary.pending_rest_audits == 0
    assert summary.parse_failures == 0
    assert summary.scans == scans_before + 2


def test_rest_audit_fence_distinguishes_economic_race_from_last_trade() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-rest-audit-fence")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    race_start, race_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="economic-race",
        admitted_ordinal=0,
    )
    assert processor.process_replayed(race_intent).success
    economic_update = _envelope(
        run_id,
        5,
        WS_MARKET,
        _fixture("polymarket_ws_price_change.json"),
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    assert processor.process_replayed(economic_update).success
    raced = processor.process_replayed(
        _audit_response(
            run_id,
            6,
            race_start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=1,
        )
    )

    assert raced.success
    assert not raced.needs_resync
    assert raced.audit_outcomes[0].outcome is AuditOutcome.RACED

    trade_start, trade_intent = _audit_start(
        processor,
        run_id,
        7,
        request_id="diagnostic-only",
        admitted_ordinal=1,
    )
    assert processor.process_replayed(trade_intent).success
    trade = _envelope(
        run_id,
        8,
        WS_MARKET,
        _fixture("polymarket_ws_last_trade.json"),
        transport_metadata={"websocket_admission_ordinal": 2},
    )
    assert processor.process_replayed(trade).success
    matching_payload = (
        _fixture("polymarket_rest_book_yes.json")
        .replace(
            b'"price": "0.45", "size": "4"',
            b'"price": "0.45", "size": "7"',
        )
        .replace(
            b',\n    {"price": "0.55", "size": "3"}',
            b"",
        )
    )
    matched = processor.process_replayed(
        _audit_response(
            run_id,
            9,
            trade_start,
            matching_payload,
            boundary=2,
        )
    )

    assert matched.success
    assert not matched.needs_resync
    assert matched.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert processor.summary().rest_audit_raced == 1
    assert processor.summary().rest_audit_matches == 1


def test_rest_audit_fence_ignores_noop_but_detects_change_then_restore() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-rest-audit-economic-generation")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    noop_start, noop_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="economic-noop",
        admitted_ordinal=0,
    )
    assert processor.process_replayed(noop_intent).success
    noop = _envelope(
        run_id,
        5,
        WS_MARKET,
        _price_change_payload(bid_size="4", timestamp="1787198403000"),
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    assert processor.process_replayed(noop).success
    matched = processor.process_replayed(
        _audit_response(
            run_id,
            6,
            noop_start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=1,
        )
    )

    assert matched.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert matched.audit_outcomes[0].barrier_book is not None
    assert (
        matched.audit_outcomes[0].starting_book.generation
        == matched.audit_outcomes[0].barrier_book.generation
    )

    reverted_start, reverted_intent = _audit_start(
        processor,
        run_id,
        7,
        request_id="economic-change-then-restore",
        admitted_ordinal=1,
    )
    assert processor.process_replayed(reverted_intent).success
    changed = _envelope(
        run_id,
        8,
        WS_MARKET,
        _fixture("polymarket_ws_price_change.json"),
        transport_metadata={"websocket_admission_ordinal": 2},
    )
    restored = _envelope(
        run_id,
        9,
        WS_MARKET,
        _price_change_payload(
            bid_size="4",
            ask_size="3",
            timestamp="1787198404000",
        ),
        transport_metadata={"websocket_admission_ordinal": 3},
    )
    assert processor.process_replayed(changed).success
    assert processor.process_replayed(restored).success
    raced = processor.process_replayed(
        _audit_response(
            run_id,
            10,
            reverted_start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=3,
        )
    )

    outcome = raced.audit_outcomes[0]
    assert outcome.outcome is AuditOutcome.RACED
    assert outcome.barrier_book is not None
    assert outcome.starting_book.economic_content_sha256 == (
        outcome.barrier_book.economic_content_sha256
    )
    assert outcome.barrier_book.generation == outcome.starting_book.generation + 2


def test_rest_audit_failure_blocks_eligibility_without_forcing_socket_resync() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-rest-audit-fail-closed")
    ambiguous = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert ambiguous.process_replayed(envelope).success
    start, intent = _audit_start(
        ambiguous,
        run_id,
        4,
        request_id="ambiguous-watermark",
    )
    assert ambiguous.process_replayed(intent).success

    failed = ambiguous.process_replayed(
        _audit_response(
            run_id,
            5,
            start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=0,
            barrier=1,
        )
    )

    assert failed.success
    assert not failed.needs_resync
    assert failed.audit_outcomes[0].outcome is AuditOutcome.FAILED
    assert failed.audit_outcomes[0].reason is AuditFailureReason.WATERMARK_AMBIGUOUS
    owner = next(
        item for item in ambiguous.owners if item.instrument_id == start.starting_book.instrument_id
    )
    assert owner.health is BookHealth.HEALTHY
    assert ambiguous.audit_ineligible_instruments == (owner.instrument_id,)
    assert ambiguous.scan_results[-1].disposition is ScanDisposition.BLOCKED
    assert ambiguous.summary().audit_ineligible_books == 1

    explicit = CanonicalProcessor(config)
    explicit_run = CollectorRunId("processor-rest-audit-explicit-failure")
    for envelope in _bootstrap(explicit_run):
        assert explicit.process_replayed(envelope).success
    explicit_start, explicit_intent = _audit_start(
        explicit,
        explicit_run,
        4,
        request_id="http-failed",
    )
    assert explicit.process_replayed(explicit_intent).success
    failure = _envelope(
        explicit_run,
        5,
        CONTROL_REST_AUDIT_FAILED,
        AuditFailureCommand(
            request_id=explicit_start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        ).to_payload(),
        connection_id=explicit_start.connection_id,
    )
    explicit_result = explicit.process_replayed(failure)

    assert explicit_result.success
    assert not explicit_result.needs_resync
    assert explicit_result.audit_outcomes[0].reason is AuditFailureReason.HTTP_REQUEST_FAILED
    assert explicit.summary().rest_audit_failed == 1
    assert explicit.summary().audit_ineligible_books == 1

    recovery_start, recovery_intent = _audit_start(
        explicit,
        explicit_run,
        6,
        request_id="http-recovered",
    )
    assert explicit.process_replayed(recovery_intent).success
    recovered = explicit.process_replayed(
        _audit_response(
            explicit_run,
            7,
            recovery_start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=0,
        )
    )

    assert recovered.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert explicit.summary().audit_ineligible_books == 0


def test_scan_dirty_is_coalesced_and_last_trade_is_diagnostic_only() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-scan-dirty")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    yes_instrument = processor.current_bundles[0].instrument_for_token("token-yes").instrument_id
    owner = next(item for item in processor.owners if item.instrument_id == yes_instrument)
    scans_before = processor.summary().scans

    update = _envelope(
        run_id,
        4,
        WS_MARKET,
        _fixture("polymarket_ws_price_change.json"),
    )
    updated = processor.process_replayed(update)

    assert updated.success
    assert updated.canonical_event_count == 1
    assert updated.scan_count == 1
    assert updated.scan_triggers[0].reasons == (ScanTriggerReason.BOOK_UPDATE,)
    assert len(updated.book_freshness_updates) == 1
    assert updated.book_freshness_updates[0].instrument_id == yes_instrument
    assert updated.book_freshness_updates[0].received_monotonic == update.received_monotonic

    economic_hash = owner.economic_content_sha256()
    l2_source_event_id = owner.snapshot().last_source_event_id
    l2_provenance = owner.snapshot().state_source_provenance
    diagnostic_hash = owner.content_sha256()
    trade = _envelope(
        run_id,
        5,
        WS_MARKET,
        _fixture("polymarket_ws_last_trade.json"),
    )
    traded = processor.process_replayed(trade)

    assert traded.success
    assert traded.canonical_event_count == 1
    assert traded.scan_count == 0
    assert not traded.scan_triggers
    assert not traded.book_freshness_updates
    assert processor.summary().scans == scans_before + 1
    assert owner.economic_content_sha256() == economic_hash
    assert owner.snapshot().last_source_event_id == l2_source_event_id
    assert owner.snapshot().state_source_provenance == l2_provenance
    assert owner.content_sha256() != diagnostic_hash


def test_ws_l2_deadline_equality_is_accepted_and_strict_overrun_requires_rest() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    maximum_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    update_payload = _fixture("polymarket_ws_price_change.json")

    equality_run = CollectorRunId("processor-l2-deadline-equality")
    equality = CanonicalProcessor(config)
    for envelope in _bootstrap(equality_run):
        assert equality.process_replayed(envelope).success
    at_deadline = _envelope(
        equality_run,
        4,
        WS_MARKET,
        update_payload,
        monotonic_ns=102 + maximum_age_ns,
    )
    accepted = equality.process_replayed(at_deadline)

    assert accepted.success
    assert accepted.scan_count == 1
    assert len(accepted.book_freshness_updates) == 1
    assert not accepted.needs_resync

    overrun_run = CollectorRunId("processor-l2-deadline-overrun")
    overrun = CanonicalProcessor(config)
    for envelope in _bootstrap(overrun_run):
        assert overrun.process_replayed(envelope).success
    after_deadline = _envelope(
        overrun_run,
        4,
        WS_MARKET,
        update_payload,
        monotonic_ns=102 + maximum_age_ns + 1,
    )
    rejected = overrun.process_replayed(after_deadline)
    yes_instrument = overrun.current_bundles[0].instrument_for_token("token-yes").instrument_id
    yes_owner = next(item for item in overrun.owners if item.instrument_id == yes_instrument)

    assert rejected.success
    assert rejected.scan_count == 1
    assert rejected.needs_resync
    assert not rejected.book_freshness_updates
    assert rejected.scan_triggers[0].reasons == (ScanTriggerReason.INVALIDATION,)
    assert yes_owner.snapshot().health is BookHealth.INVALID
    assert yes_owner.snapshot().invalidation_reason is InvalidationReason.STALE_SNAPSHOT

    rest = _envelope(
        overrun_run,
        5,
        REST_BOOK,
        _fixture("polymarket_rest_book_yes.json"),
        monotonic_ns=102 + maximum_age_ns + 2,
    )
    restored = overrun.process_replayed(rest)

    assert restored.success
    assert len(restored.book_freshness_updates) == 1
    assert restored.book_freshness_updates[0].received_monotonic == rest.received_monotonic
    assert yes_owner.snapshot().health is BookHealth.HEALTHY


def test_control_and_audit_channels_have_identical_live_and_replay_results() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-control-replay")
    live = CanonicalProcessor(config)
    replay = CanonicalProcessor(config)

    for envelope in _bootstrap(run_id):
        live_result = live.process_durable(envelope, _receipt(envelope))
        replay_result = replay.process_replayed(envelope)
        assert live_result == replay_result
    start, intent = _audit_start(
        live,
        run_id,
        4,
        request_id="live-replay-audit",
    )
    inputs = (
        intent,
        _audit_response(
            run_id,
            5,
            start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=0,
        ),
        _envelope(
            run_id,
            6,
            CONTROL_BOOK_INVALIDATION,
            canonical_json_bytes({"reason": InvalidationReason.DISCONNECT.value}),
        ),
    )
    for envelope in inputs:
        live_result = live.process_durable(envelope, _receipt(envelope))
        replay_result = replay.process_replayed(envelope)
        assert live_result == replay_result

    assert all(owner.health is BookHealth.INVALID for owner in live.owners)
    assert all(
        owner.snapshot().invalidation_reason is InvalidationReason.DISCONNECT
        for owner in live.owners
    )
    assert live.summary() == replay.summary()


def test_dynamic_tick_publication_persists_provenance_and_invalidates_all_owners() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    outputs = _RecordingOutputs()
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    run_id = CollectorRunId("processor-dynamic-tick")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    updates_before = processor.summary().reference_updates
    persisted_before = len(outputs.references)
    tick_envelope = _envelope(
        run_id,
        4,
        WS_MARKET,
        _fixture("polymarket_ws_tick_change.json"),
    )

    result = processor.process_replayed(tick_envelope)
    bundle = processor.current_bundles[0]
    provenance = processor.reference_provenance(bundle.market.market_id)

    assert result.success
    assert result.needs_resync
    assert len(result.reference_refresh_requests) == 1
    assert result.reference_refresh_requests[0].event_type is CanonicalEventType.TICK_SIZE_CHANGE
    assert result.reference_refresh_requests[0].market_id == bundle.market.market_id
    assert bundle.precision.tick_size is not None
    assert bundle.precision.tick_size.to_canonical_string() == "0.001"
    assert all(owner.health is BookHealth.INVALID for owner in processor.owners)
    assert processor.summary().reference_updates == updates_before + 1
    assert len(outputs.references) == persisted_before + 1
    assert outputs.references[-1][0] == bundle
    assert provenance == outputs.references[-1][2]
    assert provenance is not None
    assert tick_envelope.ingest_id in provenance.source_ingest_ids
    assert tick_envelope.payload_sha256 in provenance.source_payload_hashes
    assert dict(provenance.attributes)["dynamic_reference_event_id"]


def test_processor_history_retention_is_bounded_without_losing_exact_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(processor_module, "_RECENT_HISTORY_LIMIT", 2)
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-bounded-history")
    inputs = (
        *_bootstrap(run_id),
        _envelope(
            run_id,
            4,
            WS_MARKET,
            _fixture("polymarket_ws_initial_book.json"),
        ),
        _envelope(
            run_id,
            5,
            WS_MARKET,
            _fixture("polymarket_ws_last_trade.json"),
        ),
        _envelope(run_id, 6, WS_MARKET, _new_market_payload("100")),
        _envelope(run_id, 7, WS_MARKET, _new_market_payload("101")),
        _envelope(run_id, 8, WS_MARKET, _new_market_payload("102")),
    )
    first = CanonicalProcessor(config)
    second = CanonicalProcessor(config)

    for envelope in inputs:
        first.process_replayed(envelope)
        second.process_replayed(envelope)

    summary = first.summary()
    assert summary == second.summary()
    assert summary.scans > len(first.scan_results) == 2
    assert summary.scan_results_truncated
    assert summary.reference_refresh_requests == 3
    assert len(first.reference_refresh_requests) == 2
    assert summary.reference_refresh_requests_truncated
    assert len(summary.raw_input_chain_sha256) == 64
    assert len(summary.canonical_event_chain_sha256) == 64
    assert len(summary.scan_chain_sha256) == 64
