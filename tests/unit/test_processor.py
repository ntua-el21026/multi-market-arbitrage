from __future__ import annotations

import hashlib
import json
from collections import deque
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.errors import ProgrammingInvariantViolation, StorageFault
from multi_market_arbitrage.core.health import BookHealth
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    InstrumentId,
    SubscriptionId,
)
from multi_market_arbitrage.core.provenance import Provenance, RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalDurabilityState, JournalReceipt
from multi_market_arbitrage.normalization import (
    CanonicalEvent,
    CanonicalEventType,
    NormalizationError,
)
from multi_market_arbitrage.normalization import polymarket as normalization_module
from multi_market_arbitrage.observability import AvailabilityEvidenceRuntimeSnapshot
from multi_market_arbitrage.pipeline import (
    AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REST_AUDIT_CLASSIFY,
    CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
    CONTROL_REST_AUDIT_EXPIRED,
    CONTROL_REST_AUDIT_FAILED,
    CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
    CONTROL_REST_AUDIT_START,
    GAMMA_EVENT_PREFIX,
    GAMMA_MARKET_PREFIX,
    GAMMA_MARKET_RELATIONSHIP_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditClassificationCommand,
    AuditClassificationPrepareCommand,
    AuditExpiryCommand,
    AuditFailureCommand,
    AuditFailureReason,
    AuditHttpFailureHandoff,
    AuditMismatchCause,
    AuditOutcome,
    AuditStartEvidence,
    CanonicalProcessor,
    ProcessedEnvelope,
    ScanTriggerReason,
    WebSocketAdmissionWatermark,
)
from multi_market_arbitrage.pipeline import processor as processor_module
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_CLASSIFICATION_V1_SCHEMA,
    AUDIT_CLASSIFICATION_V2_SCHEMA,
    AUDIT_FAILURE_V1_SCHEMA,
    AUDIT_FAILURE_V2_SCHEMA,
    AUDIT_REPLACEMENT_CAPTURE_MAX_BYTES,
    AUDIT_START_V1_SCHEMA,
    AUDIT_START_V2_SCHEMA,
    AuditDivergencePending,
    AuditFaultFailureEvidenceKind,
    AuditFaultLifecycleRecord,
    AuditFaultOrigin,
    AuditFaultStatus,
    AuditOutcomeEvidence,
    AuditReplacementReleaseCommand,
    AuditTriggerReason,
)
from multi_market_arbitrage.pipeline.health import (
    ObservationRuntimeHealth,
    audit_availability_authority_reconciled,
)
from multi_market_arbitrage.pipeline.processor import (
    ProcessorRecoveryReason,
    ProcessorRetentionDomain,
    processor_recovery_reason_for_invalidation,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID, ReferenceBundle
from multi_market_arbitrage.research import Phase1AnalyticalOutputs
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScanResult,
    OpportunityLifecycle,
    ScanDisposition,
)
from multi_market_arbitrage.state import InvalidationReason
from multi_market_arbitrage.venues.polymarket.errors import (
    MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS,
    MAXIMUM_RETRY_AFTER_EVIDENCE_SECONDS,
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketHttpStatusError,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
)

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


def _websocket_book_payload(rest_payload: bytes, *, label: str) -> bytes:
    rest = cast(dict[str, object], json.loads(rest_payload))
    return canonical_json_bytes(
        {
            "asks": rest["asks"],
            "asset_id": rest["asset_id"],
            "bids": rest["bids"],
            "event_type": "book",
            "hash": f"processor-replacement-{label}",
            "market": rest["market"],
            "timestamp": rest["timestamp"],
        }
    )


def _best_bid_ask_payload(
    *,
    best_bid: str,
    best_ask: str,
    spread: str,
    timestamp: str,
) -> bytes:
    return canonical_json_bytes(
        {
            "asset_id": "token-yes",
            "best_ask": best_ask,
            "best_bid": best_bid,
            "event_type": "best_bid_ask",
            "market": "0xcondition",
            "spread": spread,
            "timestamp": timestamp,
        }
    )


def _top_bid_change_payload(*, timestamp: str) -> bytes:
    return canonical_json_bytes(
        {
            "event_type": "price_change",
            "market": "0xcondition",
            "price_changes": [
                {
                    "asset_id": "token-yes",
                    "best_ask": "0.55",
                    "best_bid": "0.46",
                    "hash": f"new-top-{timestamp}",
                    "price": "0.46",
                    "side": "BUY",
                    "size": "5",
                }
            ],
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
    hard_expiry_monotonic_ns: int | None = None,
    response_deadline_monotonic_ns: int | None = None,
    run_deadline_monotonic_ns: int | None = None,
    classification_protocol: str | None = None,
    request_started_monotonic_ns: int | None = None,
    trigger_reason: AuditTriggerReason = AuditTriggerReason.OTHER_TYPED_REASON,
    schema: str = AUDIT_START_V2_SCHEMA,
) -> tuple[AuditStartEvidence, RawEventEnvelope]:
    connection_id = ConnectionId("processor-test-connection")
    processed = processor.processed_websocket_watermark(connection_id)
    start = AuditStartEvidence(
        request_id=request_id,
        connection_id=connection_id,
        native_asset_id=native_asset_id,
        attempt=attempt,
        request_started_monotonic_ns=(
            100 + sequence if request_started_monotonic_ns is None else request_started_monotonic_ns
        ),
        starting_admission_watermark=WebSocketAdmissionWatermark(
            connection_id=connection_id,
            ordinal=processed.ordinal if admitted_ordinal is None else admitted_ordinal,
        ),
        starting_processed_watermark=processed,
        starting_book=processor.audit_book_identity(native_asset_id),
        trigger_reason=trigger_reason,
        schema=schema,
    )
    transport_metadata: dict[str, str | int | bool] | None = None
    if hard_expiry_monotonic_ns is not None:
        transport_metadata = {
            "hard_expiry_monotonic_ns": hard_expiry_monotonic_ns,
            "response_deadline_monotonic_ns": (
                hard_expiry_monotonic_ns
                if response_deadline_monotonic_ns is None
                else response_deadline_monotonic_ns
            ),
            "run_deadline_monotonic_ns": (
                (
                    hard_expiry_monotonic_ns
                    if response_deadline_monotonic_ns is None
                    else response_deadline_monotonic_ns
                )
                if run_deadline_monotonic_ns is None
                else run_deadline_monotonic_ns
            ),
        }
    if classification_protocol is not None:
        if transport_metadata is None:
            transport_metadata = {}
        transport_metadata["classification_protocol"] = classification_protocol
    return start, _envelope(
        run_id,
        sequence,
        CONTROL_REST_AUDIT_START,
        start.to_payload(),
        connection_id=connection_id,
        monotonic_ns=start.request_started_monotonic_ns,
        transport_metadata=transport_metadata,
    )


def _historical_direct_processor(config: Phase1Config) -> CanonicalProcessor:
    return CanonicalProcessor(
        config,
        source_journal_git_commit="ee240c51184cccfc6e425906097758b5ca019a95",
    )


def _audit_response(
    run_id: CollectorRunId,
    sequence: int,
    start: AuditStartEvidence,
    payload: bytes,
    *,
    boundary: int,
    barrier: int | None = None,
    classification_deferred: bool | str | None = None,
) -> RawEventEnvelope:
    metadata: dict[str, str | int | bool] = {
        "audit_attempt": start.attempt,
        "barrier_websocket_ordinal": boundary if barrier is None else barrier,
        "request_id": start.request_id,
        "response_boundary_websocket_ordinal": boundary,
    }
    if classification_deferred is not None:
        metadata["classification_deferred"] = classification_deferred
    return _envelope(
        run_id,
        sequence,
        REST_BOOK_AUDIT,
        payload,
        connection_id=start.connection_id,
        transport_metadata=metadata,
    )


def _audit_classification_prepare(
    run_id: CollectorRunId,
    sequence: int,
    start: AuditStartEvidence,
    response: RawEventEnvelope,
    *,
    hard_expiry_monotonic_ns: int,
    monotonic_ns: int,
) -> tuple[AuditClassificationPrepareCommand, RawEventEnvelope]:
    command = AuditClassificationPrepareCommand(
        request_id=start.request_id,
        response_ingest_seq=response.ingest_seq,
        hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
    )
    return command, _envelope(
        run_id,
        sequence,
        CONTROL_REST_AUDIT_CLASSIFY_PREPARE,
        command.to_payload(),
        monotonic_ns=monotonic_ns,
        connection_id=start.connection_id,
        transport_metadata={"request_id": start.request_id},
    )


def _audit_classification_attestation(
    run_id: CollectorRunId,
    sequence: int,
    start: AuditStartEvidence,
    response: RawEventEnvelope,
    preparation: RawEventEnvelope,
    *,
    hard_expiry_monotonic_ns: int,
    durable_monotonic_ns: int,
    processed_monotonic_ns: int,
    attestation_monotonic_ns: int | None = None,
    schema: str = AUDIT_CLASSIFICATION_V2_SCHEMA,
) -> tuple[AuditClassificationCommand, RawEventEnvelope]:
    command = AuditClassificationCommand(
        request_id=start.request_id,
        response_ingest_seq=response.ingest_seq,
        classification_monotonic_ns=processed_monotonic_ns,
        hard_expiry_monotonic_ns=hard_expiry_monotonic_ns,
        schema=schema,
    )
    return command, _envelope(
        run_id,
        sequence,
        CONTROL_REST_AUDIT_CLASSIFY,
        command.to_payload(),
        monotonic_ns=(
            processed_monotonic_ns if attestation_monotonic_ns is None else attestation_monotonic_ns
        ),
        connection_id=start.connection_id,
        transport_metadata={
            "classification_prepare_durable_monotonic_ns": durable_monotonic_ns,
            "classification_prepare_ingest_id": preparation.ingest_id.value,
            "classification_prepare_payload_sha256": preparation.payload_sha256,
            "classification_prepare_processed_monotonic_ns": processed_monotonic_ns,
            "request_id": start.request_id,
        },
    )


def _receipt(envelope: RawEventEnvelope) -> JournalReceipt:
    return JournalReceipt(
        ingest_id=envelope.ingest_id,
        collector_run_id=envelope.collector_run_id,
        ingest_seq=envelope.ingest_seq,
        state=JournalDurabilityState.DURABLE,
    )


def _bootstrap_hard_expiry_ns(
    config: Phase1Config,
    native_asset_id: str = "token-yes",
    *,
    freshness_ns: int | None = None,
) -> int:
    if freshness_ns is None:
        freshness_ns = 102 if native_asset_id == "token-yes" else 103
    return freshness_ns + int(config.scanner.maximum_book_age_seconds * 1_000_000_000)


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


def _stage_prepare_attest_audit(
    processor: CanonicalProcessor,
    config: Phase1Config,
    run_id: CollectorRunId,
    *,
    request_id: str,
) -> tuple[int, AuditStartEvidence, RawEventEnvelope, RawEventEnvelope]:
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=request_id,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    staged = processor.process_replayed(response)
    assert staged.success
    assert staged.audit_outcomes == ()
    _, preparation = _audit_classification_prepare(
        run_id,
        6,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=200,
    )
    prepared = processor.process_replayed(preparation)
    assert prepared.success
    assert prepared.audit_outcomes == ()
    return hard_expiry_ns, start, response, preparation


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


def test_every_invalidation_reason_has_one_exact_recovery_taxonomy_mapping() -> None:
    expected = {
        InvalidationReason.DISCONNECT: ProcessorRecoveryReason.TRANSPORT_DISCONNECT,
        InvalidationReason.SCHEMA_VIOLATION: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
        InvalidationReason.INVALID_PRICE: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
        InvalidationReason.INVALID_QUANTITY: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
        InvalidationReason.UNKNOWN_PRECISION: ProcessorRecoveryReason.REFERENCE_REFRESH,
        InvalidationReason.UNKNOWN_REFERENCE_DATA: ProcessorRecoveryReason.REFERENCE_REFRESH,
        InvalidationReason.BACKPRESSURE_DATA_LOSS: ProcessorRecoveryReason.BACKPRESSURE_RESOURCE,
        InvalidationReason.STALE_SNAPSHOT: ProcessorRecoveryReason.FRESHNESS_EXPIRY,
        InvalidationReason.IMPOSSIBLE_STATE: (ProcessorRecoveryReason.CANONICAL_STATE_INVALIDATION),
        InvalidationReason.PROTOCOL_VIOLATION: ProcessorRecoveryReason.SCHEMA_PROTOCOL_ERROR,
        InvalidationReason.REST_AUDIT_MISMATCH: (ProcessorRecoveryReason.AUDIT_MISMATCH_RECOVERY),
        InvalidationReason.MARKET_RESOLVED_REFERENCE_REFRESH: (
            ProcessorRecoveryReason.TARGET_LIFECYCLE
        ),
    }

    assert set(expected) == set(InvalidationReason)
    assert {
        reason: processor_recovery_reason_for_invalidation(reason) for reason in InvalidationReason
    } == expected
    assert processor_recovery_reason_for_invalidation(None) is ProcessorRecoveryReason.UNKNOWN


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
    assert request.recovery_reason is ProcessorRecoveryReason.REFERENCE_REFRESH
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


@pytest.mark.parametrize("deferred_marker", [None, False, "true"])
def test_bound_audit_start_rejects_legacy_or_invalid_deferred_response(
    deferred_marker: bool | str | None,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = _historical_direct_processor(config)
    run_id = CollectorRunId(f"processor-bound-audit-mode-{deferred_marker!s}")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="bound-audit",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert processor.process_replayed(intent).success

    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=deferred_marker,
    )
    result = processor.process_replayed(response)

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1


def test_bound_audit_start_rejects_forged_expiry_and_missing_freshness() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-bound-audit-forged-expiry")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    true_expiry_ns = _bootstrap_hard_expiry_ns(config)
    forged_expiry_ns = true_expiry_ns + int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    forged_start, forged_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="forged-future-expiry",
        hard_expiry_monotonic_ns=forged_expiry_ns,
        response_deadline_monotonic_ns=900,
    )

    rejected_start = processor.process_replayed(forged_intent)
    assert not rejected_start.success
    assert rejected_start.error_type == "NormalizationError"
    forged_response = _audit_response(
        run_id,
        5,
        forged_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert not processor.process_replayed(forged_response).success
    forged_classification = AuditClassificationCommand(
        request_id=forged_start.request_id,
        response_ingest_seq=forged_response.ingest_seq,
        classification_monotonic_ns=true_expiry_ns + 1,
        hard_expiry_monotonic_ns=forged_expiry_ns,
    )
    rejected_classification = processor.process_replayed(
        _envelope(
            run_id,
            6,
            CONTROL_REST_AUDIT_CLASSIFY,
            forged_classification.to_payload(),
            monotonic_ns=forged_classification.classification_monotonic_ns,
            connection_id=forged_start.connection_id,
        )
    )
    assert not rejected_classification.success
    assert not processor.audit_outcomes
    assert processor.summary().pending_rest_audits == 0

    no_freshness_run = CollectorRunId("processor-bound-audit-no-freshness")
    no_freshness = CanonicalProcessor(config)
    for envelope in _bootstrap(no_freshness_run)[:2]:
        assert no_freshness.process_replayed(envelope).success
    _, no_freshness_intent = _audit_start(
        no_freshness,
        no_freshness_run,
        2,
        request_id="no-accepted-freshness",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    rejected_missing_freshness = no_freshness.process_replayed(no_freshness_intent)
    assert not rejected_missing_freshness.success
    assert rejected_missing_freshness.error_type == "NormalizationError"
    assert no_freshness.summary().pending_rest_audits == 0


def test_bound_audit_start_cannot_precede_accepted_freshness() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-bound-audit-start-before-freshness")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="start-before-accepted-freshness",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
        request_started_monotonic_ns=50,
    )
    rejected = processor.process_replayed(intent)
    assert not rejected.success
    assert rejected.error_type == "NormalizationError"

    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert not processor.process_replayed(response).success
    classification = AuditClassificationCommand(
        request_id=start.request_id,
        response_ingest_seq=response.ingest_seq,
        classification_monotonic_ns=200,
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
    )
    assert not processor.process_replayed(
        _envelope(
            run_id,
            6,
            CONTROL_REST_AUDIT_CLASSIFY,
            classification.to_payload(),
            monotonic_ns=classification.classification_monotonic_ns,
            connection_id=start.connection_id,
        )
    ).success
    assert not processor.audit_outcomes
    assert processor.summary().pending_rest_audits == 0


def test_bound_audit_start_rejects_forged_run_cutoff_extension() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-bound-audit-forged-run-cutoff")
    processor = _historical_direct_processor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    yes_expiry_ns = _bootstrap_hard_expiry_ns(config, "token-yes")
    _, valid_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="bind-real-run-cutoff",
        native_asset_id="token-yes",
        hard_expiry_monotonic_ns=yes_expiry_ns,
        response_deadline_monotonic_ns=900,
        run_deadline_monotonic_ns=900,
    )
    assert processor.process_replayed(valid_intent).success

    no_expiry_ns = _bootstrap_hard_expiry_ns(config, "token-no")
    _, inconsistent_response_intent = _audit_start(
        processor,
        run_id,
        5,
        request_id="extend-only-response-cutoff",
        native_asset_id="token-no",
        hard_expiry_monotonic_ns=no_expiry_ns,
        response_deadline_monotonic_ns=no_expiry_ns,
        run_deadline_monotonic_ns=900,
    )
    assert not processor.process_replayed(inconsistent_response_intent).success

    _, forged_run_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id="extend-response-and-run-cutoff",
        native_asset_id="token-no",
        hard_expiry_monotonic_ns=no_expiry_ns,
        response_deadline_monotonic_ns=no_expiry_ns,
        run_deadline_monotonic_ns=no_expiry_ns,
    )
    rejected = processor.process_replayed(forged_run_intent)
    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1
    assert not processor.audit_outcomes

    next_epoch_run = CollectorRunId("processor-bound-audit-next-monotonic-epoch")
    next_epoch_no_book = _envelope(next_epoch_run, 5, REST_BOOK, _no_book())
    assert processor.process_replayed(next_epoch_no_book).success
    _, next_epoch_intent = _audit_start(
        processor,
        next_epoch_run,
        6,
        request_id="reuse-numeric-cutoff-in-next-epoch",
        native_asset_id="token-no",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(
            config,
            "token-no",
            freshness_ns=next_epoch_no_book.received_monotonic.nanoseconds,
        ),
        response_deadline_monotonic_ns=900,
        run_deadline_monotonic_ns=900,
    )
    rejected_epoch = processor.process_replayed(next_epoch_intent)
    assert not rejected_epoch.success
    assert rejected_epoch.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1


def test_audit_start_protocol_mode_cannot_upgrade_or_downgrade_mid_run() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))

    bound_run = CollectorRunId("processor-audit-mode-bound-first")
    bound_first = _historical_direct_processor(config)
    for envelope in _bootstrap(bound_run):
        assert bound_first.process_replayed(envelope).success
    bound_start, bound_intent = _audit_start(
        bound_first,
        bound_run,
        4,
        request_id="bound-before-downgrade",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert bound_first.process_replayed(bound_intent).success
    bound_failure = AuditFailureCommand(
        request_id=bound_start.request_id,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
    )
    assert bound_first.process_replayed(
        _envelope(
            bound_run,
            5,
            CONTROL_REST_AUDIT_FAILED,
            bound_failure.to_payload(),
            connection_id=bound_start.connection_id,
        )
    ).success
    _, legacy_downgrade = _audit_start(
        bound_first,
        bound_run,
        6,
        request_id="legacy-after-bound",
    )
    assert not bound_first.process_replayed(legacy_downgrade).success
    assert bound_first.summary().pending_rest_audits == 0

    legacy_run = CollectorRunId("processor-audit-mode-legacy-first")
    legacy_first = CanonicalProcessor(config)
    for envelope in _bootstrap(legacy_run):
        assert legacy_first.process_replayed(envelope).success
    legacy_start, legacy_intent = _audit_start(
        legacy_first,
        legacy_run,
        4,
        request_id="legacy-before-upgrade",
    )
    assert legacy_first.process_replayed(legacy_intent).success
    legacy_failure = AuditFailureCommand(
        request_id=legacy_start.request_id,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
    )
    assert legacy_first.process_replayed(
        _envelope(
            legacy_run,
            5,
            CONTROL_REST_AUDIT_FAILED,
            legacy_failure.to_payload(),
            connection_id=legacy_start.connection_id,
        )
    ).success
    _, bound_upgrade = _audit_start(
        legacy_first,
        legacy_run,
        6,
        request_id="bound-after-legacy",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert not legacy_first.process_replayed(bound_upgrade).success
    assert legacy_first.summary().pending_rest_audits == 0


def test_current_source_requires_explicit_prepare_attest_selector() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)

    omitted_run = CollectorRunId("processor-current-source-selector-omitted")
    omitted_processor = CanonicalProcessor(
        config,
        source_journal_git_commit=None,
    )
    for envelope in _bootstrap(omitted_run):
        assert omitted_processor.process_replayed(envelope).success
    _, omitted = _audit_start(
        omitted_processor,
        omitted_run,
        4,
        request_id="current-source-omitted-selector",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
    )
    rejected = omitted_processor.process_replayed(omitted)
    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert omitted_processor.summary().pending_rest_audits == 0

    selected_run = CollectorRunId("processor-current-source-selector-present")
    selected_processor = CanonicalProcessor(
        config,
        source_journal_git_commit="2222222222222222222222222222222222222222",
    )
    for envelope in _bootstrap(selected_run):
        assert selected_processor.process_replayed(envelope).success
    _, selected = _audit_start(
        selected_processor,
        selected_run,
        4,
        request_id="current-source-explicit-selector",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    assert selected_processor.process_replayed(selected).success
    assert selected_processor.summary().pending_rest_audits == 1


def test_known_historical_source_retains_direct_deferred_compatibility() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-historical-direct-deferred-source")
    processor = CanonicalProcessor(
        config,
        source_journal_git_commit="ee240c51184cccfc6e425906097758b5ca019a95",
    )
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    _, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="historical-direct-deferred",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert processor.process_replayed(intent).success
    assert processor.summary().pending_rest_audits == 1


def test_deferred_audit_response_requires_bound_start_and_predeadline_receipt() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-deferred-audit-mode")
    legacy = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert legacy.process_replayed(envelope).success
    legacy_start, legacy_intent = _audit_start(
        legacy,
        run_id,
        4,
        request_id="legacy-audit",
    )
    assert legacy.process_replayed(legacy_intent).success
    deferred = _audit_response(
        run_id,
        5,
        legacy_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert not legacy.process_replayed(deferred).success
    assert legacy.summary().pending_rest_audits == 1

    bound = _historical_direct_processor(config)
    for envelope in _bootstrap(run_id):
        assert bound.process_replayed(envelope).success
    bound_start, bound_intent = _audit_start(
        bound,
        run_id,
        4,
        request_id="deadline-bound-audit",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=105,
    )
    assert bound.process_replayed(bound_intent).success
    at_deadline = _audit_response(
        run_id,
        5,
        bound_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert at_deadline.received_monotonic.nanoseconds == 105
    assert not bound.process_replayed(at_deadline).success
    assert bound.summary().pending_rest_audits == 1


def test_deferred_protocol_rejects_cross_epoch_response_and_classification() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    start_run = CollectorRunId("processor-deferred-epoch-start")
    response_run = CollectorRunId("processor-deferred-epoch-response")

    response_epoch = _historical_direct_processor(config)
    for envelope in _bootstrap(start_run):
        assert response_epoch.process_replayed(envelope).success
    response_start, response_intent = _audit_start(
        response_epoch,
        start_run,
        4,
        request_id="cross-epoch-response",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert response_epoch.process_replayed(response_intent).success
    cross_epoch_response = _audit_response(
        response_run,
        5,
        response_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    rejected_response = response_epoch.process_replayed(cross_epoch_response)
    assert not rejected_response.success
    assert rejected_response.error_type == "NormalizationError"
    assert not response_epoch.audit_outcomes

    classification_epoch = _historical_direct_processor(config)
    for envelope in _bootstrap(start_run):
        assert classification_epoch.process_replayed(envelope).success
    classification_start, classification_intent = _audit_start(
        classification_epoch,
        start_run,
        4,
        request_id="cross-epoch-classification",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert classification_epoch.process_replayed(classification_intent).success
    staged_response = _audit_response(
        start_run,
        5,
        classification_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert classification_epoch.process_replayed(staged_response).success
    cross_epoch_classification = AuditClassificationCommand(
        request_id=classification_start.request_id,
        response_ingest_seq=staged_response.ingest_seq,
        classification_monotonic_ns=200,
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
    )
    rejected_classification = classification_epoch.process_replayed(
        _envelope(
            response_run,
            6,
            CONTROL_REST_AUDIT_CLASSIFY,
            cross_epoch_classification.to_payload(),
            monotonic_ns=cross_epoch_classification.classification_monotonic_ns,
            connection_id=classification_start.connection_id,
        )
    )
    assert not rejected_classification.success
    assert rejected_classification.error_type == "NormalizationError"
    assert not classification_epoch.audit_outcomes
    assert classification_epoch.summary().pending_rest_audits == 1


@pytest.mark.parametrize("control_kind", ["classification-v1", "failure-v1"])
def test_prepare_attest_rejects_v1_completion_authority(
    control_kind: str,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"processor-prepare-attest-reject-{control_kind}")
    processor = _historical_direct_processor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id=f"prepare-attest-reject-{control_kind}",
    )
    before = processor.summary()
    books_before = processor.snapshots()

    if control_kind == "classification-v1":
        _, rejected_envelope = _audit_classification_attestation(
            run_id,
            7,
            start,
            response,
            preparation,
            hard_expiry_monotonic_ns=hard_expiry_ns,
            durable_monotonic_ns=201,
            processed_monotonic_ns=250,
            schema=AUDIT_CLASSIFICATION_V1_SCHEMA,
        )
    else:
        rejected_command = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            schema=AUDIT_FAILURE_V1_SCHEMA,
        )
        rejected_envelope = _envelope(
            run_id,
            7,
            CONTROL_REST_AUDIT_FAILED,
            rejected_command.to_payload(),
            connection_id=start.connection_id,
        )

    rejected = processor.process_replayed(rejected_envelope)
    after_rejection = processor.summary()

    if control_kind == "classification-v1":
        assert rejected.success
        assert tuple(item.outcome for item in rejected.audit_outcomes) == (AuditOutcome.FAILED,)
        assert rejected.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
        assert rejected.canonical_event_count == 0
        assert rejected.scan_count == 0
        assert rejected.book_freshness_updates == ()
        assert processor.snapshots() == books_before
        assert after_rejection.pending_rest_audits == 0
        assert after_rejection.audit_faults_permanent == before.audit_faults_permanent + 1
        return

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.audit_outcomes == ()
    assert processor.audit_outcomes == ()
    assert after_rejection.pending_rest_audits == 1
    assert after_rejection.rest_audits == before.rest_audits
    accepted_command = AuditFailureCommand(
        request_id=start.request_id,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        schema=AUDIT_FAILURE_V2_SCHEMA,
    )
    accepted_envelope = _envelope(
        run_id,
        8,
        CONTROL_REST_AUDIT_FAILED,
        accepted_command.to_payload(),
        connection_id=start.connection_id,
    )

    accepted = processor.process_replayed(accepted_envelope)
    assert accepted.success
    assert len(accepted.audit_outcomes) == 1
    assert processor.summary().pending_rest_audits == 0


def test_rejected_duplicate_prepare_is_inert_and_retains_first_preparation() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-prepare-attest-duplicate")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-duplicate",
    )
    books_before = processor.snapshots()
    ineligible_before = processor.audit_ineligible_instruments
    summary_before = processor.summary()
    _, duplicate = _audit_classification_prepare(
        run_id,
        7,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=201,
    )

    rejected = processor.process_replayed(duplicate)
    summary_after = processor.summary()

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.canonical_event_count == 0
    assert rejected.scan_count == 0
    assert rejected.scan_triggers == ()
    assert rejected.audit_outcomes == ()
    assert rejected.book_freshness_updates == ()
    assert rejected.relevant_l2_receive_updates == ()
    assert rejected.recovery_requests == ()
    assert rejected.audit_divergences_opened == ()
    assert rejected.audit_divergences_resolved == ()
    assert rejected.audit_divergences_eligibility_restored == ()
    assert rejected.affected_instrument_ids == ()
    assert not rejected.needs_resync
    assert processor.snapshots() == books_before
    assert processor.audit_ineligible_instruments == ineligible_before
    assert summary_after.pending_rest_audits == 1
    assert summary_after.parse_failures == summary_before.parse_failures + 1
    assert summary_after.invalidations == summary_before.invalidations
    assert summary_after.scans == summary_before.scans
    assert summary_after.rest_audits == summary_before.rest_audits

    _, final = _audit_classification_attestation(
        run_id,
        8,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=202,
        processed_monotonic_ns=250,
    )
    accepted = processor.process_replayed(final)
    assert accepted.success
    assert accepted.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert processor.summary().pending_rest_audits == 0


def test_attributable_malformed_attestation_fails_closed_permanently() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-prepare-attest-missing-metadata-request")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-missing-metadata-request",
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
    malformed_metadata = final.transport_metadata
    malformed_metadata.pop("request_id")
    malformed = replace(
        final,
        transport_metadata_allowlist=tuple(sorted(malformed_metadata.items())),
    )
    books_before = processor.snapshots()
    summary_before = processor.summary()

    rejected = processor.process_replayed(malformed)
    summary_after = processor.summary()

    assert rejected.success
    assert rejected.canonical_event_count == 0
    assert rejected.scan_count == 0
    assert rejected.scan_triggers == ()
    assert tuple(item.outcome for item in rejected.audit_outcomes) == (AuditOutcome.FAILED,)
    assert rejected.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert rejected.affected_instrument_ids == ()
    assert not rejected.needs_resync
    assert processor.snapshots() == books_before
    assert summary_after.pending_rest_audits == 0
    assert summary_after.parse_failures == summary_before.parse_failures
    assert summary_after.audit_faults_permanent == summary_before.audit_faults_permanent + 1
    assert summary_after.invalidations == summary_before.invalidations
    assert summary_after.scans == summary_before.scans


def test_prepare_attestation_at_fault_capacity_is_transactionally_inert() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-prepare-attest-fault-capacity")
    processor = CanonicalProcessor(config)
    hard_expiry_ns, start, response, preparation = _stage_prepare_attest_audit(
        processor,
        config,
        run_id,
        request_id="prepare-attest-fault-capacity",
    )
    before = processor.summary()
    books_before = processor.snapshots()
    ineligible_before = processor.audit_ineligible_instruments
    _, expired_final = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=hard_expiry_ns,
        processed_monotonic_ns=hard_expiry_ns,
    )

    sentinel = cast(AuditFaultLifecycleRecord, object())
    permanent_faults = processor._permanent_audit_faults  # pyright: ignore[reportPrivateUsage]
    permanent_faults.update(
        {
            f"synthetic-capacity-{index}": sentinel
            for index in range(processor_module.MAXIMUM_ACTIVE_AUDIT_FAULTS)
        }
    )
    rejected = processor.process_replayed(expired_final)
    permanent_faults.clear()
    after_rejection = processor.summary()

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.canonical_event_count == 0
    assert rejected.scan_count == 0
    assert rejected.scan_triggers == ()
    assert rejected.audit_outcomes == ()
    assert rejected.book_freshness_updates == ()
    assert rejected.recovery_requests == ()
    assert rejected.affected_instrument_ids == ()
    assert not rejected.needs_resync
    assert processor.snapshots() == books_before
    assert processor.audit_ineligible_instruments == ineligible_before
    assert after_rejection.pending_rest_audits == 1
    assert after_rejection.parse_failures == before.parse_failures + 1
    assert after_rejection.rest_audits == before.rest_audits
    assert after_rejection.rest_audit_failed == before.rest_audit_failed
    assert after_rejection.invalidations == before.invalidations
    assert after_rejection.scans == before.scans

    _, accepted_final = _audit_classification_attestation(
        run_id,
        8,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
    )
    accepted = processor.process_replayed(accepted_final)

    assert accepted.success
    assert accepted.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert processor.summary().pending_rest_audits == 0


def test_book_invalidation_reserves_capacity_for_all_pending_audits_atomically() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-invalidation-batch-fault-capacity")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    for sequence, native_asset_id in ((4, "token-yes"), (5, "token-no")):
        _, intent = _audit_start(
            processor,
            run_id,
            sequence,
            request_id=f"invalidation-capacity-{native_asset_id}",
            native_asset_id=native_asset_id,
            hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(
                config,
                native_asset_id,
            ),
            response_deadline_monotonic_ns=900,
            classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
        )
        assert processor.process_replayed(intent).success
    before = processor.summary()
    books_before = processor.snapshots()
    ineligible_before = processor.audit_ineligible_instruments
    prepare_requests = (
        processor._prepare_attest_rest_audit_requests  # pyright: ignore[reportPrivateUsage]
    )
    assert len(prepare_requests) == 2
    invalidation = _envelope(
        run_id,
        6,
        CONTROL_BOOK_INVALIDATION,
        canonical_json_bytes({"reason": InvalidationReason.DISCONNECT.value}),
    )

    sentinel = cast(AuditFaultLifecycleRecord, object())
    permanent_faults = processor._permanent_audit_faults  # pyright: ignore[reportPrivateUsage]
    permanent_faults.update(
        {
            f"synthetic-capacity-{index}": sentinel
            for index in range(processor_module.MAXIMUM_ACTIVE_AUDIT_FAULTS - 1)
        }
    )
    rejected = processor.process_replayed(invalidation)
    permanent_faults.clear()
    after_rejection = processor.summary()

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.canonical_event_count == 0
    assert rejected.scan_count == 0
    assert rejected.scan_triggers == ()
    assert rejected.audit_outcomes == ()
    assert rejected.book_freshness_updates == ()
    assert rejected.recovery_requests == ()
    assert rejected.affected_instrument_ids == ()
    assert not rejected.needs_resync
    assert processor.snapshots() == books_before
    assert processor.audit_ineligible_instruments == ineligible_before
    assert after_rejection.pending_rest_audits == 2
    assert len(prepare_requests) == 2
    assert after_rejection.rest_audits == before.rest_audits
    assert after_rejection.rest_audit_failed == before.rest_audit_failed
    assert after_rejection.invalidations == before.invalidations
    assert after_rejection.scans == before.scans

    accepted = processor.process_replayed(
        _envelope(
            run_id,
            7,
            CONTROL_BOOK_INVALIDATION,
            invalidation.payload_bytes,
        )
    )

    assert accepted.success
    assert len(accepted.audit_outcomes) == 2
    assert all(item.outcome is AuditOutcome.FAILED for item in accepted.audit_outcomes)
    assert processor.summary().pending_rest_audits == 0


@pytest.mark.parametrize(
    ("payload_size", "accepted"),
    [(64, True), (65, False)],
    ids=["exact-bound", "bound-plus-one"],
)
def test_deferred_audit_response_enforces_configured_byte_bound_before_retention(
    payload_size: int,
    accepted: bool,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(update={"maximum_http_response_bytes": 64})
        }
    )
    run_id = CollectorRunId(f"processor-deferred-byte-bound-{payload_size}")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="deferred-byte-bound",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        5,
        start,
        b"x" * payload_size,
        boundary=0,
        classification_deferred=True,
    )

    result = processor.process_replayed(response)

    assert result.success is accepted
    assert result.audit_outcomes == ()
    assert processor.summary().pending_rest_audits == 1
    retained = processor._pending_rest_audit_responses  # pyright: ignore[reportPrivateUsage]
    assert (start.request_id in retained) is accepted
    if accepted:
        assert retained[start.request_id].payload_bytes == b"x" * payload_size
    else:
        assert result.error_type == "NormalizationError"


def test_terminal_hash_binds_completed_prepare_attestation_durable_time() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-prepare-attest-terminal-binding")
    first = CanonicalProcessor(config)
    second = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert first.process_replayed(envelope).success
        assert second.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, intent = _audit_start(
        first,
        run_id,
        4,
        request_id="prepare-attest-terminal-binding",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        classification_protocol=AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL,
    )
    for processor in (first, second):
        assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    for processor in (first, second):
        assert processor.process_replayed(response).success
    _, preparation = _audit_classification_prepare(
        run_id,
        6,
        start,
        response,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        monotonic_ns=200,
    )
    for processor in (first, second):
        assert processor.process_replayed(preparation).success
    _, final_first = _audit_classification_attestation(
        run_id,
        7,
        start,
        response,
        preparation,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        durable_monotonic_ns=201,
        processed_monotonic_ns=250,
    )
    alternate_metadata = final_first.transport_metadata
    alternate_metadata["classification_prepare_durable_monotonic_ns"] = 249
    final_second = replace(
        final_first,
        transport_metadata_allowlist=tuple(sorted(alternate_metadata.items())),
    )

    first_result = first.process_replayed(final_first)
    second_result = second.process_replayed(final_second)
    first_summary = first.summary()
    second_summary = second.summary()

    assert first_result.success
    assert second_result.success
    assert first_result.audit_outcomes == second_result.audit_outcomes
    assert first_result.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert first_summary.raw_input_chain_sha256 == second_summary.raw_input_chain_sha256
    assert first_summary.audit_outcome_chain_sha256 == second_summary.audit_outcome_chain_sha256
    assert (
        first_summary.audit_classification_attestation_chain_sha256
        != second_summary.audit_classification_attestation_chain_sha256
    )
    assert first_summary.terminal_hash != second_summary.terminal_hash


def test_deferred_match_uses_response_freshness_and_expiry_equality_fails() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-deferred-audit-freshness")
    processor = _historical_direct_processor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="deferred-match",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
    )
    assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    staged = processor.process_replayed(response)
    assert staged.success
    assert not staged.audit_outcomes
    classification = AuditClassificationCommand(
        request_id=start.request_id,
        response_ingest_seq=response.ingest_seq,
        classification_monotonic_ns=800,
        hard_expiry_monotonic_ns=hard_expiry_ns,
    )
    matched = processor.process_replayed(
        _envelope(
            run_id,
            6,
            CONTROL_REST_AUDIT_CLASSIFY,
            classification.to_payload(),
            monotonic_ns=classification.classification_monotonic_ns,
            connection_id=start.connection_id,
        )
    )
    assert matched.success
    assert matched.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert matched.book_freshness_updates[0].received_monotonic == response.received_monotonic
    assert matched.scan_source_received_monotonic_ns == response.received_monotonic.nanoseconds

    equality_processor = _historical_direct_processor(config)
    equality_run = CollectorRunId("processor-deferred-audit-expiry-equality")
    for envelope in _bootstrap(equality_run):
        assert equality_processor.process_replayed(envelope).success
    equality_start, equality_intent = _audit_start(
        equality_processor,
        equality_run,
        4,
        request_id="expiry-equality",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
    )
    assert equality_processor.process_replayed(equality_intent).success
    equality_response = _audit_response(
        equality_run,
        5,
        equality_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert equality_processor.process_replayed(equality_response).success
    equality_decision = AuditClassificationCommand(
        request_id=equality_start.request_id,
        response_ingest_seq=equality_response.ingest_seq,
        classification_monotonic_ns=hard_expiry_ns,
        hard_expiry_monotonic_ns=hard_expiry_ns,
    )
    expired = equality_processor.process_replayed(
        _envelope(
            equality_run,
            6,
            CONTROL_REST_AUDIT_CLASSIFY,
            equality_decision.to_payload(),
            monotonic_ns=equality_decision.classification_monotonic_ns,
            connection_id=equality_start.connection_id,
        )
    )
    assert expired.success
    assert expired.audit_outcomes[0].outcome is AuditOutcome.FAILED
    assert expired.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert not expired.book_freshness_updates


@pytest.mark.parametrize(
    ("classification_schema", "blocks_current_scope"),
    [
        (AUDIT_CLASSIFICATION_V1_SCHEMA, False),
        (AUDIT_CLASSIFICATION_V2_SCHEMA, True),
    ],
)
def test_classification_schema_is_exact_implicit_fault_authority(
    classification_schema: str,
    blocks_current_scope: bool,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"processor-classification-authority-{classification_schema}")
    processor = _historical_direct_processor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"classification-authority-{classification_schema}",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
    )
    assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        5,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    assert processor.process_replayed(response).success
    classification = AuditClassificationCommand(
        request_id=start.request_id,
        response_ingest_seq=response.ingest_seq,
        classification_monotonic_ns=hard_expiry_ns,
        hard_expiry_monotonic_ns=hard_expiry_ns,
        schema=classification_schema,
    )
    failed = processor.process_replayed(
        _envelope(
            run_id,
            6,
            CONTROL_REST_AUDIT_CLASSIFY,
            classification.to_payload(),
            monotonic_ns=classification.classification_monotonic_ns,
            connection_id=start.connection_id,
        )
    )
    lifecycle = processor.summary().audit_fault_lifecycle_records[0]

    assert failed.success
    assert failed.audit_outcomes[0].reason is AuditFailureReason.BOOK_FRESHNESS_EXPIRED
    assert (
        lifecycle.failure_evidence_kind
        is AuditFaultFailureEvidenceKind.AUDIT_CLASSIFICATION_CONTROL
    )
    assert lifecycle.failure_schema == classification_schema
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.blocks_current_scope is blocks_current_scope
    assert lifecycle.resolution is None
    with pytest.raises(ValueError, match="source authority"):
        replace(
            lifecycle,
            failure_evidence_kind="AUDIT_CLASSIFICATION_CONTROL",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("source_kind", "failure_channel"),
    [
        (AuditFaultFailureEvidenceKind.REST_AUDIT_RESPONSE, REST_BOOK_AUDIT),
        (
            AuditFaultFailureEvidenceKind.BOOK_INVALIDATION_CONTROL,
            CONTROL_BOOK_INVALIDATION,
        ),
    ],
)
def test_unversioned_implicit_fault_sources_are_typed_and_v2_start_authoritative(
    source_kind: AuditFaultFailureEvidenceKind,
    failure_channel: str,
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId(f"processor-implicit-fault-source-{source_kind.value.lower()}")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"implicit-fault-source-{source_kind.value.lower()}",
    )
    assert processor.process_replayed(intent).success
    if failure_channel == REST_BOOK_AUDIT:
        failure = _audit_response(run_id, 5, start, b"{}", boundary=0)
    else:
        failure = _envelope(
            run_id,
            5,
            CONTROL_BOOK_INVALIDATION,
            canonical_json_bytes({"reason": InvalidationReason.DISCONNECT.value}),
        )
    failed = processor.process_replayed(failure)
    lifecycle = processor.summary().audit_fault_lifecycle_records[0]

    assert failed.success
    assert lifecycle.audit_start_schema == AUDIT_START_V2_SCHEMA
    assert lifecycle.failure_evidence_kind is source_kind
    assert lifecycle.failure_schema is None
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.blocks_current_scope is True


def test_audit_expiry_control_rejects_same_asset_pending_request() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = _historical_direct_processor(config)
    run_id = CollectorRunId("processor-expiry-pending-audit-conflict")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    _, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="pending-before-expiry",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config),
        response_deadline_monotonic_ns=900,
    )
    assert processor.process_replayed(intent).success
    expiry = AuditExpiryCommand(
        native_asset_id="token-yes",
        last_accepted_receive_monotonic_ns=100,
    )
    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_EXPIRED,
            expiry.to_payload(),
            connection_id=ConnectionId("processor-test-connection"),
        )
    )

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1


def test_audit_expiry_control_requires_typed_coordinator_recovery() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    run_id = CollectorRunId("processor-expiry-typed-recovery")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    instrument_id = processor.audit_book_identity("token-yes").instrument_id
    expiry = AuditExpiryCommand(
        native_asset_id="token-yes",
        last_accepted_receive_monotonic_ns=100,
    )
    envelope = _envelope(
        run_id,
        4,
        CONTROL_REST_AUDIT_EXPIRED,
        expiry.to_payload(),
        connection_id=ConnectionId("processor-expiry-typed-recovery-connection"),
    )

    result = processor.process_replayed(envelope)

    assert result.success
    assert result.needs_resync
    assert result.affected_instrument_ids == (instrument_id,)
    assert len(result.recovery_requests) == 1
    recovery = result.recovery_requests[0]
    assert recovery.reason is ProcessorRecoveryReason.FRESHNESS_EXPIRY
    assert recovery.original_reason_code == AuditFailureReason.BOOK_FRESHNESS_EXPIRED.value
    assert recovery.affected_instrument_ids == result.affected_instrument_ids
    assert (
        recovery.source
        == result.source
        == RawSourceRef(
            ingest_id=envelope.ingest_id,
            payload_sha256=envelope.payload_sha256,
        )
    )
    assert processor.audit_ineligible_instruments == (instrument_id,)
    assert all(owner.health is BookHealth.HEALTHY for owner in processor.owners)


def test_error_result_preserves_typed_recovery_affected_instruments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    run_id = CollectorRunId("processor-error-typed-recovery")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    original_expiry = processor._expire_rest_audit_eligibility  # pyright: ignore[reportPrivateUsage]

    def record_then_fail(envelope: RawEventEnvelope) -> None:
        original_expiry(envelope)
        raise NormalizationError("forced failure after typed recovery")

    monkeypatch.setattr(processor, "_expire_rest_audit_eligibility", record_then_fail)
    expiry = AuditExpiryCommand(
        native_asset_id="token-yes",
        last_accepted_receive_monotonic_ns=100,
    )

    result = processor.process_replayed(
        _envelope(
            run_id,
            4,
            CONTROL_REST_AUDIT_EXPIRED,
            expiry.to_payload(),
            connection_id=ConnectionId("processor-error-typed-recovery-connection"),
        )
    )

    recovery_affected = {
        instrument_id
        for recovery in result.recovery_requests
        for instrument_id in recovery.affected_instrument_ids
    }
    assert not result.success
    assert result.needs_resync
    assert recovery_affected
    assert set(result.affected_instrument_ids) == recovery_affected
    assert any(
        recovery.reason is ProcessorRecoveryReason.FRESHNESS_EXPIRY
        for recovery in result.recovery_requests
    )


def test_terminal_hash_binds_pending_audit_deadlines_and_staged_response_mapping() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-pending-audit-terminal-hash")
    bootstrap = _bootstrap(run_id)
    expiry_a = _historical_direct_processor(config)
    expiry_b = _historical_direct_processor(config)
    for envelope in bootstrap:
        assert expiry_a.process_replayed(envelope).success
        assert expiry_b.process_replayed(envelope).success
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    _start, intent = _audit_start(
        expiry_a,
        run_id,
        4,
        request_id="expiry-bound",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
    )
    alternate_intent = replace(
        intent,
        transport_metadata_allowlist=tuple(
            sorted(
                {
                    "hard_expiry_monotonic_ns": hard_expiry_ns,
                    "response_deadline_monotonic_ns": 901,
                    "run_deadline_monotonic_ns": 901,
                }.items()
            )
        ),
    )
    assert expiry_a.process_replayed(intent).success
    assert expiry_b.process_replayed(alternate_intent).success
    assert expiry_a.summary().raw_input_chain_sha256 == expiry_b.summary().raw_input_chain_sha256
    assert expiry_a.summary().terminal_hash != expiry_b.summary().terminal_hash

    mapping_a = _historical_direct_processor(config)
    mapping_b = _historical_direct_processor(config)
    for envelope in bootstrap:
        assert mapping_a.process_replayed(envelope).success
        assert mapping_b.process_replayed(envelope).success
    start_a, intent_a = _audit_start(
        mapping_a,
        run_id,
        4,
        request_id="mapping-a",
        native_asset_id="token-yes",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config, "token-yes"),
        response_deadline_monotonic_ns=900,
    )
    assert mapping_a.process_replayed(intent_a).success
    assert mapping_b.process_replayed(intent_a).success
    start_b, intent_b = _audit_start(
        mapping_a,
        run_id,
        5,
        request_id="mapping-b",
        native_asset_id="token-no",
        hard_expiry_monotonic_ns=_bootstrap_hard_expiry_ns(config, "token-no"),
        response_deadline_monotonic_ns=900,
    )
    assert mapping_a.process_replayed(intent_b).success
    assert mapping_b.process_replayed(intent_b).success
    response_a = _audit_response(
        run_id,
        6,
        start_a,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
        classification_deferred=True,
    )
    response_b_metadata = response_a.transport_metadata
    response_b_metadata["request_id"] = start_b.request_id
    response_b_metadata["audit_attempt"] = start_b.attempt
    response_b = replace(
        response_a,
        transport_metadata_allowlist=tuple(sorted(response_b_metadata.items())),
    )
    assert mapping_a.process_replayed(response_a).success
    assert mapping_b.process_replayed(response_b).success
    assert mapping_a.summary().raw_input_chain_sha256 == mapping_b.summary().raw_input_chain_sha256
    assert mapping_a.summary().terminal_hash != mapping_b.summary().terminal_hash


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
    diagnostic = mismatch_result.audit_outcomes[0].mismatch_diagnostic
    assert diagnostic is not None
    assert diagnostic.cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
    assert diagnostic.exact_depth_difference.difference_count == 1
    assert diagnostic.response_source.ingest_id == mismatching.ingest_id
    assert diagnostic.response_source.payload_sha256 == mismatching.payload_sha256
    assert mismatch_result.scan_count == 0
    assert not mismatch_result.book_freshness_updates
    assert mismatch_result.scan_triggers[0].reasons == (ScanTriggerReason.REST_AUDIT,)
    assert owner.snapshot().health is BookHealth.INVALID
    assert owner.snapshot().invalidation_reason is InvalidationReason.REST_AUDIT_MISMATCH
    assert summary.rest_audits == 2
    assert summary.rest_audit_matches == 1
    assert summary.rest_audit_mismatches == 1
    assert summary.rest_audit_raced == 0
    assert summary.rest_audit_failed == 0
    assert dict(summary.audit_mismatch_cause_counts) == {
        cause: int(cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE)
        for cause in AuditMismatchCause
    }
    assert summary.audit_mismatch_diagnostics == (diagnostic,)
    assert not summary.audit_mismatch_diagnostics_truncated
    assert summary.pending_rest_audits == 0
    assert summary.parse_failures == 0
    assert summary.scans == scans_before + 1


def test_v1_audit_start_replays_byte_exact_with_unknown_legacy_trigger() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-v1-audit-trigger")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="historical-v1-audit",
        trigger_reason=AuditTriggerReason.UNKNOWN_LEGACY,
        schema=AUDIT_START_V1_SCHEMA,
    )
    record = json.loads(start.to_payload())
    assert record["schema"] == AUDIT_START_V1_SCHEMA
    assert "trigger_reason" not in record
    assert processor.process_replayed(intent).success
    failure = _envelope(
        run_id,
        5,
        CONTROL_REST_AUDIT_FAILED,
        AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            schema=AUDIT_FAILURE_V1_SCHEMA,
        ).to_payload(),
        connection_id=start.connection_id,
    )
    result = processor.process_replayed(failure)
    summary = processor.summary()

    assert result.success
    assert result.audit_outcomes[0].audit_start_schema == AUDIT_START_V1_SCHEMA
    assert result.audit_outcomes[0].trigger_reason is AuditTriggerReason.UNKNOWN_LEGACY
    assert dict(summary.audit_start_trigger_counts)[AuditTriggerReason.UNKNOWN_LEGACY] == 1
    assert (
        AuditTriggerReason.UNKNOWN_LEGACY,
        AuditOutcome.FAILED,
        1,
    ) in summary.audit_outcome_trigger_counts
    assert summary.audit_faults_permanent == 1
    assert summary.audit_faults_current == 0
    lifecycle = summary.audit_fault_lifecycle_records[0]
    assert lifecycle.audit_start_schema == AUDIT_START_V1_SCHEMA
    assert lifecycle.failure_schema == AUDIT_FAILURE_V1_SCHEMA
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.blocks_current_scope is False
    assert lifecycle.resolution is None
    with pytest.raises(ValueError, match="exact boolean"):
        replace(lifecycle, blocks_current_scope=0)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "trigger_reason",
    tuple(
        trigger
        for trigger in AuditTriggerReason
        if trigger is not AuditTriggerReason.UNKNOWN_LEGACY
    ),
)
def test_v2_audit_trigger_encoding_and_outcome_retention_are_deterministic(
    trigger_reason: AuditTriggerReason,
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId(f"processor-v2-trigger-{trigger_reason.value.lower()}")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"v2-{trigger_reason.value.lower()}",
        trigger_reason=trigger_reason,
    )
    payload = start.to_payload()
    record = json.loads(payload)
    assert payload == start.to_payload()
    assert payload == canonical_json_bytes(record)
    assert record["schema"] == AUDIT_START_V2_SCHEMA
    assert record["trigger_reason"] == trigger_reason.value
    assert processor.process_replayed(intent).success

    failure = _envelope(
        run_id,
        5,
        CONTROL_REST_AUDIT_FAILED,
        AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        ).to_payload(),
        connection_id=start.connection_id,
    )
    result = processor.process_replayed(failure)
    summary = processor.summary()

    assert result.success
    assert result.audit_outcomes[0].audit_start_schema == AUDIT_START_V2_SCHEMA
    assert result.audit_outcomes[0].trigger_reason is trigger_reason
    assert dict(summary.audit_start_trigger_counts)[trigger_reason] == 1
    assert (trigger_reason, AuditOutcome.FAILED, 1) in (summary.audit_outcome_trigger_counts)


def test_mismatch_divergence_closes_only_on_separate_authoritative_rest_snapshot() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-audit-divergence-resnapshot")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    assert not processor.audit_recovery_blocks_native_asset("token-yes")
    assert not processor.audit_recovery_blocks_native_asset("token-no")
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="periodic-divergence",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(intent).success
    mismatch = _audit_response(
        run_id,
        5,
        start,
        mismatched_payload,
        boundary=0,
    )
    mismatch_result = processor.process_replayed(mismatch)
    pending_summary = processor.summary()

    assert mismatch_result.needs_resync
    assert pending_summary.audit_divergences_observed == 1
    assert pending_summary.audit_divergences_pending == 1
    assert pending_summary.audit_divergences_unresolved == 1
    assert pending_summary.audit_divergences_resolved_by_authoritative_resnapshot == 0
    assert pending_summary.audit_divergences_resolved_same_generation == 0
    assert pending_summary.audit_divergence_lifecycle_reconciled
    pending = pending_summary.audit_divergence_pending_records[0]
    assert pending.request_id == start.request_id
    assert pending.audit_start_control_source.ingest_id == intent.ingest_id
    assert pending.audit_outcome.trigger_reason is AuditTriggerReason.PERIODIC
    assert pending.classification_control_source is None
    diagnostic = pending.audit_outcome.mismatch_diagnostic
    assert diagnostic is not None
    assert pending.classification_source == diagnostic.response_source
    assert (
        pending.classification_received_monotonic_ns
        == pending.audit_outcome.response_received_monotonic_ns
    )
    assert processor.audit_recovery_blocks_native_asset("token-yes")
    assert processor.audit_recovery_blocks_native_asset("token-no")

    _, blocked_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id="blocked-while-divergence-pending",
        trigger_reason=AuditTriggerReason.FRESHNESS_GUARD,
    )
    blocked = processor.process_replayed(blocked_intent)
    blocked_summary = processor.summary()
    assert not blocked.success
    assert blocked.error_type == "NormalizationError"
    assert blocked_summary.audit_divergences_pending == 1
    assert dict(blocked_summary.audit_start_trigger_counts)[AuditTriggerReason.PERIODIC] == 1
    assert dict(blocked_summary.audit_start_trigger_counts)[AuditTriggerReason.FRESHNESS_GUARD] == 0

    resnapshot = _envelope(run_id, 7, REST_BOOK, mismatched_payload)
    assert processor.process_replayed(resnapshot).success
    recovered = processor.summary()

    assert recovered.audit_divergences_observed == 1
    assert recovered.audit_divergences_pending == 0
    assert recovered.audit_divergences_unresolved == 0
    assert recovered.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert recovered.audit_divergence_lifecycle_reconciled
    assert recovered.audit_replacement_captures_pending == 1
    assert processor.audit_ineligible_instruments == (pending.instrument_id,)
    assert processor.audit_recovery_blocks_native_asset("token-yes")
    assert processor.audit_recovery_blocks_native_asset("token-no")
    assert not recovered.audit_divergence_pending_records
    resolution = recovered.audit_divergence_resolution_records[0]
    assert resolution.divergence_id == pending.divergence_id
    assert resolution.request_id == start.request_id
    assert resolution.mismatch_response_source.ingest_id == mismatch.ingest_id
    assert resolution.authoritative_snapshot_source.ingest_id == resnapshot.ingest_id
    assert resolution.authoritative_snapshot_source.payload_sha256 == resnapshot.payload_sha256
    assert resolution.trigger_reason is AuditTriggerReason.PERIODIC
    assert resolution.observed_divergence == pending
    assert resolution.authoritative_rest_book.depth.content_sha256
    assert resolution.recovered_book.instrument_id == pending.instrument_id
    assert recovered.audit_divergence_history_chain_sha256 != (
        pending_summary.audit_divergence_history_chain_sha256
    )

    _, premature_audit = _audit_start(
        processor,
        run_id,
        8,
        request_id="blocked-before-replacement-capture",
        trigger_reason=AuditTriggerReason.RESYNC_VERIFICATION,
    )
    refused = processor.process_replayed(premature_audit)
    assert not refused.success
    assert refused.error_type == "NormalizationError"
    assert processor.summary().audit_replacement_captures_pending == 1

    _, refused_sibling_audit = _audit_start(
        processor,
        run_id,
        9,
        request_id="blocked-sibling-before-replacement-capture",
        native_asset_id="token-no",
        trigger_reason=AuditTriggerReason.RESYNC_VERIFICATION,
    )
    sibling_refused = processor.process_replayed(refused_sibling_audit)
    assert not sibling_refused.success
    assert sibling_refused.error_type == "NormalizationError"

    replacement_connection = ConnectionId("processor-test-replacement-connection")
    maximum_age_ns = int(
        load_phase1_config(Path("config/phase1.toml")).scanner.maximum_book_age_seconds
        * 1_000_000_000
    )
    first_yes = processor.process_replayed(
        _envelope(
            run_id,
            10,
            WS_MARKET,
            _websocket_book_payload(mismatched_payload, label="first-yes"),
            monotonic_ns=1_000,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        )
    )
    stale_sibling = processor.process_replayed(
        _envelope(
            run_id,
            11,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="late-no"),
            monotonic_ns=1_000 + maximum_age_ns + 1,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 2},
        )
    )
    assert first_yes.success
    assert stale_sibling.success
    assert first_yes.scan_count == 0
    assert stale_sibling.scan_count == 0
    assert processor.summary().audit_replacement_captures_pending == 1
    assert processor.audit_ineligible_instruments == (pending.instrument_id,)

    fresh_yes = processor.process_replayed(
        _envelope(
            run_id,
            12,
            WS_MARKET,
            _websocket_book_payload(mismatched_payload, label="fresh-yes"),
            monotonic_ns=1_000 + maximum_age_ns + 2,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 3},
        )
    )
    assert fresh_yes.success
    assert fresh_yes.scan_count == 0
    assert processor.summary().audit_replacement_captures_pending == 1
    assert processor.audit_ineligible_instruments == (pending.instrument_id,)

    release_monotonic_ns = 1_000 + maximum_age_ns + 2
    release_commands = processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, release_monotonic_ns),
    )
    assert len(release_commands) == 1
    release_command = release_commands[0]
    assert release_command.divergence_id == pending.divergence_id
    assert release_command.instrument_id == pending.instrument_id
    released = processor.process_replayed(
        _envelope(
            run_id,
            13,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            release_command.to_payload(),
            monotonic_ns=release_monotonic_ns,
            connection_id=replacement_connection,
        )
    )
    assert released.success
    assert released.scan_count == 1
    assert processor.summary().audit_replacement_captures_pending == 0
    assert processor.audit_ineligible_instruments == ()


def test_v1_failure_schema_replays_through_match_and_replacement_release() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-v1-replacement-auto-release")
    processor = CanonicalProcessor(config)
    envelopes = list(_bootstrap(run_id))
    for envelope in envelopes:
        assert processor.process_replayed(envelope).success

    failure_start, failure_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="historical-v1-failure",
        trigger_reason=AuditTriggerReason.OTHER_TYPED_REASON,
        schema=AUDIT_START_V2_SCHEMA,
    )
    failure = _envelope(
        run_id,
        5,
        CONTROL_REST_AUDIT_FAILED,
        AuditFailureCommand(
            request_id=failure_start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            schema=AUDIT_FAILURE_V1_SCHEMA,
        ).to_payload(),
        connection_id=failure_start.connection_id,
    )
    envelopes.extend((failure_intent, failure))
    assert processor.process_replayed(failure_intent).success
    assert processor.process_replayed(failure).success
    failed_summary = processor.summary()
    historical_fault = failed_summary.audit_fault_lifecycle_records[0]
    assert historical_fault.audit_start_schema == AUDIT_START_V2_SCHEMA
    assert (
        historical_fault.failure_evidence_kind
        is AuditFaultFailureEvidenceKind.AUDIT_FAILURE_CONTROL
    )
    assert historical_fault.failure_schema == AUDIT_FAILURE_V1_SCHEMA
    assert historical_fault.status is AuditFaultStatus.PERMANENT
    assert historical_fault.blocks_current_scope is False
    assert historical_fault.resolution is None
    assert historical_fault.fault_id == canonical_sha256(
        {
            "audit_start_schema": historical_fault.audit_start_schema,
            "audit_start_source": historical_fault.audit_start_source,
            "blocks_current_scope": historical_fault.blocks_current_scope,
            "failure_evidence_kind": historical_fault.failure_evidence_kind,
            "failure_schema": historical_fault.failure_schema,
            "failure_source": historical_fault.failure_source,
            "original_reason": historical_fault.original_reason,
            "reason": historical_fault.reason,
            "request_id": historical_fault.request_id,
            "schema": "mma-rest-audit-fault-id-v1",
        }
    )
    assert failed_summary.audit_faults_permanent == 1
    assert failed_summary.audit_ineligible_books == 1

    match_start, match_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id="historical-v1-later-match",
        trigger_reason=AuditTriggerReason.UNKNOWN_LEGACY,
        schema=AUDIT_START_V1_SCHEMA,
    )
    match = _audit_response(
        run_id,
        7,
        match_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
    )
    envelopes.extend((match_intent, match))
    assert processor.process_replayed(match_intent).success
    match_result = processor.process_replayed(match)
    assert match_result.success
    assert match_result.audit_outcomes[0].outcome is AuditOutcome.MATCH
    matched_summary = processor.summary()
    assert matched_summary.audit_faults_permanent == 1
    assert matched_summary.audit_faults_resolved == 0
    assert matched_summary.audit_fault_lifecycle_records == (historical_fault,)
    assert processor.audit_ineligible_instruments == ()

    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        8,
        request_id="historical-v1-divergence",
        trigger_reason=AuditTriggerReason.UNKNOWN_LEGACY,
        schema=AUDIT_START_V1_SCHEMA,
    )
    mismatch = _audit_response(run_id, 9, start, mismatched_payload, boundary=0)
    resnapshot = _envelope(run_id, 10, REST_BOOK, mismatched_payload)
    replacement_connection = ConnectionId("historical-v1-replacement")
    replacement_yes = _envelope(
        run_id,
        11,
        WS_MARKET,
        _websocket_book_payload(mismatched_payload, label="historical-v1-yes"),
        monotonic_ns=2_000,
        connection_id=replacement_connection,
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    replacement_no = _envelope(
        run_id,
        12,
        WS_MARKET,
        _websocket_book_payload(_no_book(), label="historical-v1-no"),
        monotonic_ns=2_001,
        connection_id=replacement_connection,
        transport_metadata={"websocket_admission_ordinal": 2},
    )
    envelopes.extend((intent, mismatch, resnapshot, replacement_yes, replacement_no))

    assert processor.process_replayed(intent).success
    assert processor.process_replayed(mismatch).needs_resync
    assert processor.process_replayed(resnapshot).success
    assert processor.process_replayed(replacement_yes).scan_count == 0
    final_snapshot = processor.process_replayed(replacement_no)
    assert final_snapshot.success
    assert final_snapshot.scan_count == 1
    assert final_snapshot.audit_divergences_eligibility_restored == ()
    final_summary = processor.summary()
    assert final_summary.audit_replacement_captures_pending == 0
    assert final_summary.audit_faults_permanent == 1
    assert final_summary.audit_faults_resolved == 0
    assert final_summary.audit_fault_lifecycle_records == (historical_fault,)
    assert processor.audit_ineligible_instruments == ()
    assert (
        processor.audit_replacement_release_commands(
            connection_id=replacement_connection,
            decision_monotonic=MonotonicTimestamp(run_id.value, 2_001),
        )
        == ()
    )

    replay = CanonicalProcessor(config)
    assert all(replay.process_replayed(envelope).success for envelope in envelopes)
    assert replay.summary() == processor.summary()
    assert replay.summary().terminal_hash == processor.summary().terminal_hash


def _pending_v2_replacement_release(
    label: str,
) -> tuple[
    Phase1Config,
    CollectorRunId,
    CanonicalProcessor,
    ConnectionId,
    AuditReplacementReleaseCommand,
]:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"processor-v2-release-{label}")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"v2-release-{label}",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(intent).success
    assert processor.process_replayed(
        _audit_response(run_id, 5, start, mismatched_payload, boundary=0)
    ).needs_resync
    assert processor.process_replayed(_envelope(run_id, 6, REST_BOOK, mismatched_payload)).success
    replacement_connection = ConnectionId(f"v2-release-{label}-connection")
    assert processor.process_replayed(
        _envelope(
            run_id,
            7,
            WS_MARKET,
            _websocket_book_payload(mismatched_payload, label="release-yes"),
            monotonic_ns=2_000,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        )
    ).success
    final_snapshot = processor.process_replayed(
        _envelope(
            run_id,
            8,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="release-no"),
            monotonic_ns=2_001,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 2},
        )
    )
    assert final_snapshot.success
    assert final_snapshot.scan_count == 0
    commands = processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, 2_001),
    )
    assert len(commands) == 1
    return config, run_id, processor, replacement_connection, commands[0]


@pytest.mark.parametrize(
    ("failure_schema", "release_allowed"),
    [
        (AUDIT_FAILURE_V1_SCHEMA, True),
        (AUDIT_FAILURE_V2_SCHEMA, False),
    ],
)
def test_only_v1_fault_backed_sibling_ineligibility_allows_capture_release(
    failure_schema: str,
    release_allowed: bool,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"processor-fault-backed-sibling-{failure_schema}")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"fault-backed-sibling-{failure_schema}",
        native_asset_id="token-no",
    )
    assert processor.process_replayed(intent).success
    if failure_schema == AUDIT_FAILURE_V1_SCHEMA:
        failure_command = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            schema=AUDIT_FAILURE_V1_SCHEMA,
        )
    else:
        failure_command = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpProtocolError:RESPONSE_PROTOCOL",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
            retryable=False,
            attempt=1,
            status_code=200,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=False,
        )
    assert processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            failure_command.to_payload(),
            connection_id=start.connection_id,
        )
    ).success
    failed_summary = processor.summary()
    lifecycle = failed_summary.audit_fault_lifecycle_records[-1]
    assert lifecycle.failure_schema == failure_schema
    assert lifecycle.blocks_current_scope is (not release_allowed)

    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    mismatch_start, mismatch_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id=f"fault-backed-sibling-capture-{failure_schema}",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(mismatch_intent).success
    assert processor.process_replayed(
        _audit_response(run_id, 7, mismatch_start, mismatched_payload, boundary=0)
    ).needs_resync
    assert processor.process_replayed(_envelope(run_id, 8, REST_BOOK, mismatched_payload)).success
    replacement_connection = ConnectionId(f"fault-backed-sibling-{failure_schema}")
    assert processor.process_replayed(
        _envelope(
            run_id,
            9,
            WS_MARKET,
            _websocket_book_payload(mismatched_payload, label="fault-backed-yes"),
            monotonic_ns=2_000,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        )
    ).success
    assert processor.process_replayed(
        _envelope(
            run_id,
            10,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="fault-backed-no"),
            monotonic_ns=2_001,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 2},
        )
    ).success
    failed_summary = processor.summary()
    assert failed_summary.audit_replacement_captures_pending == 1
    assert failed_summary.audit_ineligible_books == 2

    commands = processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, 2_001),
    )
    assert len(commands) == int(release_allowed)
    if not release_allowed:
        return
    released = processor.process_replayed(
        _envelope(
            run_id,
            11,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            commands[0].to_payload(),
            monotonic_ns=2_001,
            connection_id=replacement_connection,
        )
    )
    final_summary = processor.summary()

    assert released.success
    assert final_summary.audit_replacement_captures_pending == 0
    assert final_summary.audit_faults_permanent == 1
    assert final_summary.audit_faults_resolved == 0
    assert final_summary.audit_ineligible_books == 1
    assert final_summary.audit_fault_lifecycle_records == (lifecycle,)


def test_v2_replacement_release_accepts_historical_divergence_identity() -> None:
    _, run_id, processor, replacement_connection, current_command = _pending_v2_replacement_release(
        "historical-divergence-identity"
    )
    resolution = processor.summary().audit_divergence_resolution_records[0]
    enriched_pending = resolution.observed_divergence
    enriched_outcome = enriched_pending.audit_outcome
    assert enriched_outcome.audit_start_source is not None
    assert enriched_outcome.classification_source is not None

    historical_pending = AuditDivergencePending.create(
        market_id=enriched_pending.market_id,
        audit_outcome=replace(
            enriched_outcome,
            audit_start_source=None,
            classification_source=None,
        ),
        audit_start_control_source=enriched_pending.audit_start_control_source,
        classification_source=enriched_pending.classification_source,
        classification_control_source=enriched_pending.classification_control_source,
        classification_received_wall_time=enriched_pending.classification_received_wall_time,
        classification_received_monotonic_ns=(
            enriched_pending.classification_received_monotonic_ns
        ),
    )
    assert enriched_pending.divergence_id == historical_pending.divergence_id
    assert enriched_pending.to_record() != historical_pending.to_record()

    historical_capture_record = current_command.capture_record
    historical_capture_record["divergence_id"] = historical_pending.divergence_id
    historical_command = AuditReplacementReleaseCommand.create(
        divergence_id=historical_pending.divergence_id,
        instrument_id=current_command.instrument_id,
        market_id=current_command.market_id,
        replacement_connection_id=current_command.replacement_connection_id,
        capture_record=historical_capture_record,
        monotonic_epoch_id=current_command.monotonic_epoch_id,
        decision_monotonic_ns=current_command.decision_monotonic_ns,
        schema=current_command.schema,
    )

    released = processor.process_replayed(
        _envelope(
            run_id,
            9,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            historical_command.to_payload(),
            monotonic_ns=historical_command.decision_monotonic_ns,
            connection_id=replacement_connection,
        )
    )
    assert released.success
    assert len(released.audit_divergences_eligibility_restored) == 1
    restoration = released.audit_divergences_eligibility_restored[0]
    assert restoration.divergence_id == historical_command.divergence_id
    assert restoration.replacement_release_source == released.source
    assert restoration.capture_state_sha256 == historical_command.capture_state_sha256
    assert restoration.availability_binding.replacement_release_source == released.source
    assert processor.summary().audit_replacement_captures_pending == 0


@pytest.mark.parametrize(
    "invalid_snapshot",
    [
        "missing_admission_ordinal",
        "pre_resolution_monotonic",
        "pre_resolution_ingest",
        "original_connection",
        "reused_pre_open_connection",
    ],
)
def test_v2_replacement_capture_rejects_unbound_or_pre_resolution_snapshot(
    invalid_snapshot: str,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId(f"processor-v2-capture-reject-{invalid_snapshot}")
    processor = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    prior_connection = ConnectionId(f"v2-prior-{invalid_snapshot}")
    next_sequence = 4
    if invalid_snapshot == "reused_pre_open_connection":
        prior = _envelope(
            run_id,
            next_sequence,
            WS_MARKET,
            _websocket_book_payload(
                _fixture("polymarket_rest_book_yes.json"),
                label="pre-open-connection",
            ),
            connection_id=prior_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        )
        assert processor.process_replayed(prior).success
        next_sequence += 1

    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        next_sequence,
        request_id=f"v2-capture-reject-{invalid_snapshot}",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(intent).success
    assert processor.process_replayed(
        _audit_response(run_id, next_sequence + 1, start, mismatched_payload, boundary=0)
    ).needs_resync
    resolution = _envelope(run_id, next_sequence + 2, REST_BOOK, mismatched_payload)
    assert processor.process_replayed(resolution).success
    assert processor.summary().audit_replacement_captures_pending == 1

    before = processor.summary()
    before_snapshots = processor.snapshots()
    sequence = next_sequence + 3
    monotonic_ns = resolution.received_monotonic.nanoseconds + 1
    replacement_connection = ConnectionId(f"v2-new-{invalid_snapshot}")
    metadata: dict[str, str | int | bool] | None = {
        "websocket_admission_ordinal": 1,
    }
    if invalid_snapshot == "missing_admission_ordinal":
        metadata = None
    elif invalid_snapshot == "pre_resolution_monotonic":
        monotonic_ns = resolution.received_monotonic.nanoseconds - 1
    elif invalid_snapshot == "pre_resolution_ingest":
        sequence = resolution.ingest_seq
    elif invalid_snapshot == "original_connection":
        replacement_connection = start.connection_id
    elif invalid_snapshot == "reused_pre_open_connection":
        replacement_connection = prior_connection
        metadata = {"websocket_admission_ordinal": 2}

    rejected = processor.process_replayed(
        _envelope(
            run_id,
            sequence,
            WS_MARKET,
            _websocket_book_payload(
                _fixture("polymarket_rest_book_yes.json").replace(
                    b'"price": "0.40", "size": "10"',
                    b'"price": "0.40", "size": "11"',
                ),
                label=invalid_snapshot,
            ),
            monotonic_ns=monotonic_ns,
            connection_id=replacement_connection,
            transport_metadata=metadata,
        )
    )
    after = processor.summary()

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.scan_count == 0
    assert after.scans == before.scans
    assert after.canonical_events == before.canonical_events
    assert processor.snapshots() == before_snapshots
    assert after.audit_replacement_captures_pending == 1
    assert after.audit_ineligible_books == 1
    assert not processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(
            run_id.value,
            monotonic_ns,
        ),
    )

    if invalid_snapshot == "reused_pre_open_connection":
        valid_connection = ConnectionId("v2-actual-post-open-connection")
        assert processor.process_replayed(
            _envelope(
                run_id,
                next_sequence + 4,
                WS_MARKET,
                _websocket_book_payload(mismatched_payload, label="post-open-yes"),
                monotonic_ns=2_000,
                connection_id=valid_connection,
                transport_metadata={"websocket_admission_ordinal": 1},
            )
        ).success
        assert processor.process_replayed(
            _envelope(
                run_id,
                next_sequence + 5,
                WS_MARKET,
                _websocket_book_payload(_no_book(), label="post-open-no"),
                monotonic_ns=2_001,
                connection_id=valid_connection,
                transport_metadata={"websocket_admission_ordinal": 2},
            )
        ).success
        commands = processor.audit_replacement_release_commands(
            connection_id=valid_connection,
            decision_monotonic=MonotonicTimestamp(run_id.value, 2_001),
        )
        assert len(commands) == 1
        capture_record = commands[0].capture_record
        assert capture_record["pre_open_connection_count"] == 1
        assert (
            capture_record["pre_open_connection_ids_sha256"]
            == hashlib.sha256(
                canonical_json_bytes(
                    {
                        "connection_ids": (prior_connection.value,),
                        "schema": "mma-audit-replacement-pre-open-connections-v1",
                    }
                )
            ).hexdigest()
        )
        assert capture_record["resolution_ingest_seq"] == resolution.ingest_seq
        assert capture_record["resolution_source"] == {
            "ingest_id": resolution.ingest_id.value,
            "payload_sha256": resolution.payload_sha256,
        }


@pytest.mark.parametrize("invalid_time", ["cross_epoch", "future", "stale"])
def test_v2_replacement_release_control_rejects_invalid_freshness_chronology(
    invalid_time: str,
) -> None:
    config, run_id, processor, replacement_connection, valid = _pending_v2_replacement_release(
        invalid_time
    )
    maximum_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    if invalid_time == "cross_epoch":
        invalid = replace(valid, monotonic_epoch_id=f"{run_id.value}-other")
        envelope_monotonic_ns = valid.decision_monotonic_ns
    elif invalid_time == "future":
        invalid = replace(valid, decision_monotonic_ns=2_000)
        envelope_monotonic_ns = 2_000
    else:
        invalid = replace(valid, decision_monotonic_ns=2_001 + maximum_age_ns + 1)
        envelope_monotonic_ns = invalid.decision_monotonic_ns

    rejected = processor.process_replayed(
        _envelope(
            run_id,
            9,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            invalid.to_payload(),
            monotonic_ns=envelope_monotonic_ns,
            connection_id=replacement_connection,
        )
    )
    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert processor.summary().audit_replacement_captures_pending == 1
    assert processor.audit_ineligible_instruments


def test_v2_replacement_release_rejects_decision_before_latest_required_l2() -> None:
    _, run_id, processor, replacement_connection, stale_command = _pending_v2_replacement_release(
        "future-current-book"
    )
    intervening = processor.process_replayed(
        _envelope(
            run_id,
            9,
            WS_MARKET,
            _price_change_payload(
                bid_size="5",
                timestamp="1787198403000",
            ),
            monotonic_ns=3_000,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 3},
        )
    )
    assert intervening.success
    assert intervening.scan_count == 0
    assert not processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(
            run_id.value,
            stale_command.decision_monotonic_ns,
        ),
    )
    before = processor.summary()

    rejected = processor.process_replayed(
        _envelope(
            run_id,
            10,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            stale_command.to_payload(),
            monotonic_ns=stale_command.decision_monotonic_ns,
            connection_id=replacement_connection,
        )
    )
    after = processor.summary()

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.scan_count == 0
    assert after.scans == before.scans
    assert after.audit_replacement_captures_pending == 1
    assert after.audit_ineligible_books == 1


def test_v2_replacement_release_waits_for_sibling_reported_top_reconciliation() -> None:
    _, run_id, processor, replacement_connection, stale_command = _pending_v2_replacement_release(
        "sibling-reported-top"
    )
    sibling_transition = processor.process_replayed(
        _envelope(
            run_id,
            9,
            WS_MARKET,
            canonical_json_bytes(
                {
                    "asset_id": "token-no",
                    "best_ask": "0.35",
                    "best_bid": "0.31",
                    "event_type": "best_bid_ask",
                    "market": "0xcondition",
                    "spread": "0.04",
                    "timestamp": "1787198402002",
                }
            ),
            monotonic_ns=2_002,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 3},
        )
    )
    assert sibling_transition.success
    assert sibling_transition.scan_count == 0
    assert len(processor.reported_top_transitions) == 1
    assert not processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, 2_002),
    )

    rejected = processor.process_replayed(
        _envelope(
            run_id,
            10,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            stale_command.to_payload(),
            monotonic_ns=stale_command.decision_monotonic_ns,
            connection_id=replacement_connection,
        )
    )
    assert not rejected.success
    assert rejected.scan_count == 0
    assert processor.summary().audit_replacement_captures_pending == 1

    reconciled = processor.process_replayed(
        _envelope(
            run_id,
            11,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="sibling-reconciled"),
            monotonic_ns=2_003,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 4},
        )
    )
    assert reconciled.success
    assert reconciled.scan_count == 0
    assert not processor.reported_top_transitions
    commands = processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, 2_003),
    )
    assert len(commands) == 1

    released = processor.process_replayed(
        _envelope(
            run_id,
            12,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            commands[0].to_payload(),
            monotonic_ns=2_003,
            connection_id=replacement_connection,
        )
    )
    assert released.success
    assert released.scan_count == 1
    assert processor.summary().audit_replacement_captures_pending == 0
    assert not processor.audit_ineligible_instruments


def test_v2_multi_capture_group_restores_only_after_final_control() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-v2-multi-capture-release")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    yes_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    no_payload = _no_book().replace(
        b'"price":"0.30","size":"20"',
        b'"price":"0.30","size":"21"',
    )
    yes_start, yes_intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="multi-capture-yes",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    no_start, no_intent = _audit_start(
        processor,
        run_id,
        5,
        request_id="multi-capture-no",
        native_asset_id="token-no",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(yes_intent).success
    assert processor.process_replayed(no_intent).success
    assert processor.process_replayed(
        _audit_response(run_id, 6, yes_start, yes_payload, boundary=0)
    ).needs_resync
    assert processor.process_replayed(
        _audit_response(run_id, 7, no_start, no_payload, boundary=0)
    ).needs_resync
    assert processor.process_replayed(_envelope(run_id, 8, REST_BOOK, yes_payload)).success
    assert processor.process_replayed(_envelope(run_id, 9, REST_BOOK, no_payload)).success
    assert processor.summary().audit_replacement_captures_pending == 2

    replacement_connection = ConnectionId("processor-v2-multi-capture-connection")
    assert (
        processor.process_replayed(
            _envelope(
                run_id,
                10,
                WS_MARKET,
                _websocket_book_payload(yes_payload, label="multi-yes"),
                monotonic_ns=2_000,
                connection_id=replacement_connection,
                transport_metadata={"websocket_admission_ordinal": 1},
            )
        ).scan_count
        == 0
    )
    assert (
        processor.process_replayed(
            _envelope(
                run_id,
                11,
                WS_MARKET,
                _websocket_book_payload(no_payload, label="multi-no"),
                monotonic_ns=2_001,
                connection_id=replacement_connection,
                transport_metadata={"websocket_admission_ordinal": 2},
            )
        ).scan_count
        == 0
    )
    commands = processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, 2_001),
    )
    assert len(commands) == 2

    first = processor.process_replayed(
        _envelope(
            run_id,
            12,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            commands[0].to_payload(),
            monotonic_ns=2_001,
            connection_id=replacement_connection,
        )
    )
    assert first.success
    assert first.scan_count == 0
    assert processor.summary().audit_replacement_captures_pending == 1
    second = processor.process_replayed(
        _envelope(
            run_id,
            13,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            commands[1].to_payload(),
            monotonic_ns=2_001,
            connection_id=replacement_connection,
        )
    )
    assert second.success
    assert second.scan_count == 1
    assert processor.summary().audit_replacement_captures_pending == 0
    assert not processor.audit_ineligible_instruments


@pytest.mark.parametrize(
    "invalid_evidence",
    [
        "wrong_connection",
        "wrong_receive_boundary",
        "tampered_capture_preimage",
        "tampered_capture_digest",
        "noncanonical_payload",
        "pre_capture_ingest_boundary",
    ],
)
def test_v2_replacement_release_rejects_invalid_durable_evidence_without_mutation(
    invalid_evidence: str,
) -> None:
    _, run_id, processor, replacement_connection, valid = _pending_v2_replacement_release(
        invalid_evidence
    )
    payload = valid.to_payload()
    envelope_sequence = 9
    envelope_connection = replacement_connection
    envelope_monotonic_ns = valid.decision_monotonic_ns
    if invalid_evidence == "wrong_connection":
        envelope_connection = ConnectionId("wrong-release-control-connection")
    elif invalid_evidence == "wrong_receive_boundary":
        envelope_monotonic_ns += 1
    elif invalid_evidence == "tampered_capture_preimage":
        record = cast(dict[str, object], json.loads(payload))
        capture_record = cast(dict[str, object], record["capture_record"])
        capture_record["original_connection_id"] = "tampered-original-connection"
        payload = canonical_json_bytes(record)
    elif invalid_evidence == "tampered_capture_digest":
        record = cast(dict[str, object], json.loads(payload))
        record["capture_state_sha256"] = "0" * 64
        payload = canonical_json_bytes(record)
    elif invalid_evidence == "pre_capture_ingest_boundary":
        envelope_sequence = 8
    else:
        payload = b" " + payload
    before = processor.summary()

    rejected = processor.process_replayed(
        _envelope(
            run_id,
            envelope_sequence,
            CONTROL_REST_AUDIT_REPLACEMENT_RELEASE,
            payload,
            monotonic_ns=envelope_monotonic_ns,
            connection_id=envelope_connection,
        )
    )
    after = processor.summary()

    assert not rejected.success
    assert rejected.error_type == "NormalizationError"
    assert rejected.scan_count == 0
    assert after.scans == before.scans
    assert (
        after.audit_replacement_captures_pending == before.audit_replacement_captures_pending == 1
    )
    assert after.audit_ineligible_books == before.audit_ineligible_books == 1
    assert processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(
            run_id.value,
            valid.decision_monotonic_ns,
        ),
    ) == (valid,)


def test_audit_replacement_release_capture_record_has_exact_parser_bound() -> None:
    _, _, _, _, valid = _pending_v2_replacement_release("capture-record-bound")
    capture_record = valid.capture_record
    capture_record["original_connection_id"] = ""
    base_size = len(canonical_json_bytes(capture_record))
    capture_record["original_connection_id"] = "x" * (
        AUDIT_REPLACEMENT_CAPTURE_MAX_BYTES - base_size
    )
    bounded = AuditReplacementReleaseCommand.create(
        divergence_id=valid.divergence_id,
        instrument_id=valid.instrument_id,
        market_id=valid.market_id,
        replacement_connection_id=valid.replacement_connection_id,
        capture_record=capture_record,
        monotonic_epoch_id=valid.monotonic_epoch_id,
        decision_monotonic_ns=valid.decision_monotonic_ns,
        schema=valid.schema,
    )

    assert len(bounded.capture_record_json.encode("utf-8")) == (AUDIT_REPLACEMENT_CAPTURE_MAX_BYTES)
    assert (
        AuditReplacementReleaseCommand(
            divergence_id=bounded.divergence_id,
            instrument_id=bounded.instrument_id,
            market_id=bounded.market_id,
            replacement_connection_id=bounded.replacement_connection_id,
            capture_state_sha256=bounded.capture_state_sha256,
            capture_record_json=bounded.capture_record_json,
            monotonic_epoch_id=bounded.monotonic_epoch_id,
            decision_monotonic_ns=bounded.decision_monotonic_ns,
            schema=bounded.schema,
        )
        == bounded
    )

    oversized_record = bounded.capture_record
    oversized_record["original_connection_id"] = (
        cast(
            str,
            oversized_record["original_connection_id"],
        )
        + "x"
    )
    with pytest.raises(ValueError, match="capture record exceeds its bound"):
        AuditReplacementReleaseCommand.create(
            divergence_id=valid.divergence_id,
            instrument_id=valid.instrument_id,
            market_id=valid.market_id,
            replacement_connection_id=valid.replacement_connection_id,
            capture_record=oversized_record,
            monotonic_epoch_id=valid.monotonic_epoch_id,
            decision_monotonic_ns=valid.decision_monotonic_ns,
            schema=valid.schema,
        )


def test_failed_resnapshot_and_disconnect_leave_divergence_pending_until_recovery() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-divergence-failed-recovery")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="failed-authoritative-recovery",
        trigger_reason=AuditTriggerReason.FRESHNESS_GUARD,
    )
    assert processor.process_replayed(intent).success
    mismatch = _audit_response(run_id, 5, start, mismatched_payload, boundary=0)
    assert processor.process_replayed(mismatch).needs_resync

    malformed_resnapshot = _envelope(run_id, 6, REST_BOOK, b"{}")
    failed = processor.process_replayed(malformed_resnapshot)
    after_failure = processor.summary()
    assert not failed.success
    assert failed.error_type == "PolymarketSchemaError"
    assert after_failure.audit_divergences_pending == 1
    assert after_failure.audit_divergences_unresolved == 1
    assert after_failure.audit_divergences_resolved_by_authoritative_resnapshot == 0
    assert after_failure.audit_divergence_lifecycle_reconciled

    disconnect = _envelope(
        run_id,
        7,
        CONTROL_BOOK_INVALIDATION,
        canonical_json_bytes({"reason": InvalidationReason.DISCONNECT.value}),
    )
    assert processor.process_replayed(disconnect).success
    after_disconnect = processor.summary()
    assert after_disconnect.audit_divergences_pending == 1
    assert after_disconnect.audit_divergences_unresolved == 1
    assert all(owner.health is BookHealth.INVALID for owner in processor.owners)

    authoritative_resnapshot = _envelope(run_id, 8, REST_BOOK, mismatched_payload)
    assert processor.process_replayed(authoritative_resnapshot).success
    recovered = processor.summary()
    assert recovered.audit_divergences_pending == 0
    assert recovered.audit_divergences_unresolved == 0
    assert recovered.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert recovered.audit_divergence_lifecycle_reconciled


def test_deferred_mismatch_binds_exact_classification_control_provenance() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = _historical_direct_processor(config)
    run_id = CollectorRunId("processor-deferred-divergence-provenance")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    hard_expiry_ns = _bootstrap_hard_expiry_ns(config)
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="deferred-divergence",
        hard_expiry_monotonic_ns=hard_expiry_ns,
        response_deadline_monotonic_ns=900,
        trigger_reason=AuditTriggerReason.RESYNC_VERIFICATION,
    )
    assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        5,
        start,
        mismatched_payload,
        boundary=0,
        classification_deferred=True,
    )
    staged = processor.process_replayed(response)
    assert staged.success
    assert not staged.audit_outcomes
    classification = AuditClassificationCommand(
        request_id=start.request_id,
        response_ingest_seq=response.ingest_seq,
        classification_monotonic_ns=800,
        hard_expiry_monotonic_ns=hard_expiry_ns,
    )
    classification_envelope = _envelope(
        run_id,
        6,
        CONTROL_REST_AUDIT_CLASSIFY,
        classification.to_payload(),
        monotonic_ns=classification.classification_monotonic_ns,
        connection_id=start.connection_id,
    )
    classified = processor.process_replayed(classification_envelope)
    summary = processor.summary()

    assert classified.success
    assert classified.audit_outcomes[0].outcome is AuditOutcome.MISMATCH
    pending = summary.audit_divergence_pending_records[0]
    expected_classification_source = RawSourceRef(
        ingest_id=classification_envelope.ingest_id,
        payload_sha256=classification_envelope.payload_sha256,
    )
    assert pending.classification_source == expected_classification_source
    assert pending.classification_control_source == expected_classification_source
    assert pending.classification_received_wall_time == classification_envelope.received_wall_time
    assert (
        pending.classification_received_monotonic_ns == classification.classification_monotonic_ns
    )
    diagnostic = pending.audit_outcome.mismatch_diagnostic
    assert diagnostic is not None
    assert diagnostic.response_source.ingest_id == response.ingest_id
    assert pending.audit_start_control_source.ingest_id == intent.ingest_id
    assert pending.audit_outcome.trigger_reason is AuditTriggerReason.RESYNC_VERIFICATION

    resnapshot = _envelope(
        run_id,
        7,
        REST_BOOK,
        mismatched_payload,
        monotonic_ns=classification.classification_monotonic_ns + 1,
    )
    assert processor.process_replayed(resnapshot).success
    resolution = processor.summary().audit_divergence_resolution_records[0]
    with pytest.raises(ValueError, match="precedes its classification"):
        replace(
            resolution,
            resolved_monotonic_ns=classification.classification_monotonic_ns - 1,
        )


def test_equal_depth_reference_scalar_mismatch_remains_immediate_unknown() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-rest-audit-scalar-mismatch")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="scalar-mismatch-audit",
    )
    assert processor.process_replayed(intent).success
    scalar_mismatch = _fixture("polymarket_rest_book_yes.json").replace(
        b'"tick_size": "0.01"',
        b'"tick_size": "0.001"',
    )

    result = processor.process_replayed(
        _audit_response(
            run_id,
            5,
            start,
            scalar_mismatch,
            boundary=0,
        )
    )

    assert result.success
    assert result.needs_resync
    assert len(result.audit_outcomes) == 1
    outcome = result.audit_outcomes[0]
    assert outcome.outcome is AuditOutcome.MISMATCH
    diagnostic = outcome.mismatch_diagnostic
    assert diagnostic is not None
    assert diagnostic.cause is AuditMismatchCause.UNKNOWN
    assert diagnostic.exact_depth_difference.difference_count == 0
    assert dict(processor.summary().audit_mismatch_cause_counts) == {
        cause: int(cause is AuditMismatchCause.UNKNOWN) for cause in AuditMismatchCause
    }


def test_unknown_v2_divergence_cannot_restore_research_eligibility() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-unknown-v2-release-blocked")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="unknown-v2-release-blocked",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(intent).success
    scalar_mismatch = _fixture("polymarket_rest_book_yes.json").replace(
        b'"tick_size": "0.01"',
        b'"tick_size": "0.001"',
    )
    mismatch = processor.process_replayed(
        _audit_response(run_id, 5, start, scalar_mismatch, boundary=0)
    )
    assert mismatch.needs_resync
    diagnostic = mismatch.audit_outcomes[0].mismatch_diagnostic
    assert diagnostic is not None
    assert diagnostic.cause is AuditMismatchCause.UNKNOWN
    authoritative = _fixture("polymarket_rest_book_yes.json")
    assert processor.process_replayed(_envelope(run_id, 6, REST_BOOK, authoritative)).success

    replacement_connection = ConnectionId("unknown-v2-replacement")
    first = processor.process_replayed(
        _envelope(
            run_id,
            7,
            WS_MARKET,
            _websocket_book_payload(authoritative, label="unknown-yes"),
            monotonic_ns=2_000,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 1},
        )
    )
    second = processor.process_replayed(
        _envelope(
            run_id,
            8,
            WS_MARKET,
            _websocket_book_payload(_no_book(), label="unknown-no"),
            monotonic_ns=2_001,
            connection_id=replacement_connection,
            transport_metadata={"websocket_admission_ordinal": 2},
        )
    )

    assert first.success
    assert second.success
    assert first.scan_count == second.scan_count == 0
    assert not processor.audit_replacement_release_commands(
        connection_id=replacement_connection,
        decision_monotonic=MonotonicTimestamp(run_id.value, 2_001),
    )
    summary = processor.summary()
    assert summary.audit_divergences_unknown == 1
    assert summary.audit_replacement_captures_pending == 1
    assert summary.audit_ineligible_books == 1


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
            error_type="PolymarketHttpTransportError:TIMEOUT",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.TIMEOUT,
            retryable=True,
            attempt=3,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=True,
            retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
        ).to_payload(),
        connection_id=explicit_start.connection_id,
    )
    explicit_result = explicit.process_replayed(failure)

    assert explicit_result.success
    assert not explicit_result.needs_resync
    assert explicit_result.audit_outcomes[0].reason is AuditFailureReason.HTTP_REQUEST_FAILED
    assert explicit.summary().rest_audit_failed == 1
    assert explicit.summary().audit_ineligible_books == 1
    assert explicit.summary().audit_faults_current == 1
    assert explicit.summary().audit_faults_permanent == 0
    assert explicit.summary().audit_http_faults_observed == 1
    explicit_operational = explicit.operational_snapshot()
    assert explicit_operational.audit_fault_current_representatives == (
        explicit.summary().audit_fault_current_records
    )
    assert explicit_operational.audit_fault_current_instrument_ids == (
        explicit.summary().audit_fault_current_records[0].instrument_id,
    )
    assert explicit_operational.audit_fault_current_representative_capacity == 2
    assert explicit_operational.audit_fault_current_representatives_truncated is False

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
    assert explicit.summary().audit_faults_current == 0
    assert explicit.summary().audit_faults_resolved == 1
    assert explicit.summary().audit_http_faults_resolved == 1
    assert explicit.summary().audit_http_retry_exhaustions_resolved == 1
    assert explicit.operational_snapshot().audit_fault_current_representatives == ()
    assert explicit.operational_snapshot().audit_fault_current_instrument_ids == ()


def test_canonical_replay_rejects_internal_reason_with_transient_http_metadata() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-reject-internal-http-metadata")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="reject-internal-http-metadata",
    )
    assert processor.process_replayed(intent).success
    payload = canonical_json_bytes(
        {
            "category": PolymarketHttpFailureCategory.TIMEOUT.value,
            "endpoint_class": PolymarketEndpointClass.CLOB_BOOK.value,
            "error_type": "PolymarketHttpTransportError:TIMEOUT",
            "http_attempt": 1,
            "reason": AuditFailureReason.OBSERVATION_DEADLINE.value,
            "request_id": start.request_id,
            "retry_after_disposition": PolymarketRetryAfterDisposition.ABSENT.value,
            "retry_after_nanoseconds": None,
            "retry_blocked_reason": None,
            "retry_exhausted": False,
            "retryable": True,
            "schema": AUDIT_FAILURE_V2_SCHEMA,
            "status_code": None,
        }
    )

    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            payload,
            connection_id=start.connection_id,
        )
    )

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1
    assert processor.audit_outcomes == ()


@pytest.mark.parametrize(
    ("endpoint_class", "category", "status_code"),
    [
        (
            PolymarketEndpointClass.GAMMA_MARKETS,
            PolymarketHttpFailureCategory.HTTP_STATUS,
            503,
        ),
        (
            PolymarketEndpointClass.CLOB_BOOK,
            PolymarketHttpFailureCategory.TIMEOUT,
            404,
        ),
    ],
)
def test_canonical_replay_rejects_forged_audit_http_provenance(
    endpoint_class: PolymarketEndpointClass,
    category: PolymarketHttpFailureCategory,
    status_code: int,
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId(
        f"processor-reject-forged-http-{endpoint_class.value.lower()}-{category.value.lower()}"
    )
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="reject-forged-audit-http-provenance",
    )
    assert processor.process_replayed(intent).success
    payload = canonical_json_bytes(
        {
            "category": category.value,
            "endpoint_class": endpoint_class.value,
            "error_type": f"PolymarketHttpError:{category.value}",
            "http_attempt": 3,
            "reason": AuditFailureReason.HTTP_REQUEST_FAILED.value,
            "request_id": start.request_id,
            "retry_after_disposition": PolymarketRetryAfterDisposition.ABSENT.value,
            "retry_after_nanoseconds": None,
            "retry_blocked_reason": PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED.value,
            "retry_exhausted": True,
            "retryable": True,
            "schema": AUDIT_FAILURE_V2_SCHEMA,
            "status_code": status_code,
        }
    )

    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            payload,
            connection_id=start.connection_id,
        )
    )

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1
    assert processor.audit_outcomes == ()


@pytest.mark.parametrize(
    ("attempt", "retry_after_nanoseconds", "blocked_reason"),
    [
        (2, None, PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED),
        (4, None, PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED),
        (1, 1, PolymarketRetryBlockedReason.RETRY_AFTER_POLICY),
        (3, 10_000_000_001, PolymarketRetryBlockedReason.RETRY_AFTER_POLICY),
        (1, 10_000_000_001, PolymarketRetryBlockedReason.DEADLINE),
    ],
)
def test_canonical_replay_rejects_audit_http_policy_impossibilities(
    attempt: int,
    retry_after_nanoseconds: int | None,
    blocked_reason: PolymarketRetryBlockedReason,
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId(
        f"processor-reject-http-policy-{attempt}-{blocked_reason.value.lower()}"
    )
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="reject-audit-http-policy-impossibility",
    )
    assert processor.process_replayed(intent).success
    command = AuditFailureCommand(
        request_id=start.request_id,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error_type="PolymarketHttpStatusError:HTTP_STATUS",
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.HTTP_STATUS,
        retryable=True,
        attempt=attempt,
        status_code=503,
        retry_after_nanoseconds=retry_after_nanoseconds,
        retry_after_disposition=(
            PolymarketRetryAfterDisposition.PARSED
            if retry_after_nanoseconds is not None
            else PolymarketRetryAfterDisposition.ABSENT
        ),
        retry_exhausted=(blocked_reason is PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED),
        retry_blocked_reason=blocked_reason,
    )

    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            command.to_payload(),
            connection_id=start.connection_id,
        )
    )

    assert not result.success
    assert result.error_type == "NormalizationError"
    assert processor.summary().pending_rest_audits == 1
    assert processor.audit_outcomes == ()


def test_saturated_retry_after_handoff_round_trips_through_canonical_replay() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-saturated-retry-after-roundtrip")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="saturated-retry-after-roundtrip",
    )
    assert processor.process_replayed(intent).success
    error = PolymarketHttpStatusError(
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.HTTP_STATUS,
        retryable=True,
        attempt=1,
        detail="oversized Retry-After saturated to durable evidence bound",
        status_code=503,
        retry_after_seconds=MAXIMUM_RETRY_AFTER_EVIDENCE_SECONDS,
        retry_after_disposition=PolymarketRetryAfterDisposition.PARSED,
        retry_exhausted=False,
        retry_blocked_reason=PolymarketRetryBlockedReason.RETRY_AFTER_POLICY,
    )
    command = AuditHttpFailureHandoff.from_http_error(
        request_id=start.request_id,
        connection_id=start.connection_id,
        effective_reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error=error,
    ).to_command()
    assert command.retry_after_nanoseconds == MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS

    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            command.to_payload(),
            connection_id=start.connection_id,
        )
    )
    lifecycle = processor.summary().audit_fault_lifecycle_records[0]

    assert result.success
    assert processor.summary().parse_failures == 0
    assert lifecycle.retry_after_nanoseconds == MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS
    assert lifecycle.retry_blocked_reason is PolymarketRetryBlockedReason.RETRY_AFTER_POLICY


def test_internal_effective_failure_reason_dominates_original_external_transient() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-effective-internal-fault-dominates")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="effective-socket-generation-change",
    )
    assert processor.process_replayed(intent).success
    command = AuditFailureCommand(
        request_id=start.request_id,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error_type="PolymarketHttpTransportError:TIMEOUT",
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.TIMEOUT,
        retryable=True,
        attempt=3,
        retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
        retry_exhausted=True,
        retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
    )

    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            command.to_payload(),
            connection_id=ConnectionId("wrong-failure-control-connection"),
        )
    )
    summary = processor.summary()
    lifecycle = summary.audit_fault_lifecycle_records[0]

    assert result.success
    assert result.audit_outcomes[0].reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
    assert lifecycle.original_reason is AuditFailureReason.HTTP_REQUEST_FAILED
    assert lifecycle.reason is AuditFailureReason.SOCKET_GENERATION_CHANGED
    assert lifecycle.origin is AuditFaultOrigin.INTERNAL
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert summary.audit_faults_current == 0
    assert summary.audit_faults_permanent == 1
    with pytest.raises(ValueError, match="effective internal"):
        replace(
            lifecycle,
            origin=AuditFaultOrigin.EXTERNAL,
            status=AuditFaultStatus.CURRENT,
        )


@pytest.mark.parametrize(
    (
        "label",
        "category",
        "retryable",
        "status_code",
        "failure_v1",
        "expected_origin",
        "expected_resolved",
    ),
    [
        (
            "timeout",
            PolymarketHttpFailureCategory.TIMEOUT,
            True,
            None,
            False,
            AuditFaultOrigin.EXTERNAL,
            True,
        ),
        (
            "status-503",
            PolymarketHttpFailureCategory.HTTP_STATUS,
            True,
            503,
            False,
            AuditFaultOrigin.EXTERNAL,
            True,
        ),
        (
            "response-protocol",
            PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
            False,
            200,
            False,
            AuditFaultOrigin.INDETERMINATE,
            False,
        ),
        (
            "status-404",
            PolymarketHttpFailureCategory.HTTP_STATUS,
            False,
            404,
            False,
            AuditFaultOrigin.EXTERNAL,
            False,
        ),
        (
            "legacy-v1",
            None,
            False,
            None,
            True,
            AuditFaultOrigin.INDETERMINATE,
            False,
        ),
    ],
)
def test_only_proven_transient_audit_faults_clear_on_match_or_resnapshot(
    label: str,
    category: PolymarketHttpFailureCategory | None,
    retryable: bool,
    status_code: int | None,
    failure_v1: bool,
    expected_origin: AuditFaultOrigin,
    expected_resolved: bool,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    run_id = CollectorRunId(f"processor-audit-fault-lifecycle-{label}")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id=f"fault-{label}",
        trigger_reason=AuditTriggerReason.OTHER_TYPED_REASON,
        schema=AUDIT_START_V2_SCHEMA,
    )
    assert processor.process_replayed(intent).success
    if failure_v1:
        command = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            schema=AUDIT_FAILURE_V1_SCHEMA,
        )
    else:
        assert category is not None
        command = AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type=(
                "PolymarketHttpStatusError:HTTP_STATUS"
                if category is PolymarketHttpFailureCategory.HTTP_STATUS
                else f"PolymarketHttpTransportError:{category.value}"
            ),
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=category,
            retryable=retryable,
            attempt=3,
            status_code=status_code,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=retryable,
            retry_blocked_reason=(
                PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED if retryable else None
            ),
        )
    failed = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            command.to_payload(),
            connection_id=start.connection_id,
        )
    )
    failed_summary = processor.summary()
    first_lifecycle = failed_summary.audit_fault_lifecycle_records[0]

    assert failed.success
    assert first_lifecycle.origin is expected_origin
    assert first_lifecycle.status is (
        AuditFaultStatus.CURRENT if expected_resolved else AuditFaultStatus.PERMANENT
    )
    assert first_lifecycle.audit_start_schema == AUDIT_START_V2_SCHEMA
    assert first_lifecycle.failure_schema == (
        AUDIT_FAILURE_V1_SCHEMA if failure_v1 else AUDIT_FAILURE_V2_SCHEMA
    )
    assert (
        first_lifecycle.failure_evidence_kind is AuditFaultFailureEvidenceKind.AUDIT_FAILURE_CONTROL
    )
    assert first_lifecycle.blocks_current_scope is not failure_v1
    assert failed_summary.audit_faults_current == int(expected_resolved)
    assert failed_summary.audit_faults_permanent == int(not expected_resolved)
    assert failed_summary.audit_ineligible_books == 1

    recovery_start, recovery_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id=f"fault-recovery-{label}",
    )
    assert processor.process_replayed(recovery_intent).success
    recovered = processor.process_replayed(
        _audit_response(
            run_id,
            7,
            recovery_start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=0,
        )
    )
    after_match = processor.summary()

    assert recovered.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert after_match.audit_faults_current == 0
    assert after_match.audit_faults_resolved == int(expected_resolved)
    assert after_match.audit_faults_permanent == int(not expected_resolved)
    assert after_match.audit_ineligible_books == int(not expected_resolved and not failure_v1)

    assert processor.process_replayed(
        _envelope(
            run_id,
            8,
            REST_BOOK,
            _fixture("polymarket_rest_book_yes.json"),
        )
    ).success
    after_resnapshot = processor.summary()
    assert after_resnapshot.audit_faults_resolved == int(expected_resolved)
    assert after_resnapshot.audit_faults_permanent == int(not expected_resolved)
    assert after_resnapshot.audit_ineligible_books == int(not expected_resolved and not failure_v1)
    if failure_v1:
        assert after_resnapshot.audit_fault_lifecycle_records == (first_lifecycle,)
        assert first_lifecycle.resolution is None


def test_v2_permanent_failure_blocks_current_scope_with_v1_start() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-v2-permanent-failure-v1-start")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success

    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="v2-permanent-failure-v1-start",
        trigger_reason=AuditTriggerReason.UNKNOWN_LEGACY,
        schema=AUDIT_START_V1_SCHEMA,
    )
    assert processor.process_replayed(intent).success
    failed = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            AuditFailureCommand(
                request_id=start.request_id,
                reason=AuditFailureReason.HTTP_REQUEST_FAILED,
                error_type="PolymarketHttpProtocolError:RESPONSE_PROTOCOL",
                endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                category=PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
                retryable=False,
                attempt=1,
                status_code=200,
                retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
                retry_exhausted=False,
            ).to_payload(),
            connection_id=start.connection_id,
        )
    )
    lifecycle = processor.summary().audit_fault_lifecycle_records[0]

    assert failed.success
    assert lifecycle.audit_start_schema == AUDIT_START_V1_SCHEMA
    assert lifecycle.failure_schema == AUDIT_FAILURE_V2_SCHEMA
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.blocks_current_scope is True

    recovery_start, recovery_intent = _audit_start(
        processor,
        run_id,
        6,
        request_id="v2-permanent-failure-v1-start-later-match",
    )
    assert processor.process_replayed(recovery_intent).success
    recovered = processor.process_replayed(
        _audit_response(
            run_id,
            7,
            recovery_start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=0,
        )
    )
    after_match = processor.summary()

    assert recovered.audit_outcomes[0].outcome is AuditOutcome.MATCH
    assert after_match.audit_faults_permanent == 1
    assert after_match.audit_faults_resolved == 0
    assert after_match.audit_ineligible_books == 1
    assert after_match.audit_fault_lifecycle_records == (lifecycle,)


def test_historical_one_argument_audit_outcome_hook_preserves_v2_fault_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-historical-audit-outcome-hook")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="historical-hook-v2-failure",
    )
    assert processor.process_replayed(intent).success
    command = AuditFailureCommand(
        request_id=start.request_id,
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error_type="PolymarketHttpStatusError:HTTP_STATUS",
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.HTTP_STATUS,
        retryable=True,
        attempt=3,
        status_code=503,
        retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
        retry_exhausted=True,
        retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
    )
    original_record = CanonicalProcessor._record_audit_outcome  # pyright: ignore[reportPrivateUsage]
    captured: list[AuditOutcomeEvidence] = []

    def historical_record(
        hooked_processor: CanonicalProcessor,
        evidence: AuditOutcomeEvidence,
    ) -> None:
        captured.append(evidence)
        original_record(hooked_processor, evidence)

    monkeypatch.setattr(CanonicalProcessor, "_record_audit_outcome", historical_record)
    result = processor.process_replayed(
        _envelope(
            run_id,
            5,
            CONTROL_REST_AUDIT_FAILED,
            command.to_payload(),
            connection_id=start.connection_id,
        )
    )
    lifecycle = processor.summary().audit_fault_current_records[0]

    assert result.success
    assert captured == [result.audit_outcomes[0]]
    assert lifecycle.endpoint_class is PolymarketEndpointClass.CLOB_BOOK
    assert lifecycle.category is PolymarketHttpFailureCategory.HTTP_STATUS
    assert lifecycle.status_code == 503
    assert lifecycle.retryable is True
    assert lifecycle.retry_exhausted is True
    assert lifecycle.retry_blocked_reason is PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED


def test_changed_best_bid_ask_blocks_until_ordered_price_change_reconciles_l2() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-best-bid-ask-ordering")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    owner = next(
        item
        for item in processor.owners
        if item.instrument_id
        == processor.current_bundles[0].instrument_for_token("token-yes").instrument_id
    )
    before = owner.snapshot()
    before_audit = processor.audit_book_identity("token-yes")

    reported = processor.process_replayed(
        _envelope(
            run_id,
            4,
            WS_MARKET,
            _best_bid_ask_payload(
                best_bid="0.46",
                best_ask="0.55",
                spread="0.09",
                timestamp="1787198402000",
            ),
        )
    )

    assert reported.success
    assert not reported.needs_resync
    assert reported.canonical_event_count == 1
    assert reported.scan_count == 1
    assert not reported.book_freshness_updates
    assert reported.scan_triggers[0].reasons == (ScanTriggerReason.BOOK_UPDATE,)
    assert processor.scan_results[-1].disposition is ScanDisposition.BLOCKED
    assert processor.audit_ineligible_instruments == (owner.instrument_id,)
    assert owner.snapshot().economic_content_sha256() == before.economic_content_sha256()
    after_report = processor.audit_book_identity("token-yes")
    assert after_report.economic_content_sha256 == before_audit.economic_content_sha256
    assert after_report.generation == before_audit.generation + 1

    reconciled = processor.process_replayed(
        _envelope(
            run_id,
            5,
            WS_MARKET,
            _top_bid_change_payload(timestamp="1787198402000"),
        )
    )

    assert reconciled.success
    assert not reconciled.needs_resync
    assert reconciled.scan_count == 1
    assert len(reconciled.book_freshness_updates) == 1
    assert processor.audit_ineligible_instruments == ()
    best_bid = owner.snapshot().best_bid
    assert best_bid is not None
    assert str(best_bid.price) == "0.46"
    assert processor.audit_book_identity("token-yes").generation == before_audit.generation + 2


def test_matching_prices_cannot_clear_preexisting_unresolved_best_bid_ask() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-best-bid-ask-out-and-back")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    owner = next(
        item
        for item in processor.owners
        if item.instrument_id
        == processor.current_bundles[0].instrument_for_token("token-yes").instrument_id
    )
    before = owner.snapshot()
    before_audit = processor.audit_book_identity("token-yes")

    for sequence, (best_bid, best_ask, spread) in enumerate(
        (("0.46", "0.54", "0.08"), ("0.45", "0.55", "0.10")),
        start=4,
    ):
        result = processor.process_replayed(
            _envelope(
                run_id,
                sequence,
                WS_MARKET,
                _best_bid_ask_payload(
                    best_bid=best_bid,
                    best_ask=best_ask,
                    spread=spread,
                    timestamp=str(1_787_198_402_000 + sequence),
                ),
            )
        )
        assert result.success
        assert not result.book_freshness_updates
        assert processor.audit_ineligible_instruments == (owner.instrument_id,)
        assert owner.snapshot() == before

    after_bbo = processor.audit_book_identity("token-yes")
    assert after_bbo.generation == before_audit.generation + 2

    reconciled = processor.process_replayed(
        _envelope(
            run_id,
            6,
            WS_MARKET,
            _price_change_payload(
                bid_size="4",
                timestamp="1787198403000",
            ),
        )
    )

    assert reconciled.success
    assert processor.audit_ineligible_instruments == ()
    assert processor.audit_book_identity("token-yes").generation == before_audit.generation + 3


def test_best_bid_ask_is_a_relevant_rest_audit_fence_transition() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-best-bid-ask-audit-fence")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="best-bid-ask-race",
        admitted_ordinal=0,
    )
    assert processor.process_replayed(intent).success
    bbo = _envelope(
        run_id,
        5,
        WS_MARKET,
        _best_bid_ask_payload(
            best_bid="0.46",
            best_ask="0.55",
            spread="0.09",
            timestamp="1787198402000",
        ),
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    assert processor.process_replayed(bbo).success

    raced = processor.process_replayed(
        _audit_response(
            run_id,
            6,
            start,
            _fixture("polymarket_rest_book_yes.json"),
            boundary=1,
        )
    )

    outcome = raced.audit_outcomes[0]
    assert outcome.outcome is AuditOutcome.RACED
    assert outcome.barrier_book is not None
    assert outcome.barrier_book.economic_content_sha256 == (
        outcome.starting_book.economic_content_sha256
    )
    assert outcome.barrier_book.generation == outcome.starting_book.generation + 1


def test_lagging_rest_match_cannot_clear_a_contradictory_pending_best_bid_ask() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    run_id = CollectorRunId("processor-best-bid-ask-stale-rest-match")
    bootstrap = _bootstrap(run_id)
    for envelope in bootstrap:
        assert processor.process_replayed(envelope).success
    owner = next(
        item
        for item in processor.owners
        if item.instrument_id
        == processor.current_bundles[0].instrument_for_token("token-yes").instrument_id
    )
    before = owner.snapshot()
    bbo = _envelope(
        run_id,
        4,
        WS_MARKET,
        _best_bid_ask_payload(
            best_bid="0.46",
            best_ask="0.55",
            spread="0.09",
            timestamp="1787198402000",
        ),
        transport_metadata={"websocket_admission_ordinal": 1},
    )
    assert processor.process_replayed(bbo).success
    start, intent = _audit_start(
        processor,
        run_id,
        5,
        request_id="best-bid-ask-stale-rest-match",
    )
    assert processor.process_replayed(intent).success
    response = _audit_response(
        run_id,
        6,
        start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=1,
    )

    refused = processor.process_replayed(response)

    assert refused.success
    assert not refused.needs_resync
    assert not refused.book_freshness_updates
    assert refused.scan_count == 1
    assert owner.snapshot() == before
    assert len(refused.audit_outcomes) == 1
    outcome = refused.audit_outcomes[0]
    assert outcome.outcome is AuditOutcome.FAILED
    assert outcome.reason is AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE
    assert processor.audit_ineligible_instruments == (owner.instrument_id,)
    failed_summary = processor.summary()
    lifecycle = failed_summary.audit_fault_lifecycle_records[0]
    assert lifecycle.audit_start_schema == AUDIT_START_V2_SCHEMA
    assert lifecycle.status is AuditFaultStatus.PERMANENT
    assert lifecycle.blocks_current_scope is True
    assert failed_summary.audit_fault_lifecycle_reconciled is True
    assert processor.operational_snapshot().audit_fault_lifecycle_reconciled is True

    price_change = _envelope(
        run_id,
        7,
        WS_MARKET,
        _top_bid_change_payload(timestamp="1787198403000"),
        transport_metadata={"websocket_admission_ordinal": 2},
    )
    reconciled = processor.process_replayed(price_change)
    assert reconciled.success
    assert processor.audit_ineligible_instruments == (owner.instrument_id,)
    after_transition = processor.summary()
    assert after_transition.pending_reported_top_transitions == 0
    assert after_transition.audit_faults_permanent == 1
    assert after_transition.audit_ineligible_books == 1
    assert after_transition.audit_fault_lifecycle_reconciled is True
    assert processor.operational_snapshot().audit_fault_lifecycle_reconciled is True
    assert processor.scan_results[-1].disposition is ScanDisposition.BLOCKED

    replayed = CanonicalProcessor(config)
    for envelope in (*bootstrap, bbo, intent, response, price_change):
        assert replayed.process_replayed(envelope).success
    assert replayed.audit_outcomes == processor.audit_outcomes
    assert replayed.summary().terminal_hash == processor.summary().terminal_hash

    processor._audit_ineligible_instruments.discard(  # pyright: ignore[reportPrivateUsage]
        owner.instrument_id
    )
    assert processor.audit_ineligible_instruments == (owner.instrument_id,)
    assert processor.operational_snapshot().audit_fault_lifecycle_reconciled is False
    assert processor.summary().audit_fault_lifecycle_reconciled is False


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
    assert len(rejected.recovery_requests) == 1
    assert rejected.recovery_requests[0].reason is ProcessorRecoveryReason.FRESHNESS_EXPIRY
    assert (
        rejected.recovery_requests[0].effective_invalidation_reason
        is InvalidationReason.STALE_SNAPSHOT
    )
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
    live_result: ProcessedEnvelope | None = None
    for envelope in inputs:
        live_result = live.process_durable(envelope, _receipt(envelope))
        replay_result = replay.process_replayed(envelope)
        assert live_result == replay_result

    assert live_result is not None
    assert len(live_result.recovery_requests) == 1
    assert live_result.recovery_requests[0].reason is ProcessorRecoveryReason.TRANSPORT_DISCONNECT
    assert all(owner.health is BookHealth.INVALID for owner in live.owners)
    assert all(
        owner.snapshot().invalidation_reason is InvalidationReason.DISCONNECT
        for owner in live.owners
    )
    assert live.summary() == replay.summary()
    live_summary = live.summary()
    operational = live.operational_snapshot()
    assert (
        dict(live_summary.recovery_reason_counts)[ProcessorRecoveryReason.TRANSPORT_DISCONNECT] == 1
    )
    assert operational.recovery_reason_counts == live_summary.recovery_reason_counts
    assert operational.recovery_request_chain_sha256 == live_summary.recovery_request_chain_sha256


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
    assert (
        result.reference_refresh_requests[0].recovery_reason
        is ProcessorRecoveryReason.REFERENCE_REFRESH
    )
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


def _resolved_divergence_processor(run_id: CollectorRunId) -> CanonicalProcessor:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="retention-independent-divergence",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(intent).success
    assert processor.process_replayed(
        _audit_response(run_id, 5, start, mismatched_payload, boundary=0)
    ).needs_resync
    assert processor.process_replayed(_envelope(run_id, 6, REST_BOOK, mismatched_payload)).success
    return processor


def _empty_availability_writer_runtime() -> AvailabilityEvidenceRuntimeSnapshot:
    return AvailabilityEvidenceRuntimeSnapshot(
        healthy=True,
        queue_depth=0,
        queue_high_water=0,
        accepted=0,
        written=0,
        rejected=0,
        file_bytes=0,
        unavailable_duration_nanoseconds=0,
        longest_interval_nanoseconds=0,
        open_at_shutdown_intervals=0,
        failure_type=None,
    )


def test_audit_availability_rejects_current_binding_source_and_capture_disagreement() -> None:
    run_id = CollectorRunId("processor-availability-current-binding-reconcile")
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        processor,
        run_id,
        4,
        request_id="availability-current-binding-reconcile",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert processor.process_replayed(intent).success
    assert processor.process_replayed(
        _audit_response(run_id, 5, start, mismatched_payload, boundary=0)
    ).needs_resync

    pending = processor.operational_snapshot()
    pending_runtime = replace(
        ObservationRuntimeHealth().snapshot(),
        audit_availability_authority=pending.audit_availability_authority,
    )
    writer = _empty_availability_writer_runtime()
    assert audit_availability_authority_reconciled(pending, pending_runtime, writer)

    pending_binding = pending.audit_availability_authority.current_bindings[0]
    wrong_pending_binding = replace(pending_binding, request_id="wrong-request")
    wrong_pending_authority = replace(
        pending.audit_availability_authority,
        current_bindings=(wrong_pending_binding,),
    )
    assert not audit_availability_authority_reconciled(
        replace(pending, audit_availability_authority=wrong_pending_authority),
        replace(pending_runtime, audit_availability_authority=wrong_pending_authority),
        writer,
    )

    assert processor.process_replayed(_envelope(run_id, 6, REST_BOOK, mismatched_payload)).success
    resolved = processor.operational_snapshot()
    resolved_runtime = replace(
        pending_runtime,
        audit_availability_authority=resolved.audit_availability_authority,
    )
    assert audit_availability_authority_reconciled(resolved, resolved_runtime, writer)
    capture = resolved.audit_replacement_capture_authority[0]
    binding = capture.availability_binding
    assert binding is not None
    wrong_capture = replace(
        capture,
        availability_binding=replace(binding, request_id="wrong-request"),
    )
    assert not audit_availability_authority_reconciled(
        replace(resolved, audit_replacement_capture_authority=(wrong_capture,)),
        resolved_runtime,
        writer,
    )


def test_summary_exposes_complete_current_capture_without_changing_terminal_identity() -> None:
    _, _, processor, _, release_command = _pending_v2_replacement_release(
        "summary-current-capture-authority"
    )

    before = processor.summary()
    operational = processor.operational_snapshot()
    after = processor.summary()

    assert before.terminal_hash == after.terminal_hash
    assert before.audit_replacement_capture_authority == (
        operational.audit_replacement_capture_authority
    )
    assert before.audit_replacement_captures_pending == len(
        before.audit_replacement_capture_authority
    )
    capture = before.audit_replacement_capture_authority[0]
    terminal_capture_record = release_command.capture_record
    assert capture.instrument_id == release_command.instrument_id
    assert capture.divergence_id == release_command.divergence_id
    assert capture.market_id == release_command.market_id
    assert capture.audit_start_schema == AUDIT_START_V2_SCHEMA
    assert capture.availability_binding == before.audit_availability_authority.current_bindings[0]
    assert capture.pre_open_connection_count == terminal_capture_record["pre_open_connection_count"]
    assert (
        capture.pre_open_connection_ids_sha256
        == terminal_capture_record["pre_open_connection_ids_sha256"]
    )
    assert capture.resolution_ingest_seq == terminal_capture_record["resolution_ingest_seq"]
    assert canonical_json_bytes(capture.resolution_source) == canonical_json_bytes(
        terminal_capture_record["resolution_source"]
    )
    assert canonical_json_bytes(capture.captured_snapshot_sources) == canonical_json_bytes(
        terminal_capture_record["captured_snapshot_sources"]
    )
    assert "audit_start_schema" not in terminal_capture_record
    assert "availability_binding" not in terminal_capture_record


def test_operational_snapshot_matches_full_summary_without_terminal_material() -> None:
    processor = _resolved_divergence_processor(
        CollectorRunId("processor-operational-summary-parity")
    )

    operational = processor.operational_snapshot()
    summary = processor.summary()

    shared_fields = (
        "processed_raw_messages",
        "parse_failures",
        "canonical_events",
        "reference_updates",
        "scans",
        "scan_disposition_counts",
        "candidates",
        "validated_opportunities",
        "invalidations",
        "resyncs",
        "rest_audits",
        "rest_audit_matches",
        "rest_audit_mismatches",
        "rest_audit_raced",
        "rest_audit_failed",
        "audit_start_generation_rebases",
        "audit_mismatch_cause_counts",
        "audit_start_trigger_counts",
        "audit_outcome_trigger_counts",
        "audit_divergences_observed",
        "audit_divergences_resolved_by_authoritative_resnapshot",
        "audit_divergences_resolved_same_generation",
        "audit_divergences_pending",
        "audit_divergences_unresolved",
        "audit_divergences_unknown",
        "audit_local_book_defects",
        "audit_divergence_lifecycle_reconciled",
        "audit_faults_observed",
        "audit_faults_current",
        "audit_faults_resolved",
        "audit_faults_permanent",
        "audit_http_faults_observed",
        "audit_http_faults_resolved",
        "audit_http_faults_permanent",
        "audit_http_retry_exhaustions_observed",
        "audit_http_retry_exhaustions_resolved",
        "audit_http_retry_exhaustions_permanent",
        "audit_http_endpoint_snapshots",
        "audit_fault_lifecycle_reconciled",
        "audit_availability_authority",
        "audit_replacement_captures_pending",
        "audit_replacement_capture_authority",
        "audit_fault_current_instrument_ids",
        "pending_rest_audits",
        "audit_ineligible_books",
        "reported_top_transitions_started",
        "reported_top_transitions_converged",
        "reported_top_transitions_superseded",
        "reported_top_transitions_resnapshotted",
        "reported_top_transitions_expired",
        "reported_top_transitions_invalidated",
        "reported_top_transition_frames",
        "reported_top_transition_duration_total_ns",
        "reported_top_transition_duration_max_ns",
        "reported_top_transition_duration_lower_bound_count",
        "reported_top_transition_entry_counts",
        "reported_top_transition_reason_counts",
        "reported_top_transition_research_eligibility_restored",
        "reported_top_no_pending_recognized",
        "reported_top_no_pending_rejected",
        "pending_reported_top_transitions",
        "reference_refresh_requests",
        "healthy_books",
        "raw_input_chain_sha256",
        "canonical_event_chain_sha256",
        "error_chain_sha256",
        "scan_chain_sha256",
        "validated_opportunity_chain_sha256",
        "reference_refresh_chain_sha256",
        "audit_start_chain_sha256",
        "audit_outcome_chain_sha256",
        "audit_divergence_history_chain_sha256",
        "audit_fault_lifecycle_chain_sha256",
        "reported_top_transition_chain_sha256",
        "audit_divergence_pending_records",
        "reported_top_transitions",
    )
    for field_name in shared_fields:
        assert getattr(operational, field_name) == getattr(summary, field_name)

    full_books = processor.snapshots()
    assert len(operational.books) == len(full_books)
    for compact, full in zip(operational.books, full_books, strict=True):
        assert compact.instrument_id == full.instrument_id
        assert compact.market_id == full.market_id
        assert compact.generation == full.generation
        assert compact.health is full.health
        assert compact.invalidation_reason is full.invalidation_reason
        assert compact.market_version_id == full.market_version_id
        assert compact.precision_rule_version_id == full.precision_rule_version_id
        assert compact.cost_rule_set_version_id == full.cost_rule_set_version_id

    assert operational.audit_mismatch_diagnostic_representatives == (
        summary.audit_mismatch_diagnostics[-1],
    )
    assert operational.audit_divergence_resolution_representatives == (
        summary.audit_divergence_resolution_records[-1],
    )
    assert operational.audit_fault_current_representatives == (summary.audit_fault_current_records)
    assert operational.audit_fault_current_representative_capacity == len(operational.books)
    assert operational.audit_fault_current_representatives_truncated is False
    assert not hasattr(operational, "terminal_hash")
    assert not hasattr(operational, "audit_outcomes")
    assert not hasattr(operational, "audit_mismatch_diagnostics")
    assert not hasattr(operational, "audit_divergence_resolution_records")


def test_operational_snapshot_does_not_iterate_retained_histories_or_build_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor = _resolved_divergence_processor(
        CollectorRunId("processor-operational-no-history-materialization")
    )

    class _NonIterableDeque(deque[object]):
        def __iter__(self):  # type: ignore[no-untyped-def]
            raise AssertionError("operational snapshot iterated retained history")

    class _NonIterableCurrentFaults(dict[str, object]):
        def items(self):  # type: ignore[no-untyped-def]
            raise AssertionError("operational snapshot iterated all current audit faults")

        def values(self):  # type: ignore[no-untyped-def]
            raise AssertionError("operational snapshot iterated all current audit faults")

    private_state = vars(processor)
    for attribute in (
        "_scan_results",
        "_validated_opportunity_hashes",
        "_reference_refresh_requests",
        "_audit_outcomes",
        "_audit_mismatch_diagnostics",
        "_audit_divergence_resolutions",
        "_audit_fault_lifecycle_records",
        "_reported_top_transition_outcomes",
    ):
        retained = cast(deque[object], private_state[attribute])
        private_state[attribute] = _NonIterableDeque(maxlen=retained.maxlen)
    private_state["_current_audit_faults"] = _NonIterableCurrentFaults()

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("operational snapshot entered a terminal/full-depth path")

    monkeypatch.setattr(processor, "summary", forbidden)
    monkeypatch.setattr(processor, "snapshots", forbidden)
    monkeypatch.setattr(processor_module, "canonical_sha256", forbidden)
    for owner in processor.owners:
        monkeypatch.setattr(owner, "snapshot", forbidden)
        monkeypatch.setattr(owner, "content_sha256", forbidden)

    operational = processor.operational_snapshot()

    assert operational.rest_audits == 1
    assert operational.audit_divergences_resolved_by_authoritative_resnapshot == 1
    assert len(operational.audit_mismatch_diagnostic_representatives) == 1
    assert len(operational.audit_divergence_resolution_representatives) == 1
    assert all(item.retained == 0 for item in operational.retention)


def test_audit_history_saturation_preserves_exact_counters_chains_and_terminal_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-audit-history-saturation")
    monkeypatch.setattr(processor_module, "_AUDIT_OUTCOME_HISTORY_LIMIT", 2)
    bounded = CanonicalProcessor(config)
    monkeypatch.setattr(processor_module, "_AUDIT_OUTCOME_HISTORY_LIMIT", 16)
    retained = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert bounded.process_replayed(envelope).success
        assert retained.process_replayed(envelope).success

    for attempt in range(5):
        sequence = 4 + attempt * 2
        start, intent = _audit_start(
            bounded,
            run_id,
            sequence,
            request_id=f"saturated-audit-{attempt}",
            attempt=attempt + 1,
        )
        assert bounded.process_replayed(intent).success
        assert retained.process_replayed(intent).success
        failure = _envelope(
            run_id,
            sequence + 1,
            CONTROL_REST_AUDIT_FAILED,
            AuditFailureCommand(
                request_id=start.request_id,
                reason=AuditFailureReason.RACE_RETRY_EXHAUSTED,
            ).to_payload(),
            connection_id=start.connection_id,
        )
        assert bounded.process_replayed(failure).success
        assert retained.process_replayed(failure).success

    bounded_operational = bounded.operational_snapshot()
    retained_operational = retained.operational_snapshot()
    bounded_summary = bounded.summary()
    retained_summary = retained.summary()
    bounded_retention = bounded_operational.retention_for(ProcessorRetentionDomain.AUDIT_OUTCOMES)

    assert bounded_retention.retained == bounded_retention.capacity == 2
    assert bounded_retention.total == bounded_operational.rest_audits == 5
    assert bounded_retention.truncated
    assert bounded_operational.audit_race_budget_exhaustions == 5
    assert bounded_operational.audit_outcome_chain_sha256 == (
        retained_operational.audit_outcome_chain_sha256
    )
    assert bounded_summary.terminal_hash == retained_summary.terminal_hash
    assert len(bounded_summary.audit_outcomes) == 2
    assert len(retained_summary.audit_outcomes) == 5


def test_terminal_hash_is_independent_of_all_diagnostic_retention_capacities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retention_constants = (
        "_SCAN_RESULT_HISTORY_LIMIT",
        "_VALIDATED_OPPORTUNITY_HISTORY_LIMIT",
        "_REFERENCE_REFRESH_HISTORY_LIMIT",
        "_AUDIT_OUTCOME_HISTORY_LIMIT",
        "_AUDIT_MISMATCH_DIAGNOSTIC_HISTORY_LIMIT",
        "_AUDIT_DIVERGENCE_RESOLUTION_HISTORY_LIMIT",
        "_REPORTED_TOP_TRANSITION_OUTCOME_HISTORY_LIMIT",
    )
    for constant in retention_constants:
        monkeypatch.setattr(processor_module, constant, 0)
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-terminal-retention-independence")
    lean = CanonicalProcessor(config)
    for constant in retention_constants:
        monkeypatch.setattr(processor_module, constant, 16)
    retained = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert lean.process_replayed(envelope).success
        assert retained.process_replayed(envelope).success
    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    start, intent = _audit_start(
        lean,
        run_id,
        4,
        request_id="retention-independent-divergence",
        trigger_reason=AuditTriggerReason.PERIODIC,
    )
    assert lean.process_replayed(intent).success
    assert retained.process_replayed(intent).success
    mismatch = _audit_response(run_id, 5, start, mismatched_payload, boundary=0)
    assert lean.process_replayed(mismatch).needs_resync
    assert retained.process_replayed(mismatch).needs_resync
    resnapshot = _envelope(run_id, 6, REST_BOOK, mismatched_payload)
    assert lean.process_replayed(resnapshot).success
    assert retained.process_replayed(resnapshot).success

    lean_summary = lean.summary()
    retained_summary = retained.summary()

    assert lean_summary.audit_outcomes == ()
    assert lean_summary.audit_mismatch_diagnostics == ()
    assert lean_summary.audit_divergence_resolution_records == ()
    assert retained_summary.audit_outcomes
    assert retained_summary.audit_mismatch_diagnostics
    assert retained_summary.audit_divergence_resolution_records
    assert lean_summary.audit_outcome_chain_sha256 == retained_summary.audit_outcome_chain_sha256
    assert (
        lean_summary.audit_divergence_history_chain_sha256
        == retained_summary.audit_divergence_history_chain_sha256
    )
    assert lean_summary.terminal_hash_schema == retained_summary.terminal_hash_schema
    assert lean_summary.terminal_hash == retained_summary.terminal_hash


def test_terminal_hash_is_independent_of_nonempty_audit_fault_retention_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-fault-retention-independence")
    monkeypatch.setattr(processor_module, "_AUDIT_FAULT_LIFECYCLE_HISTORY_LIMIT", 0)
    lean = CanonicalProcessor(config)
    monkeypatch.setattr(processor_module, "_AUDIT_FAULT_LIFECYCLE_HISTORY_LIMIT", 4)
    retained = CanonicalProcessor(config)
    for envelope in _bootstrap(run_id):
        assert lean.process_replayed(envelope).success
        assert retained.process_replayed(envelope).success

    start, intent = _audit_start(
        lean,
        run_id,
        4,
        request_id="retention-independent-http-fault",
    )
    assert lean.process_replayed(intent).success
    assert retained.process_replayed(intent).success
    failure = _envelope(
        run_id,
        5,
        CONTROL_REST_AUDIT_FAILED,
        AuditFailureCommand(
            request_id=start.request_id,
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpTransportError:TIMEOUT",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.TIMEOUT,
            retryable=True,
            attempt=3,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=True,
            retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
        ).to_payload(),
        connection_id=start.connection_id,
    )
    assert lean.process_replayed(failure).success
    assert retained.process_replayed(failure).success

    lean_current = lean.summary()
    retained_current = retained.summary()
    lean_current_retention = lean.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
    )
    retained_current_retention = retained.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
    )
    assert lean_current.audit_faults_current == retained_current.audit_faults_current == 1
    assert lean_current.audit_fault_lifecycle_records == ()
    assert len(retained_current.audit_fault_lifecycle_records) == 1
    assert (lean_current_retention.capacity, lean_current_retention.retained) == (0, 0)
    assert (retained_current_retention.capacity, retained_current_retention.retained) == (4, 1)
    assert lean_current_retention.total == retained_current_retention.total == 1
    assert lean_current.audit_fault_lifecycle_chain_sha256 == (
        retained_current.audit_fault_lifecycle_chain_sha256
    )
    assert lean_current.terminal_hash == retained_current.terminal_hash

    recovery_start, recovery_intent = _audit_start(
        lean,
        run_id,
        6,
        request_id="retention-independent-http-fault-recovery",
    )
    assert lean.process_replayed(recovery_intent).success
    assert retained.process_replayed(recovery_intent).success
    recovery = _audit_response(
        run_id,
        7,
        recovery_start,
        _fixture("polymarket_rest_book_yes.json"),
        boundary=0,
    )
    assert lean.process_replayed(recovery).success
    assert retained.process_replayed(recovery).success

    lean_resolved = lean.summary()
    retained_resolved = retained.summary()
    lean_resolved_retention = lean.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
    )
    retained_resolved_retention = retained.operational_snapshot().retention_for(
        ProcessorRetentionDomain.AUDIT_FAULT_LIFECYCLES
    )
    assert lean_resolved.audit_faults_current == retained_resolved.audit_faults_current == 0
    assert lean_resolved.audit_faults_resolved == retained_resolved.audit_faults_resolved == 1
    assert lean_resolved.audit_fault_lifecycle_records == ()
    assert len(retained_resolved.audit_fault_lifecycle_records) == 2
    assert (lean_resolved_retention.capacity, lean_resolved_retention.retained) == (0, 0)
    assert (retained_resolved_retention.capacity, retained_resolved_retention.retained) == (4, 2)
    assert lean_resolved_retention.total == retained_resolved_retention.total == 2
    assert lean_resolved.audit_fault_lifecycle_chain_sha256 == (
        retained_resolved.audit_fault_lifecycle_chain_sha256
    )
    assert lean_resolved.terminal_hash == retained_resolved.terminal_hash


def test_processor_history_retention_is_bounded_without_losing_exact_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(processor_module, "_SCAN_RESULT_HISTORY_LIMIT", 2)
    monkeypatch.setattr(processor_module, "_VALIDATED_OPPORTUNITY_HISTORY_LIMIT", 2)
    monkeypatch.setattr(processor_module, "_REFERENCE_REFRESH_HISTORY_LIMIT", 2)
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


def test_terminal_hash_binds_hidden_audit_economic_generation_state() -> None:
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    run_id = CollectorRunId("processor-terminal-audit-generation-binding")
    for envelope in _bootstrap(run_id):
        assert processor.process_replayed(envelope).success
    before = processor.summary()
    before_identity = processor.audit_book_identity("token-yes")

    # Simulate an otherwise invisible corruption of the dedicated generation
    # that fences audit requests. The terminal commitment must detect it even
    # though ProcessorSummary exposes no separate mutable-state field for it.
    generations = cast(
        dict[InstrumentId, int],
        vars(processor)["_audit_economic_generations"],
    )
    generations[before_identity.instrument_id] += 1

    after = processor.summary()
    after_identity = processor.audit_book_identity("token-yes")
    assert after_identity.generation == before_identity.generation + 1
    assert after.terminal_hash != before.terminal_hash
    assert replace(after, terminal_hash=before.terminal_hash) == before


def test_v1_audit_generation_rebase_is_limited_to_exact_failed_producer() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-v1-audit-generation-rebase")

    def stage_with_recorded_generation(
        source_commit: str | None,
    ) -> tuple[CanonicalProcessor, ProcessedEnvelope]:
        processor = CanonicalProcessor(
            config,
            source_journal_git_commit=source_commit,
        )
        assert all(processor.process_replayed(envelope).success for envelope in _bootstrap(run_id))
        start, _envelope_template = _audit_start(
            processor,
            run_id,
            4,
            request_id="v1-generation-rebase",
        )
        recorded = replace(
            start,
            starting_book=replace(
                start.starting_book,
                generation=start.starting_book.generation + 1,
            ),
        )
        envelope = _envelope(
            run_id,
            4,
            CONTROL_REST_AUDIT_START,
            recorded.to_payload(),
            connection_id=recorded.connection_id,
            monotonic_ns=recorded.request_started_monotonic_ns,
        )
        return processor, processor.process_replayed(envelope)

    legacy, legacy_result = stage_with_recorded_generation(
        "a382b93220011428703fe02483f7034f59c1f045"
    )
    ordinary, ordinary_result = stage_with_recorded_generation(None)
    near_miss, near_miss_result = stage_with_recorded_generation(
        "a382b93220011428703fe02483f7034f59c1f0450"
    )
    economic_mismatch = CanonicalProcessor(
        config,
        source_journal_git_commit="a382b93220011428703fe02483f7034f59c1f045",
    )
    assert all(
        economic_mismatch.process_replayed(envelope).success for envelope in _bootstrap(run_id)
    )
    mismatch_start, _ = _audit_start(
        economic_mismatch,
        run_id,
        4,
        request_id="v1-economic-mismatch",
    )
    mismatch_start = replace(
        mismatch_start,
        starting_book=replace(
            mismatch_start.starting_book,
            generation=mismatch_start.starting_book.generation + 1,
            economic_content_sha256="f" * 64,
        ),
    )
    economic_mismatch_result = economic_mismatch.process_replayed(
        _envelope(
            run_id,
            4,
            CONTROL_REST_AUDIT_START,
            mismatch_start.to_payload(),
            connection_id=mismatch_start.connection_id,
            monotonic_ns=mismatch_start.request_started_monotonic_ns,
        )
    )

    assert legacy_result.success
    assert legacy.summary().audit_start_generation_rebases == 1
    assert not ordinary_result.success
    assert ordinary_result.error_type == "NormalizationError"
    assert ordinary.summary().audit_start_generation_rebases == 0
    assert not near_miss_result.success
    assert near_miss.summary().audit_start_generation_rebases == 0
    assert not economic_mismatch_result.success
    assert economic_mismatch.summary().audit_start_generation_rebases == 0
