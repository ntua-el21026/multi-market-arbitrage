from __future__ import annotations

import hashlib
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
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline import processor as processor_module
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID, ReferenceBundle
from multi_market_arbitrage.research import Phase1AnalyticalOutputs
from multi_market_arbitrage.scanners.polymarket_complete_set import (
    CompleteSetScanResult,
    OpportunityLifecycle,
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
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("processor-test-connection"),
        subscription_id=SubscriptionId("processor-test-subscription"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(_BASE_TIME_NS + sequence * 1_000_000_000),
        received_monotonic=MonotonicTimestamp(run_id.value, 100 + sequence),
        payload_bytes=payload,
        payload_encoding="utf-8",
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
        (REST_BOOK_AUDIT, b"{}", "PolymarketSchemaError"),
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

    matching = _envelope(
        run_id,
        4,
        REST_BOOK_AUDIT,
        _fixture("polymarket_rest_book_yes.json"),
    )
    matching_result = processor.process_replayed(matching)
    matched = owner.snapshot()

    assert matching_result.success
    assert not matching_result.needs_resync
    assert matched.generation == before.generation
    assert matched.bids == before.bids
    assert matched.asks == before.asks
    assert processor.summary().scans == scans_before

    mismatched_payload = _fixture("polymarket_rest_book_yes.json").replace(
        b'"price": "0.40", "size": "10"',
        b'"price": "0.40", "size": "11"',
    )
    mismatching = _envelope(run_id, 5, REST_BOOK_AUDIT, mismatched_payload)
    mismatch_result = processor.process_replayed(mismatching)
    summary = processor.summary()

    assert mismatch_result.success
    assert mismatch_result.needs_resync
    assert owner.snapshot().health is BookHealth.INVALID
    assert owner.snapshot().invalidation_reason is InvalidationReason.REST_AUDIT_MISMATCH
    assert summary.rest_audits == 2
    assert summary.rest_audit_mismatches == 1
    assert summary.parse_failures == 0
    assert summary.scans == scans_before


def test_control_and_audit_channels_have_identical_live_and_replay_results() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("processor-control-replay")
    inputs = (
        *_bootstrap(run_id),
        _envelope(
            run_id,
            4,
            REST_BOOK_AUDIT,
            _fixture("polymarket_rest_book_yes.json"),
        ),
        _envelope(
            run_id,
            5,
            CONTROL_BOOK_INVALIDATION,
            canonical_json_bytes({"reason": InvalidationReason.DISCONNECT.value}),
        ),
    )
    live = CanonicalProcessor(config)
    replay = CanonicalProcessor(config)

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
