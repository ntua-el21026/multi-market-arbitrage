from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from multi_market_arbitrage.core.config import Phase1Config, load_phase1_config
from multi_market_arbitrage.core.ids import (
    CollectorRunId,
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    SubscriptionId,
)
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import JournalMetricsSnapshot
from multi_market_arbitrage.observability import (
    HealthCheckpoint,
    HealthDecisionReason,
    HealthEvidenceRuntimeSnapshot,
    InMemoryMetrics,
)
from multi_market_arbitrage.pipeline.audit import (
    AuditBookIdentity,
    AuditOutcome,
    AuditOutcomeEvidence,
)
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
)
from multi_market_arbitrage.pipeline.health import (
    ObservationRuntimeHealth,
    ObservationRuntimeHealthSnapshot,
    ProcessResourceSnapshot,
    build_observation_health_checkpoint,
)
from multi_market_arbitrage.pipeline.ingress import BoundedRawIngress
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor, ProcessorSummary
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    ResolvedPolymarketTarget,
)
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.research import AnalyticalRuntimeSnapshot
from multi_market_arbitrage.state import ImmutableOrderBookSnapshot
from multi_market_arbitrage.venues.polymarket import ReceivedWebSocketMessage
from multi_market_arbitrage.venues.polymarket.public_reads import PublicReadMetricsSnapshot

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"


class _ScopedProcessorView:
    def __init__(
        self,
        processor: CanonicalProcessor,
        snapshots: tuple[ImmutableOrderBookSnapshot, ...],
        *,
        audit_ineligible_instruments: tuple[InstrumentId, ...] = (),
        audit_outcomes: tuple[AuditOutcomeEvidence, ...] | None = None,
    ) -> None:
        self._processor = processor
        self._snapshots = snapshots
        self._audit_ineligible_instruments = audit_ineligible_instruments
        self._audit_outcomes = audit_outcomes

    def summary(self) -> ProcessorSummary:
        summary = replace(
            self._processor.summary(),
            healthy_books=len(self._snapshots),
            audit_ineligible_books=len(self._audit_ineligible_instruments),
        )
        if self._audit_outcomes is None:
            return summary
        return replace(
            summary,
            audit_outcomes=self._audit_outcomes,
            rest_audits=len(self._audit_outcomes),
            rest_audit_matches=sum(
                outcome.outcome is AuditOutcome.MATCH for outcome in self._audit_outcomes
            ),
        )

    def snapshots(self) -> tuple[ImmutableOrderBookSnapshot, ...]:
        return self._snapshots

    @property
    def audit_ineligible_instruments(self) -> tuple[InstrumentId, ...]:
        return self._audit_ineligible_instruments


def _ingress(connection_id: ConnectionId) -> BoundedRawIngress:
    return BoundedRawIngress(
        max_items=8,
        max_bytes=1024,
        connection_id=connection_id,
    )


def _message(payload: bytes, monotonic_ns: int) -> ReceivedWebSocketMessage:
    return ReceivedWebSocketMessage(
        payload_bytes=payload,
        received_wall_time=UtcTimestamp(1_000),
        received_monotonic_ns=monotonic_ns,
        message_type="TEXT",
    )


def _raw_envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
    *,
    received_at: UtcTimestamp,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("pipeline-health-test-connection"),
        subscription_id=SubscriptionId("pipeline-health-test-subscription"),
        channel=channel,
        transport_type="HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=received_at,
        received_monotonic=MonotonicTimestamp(run_id.value, sequence + 1),
        payload_bytes=payload,
        payload_encoding="utf-8",
    )


def _healthy_target_processor(
    config: Phase1Config,
    *,
    updated_at: UtcTimestamp,
) -> tuple[CanonicalProcessor, ResolvedPolymarketTarget]:
    processor = CanonicalProcessor(config)
    run_id = CollectorRunId("pipeline-health-freshness")
    no_book = b"""{
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
    inputs = (
        (
            GAMMA_MARKETS_KEYSET,
            (FIXTURES / "polymarket_gamma_market_keyset.json").read_bytes(),
        ),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            (FIXTURES / "polymarket_clob_market_info.json").read_bytes(),
        ),
        (REST_BOOK, (FIXTURES / "polymarket_rest_book_yes.json").read_bytes()),
        (REST_BOOK, no_book),
    )
    for sequence, (channel, payload) in enumerate(inputs):
        result = processor.process_replayed(
            _raw_envelope(
                run_id,
                sequence,
                channel,
                payload,
                received_at=UtcTimestamp(
                    updated_at.nanoseconds_since_epoch + sequence,
                ),
            )
        )
        assert result.success
    target = ResolvedPolymarketTarget(
        mode=ObservationTargetMode.EXPLICIT_GATE1,
        requested_gamma_market_id="101",
        resolved_gamma_market_id="101",
        condition_id="0xcondition",
        event_id="501",
        yes_token_id="token-yes",  # noqa: S106 - public venue identifier
        no_token_id="token-no",  # noqa: S106 - public venue identifier
        reference_version_ids=("reference",),
        semantics_version_id="semantics",
        selection_rationale="test target",
    )
    snapshots = tuple(
        replace(snapshot, last_valid_update_time=updated_at) for snapshot in processor.snapshots()
    )
    return cast(CanonicalProcessor, _ScopedProcessorView(processor, snapshots)), target


def _checkpoint(
    *,
    config: Phase1Config,
    target: ResolvedPolymarketTarget,
    processor: CanonicalProcessor,
    book_last_valid_receive_monotonic_ns: Mapping[InstrumentId, int],
    emitted_at: UtcTimestamp,
    monotonic_now_ns: int,
    runtime_snapshot: ObservationRuntimeHealthSnapshot | None = None,
) -> HealthCheckpoint:
    if runtime_snapshot is None:
        runtime = ObservationRuntimeHealth()
        connection_id = ConnectionId.new()
        ingress = _ingress(connection_id)
        runtime.begin_generation(connection_id, ingress)
        runtime.mark_connected(now_ns=10)
        runtime_snapshot = runtime.snapshot()
    return build_observation_health_checkpoint(
        config=config,
        collector_run_id=CollectorRunId.new(),
        git_commit="a" * 40,
        target=target,
        runtime=runtime_snapshot,
        processor=processor,
        journal=JournalMetricsSnapshot(
            received=0,
            enqueued=0,
            appended=0,
            durable=0,
            sealed=0,
            verified=0,
            queue_depth=0,
            queue_high_water=0,
            backpressure_failures=0,
            writer_failures=0,
            recovered_records=0,
            truncated_tails=0,
        ),
        journal_failure_type=None,
        analytical=AnalyticalRuntimeSnapshot(
            healthy=True,
            accepted_records=0,
            processed_records=0,
            rejected_records=0,
            queues=(),
            failure_type=None,
        ),
        health_writer=HealthEvidenceRuntimeSnapshot(
            healthy=True,
            queue_depth=0,
            queue_high_water=0,
            accepted=0,
            written=0,
            rejected=0,
            file_bytes=0,
            failure_type=None,
        ),
        public_reads=PublicReadMetricsSnapshot(
            endpoints=(),
            active_attempts=0,
            active_attempts_high_water=0,
        ),
        metrics=InMemoryMetrics(),
        resources=ProcessResourceSnapshot(
            free_disk_bytes=2 * 1024 * 1024 * 1024,
            cpu_user_seconds=1.0,
            cpu_system_seconds=0.5,
            peak_rss_bytes=1024,
        ),
        book_last_valid_receive_monotonic_ns=book_last_valid_receive_monotonic_ns,
        emitted_at=emitted_at,
        monotonic_now_ns=monotonic_now_ns,
    )


def _target_receive_times(
    target: ResolvedPolymarketTarget,
    received_monotonic_ns: int,
) -> dict[InstrumentId, int]:
    return {
        InstrumentId.from_native(
            POLYMARKET_VENUE_ID,
            native_token_id,
        ): received_monotonic_ns
        for native_token_id in (target.yes_token_id, target.no_token_id)
    }


def _match_outcome(
    *,
    target: ResolvedPolymarketTarget,
    response_wall_ns: int,
    response_monotonic_ns: int,
) -> AuditOutcomeEvidence:
    connection_id = ConnectionId("pipeline-health-audit-connection")
    identity = AuditBookIdentity(
        instrument_id=InstrumentId.from_native(
            POLYMARKET_VENUE_ID,
            target.yes_token_id,
        ),
        generation=1,
        economic_content_sha256="a" * 64,
    )
    return AuditOutcomeEvidence(
        request_id="pipeline-health-audit-match",
        connection_id=connection_id,
        native_asset_id=target.yes_token_id,
        attempt=1,
        request_started_monotonic_ns=0,
        starting_admission_ordinal=0,
        starting_processed_ordinal=0,
        starting_book=identity,
        outcome=AuditOutcome.MATCH,
        reason=None,
        response_ingest_id=IngestId("pipeline-health-audit-response"),
        raw_response_sha256="b" * 64,
        response_received_wall_time=UtcTimestamp(response_wall_ns),
        response_received_monotonic_ns=response_monotonic_ns,
        response_boundary_ordinal=0,
        barrier_processed_ordinal=0,
        barrier_book=identity,
        requires_resync=False,
    )


def test_runtime_health_accumulates_generations_and_unavailable_interval() -> None:
    runtime = ObservationRuntimeHealth()
    first_id = ConnectionId.new()
    first = _ingress(first_id)
    runtime.begin_generation(first_id, first)
    runtime.mark_connected(now_ns=10)
    first.admit_nowait(_message(b"first", 11))
    runtime.finish_generation(continuity_interrupted=True, now_ns=20)
    runtime.record_reconnect()

    second_id = ConnectionId.new()
    second = _ingress(second_id)
    runtime.begin_generation(second_id, second)
    runtime.mark_connected(now_ns=30)
    second.admit_nowait(_message(b"second", 31))
    runtime.finish_generation(continuity_interrupted=False, now_ns=40)

    snapshot = runtime.snapshot(now_ns=40)
    assert snapshot.socket_generation == 2
    assert snapshot.connection_id == second_id.value
    assert snapshot.socket_connected is True
    assert snapshot.unavailable_intervals == 1
    assert snapshot.unavailable_total_nanoseconds == 10
    assert snapshot.longest_unavailable_nanoseconds == 10
    assert snapshot.reconnects == 1
    assert snapshot.ingress.received == 2
    assert snapshot.ingress.admitted_bytes == len(b"firstsecond")
    assert snapshot.last_frame_received_monotonic_ns == 31


def test_runtime_health_tracks_longest_completed_and_terminal_unavailable_interval() -> None:
    runtime = ObservationRuntimeHealth()

    first_id = ConnectionId.new()
    runtime.begin_generation(first_id, _ingress(first_id))
    runtime.mark_connected(now_ns=10)
    runtime.finish_generation(continuity_interrupted=True, now_ns=20)

    second_id = ConnectionId.new()
    runtime.begin_generation(second_id, _ingress(second_id))
    runtime.mark_connected(now_ns=30)
    runtime.finish_generation(continuity_interrupted=True, now_ns=40)

    # A failed reconnect generation remains part of the same outage.
    failed_id = ConnectionId.new()
    runtime.begin_generation(failed_id, _ingress(failed_id))
    runtime.finish_generation(continuity_interrupted=True, now_ns=50)

    third_id = ConnectionId.new()
    runtime.begin_generation(third_id, _ingress(third_id))
    runtime.mark_connected(now_ns=70)
    runtime.finish_generation(continuity_interrupted=True, now_ns=80)

    snapshot = runtime.snapshot(now_ns=130)
    assert snapshot.unavailable_intervals == 3
    assert snapshot.unavailable_total_nanoseconds == 40
    assert snapshot.current_unavailable_started_monotonic_ns == 80
    assert snapshot.longest_unavailable_nanoseconds == 50


def test_runtime_health_keeps_failure_domains_explicit() -> None:
    runtime = ObservationRuntimeHealth()
    runtime.record_resync()
    runtime.record_protocol_deviation()
    runtime.record_data_loss()

    snapshot = runtime.snapshot()
    assert snapshot.resyncs == 1
    assert snapshot.protocol_deviations == 1
    assert snapshot.data_loss_count == 1
    assert snapshot.ingress.overflows == 0


def test_checkpoint_builder_emits_all_scoped_groups_and_fail_closed_decision() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    target = ResolvedPolymarketTarget(
        mode=ObservationTargetMode.EXPLICIT_GATE1,
        requested_gamma_market_id="gamma",
        resolved_gamma_market_id="gamma",
        condition_id="condition",
        event_id="event",
        yes_token_id="public-asset-yes",  # noqa: S106 - public venue identifier
        no_token_id="public-asset-no",  # noqa: S106 - public venue identifier
        reference_version_ids=("reference",),
        semantics_version_id="semantics",
        selection_rationale="test target",
    )
    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=CanonicalProcessor(config),
        book_last_valid_receive_monotonic_ns={},
        emitted_at=UtcTimestamp(1_000_000_000),
        monotonic_now_ns=20,
    )

    assert checkpoint.target["requested_gamma_market_id"] == "gamma"
    assert checkpoint.capture["connected"] is True
    assert checkpoint.capture["longest_unavailable_nanoseconds"] == 0
    assert checkpoint.http["active_attempts"] == 0
    assert checkpoint.recovery["longest_unavailable_nanoseconds"] == 0
    assert checkpoint.gate1_eligible_so_far is False
    assert HealthDecisionReason.BOOK_NOT_HEALTHY in checkpoint.reason_codes


def test_checkpoint_book_freshness_accepts_exact_bound_and_scopes_selected_target() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    target_snapshots = processor.snapshots()
    unrelated_instrument = InstrumentId.from_native(
        POLYMARKET_VENUE_ID,
        "unrelated-token",
    )
    unrelated_snapshot = replace(
        target_snapshots[0],
        instrument_id=unrelated_instrument,
        market_id=MarketId.from_native(POLYMARKET_VENUE_ID, "unrelated-condition"),
    )
    scoped_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            (*target_snapshots, unrelated_snapshot),
            audit_ineligible_instruments=(unrelated_instrument,),
        ),
    )
    maximum_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    received_monotonic_ns = 10_000

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=scoped_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            received_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch + maximum_age_ns),
        monotonic_now_ns=received_monotonic_ns + maximum_age_ns,
    )

    expected_instruments = (
        InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id).value,
        InstrumentId.from_native(POLYMARKET_VENUE_ID, target.no_token_id).value,
    )
    assert tuple(book["instrument_id"] for book in checkpoint.books) == expected_instruments
    assert all(book["freshness_age_nanoseconds"] == maximum_age_ns for book in checkpoint.books)
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is True
    assert checkpoint.gate1_eligible_so_far is True
    assert checkpoint.reason_codes == ()


def test_checkpoint_book_freshness_fails_one_nanosecond_over_bound() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    maximum_age_ns = int(config.scanner.maximum_book_age_seconds * 1_000_000_000)
    received_monotonic_ns = 10_000

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            received_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch + maximum_age_ns + 1),
        monotonic_now_ns=received_monotonic_ns + maximum_age_ns + 1,
    )

    assert all(book["freshness_age_nanoseconds"] == maximum_age_ns + 1 for book in checkpoint.books)
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is False
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.BOOK_NOT_HEALTHY,)


@pytest.mark.parametrize("wall_step_ns", [-60_000_000_000, 86_400_000_000_000])
def test_checkpoint_book_freshness_uses_monotonic_age_across_wall_clock_steps(
    wall_step_ns: int,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    received_monotonic_ns = 1_000_000_000
    elapsed_monotonic_ns = 1_000_000_000

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            received_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch + wall_step_ns),
        monotonic_now_ns=received_monotonic_ns + elapsed_monotonic_ns,
    )

    assert all(
        book["freshness_age_nanoseconds"] == elapsed_monotonic_ns for book in checkpoint.books
    )
    assert checkpoint.gate1_eligible_so_far is True
    assert checkpoint.reason_codes == ()


@pytest.mark.parametrize("invalid_receive_time", ["missing", "future"])
def test_checkpoint_book_freshness_fails_closed_on_invalid_monotonic_time(
    invalid_receive_time: str,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    monotonic_now_ns = 10_000
    receive_times = _target_receive_times(target, monotonic_now_ns - 1)
    yes_instrument = InstrumentId.from_native(POLYMARKET_VENUE_ID, target.yes_token_id)
    if invalid_receive_time == "missing":
        del receive_times[yes_instrument]
    else:
        receive_times[yes_instrument] = monotonic_now_ns + 1

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=processor,
        book_last_valid_receive_monotonic_ns=receive_times,
        emitted_at=updated_at,
        monotonic_now_ns=monotonic_now_ns,
    )

    yes_book = next(
        book for book in checkpoint.books if book["instrument_id"] == yes_instrument.value
    )
    assert yes_book["freshness_age_nanoseconds"] is None
    assert checkpoint.capture_healthy is True
    assert checkpoint.research_eligible is False
    assert checkpoint.gate1_eligible_so_far is False
    assert checkpoint.reason_codes == (HealthDecisionReason.BOOK_NOT_HEALTHY,)


@pytest.mark.parametrize("wall_step_ns", [-60_000_000_000, 86_400_000_000_000])
def test_checkpoint_audit_integrity_age_uses_monotonic_response_boundary(
    wall_step_ns: int,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    response_monotonic_ns = 2_000_000_000
    elapsed_ns = 750_000_000
    outcome = _match_outcome(
        target=target,
        response_wall_ns=updated_at.nanoseconds_since_epoch + wall_step_ns,
        response_monotonic_ns=response_monotonic_ns,
    )
    scoped_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_outcomes=(outcome,),
        ),
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=scoped_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            response_monotonic_ns,
        ),
        emitted_at=UtcTimestamp(updated_at.nanoseconds_since_epoch - wall_step_ns),
        monotonic_now_ns=response_monotonic_ns + elapsed_ns,
    )

    assert checkpoint.audit["last_successful_race_free_integrity_age_nanoseconds"] == elapsed_ns


def test_checkpoint_audit_integrity_age_fails_closed_on_future_monotonic_response() -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    updated_at = UtcTimestamp(1_800_000_000_000_000_000)
    processor, target = _healthy_target_processor(config, updated_at=updated_at)
    monotonic_now_ns = 2_000_000_000
    outcome = _match_outcome(
        target=target,
        response_wall_ns=updated_at.nanoseconds_since_epoch,
        response_monotonic_ns=monotonic_now_ns + 1,
    )
    scoped_processor = cast(
        CanonicalProcessor,
        _ScopedProcessorView(
            processor,
            processor.snapshots(),
            audit_outcomes=(outcome,),
        ),
    )
    connection_id = ConnectionId.new()
    future_runtime = ObservationRuntimeHealthSnapshot(
        socket_generation=1,
        connection_id=connection_id.value,
        socket_connected=False,
        last_frame_received_monotonic_ns=monotonic_now_ns + 1,
        unavailable_intervals=1,
        unavailable_total_nanoseconds=0,
        longest_unavailable_nanoseconds=0,
        current_unavailable_started_monotonic_ns=monotonic_now_ns + 1,
        reconnects=0,
        resyncs=0,
        protocol_deviations=0,
        data_loss_count=0,
        ingress=_ingress(connection_id).metrics,
    )

    checkpoint = _checkpoint(
        config=config,
        target=target,
        processor=scoped_processor,
        book_last_valid_receive_monotonic_ns=_target_receive_times(
            target,
            monotonic_now_ns,
        ),
        emitted_at=updated_at,
        monotonic_now_ns=monotonic_now_ns,
        runtime_snapshot=future_runtime,
    )

    assert checkpoint.audit["last_successful_race_free_integrity_age_nanoseconds"] is None
    assert checkpoint.capture["last_frame_age_nanoseconds"] is None
    assert checkpoint.capture["current_unavailable_nanoseconds"] is None
