"""Deterministic evidence contracts for continuous REST book audits."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final, Protocol, cast

from multi_market_arbitrage.core.exact import ExactDecimal, Price, Quantity
from multi_market_arbitrage.core.ids import (
    ConnectionId,
    IngestId,
    InstrumentId,
    MarketId,
    is_sha256_hex,
)
from multi_market_arbitrage.core.provenance import RawSourceRef
from multi_market_arbitrage.core.serialization import canonical_json_bytes, canonical_sha256
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.venues.polymarket.errors import (
    MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS,
    PolymarketEndpointClass,
    PolymarketHttpError,
    PolymarketHttpFailureCategory,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
    validate_polymarket_http_failure_metadata,
)

AUDIT_START_V1_SCHEMA: Final = "mma-rest-audit-start-v1"
AUDIT_START_V2_SCHEMA: Final = "mma-rest-audit-start-v2"
# New producers use v2.  The explicit v1 constant remains available to readers
# that must interpret immutable historical controls without upgrading them.
AUDIT_START_SCHEMA: Final = AUDIT_START_V2_SCHEMA
AUDIT_FAILURE_V1_SCHEMA: Final = "mma-rest-audit-failure-v1"
AUDIT_FAILURE_V2_SCHEMA: Final = "mma-rest-audit-failure-v2"
AUDIT_FAILURE_SCHEMA: Final = AUDIT_FAILURE_V2_SCHEMA
AUDIT_EXPIRY_SCHEMA: Final = "mma-rest-audit-expiry-v1"
AUDIT_CLASSIFICATION_V1_SCHEMA: Final = "mma-rest-audit-classification-v1"
AUDIT_CLASSIFICATION_V2_SCHEMA: Final = "mma-rest-audit-classification-v2"
AUDIT_CLASSIFICATION_SCHEMA: Final = AUDIT_CLASSIFICATION_V2_SCHEMA
AUDIT_CLASSIFICATION_PREPARE_SCHEMA: Final = "mma-rest-audit-classification-prepare-v1"
AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL: Final = "PREPARE_ATTEST_V1"
AUDIT_REPLACEMENT_RELEASE_SCHEMA: Final = "mma-rest-audit-replacement-release-v1"
AUDIT_MISMATCH_DIAGNOSTIC_SCHEMA: Final = "mma-rest-audit-mismatch-diagnostic-v1"
AUDIT_REST_BOOK_IDENTITY_SCHEMA: Final = "mma-rest-audit-rest-book-identity-v1"
AUDIT_EXACT_DEPTH_DIFFERENCE_SCHEMA: Final = "mma-rest-audit-depth-difference-v1"
AUDIT_RAW_SOURCE_SUMMARY_SCHEMA: Final = "mma-rest-audit-raw-source-summary-v1"
AUDIT_DIVERGENCE_PENDING_SCHEMA: Final = "mma-rest-audit-divergence-pending-v1"
AUDIT_DIVERGENCE_RESOLUTION_SCHEMA: Final = "mma-rest-audit-divergence-resolution-v1"
AUDIT_FAULT_LIFECYCLE_SCHEMA: Final = "mma-rest-audit-fault-lifecycle-v1"
AUDIT_REPLACEMENT_CAPTURE_MAX_BYTES: Final = 65_536

# These limits match or exceed the corresponding owner bounds while ensuring
# diagnostic construction cannot become a history-sized allocation.
AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE: Final = 4_096
AUDIT_MISMATCH_MAX_REPRESENTATIVE_DIFFERENCES: Final = 16
AUDIT_MISMATCH_MAX_RAW_SOURCES: Final = 8_193
AUDIT_MISMATCH_MAX_REPRESENTATIVE_RAW_SOURCES: Final = 16
AUDIT_MISMATCH_MAX_CONTEXT_IDENTITIES: Final = 16

_EXACT_DEPTH_IDENTITY_DOMAIN: Final = b"mma-rest-audit-exact-depth-v1\0"
_EXACT_DEPTH_DIFFERENCE_DOMAIN: Final = b"mma-rest-audit-depth-difference-v1\0"
_RAW_SOURCE_SUMMARY_DOMAIN: Final = b"mma-rest-audit-raw-source-summary-v1\0"


class _AuditBookLevel(Protocol):
    @property
    def price(self) -> Price: ...

    @property
    def quantity(self) -> Quantity: ...


class AuditOutcome(StrEnum):
    """Canonical fenced audit result."""

    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    RACED = "RACED"
    FAILED = "FAILED"


class AuditTriggerReason(StrEnum):
    """Finite reason that caused an audit request to be launched."""

    PERIODIC = "PERIODIC"
    FRESHNESS_GUARD = "FRESHNESS_GUARD"
    RACE_RETRY = "RACE_RETRY"
    RESYNC_VERIFICATION = "RESYNC_VERIFICATION"
    STARTUP_BASELINE = "STARTUP_BASELINE"
    OTHER_TYPED_REASON = "OTHER_TYPED_REASON"
    # Historical v1 controls omitted a trigger.  Replay makes that absence
    # explicit; it never guesses a modern reason from surrounding timing.
    UNKNOWN_LEGACY = "UNKNOWN_LEGACY"


class AuditDivergenceResolutionStatus(StrEnum):
    """ADR-0054 resolution outcomes implemented by the immediate fallback."""

    AUTHORITATIVE_RESNAPSHOT = "AUTHORITATIVE_RESNAPSHOT"


class AuditFailureReason(StrEnum):
    """Bounded fail-closed reasons emitted by the audit coordinator."""

    HTTP_REQUEST_FAILED = "HTTP_REQUEST_FAILED"
    OBSERVATION_DEADLINE = "OBSERVATION_DEADLINE"
    RESPONSE_WATERMARK_UNAVAILABLE = "RESPONSE_WATERMARK_UNAVAILABLE"
    WATERMARK_AMBIGUOUS = "WATERMARK_AMBIGUOUS"
    SOCKET_GENERATION_CHANGED = "SOCKET_GENERATION_CHANGED"
    RESPONSE_INVALID = "RESPONSE_INVALID"
    LOCAL_STATE_NOT_COMPARABLE = "LOCAL_STATE_NOT_COMPARABLE"
    RACE_RETRY_EXHAUSTED = "RACE_RETRY_EXHAUSTED"
    RUN_ENDED = "RUN_ENDED"
    BOOK_FRESHNESS_EXPIRED = "BOOK_FRESHNESS_EXPIRED"


_DEADLINE_EFFECTIVE_AUDIT_FAILURE_REASONS: Final = frozenset(
    {
        AuditFailureReason.OBSERVATION_DEADLINE,
        AuditFailureReason.BOOK_FRESHNESS_EXPIRED,
    }
)
_KNOWN_INTERNAL_AUDIT_FAILURE_REASONS: Final = frozenset(
    reason for reason in AuditFailureReason if reason is not AuditFailureReason.HTTP_REQUEST_FAILED
)


def _accepted_audit_http_error_types(
    category: PolymarketHttpFailureCategory,
) -> frozenset[str]:
    if category is PolymarketHttpFailureCategory.HTTP_STATUS:
        error_classes = ("PolymarketHttpStatusError",)
    elif category in {
        PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
        PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
    }:
        # New producers retain the concrete protocol-error class.  The
        # transport spelling remains readable for already durable v2 controls
        # emitted under the original category-only validator.
        error_classes = (
            "PolymarketHttpProtocolError",
            "PolymarketHttpTransportError",
        )
    else:
        error_classes = ("PolymarketHttpTransportError",)
    return frozenset(f"{error_class}:{category.value}" for error_class in error_classes)


class AuditFaultOrigin(StrEnum):
    """Whether retained evidence localizes a fault inside or outside the process."""

    EXTERNAL = "EXTERNAL"
    INTERNAL = "INTERNAL"
    INDETERMINATE = "INDETERMINATE"


class AuditFaultStatus(StrEnum):
    """Current lifecycle state for one durable audit failure."""

    CURRENT = "CURRENT"
    RESOLVED = "RESOLVED"
    PERMANENT = "PERMANENT"


class AuditFaultResolution(StrEnum):
    """Evidence that cleared a non-permanent current audit fault."""

    LATER_FENCED_MATCH = "LATER_FENCED_MATCH"
    AUTHORITATIVE_RESNAPSHOT = "AUTHORITATIVE_RESNAPSHOT"
    REPLACEMENT_CAPTURE_COMPLETED = "REPLACEMENT_CAPTURE_COMPLETED"


class AuditFaultAffectedScope(StrEnum):
    """Fail-closed scopes made unavailable by one audit fault."""

    AUDIT_ELIGIBILITY = "AUDIT_ELIGIBILITY"
    SCANNER_RESEARCH_ELIGIBILITY = "SCANNER_RESEARCH_ELIGIBILITY"


class AuditFaultFailureEvidenceKind(StrEnum):
    """Durable source whose processing produced one FAILED audit outcome."""

    AUDIT_FAILURE_CONTROL = "AUDIT_FAILURE_CONTROL"
    AUDIT_CLASSIFICATION_CONTROL = "AUDIT_CLASSIFICATION_CONTROL"
    REST_AUDIT_RESPONSE = "REST_AUDIT_RESPONSE"
    BOOK_INVALIDATION_CONTROL = "BOOK_INVALIDATION_CONTROL"


def audit_fault_evidence_blocks_current_scope(
    *,
    kind: AuditFaultFailureEvidenceKind,
    audit_start_schema: str,
    failure_schema: str | None,
) -> bool:
    """Return authority from exact durable source kind/schema, never error text."""

    if not isinstance(cast(object, kind), AuditFaultFailureEvidenceKind):
        raise ValueError("audit fault failure evidence kind is unsupported")
    if kind is AuditFaultFailureEvidenceKind.AUDIT_FAILURE_CONTROL:
        if failure_schema == AUDIT_FAILURE_V1_SCHEMA:
            return False
        if failure_schema == AUDIT_FAILURE_V2_SCHEMA:
            return True
        raise ValueError("audit failure-control source schema is unsupported")
    if kind is AuditFaultFailureEvidenceKind.AUDIT_CLASSIFICATION_CONTROL:
        if failure_schema == AUDIT_CLASSIFICATION_V1_SCHEMA:
            return False
        if failure_schema == AUDIT_CLASSIFICATION_V2_SCHEMA:
            return True
        raise ValueError("audit classification-control source schema is unsupported")
    if failure_schema is not None:
        raise ValueError("unversioned audit fault source cannot claim a source schema")
    if audit_start_schema == AUDIT_START_V1_SCHEMA:
        return False
    if audit_start_schema == AUDIT_START_V2_SCHEMA:
        return True
    raise ValueError("audit fault start schema is unsupported")


class AuditMismatchCause(StrEnum):
    """Finite, metric-safe causes for a proven REST/local mismatch.

    The processor owns classification because it has the race fence,
    transition, freshness, and reference context.  The exact comparison
    helpers below deliberately do not infer a cause from depth alone.
    """

    REPORTED_TOP_TRANSITION = "REPORTED_TOP_TRANSITION"
    TRUE_EXACT_DEPTH_DIVERGENCE = "TRUE_EXACT_DEPTH_DIVERGENCE"
    FRESHNESS_RESPONSE_WINDOW_BOUNDARY = "FRESHNESS_RESPONSE_WINDOW_BOUNDARY"
    REFERENCE_PRECISION_TRANSITION = "REFERENCE_PRECISION_TRANSITION"
    LOCAL_BOOK_DEFECT = "LOCAL_BOOK_DEFECT"
    UNKNOWN = "UNKNOWN"


class AuditBookSide(StrEnum):
    """Canonical side labels used by bounded mismatch samples."""

    BID = "BID"
    ASK = "ASK"


@dataclass(frozen=True, slots=True)
class WebSocketAdmissionWatermark:
    """Contiguous successful-admission prefix for one socket generation."""

    connection_id: ConnectionId
    ordinal: int

    def __post_init__(self) -> None:
        if self.ordinal < 0:
            raise ValueError("WebSocket admission watermark cannot be negative")


@dataclass(frozen=True, slots=True)
class AuditBookIdentity:
    """Relevant canonical identity captured by the single book owner.

    ``generation`` counts levels, health, reference transitions, and unresolved
    or restored quantity-free BBO signals that can change or invalidate the
    audited economic book. Diagnostic provenance, freshness, and
    last-trade-only changes do not advance it.
    """

    instrument_id: InstrumentId
    generation: int
    economic_content_sha256: str

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("audit book generation cannot be negative")
        if not is_sha256_hex(self.economic_content_sha256):
            raise ValueError("audit economic-content identity must be SHA-256 hex")


@dataclass(frozen=True, slots=True)
class AuditExactDepthIdentity:
    """Exact, order-independent identity of normalized bid and ask levels."""

    bid_level_count: int
    ask_level_count: int
    content_sha256: str

    def __post_init__(self) -> None:
        if not 0 <= self.bid_level_count <= AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE:
            raise ValueError("audit bid-level count exceeds its diagnostic bound")
        if not 0 <= self.ask_level_count <= AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE:
            raise ValueError("audit ask-level count exceeds its diagnostic bound")
        if not is_sha256_hex(self.content_sha256):
            raise ValueError("audit exact-depth identity must be SHA-256 hex")


@dataclass(frozen=True, slots=True)
class AuditLevelDifference:
    """One bounded representative exact-level difference."""

    side: AuditBookSide
    price: Price
    local_quantity: Quantity | None
    rest_quantity: Quantity | None

    def __post_init__(self) -> None:
        if self.local_quantity is None and self.rest_quantity is None:
            raise ValueError("audit level difference must identify at least one level")
        if self.local_quantity == self.rest_quantity:
            raise ValueError("audit level difference cannot contain equal quantities")


@dataclass(frozen=True, slots=True)
class AuditExactDepthDifference:
    """Bounded summary and full digest of an exact local/REST depth comparison."""

    local_depth: AuditExactDepthIdentity
    rest_depth: AuditExactDepthIdentity
    bid_difference_count: int
    ask_difference_count: int
    local_only_count: int
    rest_only_count: int
    quantity_difference_count: int
    difference_sha256: str
    representatives: tuple[AuditLevelDifference, ...]
    schema: str = field(init=False, default=AUDIT_EXACT_DEPTH_DIFFERENCE_SCHEMA)

    def __post_init__(self) -> None:
        counts = (
            self.bid_difference_count,
            self.ask_difference_count,
            self.local_only_count,
            self.rest_only_count,
            self.quantity_difference_count,
        )
        if min(counts) < 0:
            raise ValueError("audit difference counts cannot be negative")
        if self.difference_count != (
            self.local_only_count + self.rest_only_count + self.quantity_difference_count
        ):
            raise ValueError("audit depth-difference counts do not reconcile")
        maximum_possible = (
            self.local_depth.bid_level_count
            + self.local_depth.ask_level_count
            + self.rest_depth.bid_level_count
            + self.rest_depth.ask_level_count
        )
        if self.difference_count > maximum_possible:
            raise ValueError("audit depth-difference count exceeds the compared inputs")
        if not is_sha256_hex(self.difference_sha256):
            raise ValueError("audit depth-difference identity must be SHA-256 hex")
        if len(self.representatives) > AUDIT_MISMATCH_MAX_REPRESENTATIVE_DIFFERENCES:
            raise ValueError("audit depth-difference representatives exceed their bound")
        if len(self.representatives) > self.difference_count:
            raise ValueError("audit depth-difference sample exceeds its population")
        if self.difference_count == 0 and self.representatives:
            raise ValueError("equal audit depths cannot carry difference representatives")

    @property
    def difference_count(self) -> int:
        return self.bid_difference_count + self.ask_difference_count

    @property
    def representatives_truncated(self) -> bool:
        return len(self.representatives) < self.difference_count


@dataclass(frozen=True, slots=True)
class AuditRawSourceSummary:
    """Bounded representatives plus a full ordered digest of raw identities."""

    source_count: int
    ordered_sha256: str
    representatives: tuple[RawSourceRef, ...]
    schema: str = field(init=False, default=AUDIT_RAW_SOURCE_SUMMARY_SCHEMA)

    def __post_init__(self) -> None:
        if not 0 <= self.source_count <= AUDIT_MISMATCH_MAX_RAW_SOURCES:
            raise ValueError("audit raw-source count exceeds its bound")
        if not is_sha256_hex(self.ordered_sha256):
            raise ValueError("audit raw-source identity must be SHA-256 hex")
        if len(self.representatives) > AUDIT_MISMATCH_MAX_REPRESENTATIVE_RAW_SOURCES:
            raise ValueError("audit raw-source representatives exceed their bound")
        if len(self.representatives) > self.source_count:
            raise ValueError("audit raw-source sample exceeds its population")
        if len({source.ingest_id for source in self.representatives}) != len(self.representatives):
            raise ValueError("audit raw-source representatives contain duplicate identities")

    @property
    def representatives_truncated(self) -> bool:
        return len(self.representatives) < self.source_count


@dataclass(frozen=True, slots=True)
class AuditRestBookIdentity:
    """Exact normalized REST identity, separate from the opaque venue hash."""

    instrument_id: InstrumentId
    market_id: MarketId
    depth: AuditExactDepthIdentity
    tick_size: ExactDecimal
    minimum_order_size: Quantity
    neg_risk: bool
    normalized_content_sha256: str
    schema: str = field(init=False, default=AUDIT_REST_BOOK_IDENTITY_SCHEMA)

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.normalized_content_sha256):
            raise ValueError("audit normalized REST identity must be SHA-256 hex")
        if self.normalized_content_sha256 != _rest_book_content_sha256(
            instrument_id=self.instrument_id,
            market_id=self.market_id,
            depth=self.depth,
            tick_size=self.tick_size,
            minimum_order_size=self.minimum_order_size,
            neg_risk=self.neg_risk,
        ):
            raise ValueError("audit normalized REST identity does not match its exact content")

    @classmethod
    def create(
        cls,
        *,
        instrument_id: InstrumentId,
        market_id: MarketId,
        depth: AuditExactDepthIdentity,
        tick_size: ExactDecimal,
        minimum_order_size: Quantity,
        neg_risk: bool,
    ) -> AuditRestBookIdentity:
        return cls(
            instrument_id=instrument_id,
            market_id=market_id,
            depth=depth,
            tick_size=tick_size,
            minimum_order_size=minimum_order_size,
            neg_risk=neg_risk,
            normalized_content_sha256=_rest_book_content_sha256(
                instrument_id=instrument_id,
                market_id=market_id,
                depth=depth,
                tick_size=tick_size,
                minimum_order_size=minimum_order_size,
                neg_risk=neg_risk,
            ),
        )


@dataclass(frozen=True, slots=True)
class AuditMismatchDiagnostic:
    """Immutable, bounded evidence retained with one proven mismatch outcome."""

    cause: AuditMismatchCause
    local_book: AuditBookIdentity
    rest_book: AuditRestBookIdentity
    exact_depth_difference: AuditExactDepthDifference
    local_raw_sources: AuditRawSourceSummary
    rest_raw_sources: AuditRawSourceSummary
    response_source: RawSourceRef
    context_evidence_sha256s: tuple[str, ...] = ()
    schema: str = field(init=False, default=AUDIT_MISMATCH_DIAGNOSTIC_SCHEMA)

    def __post_init__(self) -> None:
        if self.local_book.instrument_id != self.rest_book.instrument_id:
            raise ValueError("audit local and REST identities belong to different instruments")
        if self.exact_depth_difference.rest_depth != self.rest_book.depth:
            raise ValueError("audit REST identity and depth comparison disagree")
        if (
            self.cause is AuditMismatchCause.TRUE_EXACT_DEPTH_DIVERGENCE
            and self.exact_depth_difference.difference_count == 0
        ):
            raise ValueError("true exact-depth divergence requires an exact level difference")
        if len(self.context_evidence_sha256s) > AUDIT_MISMATCH_MAX_CONTEXT_IDENTITIES:
            raise ValueError("audit mismatch context identities exceed their bound")
        if any(not is_sha256_hex(value) for value in self.context_evidence_sha256s):
            raise ValueError("audit mismatch context identities must be SHA-256 hex")
        normalized_context = tuple(sorted(set(self.context_evidence_sha256s)))
        object.__setattr__(self, "context_evidence_sha256s", normalized_context)


def _normalized_exact_side(
    levels: Sequence[_AuditBookLevel],
    *,
    reverse: bool,
) -> tuple[tuple[Price, Quantity], ...]:
    if len(levels) > AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE:
        raise ValueError("audit book side exceeds 4096 exact levels")
    by_price: dict[Price, Quantity] = {}
    for level in levels:
        price = level.price
        quantity = level.quantity
        if price in by_price:
            raise ValueError("audit book side contains a duplicate exact price")
        by_price[price] = quantity
    return tuple(
        sorted(
            by_price.items(),
            key=lambda item: item[0].value,
            reverse=reverse,
        )
    )


def _depth_identity_from_normalized(
    bids: tuple[tuple[Price, Quantity], ...],
    asks: tuple[tuple[Price, Quantity], ...],
) -> AuditExactDepthIdentity:
    digest = hashlib.sha256()
    digest.update(_EXACT_DEPTH_IDENTITY_DOMAIN)
    for side, levels in ((AuditBookSide.BID, bids), (AuditBookSide.ASK, asks)):
        side_bytes = side.value.encode("ascii")
        digest.update(len(side_bytes).to_bytes(8, "big"))
        digest.update(side_bytes)
        digest.update(len(levels).to_bytes(8, "big"))
        for price, quantity in levels:
            payload = canonical_json_bytes(
                {
                    "price": price,
                    "quantity": quantity,
                }
            )
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return AuditExactDepthIdentity(
        bid_level_count=len(bids),
        ask_level_count=len(asks),
        content_sha256=digest.hexdigest(),
    )


def audit_exact_depth_identity(
    bids: Sequence[_AuditBookLevel],
    asks: Sequence[_AuditBookLevel],
) -> AuditExactDepthIdentity:
    """Return a bounded, order-independent exact identity for two book sides."""

    normalized_bids = _normalized_exact_side(bids, reverse=True)
    normalized_asks = _normalized_exact_side(asks, reverse=False)
    return _depth_identity_from_normalized(normalized_bids, normalized_asks)


def compare_audit_exact_depth(
    *,
    local_bids: Sequence[_AuditBookLevel],
    local_asks: Sequence[_AuditBookLevel],
    rest_bids: Sequence[_AuditBookLevel],
    rest_asks: Sequence[_AuditBookLevel],
) -> AuditExactDepthDifference:
    """Compare exact levels without inferring a processor-context mismatch cause."""

    normalized_local_bids = _normalized_exact_side(local_bids, reverse=True)
    normalized_local_asks = _normalized_exact_side(local_asks, reverse=False)
    normalized_rest_bids = _normalized_exact_side(rest_bids, reverse=True)
    normalized_rest_asks = _normalized_exact_side(rest_asks, reverse=False)
    local_depth = _depth_identity_from_normalized(normalized_local_bids, normalized_local_asks)
    rest_depth = _depth_identity_from_normalized(normalized_rest_bids, normalized_rest_asks)

    digest = hashlib.sha256()
    digest.update(_EXACT_DEPTH_DIFFERENCE_DOMAIN)
    representatives: list[AuditLevelDifference] = []
    bid_difference_count = 0
    ask_difference_count = 0
    local_only_count = 0
    rest_only_count = 0
    quantity_difference_count = 0

    side_inputs = (
        (AuditBookSide.BID, normalized_local_bids, normalized_rest_bids, True),
        (AuditBookSide.ASK, normalized_local_asks, normalized_rest_asks, False),
    )
    for side, local_levels, rest_levels, reverse in side_inputs:
        local_by_price = dict(local_levels)
        rest_by_price = dict(rest_levels)
        prices = sorted(
            local_by_price.keys() | rest_by_price.keys(),
            key=lambda price: price.value,
            reverse=reverse,
        )
        for price in prices:
            local_quantity = local_by_price.get(price)
            rest_quantity = rest_by_price.get(price)
            if local_quantity == rest_quantity:
                continue
            difference = AuditLevelDifference(
                side=side,
                price=price,
                local_quantity=local_quantity,
                rest_quantity=rest_quantity,
            )
            payload = canonical_json_bytes(difference)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
            if len(representatives) < AUDIT_MISMATCH_MAX_REPRESENTATIVE_DIFFERENCES:
                representatives.append(difference)
            if side is AuditBookSide.BID:
                bid_difference_count += 1
            else:
                ask_difference_count += 1
            if local_quantity is None:
                rest_only_count += 1
            elif rest_quantity is None:
                local_only_count += 1
            else:
                quantity_difference_count += 1

    return AuditExactDepthDifference(
        local_depth=local_depth,
        rest_depth=rest_depth,
        bid_difference_count=bid_difference_count,
        ask_difference_count=ask_difference_count,
        local_only_count=local_only_count,
        rest_only_count=rest_only_count,
        quantity_difference_count=quantity_difference_count,
        difference_sha256=digest.hexdigest(),
        representatives=tuple(representatives),
    )


def _bounded_raw_sources(raw_sources: Iterable[RawSourceRef]) -> tuple[RawSourceRef, ...]:
    bounded: list[RawSourceRef] = []
    seen_ingest_ids: set[IngestId] = set()
    for source in raw_sources:
        if source.ingest_id in seen_ingest_ids:
            raise ValueError("audit raw sources contain a duplicate ingest identity")
        if len(bounded) >= AUDIT_MISMATCH_MAX_RAW_SOURCES:
            raise ValueError("audit raw sources exceed 8193 identities")
        seen_ingest_ids.add(source.ingest_id)
        bounded.append(source)
    return tuple(bounded)


def summarize_audit_raw_sources(
    raw_sources: Iterable[RawSourceRef],
) -> AuditRawSourceSummary:
    """Digest all ordered raw identities while retaining only a bounded sample."""

    sources = _bounded_raw_sources(raw_sources)
    digest = hashlib.sha256()
    digest.update(_RAW_SOURCE_SUMMARY_DOMAIN)
    for source in sources:
        payload = canonical_json_bytes(source)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return AuditRawSourceSummary(
        source_count=len(sources),
        ordered_sha256=digest.hexdigest(),
        representatives=sources[:AUDIT_MISMATCH_MAX_REPRESENTATIVE_RAW_SOURCES],
    )


def _rest_book_content_sha256(
    *,
    instrument_id: InstrumentId,
    market_id: MarketId,
    depth: AuditExactDepthIdentity,
    tick_size: ExactDecimal,
    minimum_order_size: Quantity,
    neg_risk: bool,
) -> str:
    return canonical_sha256(
        {
            "depth": depth,
            "instrument_id": instrument_id,
            "market_id": market_id,
            "minimum_order_size": minimum_order_size,
            "neg_risk": neg_risk,
            "schema": AUDIT_REST_BOOK_IDENTITY_SCHEMA,
            "tick_size": tick_size,
        }
    )


def build_audit_mismatch_diagnostic(
    *,
    cause: AuditMismatchCause,
    local_book: AuditBookIdentity,
    local_bids: Sequence[_AuditBookLevel],
    local_asks: Sequence[_AuditBookLevel],
    local_raw_sources: Iterable[RawSourceRef],
    rest_instrument_id: InstrumentId,
    rest_market_id: MarketId,
    rest_bids: Sequence[_AuditBookLevel],
    rest_asks: Sequence[_AuditBookLevel],
    rest_tick_size: ExactDecimal,
    rest_minimum_order_size: Quantity,
    rest_neg_risk: bool,
    rest_raw_sources: Iterable[RawSourceRef],
    response_source: RawSourceRef,
    context_evidence_sha256s: tuple[str, ...] = (),
) -> AuditMismatchDiagnostic:
    """Build self-contained mismatch evidence from processor-owned exact inputs."""

    difference = compare_audit_exact_depth(
        local_bids=local_bids,
        local_asks=local_asks,
        rest_bids=rest_bids,
        rest_asks=rest_asks,
    )
    rest_sources = _bounded_raw_sources(rest_raw_sources)
    if response_source not in rest_sources:
        raise ValueError("audit REST sources do not contain the response raw identity")
    return AuditMismatchDiagnostic(
        cause=cause,
        local_book=local_book,
        rest_book=AuditRestBookIdentity.create(
            instrument_id=rest_instrument_id,
            market_id=rest_market_id,
            depth=difference.rest_depth,
            tick_size=rest_tick_size,
            minimum_order_size=rest_minimum_order_size,
            neg_risk=rest_neg_risk,
        ),
        exact_depth_difference=difference,
        local_raw_sources=summarize_audit_raw_sources(local_raw_sources),
        rest_raw_sources=summarize_audit_raw_sources(rest_sources),
        response_source=response_source,
        context_evidence_sha256s=context_evidence_sha256s,
    )


@dataclass(frozen=True, slots=True)
class AuditStartEvidence:
    """Durable intent defining the conservative audit race window."""

    request_id: str
    connection_id: ConnectionId
    native_asset_id: str
    attempt: int
    request_started_monotonic_ns: int
    starting_admission_watermark: WebSocketAdmissionWatermark
    starting_processed_watermark: WebSocketAdmissionWatermark
    starting_book: AuditBookIdentity
    trigger_reason: AuditTriggerReason = AuditTriggerReason.OTHER_TYPED_REASON
    schema: str = AUDIT_START_SCHEMA

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if not self.native_asset_id or len(self.native_asset_id) > 2048:
            raise ValueError("audit native asset ID must be non-empty and bounded")
        if self.attempt <= 0:
            raise ValueError("audit attempt must be positive")
        if self.request_started_monotonic_ns < 0:
            raise ValueError("audit request-start monotonic time cannot be negative")
        if self.starting_admission_watermark.connection_id != self.connection_id:
            raise ValueError("audit admission watermark belongs to another connection")
        if self.starting_processed_watermark.connection_id != self.connection_id:
            raise ValueError("audit processed watermark belongs to another connection")
        if self.starting_processed_watermark.ordinal > self.starting_admission_watermark.ordinal:
            raise ValueError("processed watermark cannot exceed admitted watermark")
        if self.schema == AUDIT_START_V1_SCHEMA:
            if self.trigger_reason is not AuditTriggerReason.UNKNOWN_LEGACY:
                raise ValueError("v1 audit start must expose UNKNOWN_LEGACY trigger")
        elif self.schema == AUDIT_START_V2_SCHEMA:
            if self.trigger_reason is AuditTriggerReason.UNKNOWN_LEGACY:
                raise ValueError("v2 audit start cannot claim a legacy-unknown trigger")
        else:
            raise ValueError("audit start schema is unsupported")

    def to_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "attempt": self.attempt,
            "connection_id": self.connection_id.value,
            "instrument_id": self.starting_book.instrument_id.value,
            "native_asset_id": self.native_asset_id,
            "request_id": self.request_id,
            "request_started_monotonic_ns": self.request_started_monotonic_ns,
            "schema": self.schema,
            "starting_admission_ordinal": self.starting_admission_watermark.ordinal,
            "starting_book_economic_sha256": (self.starting_book.economic_content_sha256),
            "starting_book_generation": self.starting_book.generation,
            "starting_processed_ordinal": self.starting_processed_watermark.ordinal,
        }
        if self.schema == AUDIT_START_V2_SCHEMA:
            record["trigger_reason"] = self.trigger_reason.value
        return record

    def to_payload(self) -> bytes:
        return canonical_json_bytes(self.to_record())


@dataclass(frozen=True, slots=True)
class AuditFailureCommand:
    """Durable command completing one staged audit as FAILED."""

    request_id: str
    reason: AuditFailureReason
    error_type: str | None = None
    endpoint_class: PolymarketEndpointClass | None = None
    category: PolymarketHttpFailureCategory | None = None
    retryable: bool | None = None
    attempt: int | None = None
    status_code: int | None = None
    retry_after_nanoseconds: int | None = None
    retry_after_disposition: PolymarketRetryAfterDisposition | None = None
    retry_exhausted: bool | None = None
    retry_blocked_reason: PolymarketRetryBlockedReason | None = None
    schema: str = AUDIT_FAILURE_SCHEMA

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")

        if self.schema == AUDIT_FAILURE_V1_SCHEMA:
            if any(
                value is not None
                for value in (
                    self.error_type,
                    self.endpoint_class,
                    self.category,
                    self.retryable,
                    self.attempt,
                    self.status_code,
                    self.retry_after_nanoseconds,
                    self.retry_after_disposition,
                    self.retry_exhausted,
                    self.retry_blocked_reason,
                )
            ):
                raise ValueError("v1 audit failure cannot carry v2 metadata")
            return
        if self.schema != AUDIT_FAILURE_V2_SCHEMA:
            raise ValueError("audit failure schema is unsupported")
        if self.error_type is not None and (not self.error_type or len(self.error_type) > 256):
            raise ValueError("audit failure type must be bounded non-empty text")
        http_values = (
            self.endpoint_class,
            self.category,
            self.retryable,
            self.attempt,
            self.retry_after_disposition,
            self.retry_exhausted,
        )
        has_http_metadata = any(value is not None for value in http_values) or any(
            value is not None
            for value in (
                self.status_code,
                self.retry_after_nanoseconds,
                self.retry_blocked_reason,
            )
        )
        if has_http_metadata and any(value is None for value in http_values):
            raise ValueError("audit HTTP failure metadata must be complete")
        deadline_evidence_for_effective_reason = (
            self.reason in _DEADLINE_EFFECTIVE_AUDIT_FAILURE_REASONS
            and self.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
            and self.retryable is False
        )
        if (
            has_http_metadata
            and self.reason is not AuditFailureReason.HTTP_REQUEST_FAILED
            and not deadline_evidence_for_effective_reason
        ):
            raise ValueError(
                "non-HTTP audit failure reason cannot carry HTTP metadata other than "
                "non-retryable deadline evidence"
            )
        if (
            self.endpoint_class is not None
            and self.endpoint_class is not PolymarketEndpointClass.CLOB_BOOK
        ):
            raise ValueError("audit HTTP failure must name the CLOB book endpoint")
        if self.attempt is not None and self.attempt < 0:
            raise ValueError("audit HTTP failure attempt cannot be negative")
        if (
            self.attempt == 0
            and self.category is not PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
        ):
            raise ValueError("audit HTTP attempt zero is reserved for pre-attempt deadline expiry")
        if self.status_code is not None and not 100 <= self.status_code <= 599:
            raise ValueError("audit HTTP status must be between 100 and 599")
        if self.retry_after_nanoseconds is not None and not (
            0 <= self.retry_after_nanoseconds <= MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS
        ):
            raise ValueError("audit Retry-After duration is outside its evidence bound")
        if (self.retry_after_disposition is PolymarketRetryAfterDisposition.PARSED) != (
            self.retry_after_nanoseconds is not None
        ):
            raise ValueError("audit Retry-After value and disposition disagree")
        if has_http_metadata:
            accepted_error_types = _accepted_audit_http_error_types(
                cast(PolymarketHttpFailureCategory, self.category)
            )
            if self.error_type not in accepted_error_types:
                raise ValueError("audit HTTP failure type contradicts its typed category")
            validate_polymarket_http_failure_metadata(
                category=cast(PolymarketHttpFailureCategory, self.category),
                retryable=cast(bool, self.retryable),
                status_code=self.status_code,
                retry_after_disposition=cast(
                    PolymarketRetryAfterDisposition,
                    self.retry_after_disposition,
                ),
                has_retry_after=self.retry_after_nanoseconds is not None,
                retry_exhausted=cast(bool, self.retry_exhausted),
                retry_blocked_reason=self.retry_blocked_reason,
                final_failure=True,
            )

    def validate_against_public_read_policy(
        self,
        *,
        maximum_attempts: int,
        maximum_retry_after_nanoseconds: int,
    ) -> None:
        """Bind v2 final-failure evidence to the frozen adapter policy."""

        if self.schema == AUDIT_FAILURE_V1_SCHEMA or self.attempt is None:
            return
        if maximum_attempts <= 0:
            raise ValueError("public-read maximum attempts must be positive")
        if maximum_retry_after_nanoseconds <= 0:
            raise ValueError("public-read maximum Retry-After must be positive")
        if self.attempt > maximum_attempts:
            raise ValueError("audit HTTP attempt exceeds the configured public-read bound")
        blocked_reason = self.retry_blocked_reason
        if blocked_reason is PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED:
            if self.attempt != maximum_attempts:
                raise ValueError(
                    "attempt-exhaustion evidence must end on the configured final attempt"
                )
            return
        if (
            blocked_reason
            in {
                PolymarketRetryBlockedReason.DEADLINE,
                PolymarketRetryBlockedReason.RETRY_AFTER_POLICY,
            }
            and self.attempt >= maximum_attempts
        ):
            raise ValueError("deadline/Retry-After policy cannot override final-attempt exhaustion")
        if blocked_reason is PolymarketRetryBlockedReason.RETRY_AFTER_POLICY:
            if (
                self.retry_after_nanoseconds is None
                or self.retry_after_nanoseconds <= maximum_retry_after_nanoseconds
            ):
                raise ValueError(
                    "Retry-After policy block requires a delay above the configured bound"
                )
        elif (
            self.retry_after_nanoseconds is not None
            and self.retry_after_nanoseconds > maximum_retry_after_nanoseconds
        ):
            raise ValueError(
                "above-policy Retry-After evidence requires a Retry-After policy block"
            )

    @classmethod
    def from_http_error(
        cls,
        *,
        request_id: str,
        effective_reason: AuditFailureReason,
        error: PolymarketHttpError,
    ) -> AuditFailureCommand:
        retry_after_nanoseconds = (
            None
            if error.retry_after_seconds is None
            else round(error.retry_after_seconds * 1_000_000_000)
        )
        return cls(
            request_id=request_id,
            # The command retains the coordinator's effective reason while the
            # typed HTTP category records the original adapter failure.  The
            # cross-field validator permits this pairing only for proven,
            # non-retryable deadline expiry.
            reason=effective_reason,
            error_type=f"{type(error).__name__}:{error.category.value}",
            endpoint_class=error.endpoint_class,
            category=error.category,
            retryable=error.retryable,
            attempt=error.attempt,
            status_code=error.status_code,
            retry_after_nanoseconds=retry_after_nanoseconds,
            retry_after_disposition=error.retry_after_disposition,
            retry_exhausted=error.retry_exhausted,
            retry_blocked_reason=error.retry_blocked_reason,
        )

    def to_record(self) -> dict[str, object]:
        if self.schema == AUDIT_FAILURE_V1_SCHEMA:
            return {
                "reason": self.reason.value,
                "request_id": self.request_id,
                "schema": self.schema,
            }
        return {
            "category": None if self.category is None else self.category.value,
            "endpoint_class": (None if self.endpoint_class is None else self.endpoint_class.value),
            "error_type": self.error_type or self.reason.value,
            "http_attempt": self.attempt,
            "reason": self.reason.value,
            "request_id": self.request_id,
            "retry_after_disposition": (
                None if self.retry_after_disposition is None else self.retry_after_disposition.value
            ),
            "retry_after_nanoseconds": self.retry_after_nanoseconds,
            "retry_blocked_reason": (
                None if self.retry_blocked_reason is None else self.retry_blocked_reason.value
            ),
            "retry_exhausted": self.retry_exhausted,
            "retryable": self.retryable,
            "schema": self.schema,
            "status_code": self.status_code,
        }

    def to_payload(self) -> bytes:
        return canonical_json_bytes(self.to_record())


@dataclass(frozen=True, slots=True)
class AuditFaultLifecycleRecord:
    """Current or terminal state of one durable audit-failure observation."""

    fault_id: str
    request_id: str
    instrument_id: InstrumentId
    market_id: MarketId
    native_asset_id: str
    original_reason: AuditFailureReason
    reason: AuditFailureReason
    origin: AuditFaultOrigin
    status: AuditFaultStatus
    request_started_monotonic_ns: int
    audit_start_source: RawSourceRef
    failure_source: RawSourceRef
    observed_at: UtcTimestamp
    observed_monotonic_ns: int
    error_type: str
    audit_start_schema: str
    failure_evidence_kind: AuditFaultFailureEvidenceKind
    failure_schema: str | None
    blocks_current_scope: bool
    affected_scopes: tuple[AuditFaultAffectedScope, ...] = (
        AuditFaultAffectedScope.AUDIT_ELIGIBILITY,
        AuditFaultAffectedScope.SCANNER_RESEARCH_ELIGIBILITY,
    )
    response_completed_monotonic_ns: int | None = None
    endpoint_class: PolymarketEndpointClass | None = None
    category: PolymarketHttpFailureCategory | None = None
    retryable: bool | None = None
    attempt: int | None = None
    status_code: int | None = None
    retry_after_nanoseconds: int | None = None
    retry_after_disposition: PolymarketRetryAfterDisposition | None = None
    retry_exhausted: bool | None = None
    retry_blocked_reason: PolymarketRetryBlockedReason | None = None
    resolution: AuditFaultResolution | None = None
    resolution_source: RawSourceRef | None = None
    resolved_at: UtcTimestamp | None = None
    resolved_monotonic_ns: int | None = None
    schema: str = AUDIT_FAULT_LIFECYCLE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != AUDIT_FAULT_LIFECYCLE_SCHEMA:
            raise ValueError("audit fault lifecycle schema is unsupported")
        if not is_sha256_hex(self.fault_id):
            raise ValueError("audit fault ID must be SHA-256 hex")
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit fault request ID must be non-empty and bounded")
        if not self.native_asset_id or len(self.native_asset_id) > 2048:
            raise ValueError("audit fault native asset ID must be non-empty and bounded")
        if not self.error_type or len(self.error_type) > 256:
            raise ValueError("audit fault error type must be non-empty and bounded")
        if self.audit_start_schema not in {AUDIT_START_V1_SCHEMA, AUDIT_START_V2_SCHEMA}:
            raise ValueError("audit fault start schema is unsupported")
        try:
            source_blocks_current_scope = audit_fault_evidence_blocks_current_scope(
                kind=self.failure_evidence_kind,
                audit_start_schema=self.audit_start_schema,
                failure_schema=self.failure_schema,
            )
        except ValueError as error:
            raise ValueError("audit fault failure source authority is unsupported") from error
        if type(self.blocks_current_scope) is not bool:
            raise ValueError("audit fault current-scope authority must be an exact boolean")
        if min(self.request_started_monotonic_ns, self.observed_monotonic_ns) < 0:
            raise ValueError("audit fault observation monotonic time cannot be negative")
        if self.observed_monotonic_ns < self.request_started_monotonic_ns:
            raise ValueError("audit fault observation precedes request start")
        if (
            self.response_completed_monotonic_ns is not None
            and not self.request_started_monotonic_ns
            <= self.response_completed_monotonic_ns
            <= self.observed_monotonic_ns
        ):
            raise ValueError("audit fault response-completion chronology is invalid")
        if not self.affected_scopes or len(set(self.affected_scopes)) != len(self.affected_scopes):
            raise ValueError("audit fault affected scopes must be non-empty and unique")
        http_required_values = (
            self.category,
            self.retryable,
            self.attempt,
            self.retry_after_disposition,
            self.retry_exhausted,
        )
        has_http_metadata = self.endpoint_class is not None or any(
            value is not None
            for value in (
                *http_required_values,
                self.status_code,
                self.retry_after_nanoseconds,
                self.retry_blocked_reason,
            )
        )
        if has_http_metadata:
            if self.endpoint_class is not PolymarketEndpointClass.CLOB_BOOK:
                raise ValueError("audit fault HTTP evidence must name the CLOB book endpoint")
            if any(value is None for value in http_required_values):
                raise ValueError("audit fault HTTP evidence must be complete")
            category = cast(PolymarketHttpFailureCategory, self.category)
            attempt = cast(int, self.attempt)
            if attempt == 0 and category is not PolymarketHttpFailureCategory.DEADLINE_EXCEEDED:
                raise ValueError(
                    "audit fault attempt zero is reserved for pre-attempt deadline expiry"
                )
            if self.error_type not in _accepted_audit_http_error_types(category):
                raise ValueError("audit fault HTTP type contradicts its typed category")
            validate_polymarket_http_failure_metadata(
                category=category,
                retryable=cast(bool, self.retryable),
                status_code=self.status_code,
                retry_after_disposition=cast(
                    PolymarketRetryAfterDisposition,
                    self.retry_after_disposition,
                ),
                has_retry_after=self.retry_after_nanoseconds is not None,
                retry_exhausted=cast(bool, self.retry_exhausted),
                retry_blocked_reason=self.retry_blocked_reason,
                final_failure=True,
            )
        resolution_values = (
            self.resolution,
            self.resolution_source,
            self.resolved_at,
            self.resolved_monotonic_ns,
        )
        if self.status is AuditFaultStatus.RESOLVED:
            if any(value is None for value in resolution_values):
                raise ValueError("resolved audit fault requires complete resolution evidence")
            if cast(int, self.resolved_monotonic_ns) < self.observed_monotonic_ns:
                raise ValueError("audit fault resolution precedes observation")
        elif any(value is not None for value in resolution_values):
            raise ValueError("unresolved audit fault cannot carry resolution evidence")
        if self.status in {AuditFaultStatus.CURRENT, AuditFaultStatus.RESOLVED} and (
            self.origin is not AuditFaultOrigin.EXTERNAL
        ):
            raise ValueError("recoverable audit fault lifecycle requires proven external origin")
        if not source_blocks_current_scope:
            if self.status is not AuditFaultStatus.PERMANENT or self.blocks_current_scope:
                raise ValueError(
                    "historical v1 audit fault must remain non-resolving permanent history"
                )
        elif self.blocks_current_scope is (self.status is AuditFaultStatus.RESOLVED):
            raise ValueError("audit fault current-scope authority contradicts lifecycle status")
        if self.reason in _KNOWN_INTERNAL_AUDIT_FAILURE_REASONS and (
            self.origin is not AuditFaultOrigin.INTERNAL
            or self.status is not AuditFaultStatus.PERMANENT
        ):
            raise ValueError("known effective internal audit failure must be INTERNAL/PERMANENT")

    def to_record(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "category": None if self.category is None else self.category.value,
            "endpoint_class": (None if self.endpoint_class is None else self.endpoint_class.value),
            "error_type": self.error_type,
            "affected_scopes": tuple(scope.value for scope in self.affected_scopes),
            "audit_start_schema": self.audit_start_schema,
            "audit_start_source": self.audit_start_source,
            "failure_evidence_kind": self.failure_evidence_kind.value,
            "failure_source": self.failure_source,
            "failure_schema": self.failure_schema,
            "fault_id": self.fault_id,
            "instrument_id": self.instrument_id.value,
            "market_id": self.market_id.value,
            "native_asset_id": self.native_asset_id,
            "observed_at_utc_ns": self.observed_at.nanoseconds_since_epoch,
            "observed_monotonic_ns": self.observed_monotonic_ns,
            "origin": self.origin.value,
            "original_reason": self.original_reason.value,
            "reason": self.reason.value,
            "request_started_monotonic_ns": self.request_started_monotonic_ns,
            "request_id": self.request_id,
            "blocks_current_scope": self.blocks_current_scope,
            "resolution": None if self.resolution is None else self.resolution.value,
            "resolution_source": self.resolution_source,
            "resolved_at_utc_ns": (
                None if self.resolved_at is None else self.resolved_at.nanoseconds_since_epoch
            ),
            "resolved_monotonic_ns": self.resolved_monotonic_ns,
            "retry_after_disposition": (
                None if self.retry_after_disposition is None else self.retry_after_disposition.value
            ),
            "retry_after_nanoseconds": self.retry_after_nanoseconds,
            "retry_blocked_reason": (
                None if self.retry_blocked_reason is None else self.retry_blocked_reason.value
            ),
            "retry_exhausted": self.retry_exhausted,
            "retryable": self.retryable,
            "response_completed_monotonic_ns": self.response_completed_monotonic_ns,
            "schema": self.schema,
            "status": self.status.value,
            "status_code": self.status_code,
        }


def audit_fault_origin(command: AuditFailureCommand) -> AuditFaultOrigin:
    """Classify only what durable typed evidence proves; never infer externally."""

    if command.schema == AUDIT_FAILURE_V1_SCHEMA:
        return AuditFaultOrigin.INDETERMINATE
    if command.reason in _KNOWN_INTERNAL_AUDIT_FAILURE_REASONS:
        return AuditFaultOrigin.INTERNAL
    if command.endpoint_class is not None:
        if command.category is PolymarketHttpFailureCategory.LOCAL_PROTOCOL:
            return AuditFaultOrigin.INTERNAL
        if command.category is PolymarketHttpFailureCategory.RESPONSE_PROTOCOL:
            return AuditFaultOrigin.INDETERMINATE
        return AuditFaultOrigin.EXTERNAL
    # A current-schema command without HTTP endpoint metadata is a local
    # coordinator/processor decision. It must never be softened into an
    # externally recoverable venue incident.
    return AuditFaultOrigin.INTERNAL


def initial_audit_fault_status(
    command: AuditFailureCommand,
    *,
    origin: AuditFaultOrigin,
) -> AuditFaultStatus:
    """Classify only fully evidenced external transients as recoverable."""

    if command.schema == AUDIT_FAILURE_V1_SCHEMA or origin is not AuditFaultOrigin.EXTERNAL:
        return AuditFaultStatus.PERMANENT
    if command.retryable is not True:
        return AuditFaultStatus.PERMANENT
    if command.category in {
        PolymarketHttpFailureCategory.TIMEOUT,
        PolymarketHttpFailureCategory.NETWORK,
    }:
        return AuditFaultStatus.CURRENT
    if command.category is PolymarketHttpFailureCategory.HTTP_STATUS and command.status_code in {
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    }:
        return AuditFaultStatus.CURRENT
    return AuditFaultStatus.PERMANENT


@dataclass(frozen=True, slots=True)
class AuditExpiryCommand:
    """Replayable per-instrument fail-closed freshness-expiry decision."""

    native_asset_id: str
    last_accepted_receive_monotonic_ns: int | None

    def __post_init__(self) -> None:
        if not self.native_asset_id or len(self.native_asset_id) > 2048:
            raise ValueError("audit expiry native asset ID must be non-empty and bounded")
        if (
            self.last_accepted_receive_monotonic_ns is not None
            and self.last_accepted_receive_monotonic_ns < 0
        ):
            raise ValueError("audit expiry freshness boundary cannot be negative")

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "last_accepted_receive_monotonic_ns": (self.last_accepted_receive_monotonic_ns),
                "native_asset_id": self.native_asset_id,
                "schema": AUDIT_EXPIRY_SCHEMA,
            }
        )


def _require_nonnegative_audit_integer(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if value < 0:
        raise ValueError(f"{label} cannot be negative")


@dataclass(frozen=True, slots=True)
class AuditClassificationCommand:
    """Durable decision that releases one captured response to canonical comparison."""

    request_id: str
    response_ingest_seq: int
    classification_monotonic_ns: int
    hard_expiry_monotonic_ns: int
    schema: str = AUDIT_CLASSIFICATION_SCHEMA

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        _require_nonnegative_audit_integer(
            self.response_ingest_seq,
            "audit response ingest sequence",
        )
        _require_nonnegative_audit_integer(
            self.classification_monotonic_ns,
            "audit classification monotonic time",
        )
        _require_nonnegative_audit_integer(
            self.hard_expiry_monotonic_ns,
            "audit hard expiry monotonic time",
        )
        if self.schema not in {
            AUDIT_CLASSIFICATION_V1_SCHEMA,
            AUDIT_CLASSIFICATION_V2_SCHEMA,
        }:
            raise ValueError("audit classification schema is unsupported")

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "classification_monotonic_ns": self.classification_monotonic_ns,
                "hard_expiry_monotonic_ns": self.hard_expiry_monotonic_ns,
                "request_id": self.request_id,
                "response_ingest_seq": self.response_ingest_seq,
                "schema": self.schema,
            }
        )


@dataclass(frozen=True, slots=True)
class AuditClassificationPrepareCommand:
    """Durable no-outcome preparation for one bounded audit classification."""

    request_id: str
    response_ingest_seq: int
    hard_expiry_monotonic_ns: int

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        _require_nonnegative_audit_integer(
            self.response_ingest_seq,
            "audit response ingest sequence",
        )
        _require_nonnegative_audit_integer(
            self.hard_expiry_monotonic_ns,
            "audit hard expiry monotonic time",
        )

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "hard_expiry_monotonic_ns": self.hard_expiry_monotonic_ns,
                "request_id": self.request_id,
                "response_ingest_seq": self.response_ingest_seq,
                "schema": AUDIT_CLASSIFICATION_PREPARE_SCHEMA,
            }
        )


@dataclass(frozen=True, slots=True)
class AuditReplacementReleaseCommand:
    """Durable coordinator proof that releases one replacement capture.

    ``capture_state_sha256`` commits the exact bounded capture record, including
    every required full-snapshot raw source, admission ordinal, ingest sequence,
    connection, and monotonic receive timestamp.  The control receive boundary
    is the single coordinator freshness decision used by both live processing
    and deterministic replay.
    """

    divergence_id: str
    instrument_id: InstrumentId
    market_id: MarketId
    replacement_connection_id: ConnectionId
    capture_state_sha256: str
    capture_record_json: str
    monotonic_epoch_id: str
    decision_monotonic_ns: int

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.divergence_id):
            raise ValueError("audit replacement release divergence ID must be SHA-256 hex")
        if not is_sha256_hex(self.capture_state_sha256):
            raise ValueError("audit replacement release capture identity must be SHA-256 hex")
        capture_record_bytes = self.capture_record_json.encode("utf-8")
        if not 0 < len(capture_record_bytes) <= AUDIT_REPLACEMENT_CAPTURE_MAX_BYTES:
            raise ValueError("audit replacement release capture record exceeds its bound")
        try:
            raw_capture_record: object = json.loads(self.capture_record_json)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError("audit replacement release capture record is invalid") from error
        if not isinstance(raw_capture_record, dict):
            raise ValueError("audit replacement release capture record shape is invalid")
        capture_record = cast(dict[str, object], raw_capture_record)
        if set(capture_record) != {
            "captured_snapshot_sources",
            "divergence_id",
            "market_id",
            "mismatch_cause",
            "original_connection_id",
            "pre_open_connection_count",
            "pre_open_connection_ids_sha256",
            "replacement_connection_id",
            "resolution_ingest_seq",
            "resolution_monotonic",
            "resolution_source",
            "required_instrument_ids",
        }:
            raise ValueError("audit replacement release capture record shape is invalid")
        if canonical_json_bytes(capture_record) != capture_record_bytes:
            raise ValueError("audit replacement release capture record must be canonical")
        if hashlib.sha256(capture_record_bytes).hexdigest() != self.capture_state_sha256:
            raise ValueError("audit replacement release capture record differs from its digest")
        if not self.monotonic_epoch_id or len(self.monotonic_epoch_id) > 256:
            raise ValueError("audit replacement release monotonic epoch is invalid")
        if self.decision_monotonic_ns < 0:
            raise ValueError("audit replacement release decision time cannot be negative")

    @classmethod
    def create(
        cls,
        *,
        divergence_id: str,
        instrument_id: InstrumentId,
        market_id: MarketId,
        replacement_connection_id: ConnectionId,
        capture_record: object,
        monotonic_epoch_id: str,
        decision_monotonic_ns: int,
    ) -> AuditReplacementReleaseCommand:
        capture_record_bytes = canonical_json_bytes(capture_record)
        return cls(
            divergence_id=divergence_id,
            instrument_id=instrument_id,
            market_id=market_id,
            replacement_connection_id=replacement_connection_id,
            capture_state_sha256=hashlib.sha256(capture_record_bytes).hexdigest(),
            capture_record_json=capture_record_bytes.decode("utf-8"),
            monotonic_epoch_id=monotonic_epoch_id,
            decision_monotonic_ns=decision_monotonic_ns,
        )

    @property
    def capture_record(self) -> dict[str, object]:
        raw_record: object = json.loads(self.capture_record_json)
        if not isinstance(raw_record, dict):  # pragma: no cover - enforced at construction
            raise AssertionError("validated audit replacement capture record changed shape")
        return cast(dict[str, object], raw_record)

    def to_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "capture_state_sha256": self.capture_state_sha256,
                "capture_record": self.capture_record,
                "decision_monotonic_ns": self.decision_monotonic_ns,
                "divergence_id": self.divergence_id,
                "instrument_id": self.instrument_id,
                "market_id": self.market_id,
                "monotonic_epoch_id": self.monotonic_epoch_id,
                "replacement_connection_id": self.replacement_connection_id,
                "schema": AUDIT_REPLACEMENT_RELEASE_SCHEMA,
            }
        )


@dataclass(frozen=True, slots=True)
class AuditHttpResponseHandoff:
    """Immutable request-scoped handoff captured at the HTTP response boundary."""

    request_id: str
    connection_id: ConnectionId
    payload_bytes: bytes
    received_wall_time: UtcTimestamp
    received_monotonic_ns: int
    response_boundary_watermark: WebSocketAdmissionWatermark
    request_completed_monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if self.received_monotonic_ns < 0:
            raise ValueError("audit response monotonic time cannot be negative")
        if self.response_boundary_watermark.connection_id != self.connection_id:
            raise ValueError("audit response watermark belongs to another connection")
        if (
            self.request_completed_monotonic_ns is not None
            and self.request_completed_monotonic_ns < self.received_monotonic_ns
        ):
            raise ValueError("audit request completion precedes its response body")

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.payload_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class AuditHttpFailureHandoff:
    """Immutable non-secret failure handoff from the supervised HTTP task."""

    request_id: str
    connection_id: ConnectionId
    reason: AuditFailureReason
    error_type: str
    endpoint_class: PolymarketEndpointClass | None = None
    category: PolymarketHttpFailureCategory | None = None
    retryable: bool | None = None
    attempt: int | None = None
    status_code: int | None = None
    retry_after_nanoseconds: int | None = None
    retry_after_disposition: PolymarketRetryAfterDisposition | None = None
    retry_exhausted: bool | None = None
    retry_blocked_reason: PolymarketRetryBlockedReason | None = None

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if not self.error_type or len(self.error_type) > 256:
            raise ValueError("audit failure type must be non-empty and bounded")
        # Reuse the durable command validator so handoff and journal contracts
        # cannot drift while keeping the connection binding out of the payload.
        self.to_command()

    @classmethod
    def from_http_error(
        cls,
        *,
        request_id: str,
        connection_id: ConnectionId,
        effective_reason: AuditFailureReason,
        error: PolymarketHttpError,
    ) -> AuditHttpFailureHandoff:
        command = AuditFailureCommand.from_http_error(
            request_id=request_id,
            effective_reason=effective_reason,
            error=error,
        )
        return cls(
            request_id=request_id,
            connection_id=connection_id,
            reason=effective_reason,
            error_type=command.error_type or effective_reason.value,
            endpoint_class=command.endpoint_class,
            category=command.category,
            retryable=command.retryable,
            attempt=command.attempt,
            status_code=command.status_code,
            retry_after_nanoseconds=command.retry_after_nanoseconds,
            retry_after_disposition=command.retry_after_disposition,
            retry_exhausted=command.retry_exhausted,
            retry_blocked_reason=command.retry_blocked_reason,
        )

    def to_command(self) -> AuditFailureCommand:
        return AuditFailureCommand(
            request_id=self.request_id,
            reason=self.reason,
            error_type=self.error_type,
            endpoint_class=self.endpoint_class,
            category=self.category,
            retryable=self.retryable,
            attempt=self.attempt,
            status_code=self.status_code,
            retry_after_nanoseconds=self.retry_after_nanoseconds,
            retry_after_disposition=self.retry_after_disposition,
            retry_exhausted=self.retry_exhausted,
            retry_blocked_reason=self.retry_blocked_reason,
        )


type AuditHttpHandoff = AuditHttpResponseHandoff | AuditHttpFailureHandoff


@dataclass(frozen=True, slots=True)
class AuditOutcomeEvidence:
    """Replay-deterministic evidence for one completed audit request."""

    request_id: str
    connection_id: ConnectionId
    native_asset_id: str
    attempt: int
    request_started_monotonic_ns: int
    starting_admission_ordinal: int
    starting_processed_ordinal: int
    starting_book: AuditBookIdentity
    outcome: AuditOutcome
    reason: AuditFailureReason | None
    response_ingest_id: IngestId | None
    raw_response_sha256: str | None
    response_received_wall_time: UtcTimestamp | None
    response_received_monotonic_ns: int | None
    response_boundary_ordinal: int | None
    barrier_processed_ordinal: int | None
    barrier_book: AuditBookIdentity | None
    requires_resync: bool
    # Optional only for source compatibility with pre-v1 producers.  New
    # MISMATCH producers should always attach the deterministic diagnostic.
    mismatch_diagnostic: AuditMismatchDiagnostic | None = None
    audit_start_schema: str = AUDIT_START_SCHEMA
    trigger_reason: AuditTriggerReason = AuditTriggerReason.OTHER_TYPED_REASON
    # Additive exact raw identities. Historical callers may omit them, while
    # all current processor paths bind both the staged start and the durable
    # response/failure classification source.
    audit_start_source: RawSourceRef | None = None
    classification_source: RawSourceRef | None = None

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit request ID must be non-empty and bounded")
        if self.attempt <= 0:
            raise ValueError("audit attempt must be positive")
        if min(self.starting_admission_ordinal, self.starting_processed_ordinal) < 0:
            raise ValueError("audit starting watermarks cannot be negative")
        if self.starting_processed_ordinal > self.starting_admission_ordinal:
            raise ValueError("audit processed watermark cannot exceed admitted watermark")
        response_fields = (
            self.response_ingest_id,
            self.raw_response_sha256,
            self.response_received_wall_time,
            self.response_received_monotonic_ns,
            self.response_boundary_ordinal,
        )
        if any(field is not None for field in response_fields) and any(
            field is None for field in response_fields
        ):
            raise ValueError("audit response/barrier evidence must be all present or all absent")
        barrier_fields = (self.barrier_processed_ordinal, self.barrier_book)
        if any(field is not None for field in barrier_fields) and any(
            field is None for field in barrier_fields
        ):
            raise ValueError("audit barrier watermark and identity must be present together")
        if self.raw_response_sha256 is not None and not is_sha256_hex(self.raw_response_sha256):
            raise ValueError("audit raw response identity must be SHA-256 hex")
        if (
            self.response_received_monotonic_ns is not None
            and self.response_received_monotonic_ns < self.request_started_monotonic_ns
        ):
            raise ValueError("audit response cannot precede its request start")
        if (
            self.response_boundary_ordinal is not None
            and self.barrier_processed_ordinal is not None
            and self.barrier_processed_ordinal != self.response_boundary_ordinal
            and self.outcome is not AuditOutcome.FAILED
        ):
            raise ValueError("completed audit barrier must equal the response watermark")
        if self.outcome is AuditOutcome.FAILED and self.reason is None:
            raise ValueError("FAILED audit outcome requires a reason")
        if self.outcome is not AuditOutcome.FAILED and self.reason is not None:
            raise ValueError("only FAILED audit outcome may carry a failure reason")
        if self.outcome is AuditOutcome.MISMATCH and not self.requires_resync:
            raise ValueError("MISMATCH audit outcome must require resynchronization")
        if self.outcome in {AuditOutcome.MATCH, AuditOutcome.RACED, AuditOutcome.FAILED} and (
            self.requires_resync
        ):
            raise ValueError("only a proven MISMATCH audit may require resynchronization")
        if self.mismatch_diagnostic is not None:
            if self.outcome is not AuditOutcome.MISMATCH:
                raise ValueError("only a MISMATCH audit may carry a mismatch diagnostic")
            if (
                self.barrier_book is None
                or self.barrier_book != self.mismatch_diagnostic.local_book
            ):
                raise ValueError("audit mismatch diagnostic must bind the barrier book identity")
            response_source = self.mismatch_diagnostic.response_source
            if self.response_ingest_id != response_source.ingest_id:
                raise ValueError("audit mismatch diagnostic identifies another response ingest")
            if self.raw_response_sha256 != response_source.payload_sha256:
                raise ValueError("audit mismatch diagnostic identifies another response payload")
        if self.audit_start_schema == AUDIT_START_V1_SCHEMA:
            if self.trigger_reason is not AuditTriggerReason.UNKNOWN_LEGACY:
                raise ValueError("v1 audit outcome must expose UNKNOWN_LEGACY trigger")
        elif self.audit_start_schema == AUDIT_START_V2_SCHEMA:
            if self.trigger_reason is AuditTriggerReason.UNKNOWN_LEGACY:
                raise ValueError("v2 audit outcome cannot claim a legacy-unknown trigger")
        else:
            raise ValueError("audit outcome start schema is unsupported")
        if (self.audit_start_source is None) != (self.classification_source is None):
            raise ValueError("audit outcome raw control identities must be present together")
        if (
            self.classification_source is not None
            and self.response_ingest_id is not None
            and self.classification_source.ingest_id == self.response_ingest_id
            and self.classification_source.payload_sha256 != self.raw_response_sha256
        ):
            raise ValueError("audit classification source conflicts with response identity")


def _audit_outcome_divergence_identity_v1(
    audit_outcome: AuditOutcomeEvidence,
) -> dict[str, object]:
    """Return the immutable pending-v1 outcome identity projection.

    ``mma-rest-audit-divergence-pending-v1`` predates the additive exact raw
    identities on :class:`AuditOutcomeEvidence`.  Historical v2 audit-start
    journals therefore committed this precise projection (with neither raw
    identity key present) into their divergence IDs and replacement-release
    capture records.  Keep the projection explicit so future additive outcome
    evidence cannot silently rewrite that durable identity contract.

    The full enriched outcome remains part of ``AuditDivergencePending`` and
    its retained/history records; only the content-addressed v1 ID preimage is
    projected to preserve that durable identity contract during replay.
    """

    return {
        "request_id": audit_outcome.request_id,
        "connection_id": audit_outcome.connection_id,
        "native_asset_id": audit_outcome.native_asset_id,
        "attempt": audit_outcome.attempt,
        "request_started_monotonic_ns": audit_outcome.request_started_monotonic_ns,
        "starting_admission_ordinal": audit_outcome.starting_admission_ordinal,
        "starting_processed_ordinal": audit_outcome.starting_processed_ordinal,
        "starting_book": audit_outcome.starting_book,
        "outcome": audit_outcome.outcome,
        "reason": audit_outcome.reason,
        "response_ingest_id": audit_outcome.response_ingest_id,
        "raw_response_sha256": audit_outcome.raw_response_sha256,
        "response_received_wall_time": audit_outcome.response_received_wall_time,
        "response_received_monotonic_ns": audit_outcome.response_received_monotonic_ns,
        "response_boundary_ordinal": audit_outcome.response_boundary_ordinal,
        "barrier_processed_ordinal": audit_outcome.barrier_processed_ordinal,
        "barrier_book": audit_outcome.barrier_book,
        "requires_resync": audit_outcome.requires_resync,
        "mismatch_diagnostic": audit_outcome.mismatch_diagnostic,
        "audit_start_schema": audit_outcome.audit_start_schema,
        "trigger_reason": audit_outcome.trigger_reason,
    }


def _audit_divergence_id(
    *,
    audit_outcome: AuditOutcomeEvidence,
    audit_start_control_source: RawSourceRef,
    classification_source: RawSourceRef,
    classification_control_source: RawSourceRef | None,
) -> str:
    return canonical_sha256(
        {
            "audit_outcome": _audit_outcome_divergence_identity_v1(audit_outcome),
            "audit_start_control_source": audit_start_control_source,
            "classification_source": classification_source,
            "classification_control_source": classification_control_source,
            "schema": AUDIT_DIVERGENCE_PENDING_SCHEMA,
        }
    )


@dataclass(frozen=True, slots=True)
class AuditDivergencePending:
    """Exact unresolved mismatch state retained until a separate REST baseline."""

    divergence_id: str
    market_id: MarketId
    audit_outcome: AuditOutcomeEvidence
    audit_start_control_source: RawSourceRef
    classification_source: RawSourceRef
    classification_control_source: RawSourceRef | None
    classification_received_wall_time: UtcTimestamp
    classification_received_monotonic_ns: int
    schema: str = field(init=False, default=AUDIT_DIVERGENCE_PENDING_SCHEMA)

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.divergence_id):
            raise ValueError("audit divergence ID must be SHA-256 hex")
        if self.audit_outcome.outcome is not AuditOutcome.MISMATCH:
            raise ValueError("audit divergence must bind a MISMATCH outcome")
        diagnostic = self.audit_outcome.mismatch_diagnostic
        if diagnostic is None:
            raise ValueError("audit divergence requires an exact mismatch diagnostic")
        if diagnostic.rest_book.market_id != self.market_id:
            raise ValueError("audit divergence market differs from its REST target")
        nested_audit_start_source = self.audit_outcome.audit_start_source
        if (
            nested_audit_start_source is not None
            and nested_audit_start_source != self.audit_start_control_source
        ):
            raise ValueError("audit divergence start source conflicts with its outcome evidence")
        nested_classification_source = self.audit_outcome.classification_source
        if (
            nested_classification_source is not None
            and nested_classification_source != self.classification_source
        ):
            raise ValueError(
                "audit divergence classification source conflicts with its outcome evidence"
            )
        response_monotonic_ns = self.audit_outcome.response_received_monotonic_ns
        response_wall_time = self.audit_outcome.response_received_wall_time
        if response_monotonic_ns is None or response_wall_time is None:
            raise ValueError("audit divergence lacks response chronology")
        if self.classification_received_monotonic_ns < response_monotonic_ns:
            raise ValueError("audit divergence classification precedes its REST response")
        if self.classification_control_source is None:
            if (
                self.classification_source != diagnostic.response_source
                or self.classification_received_monotonic_ns != response_monotonic_ns
                or self.classification_received_wall_time != response_wall_time
            ):
                raise ValueError("immediate audit classification must bind the response evidence")
        elif self.classification_source != self.classification_control_source:
            raise ValueError("deferred audit classification must bind its control evidence")
        expected = _audit_divergence_id(
            audit_outcome=self.audit_outcome,
            audit_start_control_source=self.audit_start_control_source,
            classification_source=self.classification_source,
            classification_control_source=self.classification_control_source,
        )
        if self.divergence_id != expected:
            raise ValueError("audit divergence ID differs from its exact evidence")

    @classmethod
    def create(
        cls,
        *,
        market_id: MarketId,
        audit_outcome: AuditOutcomeEvidence,
        audit_start_control_source: RawSourceRef,
        classification_source: RawSourceRef,
        classification_control_source: RawSourceRef | None,
        classification_received_wall_time: UtcTimestamp,
        classification_received_monotonic_ns: int,
    ) -> AuditDivergencePending:
        return cls(
            divergence_id=_audit_divergence_id(
                audit_outcome=audit_outcome,
                audit_start_control_source=audit_start_control_source,
                classification_source=classification_source,
                classification_control_source=classification_control_source,
            ),
            market_id=market_id,
            audit_outcome=audit_outcome,
            audit_start_control_source=audit_start_control_source,
            classification_source=classification_source,
            classification_control_source=classification_control_source,
            classification_received_wall_time=classification_received_wall_time,
            classification_received_monotonic_ns=classification_received_monotonic_ns,
        )

    @property
    def instrument_id(self) -> InstrumentId:
        return self.audit_outcome.starting_book.instrument_id

    @property
    def request_id(self) -> str:
        return self.audit_outcome.request_id

    def to_record(self) -> dict[str, object]:
        return {
            "audit_outcome": self.audit_outcome,
            "audit_start_control_source": self.audit_start_control_source,
            "classification_control_source": self.classification_control_source,
            "classification_received_monotonic_ns": self.classification_received_monotonic_ns,
            "classification_received_wall_time": self.classification_received_wall_time,
            "classification_source": self.classification_source,
            "divergence_id": self.divergence_id,
            "instrument_id": self.instrument_id,
            "market_id": self.market_id,
            "request_id": self.request_id,
            "schema": self.schema,
        }


@dataclass(frozen=True, slots=True)
class AuditDivergenceResolution:
    """Raw-linked proof that an authoritative REST snapshot restored one book."""

    divergence_id: str
    request_id: str
    instrument_id: InstrumentId
    market_id: MarketId
    audit_start_schema: str
    trigger_reason: AuditTriggerReason
    mismatch_response_source: RawSourceRef
    authoritative_snapshot_source: RawSourceRef
    authoritative_rest_book: AuditRestBookIdentity
    recovered_book: AuditBookIdentity
    observed_divergence: AuditDivergencePending
    resolved_at: UtcTimestamp
    resolved_monotonic_ns: int
    status: AuditDivergenceResolutionStatus = (
        AuditDivergenceResolutionStatus.AUTHORITATIVE_RESNAPSHOT
    )
    schema: str = field(init=False, default=AUDIT_DIVERGENCE_RESOLUTION_SCHEMA)

    def __post_init__(self) -> None:
        if not is_sha256_hex(self.divergence_id):
            raise ValueError("audit divergence resolution ID must be SHA-256 hex")
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("audit divergence resolution request ID is invalid")
        if self.resolved_monotonic_ns < 0:
            raise ValueError("audit divergence resolution time cannot be negative")
        if (
            self.resolved_monotonic_ns
            < self.observed_divergence.classification_received_monotonic_ns
        ):
            raise ValueError("audit divergence resolution precedes its classification")
        if (
            self.authoritative_rest_book.instrument_id != self.instrument_id
            or self.authoritative_rest_book.market_id != self.market_id
            or self.recovered_book.instrument_id != self.instrument_id
        ):
            raise ValueError("audit divergence resolution identities disagree")
        if (
            self.observed_divergence.divergence_id != self.divergence_id
            or self.observed_divergence.request_id != self.request_id
            or self.observed_divergence.instrument_id != self.instrument_id
            or self.observed_divergence.market_id != self.market_id
            or self.observed_divergence.audit_outcome.audit_start_schema != self.audit_start_schema
            or self.observed_divergence.audit_outcome.trigger_reason is not self.trigger_reason
            or self.observed_divergence.audit_outcome.mismatch_diagnostic is None
            or self.observed_divergence.audit_outcome.mismatch_diagnostic.response_source
            != self.mismatch_response_source
        ):
            raise ValueError("audit divergence resolution differs from its observed state")
        if self.audit_start_schema == AUDIT_START_V1_SCHEMA:
            if self.trigger_reason is not AuditTriggerReason.UNKNOWN_LEGACY:
                raise ValueError("v1 divergence resolution must retain UNKNOWN_LEGACY")
        elif self.audit_start_schema == AUDIT_START_V2_SCHEMA:
            if self.trigger_reason is AuditTriggerReason.UNKNOWN_LEGACY:
                raise ValueError("v2 divergence resolution cannot claim UNKNOWN_LEGACY")
        else:
            raise ValueError("audit divergence resolution start schema is unsupported")

    def to_record(self) -> dict[str, object]:
        return {
            "audit_start_schema": self.audit_start_schema,
            "authoritative_rest_book": self.authoritative_rest_book,
            "authoritative_snapshot_source": self.authoritative_snapshot_source,
            "divergence_id": self.divergence_id,
            "instrument_id": self.instrument_id,
            "market_id": self.market_id,
            "mismatch_response_source": self.mismatch_response_source,
            "observed_divergence": self.observed_divergence,
            "recovered_book": self.recovered_book,
            "request_id": self.request_id,
            "resolved_at": self.resolved_at,
            "resolved_monotonic_ns": self.resolved_monotonic_ns,
            "schema": self.schema,
            "status": self.status,
            "trigger_reason": self.trigger_reason,
        }


__all__ = [
    "AUDIT_CLASSIFICATION_PREPARE_ATTEST_PROTOCOL",
    "AUDIT_CLASSIFICATION_PREPARE_SCHEMA",
    "AUDIT_CLASSIFICATION_SCHEMA",
    "AUDIT_CLASSIFICATION_V1_SCHEMA",
    "AUDIT_CLASSIFICATION_V2_SCHEMA",
    "AUDIT_DIVERGENCE_PENDING_SCHEMA",
    "AUDIT_DIVERGENCE_RESOLUTION_SCHEMA",
    "AUDIT_EXACT_DEPTH_DIFFERENCE_SCHEMA",
    "AUDIT_EXPIRY_SCHEMA",
    "AUDIT_FAILURE_SCHEMA",
    "AUDIT_FAILURE_V1_SCHEMA",
    "AUDIT_FAILURE_V2_SCHEMA",
    "AUDIT_FAULT_LIFECYCLE_SCHEMA",
    "AUDIT_MISMATCH_DIAGNOSTIC_SCHEMA",
    "AUDIT_MISMATCH_MAX_CONTEXT_IDENTITIES",
    "AUDIT_MISMATCH_MAX_LEVELS_PER_SIDE",
    "AUDIT_MISMATCH_MAX_RAW_SOURCES",
    "AUDIT_MISMATCH_MAX_REPRESENTATIVE_DIFFERENCES",
    "AUDIT_MISMATCH_MAX_REPRESENTATIVE_RAW_SOURCES",
    "AUDIT_RAW_SOURCE_SUMMARY_SCHEMA",
    "AUDIT_REPLACEMENT_CAPTURE_MAX_BYTES",
    "AUDIT_REPLACEMENT_RELEASE_SCHEMA",
    "AUDIT_REST_BOOK_IDENTITY_SCHEMA",
    "AUDIT_START_SCHEMA",
    "AUDIT_START_V1_SCHEMA",
    "AUDIT_START_V2_SCHEMA",
    "AuditBookIdentity",
    "AuditBookSide",
    "AuditClassificationCommand",
    "AuditClassificationPrepareCommand",
    "AuditDivergencePending",
    "AuditDivergenceResolution",
    "AuditDivergenceResolutionStatus",
    "AuditExactDepthDifference",
    "AuditExactDepthIdentity",
    "AuditExpiryCommand",
    "AuditFailureCommand",
    "AuditFailureReason",
    "AuditFaultAffectedScope",
    "AuditFaultFailureEvidenceKind",
    "AuditFaultLifecycleRecord",
    "AuditFaultOrigin",
    "AuditFaultResolution",
    "AuditFaultStatus",
    "AuditHttpFailureHandoff",
    "AuditHttpHandoff",
    "AuditHttpResponseHandoff",
    "AuditLevelDifference",
    "AuditMismatchCause",
    "AuditMismatchDiagnostic",
    "AuditOutcome",
    "AuditOutcomeEvidence",
    "AuditRawSourceSummary",
    "AuditReplacementReleaseCommand",
    "AuditRestBookIdentity",
    "AuditStartEvidence",
    "AuditTriggerReason",
    "WebSocketAdmissionWatermark",
    "audit_exact_depth_identity",
    "audit_fault_evidence_blocks_current_scope",
    "audit_fault_origin",
    "build_audit_mismatch_diagnostic",
    "compare_audit_exact_depth",
    "initial_audit_fault_status",
    "summarize_audit_raw_sources",
]
