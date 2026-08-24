from __future__ import annotations

import json
import math

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from multi_market_arbitrage.core.ids import ConnectionId
from multi_market_arbitrage.pipeline.audit import (
    AUDIT_FAILURE_V1_SCHEMA,
    AUDIT_FAILURE_V2_SCHEMA,
    AuditFailureCommand,
    AuditFailureReason,
    AuditFaultOrigin,
    AuditFaultStatus,
    AuditHttpFailureHandoff,
    audit_fault_origin,
    initial_audit_fault_status,
)
from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketHttpProtocolError,
    PolymarketHttpStatusError,
    PolymarketHttpTransportError,
    PolymarketPublicClient,
    PolymarketRetryAfterDisposition,
    PolymarketRetryBlockedReason,
)
from multi_market_arbitrage.venues.polymarket.errors import (
    MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS,
    MAXIMUM_RETRY_AFTER_EVIDENCE_SECONDS,
)

_RECOVERABLE_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


def _http_error_type(category: PolymarketHttpFailureCategory) -> str:
    error_class = (
        "PolymarketHttpStatusError"
        if category is PolymarketHttpFailureCategory.HTTP_STATUS
        else "PolymarketHttpTransportError"
    )
    return f"{error_class}:{category.value}"


def test_http_failure_handoff_preserves_sanitized_retry_and_status_evidence() -> None:
    error = PolymarketHttpStatusError(
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.HTTP_STATUS,
        retryable=True,
        attempt=3,
        detail="bounded injected status",
        status_code=503,
        retry_after_seconds=1.25,
        retry_after_disposition=PolymarketRetryAfterDisposition.PARSED,
        retry_exhausted=True,
        retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
    )

    handoff = AuditHttpFailureHandoff.from_http_error(
        request_id="request-1",
        connection_id=ConnectionId("connection-1"),
        effective_reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error=error,
    )
    command = handoff.to_command()
    record = json.loads(command.to_payload())

    assert record == {
        "category": "HTTP_STATUS",
        "endpoint_class": "CLOB_BOOK",
        "error_type": "PolymarketHttpStatusError:HTTP_STATUS",
        "http_attempt": 3,
        "reason": "HTTP_REQUEST_FAILED",
        "request_id": "request-1",
        "retry_after_disposition": "PARSED",
        "retry_after_nanoseconds": 1_250_000_000,
        "retry_blocked_reason": "ATTEMPTS_EXHAUSTED",
        "retry_exhausted": True,
        "retryable": True,
        "schema": AUDIT_FAILURE_V2_SCHEMA,
        "status_code": 503,
    }
    assert audit_fault_origin(command) is AuditFaultOrigin.EXTERNAL
    assert (
        initial_audit_fault_status(command, origin=audit_fault_origin(command))
        is AuditFaultStatus.CURRENT
    )


@pytest.mark.parametrize(
    ("category", "status_code", "expected_origin"),
    [
        (
            PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
            None,
            AuditFaultOrigin.INTERNAL,
        ),
        (
            PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
            200,
            AuditFaultOrigin.INDETERMINATE,
        ),
    ],
)
def test_protocol_error_handoff_retains_its_concrete_typed_class(
    category: PolymarketHttpFailureCategory,
    status_code: int | None,
    expected_origin: AuditFaultOrigin,
) -> None:
    error = PolymarketHttpProtocolError(
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=category,
        retryable=False,
        attempt=1,
        detail="bounded protocol failure",
        status_code=status_code,
    )

    command = AuditHttpFailureHandoff.from_http_error(
        request_id=f"protocol-{category.value.lower()}",
        connection_id=ConnectionId("protocol-connection"),
        effective_reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error=error,
    ).to_command()

    assert command.error_type == f"PolymarketHttpProtocolError:{category.value}"
    assert audit_fault_origin(command) is expected_origin
    assert (
        initial_audit_fault_status(command, origin=audit_fault_origin(command))
        is AuditFaultStatus.PERMANENT
    )


@pytest.mark.asyncio
async def test_oversized_retry_after_produces_bounded_policy_failure_handoff() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            headers={"retry-after": "9999999999999"},
            request=request,
        )

    async with PolymarketPublicClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PolymarketHttpStatusError) as caught:
            await client.fetch_order_book_raw("token")

    error = caught.value
    assert error.retry_blocked_reason is PolymarketRetryBlockedReason.RETRY_AFTER_POLICY
    assert error.retry_after_seconds is not None
    assert math.isfinite(error.retry_after_seconds)
    assert error.retry_after_seconds == MAXIMUM_RETRY_AFTER_EVIDENCE_SECONDS
    command = AuditHttpFailureHandoff.from_http_error(
        request_id="oversized-retry-after",
        connection_id=ConnectionId("retry-after-connection"),
        effective_reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error=error,
    ).to_command()

    assert command.retry_after_nanoseconds == MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS
    assert command.retry_blocked_reason is PolymarketRetryBlockedReason.RETRY_AFTER_POLICY
    command.validate_against_public_read_policy(
        maximum_attempts=3,
        maximum_retry_after_nanoseconds=10_000_000_000,
    )


def test_v1_failure_remains_byte_compatible_and_fail_closed() -> None:
    command = AuditFailureCommand(
        request_id="historical-request",
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        schema=AUDIT_FAILURE_V1_SCHEMA,
    )

    assert json.loads(command.to_payload()) == {
        "reason": "HTTP_REQUEST_FAILED",
        "request_id": "historical-request",
        "schema": AUDIT_FAILURE_V1_SCHEMA,
    }
    origin = audit_fault_origin(command)
    assert origin is AuditFaultOrigin.INDETERMINATE
    assert initial_audit_fault_status(command, origin=origin) is AuditFaultStatus.PERMANENT


def test_synthetic_internal_failure_is_permanent() -> None:
    command = AuditFailureCommand(
        request_id="internal-request",
        reason=AuditFailureReason.WATERMARK_AMBIGUOUS,
        error_type="ResponseBoundaryEvidenceMismatch",
    )

    origin = audit_fault_origin(command)

    assert origin is AuditFaultOrigin.INTERNAL
    assert initial_audit_fault_status(command, origin=origin) is AuditFaultStatus.PERMANENT


def test_http_attempt_zero_is_reserved_for_pre_attempt_deadline_expiry() -> None:
    with pytest.raises(ValueError, match="attempt zero"):
        PolymarketHttpTransportError(
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.TIMEOUT,
            retryable=True,
            attempt=0,
            detail="impossible pre-attempt timeout",
        )

    error = PolymarketHttpTransportError(
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
        retryable=False,
        attempt=0,
        detail="absolute deadline elapsed before admission",
    )

    assert error.attempt == 0


def test_audit_http_failure_type_is_bound_to_its_typed_category() -> None:
    with pytest.raises(ValueError, match="type contradicts"):
        AuditFailureCommand(
            request_id="forged-http-error-type",
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpStatusError:HTTP_STATUS",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.TIMEOUT,
            retryable=True,
            attempt=3,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=True,
            retry_blocked_reason=PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED,
        )


@pytest.mark.parametrize(
    ("attempt", "retry_after_nanoseconds", "blocked_reason", "match"),
    [
        (2, None, PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED, "final attempt"),
        (4, None, PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED, "exceeds"),
        (1, 1, PolymarketRetryBlockedReason.RETRY_AFTER_POLICY, "above"),
        (3, 10_000_000_001, PolymarketRetryBlockedReason.RETRY_AFTER_POLICY, "override"),
        (1, 10_000_000_001, PolymarketRetryBlockedReason.DEADLINE, "requires"),
    ],
)
def test_audit_http_failure_must_match_the_frozen_public_read_policy(
    attempt: int,
    retry_after_nanoseconds: int | None,
    blocked_reason: PolymarketRetryBlockedReason,
    match: str,
) -> None:
    command = AuditFailureCommand(
        request_id="policy-bound-http-failure",
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

    with pytest.raises(ValueError, match=match):
        command.validate_against_public_read_policy(
            maximum_attempts=3,
            maximum_retry_after_nanoseconds=10_000_000_000,
        )


@pytest.mark.parametrize(
    "endpoint_class",
    tuple(
        endpoint
        for endpoint in PolymarketEndpointClass
        if endpoint is not PolymarketEndpointClass.CLOB_BOOK
    ),
)
def test_audit_http_failure_rejects_non_book_endpoint(
    endpoint_class: PolymarketEndpointClass,
) -> None:
    with pytest.raises(ValueError, match="CLOB book endpoint"):
        AuditFailureCommand(
            request_id="wrong-audit-endpoint",
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpStatusError:HTTP_STATUS",
            endpoint_class=endpoint_class,
            category=PolymarketHttpFailureCategory.HTTP_STATUS,
            retryable=False,
            attempt=1,
            status_code=404,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=False,
        )


@pytest.mark.parametrize(
    ("category", "status_code", "retryable"),
    [
        (PolymarketHttpFailureCategory.TIMEOUT, 404, True),
        (PolymarketHttpFailureCategory.NETWORK, 500, True),
        (PolymarketHttpFailureCategory.HTTP_STATUS, None, False),
        (PolymarketHttpFailureCategory.HTTP_STATUS, 200, False),
        (PolymarketHttpFailureCategory.HTTP_STATUS, 404, True),
        (PolymarketHttpFailureCategory.HTTP_STATUS, 503, False),
        (PolymarketHttpFailureCategory.DEADLINE_EXCEEDED, None, True),
    ],
)
def test_audit_http_failure_rejects_contradictory_category_status_and_retryability(
    category: PolymarketHttpFailureCategory,
    status_code: int | None,
    retryable: bool,
) -> None:
    with pytest.raises(ValueError, match=r"status|retryab|HTTP_STATUS"):
        AuditFailureCommand(
            request_id="contradictory-http-metadata",
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type=_http_error_type(category),
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=category,
            retryable=retryable,
            attempt=1,
            status_code=status_code,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=retryable,
            retry_blocked_reason=(
                PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED if retryable else None
            ),
        )


@pytest.mark.parametrize(
    ("retry_exhausted", "blocked_reason"),
    [
        (True, None),
        (False, PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED),
        (False, None),
    ],
)
def test_final_retryable_audit_failure_requires_truthful_retry_policy_closure(
    retry_exhausted: bool,
    blocked_reason: PolymarketRetryBlockedReason | None,
) -> None:
    with pytest.raises(ValueError, match=r"retry|final"):
        AuditFailureCommand(
            request_id="contradictory-retry-policy",
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type="PolymarketHttpTransportError:TIMEOUT",
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=PolymarketHttpFailureCategory.TIMEOUT,
            retryable=True,
            attempt=1,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=retry_exhausted,
            retry_blocked_reason=blocked_reason,
        )


@pytest.mark.parametrize(
    ("reason", "category", "retryable"),
    [
        (
            AuditFailureReason.OBSERVATION_DEADLINE,
            PolymarketHttpFailureCategory.TIMEOUT,
            True,
        ),
        (
            AuditFailureReason.OBSERVATION_DEADLINE,
            PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
            True,
        ),
        (
            AuditFailureReason.LOCAL_STATE_NOT_COMPARABLE,
            PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
            False,
        ),
        (
            AuditFailureReason.RUN_ENDED,
            PolymarketHttpFailureCategory.HTTP_STATUS,
            False,
        ),
    ],
)
def test_internal_reason_rejects_nonmatching_http_metadata(
    reason: AuditFailureReason,
    category: PolymarketHttpFailureCategory,
    retryable: bool,
) -> None:
    with pytest.raises(ValueError, match="non-HTTP audit failure reason"):
        AuditFailureCommand(
            request_id="invalid-cross-field-metadata",
            reason=reason,
            error_type=_http_error_type(category),
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=category,
            retryable=retryable,
            attempt=1,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=False,
        )


@pytest.mark.parametrize(
    "effective_reason",
    [
        AuditFailureReason.OBSERVATION_DEADLINE,
        AuditFailureReason.BOOK_FRESHNESS_EXPIRED,
    ],
)
def test_nonretryable_http_deadline_preserves_original_and_effective_reason_permanently(
    effective_reason: AuditFailureReason,
) -> None:
    error = PolymarketHttpTransportError(
        endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
        category=PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
        retryable=False,
        attempt=2,
        detail="bounded deadline evidence",
    )

    handoff = AuditHttpFailureHandoff.from_http_error(
        request_id="deadline-effective-reason",
        connection_id=ConnectionId("deadline-connection"),
        effective_reason=effective_reason,
        error=error,
    )
    command = handoff.to_command()
    origin = audit_fault_origin(command)

    assert command.reason is effective_reason
    assert command.error_type == "PolymarketHttpTransportError:DEADLINE_EXCEEDED"
    assert command.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
    assert command.retryable is False
    assert command.status_code is None
    assert origin is AuditFaultOrigin.INTERNAL
    assert initial_audit_fault_status(command, origin=origin) is AuditFaultStatus.PERMANENT


@pytest.mark.parametrize(
    ("category", "retryable", "status_code", "expected_origin", "expected_status"),
    [
        (
            PolymarketHttpFailureCategory.TIMEOUT,
            True,
            None,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.CURRENT,
        ),
        (
            PolymarketHttpFailureCategory.NETWORK,
            True,
            None,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.CURRENT,
        ),
        (
            PolymarketHttpFailureCategory.HTTP_STATUS,
            True,
            503,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.CURRENT,
        ),
        (
            PolymarketHttpFailureCategory.CLIENT_CLOSED,
            False,
            None,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.PERMANENT,
        ),
        (
            PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
            False,
            None,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.PERMANENT,
        ),
        (
            PolymarketHttpFailureCategory.REMOTE_PROTOCOL,
            False,
            None,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.PERMANENT,
        ),
        (
            PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
            False,
            None,
            AuditFaultOrigin.INTERNAL,
            AuditFaultStatus.PERMANENT,
        ),
        (
            PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
            False,
            200,
            AuditFaultOrigin.INDETERMINATE,
            AuditFaultStatus.PERMANENT,
        ),
        (
            PolymarketHttpFailureCategory.HTTP_STATUS,
            False,
            404,
            AuditFaultOrigin.EXTERNAL,
            AuditFaultStatus.PERMANENT,
        ),
    ],
)
def test_v2_fault_status_is_recoverable_only_for_proven_external_transients(
    category: PolymarketHttpFailureCategory,
    retryable: bool,
    status_code: int | None,
    expected_origin: AuditFaultOrigin,
    expected_status: AuditFaultStatus,
) -> None:
    command = AuditFailureCommand(
        request_id=f"classification-{category.value.lower()}",
        reason=AuditFailureReason.HTTP_REQUEST_FAILED,
        error_type=_http_error_type(category),
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

    origin = audit_fault_origin(command)

    assert origin is expected_origin
    assert initial_audit_fault_status(command, origin=origin) is expected_status


@given(
    category=st.sampled_from(tuple(PolymarketHttpFailureCategory)),
    retryable=st.booleans(),
    status_code=st.one_of(
        st.none(),
        st.sampled_from(tuple(sorted(_RECOVERABLE_HTTP_STATUS_CODES))),
        st.integers(min_value=100, max_value=599),
    ),
)
@settings(max_examples=256, deadline=None, derandomize=True)
def test_current_v2_fault_status_obeys_exact_external_transient_whitelist(
    category: PolymarketHttpFailureCategory,
    retryable: bool,
    status_code: int | None,
) -> None:
    fixed_nonretryable = {
        PolymarketHttpFailureCategory.CLIENT_CLOSED,
        PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
        PolymarketHttpFailureCategory.REMOTE_PROTOCOL,
        PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
        PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
    }
    if category is PolymarketHttpFailureCategory.HTTP_STATUS:
        truthful = (
            status_code is not None
            and not 200 <= status_code <= 299
            and retryable is (status_code in _RECOVERABLE_HTTP_STATUS_CODES)
        )
    else:
        truthful = (status_code is None or 200 <= status_code <= 299) and not (
            category in fixed_nonretryable and retryable
        )

    def build_command() -> AuditFailureCommand:
        return AuditFailureCommand(
            request_id="hypothesis-current-v2-classification",
            reason=AuditFailureReason.HTTP_REQUEST_FAILED,
            error_type=_http_error_type(category),
            endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
            category=category,
            retryable=retryable,
            attempt=1,
            status_code=status_code,
            retry_after_disposition=PolymarketRetryAfterDisposition.ABSENT,
            retry_exhausted=retryable,
            retry_blocked_reason=(
                PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED if retryable else None
            ),
        )

    if not truthful:
        with pytest.raises(ValueError, match=r"status|retryab|HTTP_STATUS"):
            build_command()
        return
    command = build_command()

    origin = audit_fault_origin(command)
    status = initial_audit_fault_status(command, origin=origin)
    may_start_current = retryable and (
        category
        in {
            PolymarketHttpFailureCategory.TIMEOUT,
            PolymarketHttpFailureCategory.NETWORK,
        }
        or (
            category is PolymarketHttpFailureCategory.HTTP_STATUS
            and status_code in _RECOVERABLE_HTTP_STATUS_CODES
        )
    )

    assert status is (AuditFaultStatus.CURRENT if may_start_current else AuditFaultStatus.PERMANENT)
    if may_start_current:
        assert origin is AuditFaultOrigin.EXTERNAL
