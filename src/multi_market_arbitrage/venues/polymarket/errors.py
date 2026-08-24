"""Typed, non-secret failures for the public Polymarket adapter."""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Final, Self


class PolymarketEndpointClass(StrEnum):
    """Bounded labels for the fixed public endpoints exposed by the adapter."""

    GAMMA_GENERAL = "GAMMA_GENERAL"
    GAMMA_MARKETS = "GAMMA_MARKETS"
    CLOB_GENERAL = "CLOB_GENERAL"
    CLOB_BOOK = "CLOB_BOOK"
    CLOB_BOOKS = "CLOB_BOOKS"
    MARKET_WEBSOCKET = "MARKET_WEBSOCKET"


class PolymarketHttpFailureCategory(StrEnum):
    """Finite HTTP failure categories suitable for metrics and evidence."""

    CLIENT_CLOSED = "CLIENT_CLOSED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    TIMEOUT = "TIMEOUT"
    NETWORK = "NETWORK"
    REMOTE_PROTOCOL = "REMOTE_PROTOCOL"
    LOCAL_PROTOCOL = "LOCAL_PROTOCOL"
    RESPONSE_PROTOCOL = "RESPONSE_PROTOCOL"
    HTTP_STATUS = "HTTP_STATUS"


class PolymarketRetryBlockedReason(StrEnum):
    """Why an otherwise retryable public read could not make another attempt."""

    ATTEMPTS_EXHAUSTED = "ATTEMPTS_EXHAUSTED"
    DEADLINE = "DEADLINE"
    RETRY_AFTER_POLICY = "RETRY_AFTER_POLICY"


class PolymarketRetryAfterDisposition(StrEnum):
    """Bounded parse disposition for an untrusted Retry-After header."""

    ABSENT = "ABSENT"
    PARSED = "PARSED"
    MALFORMED = "MALFORMED"


RECOVERABLE_HTTP_STATUS_CODES: Final = frozenset({408, 425, 429, 500, 502, 503, 504})
MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS: Final = 10_000_000_000_000_000_000
MAXIMUM_RETRY_AFTER_EVIDENCE_SECONDS: Final = (
    MAXIMUM_RETRY_AFTER_EVIDENCE_NANOSECONDS / 1_000_000_000
)


def validate_polymarket_http_failure_metadata(
    *,
    category: PolymarketHttpFailureCategory,
    retryable: bool,
    status_code: int | None,
    retry_after_disposition: PolymarketRetryAfterDisposition,
    has_retry_after: bool,
    retry_exhausted: bool,
    retry_blocked_reason: PolymarketRetryBlockedReason | None,
    final_failure: bool,
) -> None:
    """Reject contradictory public-HTTP failure evidence.

    Transport/protocol failures may carry a status only after a successful
    response header, so that status must be 2xx.  Conversely, ``HTTP_STATUS``
    denotes a non-success response and its retryability is the adapter's fixed
    public-read whitelist.  The retry-policy fields describe a final failure
    only when ``final_failure`` is true.
    """

    if category is PolymarketHttpFailureCategory.HTTP_STATUS:
        if status_code is None or 200 <= status_code <= 299:
            raise ValueError("HTTP_STATUS failure requires a non-success status code")
        expected_retryable = status_code in RECOVERABLE_HTTP_STATUS_CODES
        if retryable is not expected_retryable:
            raise ValueError("HTTP_STATUS retryability contradicts the fixed status whitelist")
    elif status_code is not None and not 200 <= status_code <= 299:
        raise ValueError("non-HTTP_STATUS failure cannot carry a non-success status code")

    if (
        category is not PolymarketHttpFailureCategory.HTTP_STATUS
        and retry_after_disposition is not PolymarketRetryAfterDisposition.ABSENT
    ):
        raise ValueError("only HTTP_STATUS failures may carry Retry-After evidence")
    if (retry_after_disposition is PolymarketRetryAfterDisposition.PARSED) is not has_retry_after:
        raise ValueError("parsed Retry-After disposition must agree with its delay")

    fixed_nonretryable = {
        PolymarketHttpFailureCategory.CLIENT_CLOSED,
        PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
        PolymarketHttpFailureCategory.REMOTE_PROTOCOL,
        PolymarketHttpFailureCategory.LOCAL_PROTOCOL,
        PolymarketHttpFailureCategory.RESPONSE_PROTOCOL,
    }
    if category in fixed_nonretryable and retryable:
        raise ValueError(f"{category.value} failure cannot be retryable")

    attempts_exhausted = retry_blocked_reason is PolymarketRetryBlockedReason.ATTEMPTS_EXHAUSTED
    if retry_exhausted is not attempts_exhausted:
        raise ValueError("retry exhaustion and blocked reason disagree")
    if retry_blocked_reason is not None and not retryable:
        raise ValueError("non-retryable failure cannot carry a retry-blocked reason")
    if retry_blocked_reason is PolymarketRetryBlockedReason.RETRY_AFTER_POLICY and (
        category is not PolymarketHttpFailureCategory.HTTP_STATUS
        or retry_after_disposition is not PolymarketRetryAfterDisposition.PARSED
    ):
        raise ValueError("Retry-After policy block requires parsed HTTP_STATUS evidence")
    if final_failure and retryable and retry_blocked_reason is None:
        raise ValueError("final retryable failure requires a retry-blocked reason")


class PolymarketWebSocketFailureCategory(StrEnum):
    """Finite WebSocket transport/protocol failure categories."""

    CONNECTION = "CONNECTION"
    CONNECTION_CLOSED = "CONNECTION_CLOSED"
    NORMAL_CLOSURE = "NORMAL_CLOSURE"
    HANDSHAKE = "HANDSHAKE"
    PROTOCOL = "PROTOCOL"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"


class PolymarketAdapterError(Exception):
    """Base class for Polymarket adapter failures."""


class PolymarketTransportError(PolymarketAdapterError):
    """Compatibility base for typed public HTTP and WebSocket I/O failures."""


class PolymarketHttpError(PolymarketTransportError):
    """Base class carrying bounded evidence for one public HTTP failure."""

    def __init__(
        self,
        *,
        endpoint_class: PolymarketEndpointClass,
        category: PolymarketHttpFailureCategory,
        retryable: bool,
        attempt: int,
        detail: str,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        retry_after_disposition: PolymarketRetryAfterDisposition | None = None,
        retry_exhausted: bool = False,
        retry_blocked_reason: PolymarketRetryBlockedReason | None = None,
    ) -> None:
        if endpoint_class is PolymarketEndpointClass.MARKET_WEBSOCKET:
            raise ValueError("HTTP errors require an HTTP endpoint class")
        if attempt < 0:
            raise ValueError("HTTP attempt cannot be negative")
        if attempt == 0 and category is not PolymarketHttpFailureCategory.DEADLINE_EXCEEDED:
            raise ValueError(
                "HTTP attempt zero is reserved for deadline expiry before the first attempt"
            )
        if status_code is not None and not 100 <= status_code <= 599:
            raise ValueError("HTTP status code must be between 100 and 599")
        if retry_after_seconds is not None and (
            not math.isfinite(retry_after_seconds)
            or not 0 <= retry_after_seconds <= MAXIMUM_RETRY_AFTER_EVIDENCE_SECONDS
        ):
            raise ValueError("Retry-After delay must be finite and within the evidence bound")
        if retry_after_disposition is None:
            retry_after_disposition = (
                PolymarketRetryAfterDisposition.PARSED
                if retry_after_seconds is not None
                else PolymarketRetryAfterDisposition.ABSENT
            )
        if (retry_after_disposition is PolymarketRetryAfterDisposition.PARSED) != (
            retry_after_seconds is not None
        ):
            raise ValueError("parsed Retry-After disposition must agree with its delay")
        validate_polymarket_http_failure_metadata(
            category=category,
            retryable=retryable,
            status_code=status_code,
            retry_after_disposition=retry_after_disposition,
            has_retry_after=retry_after_seconds is not None,
            retry_exhausted=retry_exhausted,
            retry_blocked_reason=retry_blocked_reason,
            final_failure=False,
        )
        if not detail or detail != detail.strip():
            raise ValueError("HTTP error detail must be non-empty without surrounding whitespace")
        self.endpoint_class = endpoint_class
        self.category = category
        self.retryable = retryable
        self.attempt = attempt
        self.detail = detail
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.retry_after_disposition = retry_after_disposition
        self.retry_exhausted = retry_exhausted
        self.retry_blocked_reason = retry_blocked_reason
        super().__init__(self._message())

    @property
    def attempt_count(self) -> int:
        """Compatibility-friendly spelling used by operational evidence."""

        return self.attempt

    def _message(self) -> str:
        fields = [
            "public HTTP request failed",
            f"endpoint={self.endpoint_class.value}",
            f"category={self.category.value}",
            f"attempt={self.attempt}",
            f"retryable={str(self.retryable).lower()}",
        ]
        if self.status_code is not None:
            fields.append(f"status={self.status_code}")
        if self.retry_after_disposition is not PolymarketRetryAfterDisposition.ABSENT:
            fields.append(f"retry_after={self.retry_after_disposition.value}")
        if self.retry_exhausted:
            fields.append("retry_exhausted=true")
        if self.retry_blocked_reason is not None:
            fields.append(f"retry_blocked={self.retry_blocked_reason.value}")
        fields.append(self.detail)
        return "; ".join(fields)

    def finalized(
        self,
        *,
        reason: PolymarketRetryBlockedReason,
        retry_exhausted: bool,
    ) -> Self:
        """Return the same typed failure with final retry-policy evidence attached."""

        return type(self)(
            endpoint_class=self.endpoint_class,
            category=self.category,
            retryable=self.retryable,
            attempt=self.attempt,
            detail=self.detail,
            status_code=self.status_code,
            retry_after_seconds=self.retry_after_seconds,
            retry_after_disposition=self.retry_after_disposition,
            retry_exhausted=retry_exhausted,
            retry_blocked_reason=reason,
        )


class PolymarketHttpTransportError(PolymarketHttpError):
    """A public HTTP operation failed before a usable response was received."""


class PolymarketHttpStatusError(PolymarketHttpError):
    """A public HTTP endpoint returned a non-success status."""

    def __init__(
        self,
        *,
        endpoint_class: PolymarketEndpointClass,
        category: PolymarketHttpFailureCategory,
        retryable: bool,
        attempt: int,
        detail: str,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        retry_after_disposition: PolymarketRetryAfterDisposition | None = None,
        retry_exhausted: bool = False,
        retry_blocked_reason: PolymarketRetryBlockedReason | None = None,
    ) -> None:
        if status_code is None:
            raise ValueError("HTTP status errors require status_code")
        super().__init__(
            endpoint_class=endpoint_class,
            category=category,
            retryable=retryable,
            attempt=attempt,
            detail=detail,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
            retry_after_disposition=retry_after_disposition,
            retry_exhausted=retry_exhausted,
            retry_blocked_reason=retry_blocked_reason,
        )


class PolymarketHttpProtocolError(PolymarketHttpError):
    """A response violated bounded public HTTP framing requirements."""


class PolymarketWebSocketError(PolymarketTransportError):
    """Base class carrying bounded evidence for one market WebSocket failure."""

    def __init__(
        self,
        *,
        category: PolymarketWebSocketFailureCategory,
        retryable: bool,
        attempt: int = 1,
        detail: str,
    ) -> None:
        if attempt <= 0:
            raise ValueError("WebSocket attempt must be positive")
        if not detail or detail != detail.strip():
            raise ValueError(
                "WebSocket error detail must be non-empty without surrounding whitespace"
            )
        self.endpoint_class = PolymarketEndpointClass.MARKET_WEBSOCKET
        self.category = category
        self.retryable = retryable
        self.attempt = attempt
        self.detail = detail
        super().__init__(
            "; ".join(
                (
                    "public market WebSocket failed",
                    f"category={category.value}",
                    f"attempt={attempt}",
                    f"retryable={str(retryable).lower()}",
                    detail,
                )
            )
        )

    @property
    def attempt_count(self) -> int:
        return self.attempt


class PolymarketWebSocketTransportError(PolymarketWebSocketError):
    """The public market WebSocket connection or continuity failed."""


class PolymarketWebSocketProtocolError(PolymarketWebSocketError):
    """The public market WebSocket handshake or framing protocol failed."""


class PolymarketSchemaError(PolymarketAdapterError, ValueError):
    """A payload did not satisfy the documented public schema."""

    def __init__(self, path: str, message: str) -> None:
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}")


class UnsupportedPolymarketEventError(PolymarketSchemaError):
    """A well-formed payload declares an event type this adapter does not support."""
