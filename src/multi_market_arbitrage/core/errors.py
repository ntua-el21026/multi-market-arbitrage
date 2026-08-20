"""Typed fault hierarchy and explicit retry classification."""

from __future__ import annotations

from enum import StrEnum


class FaultClass(StrEnum):
    INFRASTRUCTURE = "INFRASTRUCTURE"
    CONFIGURATION = "CONFIGURATION"
    PROGRAMMING_INVARIANT = "PROGRAMMING_INVARIANT"


class RetryDisposition(StrEnum):
    NEVER = "NEVER"
    BOUNDED_SAFE_RETRY = "BOUNDED_SAFE_RETRY"
    RECONCILE_BEFORE_RETRY = "RECONCILE_BEFORE_RETRY"


class MultiMarketArbitrageError(Exception):
    """Base for implementation faults, not ordinary external-data invalidity."""


class TypedFault(MultiMarketArbitrageError):
    """Operational fault with machine-readable classification."""

    fault_class: FaultClass

    def __init__(
        self,
        message: str,
        *,
        code: str,
        component: str | None = None,
        correlation_id: str | None = None,
        retry: RetryDisposition = RetryDisposition.NEVER,
    ) -> None:
        if not message:
            raise ValueError("fault message must not be empty")
        if not code:
            raise ValueError("fault code must not be empty")
        super().__init__(message)
        self.code = code
        self.component = component
        self.correlation_id = correlation_id
        self.retry = retry


class InfrastructureFault(TypedFault):
    fault_class = FaultClass.INFRASTRUCTURE


class TransientInfrastructureFault(InfrastructureFault):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        component: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            component=component,
            correlation_id=correlation_id,
            retry=RetryDisposition.BOUNDED_SAFE_RETRY,
        )


class NetworkFault(TransientInfrastructureFault):
    pass


class RateLimitFault(TransientInfrastructureFault):
    pass


class StorageFault(InfrastructureFault):
    pass


class ConfigurationFault(TypedFault):
    fault_class = FaultClass.CONFIGURATION

    def __init__(
        self,
        message: str,
        *,
        code: str = "INVALID_CONFIGURATION",
        component: str | None = "configuration",
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            component=component,
            correlation_id=correlation_id,
            retry=RetryDisposition.NEVER,
        )


class ProgrammingInvariantViolation(TypedFault):
    fault_class = FaultClass.PROGRAMMING_INVARIANT

    def __init__(
        self,
        message: str,
        *,
        code: str = "PROGRAMMING_INVARIANT_VIOLATION",
        component: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            component=component,
            correlation_id=correlation_id,
            retry=RetryDisposition.NEVER,
        )


__all__ = [
    "ConfigurationFault",
    "FaultClass",
    "InfrastructureFault",
    "MultiMarketArbitrageError",
    "NetworkFault",
    "ProgrammingInvariantViolation",
    "RateLimitFault",
    "RetryDisposition",
    "StorageFault",
    "TransientInfrastructureFault",
    "TypedFault",
]
