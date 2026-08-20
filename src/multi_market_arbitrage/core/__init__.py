"""Canonical Phase 1 domain foundation."""

from .errors import (
    ConfigurationFault,
    FaultClass,
    InfrastructureFault,
    MultiMarketArbitrageError,
    NetworkFault,
    ProgrammingInvariantViolation,
    RateLimitFault,
    RetryDisposition,
    StorageFault,
    TransientInfrastructureFault,
    TypedFault,
)
from .exact import (
    ExactArithmeticError,
    ExactDecimal,
    InexactDivisionError,
    Money,
    Price,
    Quantity,
    Rate,
    RoundingMode,
)
from .health import BookHealth, ComponentHealth, MarketStatus, PersistenceAuthority
from .serialization import (
    CanonicalSerializationError,
    canonical_json_bytes,
    canonical_json_string,
    canonical_sha256,
    canonicalize,
)

__all__ = [
    "BookHealth",
    "CanonicalSerializationError",
    "ComponentHealth",
    "ConfigurationFault",
    "ExactArithmeticError",
    "ExactDecimal",
    "FaultClass",
    "InexactDivisionError",
    "InfrastructureFault",
    "MarketStatus",
    "Money",
    "MultiMarketArbitrageError",
    "NetworkFault",
    "PersistenceAuthority",
    "Price",
    "ProgrammingInvariantViolation",
    "Quantity",
    "Rate",
    "RateLimitFault",
    "RetryDisposition",
    "RoundingMode",
    "StorageFault",
    "TransientInfrastructureFault",
    "TypedFault",
    "canonical_json_bytes",
    "canonical_json_string",
    "canonical_sha256",
    "canonicalize",
]
