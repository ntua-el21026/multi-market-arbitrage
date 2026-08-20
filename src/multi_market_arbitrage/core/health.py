"""Scoped health, authority, and market lifecycle enums."""

from enum import StrEnum


class BookHealth(StrEnum):
    UNINITIALIZED = "UNINITIALIZED"
    SYNCING = "SYNCING"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    INVALID = "INVALID"
    RESYNCING = "RESYNCING"
    CLOSED = "CLOSED"
    UNAVAILABLE = "UNAVAILABLE"


class ComponentHealth(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    INVALID = "INVALID"
    DISCONNECTED = "DISCONNECTED"
    RECONCILING = "RECONCILING"
    PAUSED = "PAUSED"
    UNSAFE = "UNSAFE"


class PersistenceAuthority(StrEnum):
    PROVISIONAL = "PROVISIONAL"
    AUTHORITATIVE = "AUTHORITATIVE"


class MarketStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    PREOPEN = "PREOPEN"
    OPEN = "OPEN"
    HALTED = "HALTED"
    CLOSED = "CLOSED"
    SETTLED = "SETTLED"
    CANCELED = "CANCELED"


__all__ = ["BookHealth", "ComponentHealth", "MarketStatus", "PersistenceAuthority"]
