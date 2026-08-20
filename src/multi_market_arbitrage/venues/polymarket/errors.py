"""Typed Polymarket public-adapter failures."""

from __future__ import annotations


class PolymarketAdapterError(Exception):
    """Base class for Polymarket adapter failures."""


class PolymarketTransportError(PolymarketAdapterError):
    """A public HTTP or WebSocket transport operation failed."""


class PolymarketSchemaError(PolymarketAdapterError, ValueError):
    """A payload did not satisfy the documented public schema."""

    def __init__(self, path: str, message: str) -> None:
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}")


class UnsupportedPolymarketEventError(PolymarketSchemaError):
    """A well-formed payload declares an event type this adapter does not support."""
