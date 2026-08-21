"""Explicit, fail-closed Polymarket observation-target contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from multi_market_arbitrage.reference import ReferenceBundle
from multi_market_arbitrage.semantic import prove_binary_complement
from multi_market_arbitrage.venues.polymarket.schemas import GammaMarket


class ObservationTargetMode(StrEnum):
    EXPLICIT_GATE1 = "EXPLICIT_GATE1"
    DEVELOPER_DISCOVERY = "DEVELOPER_DISCOVERY"


class TargetSelectionError(RuntimeError):
    """A requested public market cannot satisfy Phase-1 target semantics."""


@dataclass(frozen=True, slots=True)
class ResolvedPolymarketTarget:
    mode: ObservationTargetMode
    requested_gamma_market_id: str | None
    resolved_gamma_market_id: str
    condition_id: str
    event_id: str | None
    yes_token_id: str
    no_token_id: str
    reference_version_ids: tuple[str, ...]
    semantics_version_id: str
    selection_rationale: str

    @property
    def gate1_explicit(self) -> bool:
        return self.mode is ObservationTargetMode.EXPLICIT_GATE1

    def to_record(self) -> dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "event_id": self.event_id,
            "mode": self.mode.value,
            "no_token_id": self.no_token_id,
            "reference_version_ids": self.reference_version_ids,
            "requested_gamma_market_id": self.requested_gamma_market_id,
            "resolved_gamma_market_id": self.resolved_gamma_market_id,
            "selection_rationale": self.selection_rationale,
            "semantics_version_id": self.semantics_version_id,
            "yes_token_id": self.yes_token_id,
        }


def require_supported_gamma_market(
    market: GammaMarket,
    *,
    requested_gamma_market_id: str | None,
) -> None:
    """Validate every externally changeable target lifecycle/shape field."""

    if (
        requested_gamma_market_id is not None
        and market.gamma_market_id != requested_gamma_market_id
    ):
        raise TargetSelectionError("direct Gamma response does not match the requested market ID")
    checks = (
        (market.active is True, "target is not explicitly active"),
        (market.closed is False, "target is closed or closure state is unknown"),
        (market.archived is False, "target is archived or archive state is unknown"),
        (
            market.accepting_orders is True,
            "target is not explicitly accepting orders",
        ),
        (
            market.enable_order_book is True,
            "target does not explicitly expose an order book",
        ),
        (market.neg_risk is False, "negative-risk status is true or unknown"),
        (
            market.neg_risk_other is not True,
            "negative-risk Other semantics are unsupported",
        ),
        (len(market.outcomes) == 2, "target must have exactly two outcomes"),
        (
            tuple(item.casefold() for item in market.outcomes) == ("yes", "no"),
            "target outcomes must be ordered exactly YES/NO",
        ),
        (len(market.clob_token_ids) == 2, "target must resolve exactly two token IDs"),
        (
            len(set(market.clob_token_ids)) == 2,
            "target YES/NO token IDs must be distinct",
        ),
    )
    for valid, message in checks:
        if not valid:
            raise TargetSelectionError(message)


def resolve_polymarket_target(
    market: GammaMarket,
    bundle: ReferenceBundle,
    *,
    requested_gamma_market_id: str | None,
) -> ResolvedPolymarketTarget:
    """Freeze an exact target only after canonical reference/semantic proof."""

    require_supported_gamma_market(
        market,
        requested_gamma_market_id=requested_gamma_market_id,
    )
    if bundle.market.native_condition_id != market.condition_id:
        raise TargetSelectionError("canonical reference condition differs from Gamma target")
    proof = prove_binary_complement(bundle)
    if not proof.is_proven:
        raise TargetSelectionError(proof.reason or "ordinary binary semantic proof failed")
    yes_token_id, no_token_id = market.clob_token_ids
    yes_reference = bundle.instrument_for_token(yes_token_id)
    no_reference = bundle.instrument_for_token(no_token_id)
    if yes_reference.instrument_id != proof.yes_instrument_id:
        raise TargetSelectionError("YES token does not match the proven YES instrument")
    if no_reference.instrument_id != proof.no_instrument_id:
        raise TargetSelectionError("NO token does not match the proven NO instrument")
    explicit = requested_gamma_market_id is not None
    return ResolvedPolymarketTarget(
        mode=(
            ObservationTargetMode.EXPLICIT_GATE1
            if explicit
            else ObservationTargetMode.DEVELOPER_DISCOVERY
        ),
        requested_gamma_market_id=requested_gamma_market_id,
        resolved_gamma_market_id=market.gamma_market_id,
        condition_id=market.condition_id,
        event_id=market.event_id,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        reference_version_ids=tuple(item.value for item in bundle.current_version_ids),
        semantics_version_id=proof.semantics_version_id.value,
        selection_rationale=(
            "operator-specified ordinary-binary Gate-1 target"
            if explicit
            else "bounded developer discovery; not Gate-1 eligible"
        ),
    )


__all__ = [
    "ObservationTargetMode",
    "ResolvedPolymarketTarget",
    "TargetSelectionError",
    "require_supported_gamma_market",
    "resolve_polymarket_target",
]
