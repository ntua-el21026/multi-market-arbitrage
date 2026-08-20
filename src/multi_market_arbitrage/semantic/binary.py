"""Fail-closed deterministic proof for one Polymarket binary condition."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from multi_market_arbitrage.core.exact import Money
from multi_market_arbitrage.core.ids import (
    AssetId,
    ContractSemanticsVersionId,
    InstrumentId,
    MarketId,
)
from multi_market_arbitrage.reference import ReferenceBundle


class RelationType(StrEnum):
    EQUIVALENT = "EQUIVALENT"
    COMPLEMENTARY = "COMPLEMENTARY"
    IMPLIES = "IMPLIES"
    MUTUALLY_EXCLUSIVE = "MUTUALLY_EXCLUSIVE"
    EXHAUSTIVE_PARTITION_MEMBER = "EXHAUSTIVE_PARTITION_MEMBER"
    UNKNOWN = "UNKNOWN"


class ProofStatus(StrEnum):
    PROVEN = "PROVEN"
    POTENTIAL = "POTENTIAL"
    AMBIGUOUS = "AMBIGUOUS"
    REJECTED = "REJECTED"
    UNPARSEABLE = "UNPARSEABLE"


@dataclass(frozen=True, slots=True)
class BinarySemanticProof:
    relation_type: RelationType
    proof_status: ProofStatus
    market_id: MarketId
    native_condition_id: str
    yes_instrument_id: InstrumentId | None
    no_instrument_id: InstrumentId | None
    payout_asset_id: AssetId
    payout_per_complete_set: Money
    semantics_version_id: ContractSemanticsVersionId
    evidence: tuple[str, ...]
    reason: str | None

    @property
    def is_proven(self) -> bool:
        return (
            self.relation_type is RelationType.COMPLEMENTARY
            and self.proof_status is ProofStatus.PROVEN
            and self.yes_instrument_id is not None
            and self.no_instrument_id is not None
            and self.reason is None
        )


def prove_binary_complement(bundle: ReferenceBundle) -> BinarySemanticProof:
    """Prove only exact same-condition ordinary ``Yes``/``No`` complements.

    Polymarket negative-risk sets, augmented placeholders, and ``Other`` have
    event-level conversion/partition semantics that are outside the Phase 1
    binary proof.  They are rejected rather than inferred from UI grouping.
    """

    instruments = tuple(sorted(bundle.instruments, key=lambda item: item.outcome_index))
    reason: str | None = None
    yes_id: InstrumentId | None = None
    no_id: InstrumentId | None = None
    if bundle.market.neg_risk is not False:
        reason = "negative-risk status is true or unknown; ordinary binary proof is unsupported"
    elif bundle.market.neg_risk_augmented is True:
        reason = "augmented negative-risk semantics are unsupported in Phase 1"
    elif bundle.market.neg_risk_other is True:
        reason = "negative-risk Other semantics are unsupported in Phase 1"
    elif len(instruments) != 2:
        reason = "exactly two outcome instruments are required"
    elif tuple(item.outcome_index for item in instruments) != (0, 1):
        reason = "binary outcomes must retain documented index positions 0 and 1"
    elif tuple(item.outcome_name.casefold() for item in instruments) != ("yes", "no"):
        reason = "outcomes must be exactly Yes and No in documented index order"
    elif any(item.market_id != bundle.market.market_id for item in instruments):
        reason = "outcome instruments are not from the exact same condition"
    else:
        yes_id = instruments[0].instrument_id
        no_id = instruments[1].instrument_id

    status = ProofStatus.PROVEN if reason is None else ProofStatus.REJECTED
    relation = RelationType.COMPLEMENTARY if reason is None else RelationType.UNKNOWN
    version_id = ContractSemanticsVersionId.from_components(
        "PolymarketBinarySemantics-v2",
        bundle.market.market_id,
        bundle.market.native_condition_id,
        "" if yes_id is None else yes_id,
        "" if no_id is None else no_id,
        str(bundle.market.neg_risk),
        str(bundle.market.neg_risk_augmented),
        str(bundle.market.neg_risk_other),
        bundle.payout_asset_id,
        bundle.payout_per_set.to_canonical_string(),
        status.value,
        reason or "",
    )
    evidence = (
        f"condition:{bundle.market.native_condition_id}",
        f"market-version:{bundle.market.version_id}",
        *(f"instrument-version:{item.version_id}" for item in instruments),
        "rule:polymarket-same-condition-yes-no-v2",
    )
    return BinarySemanticProof(
        relation_type=relation,
        proof_status=status,
        market_id=bundle.market.market_id,
        native_condition_id=bundle.market.native_condition_id,
        yes_instrument_id=yes_id,
        no_instrument_id=no_id,
        payout_asset_id=bundle.payout_asset_id,
        payout_per_complete_set=bundle.payout_per_set,
        semantics_version_id=version_id,
        evidence=evidence,
        reason=reason,
    )


__all__ = [
    "BinarySemanticProof",
    "ProofStatus",
    "RelationType",
    "prove_binary_complement",
]
