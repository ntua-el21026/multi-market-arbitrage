"""Canonical Polymarket reference construction and cross-source validation."""

from __future__ import annotations

from dataclasses import replace

from multi_market_arbitrage.core.exact import ExactDecimal, Money, Rate
from multi_market_arbitrage.core.ids import (
    AssetId,
    CostRuleSetVersionId,
    InstrumentId,
    InstrumentVersionId,
    MarketId,
    MarketVersionId,
    OutcomeId,
    PrecisionRuleVersionId,
    VenueId,
)
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.venues.polymarket.schemas import (
    ClobMarketInfo,
    FeeScheduleSchema,
    GammaEvent,
    GammaMarket,
    RestBookSnapshot,
)

from .models import (
    AssetReference,
    BitemporalFields,
    CostCertainty,
    CostRuleSet,
    CostStatus,
    InstrumentReference,
    MarketReference,
    MarketStatus,
    PrecisionRule,
    ReferenceBundle,
    ReferenceStatus,
)

POLYMARKET_VENUE_ID = VenueId("polymarket")

# pUSD is deliberately not identified as native USDC or legacy USDC.e.  The
# current official pUSD page establishes Polygon mainnet, ERC-20, six decimals,
# and USDC backing, but directs contract identity to a separate contracts page.
POLYMARKET_PUSD_ASSET_ID = AssetId.from_components(
    "AssetId", "polymarket-pusd", "eip155:137", "erc20", "6"
)
POLYMARKET_PUSD = AssetReference(
    asset_id=POLYMARKET_PUSD_ASSET_ID,
    symbol="pUSD",
    network="eip155:137",
    token_standard="-".join(("ERC", "20")),
    decimals=6,
    backing_asset="native USDC claim",
)

_FEE_QUANTUM = Money(ExactDecimal.parse("0.00001"))
_SUPPORTED_FEE_EXPONENTS = frozenset({1, 2})


class ReferenceBuildError(ValueError):
    """Cross-source identity or schema data cannot form a canonical reference."""


def _market_status(market: GammaMarket) -> MarketStatus:
    # Gamma `closed` means resolved *or* merely closed; it is not settlement proof.
    if market.closed is True or market.archived is True:
        return MarketStatus.CLOSED
    if market.active is False:
        return MarketStatus.PREOPEN
    if market.accepting_orders is False and market.active is True:
        return MarketStatus.HALTED
    if (
        market.active is True
        and market.closed is False
        and market.archived is False
        and market.accepting_orders is True
        and market.enable_order_book is True
    ):
        return MarketStatus.OPEN
    return MarketStatus.UNKNOWN


def _schedule_signature(
    schedule: FeeScheduleSchema,
) -> tuple[str, int, bool]:
    return (schedule.rate.to_canonical_string(), schedule.exponent, schedule.taker_only)


class PolymarketReferenceBuilder:
    """Build one immutable reference bundle from independent public surfaces."""

    def build(
        self,
        *,
        gamma: GammaMarket,
        clob: ClobMarketInfo,
        observed_at: UtcTimestamp,
        rest_book: RestBookSnapshot | None = None,
        parent_event: GammaEvent | None = None,
        effective_from: UtcTimestamp | None = None,
    ) -> ReferenceBundle:
        if gamma.condition_id != clob.condition_id:
            raise ReferenceBuildError("Gamma and CLOB condition IDs differ")
        if rest_book is not None and rest_book.condition_id != gamma.condition_id:
            raise ReferenceBuildError("REST book and Gamma condition IDs differ")
        if parent_event is not None and gamma.event_id not in {None, parent_event.event_id}:
            raise ReferenceBuildError("Gamma market points to a different parent event")

        gamma_pairs = tuple(zip(gamma.clob_token_ids, gamma.outcomes, strict=True))
        clob_pairs = tuple((token.token_id, token.outcome) for token in clob.tokens)
        if len(gamma_pairs) != len(clob_pairs) or any(
            gamma_token != clob_token or gamma_outcome.casefold() != clob_outcome.casefold()
            for (gamma_token, gamma_outcome), (clob_token, clob_outcome) in zip(
                gamma_pairs, clob_pairs, strict=True
            )
        ):
            raise ReferenceBuildError(
                "Gamma and CLOB token/outcome index mappings are not identical"
            )
        if rest_book is not None and rest_book.asset_id not in gamma.clob_token_ids:
            raise ReferenceBuildError("REST book token is not part of the Gamma market")

        effective = observed_at if effective_from is None else effective_from
        times = BitemporalFields(observed_at=observed_at, effective_from=effective)
        market_id = MarketId.from_native(POLYMARKET_VENUE_ID, gamma.condition_id)
        instruments = self._build_instruments(gamma, market_id, times)
        issues: list[str] = []
        neg_risk = gamma.neg_risk
        if rest_book is not None:
            if neg_risk is None:
                neg_risk = rest_book.neg_risk
            elif neg_risk != rest_book.neg_risk:
                neg_risk = None
                issues.append("Gamma and REST book negative-risk flags disagree")

        augmented = parent_event.neg_risk_augmented if parent_event is not None else None
        if neg_risk is True and parent_event is None:
            issues.append("negative-risk parent event metadata was not observed")
        if (
            parent_event is not None
            and parent_event.enable_neg_risk is not None
            and neg_risk is not None
            and neg_risk != parent_event.enable_neg_risk
        ):
            neg_risk = None
            issues.append("market and parent-event negative-risk flags disagree")

        market = self._build_market(
            gamma=gamma,
            market_id=market_id,
            neg_risk=neg_risk,
            neg_risk_augmented=augmented,
            times=times,
        )
        precision = self._build_precision(
            gamma=gamma,
            clob=clob,
            rest_book=rest_book,
            market_id=market_id,
            times=times,
        )
        if precision.status is ReferenceStatus.INDETERMINATE:
            issues.append(precision.reason or "precision is indeterminate")
        cost = self._build_cost(
            gamma=gamma,
            clob=clob,
            market_id=market_id,
            instrument_ids=tuple(item.instrument_id for item in instruments),
            times=times,
        )
        if cost.status is CostStatus.INDETERMINATE:
            issues.append(cost.reason or "cost is indeterminate")
        return ReferenceBundle(
            market=market,
            instruments=instruments,
            precision=precision,
            cost_rule_set=cost,
            payout_asset_id=POLYMARKET_PUSD_ASSET_ID,
            payout_per_set=Money(ExactDecimal.one()),
            issues=tuple(issues),
        )

    def _build_instruments(
        self,
        gamma: GammaMarket,
        market_id: MarketId,
        times: BitemporalFields,
    ) -> tuple[InstrumentReference, ...]:
        result: list[InstrumentReference] = []
        for index, (token_id, outcome) in enumerate(
            zip(gamma.clob_token_ids, gamma.outcomes, strict=True)
        ):
            instrument_id = InstrumentId.from_native(POLYMARKET_VENUE_ID, token_id)
            outcome_id = OutcomeId.from_components(
                "PolymarketOutcomeId", market_id, str(index), outcome
            )
            version_id = InstrumentVersionId.from_components(
                "PolymarketInstrumentVersion-v1",
                instrument_id,
                market_id,
                token_id,
                outcome_id,
                outcome,
                index,
            )
            result.append(
                InstrumentReference(
                    instrument_id=instrument_id,
                    version_id=version_id,
                    venue_id=POLYMARKET_VENUE_ID,
                    market_id=market_id,
                    native_token_id=token_id,
                    outcome_id=outcome_id,
                    outcome_name=outcome,
                    outcome_index=index,
                    times=times,
                )
            )
        return tuple(result)

    def _build_market(
        self,
        *,
        gamma: GammaMarket,
        market_id: MarketId,
        neg_risk: bool | None,
        neg_risk_augmented: bool | None,
        times: BitemporalFields,
    ) -> MarketReference:
        status = _market_status(gamma)
        version_id = MarketVersionId.from_components(
            "PolymarketMarketVersion-v2",
            market_id,
            gamma.gamma_market_id,
            gamma.event_id or "",
            gamma.question,
            gamma.slug or "",
            status.value,
            str(gamma.active),
            str(gamma.closed),
            str(gamma.archived),
            str(gamma.accepting_orders),
            str(gamma.enable_order_book),
            str(gamma.restricted),
            str(neg_risk),
            str(neg_risk_augmented),
            str(gamma.neg_risk_other),
            gamma.resolution_source or "",
            gamma.resolved_by or "",
            gamma.uma_resolution_status or "",
            str(gamma.automatically_resolved),
            "" if gamma.start_time is None else gamma.start_time.to_iso8601(),
            "" if gamma.end_time is None else gamma.end_time.to_iso8601(),
            "" if gamma.closed_time is None else gamma.closed_time.to_iso8601(),
        )
        return MarketReference(
            market_id=market_id,
            version_id=version_id,
            venue_id=POLYMARKET_VENUE_ID,
            native_condition_id=gamma.condition_id,
            gamma_market_id=gamma.gamma_market_id,
            gamma_event_id=gamma.event_id,
            question=gamma.question,
            slug=gamma.slug,
            status=status,
            active=gamma.active,
            closed=gamma.closed,
            archived=gamma.archived,
            accepting_orders=gamma.accepting_orders,
            enable_order_book=gamma.enable_order_book,
            restricted=gamma.restricted,
            neg_risk=neg_risk,
            neg_risk_augmented=neg_risk_augmented,
            neg_risk_other=gamma.neg_risk_other,
            resolution_source=gamma.resolution_source,
            resolved_by=gamma.resolved_by,
            uma_resolution_status=gamma.uma_resolution_status,
            automatically_resolved=gamma.automatically_resolved,
            start_time=gamma.start_time,
            end_time=gamma.end_time,
            closed_time=gamma.closed_time,
            times=times,
        )

    def _build_precision(
        self,
        *,
        gamma: GammaMarket,
        clob: ClobMarketInfo,
        rest_book: RestBookSnapshot | None,
        market_id: MarketId,
        times: BitemporalFields,
    ) -> PrecisionRule:
        tick_values = [gamma.tick_size, clob.tick_size]
        # Gamma documents `orderMinSize` as a USDC notional, while both CLOB
        # market info and REST books document their minimum as a share quantity.
        # Keep those dimensions separate: Gamma remains an independent tick
        # cross-check but cannot participate in share-minimum reconciliation.
        minimum_values = [clob.minimum_order_size]
        sources = ["gamma:market", "clob:clob-market-info"]
        if rest_book is not None:
            tick_values.append(rest_book.tick_size)
            minimum_values.append(rest_book.minimum_order_size)
            sources.append("clob:rest-book")
        reason: str | None = None
        if any(value is None for value in tick_values):
            reason = "tick size or minimum order quantity is missing from a required source"
        else:
            ticks = tuple(value for value in tick_values if value is not None)
            if len(set(ticks)) != 1:
                reason = "cross-source tick sizes disagree"
            elif len(set(minimum_values)) != 1:
                reason = "cross-source minimum order quantities disagree"
        status = ReferenceStatus.VERIFIED if reason is None else ReferenceStatus.INDETERMINATE
        tick = tick_values[0] if status is ReferenceStatus.VERIFIED else None
        minimum = minimum_values[0] if status is ReferenceStatus.VERIFIED else None
        version_id = PrecisionRuleVersionId.from_components(
            "PolymarketPrecisionRule-v3",
            market_id,
            *("?" if value is None else value.to_canonical_string() for value in tick_values),
            *(value.to_canonical_string() for value in minimum_values),
            status.value,
            reason or "",
            *sources,
        )
        return PrecisionRule(
            version_id=version_id,
            market_id=market_id,
            tick_size=tick,
            minimum_order_size=minimum,
            status=status,
            reason=reason,
            sources=tuple(sources),
            times=times,
        )

    def _build_cost(
        self,
        *,
        gamma: GammaMarket,
        clob: ClobMarketInfo,
        market_id: MarketId,
        instrument_ids: tuple[InstrumentId, ...],
        times: BitemporalFields,
    ) -> CostRuleSet:
        gamma_schedule = gamma.fee_schedule
        clob_schedule = clob.fee_schedule
        reason: str | None = None
        selected: FeeScheduleSchema | None = None
        if gamma.fees_enabled is False:
            nonzero_schedule = next(
                (
                    schedule
                    for schedule in (gamma_schedule, clob_schedule)
                    if schedule is not None and schedule.rate != ExactDecimal.zero()
                ),
                None,
            )
            if nonzero_schedule is not None:
                reason = "feesEnabled=false conflicts with a nonzero fee schedule"
            elif clob_schedule is not None:
                # `feesEnabled=false` plus an explicit zero CLOB `fd` schedule
                # are two independent public signals. Gamma may omit feeSchedule
                # entirely for a fee-free market.
                selected = clob_schedule
            else:
                reason = "fee-free Gamma flag lacks an explicit zero CLOB fd schedule"
        elif gamma_schedule is None or clob_schedule is None:
            reason = "current fee schedule is missing from Gamma or CLOB market info"
        elif _schedule_signature(gamma_schedule) != _schedule_signature(clob_schedule):
            reason = "Gamma feeSchedule and CLOB fd parameters disagree"
        else:
            selected = gamma_schedule

        if selected is not None:
            if selected.rate < ExactDecimal.zero():
                reason = "negative fee rate is unsupported"
                selected = None
            elif selected.exponent not in _SUPPORTED_FEE_EXPONENTS:
                reason = f"unsupported fee exponent {selected.exponent}"
                selected = None
        status = (
            CostStatus.DETERMINATE
            if selected is not None and reason is None
            else CostStatus.INDETERMINATE
        )
        sources = ("gamma:feeSchedule", "clob:clob-market-info:fd")
        version_id = CostRuleSetVersionId.from_components(
            "PolymarketCostRuleSet-v2",
            market_id,
            "?" if gamma_schedule is None else str(_schedule_signature(gamma_schedule)),
            "?" if clob_schedule is None else str(_schedule_signature(clob_schedule)),
            "?"
            if gamma_schedule is None or gamma_schedule.rebate_rate is None
            else gamma_schedule.rebate_rate.to_canonical_string(),
            str(gamma.fees_enabled),
            *instrument_ids,
            POLYMARKET_PUSD_ASSET_ID,
            _FEE_QUANTUM.to_canonical_string(),
            status.value,
            reason or "",
            *sources,
        )
        return CostRuleSet(
            version_id=version_id,
            venue_id=POLYMARKET_VENUE_ID,
            market_id=market_id,
            applicable_instrument_ids=instrument_ids,
            status=status,
            certainty=(
                CostCertainty.KNOWN_DETERMINISTIC
                if status is CostStatus.DETERMINATE
                else CostCertainty.UNKNOWN
            ),
            rate=None if selected is None else Rate(selected.rate),
            exponent=None if selected is None else selected.exponent,
            taker_only=None if selected is None else selected.taker_only,
            rebate_rate=(
                None
                if selected is None or selected.rebate_rate is None
                else Rate(selected.rebate_rate)
            ),
            fee_asset_id=POLYMARKET_PUSD_ASSET_ID,
            fee_quantum=_FEE_QUANTUM,
            reason=reason,
            sources=sources,
            times=times,
        )

    def with_tick_size(
        self,
        bundle: ReferenceBundle,
        *,
        new_tick_size: ExactDecimal,
        observed_at: UtcTimestamp,
        effective_from: UtcTimestamp,
        cross_source_confirmed: bool = False,
    ) -> ReferenceBundle:
        """Version a documented tick update and keep audit status explicit."""

        if new_tick_size <= ExactDecimal.zero():
            raise ValueError("new tick size must be positive")
        status = (
            ReferenceStatus.VERIFIED if cross_source_confirmed else ReferenceStatus.INDETERMINATE
        )
        reason = (
            None if cross_source_confirmed else "dynamic tick update awaits REST/reference audit"
        )
        times = BitemporalFields(observed_at=observed_at, effective_from=effective_from)
        version_id = PrecisionRuleVersionId.from_components(
            "PolymarketPrecisionRule-tick-event-v1",
            bundle.market.market_id,
            new_tick_size.to_canonical_string(),
            bundle.precision.minimum_order_size.to_canonical_string()
            if bundle.precision.minimum_order_size is not None
            else "?",
            status.value,
        )
        precision = PrecisionRule(
            version_id=version_id,
            market_id=bundle.market.market_id,
            tick_size=new_tick_size,
            minimum_order_size=bundle.precision.minimum_order_size,
            status=status,
            reason=reason,
            sources=("clob-wss:tick_size_change",),
            times=times,
        )
        issues = tuple(issue for issue in bundle.issues if issue != bundle.precision.reason)
        if reason is not None:
            issues = (*issues, reason)
        return replace(bundle, precision=precision, issues=issues)


__all__ = [
    "POLYMARKET_PUSD",
    "POLYMARKET_PUSD_ASSET_ID",
    "POLYMARKET_VENUE_ID",
    "PolymarketReferenceBuilder",
    "ReferenceBuildError",
]
