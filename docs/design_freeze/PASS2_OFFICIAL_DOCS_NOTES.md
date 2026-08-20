# Pass 2 — Official Venue Documentation Notes

**Purpose:** Record the official documentation surfaces consulted while checking whether the generic research architecture remains compatible with current venue mechanics.

These are not hard-coded assumptions. Venue rules remain runtime/versioned reference data.

## Polymarket

Current official documentation confirms that:
- trading fees apply to certain markets and can fund maker rebates;
- maker rebates are a distinct incentive mechanism;
- order-book depth matters for size and large orders can move execution price;
- orders follow a venue-defined lifecycle.

Implementation must therefore obtain current fee/rebate rules rather than embed design-time constants.

## Kalshi

Current official documentation confirms that:
- live order-book updates are streamed incrementally;
- order-book representation has side/price conventions that must be normalized explicitly;
- fee changes can be scheduled/exposed through API reference data;
- request-rate limits are tier/token based.

Implementation must preserve subscription/representation semantics and use versioned fee/rate-limit reference data.

## Kraken

Current official documentation confirms that:
- Spot and Derivatives are distinct trading engines/API surfaces;
- derivatives provide WebSocket feeds and rate-limit semantics;
- current developer documentation contains changeable fee/API behavior.

Implementation must keep native adapters separate beneath canonical interfaces and treat current fee/funding/rate-limit rules as runtime reference data.

## Research consequence

Pass 2 intentionally defines a generic `CostRuleSet`, `CostContext`, event-driven replay, and venue capability model rather than assuming fixed venue economics.
