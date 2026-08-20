# Pass 3 — Official Venue Documentation Notes

**Verified:** 2026-08-19  
**Purpose:** Confirm that the generic execution/safety architecture maps to current official venue capabilities. These facts are not hard-coded; implementation must refresh current documentation/reference data.

## Polymarket

Current official documentation confirms:
- CLOB authentication uses wallet/L1 signing to establish credentials and L2 HMAC-authenticated private requests;
- order placement is authenticated and signed;
- authenticated user WebSocket streams order/trade lifecycle updates;
- account-wide cancel-all is available;
- a trading heartbeat mechanism automatically cancels open orders if required heartbeats stop;
- marketable orders may have cancellation timing nuances/pending windows;
- geographic restriction checks are exposed and should be checked before placing orders;
- order and cancellation traffic have venue-specific rate limiting.

Design implications:
- narrow signer capability;
- private lifecycle stream + REST reconciliation;
- venue-native heartbeat safety;
- cancellation priority;
- runtime eligibility preflight;
- no blind retry.

## Kalshi

Current official documentation confirms:
- authenticated requests use an API key ID plus RSA private-key signatures;
- authenticated WebSocket provides user order/fill updates;
- order types include fill-or-kill, immediate-or-cancel, post-only and self-trade-prevention options on current order endpoints;
- batch cancel exists;
- order groups can enforce server-side risk/cancellation behavior and expose lifecycle updates;
- production/demo credentials are separate;
- subaccount-restricted API keys are supported in current API behavior.

Design implications:
- key/subaccount isolation;
- capability-driven order policy;
- server-side order-group risk controls where applicable;
- private lifecycle stream + reconciliation;
- demo is not equivalent to production evidence.

## Kraken

Current official documentation confirms:
- Spot and Derivatives are separate trading engines/API surfaces;
- API permissions distinguish order management from funding/withdrawal capabilities;
- withdrawal permission is separately privileged;
- Spot and Derivatives support cancel-all-after/dead-man's-switch safety mechanisms;
- authenticated order/private trade/open-order data are available;
- current order interfaces support client order identifiers and order flags such as post/reduce-only where applicable.

Design implications:
- separate native adapters;
- automated trading key without withdrawal permission;
- client order IDs;
- dead-man switch;
- authenticated lifecycle/reconciliation;
- margin-specific continuous risk.

## General rule

All venue capabilities above are represented through capability interfaces and runtime/versioned reference data. A current feature is never assumed permanent merely because it existed when this document was written.
