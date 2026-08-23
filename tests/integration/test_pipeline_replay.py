from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import cast

import pytest

from multi_market_arbitrage.cli.main import EXIT_OK, EXIT_REPLAY_MISMATCH, main
from multi_market_arbitrage.core.config import load_phase1_config
from multi_market_arbitrage.core.ids import (
    CanonicalEventId,
    CollectorRunId,
    ConnectionId,
    InstrumentId,
    SubscriptionId,
    WriterId,
)
from multi_market_arbitrage.core.time import MonotonicTimestamp, UtcTimestamp
from multi_market_arbitrage.ingestion import RawEventEnvelope
from multi_market_arbitrage.journal import (
    JournalCorruptionError,
    JournalDurabilityState,
    JournalReader,
    JournalReceipt,
    JournalWriterConfig,
    RawJournalWriter,
    completion_path_for_run,
    load_run_completion_head,
)
from multi_market_arbitrage.normalization import CanonicalEventType
from multi_market_arbitrage.observability import InMemoryMetrics
from multi_market_arbitrage.pipeline import (
    CLOB_MARKET_PREFIX,
    CONTROL_BOOK_INVALIDATION,
    CONTROL_REST_AUDIT_START,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
    REST_BOOK_AUDIT,
    WS_MARKET,
    AuditFailureReason,
    AuditOutcome,
    CanonicalProcessor,
)
from multi_market_arbitrage.pipeline import observe as observe_module
from multi_market_arbitrage.pipeline.health import ObservationRuntimeHealth
from multi_market_arbitrage.pipeline.observe import PolymarketObservationService
from multi_market_arbitrage.pipeline.processor import ProcessedEnvelope, ReferenceRefreshRequest
from multi_market_arbitrage.reference import POLYMARKET_VENUE_ID
from multi_market_arbitrage.replay import (
    ReplayTerminalHashMismatch,
    rebuild_analytical_from_journal,
    replay_journal,
    replay_run_ids,
)
from multi_market_arbitrage.research import (
    Phase1AnalyticalOutputs,
    SupervisedAnalyticalOutputs,
    build_scan_report,
    validate_data,
)
from multi_market_arbitrage.state.order_books import InvalidationReason
from multi_market_arbitrage.storage import (
    DuckDBReadOnlyQuery,
    ReadOnlyQueryError,
    analytical_run_completion_path,
    load_analytical_run_completion,
)
from multi_market_arbitrage.venues.polymarket import (
    PolymarketEndpointClass,
    PolymarketHttpFailureCategory,
    PolymarketHttpTransportError,
    PolymarketPublicClient,
    PublicReadEvidence,
    PublicReadMetricsSnapshot,
    PublicReadResult,
    ReceivedWebSocketMessage,
)
from multi_market_arbitrage.venues.polymarket.schemas import GammaMarket, parse_gamma_markets

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"

_CALIBRATION_CONDITION_ID = "0x39ea1503427df7f1439e7dd6567aad5ed72284ed49c58d09e5b917a24389fab7"
_CALIBRATION_YES_ASSET_ID = (
    "18258395755041392118198940898057846234774760894152262988568951221464458542850"
)
_CALIBRATION_NO_ASSET_ID = (
    "114727316628172873125190766157418606846347212023742363393254518987604640648589"
)
_CALIBRATION_BBO_YES = (
    b'{"market":"0x39ea1503427df7f1439e7dd6567aad5ed72284ed49c58d09e5b917a24389fab7", '
    b'"asset_id":"18258395755041392118198940898057846234774760894152262988568951221464'
    b'458542850", "best_bid":"0.014", "best_ask":"0.015", "spread":"0.001", '
    b'"timestamp":"1787430801823", "event_type":"best_bid_ask"}'
)
_CALIBRATION_BBO_NO = (
    b'{"market":"0x39ea1503427df7f1439e7dd6567aad5ed72284ed49c58d09e5b917a24389fab7", '
    b'"asset_id":"11472731662817287312519076615741860684634721202374236339325451898760'
    b'4640648589", "best_bid":"0.985", "best_ask":"0.986", "spread":"0.001", '
    b'"timestamp":"1787430801823", "event_type":"best_bid_ask"}'
)
_CALIBRATION_PRICE_CHANGE = (
    b'{"market":"0x39ea1503427df7f1439e7dd6567aad5ed72284ed49c58d09e5b917a24389fab7", '
    b'"price_changes":[{"asset_id":"182583957550413921181989408980578462347747608941522'
    b'62988568951221464458542850", "price":"0.015", "size":"3492.31", "side":"SELL", '
    b'"hash":"1f2c7ba0fe2bdfa345aea237d2c49f6af8d2c19c", "best_bid":"0.014", '
    b'"best_ask":"0.015"}, {"asset_id":"1147273166281728731251907661574186068463472120'
    b'23742363393254518987604640648589", "price":"0.985", "size":"3492.31", '
    b'"side":"BUY", "hash":"9f0ad839d3406c2818ae5da598e435f52b99dc7d", '
    b'"best_bid":"0.985", "best_ask":"0.986"}], "timestamp":"1787430801823", '
    b'"event_type":"price_change"}'
)

# Exact payload bytes preserved from sealed calibration run
# 01a02b58-1a43-7a91-b4ff-357a7ec582c5.  The sequence 480-484 transition
# demonstrates the venue's BBO -> add-new-top -> remove-old-top framing; the
# later sequence 620-621 transition repeats it on the opposite book sides.
_SEALED_CALIBRATION_SEQ_480 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAiYXNzZXRfaWQiOiIxODI1ODM5NTc1NTA0MTM5MjExODE5ODk0MDg5ODA1Nzg0"
    "NjIzNDc3NDc2MDg5NDE1MjI2Mjk4ODU2ODk1MTIyMTQ2NDQ1ODU0Mjg1MCIsICJiZXN0X2JpZCI6IjAuMDEz"
    "IiwgImJlc3RfYXNrIjoiMC4wMTUiLCAic3ByZWFkIjoiMC4wMDIiLCAidGltZXN0YW1wIjoiMTc4NzQzMzY5"
    "MDE0NiIsICJldmVudF90eXBlIjoiYmVzdF9iaWRfYXNrIn0="
)
_SEALED_CALIBRATION_SEQ_481 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAiYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4NzMxMjUxOTA3NjYxNTc0MTg2"
    "MDY4NDYzNDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODkiLCAiYmVzdF9iaWQiOiIwLjk4"
    "NSIsICJiZXN0X2FzayI6IjAuOTg3IiwgInNwcmVhZCI6IjAuMDAyIiwgInRpbWVzdGFtcCI6IjE3ODc0MzM2"
    "OTAxNDYiLCAiZXZlbnRfdHlwZSI6ImJlc3RfYmlkX2FzayJ9"
)
_SEALED_CALIBRATION_SEQ_482 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAicHJpY2VfY2hhbmdlcyI6W3siYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4"
    "NzMxMjUxOTA3NjYxNTc0MTg2MDY4NDYzNDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODki"
    "LCAicHJpY2UiOiIwLjk4NyIsICJzaXplIjoiNTkzLjM5IiwgInNpZGUiOiJTRUxMIiwgImhhc2giOiJjNDQ5"
    "Y2U0YzQ2ODAxMGRiM2EwZjA2MzE4NmE2OTQ2YmIyZGNmY2JiIiwgImJlc3RfYmlkIjoiMC45ODUiLCAiYmVz"
    "dF9hc2siOiIwLjk4NyJ9LCB7ImFzc2V0X2lkIjoiMTgyNTgzOTU3NTUwNDEzOTIxMTgxOTg5NDA4OTgwNTc4"
    "NDYyMzQ3NzQ3NjA4OTQxNTIyNjI5ODg1Njg5NTEyMjE0NjQ0NTg1NDI4NTAiLCAicHJpY2UiOiIwLjAxMyIs"
    "ICJzaXplIjoiNTkzLjM5IiwgInNpZGUiOiJCVVkiLCAiaGFzaCI6IjJhYjZiYzM1NjFjY2IwNGY4MmU0M2Vi"
    "ZmY4ZDZhZTk1NDM2MWNmZmMiLCAiYmVzdF9iaWQiOiIwLjAxMyIsICJiZXN0X2FzayI6IjAuMDE1In1dLCAi"
    "dGltZXN0YW1wIjoiMTc4NzQzMzY5MDE0NiIsICJldmVudF90eXBlIjoicHJpY2VfY2hhbmdlIn0="
)
_SEALED_CALIBRATION_SEQ_483 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAicHJpY2VfY2hhbmdlcyI6W3siYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4"
    "NzMxMjUxOTA3NjYxNTc0MTg2MDY4NDYzNDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODki"
    "LCAicHJpY2UiOiIwLjk4NiIsICJzaXplIjoiMCIsICJzaWRlIjoiU0VMTCIsICJoYXNoIjoiYzQ0OWNlNGM0"
    "NjgwMTBkYjNhMGYwNjMxODZhNjk0NmJiMmRjZmNiYiIsICJiZXN0X2JpZCI6IjAuOTg1IiwgImJlc3RfYXNr"
    "IjoiMC45ODcifSwgeyJhc3NldF9pZCI6IjE4MjU4Mzk1NzU1MDQxMzkyMTE4MTk4OTQwODk4MDU3ODQ2MjM0"
    "Nzc0NzYwODk0MTUyMjYyOTg4NTY4OTUxMjIxNDY0NDU4NTQyODUwIiwgInByaWNlIjoiMC4wMTQiLCAic2l6"
    "ZSI6IjAiLCAic2lkZSI6IkJVWSIsICJoYXNoIjoiMmFiNmJjMzU2MWNjYjA0ZjgyZTQzZWJmZjhkNmFlOTU0"
    "MzYxY2ZmYyIsICJiZXN0X2JpZCI6IjAuMDEzIiwgImJlc3RfYXNrIjoiMC4wMTUifV0sICJ0aW1lc3RhbXAi"
    "OiIxNzg3NDMzNjkwMTQ2IiwgImV2ZW50X3R5cGUiOiJwcmljZV9jaGFuZ2UifQ=="
)
_SEALED_CALIBRATION_SEQ_484 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAicHJpY2VfY2hhbmdlcyI6W3siYXNzZXRfaWQiOiIxODI1ODM5NTc1NTA0MTM5"
    "MjExODE5ODk0MDg5ODA1Nzg0NjIzNDc3NDc2MDg5NDE1MjI2Mjk4ODU2ODk1MTIyMTQ2NDQ1ODU0Mjg1MCIs"
    "ICJwcmljZSI6IjAuMDE0IiwgInNpemUiOiIwIiwgInNpZGUiOiJCVVkiLCAiaGFzaCI6IjJhYjZiYzM1NjFj"
    "Y2IwNGY4MmU0M2ViZmY4ZDZhZTk1NDM2MWNmZmMiLCAiYmVzdF9iaWQiOiIwLjAxMyIsICJiZXN0X2FzayI6"
    "IjAuMDE1In0sIHsiYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4NzMxMjUxOTA3NjYxNTc0MTg2MDY4NDYz"
    "NDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODkiLCAicHJpY2UiOiIwLjk4NiIsICJzaXpl"
    "IjoiMCIsICJzaWRlIjoiU0VMTCIsICJoYXNoIjoiYzQ0OWNlNGM0NjgwMTBkYjNhMGYwNjMxODZhNjk0NmJi"
    "MmRjZmNiYiIsICJiZXN0X2JpZCI6IjAuOTg1IiwgImJlc3RfYXNrIjoiMC45ODcifV0sICJ0aW1lc3RhbXAi"
    "OiIxNzg3NDMzNjkwMTQ2IiwgImV2ZW50X3R5cGUiOiJwcmljZV9jaGFuZ2UifQ=="
)
_SEALED_CALIBRATION_SEQ_618 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAiYXNzZXRfaWQiOiIxODI1ODM5NTc1NTA0MTM5MjExODE5ODk0MDg5ODA1Nzg0"
    "NjIzNDc3NDc2MDg5NDE1MjI2Mjk4ODU2ODk1MTIyMTQ2NDQ1ODU0Mjg1MCIsICJiZXN0X2JpZCI6IjAuMDE0"
    "IiwgImJlc3RfYXNrIjoiMC4wMTgiLCAic3ByZWFkIjoiMC4wMDQiLCAidGltZXN0YW1wIjoiMTc4NzQzMzY5"
    "ODczNCIsICJldmVudF90eXBlIjoiYmVzdF9iaWRfYXNrIn0="
)
_SEALED_CALIBRATION_SEQ_619 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAiYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4NzMxMjUxOTA3NjYxNTc0MTg2"
    "MDY4NDYzNDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODkiLCAiYmVzdF9iaWQiOiIwLjk4"
    "MiIsICJiZXN0X2FzayI6IjAuOTg2IiwgInNwcmVhZCI6IjAuMDA0IiwgInRpbWVzdGFtcCI6IjE3ODc0MzM2"
    "OTg3MzQiLCAiZXZlbnRfdHlwZSI6ImJlc3RfYmlkX2FzayJ9"
)
_SEALED_CALIBRATION_SEQ_620 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAicHJpY2VfY2hhbmdlcyI6W3siYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4"
    "NzMxMjUxOTA3NjYxNTc0MTg2MDY4NDYzNDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODki"
    "LCAicHJpY2UiOiIwLjk4MiIsICJzaXplIjoiOTYxLjAzIiwgInNpZGUiOiJCVVkiLCAiaGFzaCI6ImY4Y2Ix"
    "ZWJiZTFjNDdkNDI4YjNmMjQ4YWNkZTY0ZmQyYzA3MWJjMmYiLCAiYmVzdF9iaWQiOiIwLjk4MiIsICJiZXN0"
    "X2FzayI6IjAuOTg2In0sIHsiYXNzZXRfaWQiOiIxODI1ODM5NTc1NTA0MTM5MjExODE5ODk0MDg5ODA1Nzg0"
    "NjIzNDc3NDc2MDg5NDE1MjI2Mjk4ODU2ODk1MTIyMTQ2NDQ1ODU0Mjg1MCIsICJwcmljZSI6IjAuMDE4Iiwg"
    "InNpemUiOiI5NjEuMDMiLCAic2lkZSI6IlNFTEwiLCAiaGFzaCI6IjFhYzhkMmJjYjY2YTYyNWZiMTgxZWIz"
    "ZTVmMTQ4ZWZkMzk0NWMyNjAiLCAiYmVzdF9iaWQiOiIwLjAxNCIsICJiZXN0X2FzayI6IjAuMDE4In1dLCAi"
    "dGltZXN0YW1wIjoiMTc4NzQzMzY5ODczNCIsICJldmVudF90eXBlIjoicHJpY2VfY2hhbmdlIn0="
)
_SEALED_CALIBRATION_SEQ_621 = base64.b64decode(
    "eyJtYXJrZXQiOiIweDM5ZWExNTAzNDI3ZGY3ZjE0MzllN2RkNjU2N2FhZDVlZDcyMjg0ZWQ0OWM1OGQwOWU1"
    "YjkxN2EyNDM4OWZhYjciLCAicHJpY2VfY2hhbmdlcyI6W3siYXNzZXRfaWQiOiIxMTQ3MjczMTY2MjgxNzI4"
    "NzMxMjUxOTA3NjYxNTc0MTg2MDY4NDYzNDcyMTIwMjM3NDIzNjMzOTMyNTQ1MTg5ODc2MDQ2NDA2NDg1ODki"
    "LCAicHJpY2UiOiIwLjk4NSIsICJzaXplIjoiMCIsICJzaWRlIjoiQlVZIiwgImhhc2giOiJmOGNiMWViYmUx"
    "YzQ3ZDQyOGIzZjI0OGFjZGU2NGZkMmMwNzFiYzJmIiwgImJlc3RfYmlkIjoiMC45ODIiLCAiYmVzdF9hc2si"
    "OiIwLjk4NiJ9LCB7ImFzc2V0X2lkIjoiMTgyNTgzOTU3NTUwNDEzOTIxMTgxOTg5NDA4OTgwNTc4NDYyMzQ3"
    "NzQ3NjA4OTQxNTIyNjI5ODg1Njg5NTEyMjE0NjQ0NTg1NDI4NTAiLCAicHJpY2UiOiIwLjAxNSIsICJzaXpl"
    "IjoiMCIsICJzaWRlIjoiU0VMTCIsICJoYXNoIjoiMWFjOGQyYmNiNjZhNjI1ZmIxODFlYjNlNWYxNDhlZmQz"
    "OTQ1YzI2MCIsICJiZXN0X2JpZCI6IjAuMDE0IiwgImJlc3RfYXNrIjoiMC4wMTgifV0sICJ0aW1lc3RhbXAi"
    "OiIxNzg3NDMzNjk4NzM0IiwgImV2ZW50X3R5cGUiOiJwcmljZV9jaGFuZ2UifQ=="
)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _no_book() -> bytes:
    return json.dumps(
        {
            "market": "0xcondition",
            "asset_id": "token-no",
            "timestamp": "1787198400124",
            "hash": "opaque-rest-hash-no",
            "bids": [{"price": "0.30", "size": "20"}],
            "asks": [{"price": "0.35", "size": "20"}],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
            "last_trade_price": "0.34",
        },
        separators=(",", ":"),
    ).encode()


def _calibration_gamma() -> bytes:
    return (
        _fixture("polymarket_gamma_market_keyset.json")
        .replace(b"0xcondition", _CALIBRATION_CONDITION_ID.encode())
        .replace(b"token-yes", _CALIBRATION_YES_ASSET_ID.encode())
        .replace(b"token-no", _CALIBRATION_NO_ASSET_ID.encode())
        .replace(b"0.0100", b"0.0010")
    )


def _calibration_clob() -> bytes:
    return (
        _fixture("polymarket_clob_market_info.json")
        .replace(b"token-yes", _CALIBRATION_YES_ASSET_ID.encode())
        .replace(b"token-no", _CALIBRATION_NO_ASSET_ID.encode())
        .replace(b'"mts": 0.01', b'"mts": 0.001')
    )


def _calibration_book(*, yes: bool) -> bytes:
    if yes:
        asset_id = _CALIBRATION_YES_ASSET_ID
        bids = [("0.013", "10"), ("0.014", "4")]
        asks = [("0.015", "3"), ("0.016", "8")]
        last_trade_price = "0.015"
    else:
        asset_id = _CALIBRATION_NO_ASSET_ID
        bids = [("0.984", "8"), ("0.985", "3")]
        asks = [("0.986", "4"), ("0.987", "10")]
        last_trade_price = "0.985"
    return json.dumps(
        {
            "market": _CALIBRATION_CONDITION_ID,
            "asset_id": asset_id,
            "timestamp": "1787430801000",
            "hash": f"calibration-{'yes' if yes else 'no'}-rest",
            "bids": [{"price": price, "size": size} for price, size in bids],
            "asks": [{"price": price, "size": size} for price, size in asks],
            "min_order_size": "5",
            "tick_size": "0.001",
            "neg_risk": False,
            "last_trade_price": last_trade_price,
        },
        separators=(",", ":"),
    ).encode()


def _calibration_second_multiframe_book(*, yes: bool) -> bytes:
    if yes:
        asset_id = _CALIBRATION_YES_ASSET_ID
        bids = [("0.013", "10"), ("0.014", "4")]
        asks = [("0.015", "3"), ("0.019", "8")]
        last_trade_price = "0.015"
    else:
        asset_id = _CALIBRATION_NO_ASSET_ID
        bids = [("0.981", "8"), ("0.985", "3")]
        asks = [("0.986", "4"), ("0.987", "10")]
        last_trade_price = "0.985"
    return json.dumps(
        {
            "market": _CALIBRATION_CONDITION_ID,
            "asset_id": asset_id,
            "timestamp": "1787433698000",
            "hash": f"calibration-second-multiframe-{'yes' if yes else 'no'}-rest",
            "bids": [{"price": price, "size": size} for price, size in bids],
            "asks": [{"price": price, "size": size} for price, size in asks],
            "min_order_size": "5",
            "tick_size": "0.001",
            "neg_risk": False,
            "last_trade_price": last_trade_price,
        },
        separators=(",", ":"),
    ).encode()


def _calibration_bbo_inputs(run_id: CollectorRunId) -> tuple[RawEventEnvelope, ...]:
    return tuple(
        _envelope(run_id, sequence, channel, payload)
        for sequence, (channel, payload) in enumerate(
            (
                (GAMMA_MARKETS_KEYSET, _calibration_gamma()),
                (f"{CLOB_MARKET_PREFIX}{_CALIBRATION_CONDITION_ID}", _calibration_clob()),
                (REST_BOOK, _calibration_book(yes=True)),
                (REST_BOOK, _calibration_book(yes=False)),
                (WS_MARKET, _CALIBRATION_BBO_YES),
                (WS_MARKET, _CALIBRATION_BBO_NO),
                (WS_MARKET, _CALIBRATION_PRICE_CHANGE),
            )
        )
    )


def _sealed_calibration_multiframe_inputs(
    run_id: CollectorRunId,
    *,
    later_transition: bool,
) -> tuple[RawEventEnvelope, ...]:
    if later_transition:
        yes_book = _calibration_second_multiframe_book(yes=True)
        no_book = _calibration_second_multiframe_book(yes=False)
        websocket_payloads = (
            _SEALED_CALIBRATION_SEQ_618,
            _SEALED_CALIBRATION_SEQ_619,
            _SEALED_CALIBRATION_SEQ_620,
            _SEALED_CALIBRATION_SEQ_621,
        )
    else:
        yes_book = _calibration_book(yes=True)
        no_book = _calibration_book(yes=False)
        websocket_payloads = (
            _SEALED_CALIBRATION_SEQ_480,
            _SEALED_CALIBRATION_SEQ_481,
            _SEALED_CALIBRATION_SEQ_482,
            _SEALED_CALIBRATION_SEQ_483,
            _SEALED_CALIBRATION_SEQ_484,
        )
    channel_payloads = (
        (GAMMA_MARKETS_KEYSET, _calibration_gamma()),
        (f"{CLOB_MARKET_PREFIX}{_CALIBRATION_CONDITION_ID}", _calibration_clob()),
        (REST_BOOK, yes_book),
        (REST_BOOK, no_book),
        *((WS_MARKET, payload) for payload in websocket_payloads),
    )
    return tuple(
        _envelope(run_id, sequence, channel, payload)
        for sequence, (channel, payload) in enumerate(channel_payloads)
    )


def _envelope(
    run_id: CollectorRunId,
    sequence: int,
    channel: str,
    payload: bytes,
) -> RawEventEnvelope:
    return RawEventEnvelope.capture(
        collector_run_id=run_id,
        ingest_seq=sequence,
        venue_id=POLYMARKET_VENUE_ID,
        connection_id=ConnectionId("connection-fixture"),
        subscription_id=SubscriptionId("subscription-fixture"),
        channel=channel,
        transport_type="WEBSOCKET" if channel == WS_MARKET else "HTTP",
        subscription_semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        received_wall_time=UtcTimestamp(1_000_000_000 + sequence),
        received_monotonic=MonotonicTimestamp(run_id.value, 100 + sequence),
        payload_bytes=payload,
        payload_encoding="utf-8",
    )


def _inputs(run_id: CollectorRunId) -> tuple[RawEventEnvelope, ...]:
    return tuple(
        _envelope(run_id, sequence, channel, payload)
        for sequence, (channel, payload) in enumerate(
            (
                (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
                (
                    f"{CLOB_MARKET_PREFIX}0xcondition",
                    _fixture("polymarket_clob_market_info.json"),
                ),
                (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
                (REST_BOOK, _no_book()),
                (WS_MARKET, _fixture("polymarket_ws_price_change.json")),
            )
        )
    )


async def _write_completed_raw_fixture(
    root: Path,
    run_id: CollectorRunId,
    *,
    envelopes: tuple[RawEventEnvelope, ...] | None = None,
) -> None:
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=root,
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-{run_id.value}"),
            writer_software_version="test",
            git_commit="raw-fixture",
            max_events_per_segment=2,
        )
    )
    await writer.start()
    for envelope in _inputs(run_id) if envelopes is None else envelopes:
        await writer.submit(envelope)
    await writer.close()


def _hashed_inventory(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_processor_refuses_to_parse_without_matching_durable_receipt() -> None:
    run_id = CollectorRunId("run-durability-fixture")
    envelope = _inputs(run_id)[0]
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    receipt = JournalReceipt(
        ingest_id=envelope.ingest_id,
        collector_run_id=run_id,
        ingest_seq=envelope.ingest_seq,
        state=JournalDurabilityState.RECEIVED,
    )

    with pytest.raises(ValueError, match="before matching durable receipt"):
        processor.process_durable(envelope, receipt)


@pytest.mark.asyncio
async def test_live_durable_path_and_replay_have_identical_terminal_hash(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-replay-fixture")
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-fixture"),
            writer_software_version="test",
            git_commit="fixture",
            max_events_per_segment=2,
        )
    )
    await writer.start()
    for envelope in _inputs(run_id):
        receipt = await writer.submit(envelope)
        processor.process_durable(envelope, receipt)
    await writer.close()

    live = processor.summary()
    first = replay_journal(tmp_path / "raw", run_id, config)
    second = replay_journal(tmp_path / "raw", run_id, config)

    assert live.terminal_hash == first.processor.terminal_hash
    assert first.processor == second.processor
    assert first.raw_record_count == 5
    assert first.processor.healthy_books == 2
    assert first.processor.parse_failures == 0

    completion = completion_path_for_run(tmp_path / "raw", run_id, "polymarket-public")
    await asyncio.to_thread(completion.unlink)
    assert replay_run_ids(tmp_path / "raw") == ()
    assert replay_run_ids(tmp_path / "raw", include_partial=True) == (run_id,)
    with pytest.raises(JournalCorruptionError, match="completion head is missing"):
        replay_journal(tmp_path / "raw", run_id, config)


@pytest.mark.asyncio
async def test_failed_calibration_best_bid_ask_frames_replay_without_protocol_failure(
    tmp_path: Path,
) -> None:
    assert hashlib.sha256(_CALIBRATION_BBO_YES).hexdigest() == (
        "cbd97c429b9f510c2373f8dabff9e097e2c771d9e5746546ebd0ddce70d015a3"
    )
    assert hashlib.sha256(_CALIBRATION_BBO_NO).hexdigest() == (
        "1545fcfcfbfaeb9381faa528ba2c5dba15f35943ee9e4dd6ba59e75dc63ba6f3"
    )
    assert hashlib.sha256(_CALIBRATION_PRICE_CHANGE).hexdigest() == (
        "d7978e95f12f06f749953a38a6959b1ee32135854a579fbc61b3ec405eff32d6"
    )
    run_id = CollectorRunId("failed-calibration-best-bid-ask-replay")
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-failed-calibration-bbo"),
            writer_software_version="test",
            git_commit="failed-calibration-bbo-fixture",
            max_events_per_segment=3,
        )
    )
    await writer.start()
    live_results: list[ProcessedEnvelope] = []
    for envelope in _calibration_bbo_inputs(run_id):
        receipt = await writer.submit(envelope)
        live_results.append(processor.process_durable(envelope, receipt))
    await writer.close()

    live = processor.summary()
    first = replay_journal(tmp_path / "raw", run_id, config)
    second = replay_journal(tmp_path / "raw", run_id, config)

    assert all(result.success for result in live_results)
    assert all(not result.book_freshness_updates for result in live_results[4:6])
    assert all(len(result.relevant_l2_receive_updates) == 1 for result in live_results[4:6])
    assert len(live_results[6].relevant_l2_receive_updates) == 2
    assert live.parse_failures == 0
    assert live.canonical_events == 4
    assert live.invalidations == 0
    assert live.healthy_books == 2
    assert live.audit_ineligible_books == 0
    assert live.terminal_hash == first.processor.terminal_hash
    assert first == second
    assert first.raw_record_count == 7


@pytest.mark.parametrize(
    ("later_transition", "expected_pending_counts", "expected_tops"),
    [
        (False, (1, 2, 2, 0, 0), (("0.013", "0.015"), ("0.985", "0.987"))),
        (True, (1, 2, 2, 0), (("0.014", "0.018"), ("0.982", "0.986"))),
    ],
)
@pytest.mark.asyncio
async def test_sealed_calibration_multiframe_bbo_transition_replays_deterministically(
    tmp_path: Path,
    *,
    later_transition: bool,
    expected_pending_counts: tuple[int, ...],
    expected_tops: tuple[tuple[str, str], tuple[str, str]],
) -> None:
    source_hashes = {
        480: "154657379e0197e2f793cf10a55db8117a00cf8eef1c3e9d2585218c1d5812ef",
        481: "df5d8506ca80652898f8849496eced222af02b8e9989c7f096ada080f8d5ff4b",
        482: "5977ded27c66ca542e0e9b45ab5827c61564abb152685a2df11501b5ce14d0d7",
        483: "aa5c4577df6431c85c15811efae18bcbd7ec46555ae23d034705fdb3585bb8c1",
        484: "ba49ffa4feae8a97bdb0008a20d41196e1830d6b98dc4b1cf2e7787eb505f57c",
        618: "cf7db880b08e3c6dc541caadce4c1bba48a37cce4c1b93d4497e0bc2b957ce90",
        619: "3510a77611db584b6860d65cad6a2f07d7fe0f944cec97723b2939fe53d4fc6d",
        620: "77fedc3bb596641d6847a62282c24864d0ebfdc412c47e114d0e3e6872f4aa79",
        621: "5e2d3fcdc73b0c8b2f917b4b9778ca5dfa2b2d2346c3aa41ac5972cc5ca4419f",
    }
    exact_frames = {
        480: _SEALED_CALIBRATION_SEQ_480,
        481: _SEALED_CALIBRATION_SEQ_481,
        482: _SEALED_CALIBRATION_SEQ_482,
        483: _SEALED_CALIBRATION_SEQ_483,
        484: _SEALED_CALIBRATION_SEQ_484,
        618: _SEALED_CALIBRATION_SEQ_618,
        619: _SEALED_CALIBRATION_SEQ_619,
        620: _SEALED_CALIBRATION_SEQ_620,
        621: _SEALED_CALIBRATION_SEQ_621,
    }
    assert {
        sequence: hashlib.sha256(payload).hexdigest() for sequence, payload in exact_frames.items()
    } == source_hashes

    suffix = "620-621" if later_transition else "480-484"
    run_id = CollectorRunId(f"sealed-calibration-multiframe-{suffix}")
    config = load_phase1_config(Path("config/phase1.toml"))
    processor = CanonicalProcessor(config)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId(f"writer-sealed-multiframe-{suffix}"),
            writer_software_version="test",
            git_commit="sealed-calibration-multiframe-fixture",
            max_events_per_segment=3,
        )
    )
    await writer.start()
    live_results: list[ProcessedEnvelope] = []
    pending_counts: list[int] = []
    envelopes = _sealed_calibration_multiframe_inputs(
        run_id,
        later_transition=later_transition,
    )
    for index, envelope in enumerate(envelopes):
        receipt = await writer.submit(envelope)
        live_results.append(processor.process_durable(envelope, receipt))
        if index >= 4:
            pending_counts.append(len(processor.audit_ineligible_instruments))
    await writer.close()

    assert all(result.success for result in live_results)
    assert pending_counts == list(expected_pending_counts)
    assert not live_results[4].book_freshness_updates
    assert not live_results[5].book_freshness_updates
    assert len(live_results[6].book_freshness_updates) == 2
    assert len(live_results[6].relevant_l2_receive_updates) == 2
    assert len(live_results[7].book_freshness_updates) == 2
    live = processor.summary()
    assert live.parse_failures == 0
    assert live.invalidations == 0
    assert live.healthy_books == 2
    assert live.audit_ineligible_books == 0

    bundle = processor.current_bundles[0]
    for native_asset_id, expected_top in zip(
        (_CALIBRATION_YES_ASSET_ID, _CALIBRATION_NO_ASSET_ID),
        expected_tops,
        strict=True,
    ):
        instrument_id = bundle.instrument_for_token(native_asset_id).instrument_id
        owner = next(item for item in processor.owners if item.instrument_id == instrument_id)
        snapshot = owner.snapshot()
        assert snapshot.best_bid is not None
        assert snapshot.best_ask is not None
        assert (str(snapshot.best_bid.price), str(snapshot.best_ask.price)) == expected_top
        assert snapshot.last_source_provenance is not None
        expected_last_sequence = 621 if later_transition else 484
        assert snapshot.last_source_provenance.source_payload_hashes == (
            source_hashes[expected_last_sequence],
        )

    first = replay_journal(tmp_path / "raw", run_id, config)
    second = replay_journal(tmp_path / "raw", run_id, config)
    assert live.terminal_hash == first.processor.terminal_hash
    assert live.canonical_event_chain_sha256 == first.processor.canonical_event_chain_sha256
    assert first == second
    assert first.raw_record_count == len(envelopes)


def test_sealed_multiframe_intermediate_without_pending_bbo_remains_fail_closed() -> None:
    run_id = CollectorRunId("sealed-multiframe-without-pending-bbo")
    processor = CanonicalProcessor(load_phase1_config(Path("config/phase1.toml")))
    bootstrap = _sealed_calibration_multiframe_inputs(run_id, later_transition=False)[:4]
    for envelope in bootstrap:
        assert processor.process_replayed(envelope).success

    rejected = processor.process_replayed(
        _envelope(run_id, 4, WS_MARKET, _SEALED_CALIBRATION_SEQ_482)
    )

    # Expected venue-side uncertainty is represented as a typed, fail-closed
    # resync state.  It is not an envelope parse/programming failure.
    assert rejected.success
    assert rejected.needs_resync
    assert rejected.error_type is None
    assert processor.summary().parse_failures == 0
    assert processor.summary().invalidations == 2
    assert processor.summary().healthy_books == 0
    assert len(rejected.affected_instrument_ids) == 2
    assert processor.audit_ineligible_instruments == ()


@pytest.mark.asyncio
async def test_completed_raw_run_rebuilds_deterministic_analytical_outputs_atomically(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-derived-rebuild-fixture")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    config = load_phase1_config(Path("config/phase1.toml"))
    raw_head = load_run_completion_head(
        completion_path_for_run(raw_root, run_id, "polymarket-public")
    )
    expected_replay = replay_journal(raw_root, run_id, config)

    first = rebuild_analytical_from_journal(
        raw_root,
        run_id,
        config,
        output_root=tmp_path / "rebuilt-first",
        code_commit="rebuild-fixture",
        expect_terminal_hash=expected_replay.processor.terminal_hash,
    )
    second = rebuild_analytical_from_journal(
        raw_root,
        run_id,
        config,
        output_root=tmp_path / "rebuilt-second",
        code_commit="rebuild-fixture",
    )

    assert first.replay == expected_replay
    assert second.replay == expected_replay
    assert first.partition_date == second.partition_date == "1970-01-01"
    assert first.source_journal_completion_sha256 == raw_head.completion_sha256
    assert first.analytical_outputs.health == "HEALTHY"
    assert first.analytical_outputs.research_eligible
    first_head = load_analytical_run_completion(
        analytical_run_completion_path(tmp_path / "rebuilt-first", run_id)
    )
    assert first_head.source_journal_completion_sha256 == raw_head.completion_sha256
    assert {dataset.dataset: dataset.record_count for dataset in first_head.datasets} == {
        "canonical_events": expected_replay.processor.canonical_events,
        "complete_set_scans": expected_replay.processor.scans,
        "reference": expected_replay.processor.reference_updates,
    }
    assert validate_data(tmp_path / "rebuilt-first").valid
    assert _hashed_inventory(tmp_path / "rebuilt-first") == _hashed_inventory(
        tmp_path / "rebuilt-second"
    )
    staging_paths = await asyncio.to_thread(lambda: tuple(tmp_path.glob(".rebuilt-*.rebuild-*")))
    assert not staging_paths


@pytest.mark.asyncio
async def test_analytical_rebuild_failure_publishes_no_destination_or_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multi_market_arbitrage.storage import analytical as analytical_module

    run_id = CollectorRunId("run-derived-rebuild-failure")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    raw_inventory = _hashed_inventory(raw_root)
    config = load_phase1_config(Path("config/phase1.toml"))
    expected = replay_journal(raw_root, run_id, config).processor.terminal_hash

    with pytest.raises(ReplayTerminalHashMismatch):
        rebuild_analytical_from_journal(
            raw_root,
            run_id,
            config,
            output_root=tmp_path / "hash-mismatch",
            code_commit="rebuild-fixture",
            expect_terminal_hash="0" * 64,
        )
    assert not (tmp_path / "hash-mismatch").exists()

    def fail_parquet(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("injected rebuild Parquet failure")

    monkeypatch.setattr(analytical_module, "pyarrow_write_table", fail_parquet)
    with pytest.raises(OSError, match="injected rebuild Parquet failure"):
        rebuild_analytical_from_journal(
            raw_root,
            run_id,
            config,
            output_root=tmp_path / "parquet-failure",
            code_commit="rebuild-fixture",
            expect_terminal_hash=expected,
        )

    assert not (tmp_path / "parquet-failure").exists()
    staging_paths = await asyncio.to_thread(lambda: tuple(tmp_path.glob(".*.rebuild-*")))
    assert not staging_paths
    assert _hashed_inventory(raw_root) == raw_inventory


@pytest.mark.asyncio
async def test_analytical_rebuild_requires_fresh_disjoint_non_symlink_destination(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-derived-rebuild-path-safety")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    config = load_phase1_config(Path("config/phase1.toml"))
    existing = tmp_path / "existing"
    existing.mkdir()
    symlink = tmp_path / "symlink"
    symlink.symlink_to(existing, target_is_directory=True)

    for destination, message in (
        (existing, "must not already exist"),
        (symlink, "contains a symlink"),
        (raw_root / "derived", "must be disjoint"),
    ):
        with pytest.raises(ValueError, match=message):
            rebuild_analytical_from_journal(
                raw_root,
                run_id,
                config,
                output_root=destination,
                code_commit="rebuild-fixture",
            )

    assert tuple(existing.iterdir()) == ()
    assert not (raw_root / "derived").exists()


@pytest.mark.asyncio
async def test_empty_completed_raw_run_uses_recorded_completion_date_for_rebuild(
    tmp_path: Path,
) -> None:
    run_id = CollectorRunId("run-empty-derived-rebuild")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id, envelopes=())
    head = load_run_completion_head(completion_path_for_run(raw_root, run_id, "polymarket-public"))

    rebuilt = rebuild_analytical_from_journal(
        raw_root,
        run_id,
        load_phase1_config(Path("config/phase1.toml")),
        output_root=tmp_path / "empty-rebuilt",
        code_commit="rebuild-fixture",
    )

    assert rebuilt.partition_date == head.completed_at.to_datetime().date().isoformat()
    assert rebuilt.replay.raw_record_count == 0
    assert rebuilt.analytical_outputs.accepted_records == 0
    assert validate_data(tmp_path / "empty-rebuilt").valid


@pytest.mark.asyncio
async def test_rebuild_analytical_cli_publishes_json_and_fails_closed_on_hash_mismatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_id = CollectorRunId("run-derived-rebuild-cli")
    raw_root = tmp_path / "raw"
    await _write_completed_raw_fixture(raw_root, run_id)
    config_path = Path("config/phase1.toml")
    terminal_hash = replay_journal(
        raw_root,
        run_id,
        load_phase1_config(config_path),
    ).processor.terminal_hash

    exit_code = await asyncio.to_thread(
        main,
        (
            "rebuild-analytical",
            str(raw_root),
            "--output",
            str(tmp_path / "cli-rebuilt"),
            "--run-id",
            run_id.value,
            "--expect-terminal-hash",
            terminal_hash,
            "--config",
            str(config_path),
        ),
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert exit_code == EXIT_OK
    assert captured.err == ""
    assert result["replay"]["processor"]["terminal_hash"] == terminal_hash
    assert result["analytical_outputs"]["research_eligible"] is True

    mismatch_exit = await asyncio.to_thread(
        main,
        (
            "rebuild-analytical",
            str(raw_root),
            "--output",
            str(tmp_path / "cli-mismatch"),
            "--run-id",
            run_id.value,
            "--expect-terminal-hash",
            "0" * 64,
            "--config",
            str(config_path),
        ),
    )
    mismatch = capsys.readouterr()

    assert mismatch_exit == EXIT_REPLAY_MISMATCH
    assert "terminal hash mismatch" in mismatch.err
    assert not (tmp_path / "cli-mismatch").exists()


@pytest.mark.asyncio
async def test_raw_capture_continues_after_derived_parquet_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multi_market_arbitrage.storage import analytical as analytical_module

    def fail_parquet(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("injected derived Parquet failure")

    monkeypatch.setattr(analytical_module, "pyarrow_write_table", fail_parquet)
    run_id = CollectorRunId("run-derived-failure-isolation")
    config = load_phase1_config(Path("config/phase1.toml"))
    persistent = Phase1AnalyticalOutputs(
        root=tmp_path / "analytical",
        partition_date="2026-08-21",
        batch_size=1,
        collector_run_id=run_id,
        code_commit="fixture",
        code_version="test",
        config_hash=config.config_hash.value,
    )
    outputs = SupervisedAnalyticalOutputs(persistent, queue_capacity=8)
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-derived-failure"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    await writer.start()
    await outputs.start()

    for envelope in _inputs(run_id):
        receipt = await writer.submit(envelope)
        processor.process_durable(envelope, receipt)
        await asyncio.sleep(0)

    await writer.close()
    raw_head = load_run_completion_head(
        completion_path_for_run(tmp_path / "raw", run_id, "polymarket-public")
    )
    analytical = await outputs.close(
        source_journal_completion_sha256=raw_head.completion_sha256,
    )

    assert writer.metrics.durable == len(_inputs(run_id))
    assert replay_journal(tmp_path / "raw", run_id, config).raw_record_count == len(_inputs(run_id))
    assert analytical.health == "DEGRADED"
    assert analytical.research_eligible is False
    assert analytical.failure_type == "OSError"
    assert not tuple((tmp_path / "analytical").glob("_analytical-run.*.completed.json"))


def test_analytical_outputs_are_schema_stable_and_reportable(tmp_path: Path) -> None:
    run_id = CollectorRunId("run-analytical-fixture")
    config = load_phase1_config(Path("config/phase1.toml"))
    outputs = Phase1AnalyticalOutputs(
        root=tmp_path / "analytical",
        partition_date="2026-08-20",
        batch_size=1,
        collector_run_id=run_id,
        code_commit="fixture",
        code_version="test",
        config_hash=config.config_hash.value,
    )
    processor = CanonicalProcessor(config, analytical_outputs=outputs)
    inputs = _inputs(run_id)
    for envelope in inputs:
        processor.process_replayed(envelope)
    outputs.close()

    validation = validate_data(tmp_path / "analytical")
    report = build_scan_report(tmp_path / "analytical")

    assert validation.valid
    assert validation.parquet_fragments > 3
    assert report.total_scans == processor.summary().scans
    assert report.candidates == processor.summary().candidates
    assert report.unique_lifecycles == processor.summary().unique_opportunity_lifecycles
    assert report.unique_opportunity_observations <= report.candidates
    assert len(report.terminal_hashes) == report.total_scans

    bundle = processor.current_bundles[0]
    reference_provenance = processor.reference_provenance(bundle.market.market_id)
    assert reference_provenance is not None
    expected_reference_sources = {inputs[index].ingest_id.value for index in (0, 1, 3)}
    assert {
        item.value for item in reference_provenance.source_ingest_ids
    } == expected_reference_sources
    final_candidate = next(
        result.candidate
        for result in reversed(processor.scan_results)
        if result.candidate is not None
    )
    assert expected_reference_sources <= {
        item.value for item in final_candidate.provenance.source_ingest_ids
    }
    assert set(reference_provenance.source_payload_hashes) <= set(
        final_candidate.provenance.source_payload_hashes
    )

    with DuckDBReadOnlyQuery(tmp_path / "analytical") as query:
        query.register_parquet_view("reference", tmp_path / "analytical" / "reference")
        persisted = query.query(
            "SELECT source_ingest_ids_json, source_payload_hashes_json "
            "FROM reference ORDER BY observed_at_utc_ns DESC LIMIT 1"
        ).rows[0]
    assert set(json.loads(str(persisted[0]))) == expected_reference_sources
    assert set(json.loads(str(persisted[1]))) == set(reference_provenance.source_payload_hashes)

    scan_fragment = next((tmp_path / "analytical" / "complete_set_scans").rglob("*.parquet"))
    scan_fragment.with_name(f"{scan_fragment.name}.manifest.json").unlink()
    scan_fragment.unlink()
    with pytest.raises(ReadOnlyQueryError, match="refused invalid analytical inventory"):
        build_scan_report(tmp_path / "analytical")


class _BrokenPublicClient:
    def __init__(self, **kwargs: object) -> None:
        del kwargs

    async def __aenter__(self) -> _BrokenPublicClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def fetch_markets_keyset_raw(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
        closed: bool = False,
        deadline: float | None = None,
    ) -> bytes:
        del after_cursor, limit, closed, deadline
        raise RuntimeError("bootstrap fixture failure")

    def public_read_metrics_snapshot(self) -> PublicReadMetricsSnapshot:
        return PublicReadMetricsSnapshot((), 0, 0)


class _ExplodingSocket:
    def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
        del asset_ids, kwargs

    async def raw_messages(self) -> AsyncIterator[bytes]:
        raise RuntimeError("websocket fixture failure")
        yield b"unreachable"


@pytest.mark.asyncio
async def test_ingress_overflow_is_journaled_and_forces_rest_resnapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "system": config.system.model_copy(update={"queue_capacity": 1}),
        }
    )
    metrics = InMemoryMetrics()
    run_id = CollectorRunId("observation-ingress-overflow")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-ingress-overflow"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )

    class _AcceptingProcessor:
        def process_durable(
            self,
            envelope: RawEventEnvelope,
            receipt: JournalReceipt,
        ) -> ProcessedEnvelope:
            assert receipt.ingest_id == envelope.ingest_id
            return ProcessedEnvelope(
                ingest_seq=envelope.ingest_seq,
                success=True,
                canonical_event_count=0,
                scan_count=0,
                needs_resync=False,
                affected_instrument_ids=(),
                reference_refresh_requests=(),
                book_freshness_updates=(),
                scan_triggers=(),
                error_type=None,
            )

        def on_backpressure(self) -> None:
            raise AssertionError("raw journal did not saturate")

        @staticmethod
        def audit_book_identity(native_asset_id: str) -> object:
            return SimpleNamespace(instrument_id=InstrumentId(native_asset_id))

    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=cast(CanonicalProcessor, _AcceptingProcessor()),
        metrics=metrics,
    )
    capture._book_freshness_ns.update(  # pyright: ignore[reportPrivateUsage]
        {
            InstrumentId(asset_id): observe_module.time.monotonic_ns()
            for asset_id in ("token-yes", "token-no")
        }
    )

    class _OverflowThenBlockingSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            self.generation = type(self).instances
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(b"fixture").hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            if self.generation == 0:
                for index in range(2):
                    admit(
                        ReceivedWebSocketMessage(
                            payload_bytes=f"frame-{index}".encode(),
                            received_wall_time=UtcTimestamp.now(),
                            received_monotonic_ns=observe_module.time.monotonic_ns(),
                            message_type="BINARY",
                        )
                    )
                raise AssertionError("second frame should exhaust one-item ingress")
            await asyncio.Event().wait()

    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    resnapshots: list[tuple[str, ...]] = []

    async def record_resnapshot(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
        refresh_reference: bool = True,
    ) -> None:
        del service, client, capture, deadline, refresh_reference
        resnapshots.append(tuple(market.gamma_market_id for market in selected))

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _OverflowThenBlockingSocket,
    )
    monkeypatch.setattr(PolymarketObservationService, "_resnapshot", record_resnapshot)
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    observe_websocket = cast(
        Callable[..., Awaitable[observe_module._StreamResult]],  # pyright: ignore[reportPrivateUsage]
        service.__getattribute__("_observe_websocket"),
    )

    await writer.start()
    runtime_health = ObservationRuntimeHealth()
    stream = await observe_websocket(
        client=object(),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=asyncio.get_running_loop().time() + 0.2,
        logger=service.__getattribute__("_logger"),
        runtime_health=runtime_health,
    )
    await writer.close()

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert stream.completed
    assert stream.ingress_metrics.received == 2
    assert stream.ingress_metrics.admitted == 1
    assert stream.ingress_metrics.overflows == 1
    assert "bounded application ingress queue exhausted" in stream.deviations
    assert resnapshots == [(selected.gamma_market_id,)]
    assert counters["mma_ingress_overflows_total"] == 1
    assert counters["mma_resynchronizations_total"] == 1
    assert runtime_health.snapshot().data_loss_count == 1
    assert runtime_health.snapshot().resyncs == 1
    assert [record.channel for record in records] == [WS_MARKET, CONTROL_BOOK_INVALIDATION]
    assert json.loads(records[-1].payload_bytes) == {
        "reason": InvalidationReason.BACKPRESSURE_DATA_LOSS.value
    }


@pytest.mark.asyncio
async def test_continuous_audit_fences_response_without_replacing_socket_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    config = config.model_copy(
        update={
            "polymarket": config.polymarket.model_copy(
                update={
                    "snapshot_audit_freshness_guard_seconds": 0.01,
                    "snapshot_audit_interval_seconds": 0.05,
                    "snapshot_audit_race_retry_quiet_seconds": 0.005,
                }
            )
        }
    )
    run_id = CollectorRunId("continuous-audit-generation")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-continuous-audit"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]

    class _ContinuousSocket:
        instances = 0
        active: _ContinuousSocket | None = None

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            del asset_ids, kwargs
            type(self).instances += 1
            type(self).active = self
            self.admitted = 0
            self.emit_economic_update = False
            self.frame_admitted = asyncio.Event()
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"continuous-audit-fixture"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            for _ in range(100):
                await asyncio.sleep(0.003)
                payload = (
                    _fixture("polymarket_ws_price_change.json")
                    if self.emit_economic_update
                    else _fixture("polymarket_ws_last_trade.json")
                )
                self.emit_economic_update = False
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=observe_module.time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                self.admitted += 1
                self.frame_admitted.set()
            await asyncio.Event().wait()

    class _AuditClient:
        def __init__(self) -> None:
            self.frames_admitted_during_requests: list[int] = []
            self.calls = 0
            self.attempts_by_asset: dict[str, int] = {}

        async def fetch_order_book_raw_with_evidence(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
            on_response_body_complete: Callable[[UtcTimestamp, int], None] | None = None,
        ) -> PublicReadResult:
            del deadline
            socket = _ContinuousSocket.active
            assert socket is not None
            self.calls += 1
            asset_attempt = self.attempts_by_asset.get(asset_id, 0) + 1
            self.attempts_by_asset[asset_id] = asset_attempt
            if asset_id == "token-yes" and asset_attempt == 1:
                socket.emit_economic_update = True
            before = socket.admitted
            while socket.admitted < before + 2:
                socket.frame_admitted.clear()
                if socket.admitted < before + 2:
                    await socket.frame_admitted.wait()
            self.frames_admitted_during_requests.append(socket.admitted - before)
            if asset_id == "token-no" and asset_attempt == 1:
                raise PolymarketHttpTransportError(
                    endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                    category=PolymarketHttpFailureCategory.NETWORK,
                    retryable=False,
                    attempt=3,
                    detail="bounded audit fixture exhausted HTTP retries",
                )
            if asset_id == "token-yes":
                payload = _fixture("polymarket_rest_book_yes.json")
                if asset_attempt >= 2:
                    payload = payload.replace(
                        b'"price": "0.45", "size": "4"',
                        b'"price": "0.45", "size": "7"',
                    ).replace(
                        b',\n    {"price": "0.55", "size": "3"}',
                        b"",
                    )
            else:
                payload = _no_book()
            response_ns = observe_module.time.monotonic_ns()
            response_wall_time = UtcTimestamp.now()
            if on_response_body_complete is not None:
                on_response_body_complete(response_wall_time, response_ns)
            return PublicReadResult(
                payload_bytes=payload,
                evidence=PublicReadEvidence(
                    endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                    attempts=1,
                    started_monotonic_ns=response_ns - 1,
                    response_received_wall_time=response_wall_time,
                    response_received_monotonic_ns=response_ns,
                    completed_monotonic_ns=response_ns,
                    status_code=200,
                    cumulative_backoff_seconds=0.0,
                    cumulative_rate_limit_wait_seconds=0.0,
                    payload_sha256=hashlib.sha256(payload).hexdigest(),
                ),
            )

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _ContinuousSocket,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    await writer.start()
    for channel, payload in (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book()),
    ):
        result = await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        )
        assert result.success

    client = _AuditClient()
    runtime_health = ObservationRuntimeHealth()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.2
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, client),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=deadline,
        deadline_ns=observe_module.time.monotonic_ns() + 200_000_000,
        periodic_audit_due_ns=observe_module.time.monotonic_ns(),
        connection_id=ConnectionId("continuous-audit-socket"),
        subscription_id=SubscriptionId("continuous-audit-subscription"),
        unavailable_started_ns=None,
        runtime_health=runtime_health,
    )
    runtime_health.finish_generation(
        continuity_interrupted=False,
        now_ns=observe_module.time.monotonic_ns(),
    )
    await writer.close()

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    live = processor.summary()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    assert generation.reason.value == "DEADLINE"
    assert _ContinuousSocket.instances == 1
    assert len(client.frames_admitted_during_requests) >= 3
    assert all(count >= 2 for count in client.frames_admitted_during_requests)
    assert live.rest_audit_matches >= 1
    assert live.rest_audit_raced == 1
    assert live.rest_audit_failed >= 1
    assert tuple(outcome.outcome for outcome in live.audit_outcomes[:3]) == (
        AuditOutcome.FAILED,
        AuditOutcome.RACED,
        AuditOutcome.MATCH,
    )
    assert live.audit_outcomes[0].reason is AuditFailureReason.HTTP_REQUEST_FAILED
    assert live.terminal_hash == replayed.terminal_hash
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_rest_audit_retries_total"] == 1
    assert runtime_health.snapshot().socket_connected
    assert runtime_health.snapshot().unavailable_intervals == 0
    assert runtime_health.snapshot().reconnects == 0
    assert [record.channel for record in records].count(CONTROL_REST_AUDIT_START) >= 3
    assert [record.channel for record in records].count(REST_BOOK_AUDIT) >= 2


@pytest.mark.asyncio
async def test_unrelated_new_market_is_preserved_without_rotating_selected_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    run_id = CollectorRunId("unrelated-new-market-continuity")
    writer = RawJournalWriter(
        JournalWriterConfig(
            root=tmp_path / "raw",
            collector_run_id=run_id,
            venue_id=POLYMARKET_VENUE_ID,
            stream="polymarket-public",
            writer_id=WriterId("writer-unrelated-new-market"),
            writer_software_version="test",
            git_commit="fixture",
        )
    )
    processor = CanonicalProcessor(config)
    metrics = InMemoryMetrics()
    capture = observe_module._RawCaptureBoundary(  # pyright: ignore[reportPrivateUsage]
        collector_run_id=run_id,
        writer=writer,
        processor=processor,
        metrics=metrics,
    )
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    unrelated_payload = json.dumps(
        {
            "assets_ids": ["unrelated-yes", "unrelated-no"],
            "event_type": "new_market",
            "id": "unrelated-gamma-id",
            "market": "unrelated-condition",
            "timestamp": "1787198406000",
        },
        separators=(",", ":"),
    ).encode()

    class _UnrelatedThenSelectedSocket:
        instances = 0

        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            type(self).instances += 1
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"unrelated-new-market-continuity"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            for payload in (
                unrelated_payload,
                _fixture("polymarket_ws_price_change.json"),
            ):
                admit(
                    ReceivedWebSocketMessage(
                        payload_bytes=payload,
                        received_wall_time=UtcTimestamp.now(),
                        received_monotonic_ns=observe_module.time.monotonic_ns(),
                        message_type="TEXT",
                    )
                )
                await asyncio.sleep(0)
            await asyncio.Event().wait()

    class _NoAuditClient:
        async def fetch_order_book_raw_with_evidence(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            raise AssertionError("short continuity fixture must not launch an audit")

    monkeypatch.setattr(
        observe_module,
        "PolymarketMarketWebSocket",
        _UnrelatedThenSelectedSocket,
    )
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    await writer.start()
    for channel, payload in (
        (GAMMA_MARKETS_KEYSET, _fixture("polymarket_gamma_market_keyset.json")),
        (
            f"{CLOB_MARKET_PREFIX}0xcondition",
            _fixture("polymarket_clob_market_info.json"),
        ),
        (REST_BOOK, _fixture("polymarket_rest_book_yes.json")),
        (REST_BOOK, _no_book()),
    ):
        result = await capture.preserve_then_process(
            channel=channel,
            payload=payload,
            transport_type="HTTP",
            semantics_hash=hashlib.sha256(channel.encode()).hexdigest(),
        )
        assert result.success

    runtime_health = ObservationRuntimeHealth()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.1
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, _NoAuditClient()),
        capture=capture,
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=deadline,
        deadline_ns=observe_module.time.monotonic_ns() + 100_000_000,
        periodic_audit_due_ns=observe_module.time.monotonic_ns() + 10_000_000_000,
        connection_id=ConnectionId("unrelated-new-market-socket"),
        subscription_id=SubscriptionId("unrelated-new-market-subscription"),
        unavailable_started_ns=None,
        runtime_health=runtime_health,
    )
    runtime_health.finish_generation(
        continuity_interrupted=False,
        now_ns=observe_module.time.monotonic_ns(),
    )
    await writer.close()

    records = JournalReader(tmp_path / "raw", run_id).read_all()
    live = processor.summary()
    replayed = replay_journal(tmp_path / "raw", run_id, config).processor
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    runtime = runtime_health.snapshot()
    assert generation.reason.value == "DEADLINE"
    assert generation.refresh_requests == ()
    assert _UnrelatedThenSelectedSocket.instances == 1
    assert [json.loads(record.payload_bytes)["event_type"] for record in records[-2:]] == [
        "new_market",
        "price_change",
    ]
    assert live.reference_refresh_requests == 1
    assert live.terminal_hash == replayed.terminal_hash
    assert counters["mma_reference_refresh_requests_total"] == 1
    assert counters["mma_reference_refresh_requests_ignored_total"] == 1
    assert runtime.socket_connected
    assert runtime.unavailable_intervals == 0
    assert runtime.reconnects == 0
    assert runtime.protocol_deviations == 0


@pytest.mark.asyncio
async def test_successful_state_invalidation_resync_is_not_a_protocol_deviation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_phase1_config(Path("config/phase1.toml"))
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]

    class _OneFrameSocket:
        def __init__(self, asset_ids: tuple[str, ...], **kwargs: object) -> None:
            assert asset_ids == selected.clob_token_ids
            del kwargs
            self.subscription_semantics_sha256 = hashlib.sha256(
                b"successful-state-invalidation"
            ).hexdigest()

        async def pump_received(
            self,
            admit: Callable[[ReceivedWebSocketMessage], None],
            *,
            ready: asyncio.Event,
        ) -> None:
            ready.set()
            admit(
                ReceivedWebSocketMessage(
                    payload_bytes=b"preserved-state-invalidation",
                    received_wall_time=UtcTimestamp.now(),
                    received_monotonic_ns=observe_module.time.monotonic_ns(),
                    message_type="BINARY",
                )
            )
            await asyncio.Event().wait()

    class _SuccessfulInvalidationCapture:
        def __init__(self) -> None:
            now_ns = observe_module.time.monotonic_ns()
            self.book_freshness_receive_ns = (now_ns, now_ns)
            self.book_freshness_receive_ns_by_instrument = {
                InstrumentId(asset_id): now_ns for asset_id in ("token-yes", "token-no")
            }
            self.processor = self

        @staticmethod
        def audit_book_identity(native_asset_id: str) -> object:
            return SimpleNamespace(instrument_id=InstrumentId(native_asset_id))

        async def preserve_websocket_batch(
            self,
            items: tuple[object, ...],
            **kwargs: object,
        ) -> tuple[ProcessedEnvelope, ...]:
            assert len(items) == 1
            del kwargs
            return (
                ProcessedEnvelope(
                    ingest_seq=7,
                    success=True,
                    canonical_event_count=1,
                    scan_count=1,
                    needs_resync=True,
                    affected_instrument_ids=(),
                    reference_refresh_requests=(),
                    book_freshness_updates=(),
                    scan_triggers=(),
                    error_type=None,
                ),
            )

    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _OneFrameSocket)
    metrics = InMemoryMetrics()
    service = PolymarketObservationService(
        config,
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    runtime_health = ObservationRuntimeHealth()
    loop = asyncio.get_running_loop()
    generation = await service._run_generation(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, object()),
        capture=cast(observe_module._RawCaptureBoundary, _SuccessfulInvalidationCapture()),  # pyright: ignore[reportPrivateUsage]
        selected=(selected,),
        asset_ids=selected.clob_token_ids,
        deadline=loop.time() + 1.0,
        deadline_ns=observe_module.time.monotonic_ns() + 1_000_000_000,
        periodic_audit_due_ns=observe_module.time.monotonic_ns() + 30_000_000_000,
        connection_id=ConnectionId("successful-state-invalidation-socket"),
        subscription_id=SubscriptionId("successful-state-invalidation-subscription"),
        unavailable_started_ns=None,
        runtime_health=runtime_health,
    )

    assert generation.reason.value == "RESYNC_REQUIRED"
    assert generation.deviations == ()
    assert runtime_health.snapshot().protocol_deviations == 0


@pytest.mark.asyncio
async def test_observation_closes_writer_task_on_bootstrap_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BrokenPublicClient)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )

    with pytest.raises(RuntimeError, match="bootstrap fixture failure"):
        await service.run(duration_seconds=1)

    active_names = {
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    }
    assert not any(name.startswith("raw-journal-") for name in active_names)


@pytest.mark.asyncio
async def test_observation_duration_uses_monotonic_time_when_wall_clock_steps_backward(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wall_times = iter(
        (
            UtcTimestamp(2_000_000_000),
            UtcTimestamp(1_500_000_000),
            UtcTimestamp(1_000_000_000),
        )
    )
    monotonic_times = [10_000_000_000, 10_250_000_000, 10_250_000_000]

    class _SteppingWallClock:
        @staticmethod
        def now() -> UtcTimestamp:
            return next(wall_times)

    class _SteadyObserveClock:
        @staticmethod
        def monotonic_ns() -> int:
            if len(monotonic_times) > 1:
                return monotonic_times.pop(0)
            return monotonic_times[0]

    async def empty_bootstrap(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        processor: object,
        deadline: float | None = None,
    ) -> tuple[tuple[object, ...], tuple[str, ...], None]:
        del service, client, capture, processor, deadline
        return (), (), None

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BrokenPublicClient)
    monkeypatch.setattr(observe_module, "UtcTimestamp", _SteppingWallClock)
    monkeypatch.setattr(observe_module, "time", _SteadyObserveClock)
    monkeypatch.setattr(PolymarketObservationService, "_bootstrap", empty_bootstrap)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )

    summary = await service.run(duration_seconds=1)

    assert summary.ended_at < summary.started_at
    assert summary.actual_duration_nanoseconds == 250_000_000


@pytest.mark.asyncio
async def test_observation_closes_writer_task_on_websocket_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def bootstrap_fixture(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        processor: object,
        deadline: float | None = None,
    ) -> tuple[tuple[object, ...], tuple[str, ...], None]:
        del service, client, capture, processor, deadline
        market = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
        return (market,), market.clob_token_ids, None

    monkeypatch.setattr(observe_module, "PolymarketPublicClient", _BrokenPublicClient)
    monkeypatch.setattr(observe_module, "PolymarketMarketWebSocket", _ExplodingSocket)
    monkeypatch.setattr(PolymarketObservationService, "_bootstrap", bootstrap_fixture)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )

    with pytest.raises(RuntimeError, match="websocket fixture failure"):
        await service.run(duration_seconds=1)

    active_names = {
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and not task.done()
    }
    assert not any(name.startswith("raw-journal-") for name in active_names)


@pytest.mark.asyncio
async def test_refresh_requests_are_selected_market_bounded_and_coalesced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = parse_gamma_markets(_fixture("polymarket_gamma_market_keyset.json")).markets[0]
    requests = (
        *(
            ReferenceRefreshRequest(
                source_event_id=CanonicalEventId(f"refresh-{index}"),
                event_type=CanonicalEventType.TICK_SIZE_CHANGE,
                market_id=None,
                native_market_id=selected.gamma_market_id,
                native_condition_id=selected.condition_id,
                native_asset_ids=selected.clob_token_ids,
            )
            for index in range(4_095)
        ),
        ReferenceRefreshRequest(
            source_event_id=CanonicalEventId("refresh-unrelated"),
            event_type=CanonicalEventType.TICK_SIZE_CHANGE,
            market_id=None,
            native_market_id="unrelated-market",
            native_condition_id="unrelated-condition",
            native_asset_ids=("unrelated-yes", "unrelated-no"),
        ),
    )
    refreshed: list[tuple[str, float | None]] = []
    snapshots: list[tuple[tuple[str, ...], float | None, bool]] = []

    async def record_refresh(
        service: PolymarketObservationService,
        client: object,
        capture: object,
        *,
        gamma_market_id: str,
        fallback: GammaMarket | None = None,
        deadline: float | None = None,
    ) -> None:
        del service, client, capture, fallback
        refreshed.append((gamma_market_id, deadline))

    async def record_snapshot(
        service: PolymarketObservationService,
        *,
        client: object,
        capture: object,
        selected: tuple[GammaMarket, ...],
        deadline: float | None = None,
        refresh_reference: bool = True,
    ) -> None:
        del service, client, capture
        snapshots.append(
            (tuple(market.gamma_market_id for market in selected), deadline, refresh_reference)
        )

    monkeypatch.setattr(
        PolymarketObservationService,
        "_refresh_reference_by_id",
        record_refresh,
    )
    monkeypatch.setattr(PolymarketObservationService, "_resnapshot", record_snapshot)
    metrics = InMemoryMetrics()
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
        metrics=metrics,
    )
    deadline = asyncio.get_running_loop().time() + 1.0
    handle_refresh = cast(
        Callable[..., Awaitable[bool]],
        service.__getattribute__("_handle_refresh_requests"),
    )

    resolved = await handle_refresh(
        requests=requests,
        client=object(),
        capture=object(),
        selected=(selected,),
        deadline=deadline,
    )

    assert not resolved
    assert refreshed == [(selected.gamma_market_id, deadline)]
    assert snapshots == [((selected.gamma_market_id,), deadline, False)]
    counters = {key.name: value for key, value in metrics.snapshot().counters}
    assert counters["mma_reference_refresh_requests_total"] == 4_096
    assert counters["mma_reference_refresh_requests_ignored_total"] == 1


@pytest.mark.asyncio
async def test_remote_deadline_is_adapter_owned_but_not_applied_to_payload_preservation(
    tmp_path: Path,
) -> None:
    class _SlowClient:
        def __init__(self) -> None:
            self.deadline: float | None = None

        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            del asset_id
            self.deadline = deadline
            raise PolymarketHttpTransportError(
                endpoint_class=PolymarketEndpointClass.CLOB_BOOK,
                category=PolymarketHttpFailureCategory.DEADLINE_EXCEEDED,
                retryable=False,
                attempt=0,
                detail="adapter fixture enforced the absolute deadline",
            )

    class _ImmediateClient:
        async def fetch_order_book_raw(
            self,
            asset_id: str,
            *,
            deadline: float | None = None,
        ) -> bytes:
            del asset_id
            assert deadline is not None
            return b"received"

    class _SlowCapture:
        def __init__(self, result: ProcessedEnvelope) -> None:
            self.result = result
            self.payload: bytes | None = None

        async def preserve_then_process(
            self,
            *,
            channel: str,
            payload: bytes,
            transport_type: str,
            semantics_hash: str,
        ) -> ProcessedEnvelope:
            del channel, transport_type, semantics_hash
            self.payload = payload
            await asyncio.sleep(0.02)
            return self.result

    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )
    slow_client = _SlowClient()
    loop = asyncio.get_running_loop()
    fetch_book = cast(
        Callable[..., Awaitable[ProcessedEnvelope]],
        service.__getattribute__("_fetch_book"),
    )

    absolute_deadline = loop.time() + 0.01
    with pytest.raises(PolymarketHttpTransportError) as deadline_error:
        await fetch_book(
            slow_client,
            object(),
            "asset",
            audit=False,
            deadline=absolute_deadline,
        )
    assert deadline_error.value.category is PolymarketHttpFailureCategory.DEADLINE_EXCEEDED
    assert slow_client.deadline == absolute_deadline

    expected = cast(ProcessedEnvelope, object())
    slow_capture = _SlowCapture(expected)
    actual = await fetch_book(
        _ImmediateClient(),
        slow_capture,
        "asset",
        audit=False,
        deadline=loop.time() + 0.005,
    )
    assert actual is expected
    assert slow_capture.payload == b"received"


@pytest.mark.asyncio
async def test_observation_rejects_symlinked_output_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    workspace.mkdir()
    outside.mkdir()
    (workspace / "data").symlink_to(outside, target_is_directory=True)
    service = PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=workspace,
        code_commit="fixture",
    )

    with pytest.raises(ValueError, match="outside workspace"):
        await service.run(duration_seconds=1)
