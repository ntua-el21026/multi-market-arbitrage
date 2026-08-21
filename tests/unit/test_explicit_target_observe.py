from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from multi_market_arbitrage.core.health import ComponentHealth
from multi_market_arbitrage.core.time import UtcTimestamp
from multi_market_arbitrage.pipeline.channels import (
    CLOB_MARKET_PREFIX,
    GAMMA_EVENT_PREFIX,
    GAMMA_MARKET_PREFIX,
    GAMMA_MARKETS_KEYSET,
    REST_BOOK,
)
from multi_market_arbitrage.pipeline.observe import (
    PolymarketObservationService,
    _RawCaptureBoundary,  # pyright: ignore[reportPrivateUsage]
)
from multi_market_arbitrage.pipeline.processor import CanonicalProcessor, ProcessedEnvelope
from multi_market_arbitrage.pipeline.targeting import (
    ObservationTargetMode,
    TargetSelectionError,
)
from multi_market_arbitrage.reference import PolymarketReferenceBuilder
from multi_market_arbitrage.venues.polymarket import (
    PolymarketPublicClient,
    parse_clob_market_info,
    parse_gamma_event,
    parse_gamma_market,
    parse_gamma_markets,
    parse_rest_book,
)

FIXTURES = Path(__file__).parents[1] / "golden" / "fixtures"
YES_ASSET_ID = "token-yes"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _direct_gamma_payload(**changes: object) -> bytes:
    page = json.loads(_fixture("polymarket_gamma_market_keyset.json"))
    market = page["markets"][0]
    market.update(changes)
    return json.dumps(market, separators=(",", ":")).encode()


def _no_book() -> bytes:
    value = json.loads(_fixture("polymarket_rest_book_yes.json"))
    value["asset_id"] = "token-no"
    value["hash"] = "no-book-hash"
    return json.dumps(value, separators=(",", ":")).encode()


def _processed(ingest_seq: int) -> ProcessedEnvelope:
    return ProcessedEnvelope(
        ingest_seq=ingest_seq,
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


class _ProcessorFixture:
    def __init__(self, *, bundle: object | None = None) -> None:
        self.discovered_markets: tuple[object, ...] = ()
        self.current_bundles: tuple[object, ...] = ()
        self.bundle = bundle


class _CaptureFixture:
    def __init__(self, processor: _ProcessorFixture) -> None:
        self.processor = processor
        self.channels: list[str] = []
        self._books = 0

    async def preserve_then_process(
        self,
        *,
        channel: str,
        payload: bytes,
        transport_type: str,
        semantics_hash: str,
    ) -> ProcessedEnvelope:
        del transport_type, semantics_hash
        self.channels.append(channel)
        if channel == GAMMA_MARKETS_KEYSET:
            self.processor.discovered_markets = parse_gamma_markets(payload).markets
        elif channel.startswith(GAMMA_MARKET_PREFIX):
            self.processor.discovered_markets = (parse_gamma_market(payload),)
        elif channel == REST_BOOK:
            self._books += 1
            if self._books == 2 and self.processor.bundle is not None:
                self.processor.current_bundles = (self.processor.bundle,)
        return _processed(len(self.channels) - 1)


class _ClientFixture:
    def __init__(self, *, gamma_payload: bytes, fail_direct: BaseException | None = None) -> None:
        self.gamma_payload = gamma_payload
        self.fail_direct = fail_direct
        self.calls: list[tuple[str, str]] = []

    async def fetch_gamma_market_raw(
        self,
        gamma_market_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        self.calls.append(("gamma-market", gamma_market_id))
        if self.fail_direct is not None:
            raise self.fail_direct
        return self.gamma_payload

    async def fetch_gamma_event_raw(
        self,
        event_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        self.calls.append(("gamma-event", event_id))
        return b'{"id":501,"enableNegRisk":false,"negRiskAugmented":false}'

    async def fetch_clob_market_raw(
        self,
        condition_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        self.calls.append(("clob-market", condition_id))
        return _fixture("polymarket_clob_market_info.json")

    async def fetch_order_book_raw(
        self,
        token_id: str,
        *,
        deadline: float | None = None,
    ) -> bytes:
        del deadline
        self.calls.append(("book", token_id))
        if token_id == YES_ASSET_ID:
            return _fixture("polymarket_rest_book_yes.json")
        return _no_book()

    async def fetch_markets_keyset_raw(self, **kwargs: object) -> bytes:
        del kwargs
        raise AssertionError("explicit target mode must never invoke discovery")


class _DiscoveryClientFixture(_ClientFixture):
    async def fetch_markets_keyset_raw(self, **kwargs: object) -> bytes:
        del kwargs
        self.calls.append(("discovery", "bounded"))
        return _fixture("polymarket_gamma_market_keyset.json")


def _service(tmp_path: Path) -> PolymarketObservationService:
    from multi_market_arbitrage.core.config import load_phase1_config

    return PolymarketObservationService(
        load_phase1_config(Path("config/phase1.toml")),
        workspace_root=tmp_path,
        code_commit="fixture",
    )


def _bundle_for(gamma_payload: bytes):
    gamma = parse_gamma_market(gamma_payload)
    event = parse_gamma_event(b'{"id":501,"enableNegRisk":false,"negRiskAugmented":false}')
    clob = parse_clob_market_info(
        _fixture("polymarket_clob_market_info.json"),
        condition_id=gamma.condition_id,
    )
    return PolymarketReferenceBuilder().build(
        gamma=gamma,
        clob=clob,
        rest_book=parse_rest_book(_fixture("polymarket_rest_book_yes.json")),
        parent_event=event,
        observed_at=UtcTimestamp(1),
    )


@pytest.mark.asyncio
async def test_explicit_bootstrap_never_discovers_and_binds_canonical_target(
    tmp_path: Path,
) -> None:
    gamma_payload = _direct_gamma_payload()
    bundle = _bundle_for(gamma_payload)
    processor = _ProcessorFixture(bundle=bundle)
    capture = _CaptureFixture(processor)
    client = _ClientFixture(gamma_payload=gamma_payload)

    selected, assets, target = await _service(tmp_path)._bootstrap_explicit(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, client),
        capture=cast(_RawCaptureBoundary, capture),
        processor=cast(CanonicalProcessor, processor),
        gamma_market_id="101",
    )

    assert selected[0].gamma_market_id == "101"
    assert assets == ("token-yes", "token-no")
    assert target.mode is ObservationTargetMode.EXPLICIT_GATE1
    assert target.requested_gamma_market_id == "101"
    assert target.resolved_gamma_market_id == "101"
    assert target.condition_id == "0xcondition"
    assert target.event_id == "501"
    assert (target.yes_token_id, target.no_token_id) == assets
    assert target.reference_version_ids == tuple(
        version.value for version in bundle.current_version_ids
    )
    assert client.calls == [
        ("gamma-market", "101"),
        ("gamma-event", "501"),
        ("clob-market", "0xcondition"),
        ("book", "token-yes"),
        ("book", "token-no"),
    ]
    assert capture.channels == [
        f"{GAMMA_MARKET_PREFIX}101",
        f"{GAMMA_EVENT_PREFIX}501",
        f"{CLOB_MARKET_PREFIX}0xcondition",
        REST_BOOK,
        REST_BOOK,
    ]


@pytest.mark.asyncio
async def test_developer_bootstrap_records_resolved_non_gate1_target(tmp_path: Path) -> None:
    gamma_payload = _direct_gamma_payload()
    bundle = _bundle_for(gamma_payload)
    processor = _ProcessorFixture(bundle=bundle)
    capture = _CaptureFixture(processor)
    client = _DiscoveryClientFixture(gamma_payload=gamma_payload)

    selected, assets, target = await _service(tmp_path)._bootstrap(  # pyright: ignore[reportPrivateUsage]
        client=cast(PolymarketPublicClient, client),
        capture=cast(_RawCaptureBoundary, capture),
        processor=cast(CanonicalProcessor, processor),
    )

    assert selected[0].gamma_market_id == "101"
    assert assets == ("token-yes", "token-no")
    assert target.mode is ObservationTargetMode.DEVELOPER_DISCOVERY
    assert target.requested_gamma_market_id is None
    assert target.resolved_gamma_market_id == "101"
    assert target.gate1_explicit is False
    assert target.reference_version_ids == tuple(
        version.value for version in bundle.current_version_ids
    )
    assert client.calls[0] == ("discovery", "bounded")
    assert capture.channels == [
        GAMMA_MARKETS_KEYSET,
        f"{GAMMA_MARKET_PREFIX}101",
        f"{GAMMA_EVENT_PREFIX}501",
        f"{CLOB_MARKET_PREFIX}0xcondition",
        REST_BOOK,
        REST_BOOK,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"closed": True},
        {"archived": True},
        {"negRisk": True},
        {"outcomes": '["No","Yes"]'},
    ],
)
async def test_explicit_invalid_target_fails_before_related_fetches_without_fallback(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    gamma_payload = _direct_gamma_payload(**changes)
    processor = _ProcessorFixture()
    capture = _CaptureFixture(processor)
    client = _ClientFixture(gamma_payload=gamma_payload)

    with pytest.raises(TargetSelectionError):
        await _service(tmp_path)._bootstrap_explicit(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=cast(_RawCaptureBoundary, capture),
            processor=cast(CanonicalProcessor, processor),
            gamma_market_id="101",
        )

    assert client.calls == [("gamma-market", "101")]
    assert capture.channels == [f"{GAMMA_MARKET_PREFIX}101"]


@pytest.mark.asyncio
async def test_explicit_nonexistent_target_propagates_without_discovery_fallback(
    tmp_path: Path,
) -> None:
    processor = _ProcessorFixture()
    capture = _CaptureFixture(processor)
    client = _ClientFixture(
        gamma_payload=b"unused",
        fail_direct=RuntimeError("direct Gamma 404"),
    )

    with pytest.raises(RuntimeError, match="404"):
        await _service(tmp_path)._bootstrap_explicit(  # pyright: ignore[reportPrivateUsage]
            client=cast(PolymarketPublicClient, client),
            capture=cast(_RawCaptureBoundary, capture),
            processor=cast(CanonicalProcessor, processor),
            gamma_market_id="does-not-exist",
        )

    assert client.calls == [("gamma-market", "does-not-exist")]
    assert capture.channels == []


def test_cli_forwards_explicit_gamma_market_id(monkeypatch: pytest.MonkeyPatch) -> None:
    cli = importlib.import_module("multi_market_arbitrage.cli.main")
    observed: dict[str, object] = {}

    class _Service:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def run(
            self,
            *,
            duration_seconds: float,
            gamma_market_id: str | None = None,
        ) -> object:
            observed.update(
                duration_seconds=duration_seconds,
                gamma_market_id=gamma_market_id,
            )
            return SimpleNamespace(status=ComponentHealth.HEALTHY)

    def discard_print(value: object) -> None:
        del value

    monkeypatch.setattr(cli, "PolymarketObservationService", _Service)
    monkeypatch.setattr(cli, "_print", discard_print)

    exit_code = cli.main(
        [
            "observe",
            "polymarket",
            "--duration",
            "3",
            "--gamma-market-id",
            "101",
        ]
    )

    assert exit_code == 0
    assert observed == {"duration_seconds": 3.0, "gamma_market_id": "101"}
