"""AlpacaBrokerAdapter and AlpacaMarketDataAdapter against an in-memory Alpaca (no network)."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import pytest

from packages.brokers.alpaca.adapter import AlpacaBrokerAdapter
from packages.brokers.alpaca.mapping import intent_from_client_order_id, price_for_alpaca, split_leg_id
from packages.brokers.base import OrderQueryStatus
from packages.common.clock import SimulatedClock
from packages.common.config import AlpacaDataSection, AlpacaSection, CostsSection
from packages.common.costs import CostModel
from packages.common.entities import MarketBar, MarketQuote, OrderRequest
from packages.common.enums import OrderClass, OrderIntent, OrderStatus, Side, Timeframe
from packages.common.errors import (
    AmbiguousSubmission,
    BrokerUnavailable,
    DataError,
    DuplicateClientOrderId,
    OrderRejected,
)
from packages.market_data.alpaca_stream import AlpacaMarketDataAdapter
from tests.fake_alpaca_live import FakeAlpacaLive

NY = ZoneInfo("America/New_York")
PREVIOUS, TODAY = date(2024, 3, 25), date(2024, 3, 26)


async def no_sleep(seconds: float) -> None:
    await asyncio.sleep(0)


def broker(fake: FakeAlpacaLive, clock: SimulatedClock, **kwargs: object) -> AlpacaBrokerAdapter:
    return AlpacaBrokerAdapter(
        AlpacaSection(enabled=True), CostModel(CostsSection()), clock, key="test-key", secret="secret",
        transport=kwargs.pop("transport", fake.transport()), connector=fake.connector, sleep=no_sleep,  # type: ignore[arg-type]
    )  # fmt: skip


def bracket(cid: str = "jev-S1-en", side: Side = Side.BUY) -> OrderRequest:
    tp, sl = (101.0, 99.0) if side is Side.BUY else (99.0, 101.0)
    return OrderRequest(
        client_order_id=cid, symbol="SPY", side=side, quantity=10, order_class=OrderClass.BRACKET,
        take_profit_price=tp, stop_loss_price=sl, intent=OrderIntent.ENTRY, signal_id="S1",
    )  # fmt: skip


def clock_and_fake() -> tuple[SimulatedClock, FakeAlpacaLive]:
    clock = SimulatedClock(datetime(2024, 3, 26, 10, 0, tzinfo=NY))
    fake = FakeAlpacaLive(clock)
    fake.last_price["SPY"] = 100.0
    return clock, fake


def test_client_order_id_helpers() -> None:
    assert split_leg_id("jev-S1-en-tp") == ("jev-S1-en", OrderIntent.TAKE_PROFIT)
    assert split_leg_id("jev-S1-en2-sl") == ("jev-S1-en2", OrderIntent.STOP_LOSS)
    assert split_leg_id("jev-S1-tx") is None and split_leg_id("manual-tp") is None
    assert intent_from_client_order_id("jev-S1-tx2") is OrderIntent.TIME_EXIT
    assert intent_from_client_order_id("my-own-order") is OrderIntent.MANUAL
    assert price_for_alpaca(101.234) == "101.23" and price_for_alpaca(0.12345) == "0.1235"


async def test_bracket_legs_get_canonical_ids_and_duplicates_are_detected() -> None:
    clock, fake = clock_and_fake()
    adapter = broker(fake, clock)
    await adapter.connect()
    try:
        order = await adapter.submit_order(bracket())
        assert order.status is OrderStatus.ACKNOWLEDGED and order.signal_id == "S1"
        assert [leg.client_order_id for leg in order.legs] == ["jev-S1-en-tp", "jev-S1-en-sl"]
        assert [leg.intent for leg in order.legs] == [OrderIntent.TAKE_PROFIT, OrderIntent.STOP_LOSS]
        assert {leg.parent_client_order_id for leg in order.legs} == {"jev-S1-en"}
        sent = fake.order_requests[0]
        assert sent["take_profit"] == {"limit_price": "101.00"} and sent["stop_loss"] == {
            "stop_price": "99.00"
        }
        leg = await adapter.get_order("jev-S1-en-sl")
        assert leg is not None and leg.intent is OrderIntent.STOP_LOSS and leg.stop_price == 99.0
        assert await adapter.get_order("jev-unknown-en") is None
        with pytest.raises(DuplicateClientOrderId):
            await adapter.submit_order(bracket())
        open_orders = await adapter.get_orders(OrderQueryStatus.OPEN)
        assert [o.client_order_id for o in open_orders] == ["jev-S1-en"] and len(open_orders[0].legs) == 2
        account = await adapter.get_account()
        assert account.is_paper and account.account_ref == "***TEST"
        health = await adapter.health()
        assert health.order_stream_connected and health.server_time is not None
        assert abs((health.server_time - clock.now()).total_seconds()) < 1.0
    finally:
        await adapter.disconnect()


async def test_fills_arrive_normalized_and_missed_ones_are_resynced() -> None:
    clock, fake = clock_and_fake()
    adapter = broker(fake, clock)
    await adapter.connect()
    events = adapter.stream_order_events()
    try:
        await adapter.submit_order(bracket())
        first = await asyncio.wait_for(anext(events), 1)
        assert first.client_order_id == "jev-S1-en" and first.event_type.value == "acknowledged"
        bar = MarketBar(symbol="SPY", timeframe="1Min", start=clock.now() + timedelta(minutes=1), open=100.0,
                        high=100.2, low=99.9, close=100.1, volume=1000, source="fake")  # fmt: skip
        await fake.advance([bar])
        fill = await asyncio.wait_for(anext(events), 1)
        assert fill.event_type.value == "fill" and fill.order.filled_quantity == 10
        legs = [await asyncio.wait_for(anext(events), 1) for _ in range(2)]
        assert {e.client_order_id for e in legs} == {"jev-S1-en-tp", "jev-S1-en-sl"}

        # the stream drops; the take profit fills meanwhile; after reconnecting the adapter re-reads the order
        fake.drop_connections("trading")
        await asyncio.sleep(0)
        top = MarketBar(symbol="SPY", timeframe="1Min", start=bar.end + timedelta(minutes=1), open=100.5, high=101.5, low=100.4,
                        close=101.2, volume=1000, source="fake")  # fmt: skip
        fake._trade_through(top)
        synced = [await asyncio.wait_for(anext(events), 2) for _ in range(2)]
        by_id = {e.client_order_id: e for e in synced}
        assert by_id["jev-S1-en-tp"].order.status is OrderStatus.FILLED
        assert by_id["jev-S1-en-sl"].order.status is OrderStatus.CANCELLED
        assert all(e.reason == "sync_after_reconnect" for e in synced)
    finally:
        await adapter.disconnect()


async def test_submission_errors_follow_the_contract() -> None:
    clock, fake = clock_and_fake()

    def respond(
        status: int | None, exc: type[Exception] | None = None, message: str = "x"
    ) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                if exc is not None:
                    raise exc("boom")
                return httpx.Response(status or 500, json={"message": message})
            return fake.handle(request)

        return httpx.MockTransport(handler)

    cases = [
        (respond(500), AmbiguousSubmission),
        (respond(None, httpx.ReadTimeout), AmbiguousSubmission),
        (respond(None, httpx.ConnectError), BrokerUnavailable),
        (respond(429), BrokerUnavailable),
        (respond(403, message="insufficient buying power"), OrderRejected),
        (respond(422, message="client_order_id must be unique"), DuplicateClientOrderId),
    ]
    for transport, error in cases:
        adapter = broker(fake, clock, transport=transport)
        await adapter.connect()
        try:
            with pytest.raises(error):
                await adapter.submit_order(bracket())
        finally:
            await adapter.disconnect()


async def test_market_stream_parses_bars_and_quotes_and_stops_on_fatal_errors() -> None:
    clock, fake = clock_and_fake()
    adapter = AlpacaMarketDataAdapter(
        AlpacaDataSection(feed="iex"), clock, key="test-key", secret="secret", transport=fake.transport(),
        connector=fake.connector, sleep=no_sleep,
    )  # fmt: skip
    await adapter.connect()
    await adapter.subscribe_bars(["SPY"], Timeframe.MIN_1)
    await adapter.subscribe_quotes(["SPY"])
    stream = adapter.stream()
    reader = asyncio.ensure_future(anext(stream))
    for _ in range(100):
        if any(s.kind == "data" and s.ready for s in fake.sockets):
            break
        await asyncio.sleep(0.005)
    assert fake.subscriptions[-1] == {"action": "subscribe", "bars": ["SPY"], "quotes": ["SPY"]}
    socket = next(s for s in fake.sockets if s.kind == "data")
    stamp = "2024-03-26T14:00:00.123456789Z"
    socket.inbox.put_nowait(json.dumps([
        {"T": "q", "S": "SPY", "bp": 0, "ap": 100.02, "bs": 1, "as": 2, "t": stamp},  # one side empty: skipped
        {"T": "q", "S": "SPY", "bp": 100.0, "ap": 100.02, "bs": 3, "as": 2, "t": stamp},
        {"T": "u", "S": "SPY", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "t": "2024-03-26T13:59:00Z"},
        {"T": "b", "S": "SPY", "o": 100, "h": 100.1, "l": 99.9, "c": 100.05, "v": 900, "t": "2024-03-26T13:59:00Z", "n": 12, "vw": 100.01},
    ]))  # fmt: skip
    quote = await asyncio.wait_for(reader, 1)
    bar = await asyncio.wait_for(anext(stream), 1)
    assert isinstance(quote, MarketQuote) and quote.bid == 100.0 and quote.source == "alpaca:iex"
    assert isinstance(bar, MarketBar) and bar.close == 100.05 and bar.trade_count == 12
    assert adapter.corrected_bars == 1
    socket.inbox.put_nowait(json.dumps([{"T": "error", "code": 406, "msg": "connection limit exceeded"}]))
    with pytest.raises(DataError, match="connection limit"):
        await asyncio.wait_for(anext(stream), 1)
    await adapter.disconnect()
