"""BrokerAdapter contract (docs/BROKER_ARCHITECTURE.md §2).

Every adapter must pass these tests. Only the mock exists today; Alpaca Paper (phase 4) and IBKR Paper (phase 8)
join the `adapter_factory` parametrization. Market movement is driven through the mock's simulation hooks, which
the other adapters will replace with their paper environments.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

import pytest

from packages.brokers.base import BrokerAdapter, OrderQueryStatus
from packages.brokers.mock import MockBrokerAdapter
from packages.common.calendar import RegularHoursCalendar
from packages.common.clock import SimulatedClock
from packages.common.config import CostsSection, MockBrokerSection
from packages.common.costs import CostModel
from packages.common.entities import OrderRequest
from packages.common.enums import OrderClass, OrderEventType, OrderIntent, OrderStatus, OrderType, Side
from packages.common.errors import (
    AmbiguousSubmission,
    BrokerUnavailable,
    DuplicateClientOrderId,
    OrderNotFound,
    OrderRejected,
)
from tests.helpers import SESSION_OPEN, make_bar, make_quote

CALENDAR = RegularHoursCalendar()
T = SESSION_OPEN + timedelta(minutes=30)


class Harness:
    def __init__(self, broker: MockBrokerAdapter, clock: SimulatedClock) -> None:
        self.broker = broker
        self.clock = clock
        self.minute = 30

    def bar(self, close: float = 100.0, **kwargs: float) -> None:
        start = SESSION_OPEN + timedelta(minutes=self.minute)
        self.minute += 1
        self.clock.advance_to(start + timedelta(minutes=1))
        self.broker.on_quote(make_quote("TEST", at=start, bid=close - 0.01, ask=close + 0.01))
        self.broker.on_bar(make_bar(symbol="TEST", start=start, close=close, **kwargs))


def mock_factory(**config: object) -> Harness:
    clock = SimulatedClock(T)
    broker = MockBrokerAdapter(MockBrokerSection(**config), CostModel(CostsSection()), clock, CALENDAR)
    return Harness(broker, clock)


@pytest.fixture(params=[mock_factory], ids=["mock"])
def adapter_factory(request: pytest.FixtureRequest) -> Callable[..., Harness]:
    return request.param


@pytest.fixture
async def harness(adapter_factory: Callable[..., Harness]) -> Harness:
    h = adapter_factory()
    await h.broker.connect()
    h.bar(100.0)  # the instrument has a price
    return h


def market(
    cid: str, side: Side = Side.BUY, quantity: float = 10, intent: OrderIntent = OrderIntent.ENTRY
) -> OrderRequest:
    return OrderRequest(client_order_id=cid, symbol="TEST", side=side, quantity=quantity, intent=intent)


def bracket(cid: str, side: Side = Side.BUY) -> OrderRequest:
    tp, sl = (101.0, 99.0) if side is Side.BUY else (99.0, 101.0)
    return OrderRequest(
        client_order_id=cid,
        symbol="TEST",
        side=side,
        quantity=10,
        order_class=OrderClass.BRACKET,
        take_profit_price=tp,
        stop_loss_price=sl,
        intent=OrderIntent.ENTRY,
    )


async def test_identity_is_paper_and_account_is_masked(harness: Harness) -> None:
    broker: BrokerAdapter = harness.broker
    assert broker.capabilities.is_paper
    account = await broker.get_account()
    assert account.is_paper and account.account_ref.startswith("***")
    health = await broker.health()
    assert health.connected and health.order_stream_connected and health.account_available


async def test_submit_acknowledges_and_never_reports_unconfirmed_fills(harness: Harness) -> None:
    order = await harness.broker.submit_order(market("c-1"))
    assert order.status is OrderStatus.ACKNOWLEDGED and order.filled_quantity == 0
    assert order.broker_order_id
    assert (await harness.broker.get_order("c-1")).status is OrderStatus.ACKNOWLEDGED


async def test_market_order_fills_on_the_next_bar_with_adverse_costs(harness: Harness) -> None:
    await harness.broker.submit_order(market("c-1"))
    harness.bar(close=100.5, open_=100.2)
    filled = await harness.broker.get_order("c-1")
    assert filled is not None and filled.status is OrderStatus.FILLED
    assert (
        filled.average_fill_price is not None and filled.average_fill_price > 100.2
    )  # open + half spread + slippage
    positions = await harness.broker.get_positions()
    assert [(p.symbol, p.quantity) for p in positions] == [("TEST", 10)]
    events = harness.broker.drain_events()
    assert [e.event_type for e in events] == [OrderEventType.ACKNOWLEDGED, OrderEventType.FILL]
    assert len({e.event_id for e in events}) == 2


async def test_no_fill_on_a_bar_that_started_before_submission(harness: Harness) -> None:
    harness.clock.advance_to(harness.clock.now() + timedelta(seconds=30))  # submitted inside the 14:31 bar
    await harness.broker.submit_order(market("c-1"))
    harness.broker.on_bar(make_bar(symbol="TEST", start=SESSION_OPEN + timedelta(minutes=31), close=100))
    assert (await harness.broker.get_order("c-1")).status is OrderStatus.ACKNOWLEDGED  # no look-ahead
    harness.broker.on_bar(make_bar(symbol="TEST", start=SESSION_OPEN + timedelta(minutes=32), close=100))
    assert (await harness.broker.get_order("c-1")).status is OrderStatus.FILLED


async def test_duplicate_client_order_id_is_refused(harness: Harness) -> None:
    await harness.broker.submit_order(market("c-1"))
    with pytest.raises(DuplicateClientOrderId):
        await harness.broker.submit_order(market("c-1"))
    assert len(await harness.broker.get_orders(OrderQueryStatus.ALL)) == 1


async def test_cancel_semantics(harness: Harness) -> None:
    await harness.broker.submit_order(market("c-1"))
    await harness.broker.cancel_order("c-1")
    assert (await harness.broker.get_order("c-1")).status is OrderStatus.CANCELLED
    await harness.broker.cancel_order("c-1")  # terminal: no-op
    with pytest.raises(OrderNotFound):
        await harness.broker.cancel_order("unknown")
    assert await harness.broker.get_order("unknown") is None


async def test_bracket_legs_activate_on_fill_and_are_one_cancels_other(harness: Harness) -> None:
    order = await harness.broker.submit_order(bracket("b-1"))
    assert [leg.intent for leg in order.legs] == [OrderIntent.TAKE_PROFIT, OrderIntent.STOP_LOSS]
    harness.bar(close=100.0)  # entry fills; take profit cannot trigger on its activation bar
    harness.bar(close=101.5, open_=100.2, high=101.6)  # take profit reached
    view = await harness.broker.get_order("b-1")
    statuses = {leg.intent: leg.status for leg in view.legs}
    assert statuses == {
        OrderIntent.TAKE_PROFIT: OrderStatus.FILLED,
        OrderIntent.STOP_LOSS: OrderStatus.CANCELLED,
    }
    assert await harness.broker.get_positions() == []


async def test_protective_quantity_cannot_be_sold_twice(harness: Harness) -> None:
    await harness.broker.submit_order(bracket("b-1"))
    harness.bar(close=100.0)
    with pytest.raises(OrderRejected, match="insufficient qty"):
        await harness.broker.submit_order(market("x-1", Side.SELL, 10, OrderIntent.TIME_EXIT))
    for leg in ("b-1-tp", "b-1-sl"):
        await harness.broker.cancel_order(leg)
    exit_order = await harness.broker.submit_order(market("x-2", Side.SELL, 10, OrderIntent.TIME_EXIT))
    assert exit_order.status is OrderStatus.ACKNOWLEDGED


async def test_rejections_are_synchronous(harness: Harness) -> None:
    with pytest.raises(OrderRejected, match="buying power"):
        await harness.broker.submit_order(market("big", quantity=10_000))
    with pytest.raises(OrderRejected, match="fractional"):
        await harness.broker.submit_order(market("frac", quantity=1.5))


async def test_injected_failures_map_to_the_error_contract(harness: Harness) -> None:
    broker = harness.broker
    broker.inject_failure("timeout_before_accept")
    with pytest.raises(AmbiguousSubmission):
        await broker.submit_order(market("a-1"))
    assert await broker.get_order("a-1") is None
    broker.inject_failure("timeout_after_accept")
    with pytest.raises(AmbiguousSubmission):
        await broker.submit_order(market("a-2"))
    assert (await broker.get_order("a-2")) is not None
    broker.inject_failure("unavailable")
    with pytest.raises(BrokerUnavailable):
        await broker.submit_order(market("a-3"))
    broker.inject_failure("reject")
    with pytest.raises(OrderRejected):
        await broker.submit_order(market("a-4"))


async def test_disconnected_broker_refuses_everything(harness: Harness) -> None:
    await harness.broker.disconnect()
    with pytest.raises(BrokerUnavailable):
        await harness.broker.get_account()
    with pytest.raises(BrokerUnavailable):
        await harness.broker.submit_order(market("d-1"))
    assert not (await harness.broker.health()).connected


async def test_order_event_stream_delivers_normalized_events(harness: Harness) -> None:
    await harness.broker.submit_order(market("s-1"))
    stream = harness.broker.stream_order_events()
    event = await anext(stream)
    assert event.client_order_id == "s-1" and event.event_type is OrderEventType.ACKNOWLEDGED
    assert event.order.status is OrderStatus.ACKNOWLEDGED and event.broker == harness.broker.name
    await harness.broker.disconnect()
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


async def test_shortable_list_is_enforced(adapter_factory: Callable[..., Harness]) -> None:
    h = adapter_factory(shortable_symbols=["OTHER"])
    await h.broker.connect()
    h.bar(100.0)
    with pytest.raises(OrderRejected, match="shortable"):
        await h.broker.submit_order(market("short-1", Side.SELL))
    assert not (await h.broker.get_instrument("TEST")).shortable


@pytest.mark.parametrize("order_type", [OrderType.STOP_LIMIT])
async def test_unsupported_order_types_are_rejected(harness: Harness, order_type: OrderType) -> None:
    request = OrderRequest(
        client_order_id="sl-1",
        symbol="TEST",
        side=Side.BUY,
        quantity=1,
        order_type=order_type,
        limit_price=100,
        stop_price=100,
        intent=OrderIntent.ENTRY,
    )
    with pytest.raises(OrderRejected):
        await harness.broker.submit_order(request)
