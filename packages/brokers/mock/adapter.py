"""MockBrokerAdapter: deterministic simulated exchange + paper account.

Used as the development broker, as the execution simulator of backtests and (phase 7) as the executor of shadow
mode. Fill rules (docs/BROKER_ARCHITECTURE.md §5) never look ahead: an order can only fill on bars that start at
or after its submission time. Within a bar, protective legs follow a conservative policy by default (stop loss
before take profit; only the stop can trigger on the bar where the entry filled).
"""

from __future__ import annotations

import asyncio
import math
import secrets
from collections import deque
from collections.abc import AsyncIterator
from datetime import date, datetime

from packages.brokers.base import BrokerAdapter, OrderQueryStatus
from packages.common.calendar import MarketCalendar
from packages.common.clock import Clock
from packages.common.config import MockBrokerSection
from packages.common.costs import CostModel
from packages.common.entities import (
    AccountSnapshot,
    BrokerCapabilities,
    BrokerHealth,
    InstrumentInfo,
    MarketBar,
    MarketQuote,
    Order,
    OrderEvent,
    OrderReplace,
    OrderRequest,
    Position,
)
from packages.common.enums import (
    AssetClass,
    ConnectionStatus,
    OrderClass,
    OrderEventType,
    OrderIntent,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from packages.common.errors import (
    AmbiguousSubmission,
    BrokerUnavailable,
    DuplicateClientOrderId,
    OrderNotFound,
    OrderRejected,
)
from packages.common.numbers import round_to_tick
from packages.common.positions import EPSILON, PositionState

FAILURE_KINDS = frozenset({"reject", "timeout_before_accept", "timeout_after_accept", "unavailable"})
TICK = 0.01


class MockBrokerAdapter(BrokerAdapter):
    name = "mock"

    def __init__(
        self, config: MockBrokerSection, cost_model: CostModel, clock: Clock, calendar: MarketCalendar
    ) -> None:
        self._cfg = config
        self._costs = cost_model
        self._clock = clock
        self._calendar = calendar
        self._instance = secrets.token_hex(3)
        self._connected = False
        self._cash = config.initial_cash
        self._last_equity = config.initial_cash
        self._positions: dict[str, PositionState] = {}
        self._orders: dict[str, Order] = {}
        self._sequence: list[str] = []
        self._active_legs: dict[str, datetime] = {}
        self._last_bar: dict[str, MarketBar] = {}
        self._last_price: dict[str, float] = {}
        self._last_quote: dict[str, MarketQuote] = {}
        self._session_day: date | None = None
        self._events: deque[OrderEvent] = deque()
        self._wakeup = asyncio.Event()
        self._failures: deque[str] = deque()
        self._order_counter = 0
        self._event_counter = 0
        self._last_event_at: datetime | None = None

    # ------------------------------------------------------------------ test & simulation hooks

    def set_clock(self, clock: Clock) -> None:
        self._clock = clock

    def inject_failure(self, kind: str, times: int = 1) -> None:
        if kind not in FAILURE_KINDS:
            raise ValueError(f"unknown failure kind '{kind}'; expected one of {sorted(FAILURE_KINDS)}")
        self._failures.extend([kind] * times)

    def drain_events(self) -> list[OrderEvent]:
        events = list(self._events)
        self._events.clear()
        return events

    def force_position(self, symbol: str, quantity: float, price: float) -> None:
        """Simulate activity outside the platform (e.g. a manual trade) to test reconciliation."""
        state = self._positions.setdefault(symbol, PositionState(symbol))
        self._cash -= quantity * price
        state.apply(quantity, price)
        self._last_price.setdefault(symbol, price)

    def on_quote(self, quote: MarketQuote) -> None:
        self._last_quote[quote.symbol] = quote

    def on_bar(self, bar: MarketBar) -> None:
        previous = self._last_bar.get(bar.symbol)
        if previous is not None and bar.start <= previous.start:
            return  # duplicate or late bar: the exchange already traded through this interval
        self._roll_session(bar)
        self._process_orders(bar)
        self._last_bar[bar.symbol] = bar
        self._last_price[bar.symbol] = bar.close

    # ------------------------------------------------------------------ BrokerAdapter: connectivity

    @property
    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            broker=self.name,
            is_paper=True,
            asset_classes=(AssetClass.US_EQUITY, AssetClass.ETF),
            supports_short=True,
            supports_fractional=False,
            supports_bracket=True,
            supports_replace=True,
            supports_extended_hours=False,
            max_client_order_id_length=64,
        )

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        self._wakeup.set()

    def set_connected(self, connected: bool) -> None:
        self._connected = connected

    async def health(self) -> BrokerHealth:
        return BrokerHealth(
            broker=self.name,
            status=ConnectionStatus.CONNECTED if self._connected else ConnectionStatus.DISCONNECTED,
            connected=self._connected,
            order_stream_connected=self._connected,
            account_available=self._connected,
            latency_ms=0.0,
            last_event_at=self._last_event_at,
            server_time=self._clock.now(),
        )

    def _require_connected(self) -> None:
        if not self._connected:
            raise BrokerUnavailable("mock broker is disconnected")

    # ------------------------------------------------------------------ BrokerAdapter: account & orders

    async def get_account(self) -> AccountSnapshot:
        self._require_connected()
        long_value = sum(p.quantity * self._mark(s) for s, p in self._positions.items() if p.quantity > 0)
        short_value = sum(p.quantity * self._mark(s) for s, p in self._positions.items() if p.quantity < 0)
        return AccountSnapshot(
            broker=self.name,
            account_ref="***" + self._cfg.account_ref[-4:],
            is_paper=True,
            cash=self._cash,
            equity=self._equity(),
            buying_power=self._buying_power(),
            last_equity=self._last_equity,
            long_market_value=long_value,
            short_market_value=short_value,
            timestamp=self._clock.now(),
        )

    async def get_positions(self) -> list[Position]:
        self._require_connected()
        return [
            Position(
                broker=self.name,
                symbol=symbol,
                quantity=state.quantity,
                average_entry_price=state.average_price,
                market_price=self._last_price.get(symbol),
            )
            for symbol, state in self._positions.items()
            if abs(state.quantity) > EPSILON
        ]

    async def get_orders(self, status: OrderQueryStatus = OrderQueryStatus.OPEN) -> list[Order]:
        self._require_connected()
        orders = [self._orders[cid] for cid in self._sequence]
        if status is OrderQueryStatus.OPEN:
            orders = [o for o in orders if not o.is_terminal]
        elif status is OrderQueryStatus.CLOSED:
            orders = [o for o in orders if o.is_terminal]
        return [self._view(o) for o in orders]

    async def get_order(self, client_order_id: str) -> Order | None:
        self._require_connected()
        order = self._orders.get(client_order_id)
        return self._view(order) if order is not None else None

    async def get_instrument(self, symbol: str) -> InstrumentInfo:
        self._require_connected()
        shortable = self._cfg.shortable_symbols is None or symbol in self._cfg.shortable_symbols
        return InstrumentInfo(symbol=symbol, shortable=shortable, easy_to_borrow=shortable)

    async def submit_order(self, order: OrderRequest) -> Order:
        request = order
        self._require_connected()
        failure = self._failures.popleft() if self._failures else None
        if failure == "unavailable":
            raise BrokerUnavailable("injected failure: broker unavailable")
        if failure == "timeout_before_accept":
            raise AmbiguousSubmission("injected failure: timeout before the broker received the order")
        if request.client_order_id in self._orders:
            raise DuplicateClientOrderId(request.client_order_id)
        if failure == "reject":
            raise OrderRejected("injected rejection", request.client_order_id)
        self._validate(request)

        now = self._clock.now()
        parent = Order.from_request(request, broker=self.name, at=now)
        parent.broker_order_id = self._next_order_id()
        parent.status = OrderStatus.ACKNOWLEDGED
        parent.submitted_at = now
        self._store(parent)
        if request.order_class is OrderClass.BRACKET:
            exit_side = request.side.opposite
            legs = [
                self._new_leg(parent, exit_side, OrderIntent.TAKE_PROFIT, OrderType.LIMIT, request.take_profit_price),
                self._new_leg(parent, exit_side, OrderIntent.STOP_LOSS, OrderType.STOP, request.stop_loss_price),
            ]
            parent.leg_client_order_ids = [leg.client_order_id for leg in legs]
        self._emit(parent, OrderEventType.ACKNOWLEDGED, at=now)
        if failure == "timeout_after_accept":
            raise AmbiguousSubmission("injected failure: timeout after the broker accepted the order")
        return self._view(parent)

    async def cancel_order(self, client_order_id: str) -> None:
        self._require_connected()
        order = self._orders.get(client_order_id)
        if order is None:
            raise OrderNotFound(client_order_id)
        if order.is_terminal:
            return
        now = self._clock.now()
        self._finish(order, OrderStatus.CANCELLED, now, reason="cancelled")
        if order.filled_quantity <= EPSILON:
            for leg_id in order.leg_client_order_ids:
                leg = self._orders[leg_id]
                if not leg.is_terminal:
                    self._finish(leg, OrderStatus.CANCELLED, now, reason="parent_cancelled")

    async def replace_order(self, client_order_id: str, changes: OrderReplace) -> Order:
        self._require_connected()
        order = self._orders.get(client_order_id)
        if order is None:
            raise OrderNotFound(client_order_id)
        if order.is_terminal:
            raise OrderRejected("order is not open", client_order_id)
        if changes.quantity is not None:
            if changes.quantity < order.filled_quantity:
                raise OrderRejected("quantity below filled quantity", client_order_id)
            order.quantity = changes.quantity
        if changes.limit_price is not None:
            if order.order_type not in (OrderType.LIMIT, OrderType.STOP_LIMIT):
                raise OrderRejected("limit price on a non-limit order", client_order_id)
            order.limit_price = changes.limit_price
        if changes.stop_price is not None:
            if order.order_type not in (OrderType.STOP, OrderType.STOP_LIMIT):
                raise OrderRejected("stop price on a non-stop order", client_order_id)
            order.stop_price = changes.stop_price
        if changes.time_in_force is not None:
            order.time_in_force = changes.time_in_force
        order.updated_at = self._clock.now()
        self._emit(order, OrderEventType.REPLACED, at=order.updated_at)
        return self._view(order)

    async def stream_order_events(self) -> AsyncIterator[OrderEvent]:
        while self._connected:
            if self._events:
                yield self._events.popleft()
                continue
            self._wakeup.clear()
            await self._wakeup.wait()

    # ------------------------------------------------------------------ validation & bookkeeping

    def _validate(self, request: OrderRequest) -> None:
        cid = request.client_order_id
        if request.symbol not in self._last_price:
            raise OrderRejected("no market data for symbol yet", cid)
        if request.asset_class not in self.capabilities.asset_classes:
            raise OrderRejected(f"asset class {request.asset_class.value} not supported", cid)
        if request.extended_hours:
            raise OrderRejected("extended hours not supported", cid)
        if request.order_type is OrderType.STOP_LIMIT:
            raise OrderRejected("stop_limit orders not supported by the mock broker", cid)
        if request.quantity != math.floor(request.quantity):
            raise OrderRejected("fractional quantities not supported", cid)
        if request.order_class is OrderClass.BRACKET and len(cid) > 61:
            raise OrderRejected("client_order_id too long for bracket legs", cid)

        price = self._last_price[request.symbol]
        state = self._positions.get(request.symbol)
        current = state.quantity if state is not None else 0.0
        signed = request.side.sign * request.quantity
        reducing = abs(current) > EPSILON and (current > 0) != (signed > 0)
        if reducing:
            if request.order_class is OrderClass.BRACKET:
                raise OrderRejected("bracket orders must open or increase a position", cid)
            if request.quantity > abs(current) + EPSILON:
                raise OrderRejected("order would flip the position in one step", cid)
            available = abs(current) - self._held_quantity(request.symbol, request.side)
            if request.quantity > available + EPSILON:
                raise OrderRejected(f"insufficient qty available ({available:g})", cid)
            return
        if request.side is Side.SELL and self._cfg.shortable_symbols is not None:
            if request.symbol not in self._cfg.shortable_symbols:
                raise OrderRejected("symbol not shortable", cid)
        if request.quantity * price > self._buying_power() + EPSILON:
            raise OrderRejected("insufficient buying power", cid)

    def _held_quantity(self, symbol: str, side: Side) -> float:
        """Quantity already committed by working orders on the same side (an OCO pair counts once)."""
        held_by_group: dict[str, float] = {}
        for cid in self._sequence:
            order = self._orders[cid]
            if order.symbol != symbol or order.side is not side or order.is_terminal:
                continue
            if order.parent_client_order_id is not None and cid not in self._active_legs:
                continue
            group = order.parent_client_order_id or cid
            held_by_group[group] = max(held_by_group.get(group, 0.0), order.remaining_quantity)
        return sum(held_by_group.values())

    def _new_leg(
        self, parent: Order, side: Side, intent: OrderIntent, order_type: OrderType, price: float | None
    ) -> Order:
        leg = Order(
            client_order_id=f"{parent.client_order_id}-{intent.code}",
            broker=self.name,
            symbol=parent.symbol,
            side=side,
            quantity=parent.quantity,
            order_type=order_type,
            time_in_force=parent.time_in_force,
            intent=intent,
            status=OrderStatus.ACKNOWLEDGED,
            broker_order_id=self._next_order_id(),
            limit_price=price if order_type is OrderType.LIMIT else None,
            stop_price=price if order_type is OrderType.STOP else None,
            parent_client_order_id=parent.client_order_id,
            signal_id=parent.signal_id,
            asset_class=parent.asset_class,
            created_at=parent.created_at,
            submitted_at=parent.submitted_at,
            updated_at=parent.updated_at,
        )
        self._store(leg)
        return leg

    def _store(self, order: Order) -> None:
        self._orders[order.client_order_id] = order
        self._sequence.append(order.client_order_id)

    def _next_order_id(self) -> str:
        self._order_counter += 1
        return f"MB-{self._instance}-{self._order_counter:06d}"

    def _view(self, order: Order) -> Order:
        view = order.model_copy(deep=True)
        view.legs = [self._orders[leg].model_copy(deep=True) for leg in order.leg_client_order_ids]
        return view

    def _emit(
        self,
        order: Order,
        event_type: OrderEventType,
        *,
        at: datetime,
        fill_quantity: float | None = None,
        fill_price: float | None = None,
        fee: float = 0.0,
        reason: str | None = None,
    ) -> None:
        self._event_counter += 1
        self._events.append(
            OrderEvent(
                event_id=f"mock-{self._instance}-{self._event_counter:08d}",
                broker=self.name,
                event_type=event_type,
                timestamp=at,
                order=self._view(order),
                fill_quantity=fill_quantity,
                fill_price=fill_price,
                fee=fee,
                reason=reason,
                received_at=self._clock.now(),
            )
        )
        self._last_event_at = at
        self._wakeup.set()

    def _finish(self, order: Order, status: OrderStatus, at: datetime, *, reason: str) -> None:
        order.status = status
        order.updated_at = at
        if status is OrderStatus.REJECTED:
            order.reject_reason = reason
        self._active_legs.pop(order.client_order_id, None)
        event_type = {
            OrderStatus.CANCELLED: OrderEventType.CANCELLED,
            OrderStatus.EXPIRED: OrderEventType.EXPIRED,
            OrderStatus.REJECTED: OrderEventType.REJECTED,
        }[status]
        self._emit(order, event_type, at=at, reason=reason)

    # ------------------------------------------------------------------ account math

    def _mark(self, symbol: str) -> float:
        state = self._positions.get(symbol)
        fallback = state.average_price if state is not None else 0.0
        return self._last_price.get(symbol, fallback)

    def _equity(self) -> float:
        return self._cash + sum(p.quantity * self._mark(s) for s, p in self._positions.items())

    def _buying_power(self) -> float:
        gross = sum(abs(p.quantity) * self._mark(s) for s, p in self._positions.items())
        reserved = 0.0
        for cid in self._sequence:
            order = self._orders[cid]
            if order.intent is OrderIntent.ENTRY and not order.is_terminal:
                reserved += order.remaining_quantity * self._last_price.get(order.symbol, 0.0)
        return max(0.0, self._equity() * self._cfg.buying_power_multiplier - gross - reserved)

    # ------------------------------------------------------------------ market simulation

    def _roll_session(self, bar: MarketBar) -> None:
        day = self._calendar.trading_date(bar.start)
        if self._session_day is None:
            self._session_day = day
            return
        if day == self._session_day:
            return
        for cid in list(self._sequence):
            order = self._orders[cid]
            if not order.is_terminal and order.time_in_force is TimeInForce.DAY:
                self._finish(order, OrderStatus.EXPIRED, bar.start, reason="day_order_expired")
        self._last_equity = self._equity()
        self._session_day = day

    def _process_orders(self, bar: MarketBar) -> None:
        for cid in list(self._sequence):
            order = self._orders[cid]
            if order.symbol != bar.symbol or order.is_terminal or order.parent_client_order_id is not None:
                continue
            if order.submitted_at is None or order.submitted_at > bar.start:
                continue  # submitted during/after this bar: it can only trade on later bars
            self._try_fill(order, bar)
        for cid in list(self._sequence):
            parent = self._orders[cid]
            if parent.symbol != bar.symbol or not parent.leg_client_order_ids:
                continue
            legs = [self._orders[leg] for leg in parent.leg_client_order_ids]
            active = [leg for leg in legs if leg.client_order_id in self._active_legs and not leg.is_terminal]
            if active:
                self._process_protective_legs(active, bar)

    def _try_fill(self, order: Order, bar: MarketBar) -> None:
        fill: tuple[float, datetime] | None
        if order.order_type is OrderType.MARKET:
            fill = (self._marketable_price(order.side, bar.open, bar.symbol), bar.start)
        elif order.order_type is OrderType.LIMIT:
            fill = self._limit_fill(order, bar)
        else:
            fill = self._stop_fill(order, bar)
        if fill is None:
            return
        quantity = order.remaining_quantity
        if self._cfg.partial_fills and order.order_class is OrderClass.SIMPLE:
            quantity = min(quantity, max(1.0, math.floor(self._cfg.participation_rate * bar.volume)))
        self._fill(order, quantity, fill[0], fill[1])

    def _process_protective_legs(self, legs: list[Order], bar: MarketBar) -> None:
        stop = next((leg for leg in legs if leg.intent is OrderIntent.STOP_LOSS), None)
        target = next((leg for leg in legs if leg.intent is OrderIntent.TAKE_PROFIT), None)
        activated_this_bar = any(self._active_legs[leg.client_order_id] >= bar.start for leg in legs)
        conservative = self._cfg.intrabar_policy == "conservative"
        ordered: list[Order] = []
        if conservative:
            ordered = [leg for leg in (stop, target) if leg is not None]
        else:
            ordered = [leg for leg in (target, stop) if leg is not None]
        for leg in ordered:
            if leg.intent is OrderIntent.TAKE_PROFIT and conservative and activated_this_bar:
                continue
            fill = self._limit_fill(leg, bar) if leg.order_type is OrderType.LIMIT else self._stop_fill(leg, bar)
            if fill is not None:
                self._fill(leg, leg.remaining_quantity, fill[0], fill[1])
                return

    def _marketable_price(self, side: Side, reference: float, symbol: str) -> float:
        quote = self._last_quote.get(symbol)
        raw = self._costs.execution_price(side, reference, quote.spread_bps if quote is not None else None)
        return round_to_tick(raw, TICK, "up" if side is Side.BUY else "down")

    @staticmethod
    def _limit_fill(order: Order, bar: MarketBar) -> tuple[float, datetime] | None:
        limit = order.limit_price
        if limit is None:
            return None
        if order.side is Side.BUY:
            if bar.open <= limit:
                return bar.open, bar.start
            if bar.low < limit:
                return limit, bar.end
            return None
        if bar.open >= limit:
            return bar.open, bar.start
        if bar.high > limit:
            return limit, bar.end
        return None

    def _stop_fill(self, order: Order, bar: MarketBar) -> tuple[float, datetime] | None:
        stop = order.stop_price
        if stop is None:
            return None
        if order.side is Side.SELL:
            if bar.open <= stop:
                base, at = bar.open, bar.start
            elif bar.low <= stop:
                base, at = stop, bar.end
            else:
                return None
        elif bar.open >= stop:
            base, at = bar.open, bar.start
        elif bar.high >= stop:
            base, at = stop, bar.end
        else:
            return None
        slipped = base * (1.0 + order.side.sign * self._costs.config.slippage_bps / 1e4)
        return round_to_tick(slipped, TICK, "up" if order.side is Side.BUY else "down"), at

    def _fill(self, order: Order, quantity: float, price: float, at: datetime) -> None:
        if quantity <= EPSILON:
            return
        fee = self._costs.fees(order.side, quantity, price)
        state = self._positions.setdefault(order.symbol, PositionState(order.symbol))
        state.apply(order.side.sign * quantity, price)
        self._cash -= order.side.sign * quantity * price + fee
        previous = order.filled_quantity
        order.filled_quantity = previous + quantity
        order.average_fill_price = ((order.average_fill_price or 0.0) * previous + price * quantity) / order.filled_quantity
        order.status = OrderStatus.FILLED if order.remaining_quantity <= EPSILON else OrderStatus.PARTIALLY_FILLED
        order.updated_at = at
        event_type = OrderEventType.FILL if order.status is OrderStatus.FILLED else OrderEventType.PARTIAL_FILL
        self._emit(order, event_type, at=at, fill_quantity=quantity, fill_price=price, fee=fee)
        if order.status is not OrderStatus.FILLED:
            return
        for leg_id in order.leg_client_order_ids:  # bracket entry filled: activate protective legs
            leg = self._orders[leg_id]
            if not leg.is_terminal:
                self._active_legs[leg_id] = at
                self._emit(leg, OrderEventType.ACKNOWLEDGED, at=at, reason="leg_activated")
        if order.parent_client_order_id is not None:  # one-cancels-other
            parent = self._orders[order.parent_client_order_id]
            for sibling_id in parent.leg_client_order_ids:
                sibling = self._orders[sibling_id]
                if sibling_id != order.client_order_id and not sibling.is_terminal:
                    self._finish(sibling, OrderStatus.CANCELLED, at, reason="oco")
