"""Position lifecycle: PENDING_ENTRY → OPEN → EXITING → CLOSED (docs/EXECUTION.md §6).

Protective stop loss / take profit live at the broker as bracket legs (OCO). This manager handles the exits the
broker cannot know about: prediction horizon reached, end of session, kill switch flattening and the remainder
of a partially filled entry. A managed exit always cancels the protective legs first and then closes only the
quantity that is still open, so it can never over-sell.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from packages.common.calendar import MarketCalendar
from packages.common.config import ExecutionSection
from packages.common.entities import Fill, Order, OrderRequest, RiskDecision, Signal
from packages.common.enums import (
    AssetClass,
    Direction,
    OrderIntent,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from packages.common.ids import make_client_order_id
from packages.execution.engine import BrokerExecutionEngine
from packages.execution.order_store import OrderStore

log = logging.getLogger(__name__)

EPSILON = 1e-9
MANAGED_EXIT_INTENTS = frozenset(
    {OrderIntent.TIME_EXIT, OrderIntent.EOD_EXIT, OrderIntent.KILL_EXIT, OrderIntent.RISK_EXIT}
)


class LifecycleState(StrEnum):
    PENDING_ENTRY = "PENDING_ENTRY"
    OPEN = "OPEN"
    EXITING = "EXITING"
    CLOSED = "CLOSED"


@dataclass
class ManagedPosition:
    signal_id: str
    symbol: str
    direction: Direction
    entry_client_order_id: str
    horizon: timedelta
    signal_expires_at: datetime
    take_profit_price: float | None = None
    stop_loss_price: float | None = None
    take_profit_id: str | None = None
    stop_loss_id: str | None = None
    entry_quantity: float = 0.0
    exit_quantity: float = 0.0
    entry_time: datetime | None = None
    entry_terminal: bool = False
    entry_cancel_requested: bool = False
    state: LifecycleState = LifecycleState.PENDING_ENTRY
    exit_reason: OrderIntent | None = None
    exit_reference_price: float | None = None
    exit_requested_at: datetime | None = None
    exit_attempts: int = 0
    closed_at: datetime | None = None

    @property
    def open_quantity(self) -> float:
        return max(0.0, self.entry_quantity - self.exit_quantity)

    @property
    def signed_open_quantity(self) -> float:
        return self.direction.sign * self.open_quantity

    @property
    def deadline(self) -> datetime | None:
        return self.entry_time + self.horizon if self.entry_time is not None else None

    @property
    def leg_ids(self) -> tuple[str, ...]:
        return tuple(leg for leg in (self.take_profit_id, self.stop_loss_id) if leg)


ExitFailureListener = Callable[[ManagedPosition, str], Awaitable[None]]


class PositionManager:
    def __init__(
        self,
        *,
        execution: BrokerExecutionEngine,
        store: OrderStore,
        calendar: MarketCalendar,
        config: ExecutionSection,
        asset_class: AssetClass,
    ) -> None:
        self._execution = execution
        self._store = store
        self._calendar = calendar
        self._cfg = config
        self._asset_class = asset_class
        self._positions: dict[str, ManagedPosition] = {}
        self._exit_failure_listener: ExitFailureListener | None = None

    def set_exit_failure_listener(self, listener: ExitFailureListener | None) -> None:
        self._exit_failure_listener = listener

    # ------------------------------------------------------------------ registry

    def register_pending(
        self, signal: Signal, decision: RiskDecision, entry_client_order_id: str
    ) -> ManagedPosition:
        position = ManagedPosition(
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            direction=signal.direction,
            entry_client_order_id=entry_client_order_id,
            horizon=timedelta(minutes=signal.horizon_minutes),
            signal_expires_at=signal.expires_at,
            take_profit_price=decision.take_profit,
            stop_loss_price=decision.stop_loss,
        )
        self._positions[signal.signal_id] = position
        return position

    def attach_entry_order(self, order: Order) -> None:
        position = self._positions.get(order.signal_id or "")
        if position is None or order.client_order_id != position.entry_client_order_id:
            return
        for leg in order.legs:
            self._record_leg(position, leg)

    @staticmethod
    def _record_leg(position: ManagedPosition, leg: Order) -> None:
        if leg.parent_client_order_id not in (None, position.entry_client_order_id):
            return
        if leg.intent is OrderIntent.TAKE_PROFIT:
            position.take_profit_id = leg.client_order_id
        elif leg.intent is OrderIntent.STOP_LOSS:
            position.stop_loss_id = leg.client_order_id

    def get(self, signal_id: str) -> ManagedPosition | None:
        return self._positions.get(signal_id)

    def active(self) -> list[ManagedPosition]:
        return [p for p in self._positions.values() if p.state is not LifecycleState.CLOSED]

    def pending_entries(self) -> list[ManagedPosition]:
        return [p for p in self.active() if p.state is LifecycleState.PENDING_ENTRY]

    def pending_entry_symbols(self) -> set[str]:
        return {p.symbol for p in self.pending_entries()}

    def expected_positions(self) -> dict[str, float]:
        expected: dict[str, float] = {}
        for position in self.active():
            if position.open_quantity > EPSILON:
                expected[position.symbol] = expected.get(position.symbol, 0.0) + position.signed_open_quantity
        return expected

    def exit_reference(self, signal_id: str, intent: OrderIntent) -> float | None:
        position = self._positions.get(signal_id)
        if position is None:
            return None
        if intent is OrderIntent.TAKE_PROFIT:
            return position.take_profit_price
        if intent is OrderIntent.STOP_LOSS:
            return position.stop_loss_price
        return position.exit_reference_price

    # ------------------------------------------------------------------ reactions

    def on_fill(self, fill: Fill) -> ManagedPosition | None:
        position = self._positions.get(fill.signal_id) if fill.signal_id else None
        if position is None:
            return None
        if fill.intent is OrderIntent.ENTRY:
            position.entry_quantity += fill.quantity
            if position.entry_time is None:
                position.entry_time = fill.timestamp
            if position.state is LifecycleState.PENDING_ENTRY:
                position.state = LifecycleState.OPEN
        elif fill.intent.is_exit:
            position.exit_quantity += fill.quantity
            if position.exit_reason is None:
                position.exit_reason = fill.intent
        self._maybe_close(position, fill.timestamp)
        return position

    async def on_order(self, order: Order) -> None:
        position = self._positions.get(order.signal_id) if order.signal_id else None
        if position is None or position.state is LifecycleState.CLOSED:
            return
        if order.parent_client_order_id == position.entry_client_order_id:
            # Merged broker snapshots carry leg ids only: learn the legs as they are reported.
            self._record_leg(position, order)
        if order.client_order_id == position.entry_client_order_id and order.is_terminal:
            position.entry_terminal = True
            if order.filled_quantity <= EPSILON:
                position.state = LifecycleState.CLOSED
                position.closed_at = order.updated_at
                return
            if order.filled_quantity < order.quantity - EPSILON and position.state is LifecycleState.OPEN:
                await self.request_exit(position, OrderIntent.RISK_EXIT, None, order.updated_at)
                return
        self._maybe_close(position, order.updated_at)
        if position.state is LifecycleState.EXITING:
            await self._progress_exit(position, order.updated_at)

    async def on_timer(
        self, now: datetime, prices: Mapping[str, float], *, flatten_all: bool = False
    ) -> None:
        session = self._calendar.session_for(now)
        for position in self.active():
            if position.state is LifecycleState.PENDING_ENTRY:
                if now > position.signal_expires_at and not position.entry_cancel_requested:
                    position.entry_cancel_requested = True
                    await self._execution.cancel(position.entry_client_order_id)
                continue
            if position.state is LifecycleState.OPEN:
                reason: OrderIntent | None = None
                if flatten_all:
                    reason = OrderIntent.KILL_EXIT
                elif (
                    session is None or session.minutes_to_close(now) <= self._cfg.flatten_minutes_before_close
                ):
                    reason = OrderIntent.EOD_EXIT
                elif self._cfg.exit_at_horizon and position.deadline is not None and now >= position.deadline:
                    reason = OrderIntent.TIME_EXIT
                if reason is not None:
                    await self.request_exit(position, reason, prices.get(position.symbol), now)
            elif position.state is LifecycleState.EXITING:
                await self._progress_exit(position, now)

    async def cancel_pending_entries(self) -> int:
        cancelled = 0
        for position in self.pending_entries():
            if not position.entry_cancel_requested:
                position.entry_cancel_requested = True
                await self._execution.cancel(position.entry_client_order_id)
                cancelled += 1
        return cancelled

    async def request_exit(
        self, position: ManagedPosition, reason: OrderIntent, reference_price: float | None, now: datetime
    ) -> None:
        if position.state is not LifecycleState.OPEN:
            return
        position.state = LifecycleState.EXITING
        position.exit_reason = reason
        position.exit_reference_price = reference_price
        position.exit_requested_at = now
        for leg_id in position.leg_ids:
            leg = await self._store.get(leg_id)
            if leg is not None and not leg.is_terminal:
                await self._execution.cancel(leg_id)
        await self._progress_exit(position, now)

    def _maybe_close(self, position: ManagedPosition, at: datetime) -> None:
        if (
            position.state is not LifecycleState.CLOSED
            and position.entry_terminal
            and position.entry_quantity > EPSILON
            and position.open_quantity <= EPSILON
        ):
            position.state = LifecycleState.CLOSED
            position.closed_at = at

    async def _progress_exit(self, position: ManagedPosition, now: datetime) -> None:
        for leg_id in position.leg_ids:
            leg = await self._store.get(leg_id)
            if leg is not None and not leg.is_terminal:
                return  # wait until the protective legs are confirmed cancelled (or filled)
        orders = await self._store.list_by_signal(position.signal_id)
        if any(o.intent in MANAGED_EXIT_INTENTS and not o.is_terminal for o in orders):
            return  # an exit order is still working
        if position.open_quantity <= EPSILON:
            self._maybe_close(position, now)
            return
        if position.exit_attempts >= self._cfg.max_exit_attempts:
            log.error("position could not be closed", extra={"signal_id": position.signal_id})
            if self._exit_failure_listener is not None:
                await self._exit_failure_listener(position, "max_exit_attempts_reached")
            return
        position.exit_attempts += 1
        intent = (
            position.exit_reason if position.exit_reason in MANAGED_EXIT_INTENTS else OrderIntent.RISK_EXIT
        )
        assert intent is not None
        request = OrderRequest(
            client_order_id=make_client_order_id(position.signal_id, intent, position.exit_attempts),
            symbol=position.symbol,
            side=position.direction.entry_side.opposite,
            quantity=position.open_quantity,
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
            intent=intent,
            signal_id=position.signal_id,
            asset_class=self._asset_class,
        )
        order = await self._execution.submit(request)
        if order.status in (OrderStatus.REJECTED, OrderStatus.ERROR):
            log.warning(
                "exit order not accepted; will retry",
                extra={"client_order_id": order.client_order_id, "reason": order.reject_reason},
            )

    # ------------------------------------------------------------------ restart

    def rebuild(
        self, orders: Sequence[Order], *, horizon: timedelta, signal_ttl: timedelta
    ) -> list[ManagedPosition]:
        """Reconstruct lifecycles from stored orders after a restart."""
        self._positions.clear()
        groups: dict[str, list[Order]] = {}
        for order in sorted(orders, key=lambda o: o.created_at):
            if order.signal_id:
                groups.setdefault(order.signal_id, []).append(order)
        restored: list[ManagedPosition] = []
        for signal_id, group in groups.items():
            entry = next(
                (o for o in group if o.intent is OrderIntent.ENTRY and o.parent_client_order_id is None), None
            )
            if entry is None:
                continue
            exit_quantity = sum(o.filled_quantity for o in group if o.intent.is_exit)
            if entry.is_terminal and entry.filled_quantity - exit_quantity <= EPSILON:
                continue
            position = ManagedPosition(
                signal_id=signal_id,
                symbol=entry.symbol,
                direction=Direction.LONG if entry.side is Side.BUY else Direction.SHORT,
                entry_client_order_id=entry.client_order_id,
                horizon=horizon,
                signal_expires_at=entry.created_at + signal_ttl,
                take_profit_price=entry.take_profit_price,
                stop_loss_price=entry.stop_loss_price,
                entry_quantity=entry.filled_quantity,
                exit_quantity=exit_quantity,
                entry_terminal=entry.is_terminal,
                entry_time=entry.updated_at if entry.filled_quantity > EPSILON else None,
            )
            for order in group:
                if order.parent_client_order_id == entry.client_order_id:
                    if order.intent is OrderIntent.TAKE_PROFIT:
                        position.take_profit_id = order.client_order_id
                    elif order.intent is OrderIntent.STOP_LOSS:
                        position.stop_loss_id = order.client_order_id
            managed_exits = [o for o in group if o.intent in MANAGED_EXIT_INTENTS]
            position.exit_attempts = len(managed_exits)
            if managed_exits:
                position.state = LifecycleState.EXITING
                position.exit_reason = managed_exits[-1].intent
            elif entry.filled_quantity > EPSILON:
                position.state = LifecycleState.OPEN
            self._positions[signal_id] = position
            restored.append(position)
        return restored
