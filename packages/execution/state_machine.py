"""Order state machine.

Fills are derived ONLY from the broker's cumulative filled quantity: an order is never considered executed
because it was sent. Terminal states are final; ERROR is not terminal (it means "unknown, reconcile").
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from packages.common.entities import Fill, Order, OrderEvent
from packages.common.enums import OrderStatus

EPSILON = 1e-9
_S = OrderStatus

ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    _S.CREATED: frozenset(
        {_S.SUBMITTED, _S.ACKNOWLEDGED, _S.PARTIALLY_FILLED, _S.FILLED, _S.CANCEL_REQUESTED, _S.CANCELLED,
         _S.REJECTED, _S.EXPIRED, _S.ERROR}
    ),
    _S.SUBMITTED: frozenset(
        {_S.ACKNOWLEDGED, _S.PARTIALLY_FILLED, _S.FILLED, _S.CANCEL_REQUESTED, _S.CANCELLED, _S.REJECTED,
         _S.EXPIRED, _S.ERROR}
    ),
    _S.ACKNOWLEDGED: frozenset(
        {_S.PARTIALLY_FILLED, _S.FILLED, _S.CANCEL_REQUESTED, _S.CANCELLED, _S.REJECTED, _S.EXPIRED, _S.ERROR}
    ),
    _S.PARTIALLY_FILLED: frozenset({_S.FILLED, _S.CANCEL_REQUESTED, _S.CANCELLED, _S.EXPIRED, _S.ERROR}),
    _S.CANCEL_REQUESTED: frozenset(
        {_S.ACKNOWLEDGED, _S.PARTIALLY_FILLED, _S.FILLED, _S.CANCELLED, _S.EXPIRED, _S.ERROR}
    ),
    _S.ERROR: frozenset(
        {_S.SUBMITTED, _S.ACKNOWLEDGED, _S.PARTIALLY_FILLED, _S.FILLED, _S.CANCEL_REQUESTED, _S.CANCELLED,
         _S.REJECTED, _S.EXPIRED}
    ),
    _S.FILLED: frozenset(),
    _S.CANCELLED: frozenset(),
    _S.REJECTED: frozenset(),
    _S.EXPIRED: frozenset(),
}


def can_transition(current: OrderStatus, new: OrderStatus) -> bool:
    return new == current or new in ALLOWED_TRANSITIONS[current]


def fill_id_for(client_order_id: str, cumulative_quantity: float) -> str:
    """Deterministic: the same cumulative fill level always maps to the same fill id."""
    return f"{client_order_id}:{cumulative_quantity:g}"


@dataclass(frozen=True)
class AppliedUpdate:
    order: Order
    fill: Fill | None
    applied: bool
    anomaly: str | None = None


def apply_snapshot(
    order: Order,
    snapshot: Order,
    *,
    timestamp: datetime,
    fill_price: float | None = None,
    fee: float = 0.0,
    reason: str | None = None,
) -> AppliedUpdate:
    """Merge the broker's view of an order into the local record."""
    if order.is_terminal:
        changed = snapshot.status != order.status or snapshot.filled_quantity > order.filled_quantity + EPSILON
        return AppliedUpdate(order, None, False, "update_after_terminal_state" if changed else None)
    if not can_transition(order.status, snapshot.status):
        return AppliedUpdate(order, None, False, f"invalid_transition:{order.status.value}->{snapshot.status.value}")
    delta = snapshot.filled_quantity - order.filled_quantity
    if delta < -EPSILON:
        return AppliedUpdate(order, None, False, "cumulative_quantity_decreased")

    updated = order.model_copy(deep=True)
    updated.legs = []
    updated.status = snapshot.status
    updated.updated_at = max(order.updated_at, timestamp)
    updated.broker_order_id = snapshot.broker_order_id or order.broker_order_id
    updated.quantity = snapshot.quantity
    updated.limit_price = snapshot.limit_price
    updated.stop_price = snapshot.stop_price
    if snapshot.leg_client_order_ids:
        updated.leg_client_order_ids = list(snapshot.leg_client_order_ids)
    if snapshot.status is OrderStatus.REJECTED:
        updated.reject_reason = snapshot.reject_reason or reason

    fill: Fill | None = None
    if delta > EPSILON:
        price = fill_price
        if price is None:
            previous_notional = order.filled_quantity * (order.average_fill_price or 0.0)
            new_notional = snapshot.filled_quantity * (snapshot.average_fill_price or 0.0)
            price = (new_notional - previous_notional) / delta
        updated.filled_quantity = snapshot.filled_quantity
        updated.average_fill_price = snapshot.average_fill_price if snapshot.average_fill_price is not None else price
        fill = Fill(
            fill_id=fill_id_for(order.client_order_id, snapshot.filled_quantity),
            client_order_id=order.client_order_id,
            broker=order.broker,
            symbol=order.symbol,
            side=order.side,
            quantity=delta,
            price=price,
            fee=fee,
            timestamp=timestamp,
            intent=order.intent,
            signal_id=order.signal_id,
        )
    return AppliedUpdate(updated, fill, True)


def apply_event(order: Order, event: OrderEvent) -> AppliedUpdate:
    return apply_snapshot(
        order,
        event.order,
        timestamp=event.timestamp,
        fill_price=event.fill_price,
        fee=event.fee,
        reason=event.reason,
    )
