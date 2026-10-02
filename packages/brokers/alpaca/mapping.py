"""Translation between Alpaca's REST/stream payloads and the platform's normalized entities.

Bracket legs: Alpaca gives each leg its own random `client_order_id`. The platform names them deterministically
(`<parent>-tp`, `<parent>-sl`, as the mock broker does), so restarts and reconciliation never depend on ids the
broker invented. The adapter keeps the native-id <-> canonical-id map.
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

from packages.common.entities import Order, Position
from packages.common.enums import (
    AssetClass,
    OrderClass,
    OrderEventType,
    OrderIntent,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)

ORDER_STATUS: dict[str, OrderStatus] = {
    "pending_new": OrderStatus.SUBMITTED,
    "new": OrderStatus.ACKNOWLEDGED,
    "accepted": OrderStatus.ACKNOWLEDGED,
    "accepted_for_bidding": OrderStatus.ACKNOWLEDGED,
    "held": OrderStatus.ACKNOWLEDGED,  # bracket legs wait for the entry to fill
    "calculated": OrderStatus.ACKNOWLEDGED,
    "stopped": OrderStatus.ACKNOWLEDGED,
    "suspended": OrderStatus.ACKNOWLEDGED,
    "pending_review": OrderStatus.ACKNOWLEDGED,
    "pending_replace": OrderStatus.ACKNOWLEDGED,
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "pending_cancel": OrderStatus.CANCEL_REQUESTED,
    "canceled": OrderStatus.CANCELLED,
    "replaced": OrderStatus.CANCELLED,
    "expired": OrderStatus.EXPIRED,
    "done_for_day": OrderStatus.EXPIRED,  # day orders: no more fills today
    "rejected": OrderStatus.REJECTED,
}

EVENT_TYPE: dict[str, OrderEventType] = {
    "pending_new": OrderEventType.SUBMITTED,
    "new": OrderEventType.ACKNOWLEDGED,
    "accepted": OrderEventType.ACKNOWLEDGED,
    "held": OrderEventType.ACKNOWLEDGED,
    "calculated": OrderEventType.ACKNOWLEDGED,
    "stopped": OrderEventType.ACKNOWLEDGED,
    "suspended": OrderEventType.ACKNOWLEDGED,
    "pending_replace": OrderEventType.ACKNOWLEDGED,
    "partial_fill": OrderEventType.PARTIAL_FILL,
    "fill": OrderEventType.FILL,
    "pending_cancel": OrderEventType.CANCEL_REQUESTED,
    "canceled": OrderEventType.CANCELLED,
    "expired": OrderEventType.EXPIRED,
    "done_for_day": OrderEventType.EXPIRED,
    "replaced": OrderEventType.REPLACED,
    "rejected": OrderEventType.REJECTED,
    "order_cancel_rejected": OrderEventType.ERROR,
    "order_replace_rejected": OrderEventType.ERROR,
}

_CODE_TO_INTENT = {intent.code: intent for intent in OrderIntent}
_SUFFIX = re.compile(r"-([a-z]{2})\d*$")
LEG_CODES = {
    OrderIntent.TAKE_PROFIT.code: OrderIntent.TAKE_PROFIT,
    OrderIntent.STOP_LOSS.code: OrderIntent.STOP_LOSS,
}


def leg_client_order_id(parent_client_order_id: str, intent: OrderIntent) -> str:
    return f"{parent_client_order_id}-{intent.code}"


def split_leg_id(client_order_id: str) -> tuple[str, OrderIntent] | None:
    """`<parent>-tp` -> (parent, TAKE_PROFIT) when the parent is one of our entries; None otherwise."""
    parent, _, code = client_order_id.rpartition("-")
    intent = LEG_CODES.get(code)
    if intent is None or not parent:
        return None
    parent_intent = intent_from_client_order_id(parent)
    return (parent, intent) if parent_intent is OrderIntent.ENTRY else None


def intent_from_client_order_id(client_order_id: str) -> OrderIntent:
    """Our ids end with the intent code (`jev-<signal>-en`, `...-tx2`). Anything else is a manual order."""
    match = _SUFFIX.search(client_order_id)
    if match is None or not client_order_id.startswith("jev-"):
        return OrderIntent.MANUAL
    return _CODE_TO_INTENT.get(match.group(1), OrderIntent.MANUAL)


def leg_intent(raw: dict[str, Any]) -> OrderIntent:
    """Bracket legs: the limit leg takes profit, the stop leg stops the loss."""
    kind = raw.get("type") or raw.get("order_type")
    return OrderIntent.TAKE_PROFIT if kind == "limit" else OrderIntent.STOP_LOSS


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    # Alpaca sends nanoseconds; Python keeps microseconds.
    match = re.match(r"^(.*\.\d{6})\d*(.*)$", text)
    if match:
        text = match.group(1) + match.group(2)
    return datetime.fromisoformat(text)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def order_from_alpaca(
    raw: dict[str, Any],
    *,
    broker: str,
    client_order_id: str | None = None,
    intent: OrderIntent | None = None,
    parent_client_order_id: str | None = None,
    signal_id: str | None = None,
    received_at: datetime,
) -> Order:
    """One Alpaca order (without its legs) as a normalized Order. Canonical id / intent override Alpaca's."""
    cid = client_order_id or raw["client_order_id"]
    created = parse_time(raw.get("created_at")) or received_at
    updated = parse_time(raw.get("updated_at")) or created
    status = ORDER_STATUS.get(raw.get("status", ""), OrderStatus.ERROR)
    order_class = OrderClass.BRACKET if raw.get("order_class") == "bracket" else OrderClass.SIMPLE
    kind = raw.get("type") or raw.get("order_type") or "market"
    tif = raw.get("time_in_force", "day")
    return Order(
        client_order_id=cid,
        broker=broker,
        symbol=raw["symbol"],
        side=Side(raw["side"]),
        quantity=float(raw.get("qty") or 0.0),
        order_type=OrderType(kind) if kind in OrderType._value2member_map_ else OrderType.MARKET,
        time_in_force=TimeInForce(tif) if tif in TimeInForce._value2member_map_ else TimeInForce.DAY,
        intent=intent or intent_from_client_order_id(cid),
        status=status,
        broker_order_id=raw.get("id"),
        limit_price=_float(raw.get("limit_price")),
        stop_price=_float(raw.get("stop_price")),
        order_class=order_class,
        parent_client_order_id=parent_client_order_id,
        signal_id=signal_id,
        asset_class=AssetClass.US_EQUITY,
        filled_quantity=float(raw.get("filled_qty") or 0.0),
        average_fill_price=_float(raw.get("filled_avg_price")),
        reject_reason=raw.get("reject_reason") if status is OrderStatus.REJECTED else None,
        created_at=created,
        submitted_at=parse_time(raw.get("submitted_at")),
        updated_at=updated,
    )


def position_from_alpaca(raw: dict[str, Any], *, broker: str) -> Position:
    quantity = abs(float(raw["qty"]))
    if raw.get("side") == "short" or float(raw["qty"]) < 0:
        quantity = -quantity
    return Position(
        broker=broker,
        symbol=raw["symbol"],
        quantity=quantity,
        average_entry_price=float(raw["avg_entry_price"]),
        market_price=_float(raw.get("current_price")),
    )


def price_for_alpaca(price: float) -> str:
    """Alpaca rejects sub-penny prices at or above 1 USD (4 decimals below)."""
    decimals = 2 if price >= 1.0 else 4
    rounded = round(price, decimals)
    if not math.isfinite(rounded) or rounded <= 0:
        raise ValueError(f"invalid price {price!r}")
    return f"{rounded:.{decimals}f}"
