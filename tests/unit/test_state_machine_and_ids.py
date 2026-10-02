from __future__ import annotations

from datetime import timedelta

import pytest

from packages.common.entities import Order, OrderRequest
from packages.common.enums import OrderClass, OrderIntent, OrderStatus, OrderType, Side, TimeInForce
from packages.common.ids import (
    digest,
    make_client_order_id,
    make_decision_id,
    make_feature_id,
    make_prediction_id,
    make_signal_id,
    make_trade_id,
    new_id,
)
from packages.execution.state_machine import apply_snapshot, can_transition, fill_id_for
from tests.helpers import SESSION_OPEN

T0 = SESSION_OPEN


def order(
    status: OrderStatus = OrderStatus.SUBMITTED, filled: float = 0.0, avg: float | None = None
) -> Order:
    return Order(
        client_order_id="jev-S-TEST-en",
        broker="mock",
        symbol="TEST",
        side=Side.BUY,
        quantity=100,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
        intent=OrderIntent.ENTRY,
        status=status,
        filled_quantity=filled,
        average_fill_price=avg,
        signal_id="S-TEST",
        created_at=T0,
        updated_at=T0,
    )


def test_transitions() -> None:
    assert can_transition(OrderStatus.SUBMITTED, OrderStatus.FILLED)
    assert can_transition(
        OrderStatus.ERROR, OrderStatus.FILLED
    )  # ERROR means "unknown": reconcile can resolve it
    assert not can_transition(OrderStatus.FILLED, OrderStatus.CANCELLED)
    assert not can_transition(OrderStatus.PARTIALLY_FILLED, OrderStatus.REJECTED)
    assert not can_transition(OrderStatus.CANCELLED, OrderStatus.ACKNOWLEDGED)


def test_fills_come_only_from_cumulative_quantity() -> None:
    local = order()
    acked = apply_snapshot(local, order(OrderStatus.ACKNOWLEDGED), timestamp=T0)
    assert acked.applied and acked.fill is None
    partial = apply_snapshot(acked.order, order(OrderStatus.PARTIALLY_FILLED, 40, 10.0), timestamp=T0)
    assert (
        partial.fill is not None and partial.fill.quantity == 40 and partial.fill.price == pytest.approx(10.0)
    )
    full = apply_snapshot(
        partial.order, order(OrderStatus.FILLED, 100, 10.6), timestamp=T0 + timedelta(minutes=1)
    )
    assert full.fill is not None and full.fill.quantity == pytest.approx(60)
    assert full.fill.price == pytest.approx((100 * 10.6 - 40 * 10.0) / 60)
    assert full.fill.fill_id == fill_id_for("jev-S-TEST-en", 100)
    assert full.order.status is OrderStatus.FILLED


def test_anomalies_are_reported_not_applied() -> None:
    filled = order(OrderStatus.FILLED, 100, 10.0)
    after_terminal = apply_snapshot(filled, order(OrderStatus.CANCELLED, 100, 10.0), timestamp=T0)
    assert not after_terminal.applied and after_terminal.anomaly == "update_after_terminal_state"
    partial = order(OrderStatus.PARTIALLY_FILLED, 50, 10.0)
    shrinking = apply_snapshot(partial, order(OrderStatus.PARTIALLY_FILLED, 40, 10.0), timestamp=T0)
    assert not shrinking.applied and shrinking.anomaly == "cumulative_quantity_decreased"
    invalid = apply_snapshot(partial, order(OrderStatus.REJECTED), timestamp=T0)
    assert not invalid.applied and invalid.anomaly is not None
    duplicate = apply_snapshot(filled, filled, timestamp=T0)
    assert not duplicate.applied and duplicate.anomaly is None


def test_decision_ids_are_deterministic_and_namespaced() -> None:
    assert digest("a", 1) == digest("a", 1) != digest("a", 2)
    assert make_feature_id("ns", "TEST", T0, "1", "h") == make_feature_id("ns", "TEST", T0, "1", "h")
    assert make_prediction_id("ns", "m", "1", "TEST", T0) != make_prediction_id("other", "m", "1", "TEST", T0)
    signal_id = make_signal_id("ns", "strategy", "BRK.B", T0)
    assert signal_id.startswith("S-BRKB-202403041430-")
    assert make_client_order_id(signal_id, OrderIntent.ENTRY) == f"jev-{signal_id}-en"
    assert make_client_order_id(signal_id, OrderIntent.TIME_EXIT, 2) == f"jev-{signal_id}-tx2"
    assert make_decision_id(signal_id).startswith("R-") and make_trade_id(signal_id).startswith("T-")
    assert new_id("run") != new_id("run")


def test_client_order_ids_fit_bracket_leg_limits() -> None:
    signal_id = make_signal_id("x" * 40, "a-long-strategy-name", "VERYLONGSYM", T0)
    cid = make_client_order_id(signal_id, OrderIntent.ENTRY)
    assert len(cid) <= 61  # leaves room for the "-tp"/"-sl" leg suffix within 64 characters
    request = OrderRequest(
        client_order_id=cid,
        symbol="VERYLONGSYM",
        side=Side.BUY,
        quantity=1,
        order_class=OrderClass.BRACKET,
        take_profit_price=11,
        stop_loss_price=9,
        intent=OrderIntent.ENTRY,
    )
    assert request.client_order_id == cid


def test_order_request_validation() -> None:
    with pytest.raises(ValueError):
        OrderRequest(
            client_order_id="bad id", symbol="X", side=Side.BUY, quantity=1, intent=OrderIntent.ENTRY
        )
    with pytest.raises(ValueError):
        OrderRequest(
            client_order_id="ok",
            symbol="X",
            side=Side.BUY,
            quantity=1,
            order_class=OrderClass.BRACKET,
            take_profit_price=9,
            stop_loss_price=11,
            intent=OrderIntent.ENTRY,
        )
