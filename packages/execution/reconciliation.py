"""Startup reconciliation: the broker is the source of truth (docs/EXECUTION.md §7).

Rebuilds open orders, positions, exposure and daily PnL after any restart. Discrepancies are never hidden:
they are reported and the trading engine engages the kill switch.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from packages.brokers.base import BrokerAdapter, OrderQueryStatus
from packages.common.entities import Order, Position
from packages.common.enums import OrderStatus
from packages.execution.engine import BrokerExecutionEngine
from packages.execution.order_store import OrderStore

EPSILON = 1e-9


@dataclass
class ReconciliationReport:
    updated_orders: list[str] = field(default_factory=list)
    never_submitted: list[str] = field(default_factory=list)
    missing_at_broker: list[str] = field(default_factory=list)
    adopted_orders: list[str] = field(default_factory=list)
    unknown_broker_orders: list[str] = field(default_factory=list)
    unexpected_positions: dict[str, float] = field(default_factory=dict)
    position_mismatches: dict[str, tuple[float, float]] = field(default_factory=dict)

    @property
    def clean(self) -> bool:
        return not (
            self.missing_at_broker
            or self.unknown_broker_orders
            or self.unexpected_positions
            or self.position_mismatches
        )

    def summary(self) -> dict[str, Any]:
        return {
            "clean": self.clean,
            "updated_orders": self.updated_orders,
            "never_submitted": self.never_submitted,
            "missing_at_broker": self.missing_at_broker,
            "adopted_orders": self.adopted_orders,
            "unknown_broker_orders": self.unknown_broker_orders,
            "unexpected_positions": self.unexpected_positions,
            "position_mismatches": {k: list(v) for k, v in self.position_mismatches.items()},
        }


class Reconciler:
    def __init__(self, *, execution: BrokerExecutionEngine, store: OrderStore) -> None:
        self._execution = execution
        self._store = store

    async def reconcile_orders(self, broker: BrokerAdapter) -> ReconciliationReport:
        report = ReconciliationReport()
        for order in await self._store.list_open(broker=broker.name):
            remote = await broker.get_order(order.client_order_id)
            if remote is None:
                if order.status is OrderStatus.CREATED:
                    # Crashed after persisting but before sending: never resend an old intention automatically.
                    await self._execution.mark_local(order, OrderStatus.CANCELLED, "never_submitted")
                    report.never_submitted.append(order.client_order_id)
                else:
                    await self._execution.mark_local(order, OrderStatus.ERROR, "missing_at_broker")
                    report.missing_at_broker.append(order.client_order_id)
                continue
            merged = await self._execution.adopt_remote(order, remote)
            if (merged.status, merged.filled_quantity) != (order.status, order.filled_quantity):
                report.updated_orders.append(order.client_order_id)

        seen: set[str] = set()
        for remote in await broker.get_orders(OrderQueryStatus.OPEN):
            for candidate in (remote, *remote.legs):
                if candidate.client_order_id in seen:
                    continue
                seen.add(candidate.client_order_id)
                if await self._store.get(candidate.client_order_id) is not None:
                    continue
                parent_id = candidate.parent_client_order_id
                parent = await self._store.get(parent_id) if parent_id else None
                if parent is not None:
                    await self._execution.adopt_leg(candidate, parent)
                    report.adopted_orders.append(candidate.client_order_id)
                else:
                    report.unknown_broker_orders.append(candidate.client_order_id)
        return report

    @staticmethod
    def compare_positions(
        report: ReconciliationReport, expected: Mapping[str, float], broker_positions: Sequence[Position]
    ) -> None:
        actual = {p.symbol: p.quantity for p in broker_positions if abs(p.quantity) > EPSILON}
        for symbol, quantity in actual.items():
            wanted = expected.get(symbol, 0.0)
            if abs(wanted) <= EPSILON:
                report.unexpected_positions[symbol] = quantity
            elif abs(wanted - quantity) > EPSILON:
                report.position_mismatches[symbol] = (wanted, quantity)
        for symbol, wanted in expected.items():
            if abs(wanted) > EPSILON and symbol not in actual:
                report.position_mismatches[symbol] = (wanted, 0.0)

    @staticmethod
    def open_orders(orders: Sequence[Order]) -> list[Order]:
        return [o for o in orders if not o.is_terminal]
