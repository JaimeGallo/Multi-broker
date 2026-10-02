"""Durable order records. `save` must be durable when it returns: orders are written BEFORE they are sent."""

from __future__ import annotations

from typing import Protocol

from packages.common.entities import Order


class OrderStore(Protocol):
    async def get(self, client_order_id: str) -> Order | None: ...

    async def save(self, order: Order) -> None: ...

    async def list_open(self, broker: str | None = None) -> list[Order]: ...

    async def list_by_signal(self, signal_id: str) -> list[Order]: ...

    async def list_all(self) -> list[Order]: ...


class InMemoryOrderStore:
    """Test/dev store. Keeps copies so callers can never mutate stored state by accident."""

    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}

    async def get(self, client_order_id: str) -> Order | None:
        order = self._orders.get(client_order_id)
        return order.snapshot() if order is not None else None

    async def save(self, order: Order) -> None:
        stored = order.model_copy(deep=True)
        stored.legs = []
        self._orders[order.client_order_id] = stored

    async def list_open(self, broker: str | None = None) -> list[Order]:
        return [
            o.snapshot()
            for o in self._orders.values()
            if not o.is_terminal and (broker is None or o.broker == broker)
        ]

    async def list_by_signal(self, signal_id: str) -> list[Order]:
        return [o.snapshot() for o in self._orders.values() if o.signal_id == signal_id]

    async def list_all(self) -> list[Order]:
        return [o.snapshot() for o in self._orders.values()]
