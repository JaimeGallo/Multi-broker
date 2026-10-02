"""Execution engine contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from packages.common.entities import Order, OrderReplace, OrderRequest


class ExecutionEngine(ABC):
    """Sends, cancels and replaces orders through a broker. Knows nothing about JEV internals."""

    @abstractmethod
    async def submit(self, order: OrderRequest) -> Order: ...

    @abstractmethod
    async def cancel(self, client_order_id: str) -> None: ...

    @abstractmethod
    async def replace(self, client_order_id: str, changes: OrderReplace) -> Order: ...
