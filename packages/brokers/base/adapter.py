"""BrokerAdapter: the only place where a broker's native API is touched.

Contract (see docs/BROKER_ARCHITECTURE.md §2):
- `client_order_id` is the canonical id everywhere; adapters map it to the broker's native id.
- `submit_order` returns the broker's view (normally ACKNOWLEDGED) and raises `OrderRejected`,
  `DuplicateClientOrderId`, `AmbiguousSubmission` (outcome unknown) or `BrokerUnavailable` (not sent).
  It never reports a fill the broker has not confirmed.
- `cancel_order` on a terminal order is a no-op; on an unknown order it raises `OrderNotFound`.
- `get_order` returns None for unknown orders.
- `stream_order_events` delivers normalized events at least once; consumers deduplicate.
- `capabilities.is_paper` must be truthful.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum

from packages.common.entities import (
    AccountSnapshot,
    BrokerCapabilities,
    BrokerHealth,
    InstrumentInfo,
    Order,
    OrderEvent,
    OrderReplace,
    OrderRequest,
    Position,
)


class OrderQueryStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    ALL = "all"


@dataclass(frozen=True)
class ExecutionRequirements:
    bracket: bool = False
    short: bool = False
    fractional: bool = False
    extended_hours: bool = False


class BrokerAdapter(ABC):
    name: str

    @property
    @abstractmethod
    def capabilities(self) -> BrokerCapabilities: ...

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def health(self) -> BrokerHealth: ...

    @abstractmethod
    async def get_account(self) -> AccountSnapshot: ...

    @abstractmethod
    async def get_positions(self) -> list[Position]: ...

    @abstractmethod
    async def get_orders(self, status: OrderQueryStatus = OrderQueryStatus.OPEN) -> list[Order]: ...

    @abstractmethod
    async def submit_order(self, order: OrderRequest) -> Order: ...

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> None: ...

    @abstractmethod
    async def replace_order(self, client_order_id: str, changes: OrderReplace) -> Order: ...

    @abstractmethod
    async def get_order(self, client_order_id: str) -> Order | None: ...

    @abstractmethod
    def stream_order_events(self) -> AsyncIterator[OrderEvent]: ...

    @abstractmethod
    async def get_instrument(self, symbol: str) -> InstrumentInfo: ...
