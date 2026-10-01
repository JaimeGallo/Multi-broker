"""Market data adapter contract. Adapters emit NORMALIZED events only (see packages.common.entities)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from datetime import datetime

from packages.common.entities import MarketBar, MarketDataHealth, MarketEvent
from packages.common.enums import Timeframe


class MarketDataAdapter(ABC):
    """Provider-agnostic market data source.

    Contract:
    - every event is a MarketBar / MarketQuote / MarketTrade with timezone-aware UTC timestamps;
    - `stream()` yields events in time order;
    - reconnection, buffering and backfill after a disconnect are the adapter's responsibility.
    """

    name: str

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def subscribe_quotes(self, symbols: Sequence[str]) -> None: ...

    @abstractmethod
    async def subscribe_trades(self, symbols: Sequence[str]) -> None: ...

    @abstractmethod
    async def subscribe_bars(self, symbols: Sequence[str], timeframe: Timeframe) -> None: ...

    @abstractmethod
    async def get_historical_bars(
        self, symbol: str, start: datetime, end: datetime, timeframe: Timeframe = Timeframe.MIN_1
    ) -> list[MarketBar]:
        """Bars with `start <= bar.start < end`, ordered by time."""

    @abstractmethod
    def stream(self) -> AsyncIterator[MarketEvent]: ...

    @abstractmethod
    async def health(self) -> MarketDataHealth: ...
