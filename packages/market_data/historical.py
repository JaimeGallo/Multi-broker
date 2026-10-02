"""HistoricalMarketDataAdapter: replays a downloaded dataset through the standard MarketDataAdapter contract.

Bars are streamed lazily (one gzip CSV reader per symbol, merged by time), so a year of minute bars for many
symbols never has to fit in memory. Datasets hold bars only: no quotes, so spreads come from
`costs.default_spread_bps` both in the Signal Engine and in simulated fills.
"""

from __future__ import annotations

import heapq
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime

from packages.common.entities import MarketBar, MarketDataHealth, MarketEvent
from packages.common.enums import ConnectionStatus, Timeframe
from packages.common.errors import DataError
from packages.market_data.base import MarketDataAdapter
from packages.market_data.dataset import Dataset


class HistoricalMarketDataAdapter(MarketDataAdapter):
    name = "historical"

    def __init__(self, dataset: Dataset, *, start: date, end: date) -> None:
        if start < dataset.start or end > dataset.end:
            raise DataError(
                f"requested {start}..{end} is outside dataset {dataset.name} ({dataset.start}..{dataset.end})"
            )
        self._dataset = dataset
        self._start = start
        self._end = end
        self._connected = False
        self._symbols: list[str] = []
        self._last_message_at: datetime | None = None

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def subscribe_bars(self, symbols: Sequence[str], timeframe: Timeframe) -> None:
        if timeframe is not Timeframe.MIN_1:
            raise DataError("datasets hold 1Min bars; coarser decision bars are aggregated by the engine")
        missing = sorted(set(symbols) - set(self._dataset.symbols))
        if missing:
            raise DataError(f"dataset {self._dataset.name} has no data for {missing}")
        for symbol in symbols:
            if symbol not in self._symbols:
                self._symbols.append(symbol)

    async def subscribe_quotes(self, symbols: Sequence[str]) -> None:
        """Datasets have no quotes: nothing to subscribe to (spreads use the configured default)."""

    async def subscribe_trades(self, symbols: Sequence[str]) -> None:
        """Datasets have no trade prints."""

    async def get_historical_bars(
        self, symbol: str, start: datetime, end: datetime, timeframe: Timeframe = Timeframe.MIN_1
    ) -> list[MarketBar]:
        if timeframe is not Timeframe.MIN_1:
            raise DataError("datasets hold 1Min bars only")
        return [b for b in self._dataset.bars(symbol, start.date(), end.date()) if start <= b.start < end]

    async def stream(self) -> AsyncIterator[MarketEvent]:
        if not self._connected:
            raise DataError("historical adapter is not connected")
        readers = [
            ((bar.end, bar.symbol, bar) for bar in self._dataset.bars(symbol, self._start, self._end))
            for symbol in sorted(self._symbols)
        ]
        for when, _, bar in heapq.merge(*readers, key=lambda item: (item[0], item[1])):
            if not self._connected:
                return
            self._last_message_at = when
            yield bar

    async def health(self) -> MarketDataHealth:
        return MarketDataHealth(
            provider=self.name,
            status=ConnectionStatus.CONNECTED if self._connected else ConnectionStatus.DISCONNECTED,
            connected=self._connected,
            last_message_at=self._last_message_at,
            subscribed_symbols=tuple(self._symbols),
            detail=f"dataset {self._dataset.name} ({self._dataset.version})",
        )
