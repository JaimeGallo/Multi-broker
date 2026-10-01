"""MockMarketDataAdapter: serves the deterministic synthetic market through the standard contract."""

from __future__ import annotations

import random
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime

from packages.common.calendar import MarketCalendar
from packages.common.config import SyntheticMarketSection
from packages.common.entities import MarketBar, MarketDataHealth, MarketEvent
from packages.common.enums import ConnectionStatus, Timeframe
from packages.common.errors import DataError
from packages.market_data.base import MarketDataAdapter
from packages.market_data.synthetic import SyntheticMarket


class MockMarketDataAdapter(MarketDataAdapter):
    """Synthetic bars and closing quotes for the configured date range.

    Optional anomaly injection (missing bars, duplicates, bad prints) exercises the data-quality layer.
    Anomalies use their own RNG so enabling them never changes the underlying price path.
    The mock does not emit trade prints.
    """

    name = "mock"

    def __init__(
        self,
        config: SyntheticMarketSection,
        calendar: MarketCalendar,
        *,
        start: date,
        end: date,
        timeframe: Timeframe = Timeframe.MIN_1,
    ) -> None:
        self._cfg = config
        self._market = SyntheticMarket(config, calendar, timeframe)
        self._start = start
        self._end = end
        self._tf = timeframe
        self._connected = False
        self._bar_symbols: list[str] = []
        self._quote_symbols: set[str] = set()
        self._trade_symbols: set[str] = set()
        self._last_message_at: datetime | None = None
        self._anomaly_rng = random.Random(config.seed * 7919 + 1)
        self._history: dict[str, list[MarketBar]] = {}

    @property
    def source(self) -> str:
        return self._market.source

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    def _check_timeframe(self, timeframe: Timeframe) -> None:
        if timeframe != self._tf:
            raise DataError(f"mock feed produces {self._tf.value} bars only, not {timeframe.value}")

    async def subscribe_bars(self, symbols: Sequence[str], timeframe: Timeframe) -> None:
        self._check_timeframe(timeframe)
        for symbol in symbols:
            if symbol not in self._bar_symbols:
                self._bar_symbols.append(symbol)

    async def subscribe_quotes(self, symbols: Sequence[str]) -> None:
        self._quote_symbols.update(symbols)

    async def subscribe_trades(self, symbols: Sequence[str]) -> None:
        self._trade_symbols.update(symbols)

    async def get_historical_bars(
        self, symbol: str, start: datetime, end: datetime, timeframe: Timeframe = Timeframe.MIN_1
    ) -> list[MarketBar]:
        self._check_timeframe(timeframe)
        if symbol not in self._history:
            self._history[symbol] = [
                bar for _, bars, _ in self._market.generate([symbol], self._start, self._end) for bar in bars
            ]
        return [bar for bar in self._history[symbol] if start <= bar.start < end]

    async def stream(self) -> AsyncIterator[MarketEvent]:
        if not self._connected:
            raise DataError("mock market data adapter is not connected")
        symbols = sorted(set(self._bar_symbols) | self._quote_symbols)
        for timestamp, bars, quotes in self._market.generate(symbols, self._start, self._end):
            if not self._connected:
                return
            for bar in bars:
                if bar.symbol in self._bar_symbols:
                    for event in self._with_anomalies(bar):
                        yield event
            for quote in quotes:
                if quote.symbol in self._quote_symbols:
                    yield quote
            self._last_message_at = timestamp

    def _with_anomalies(self, bar: MarketBar) -> list[MarketBar]:
        cfg, rng = self._cfg, self._anomaly_rng
        if cfg.gap_rate and rng.random() < cfg.gap_rate:
            return []
        emitted = [bar]
        if cfg.outlier_rate and rng.random() < cfg.outlier_rate:
            bad_close = round(bar.close * (1.25 if rng.random() < 0.5 else 0.8), 2)
            emitted = [
                bar.model_copy(
                    update={"close": bad_close, "high": max(bar.high, bad_close), "low": min(bar.low, bad_close)}
                )
            ]
        if cfg.duplicate_rate and rng.random() < cfg.duplicate_rate:
            emitted.append(emitted[-1])
        return emitted

    async def health(self) -> MarketDataHealth:
        return MarketDataHealth(
            provider=self.name,
            status=ConnectionStatus.CONNECTED if self._connected else ConnectionStatus.DISCONNECTED,
            connected=self._connected,
            last_message_at=self._last_message_at,
            subscribed_symbols=tuple(self._bar_symbols),
        )
