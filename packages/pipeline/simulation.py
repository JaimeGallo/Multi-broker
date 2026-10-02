"""SimulationRunner: drives the TradingEngine from a recorded or synthetic feed on a simulated clock.

It is the core of the future backtester (phase 3). Events are grouped by the time they become known (a bar at its
`end`, a quote at its `timestamp`) and each group is processed in a fixed order:

1. the clock advances to the group time;
2. the simulated exchange trades through the new bars (fills of orders sent BEFORE those bars started);
3. quotes reach the exchange and the engine;
4. bars reach the engine (one decision per bar);
5. timers run (horizon / end-of-day exits, timeouts, kill switch effects, snapshots).

After every step the exchange's order events are drained into the engine, so the same feed always produces
exactly the same sequence of decisions, orders and fills.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime

from packages.brokers.mock import MockBrokerAdapter
from packages.common.clock import SimulatedClock
from packages.common.entities import MarketBar, MarketEvent, MarketQuote, event_time
from packages.common.enums import Timeframe
from packages.market_data.base import MarketDataAdapter
from packages.pipeline.engine import TradingEngine

log = logging.getLogger(__name__)


@dataclass
class SimulationResult:
    events: int = 0
    groups: int = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    completed: bool = False
    interrupted: bool = False


class SimulationRunner:
    def __init__(
        self,
        *,
        engine: TradingEngine,
        market_data: MarketDataAdapter,
        broker: MockBrokerAdapter,
        clock: SimulatedClock,
        symbols: list[str],
        timeframe: Timeframe,
        pace_seconds: float = 0.0,
        max_events: int | None = None,
    ) -> None:
        self._engine = engine
        self._market = market_data
        self._broker = broker
        self._clock = clock
        self._symbols = list(symbols)
        self._timeframe = timeframe
        self._pace = pace_seconds
        self._max_events = max_events

    async def run(self, *, finalize: bool = True) -> SimulationResult:
        """Process the whole feed. With `max_events` the run stops early WITHOUT finalizing (simulated crash)."""
        await self._market.subscribe_bars(self._symbols, self._timeframe)
        await self._market.subscribe_quotes(self._symbols)
        result = SimulationResult()
        stream = self._market.stream()
        try:
            async for timestamp, events in self._groups(stream, result):
                await self._process(timestamp, events)
                result.groups += 1
                result.first_timestamp = result.first_timestamp or timestamp
                result.last_timestamp = timestamp
                if self._pace > 0:
                    await asyncio.sleep(self._pace)
            result.completed = not result.interrupted
        finally:
            await _close(stream)
        if result.completed and finalize:
            await self._engine.finalize()
        return result

    async def _groups(
        self, stream: AsyncIterator[MarketEvent], result: SimulationResult
    ) -> AsyncIterator[tuple[datetime, list[MarketEvent]]]:
        current: datetime | None = None
        group: list[MarketEvent] = []
        async for event in stream:
            if self._max_events is not None and result.events >= self._max_events:
                result.interrupted = True
                break
            result.events += 1
            when = event_time(event)
            if current is not None and when != current:
                yield current, group
                group = []
            if current is None or when >= current:
                current = when
            group.append(event)
        if current is not None and group:
            yield current, group

    async def _process(self, timestamp: datetime, events: list[MarketEvent]) -> None:
        engine, broker = self._engine, self._broker
        if timestamp > self._clock.now():
            self._clock.advance_to(timestamp)
        bars = [e for e in events if isinstance(e, MarketBar)]
        quotes = [e for e in events if isinstance(e, MarketQuote)]
        others = [e for e in events if not isinstance(e, MarketBar | MarketQuote)]

        for bar in bars:
            broker.on_bar(bar)
        await self._drain()
        for quote in quotes:
            broker.on_quote(quote)
            await engine.handle_market_event(quote)
        for event in others:
            await engine.handle_market_event(event)
        for bar in bars:
            await engine.handle_market_event(bar)
            await self._drain()
        await engine.handle_timer(self._clock.now())
        await self._drain()

    async def _drain(self) -> None:
        # Handling an event can trigger new orders (exits, cancels) which produce more events: loop until quiet.
        while True:
            events = self._broker.drain_events()
            if not events:
                return
            for event in events:
                await self._engine.handle_order_event(event)


async def _close(stream: AsyncIterator[MarketEvent]) -> None:
    close = getattr(stream, "aclose", None)
    if close is not None:
        try:
            await close()
        except Exception:
            log.exception("could not close the market data stream")
