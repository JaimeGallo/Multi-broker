"""RealtimeRunner: drives the TradingEngine from live market data, live broker events and a wall-clock timer.

The engine is the same one the backtests use; only the event sources change. Three concurrent loops feed it:
market data, order events (one per broker) and a timer (horizon / end-of-day exits, timeouts, health, kill switch,
snapshots). A single lock serializes every engine call, so handlers never interleave.

Warm-up: before the live stream starts, recent bars come from REST (the previous session and today so far).
They are older than `data_quality.max_bar_delay_seconds`, so the engine only uses them to fill its feature
window: it never trades on them.

Stopping: at `stop_at` (normally a few minutes after the close, once the end-of-day exits are confirmed), after
`max_duration`, on Ctrl+C, or when a source fails for good (e.g. wrong keys). The engine is always finalized and
stopped (audit flush, broker and stream disconnection).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from packages.brokers.base import BrokerAdapter
from packages.common.clock import Clock
from packages.common.entities import MarketBar
from packages.common.enums import Timeframe
from packages.market_data.base import MarketDataAdapter
from packages.pipeline.engine import TradingEngine

log = logging.getLogger(__name__)

StatusCallback = Callable[["RealtimeResult"], None]


@dataclass
class RealtimeResult:
    started_at: datetime
    stopped_at: datetime | None = None
    warmup_bars: int = 0
    market_events: int = 0
    bars: int = 0
    order_events: int = 0
    timer_ticks: int = 0
    stop_reason: str = ""
    error: str | None = None
    last_bar_at: datetime | None = None
    per_symbol_bars: dict[str, int] = field(default_factory=dict)


class RealtimeRunner:
    def __init__(
        self,
        *,
        engine: TradingEngine,
        market_data: MarketDataAdapter,
        brokers: Sequence[BrokerAdapter],
        clock: Clock,
        symbols: list[str],
        timeframe: Timeframe,
        stop_at: datetime | None = None,
        max_duration: timedelta | None = None,
        timer_interval: float = 2.0,
        warmup: Callable[[], Awaitable[list[MarketBar]]] | None = None,
        on_status: StatusCallback | None = None,
        status_every: timedelta = timedelta(minutes=5),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._engine = engine
        self._market = market_data
        self._brokers = list(brokers)
        self._clock = clock
        self._symbols = list(symbols)
        self._timeframe = timeframe
        self._stop_at = stop_at
        self._max_duration = max_duration
        self._interval = timer_interval
        self._warmup = warmup
        self._on_status = on_status
        self._status_every = status_every
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._result: RealtimeResult | None = None

    def request_stop(self, reason: str = "requested") -> None:
        if self._result is not None and not self._result.stop_reason:
            self._result.stop_reason = reason
        self._stop.set()

    async def run(self) -> RealtimeResult:
        """The engine must already be started (connected and reconciled)."""
        result = RealtimeResult(started_at=self._clock.now())
        self._result = result
        deadline = self._stop_at
        if self._max_duration is not None:
            limit = result.started_at + self._max_duration
            deadline = min(deadline, limit) if deadline is not None else limit
        if self._warmup is not None:
            for bar in await self._warmup():
                async with self._lock:
                    await self._engine.handle_market_event(bar)
                result.warmup_bars += 1
        await self._market.subscribe_bars(self._symbols, self._timeframe)
        await self._market.subscribe_quotes(self._symbols)
        tasks = [
            asyncio.create_task(self._market_loop(result), name="market-loop"),
            asyncio.create_task(self._timer_loop(result, deadline), name="timer-loop"),
            *(
                asyncio.create_task(self._order_loop(broker, result), name=f"orders-{broker.name}")
                for broker in self._brokers
            ),
        ]
        try:
            await self._stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            result.stopped_at = self._clock.now()
            result.stop_reason = result.stop_reason or "cancelled"
        return result

    async def _market_loop(self, result: RealtimeResult) -> None:
        try:
            async for event in self._market.stream():
                async with self._lock:
                    await self._engine.handle_market_event(event)
                result.market_events += 1
                if isinstance(event, MarketBar):
                    result.bars += 1
                    result.last_bar_at = event.end
                    result.per_symbol_bars[event.symbol] = result.per_symbol_bars.get(event.symbol, 0) + 1
            self._fail(result, "market data stream ended")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("market data loop failed")
            self._fail(result, f"market data: {exc}")

    async def _order_loop(self, broker: BrokerAdapter, result: RealtimeResult) -> None:
        try:
            async for event in broker.stream_order_events():
                async with self._lock:
                    await self._engine.handle_order_event(event)
                result.order_events += 1
            if not self._stop.is_set():
                self._fail(result, f"{broker.name} order stream ended")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("order event loop failed")
            self._fail(result, f"{broker.name} orders: {exc}")

    async def _timer_loop(self, result: RealtimeResult, deadline: datetime | None) -> None:
        next_status = self._clock.now() + self._status_every
        try:
            while not self._stop.is_set():
                now = self._clock.now()
                async with self._lock:
                    await self._engine.handle_timer(now)
                result.timer_ticks += 1
                if self._on_status is not None and now >= next_status:
                    next_status = now + self._status_every
                    self._on_status(result)
                if deadline is not None and now >= deadline:
                    result.stop_reason = result.stop_reason or "end of the trading window"
                    self._stop.set()
                    return
                await self._sleep(self._interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("timer loop failed")
            self._fail(result, f"timer: {exc}")

    def _fail(self, result: RealtimeResult, reason: str) -> None:
        if not self._stop.is_set():
            result.error = result.error or reason
            result.stop_reason = result.stop_reason or reason
            self._stop.set()
