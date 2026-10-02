"""MarketDataAdapter contract: normalized events, UTC timestamps, time order, history semantics."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, timedelta

import pytest

from packages.common.calendar import RegularHoursCalendar
from packages.common.config import SyntheticMarketSection
from packages.common.entities import MarketBar, MarketQuote, event_time
from packages.common.enums import Timeframe
from packages.common.errors import DataError
from packages.market_data.base import MarketDataAdapter
from packages.market_data.mock import MockMarketDataAdapter
from tests.helpers import SESSION_DAY, SESSION_OPEN

CALENDAR = RegularHoursCalendar()


def mock_adapter() -> MarketDataAdapter:
    return MockMarketDataAdapter(SyntheticMarketSection(), CALENDAR, start=SESSION_DAY, end=SESSION_DAY)


@pytest.fixture(params=[mock_adapter], ids=["mock"])
def factory(request: pytest.FixtureRequest) -> Callable[[], MarketDataAdapter]:
    return request.param


async def test_stream_requires_connection(factory: Callable[[], MarketDataAdapter]) -> None:
    adapter = factory()
    with pytest.raises(DataError):
        await anext(adapter.stream())
    assert not (await adapter.health()).connected


async def test_stream_is_normalized_and_time_ordered(factory: Callable[[], MarketDataAdapter]) -> None:
    adapter = factory()
    await adapter.connect()
    await adapter.subscribe_bars(["MOCKA", "MOCKB"], Timeframe.MIN_1)
    await adapter.subscribe_quotes(["MOCKA"])
    events = [event async for event in adapter.stream()]
    times = [event_time(e) for e in events]
    assert times == sorted(times)
    assert all(t.tzinfo is not None and t.utcoffset() == timedelta(0) for t in times)
    bars = [e for e in events if isinstance(e, MarketBar)]
    quotes = [e for e in events if isinstance(e, MarketQuote)]
    assert {b.symbol for b in bars} == {"MOCKA", "MOCKB"} and {q.symbol for q in quotes} == {"MOCKA"}
    assert all(q.ask > q.bid > 0 for q in quotes)
    health = await adapter.health()
    assert health.connected and health.last_message_at == times[-1]


async def test_unsupported_timeframe_is_refused(factory: Callable[[], MarketDataAdapter]) -> None:
    adapter = factory()
    with pytest.raises(DataError):
        await adapter.subscribe_bars(["MOCKA"], Timeframe.MIN_5)


async def test_historical_bars_are_half_open_and_ordered(factory: Callable[[], MarketDataAdapter]) -> None:
    adapter = factory()
    start = SESSION_OPEN + timedelta(minutes=10)
    bars = await adapter.get_historical_bars("MOCKA", start, start + timedelta(minutes=5))
    assert [b.start for b in bars] == [start + timedelta(minutes=i) for i in range(5)]
    assert all(b.start.tzinfo is UTC or b.start.utcoffset() == timedelta(0) for b in bars)
    streamed = mock_adapter()
    await streamed.connect()
    await streamed.subscribe_bars(["MOCKA"], Timeframe.MIN_1)
    live = {e.start: e for e in [x async for x in streamed.stream()] if isinstance(e, MarketBar)}
    assert all(live[b.start] == b for b in bars)  # history and live feed agree
