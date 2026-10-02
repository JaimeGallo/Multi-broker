from __future__ import annotations

from datetime import date, timedelta

import pytest

from packages.brokers.base import ExecutionRequirements
from packages.brokers.mock import MockBrokerAdapter
from packages.brokers.router import BrokerRouter
from packages.common.calendar import RegularHoursCalendar
from packages.common.clock import SimulatedClock
from packages.common.config import BrokerSection, CostsSection, MockBrokerSection, SyntheticMarketSection
from packages.common.costs import CostModel
from packages.common.entities import Fill, MarketBar, Position
from packages.common.enums import AssetClass, ConnectionStatus, OrderIntent, Side, Timeframe
from packages.common.errors import BrokerUnavailable, SafetyError
from packages.market_data.aggregator import BarAggregator
from packages.market_data.mock import MockMarketDataAdapter
from packages.market_data.synthetic import SyntheticMarket
from packages.risk.kill_switch import KillSwitch, KillSwitchReason, KillSwitchState, TradingControls
from packages.risk.portfolio import PortfolioTracker
from tests.helpers import SESSION_DAY, SESSION_OPEN, make_bar

CALENDAR = RegularHoursCalendar()


# ---------------------------------------------------------------- synthetic market & mock feed


def generate(symbols: list[str], config: SyntheticMarketSection | None = None) -> dict[str, list[MarketBar]]:
    market = SyntheticMarket(config or SyntheticMarketSection(), CALENDAR)
    out: dict[str, list[MarketBar]] = {s: [] for s in symbols}
    for _, bars, _ in market.generate(symbols, SESSION_DAY, SESSION_DAY + timedelta(days=1)):
        for bar in bars:
            out[bar.symbol].append(bar)
    return out


def test_synthetic_market_is_deterministic_and_per_symbol_independent() -> None:
    alone = generate(["MOCKA"])["MOCKA"]
    together = generate(["MOCKA", "MOCKB"])
    assert alone == together["MOCKA"]
    assert generate(["MOCKA"])["MOCKA"] == alone
    assert alone != together["MOCKB"]
    other_seed = generate(["MOCKA"], SyntheticMarketSection(seed=8))["MOCKA"]
    assert other_seed[-1].close != alone[-1].close


def test_synthetic_bars_respect_sessions_and_ohlc() -> None:
    bars = generate(["MOCKA"])["MOCKA"]
    assert len(bars) == 2 * 390
    for bar in bars:
        session = CALENDAR.session_for(bar.start)
        assert session is not None and bar.end <= session.close
        assert bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
        assert bar.volume > 0


async def test_mock_feed_injects_anomalies_without_changing_the_path() -> None:
    noisy_cfg = SyntheticMarketSection(duplicate_rate=0.05, gap_rate=0.02)
    clean = MockMarketDataAdapter(SyntheticMarketSection(), CALENDAR, start=SESSION_DAY, end=SESSION_DAY)
    noisy = MockMarketDataAdapter(noisy_cfg, CALENDAR, start=SESSION_DAY, end=SESSION_DAY)
    results = []
    for adapter in (clean, noisy):
        await adapter.connect()
        await adapter.subscribe_bars(["MOCKA"], Timeframe.MIN_1)
        results.append([e for e in [event async for event in adapter.stream()] if isinstance(e, MarketBar)])
    clean_bars, noisy_bars = results
    assert len(clean_bars) == 390
    assert len({b.start for b in noisy_bars}) < 390  # gaps
    assert len(noisy_bars) != len({b.start for b in noisy_bars})  # duplicates
    clean_by_start = {b.start: b.close for b in clean_bars}
    assert all(clean_by_start[b.start] == b.close for b in noisy_bars)


# ---------------------------------------------------------------- aggregation


def test_aggregator_builds_clock_aligned_bars() -> None:
    aggregator = BarAggregator(Timeframe.MIN_1, Timeframe.MIN_5)
    closes = [100.0, 101.0, 99.0, 102.0, 103.0]
    outputs = [
        aggregator.add(make_bar(start=SESSION_OPEN + timedelta(minutes=i), close=c))
        for i, c in enumerate(closes)
    ]
    assert all(out == [] for out in outputs[:-1])
    (bar,) = outputs[-1]
    assert bar.timeframe is Timeframe.MIN_5 and bar.start == SESSION_OPEN
    assert bar.open == 100.0 and bar.close == 103.0
    assert bar.high == pytest.approx(103.05) and bar.low == pytest.approx(98.95)
    assert bar.volume == 5_000


def test_aggregator_flushes_a_partial_bucket() -> None:
    aggregator = BarAggregator(Timeframe.MIN_1, Timeframe.MIN_5)
    assert aggregator.add(make_bar(start=SESSION_OPEN, close=100)) == []
    completed = aggregator.add(make_bar(start=SESSION_OPEN + timedelta(minutes=6), close=101))
    assert len(completed) == 1 and completed[0].start == SESSION_OPEN and completed[0].volume == 1_000
    with pytest.raises(ValueError):
        BarAggregator(Timeframe.MIN_5, Timeframe.MIN_1)


# ---------------------------------------------------------------- router


def mock_broker() -> MockBrokerAdapter:
    return MockBrokerAdapter(
        MockBrokerSection(), CostModel(CostsSection()), SimulatedClock(SESSION_OPEN), CALENDAR
    )


async def test_router_selects_by_asset_class_and_capabilities() -> None:
    broker = mock_broker()
    router = BrokerRouter(BrokerSection(), {"mock": broker})
    assert router.primary() is broker
    chosen = await router.select_broker(
        "X", AssetClass.US_EQUITY, "s", ExecutionRequirements(bracket=True, short=True)
    )
    assert chosen is broker
    with pytest.raises(BrokerUnavailable):
        await router.select_broker("X", AssetClass.US_EQUITY, "s", ExecutionRequirements(fractional=True))
    with pytest.raises(BrokerUnavailable):
        await router.select_broker("ES", AssetClass.FUTURES, "s")
    routed = BrokerRouter(BrokerSection(routing={AssetClass.FUTURES: "ibkr"}), {"mock": broker})
    assert routed.name_for(AssetClass.FUTURES) == "ibkr"
    with pytest.raises(BrokerUnavailable):
        routed.get("ibkr")
    statuses = await routed.statuses()
    assert statuses["alpaca"] is ConnectionStatus.NOT_CONFIGURED


# ---------------------------------------------------------------- kill switch & controls


async def test_kill_switch_engages_once_and_resets_only_manually() -> None:
    changes: list[tuple[KillSwitchState, str]] = []

    async def listener(state: KillSwitchState, event: str) -> None:
        changes.append((state, event))

    switch = KillSwitch(SimulatedClock(SESSION_OPEN), on_change=listener)
    assert await switch.engage(KillSwitchReason.STALE_DATA, "no data")
    assert not await switch.engage(KillSwitchReason.DAILY_LOSS, "later")
    assert switch.state.reason is KillSwitchReason.STALE_DATA
    with pytest.raises(SafetyError):
        await switch.reset(by="system", note="auto")
    with pytest.raises(SafetyError):
        await switch.reset(by="ana", note=" ")
    await switch.reset(by="ana", note="feed restored")
    assert not switch.engaged
    assert [event for _, event in changes] == ["KILL_SWITCH_ENGAGED", "KILL_SWITCH_RESET"]
    restored = KillSwitch(SimulatedClock(SESSION_OPEN), state=changes[0][0])
    assert restored.engaged  # survives restarts through its persisted state


async def test_trading_controls_pause_and_resume() -> None:
    controls = TradingControls(SimulatedClock(SESSION_OPEN))
    await controls.pause(by="ana", note="news")
    assert controls.paused and controls.state.changed_by == "ana"
    await controls.resume(by="ana")
    assert not controls.paused


# ---------------------------------------------------------------- portfolio


def test_portfolio_tracks_fills_marks_and_exposure() -> None:
    portfolio = PortfolioTracker("mock")
    portfolio.apply_fill(
        Fill(fill_id="1", client_order_id="a", broker="mock", symbol="X", side=Side.BUY, quantity=10, price=100.0,
             fee=1.0, timestamp=SESSION_OPEN, intent=OrderIntent.ENTRY)
    )  # fmt: skip
    portfolio.mark("X", 110.0)
    assert portfolio.open_positions() == {"X": 10}
    assert portfolio.gross_exposure() == pytest.approx(1_100.0)
    realized = portfolio.apply_fill(
        Fill(fill_id="2", client_order_id="b", broker="mock", symbol="X", side=Side.SELL, quantity=10, price=110.0,
             timestamp=SESSION_OPEN, intent=OrderIntent.TIME_EXIT)
    )  # fmt: skip
    assert realized == pytest.approx(100.0) and portfolio.open_positions() == {}
    assert portfolio.snapshot(SESSION_OPEN) is None  # no account yet: no invented snapshot
    portfolio.load_positions(
        [Position(broker="mock", symbol="Y", quantity=-5, average_entry_price=20, market_price=21)]
    )
    assert portfolio.open_positions() == {"Y": -5} and portfolio.net_exposure() == pytest.approx(-105.0)


def test_calendar_sessions() -> None:
    assert CALENDAR.session_on(date(2024, 3, 9)) is None  # Saturday
    session = CALENDAR.session_on(SESSION_DAY)
    assert session is not None and session.open == SESSION_OPEN
    assert CALENDAR.is_open(SESSION_OPEN) and not CALENDAR.is_open(SESSION_OPEN - timedelta(minutes=1))
    assert CALENDAR.day_start(SESSION_OPEN).hour == 5  # midnight New York in UTC (EST)
