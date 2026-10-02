"""Phase 4 end to end against an in-memory Alpaca: paper broker + live stream + warm-up + restart reconciliation."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from apps.trading_engine.paper import PaperOptions, build_paper, run_paper
from packages.brokers.factory import AlpacaWiring
from packages.common.clock import SimulatedClock
from packages.common.config import AppConfig
from packages.common.enums import TradingMode
from packages.persistence.database import Database
from packages.persistence.repositories import AuditRepository, SqlOrderStore
from packages.pipeline.replay import DecisionVerifier
from tests.fake_alpaca_live import FakeAlpacaLive
from tests.helpers import make_config, sqlite_url

NY = ZoneInfo("America/New_York")
PREVIOUS, TODAY = date(2024, 3, 25), date(2024, 3, 26)
SYMBOLS = ["SPY", "AAPL"]
ENV = {"APCA_API_KEY_ID": "test-key", "APCA_API_SECRET_KEY": "secret"}


def paper_config(db: Path, **extra: Any) -> AppConfig:
    overrides: dict[str, Any] = {
        "trading": {"mode": "paper", "symbols": SYMBOLS},
        "market_data": {"provider": "alpaca", "alpaca": {"feed": "iex"}},
        "broker": {
            "mode": "paper",
            "active": "alpaca",
            "mock": {"enabled": False},
            "alpaca": {"enabled": True},
        },
        "risk": {"allow_short": True},
        "persistence": {"database_url": sqlite_url(db)},
        "logging": {"level": "WARNING"},
        **extra,
    }
    return make_config(overrides)


def options(fake: FakeAlpacaLive, clock: SimulatedClock, minutes: float) -> PaperOptions:
    async def fast_sleep(seconds: float) -> None:
        await asyncio.sleep(0.002)

    return PaperOptions(
        clock=clock,
        max_duration=timedelta(minutes=minutes),
        timer_interval=0.0,
        git_commit="test",
        sleep=fast_sleep,
        wiring=AlpacaWiring(
            environ=ENV,
            trading_transport=fake.transport(),
            data_transport=fake.transport(),
            connector=fake.connector,
        ),
    )


async def drive(fake: FakeAlpacaLive, minutes: int, *, start_index: int = 0) -> None:
    series = {s: fake.regular_bars(s, PREVIOUS, TODAY) for s in SYMBOLS}
    for i in range(start_index, start_index + minutes):
        await fake.advance([series[s][i] for s in SYMBOLS])


async def session(
    fake: FakeAlpacaLive, clock: SimulatedClock, config: AppConfig, *, minutes: int, start_index: int
):
    ctx = await build_paper(config, options(fake, clock, minutes + 1))
    task = asyncio.create_task(run_paper(ctx))
    for _ in range(200):  # until the runner subscribed to the live stream
        if any(s.kind == "data" and s.ready and not s.closed for s in fake.sockets):
            break
        await asyncio.sleep(0.005)
    await drive(fake, minutes, start_index=start_index)
    clock.advance(timedelta(minutes=2))  # past the run's deadline
    return await asyncio.wait_for(task, timeout=30)


async def test_paper_session_trades_reconciles_and_verifies(tmp_path: Path) -> None:
    db = tmp_path / "paper.db"
    open_at = datetime(2024, 3, 26, 9, 30, tzinfo=NY)
    clock = SimulatedClock(open_at - timedelta(seconds=30))
    fake = FakeAlpacaLive(clock)
    config = paper_config(db)

    report = await session(fake, clock, config, minutes=150, start_index=0)
    assert report.status == "COMPLETED", report.result
    assert report.result is not None and report.result.warmup_bars == 2 * 390  # the previous session
    assert report.reconciliation is not None and report.reconciliation["clean"]
    counters = report.summary["counters"]
    assert counters["entries_submitted"] > 0 and counters["fills"] > 0
    # every order the engine sent reached Alpaca as a bracket with our deterministic id; never twice
    entries = [b for b in fake.order_requests if b.get("order_class") == "bracket"]
    assert entries and all(b["client_order_id"].startswith("jev-") for b in entries)
    ids = [b["client_order_id"] for b in fake.order_requests]
    assert len(ids) == len(set(ids))
    assert not fake.rejections  # exits always waited for the protective legs to be cancelled
    assert report.summary["kill_switch"]["engaged"] is False

    database = Database(sqlite_url(db))
    try:
        repo = AuditRepository(database, run_id=report.run_id, mode=TradingMode.PAPER)
        verifier = DecisionVerifier(repo)
        signals = await repo.list_signals(limit=30, run_id=report.run_id)
        assert signals and all([(await verifier.verify_decision(s["signal_id"])).ok for s in signals])
        store = SqlOrderStore(database, mode=TradingMode.PAPER, run_id=report.run_id)
        legs = [o for o in await store.list_all() if o.parent_client_order_id]
        assert legs and all(o.client_order_id.endswith(("-tp", "-sl")) for o in legs)
    finally:
        await database.dispose()

    # restart later the same day: reconciliation finds every broker order and position it expects
    restarted = await session(fake, clock, config, minutes=30, start_index=152)
    assert restarted.status == "COMPLETED", restarted.result
    assert restarted.reconciliation is not None and restarted.reconciliation["clean"]
    assert restarted.summary["kill_switch"]["engaged"] is False
    ids = [b["client_order_id"] for b in fake.order_requests]
    assert len(ids) == len(set(ids))


async def test_streams_reconnect_backfill_and_resync(tmp_path: Path) -> None:
    open_at = datetime(2024, 3, 26, 9, 30, tzinfo=NY)
    clock = SimulatedClock(open_at - timedelta(seconds=30))
    fake = FakeAlpacaLive(clock)
    ctx = await build_paper(paper_config(tmp_path / "p.db"), options(fake, clock, 40))
    task = asyncio.create_task(run_paper(ctx))
    for _ in range(200):
        if any(s.kind == "data" and s.ready and not s.closed for s in fake.sockets):
            break
        await asyncio.sleep(0.005)
    await drive(fake, 10)
    fake.drop_connections("data")
    fake.drop_connections("trading")
    series = {s: fake.regular_bars(s, PREVIOUS, TODAY) for s in SYMBOLS}
    clock.advance_to(series["SPY"][14].end + timedelta(seconds=1))  # 5 minutes pass while disconnected
    for _ in range(300):
        live = [s for s in fake.sockets if s.ready and not s.closed]
        if len({s.kind for s in live}) == 2 and ctx.market_data.reconnections:
            break
        await asyncio.sleep(0.005)
    assert ctx.market_data.reconnections == 1
    assert ctx.market_data.backfilled_bars >= 2 * 4  # the missed minutes came back by REST
    await drive(fake, 10, start_index=15)
    clock.advance(timedelta(minutes=30))
    report = await asyncio.wait_for(task, timeout=30)
    assert report.status == "COMPLETED", report.result


async def test_live_endpoint_and_wrong_mode_are_refused(tmp_path: Path) -> None:
    from packages.common.errors import ConfigError, LiveTradingNotAllowed, SafetyError

    clock = SimulatedClock(datetime(2024, 3, 26, 9, 0, tzinfo=NY))
    fake = FakeAlpacaLive(clock)
    live_url = paper_config(tmp_path / "x.db", broker={
        "mode": "paper", "active": "alpaca", "mock": {"enabled": False},
        "alpaca": {"enabled": True, "trading_url": "https://api.alpaca.markets"},
    })  # fmt: skip
    with pytest.raises(SafetyError, match="not the paper endpoint"):
        await build_paper(live_url, options(fake, clock, 1))
    not_paper = paper_config(tmp_path / "x.db", broker={
        "mode": "paper", "active": "alpaca", "mock": {"enabled": False}, "alpaca": {"enabled": True, "paper": False},
    })  # fmt: skip
    with pytest.raises(SafetyError, match="paper must be true"):
        await build_paper(not_paper, options(fake, clock, 1))
    with pytest.raises(LiveTradingNotAllowed):
        await build_paper(
            paper_config(tmp_path / "x.db", trading={"mode": "live", "symbols": SYMBOLS}),
            options(fake, clock, 1),
        )
    with pytest.raises(ConfigError, match=r"alpaca-paper\.yaml"):
        await build_paper(make_config({"trading": {"mode": "paper"}}), options(fake, clock, 1))


async def _check(fake: FakeAlpacaLive, clock: SimulatedClock, tmp_path: Path):  # type: ignore[no-untyped-def]
    from apps.trading_engine.paper_check import paper_check

    return await paper_check(
        paper_config(tmp_path / "check.db"), clock=clock, wait_seconds=5, poll=0.005,
        database_url=sqlite_url(tmp_path / "check.db"),
        wiring=AlpacaWiring(environ=ENV, trading_transport=fake.transport(), data_transport=fake.transport(),
                            connector=fake.connector),
    )  # fmt: skip


async def test_paper_check_runs_one_full_order_lifecycle(tmp_path: Path) -> None:
    clock = SimulatedClock(datetime(2024, 3, 26, 11, 0, tzinfo=NY))
    fake = FakeAlpacaLive(clock)
    series = fake.regular_bars("SPY", TODAY, TODAY)  # the same series the REST reference price comes from
    fake.last_price["SPY"] = series[89].close
    done = asyncio.Event()

    async def market() -> None:
        minute = 90  # 11:00
        while not done.is_set():
            await asyncio.sleep(0.01)
            await fake.advance([series[minute]], settle=0.002)
            minute += 1

    driver = asyncio.create_task(market())
    try:
        report = await _check(fake, clock, tmp_path)
    finally:
        done.set()
        await driver
    assert report.passed, report.steps
    assert [s.name for s in report.steps] == [
        "connect", "market open", "account clean", "reference price", "bracket entry filled",
        "protective legs live", "legs cancelled", "position closed", "account flat",
    ]  # fmt: skip
    sent = fake.order_requests
    assert [b["client_order_id"][-3:] for b in sent] == ["-en", "-tx"] and sent[0]["order_class"] == "bracket"
    assert fake.positions["SPY"].qty == 0 and report.entry_price and report.exit_price
    assert report.order_events >= 6 and not report.cleanup


async def test_paper_check_refuses_a_symbol_that_is_already_in_use(tmp_path: Path) -> None:
    from tests.fake_alpaca_live import FakePosition

    clock = SimulatedClock(datetime(2024, 3, 26, 11, 0, tzinfo=NY))
    fake = FakeAlpacaLive(clock)
    fake.positions["SPY"] = FakePosition(qty=5, avg=100.0)
    report = await _check(fake, clock, tmp_path)
    assert not report.passed and report.steps[-1].name == "account clean"
    assert "already has 1 position" in report.steps[-1].detail and not fake.order_requests
    evening = SimulatedClock(datetime(2024, 3, 26, 18, 0, tzinfo=NY))
    closed = await _check(FakeAlpacaLive(evening), evening, tmp_path)
    assert not closed.passed and closed.steps[-1].name == "market open"
