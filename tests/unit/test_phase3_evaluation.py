"""Phase 3 building blocks: time splits, evaluation statistics, the moving-average baseline, audit backoff and the
order store cache."""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

from packages.analytics.evaluation import bootstrap_mean_ci, daily_pnl, model_summary, paired_difference
from packages.backtesting.splits import monthly_folds, walk_forward
from packages.common.calendar import RegularHoursCalendar
from packages.common.config import PersistenceSection
from packages.common.entities import Order, Trade
from packages.common.enums import (
    Direction,
    MarketRegime,
    OrderIntent,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
    TradingMode,
)
from packages.common.events import EventBus, SystemEvent, Topics
from packages.jev.baselines import MovingAverageJEVModel
from packages.persistence.database import Database
from packages.persistence.models import SystemEventRow
from packages.persistence.recorder import AuditRecorder
from packages.persistence.repositories import AuditRepository, SqlOrderStore
from tests.helpers import SESSION_OPEN, make_features, sqlite_url

CALENDAR = RegularHoursCalendar()


def sessions(start: date, end: date) -> list[date]:
    return [s.day for s in CALENDAR.sessions_between(start, end)]


# ---------------------------------------------------------------- splits


def test_monthly_folds_cover_every_session_once() -> None:
    days = sessions(date(2024, 1, 2), date(2024, 4, 3))
    folds = monthly_folds(days)
    assert [f.name for f in folds] == ["2024-01", "2024-02", "2024-03"]  # 3 April sessions join March
    assert sum(f.sessions for f in folds) == len(days)
    assert folds[-1].end == date(2024, 4, 3)
    for previous, current in pairwise(folds):
        assert previous.end < current.start


def test_walk_forward_never_overlaps_and_respects_the_embargo() -> None:
    days = sessions(date(2024, 1, 2), date(2024, 6, 28))
    splits = walk_forward(days, train_sessions=40, test_sessions=20, embargo_sessions=2)
    assert len(splits) >= 4
    for split in splits:
        assert split.train_end < split.test_start
        between = [d for d in days if split.train_end < d < split.test_start]
        assert len(between) == 2  # embargo sessions are in neither window
    for previous, current in pairwise(splits):
        assert previous.test_start < current.test_start
    with pytest.raises(ValueError):
        walk_forward(days, train_sessions=0, test_sessions=5)


# ---------------------------------------------------------------- statistics


def trade(day: date, pnl: float, direction: Direction = Direction.LONG, symbol: str = "SPY") -> Trade:
    exit_time = SESSION_OPEN.replace(year=day.year, month=day.month, day=day.day) + timedelta(hours=2)
    return Trade(
        trade_id=f"T-{day}-{pnl}", signal_id="S", broker="mock", symbol=symbol, direction=direction, quantity=10,
        entry_time=exit_time - timedelta(minutes=15), entry_price=100, entry_reference_price=100,
        exit_time=exit_time, exit_price=100, exit_reference_price=100, exit_reason=OrderIntent.TIME_EXIT,
        gross_pnl=pnl, fees=0.0, net_pnl=pnl, model_pnl=pnl, execution_shortfall=0.0, entry_slippage_bps=0,
        exit_slippage_bps=0, mae_bps=0, mfe_bps=0, holding_minutes=15, model_name="m", model_version="1",
        market_regime=MarketRegime.RANGE, confidence=0.5, probability=0.6,
    )  # fmt: skip


def test_daily_pnl_counts_idle_sessions_as_zero() -> None:
    days = sessions(date(2024, 3, 4), date(2024, 3, 8))
    daily = daily_pnl(
        [trade(days[0], 10), trade(days[0], -4), trade(days[2], 5)], CALENDAR.trading_date, days
    )
    assert list(daily.values()) == [6, 0, 5, 0, 0]


def test_bootstrap_is_seeded_and_brackets_the_mean() -> None:
    values = [1.0, -2.0, 3.0, 0.5, -1.0, 2.0, 4.0, -3.0]
    ci = bootstrap_mean_ci(values)
    assert ci == bootstrap_mean_ci(values)
    assert ci is not None and ci[0] < sum(values) / len(values) < ci[1]
    assert bootstrap_mean_ci([1.0]) is None


def test_model_summary_splits_long_and_short_and_folds() -> None:
    days = sessions(date(2024, 3, 25), date(2024, 4, 5))
    trades = [trade(days[0], 10), trade(days[1], -5, Direction.SHORT, "AAPL"), trade(days[-1], 3)]
    summary = model_summary(
        trades, trading_date=CALENDAR.trading_date, sessions=days, start_equity=100_000,
        fold_of=lambda d: f"{d:%Y-%m}",
    )  # fmt: skip
    assert summary["trades"] == 3 and summary["net_pnl"] == 8
    assert summary["long"]["trades"] == 2 and summary["short"]["net_pnl"] == -5
    assert summary["folds"] == {"2024-03": 5.0, "2024-04": 3.0} and summary["positive_folds"] == 2
    assert summary["by_symbol"] == {"AAPL": -5.0, "SPY": 13.0}
    assert summary["sessions"] == len(days)


def test_paired_difference_uses_common_sessions() -> None:
    a = {date(2024, 1, 2): 5.0, date(2024, 1, 3): 7.0, date(2024, 1, 4): 6.0}
    b = {date(2024, 1, 2): 1.0, date(2024, 1, 3): 2.0, date(2024, 1, 5): 100.0}
    result = paired_difference(a, b)
    assert result["sessions"] == 2 and result["mean_daily_difference"] == pytest.approx(4.5)
    assert result["ci95"] is not None and result["ci95"][0] > 0


# ---------------------------------------------------------------- moving-average baseline


def test_moving_average_baseline_follows_the_ema_cross() -> None:
    model = MovingAverageJEVModel(
        namespace="ns", version="1", feature_version="f", horizon_minutes=15, bar_minutes=1
    )
    up = model.predict(make_features({"ema_12_dist": 0.001, "ema_26_dist": 0.003, "realized_vol_30": 0.001}))
    down = model.predict(
        make_features({"ema_12_dist": 0.003, "ema_26_dist": 0.001, "realized_vol_30": 0.001})
    )
    flat = model.predict(
        make_features({"ema_12_dist": 0.002, "ema_26_dist": 0.00201, "realized_vol_30": 0.001})
    )
    assert up.direction is Direction.LONG and down.direction is Direction.SHORT
    assert flat.direction is Direction.NO_TRADE
    assert up.expected_return > 0 > down.expected_return


# ---------------------------------------------------------------- audit backoff and order store cache


async def test_recorder_backs_off_while_the_database_fails(tmp_path: Path) -> None:
    database = Database(sqlite_url(tmp_path / "audit.db"))
    await database.create_all()
    repo = AuditRepository(database, run_id="r", mode=TradingMode.BACKTEST)
    attempts = 0
    original = repo.insert_system_events

    async def failing(rows: Any, **kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("database down")

    repo.insert_system_events = failing  # type: ignore[method-assign]
    bus = EventBus()
    errors: list[str] = []

    async def on_error(topic: str, exc: BaseException) -> None:
        errors.append(topic)

    bus.set_error_handler(on_error)
    AuditRecorder(repo, PersistenceSection(batch_size=10), immediate_writes=False).attach(bus)
    event = SystemEvent(timestamp=SESSION_OPEN, level="INFO", component="t", event_type="e", message="m")
    for _ in range(100):
        await bus.publish(Topics.SYSTEM_EVENT, event)
    assert attempts <= 5  # 10, 20, 40, 80: no retry on every event
    repo.insert_system_events = original  # type: ignore[method-assign]
    await bus.flush()  # the barrier always retries; everything buffered is written at once
    assert await repo.count(SystemEventRow) == 100
    await database.dispose()


def order(cid: str, status: OrderStatus, signal: str = "S-1") -> Order:
    return Order(
        client_order_id=cid, broker="mock", symbol="SPY", side=Side.BUY, quantity=1, order_type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY, intent=OrderIntent.ENTRY, status=status, signal_id=signal,
        created_at=SESSION_OPEN, updated_at=SESSION_OPEN,
    )  # fmt: skip


async def test_order_store_cache_is_write_through_and_scoped(tmp_path: Path) -> None:
    database = Database(sqlite_url(tmp_path / "orders.db"))
    await database.create_all()
    store = SqlOrderStore(database, mode=TradingMode.BACKTEST, run_id="run-a")
    await store.save(order("a-1", OrderStatus.ACKNOWLEDGED))
    await store.save(order("a-2", OrderStatus.FILLED, "S-2"))
    other = SqlOrderStore(database, mode=TradingMode.BACKTEST, run_id="run-b")
    await other.save(order("b-1", OrderStatus.ACKNOWLEDGED))

    copy = await store.get("a-1")
    assert copy is not None
    copy.status = OrderStatus.CANCELLED  # mutating a returned copy never changes the store
    assert (await store.get("a-1")).status is OrderStatus.ACKNOWLEDGED  # type: ignore[union-attr]
    assert [o.client_order_id for o in await store.list_open()] == ["a-1"]
    assert [o.client_order_id for o in await store.list_by_signal("S-2")] == ["a-2"]
    assert await store.get("b-1") is None  # another run's orders are out of scope

    restarted = SqlOrderStore(database, mode=TradingMode.BACKTEST, run_id="run-a")  # loads from the database
    assert sorted(o.client_order_id for o in await restarted.list_all()) == ["a-1", "a-2"]
    await restarted.save(order("a-1", OrderStatus.FILLED))
    assert await restarted.list_open() == []
    await database.dispose()
