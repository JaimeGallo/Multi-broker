"""Failure handling and recovery (docs/EXECUTION.md §8): ambiguous submissions, restarts, reconciliation,
unexpected positions and database failures."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from apps.trading_engine.bootstrap import SimulationOptions, build_simulation, run_simulation
from packages.brokers.base import OrderQueryStatus
from packages.brokers.mock import MockBrokerAdapter
from packages.brokers.router import BrokerRouter
from packages.common.calendar import RegularHoursCalendar
from packages.common.clock import SimulatedClock
from packages.common.config import BrokerSection, CostsSection, ExecutionSection, MockBrokerSection
from packages.common.costs import CostModel
from packages.common.entities import Order, OrderRequest
from packages.common.enums import OrderIntent, OrderStatus, OrderType, Side, TimeInForce
from packages.common.events import EventBus, Topics
from packages.execution.engine import BrokerExecutionEngine
from packages.execution.order_store import InMemoryOrderStore
from packages.risk.kill_switch import KillSwitchReason
from tests.helpers import SESSION_DAY, SESSION_OPEN, make_bar, sim_config, sqlite_url

SYMBOLS = ["MOCKA", "MOCKB"]


# ---------------------------------------------------------------- ambiguous submissions


class ExecutionHarness:
    def __init__(self) -> None:
        self.clock = SimulatedClock(SESSION_OPEN + timedelta(minutes=30))
        self.broker = MockBrokerAdapter(
            MockBrokerSection(), CostModel(CostsSection()), self.clock, RegularHoursCalendar()
        )
        self.store = InMemoryOrderStore()
        self.events: list[Any] = []
        bus = EventBus()

        async def record(topic: str, payload: Any) -> None:
            self.events.append((topic, payload))

        bus.subscribe("", record)
        self.engine = BrokerExecutionEngine(
            router=BrokerRouter(BrokerSection(), {"mock": self.broker}),
            store=self.store,
            clock=self.clock,
            bus=bus,
            config=ExecutionSection(),
            strategy="test",
        )

    async def start(self) -> None:
        await self.broker.connect()
        self.broker.on_bar(make_bar(symbol="TEST", start=SESSION_OPEN + timedelta(minutes=29)))


def request(cid: str = "jev-S-TEST-1-en") -> OrderRequest:
    return OrderRequest(
        client_order_id=cid,
        symbol="TEST",
        side=Side.BUY,
        quantity=10,
        intent=OrderIntent.ENTRY,
        signal_id="S-TEST-1",
    )


async def test_ambiguous_submission_before_acceptance_is_retried_with_the_same_id() -> None:
    h = ExecutionHarness()
    await h.start()
    h.broker.inject_failure("timeout_before_accept")
    order = await h.engine.submit(request())
    assert order.status is OrderStatus.ACKNOWLEDGED
    assert [o.client_order_id for o in await h.broker.get_orders(OrderQueryStatus.ALL)] == [
        request().client_order_id
    ]


async def test_ambiguous_submission_after_acceptance_is_adopted_not_resent() -> None:
    h = ExecutionHarness()
    await h.start()
    h.broker.inject_failure("timeout_after_accept")
    order = await h.engine.submit(request())
    assert order.status is OrderStatus.ACKNOWLEDGED
    assert len(await h.broker.get_orders(OrderQueryStatus.ALL)) == 1
    assert h.engine.broker_submissions == 0  # the second attempt never happened: the order was found


async def test_unknown_outcome_after_all_attempts_is_an_error_not_a_guess() -> None:
    h = ExecutionHarness()
    await h.start()
    h.broker.inject_failure("timeout_before_accept", times=2)
    order = await h.engine.submit(request())
    assert order.status is OrderStatus.ERROR and order.reject_reason == "submission_outcome_unknown"


async def test_duplicate_requests_are_idempotent() -> None:
    h = ExecutionHarness()
    await h.start()
    first = await h.engine.submit(request())
    second = await h.engine.submit(request())
    assert first.client_order_id == second.client_order_id
    assert h.engine.broker_submissions == 1
    assert len(await h.broker.get_orders(OrderQueryStatus.ALL)) == 1


async def test_duplicate_broker_events_are_applied_once() -> None:
    h = ExecutionHarness()
    await h.start()
    await h.engine.submit(request())
    h.broker.on_bar(make_bar(symbol="TEST", start=SESSION_OPEN + timedelta(minutes=31)))
    events = h.broker.drain_events()
    fills: list[Any] = []

    async def on_fill(fill: Any) -> None:
        fills.append(fill)

    h.engine.set_fill_listener(on_fill)
    for event in events + events:  # at-least-once delivery
        await h.engine.handle_order_event(event)
    assert len(fills) == 1
    assert (await h.store.get(request().client_order_id)).status is OrderStatus.FILLED
    assert sum(1 for topic, _ in h.events if topic == Topics.ORDER_EVENT) == len(events)


# ---------------------------------------------------------------- restarts & reconciliation


def options(db: Path, **kwargs: Any) -> SimulationOptions:
    return SimulationOptions(
        start=SESSION_DAY, end=SESSION_DAY, database_url=sqlite_url(db), git_commit="test", **kwargs
    )


async def test_restart_with_the_same_broker_reconciles_and_never_duplicates(tmp_path: Path) -> None:
    db = tmp_path / "restart.db"
    config = sim_config(SYMBOLS)
    first = await build_simulation(config, options(db, run_id="run_restart", max_events=900))
    crashed = await run_simulation(config, options(db, run_id="run_restart", max_events=900), context=first)
    assert crashed.status == "INTERRUPTED"
    broker = first.broker
    open_before = {p.symbol: p.quantity for p in await broker.get_positions()}
    assert len(open_before) == 2, "precondition: the crash must happen with open positions"
    orders_before = {o.client_order_id for o in await broker.get_orders(OrderQueryStatus.ALL)}

    # Same run id (same deterministic ids), same database, same simulated exchange; the feed is replayed.
    second = await build_simulation(config, options(db, run_id="run_restart", broker=broker))
    report = await run_simulation(config, options(db, run_id="run_restart", broker=broker), context=second)
    reconciliation = report.summary["reconciliation"]
    assert reconciliation["clean"], reconciliation
    assert not report.summary["kill_switch"]["engaged"], report.summary["kill_switch"]
    assert report.status == "COMPLETED"
    await broker.connect()  # engine.stop() disconnected it
    assert await broker.get_positions() == []
    broker_orders = await broker.get_orders(OrderQueryStatus.ALL)
    assert orders_before <= {o.client_order_id for o in broker_orders}
    assert len(broker_orders) == len({o.client_order_id for o in broker_orders})
    assert report.summary["open_trades"] == 0


async def test_order_created_but_never_sent_is_cancelled_not_resent(tmp_path: Path) -> None:
    db = tmp_path / "created.db"
    config = sim_config(SYMBOLS)
    ctx = await build_simulation(config, options(db, run_id="run_created"))
    orphan = Order(
        client_order_id="jev-S-MOCKA-202403041500-ORPHAN00-en",
        broker="mock",
        symbol="MOCKA",
        side=Side.BUY,
        quantity=10,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
        intent=OrderIntent.ENTRY,
        signal_id="S-MOCKA-202403041500-ORPHAN00",
        created_at=SESSION_OPEN,
        updated_at=SESSION_OPEN,
    )
    await ctx.store.save(orphan)  # crashed after the write-ahead record, before the broker call
    try:
        report = await ctx.engine.start()
        assert report.never_submitted == [orphan.client_order_id]
        assert report.clean
        stored = await ctx.store.get(orphan.client_order_id)
        assert (
            stored is not None
            and stored.status is OrderStatus.CANCELLED
            and stored.reject_reason == "never_submitted"
        )
        assert await ctx.broker.get_order(orphan.client_order_id) is None
        assert not ctx.kill_switch.engaged
    finally:
        await ctx.database.dispose()


async def test_unexpected_position_engages_the_kill_switch_and_blocks_entries(tmp_path: Path) -> None:
    db = tmp_path / "unexpected.db"
    config = sim_config(["MOCKA"])
    ctx = await build_simulation(config, options(db, run_id="run_unexpected"))
    await ctx.broker.connect()
    ctx.broker.force_position("MOCKA", 25, 100.0)  # e.g. a manual trade outside the platform
    report = await run_simulation(config, options(db, run_id="run_unexpected"), context=ctx)
    kill = report.summary["kill_switch"]
    assert kill["engaged"] and kill["reason"] == KillSwitchReason.UNEXPECTED_POSITION.value
    assert report.summary["reconciliation"]["unexpected_positions"] == {"MOCKA": 25}
    assert report.summary["counters"]["entries_submitted"] == 0
    assert report.summary["counters"]["risk_rejections"].get("kill_switch", 0) > 0


async def test_database_failure_blocks_orders_and_engages_the_kill_switch(tmp_path: Path) -> None:
    db = tmp_path / "dbfail.db"
    config = sim_config(SYMBOLS)
    ctx = await build_simulation(config, options(db, run_id="run_dbfail"))

    async def broken(rows: Any, **kwargs: Any) -> None:
        if rows:
            raise RuntimeError("disk full")

    ctx.repository.insert_risk_decisions = broken  # type: ignore[method-assign]
    report = await run_simulation(config, options(db, run_id="run_dbfail"), context=ctx)
    kill = report.summary["kill_switch"]
    assert kill["engaged"] and kill["reason"] == KillSwitchReason.DATABASE_UNAVAILABLE.value
    assert report.broker_submissions == 0  # the audit barrier failed before the first order existed
    assert (
        report.summary["counters"]["no_trade"].get("audit_unavailable", 0)
        + report.summary["counters"]["risk_rejections"].get("kill_switch", 0)
        > 0
    )


@pytest.mark.parametrize("persisted_reason", [KillSwitchReason.MANUAL])
async def test_engaged_kill_switch_survives_a_restart(
    tmp_path: Path, persisted_reason: KillSwitchReason
) -> None:
    db = tmp_path / "persisted.db"
    config = sim_config(["MOCKA"])
    ctx = await build_simulation(config, options(db, run_id="run_persisted"))
    await ctx.kill_switch.engage(
        persisted_reason, "operator stop", by="ana"
    )  # persisted by the engine listener
    assert (await ctx.repository.get_state("run_persisted:kill_switch") or {}).get("engaged") is True
    await ctx.database.dispose()
    restarted = await build_simulation(config, options(db, run_id="run_persisted"))
    report = await run_simulation(config, options(db, run_id="run_persisted"), context=restarted)
    assert report.summary["kill_switch"]["engaged"]
    assert report.summary["kill_switch"]["engaged_by"] == "ana"
    assert report.broker_submissions == 0


async def test_ack_timeout_reconciles_or_flags_the_order() -> None:
    h = ExecutionHarness()
    await h.start()
    known = await h.broker.submit_order(
        request("jev-S-TEST-2-en")
    )  # the broker has it, we never saw the answer
    lost = Order.from_request(request("jev-S-TEST-3-en"), broker="mock", at=h.clock.now())
    for local in (Order.from_request(request("jev-S-TEST-2-en"), broker="mock", at=h.clock.now()), lost):
        local.status = OrderStatus.SUBMITTED
        local.submitted_at = h.clock.now()
        await h.store.save(local)
    anomalies: list[str] = []

    async def on_anomaly(kind: str, detail: str, details: dict[str, Any]) -> None:
        anomalies.append(kind)

    h.engine.set_anomaly_listener(on_anomaly)
    await h.engine.check_timeouts(h.clock.now() + timedelta(seconds=5))  # before ack_timeout_seconds: nothing
    assert (await h.store.get(lost.client_order_id)).status is OrderStatus.SUBMITTED
    await h.engine.check_timeouts(h.clock.now() + timedelta(seconds=20))
    assert (await h.store.get(known.client_order_id)).status is OrderStatus.ACKNOWLEDGED
    flagged = await h.store.get(lost.client_order_id)
    assert (
        flagged is not None and flagged.status is OrderStatus.ERROR and flagged.reject_reason == "ack_timeout"
    )
    assert anomalies == ["ack_timeout"]


async def test_broker_disconnection_blocks_entries_then_engages_the_kill_switch(tmp_path: Path) -> None:
    db = tmp_path / "disconnected.db"
    config = sim_config(["MOCKA"])
    ctx = await build_simulation(config, options(db, run_id="run_disconnected"))
    original_start = ctx.engine.start

    async def start_then_lose_the_broker() -> Any:
        report = await original_start()
        ctx.broker.set_connected(False)
        return report

    ctx.engine.start = start_then_lose_the_broker  # type: ignore[method-assign]
    report = await run_simulation(config, options(db, run_id="run_disconnected"), context=ctx)
    kill = report.summary["kill_switch"]
    assert kill["engaged"] and kill["reason"] == KillSwitchReason.BROKER_DISCONNECTED.value
    limit = config.kill_switch.broker_disconnect_seconds
    first_check = SESSION_OPEN + timedelta(minutes=1)
    assert kill["engaged_at"] == (first_check + timedelta(seconds=limit)).isoformat().replace("+00:00", "Z")
    assert report.broker_submissions == 0
