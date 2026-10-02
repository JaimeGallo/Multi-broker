"""`paper-check`: one complete order lifecycle against the Alpaca PAPER account, with the market open.

It validates the part of the real-time path that a model without edge never exercises: a bracket entry of a few
shares, its fill and protective legs reported by `trade_updates`, the legs cancelled, the position closed with a
market order, and the account left flat. Everything goes through the same execution engine and audit store as
`run` (orders, order events and a system event with the result land in the database).

Safety: paper endpoint only (the adapter refuses anything else); it refuses to start if the symbol already has a
position or open orders; whatever happens, it cancels what it opened and flattens what it bought.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from apps.trading_engine.paper import check_paper_config
from packages.brokers.alpaca.adapter import AlpacaBrokerAdapter
from packages.brokers.base import OrderQueryStatus
from packages.brokers.factory import AlpacaWiring, build_brokers
from packages.brokers.router import BrokerRouter
from packages.common.calendar import RegularHoursCalendar
from packages.common.clock import Clock, SystemClock
from packages.common.config import AppConfig
from packages.common.costs import CostModel
from packages.common.entities import Order, OrderRequest
from packages.common.enums import OrderClass, OrderIntent, OrderStatus, Side, TradingMode
from packages.common.errors import ConfigError
from packages.common.events import EventBus, SystemEvent, Topics
from packages.common.run import RunContext, detect_git_commit
from packages.execution.engine import BrokerExecutionEngine
from packages.market_data.alpaca_history import credentials
from packages.market_data.alpaca_stream import AlpacaMarketDataAdapter
from packages.market_data.dataset import calendar_from_days
from packages.persistence.database import Database
from packages.persistence.recorder import AuditRecorder
from packages.persistence.repositories import AuditRepository, SqlOrderStore

MIN_MINUTES_TO_CLOSE = 10.0
BRACKET_DISTANCE = 0.01  # take profit / stop loss 1 % away: the check closes the position long before either


class CheckFailed(Exception):
    pass


@dataclass
class CheckStep:
    name: str
    ok: bool
    detail: str
    seconds: float


@dataclass
class CheckReport:
    run_id: str
    symbol: str
    quantity: int
    passed: bool = False
    reference_price: float | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    order_events: int = 0
    steps: list[CheckStep] = field(default_factory=list)
    cleanup: list[str] = field(default_factory=list)

    @property
    def entry_slippage_bps(self) -> float | None:
        if self.entry_price is None or not self.reference_price:
            return None
        return (self.entry_price / self.reference_price - 1.0) * 1e4


async def paper_check(
    config: AppConfig,
    *,
    symbol: str = "SPY",
    quantity: int = 1,
    database_url: str | None = None,
    clock: Clock | None = None,
    wiring: AlpacaWiring | None = None,
    wait_seconds: float = 45.0,
    poll: float = 0.25,
    sleep: Callable[[float], Any] = asyncio.sleep,
    on_step: Callable[[CheckStep], None] | None = None,
) -> CheckReport:
    check_paper_config(config)
    if quantity < 1:
        raise ConfigError("quantity must be at least 1 share")
    clock = clock or SystemClock()
    wiring = wiring or AlpacaWiring()
    costs = CostModel(config.costs)
    adapters = build_brokers(
        config, clock=clock, calendar=RegularHoursCalendar(config.trading.exchange_timezone), cost_model=costs,
        alpaca=wiring,
    )  # fmt: skip
    broker = adapters["alpaca"]
    assert isinstance(broker, AlpacaBrokerAdapter)
    key, secret = credentials(wiring.environ)
    market = AlpacaMarketDataAdapter(
        config.market_data.alpaca, clock, key=key, secret=secret, transport=wiring.data_transport
    )
    run = RunContext.create(
        config, mode=TradingMode.PAPER, started_at=clock.now(), git_commit=detect_git_commit()
    )
    report = CheckReport(run_id=run.run_id, symbol=symbol, quantity=quantity)
    database = Database(database_url or config.persistence.database_url or "sqlite+aiosqlite:///:memory:")
    await database.create_all()
    bus = EventBus()
    repository = AuditRepository(database, run_id=run.run_id, mode=TradingMode.PAPER)
    AuditRecorder(repository, config.persistence, immediate_writes=True).attach(bus)
    store = SqlOrderStore(database, mode=TradingMode.PAPER, run_id=run.run_id)
    execution = BrokerExecutionEngine(
        router=BrokerRouter(config.broker, adapters), store=store, clock=clock, bus=bus,
        config=config.execution, strategy=run.strategy,
    )  # fmt: skip
    tag = clock.now().strftime("%Y%m%d%H%M%S")
    entry_id, exit_id = f"jev-chk{tag}-en", f"jev-chk{tag}-tx"
    pump: asyncio.Task[None] | None = None

    async def step(name: str, action: Callable[[], Any]) -> Any:
        started = time.monotonic()
        try:
            detail = await action()
        except CheckFailed as exc:
            record(CheckStep(name, False, str(exc), time.monotonic() - started))
            raise
        except Exception as exc:
            record(CheckStep(name, False, f"{type(exc).__name__}: {exc}", time.monotonic() - started))
            raise CheckFailed(str(exc)) from exc
        record(CheckStep(name, True, str(detail or ""), time.monotonic() - started))
        return detail

    def record(item: CheckStep) -> None:
        report.steps.append(item)
        if on_step is not None:
            on_step(item)

    async def wait_for(cid: str, statuses: set[OrderStatus], what: str) -> Order:
        deadline = time.monotonic() + wait_seconds
        while True:
            order = await store.get(cid)
            if order is not None and order.status in statuses:
                return order
            if order is not None and order.is_terminal:
                raise CheckFailed(f"{what}: order ended {order.status.value} ({order.reject_reason or ''})")
            if time.monotonic() > deadline:
                state = order.status.value if order is not None else "unknown"
                raise CheckFailed(f"{what}: still {state} after {wait_seconds:.0f} s")
            await sleep(poll)

    async def pump_events() -> None:
        async for event in broker.stream_order_events():
            await execution.handle_order_event(event)
            report.order_events += 1

    try:

        async def connect() -> str:
            await broker.connect()
            account = await broker.get_account()
            health = await broker.health()
            skew = abs((health.server_time - clock.now()).total_seconds()) if health.server_time else 0.0
            return f"paper account {account.account_ref}, trade_updates stream ready, clock skew {skew:.2f} s"

        await step("connect", connect)

        async def market_open() -> str:
            now = clock.now()
            today = now.astimezone(RegularHoursCalendar(config.trading.exchange_timezone).timezone).date()
            days = await broker.calendar(today - timedelta(days=7), today + timedelta(days=7))
            calendar = calendar_from_days(days, today - timedelta(days=7), today + timedelta(days=7))
            session = calendar.session_for(now)
            if session is None:
                raise CheckFailed("the market is closed: run paper-check during the regular session")
            if session.minutes_to_close(now) < MIN_MINUTES_TO_CLOSE:
                raise CheckFailed("less than 10 minutes to the close")
            return f"open, {session.minutes_to_close(now):.0f} min to the close"

        await step("market open", market_open)

        async def account_clean() -> str:
            positions = [p for p in await broker.get_positions() if p.symbol == symbol]
            orders = [o for o in await broker.get_orders(OrderQueryStatus.OPEN) if o.symbol == symbol]
            if positions or orders:
                raise CheckFailed(
                    f"{symbol} already has {len(positions)} position(s) and {len(orders)} open order(s): "
                    "close them in the Alpaca dashboard, or choose another --symbol"
                )
            return f"no position and no open order in {symbol}"

        await step("account clean", account_clean)

        async def reference() -> str:
            now = clock.now()
            bars = await market.get_historical_bars(symbol, now - timedelta(minutes=30), now)
            if not bars:
                raise CheckFailed(f"no recent {config.market_data.alpaca.feed} bar for {symbol}")
            report.reference_price = bars[-1].close
            return f"last {config.market_data.alpaca.feed} close {bars[-1].close:.2f} at {bars[-1].end:%H:%M} UTC"

        await step("reference price", reference)
        pump = asyncio.create_task(pump_events(), name="paper-check-events")
        price = report.reference_price
        assert price is not None

        async def entry() -> str:
            request = OrderRequest(
                client_order_id=entry_id, symbol=symbol, side=Side.BUY, quantity=quantity,
                order_class=OrderClass.BRACKET, take_profit_price=round(price * (1 + BRACKET_DISTANCE), 2),
                stop_loss_price=round(price * (1 - BRACKET_DISTANCE), 2), intent=OrderIntent.ENTRY,
            )  # fmt: skip
            sent = await execution.submit(request)
            if sent.status in (OrderStatus.REJECTED, OrderStatus.ERROR):
                raise CheckFailed(f"entry {sent.status.value}: {sent.reject_reason}")
            filled = await wait_for(entry_id, {OrderStatus.FILLED}, "entry fill")
            report.entry_price = filled.average_fill_price
            return f"bought {quantity} {symbol} at {filled.average_fill_price:.2f} ({entry_id})"

        await step("bracket entry filled", entry)

        async def legs() -> str:
            deadline = time.monotonic() + wait_seconds
            while True:
                parent = await store.get(entry_id)
                ids = parent.leg_client_order_ids if parent is not None else []
                found = [await store.get(i) for i in ids]
                if len(ids) == 2 and all(o is not None for o in found):
                    break
                if time.monotonic() > deadline:
                    raise CheckFailed(f"protective legs not reported (known: {ids})")
                await sleep(poll)
            return ", ".join(
                f"{o.client_order_id} {o.order_type.value} {o.limit_price or o.stop_price}"
                for o in found
                if o
            )

        await step("protective legs live", legs)
        parent = await store.get(entry_id)
        assert parent is not None
        leg_ids = list(parent.leg_client_order_ids)

        async def cancel_legs() -> str:
            for leg_id in leg_ids:
                await execution.cancel(leg_id)
            for leg_id in leg_ids:
                await wait_for(leg_id, {OrderStatus.CANCELLED}, f"cancel {leg_id}")
            return "take profit and stop loss cancelled"

        await step("legs cancelled", cancel_legs)

        async def close() -> str:
            request = OrderRequest(
                client_order_id=exit_id, symbol=symbol, side=Side.SELL, quantity=quantity,
                intent=OrderIntent.TIME_EXIT,
            )  # fmt: skip
            sent = await execution.submit(request)
            if sent.status in (OrderStatus.REJECTED, OrderStatus.ERROR):
                raise CheckFailed(f"exit {sent.status.value}: {sent.reject_reason}")
            filled = await wait_for(exit_id, {OrderStatus.FILLED}, "exit fill")
            report.exit_price = filled.average_fill_price
            return f"sold {quantity} {symbol} at {filled.average_fill_price:.2f} ({exit_id})"

        await step("position closed", close)

        async def flat() -> str:
            deadline = time.monotonic() + wait_seconds
            while True:
                positions = [p for p in await broker.get_positions() if p.symbol == symbol]
                orders = [o for o in await broker.get_orders(OrderQueryStatus.OPEN) if o.symbol == symbol]
                if not positions and not orders:
                    return f"no {symbol} position or open order left at Alpaca"
                if time.monotonic() > deadline:
                    raise CheckFailed(f"still {len(positions)} position(s) and {len(orders)} open order(s)")
                await sleep(poll)

        await step("account flat", flat)
        report.passed = True
    except CheckFailed:
        report.passed = False
    finally:
        with contextlib.suppress(Exception):
            await _cleanup(broker, store, symbol, (entry_id, exit_id), report, tag)
        with contextlib.suppress(Exception):
            await bus.publish(
                Topics.SYSTEM_EVENT,
                SystemEvent(
                    timestamp=clock.now(), level="INFO" if report.passed else "ERROR", component="paper_check",
                    event_type="paper_check.passed" if report.passed else "paper_check.failed",
                    message=f"paper-check {symbol} x{quantity}",
                    details={"steps": [s.__dict__ for s in report.steps], "cleanup": report.cleanup,
                             "entry_price": report.entry_price, "exit_price": report.exit_price},
                ),
            )  # fmt: skip
            await bus.flush()
        if pump is not None:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pump
        with contextlib.suppress(Exception):
            await broker.disconnect()
        with contextlib.suppress(Exception):
            await market.disconnect()
        await database.dispose()
    return report


async def _cleanup(
    broker: AlpacaBrokerAdapter,
    store: SqlOrderStore,
    symbol: str,
    ids: tuple[str, str],
    report: CheckReport,
    tag: str,
) -> None:
    """Leave the account as found: cancel what this check opened, flatten what it bought."""
    if report.passed:
        return
    for order in await store.list_open():
        if order.client_order_id.startswith(f"jev-chk{tag}"):
            with contextlib.suppress(Exception):
                await broker.cancel_order(order.client_order_id)
                report.cleanup.append(f"cancelled {order.client_order_id}")
    entry = await store.get(ids[0])
    if entry is None or entry.filled_quantity <= 0:
        return
    exit_order = await store.get(ids[1])
    sold = exit_order.filled_quantity if exit_order is not None else 0.0
    remaining = int(entry.filled_quantity - sold)
    if remaining <= 0:
        return
    await asyncio.sleep(1.0)  # let the leg cancellations release the shares
    await broker.submit_order(
        OrderRequest(client_order_id=f"jev-chk{tag}-kx", symbol=symbol, side=Side.SELL, quantity=remaining,
                     intent=OrderIntent.KILL_EXIT)
    )  # fmt: skip
    report.cleanup.append(f"sent a market sell of {remaining} {symbol} to flatten the check position")


def describe(report: CheckReport) -> list[str]:
    lines = [f"run_id            {report.run_id}"]
    if report.entry_price is not None and report.reference_price is not None:
        slip = report.entry_slippage_bps
        lines.append(
            f"entry             {report.entry_price:.2f} vs reference {report.reference_price:.2f}"
            + (f" ({slip:+.1f} bps)" if slip is not None else "")
        )
    if report.entry_price is not None and report.exit_price is not None:
        pnl = (report.exit_price - report.entry_price) * report.quantity
        lines.append(f"round trip        {pnl:+.2f} USD on {report.quantity} share(s) (paper)")
    lines.append(f"order events      {report.order_events} received from trade_updates")
    for item in report.cleanup:
        lines.append(f"cleanup           {item}")
    lines.append(
        "RESULT            " + ("PASSED: the real order path works end to end" if report.passed else "FAILED")
    )
    return lines
