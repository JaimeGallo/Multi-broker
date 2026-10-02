"""Paper trading in real time (phase 4): Alpaca PAPER broker + Alpaca market data stream, wall clock.

`build_paper` assembles the same engine the backtests use, with real-time adapters; `run_paper` connects,
reconciles against the broker, warms the features up with recent bars, trades the regular session and shuts
down cleanly, recording the run (`engine_runs`) and every decision, order and fill in the audit database.

Safety, in order: the mode gate refuses live; the Alpaca adapter refuses any endpoint other than paper-api; the
engine refuses an account that does not report paper; reconciliation engages the kill switch on any order or
position it does not recognize.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from apps.trading_engine.bootstrap import assemble_engine
from packages.brokers.alpaca.adapter import AlpacaBrokerAdapter
from packages.brokers.factory import AlpacaWiring, build_brokers
from packages.brokers.router import BrokerRouter
from packages.common.calendar import RegularHoursCalendar, Session
from packages.common.clock import Clock, SystemClock
from packages.common.config import AppConfig
from packages.common.costs import CostModel
from packages.common.entities import MarketBar
from packages.common.enums import TradingMode
from packages.common.errors import ConfigError
from packages.common.events import EventBus
from packages.common.run import RunContext, detect_git_commit
from packages.common.safety import enforce_mode_gate
from packages.execution.engine import BrokerExecutionEngine
from packages.jev.base import JEVModel
from packages.market_data.alpaca_history import credentials
from packages.market_data.alpaca_stream import AlpacaMarketDataAdapter
from packages.market_data.dataset import calendar_from_days
from packages.persistence.database import Database
from packages.persistence.repositories import AuditRepository
from packages.pipeline.engine import TradingEngine
from packages.pipeline.realtime import RealtimeResult, RealtimeRunner, StatusCallback
from packages.risk.kill_switch import KillSwitch

CALENDAR_DAYS_AROUND = 10


@dataclass
class PaperOptions:
    run_id: str | None = None
    database_url: str | None = None
    max_duration: timedelta | None = None
    stop_after_close: timedelta = timedelta(minutes=3)  # time for the end-of-day exits to be confirmed
    timer_interval: float = 2.0
    on_status: StatusCallback | None = None
    git_commit: str | None = None
    clock: Clock | None = None  # tests: a controllable clock
    wiring: AlpacaWiring = field(default_factory=AlpacaWiring)
    sleep: Callable[[float], Any] | None = None


@dataclass
class PaperContext:
    config: AppConfig
    run: RunContext
    clock: Clock
    calendar: RegularHoursCalendar
    session: Session | None
    bus: EventBus
    market_data: AlpacaMarketDataAdapter
    broker: AlpacaBrokerAdapter
    router: BrokerRouter
    database: Database
    repository: AuditRepository
    model: JEVModel
    kill_switch: KillSwitch
    execution: BrokerExecutionEngine
    engine: TradingEngine
    runner: RealtimeRunner


@dataclass
class PaperReport:
    run_id: str
    namespace: str
    status: str
    session: Session | None
    result: RealtimeResult | None
    summary: dict[str, Any]
    trades: list[Any]
    reconciliation: dict[str, Any] | None


def check_paper_config(config: AppConfig) -> None:
    enforce_mode_gate(config.trading.mode, config.broker.mode)
    if config.trading.mode is not TradingMode.PAPER:
        raise ConfigError("`run` trades in paper mode only: set trading.mode: paper")
    if config.broker.active != "alpaca" or not config.broker.alpaca.enabled:
        raise ConfigError(
            "`run` needs the Alpaca paper broker: use --config config/profiles/alpaca-paper.yaml"
        )
    if config.market_data.provider != "alpaca":
        raise ConfigError("`run` needs real-time Alpaca market data (market_data.provider: alpaca)")
    if config.trading.timeframe.minutes != 1:
        raise ConfigError("the Alpaca stream publishes 1Min bars: keep trading.timeframe at 1Min")


async def build_paper(config: AppConfig, options: PaperOptions) -> PaperContext:
    check_paper_config(config)
    clock = options.clock or SystemClock()
    costs = CostModel(config.costs)
    sleep_kwargs = {"sleep": options.sleep} if options.sleep is not None else {}
    wiring = options.wiring
    if sleep_kwargs:
        wiring.extra = {**wiring.extra, **sleep_kwargs}
    placeholder = RegularHoursCalendar(config.trading.exchange_timezone)
    adapters = build_brokers(config, clock=clock, calendar=placeholder, cost_model=costs, alpaca=wiring)
    broker = adapters["alpaca"]
    assert isinstance(broker, AlpacaBrokerAdapter)
    if len(adapters) != 1:
        raise ConfigError("paper trading runs on the Alpaca broker only: set broker.mock.enabled: false")
    router = BrokerRouter(config.broker, adapters)

    await broker.connect()  # also verifies the paper endpoint, the account and the trade_updates stream
    try:
        today = clock.now().astimezone(placeholder.timezone).date()
        first, last = (
            today - timedelta(days=CALENDAR_DAYS_AROUND),
            today + timedelta(days=CALENDAR_DAYS_AROUND),
        )
        days = await broker.calendar(first, last)
        calendar = calendar_from_days(days, first, last, config.trading.exchange_timezone)
    except BaseException:
        await broker.disconnect()
        raise
    key, secret = credentials(wiring.environ)
    market = AlpacaMarketDataAdapter(
        config.market_data.alpaca, clock, key=key, secret=secret, transport=wiring.data_transport,
        connector=wiring.connector, **sleep_kwargs,
    )  # fmt: skip
    git_commit = options.git_commit if options.git_commit is not None else detect_git_commit()
    run = RunContext.create(
        config, mode=TradingMode.PAPER, started_at=clock.now(), run_id=options.run_id, git_commit=git_commit
    )
    bus = EventBus()
    database = Database(
        options.database_url or config.persistence.database_url or "sqlite+aiosqlite:///:memory:"
    )
    await database.create_all()
    parts = await assemble_engine(
        config=config, run=run, clock=clock, calendar=calendar, bus=bus, market=market, router=router,
        database=database, costs=costs, immediate_writes=True, simulated=False,
    )  # fmt: skip
    session = calendar.session_on(today)
    stop_at = session.close + options.stop_after_close if session is not None else None

    async def warmup() -> list[MarketBar]:
        return await recent_bars(market, calendar, list(config.trading.symbols), clock.now())

    runner_kwargs: dict[str, Any] = {"sleep": options.sleep} if options.sleep is not None else {}
    runner = RealtimeRunner(
        engine=parts.engine, market_data=market, brokers=list(adapters.values()), clock=clock,
        symbols=list(config.trading.symbols), timeframe=config.trading.timeframe, stop_at=stop_at,
        max_duration=options.max_duration, timer_interval=options.timer_interval, warmup=warmup,
        on_status=options.on_status, **runner_kwargs,
    )  # fmt: skip
    return PaperContext(
        config=config, run=run, clock=clock, calendar=calendar, session=session, bus=bus, market_data=market,
        broker=broker, router=router, database=database, repository=parts.repository, model=parts.model,
        kill_switch=parts.kill_switch, execution=parts.execution, engine=parts.engine, runner=runner,
    )  # fmt: skip


async def recent_bars(
    market: AlpacaMarketDataAdapter, calendar: RegularHoursCalendar, symbols: list[str], now: datetime
) -> list[MarketBar]:
    """Regular-session bars from the previous session's open until now: enough history for every feature."""
    today = calendar.trading_date(now)
    sessions = [s for s in calendar.sessions_between(today - timedelta(days=CALENDAR_DAYS_AROUND), today)]
    previous = [s for s in sessions if s.close <= now]
    if not previous:
        return []
    start = previous[-1].open
    bars: list[MarketBar] = []
    for symbol in symbols:
        fetched = await market.get_historical_bars(symbol, start, now)
        bars.extend(b for b in fetched if calendar.is_open(b.start) and b.end <= now)
    return sorted(bars, key=lambda b: (b.end, b.symbol))


def next_session(calendar: RegularHoursCalendar, now: datetime) -> Session | None:
    today = calendar.trading_date(now)
    for session in calendar.sessions_between(today, today + timedelta(days=CALENDAR_DAYS_AROUND)):
        if session.close > now:
            return session
    return None


async def run_paper(ctx: PaperContext) -> PaperReport:
    repo, run = ctx.repository, ctx.run
    status = "FAILED"
    result: RealtimeResult | None = None
    try:
        await repo.register_model_version(ctx.model.metadata, created_at=ctx.clock.now())
        await repo.save_run(run, ctx.config, ctx.model.metadata)
        report = await ctx.engine.start()
        result = await ctx.runner.run()
        status = "FAILED" if result.error else "COMPLETED"
        await ctx.engine.finalize()
        summary = ctx.engine.summary()
        await repo.finish_run(stopped_at=ctx.clock.now(), status=status, summary=_jsonable(summary))
        return PaperReport(
            run_id=run.run_id, namespace=run.namespace, status=status, session=ctx.session, result=result,
            summary=summary, trades=list(ctx.engine.ledger.closed), reconciliation=report.summary(),
        )  # fmt: skip
    except BaseException:
        with contextlib.suppress(Exception):
            await ctx.engine.finalize()
        with contextlib.suppress(Exception):
            await repo.finish_run(stopped_at=ctx.clock.now(), status="INTERRUPTED", summary={})
        raise
    finally:
        with contextlib.suppress(Exception):
            await ctx.engine.stop()
        await ctx.database.dispose()


def _jsonable(summary: dict[str, Any]) -> dict[str, Any]:
    import json

    result: dict[str, Any] = json.loads(json.dumps(summary, default=str))
    return result


def session_label(session: Session | None, calendar: RegularHoursCalendar) -> str:
    if session is None:
        return "no session today"
    tz = calendar.timezone
    return f"{session.open.astimezone(tz):%Y-%m-%d %H:%M}-{session.close.astimezone(tz):%H:%M} {tz.key}"
