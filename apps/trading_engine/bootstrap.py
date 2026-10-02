"""Composition root: builds every component of the engine from the configuration.

Only this module knows the concrete classes. Every other layer depends on contracts, so swapping the mock feed or
broker for Alpaca (phase 4) or IBKR (phase 8) only changes what is constructed here.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from packages.analytics.ledger import TradeLedger
from packages.analytics.metrics import PerformanceReport, compute_performance
from packages.analytics.predictions import PredictionOutcomeTracker
from packages.brokers.base import BrokerAdapter
from packages.brokers.factory import build_brokers
from packages.brokers.mock import MockBrokerAdapter
from packages.brokers.router import BrokerRouter
from packages.common.calendar import RegularHoursCalendar
from packages.common.clock import Clock, SimulatedClock
from packages.common.config import AppConfig
from packages.common.costs import CostModel
from packages.common.entities import Trade
from packages.common.enums import HealthState, TradingMode
from packages.common.errors import ConfigError
from packages.common.events import EventBus
from packages.common.run import RunContext, detect_git_commit
from packages.common.safety import enforce_mode_gate
from packages.data_quality.engine import DataQualityEngine
from packages.execution.engine import BrokerExecutionEngine
from packages.execution.position_manager import PositionManager
from packages.execution.reconciliation import Reconciler
from packages.features.engine import FeatureEngine
from packages.features.spec import FeatureSpec
from packages.jev.base import JEVModel
from packages.jev.registry import build_model
from packages.jev.typesafe import TypeSafeJEVModel
from packages.market_data.aggregator import BarAggregator
from packages.market_data.base import MarketDataAdapter
from packages.market_data.dataset import Dataset, DatasetStore
from packages.market_data.engine import MarketDataEngine
from packages.market_data.historical import HistoricalMarketDataAdapter
from packages.market_data.mock import MockMarketDataAdapter
from packages.market_data.spreads import load_spreads, typical_spreads
from packages.persistence.database import Database
from packages.persistence.recorder import AuditRecorder
from packages.persistence.repositories import AuditRepository, SqlOrderStore
from packages.pipeline.decision import DecisionPipeline
from packages.pipeline.engine import TradingEngine
from packages.pipeline.health import HealthCheckResult, HealthMonitor
from packages.pipeline.simulation import SimulationResult, SimulationRunner
from packages.risk.engine import StandardRiskEngine
from packages.risk.kill_switch import KillSwitch, KillSwitchState, TradingControls, TradingControlState
from packages.risk.portfolio import PortfolioTracker
from packages.risk.sizing import build_sizer
from packages.signals.engine import SignalEngine
from packages.signals.regime import RegimeEngine

MAX_CLOCK_SKEW_SECONDS = 2.0
KILL_SWITCH_KEY = "kill_switch"
CONTROLS_KEY = "trading_controls"


def state_prefix(run: RunContext) -> str:
    """Backtests keep their kill switch / controls per run; paper and shadow share one durable state."""
    return f"{run.namespace}:" if run.mode in (TradingMode.BACKTEST, TradingMode.REPLAY) else ""


class ScopedStateStore:
    """Prefixes the keys of `system_state` (so a backtest can never engage the paper kill switch)."""

    def __init__(self, repository: AuditRepository, prefix: str) -> None:
        self._repo = repository
        self._prefix = prefix

    async def get_state(self, key: str) -> dict[str, Any] | None:
        return await self._repo.get_state(self._prefix + key)

    async def set_state(self, key: str, value: Mapping[str, Any], *, at: datetime) -> None:
        await self._repo.set_state(self._prefix + key, value, at=at)


def build_calendar(config: AppConfig) -> RegularHoursCalendar:
    trading = config.trading
    return RegularHoursCalendar(trading.exchange_timezone, trading.session_open, trading.session_close)


def build_health_monitor(
    *,
    config: AppConfig,
    market_data: MarketDataAdapter,
    router: BrokerRouter,
    database: Database,
    clock: Clock,
    simulated: bool,
) -> HealthMonitor:
    """The checks of spec §42. Any critical FAIL blocks new entries."""
    health = HealthMonitor()
    adapters = list(router.adapters.values())

    async def market_data_connected() -> HealthCheckResult:
        state = await market_data.health()
        return HealthCheckResult(
            "market_data_connected",
            HealthState.OK if state.connected else HealthState.FAIL,
            state.status.value,
        )

    def broker_check(name: str, attribute: str) -> Any:
        async def check() -> HealthCheckResult:
            failing: list[str] = []
            for adapter in adapters:
                state = await adapter.health()
                if not getattr(state, attribute):
                    failing.append(adapter.name)
            return HealthCheckResult(name, HealthState.FAIL if failing else HealthState.OK, ",".join(failing))

        return check

    async def database_available() -> HealthCheckResult:
        ok = await database.ping()
        return HealthCheckResult("database_available", HealthState.OK if ok else HealthState.FAIL)

    async def redis_available() -> HealthCheckResult:
        if config.redis.url is None:
            return HealthCheckResult(
                "redis_available", HealthState.NOT_APPLICABLE, "not configured", critical=False
            )
        return HealthCheckResult(
            "redis_available", HealthState.DEGRADED, "redis arrives in phase 5", critical=False
        )

    async def clock_synchronized() -> HealthCheckResult:
        if simulated:
            return HealthCheckResult("clock_synchronized", HealthState.NOT_APPLICABLE, "simulated clock")
        worst = 0.0
        for adapter in adapters:
            server_time = (await adapter.health()).server_time
            if server_time is not None:
                worst = max(worst, abs((server_time - clock.now()).total_seconds()))
        state = HealthState.OK if worst <= MAX_CLOCK_SKEW_SECONDS else HealthState.FAIL
        return HealthCheckResult("clock_synchronized", state, f"max skew {worst:.2f}s")

    health.register("market_data_connected", market_data_connected)
    health.register("broker_connected", broker_check("broker_connected", "connected"))
    health.register("account_available", broker_check("account_available", "account_available"))
    health.register(
        "order_stream_connected", broker_check("order_stream_connected", "order_stream_connected")
    )
    health.register("database_available", database_available)
    health.register("redis_available", redis_available)
    health.register("clock_synchronized", clock_synchronized)
    return health


@dataclass
class SimulationOptions:
    start: date
    end: date
    run_id: str | None = None
    database_url: str | None = None
    max_events: int | None = None
    pace_seconds: float = 0.0
    broker: MockBrokerAdapter | None = None  # reuse a simulated exchange (restart tests)
    git_commit: str | None = None
    on_session: Callable[[date], None] | None = None  # progress: called when a new session starts


@dataclass
class EngineContext:
    config: AppConfig
    run: RunContext
    clock: SimulatedClock
    calendar: RegularHoursCalendar
    bus: EventBus
    market_data: MarketDataAdapter
    broker: MockBrokerAdapter
    router: BrokerRouter
    database: Database
    repository: AuditRepository
    store: SqlOrderStore
    model: JEVModel
    kill_switch: KillSwitch
    controls: TradingControls
    execution: BrokerExecutionEngine
    engine: TradingEngine
    runner: SimulationRunner
    dataset: Dataset | None = None


@dataclass
class SimulationReport:
    run_id: str
    namespace: str
    status: str
    result: SimulationResult
    summary: dict[str, Any]
    performance: PerformanceReport
    broker_submissions: int
    extra: dict[str, Any] = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)


def load_dataset(config: AppConfig) -> Dataset | None:
    """The dataset a historical simulation replays (None for the synthetic market)."""
    if config.market_data.provider != "historical":
        return None
    name = config.market_data.historical.dataset
    if not name:
        raise ConfigError("market_data.historical.dataset is not set (use --dataset NAME)")
    dataset = DatasetStore(config.market_data.historical.root).load(name)
    missing = sorted(set(config.trading.symbols) - set(dataset.symbols))
    if missing:
        raise ConfigError(f"dataset {name} has no data for {missing}")
    if config.trading.timeframe.minutes != 1:
        raise ConfigError(
            "datasets hold 1Min bars: keep trading.timeframe at 1Min (decision_timeframe may be coarser)"
        )
    return dataset


def with_measured_spreads(config: AppConfig, dataset: Dataset) -> AppConfig:
    """Typical spreads measured for the dataset (`data spreads`), unless disabled; explicit values win.
    The result is part of the configuration recorded with the run, so `verify` uses the same costs."""
    if not config.costs.use_measured_spreads:
        return config
    payload = load_spreads(dataset)
    if payload is None:
        return config
    merged = {**typical_spreads(payload), **config.costs.spread_by_symbol}
    costs = config.costs.model_copy(update={"spread_by_symbol": merged})
    return config.model_copy(update={"costs": costs})


async def build_simulation(config: AppConfig, options: SimulationOptions) -> EngineContext:
    mode = TradingMode.BACKTEST
    enforce_mode_gate(mode, config.broker.mode)
    if config.market_data.provider not in ("mock", "historical"):
        raise ConfigError("simulations run on the mock market or a downloaded dataset (market_data.provider)")
    if options.end < options.start:
        raise ConfigError("end date must not be before start date")
    trading = config.trading
    dataset = load_dataset(config)
    if dataset is not None:
        config = with_measured_spreads(config, dataset)
    calendar = dataset.calendar(trading.exchange_timezone) if dataset is not None else build_calendar(config)
    first = next(calendar.sessions_between(options.start, options.end), None)
    if first is None:
        raise ConfigError(f"no trading session between {options.start} and {options.end}")
    clock = SimulatedClock(first.open)
    git_commit = options.git_commit if options.git_commit is not None else detect_git_commit()
    run = RunContext.create(
        config, mode=mode, started_at=clock.now(), run_id=options.run_id, git_commit=git_commit
    )

    bus = EventBus()
    costs = CostModel(config.costs)
    market: MarketDataAdapter
    if dataset is not None:
        market = HistoricalMarketDataAdapter(dataset, start=options.start, end=options.end)
    else:
        market = MockMarketDataAdapter(
            config.market_data.mock,
            calendar,
            start=options.start,
            end=options.end,
            timeframe=trading.timeframe,
        )
    adapters: dict[str, BrokerAdapter]
    if options.broker is not None:
        options.broker.set_clock(clock)
        adapters = {options.broker.name: options.broker}
    else:
        adapters = build_brokers(config, clock=clock, calendar=calendar, cost_model=costs)
    router = BrokerRouter(config.broker, adapters)
    broker = router.primary()
    if not isinstance(broker, MockBrokerAdapter):
        raise ConfigError("simulations need the mock broker as the simulated exchange")

    database = Database(
        options.database_url or config.persistence.database_url or "sqlite+aiosqlite:///:memory:"
    )
    await database.create_all()
    repository = AuditRepository(database, run_id=run.run_id, mode=mode)
    store = SqlOrderStore(database, mode=mode, run_id=run.run_id)
    AuditRecorder(repository, config.persistence, immediate_writes=False).attach(bus)  # backtest: batched
    state_store = ScopedStateStore(repository, state_prefix(run))

    spec = FeatureSpec.from_config(config.features)
    features = FeatureEngine(spec, namespace=run.namespace, day_start=calendar.day_start)
    decision_tf = trading.decision_timeframe
    model = build_model(
        config.model,
        namespace=run.namespace,
        feature_version=spec.version,
        horizon_minutes=trading.horizon_minutes,
        bar_minutes=decision_tf.minutes,
    )
    signals = SignalEngine(
        config.signals, costs, namespace=run.namespace, strategy=run.strategy, bar_seconds=decision_tf.seconds
    )
    pipeline = DecisionPipeline(
        features=features, model=model, regime=RegimeEngine(config.regime), signals=signals, clock=clock
    )
    risk = StandardRiskEngine(config.risk, build_sizer(config.risk.sizing))

    kill_state = await state_store.get_state(KILL_SWITCH_KEY)
    control_state = await state_store.get_state(CONTROLS_KEY)
    kill_switch = KillSwitch(clock, state=KillSwitchState.model_validate(kill_state) if kill_state else None)
    controls = TradingControls(
        clock, state=TradingControlState.model_validate(control_state) if control_state else None
    )

    execution = BrokerExecutionEngine(
        router=router, store=store, clock=clock, bus=bus, config=config.execution, strategy=run.strategy
    )
    positions = PositionManager(
        execution=execution,
        store=store,
        calendar=calendar,
        config=config.execution,
        asset_class=trading.asset_class,
    )
    health = build_health_monitor(
        config=config, market_data=market, router=router, database=database, clock=clock, simulated=True
    )
    aggregator = BarAggregator(trading.timeframe, decision_tf) if decision_tf != trading.timeframe else None
    engine = TradingEngine(
        config=config,
        run=run,
        clock=clock,
        calendar=calendar,
        bus=bus,
        market_data=market,
        market_engine=MarketDataEngine(calendar),
        quality=DataQualityEngine(config.data_quality),
        pipeline=pipeline,
        risk=risk,
        router=router,
        execution=execution,
        store=store,
        positions=positions,
        reconciler=Reconciler(execution=execution, store=store),
        portfolio=PortfolioTracker(broker=broker.name),
        ledger=TradeLedger(),
        outcomes=PredictionOutcomeTracker(),
        kill_switch=kill_switch,
        controls=controls,
        health=health,
        aggregator=aggregator,
        state_store=state_store,
        signal_loader=repository.load_signal,
    )
    runner = SimulationRunner(
        engine=engine,
        market_data=market,
        broker=broker,
        clock=clock,
        symbols=list(trading.symbols),
        timeframe=trading.timeframe,
        pace_seconds=options.pace_seconds,
        max_events=options.max_events,
        on_session=options.on_session,
    )
    return EngineContext(
        config=config,
        run=run,
        clock=clock,
        calendar=calendar,
        bus=bus,
        market_data=market,
        broker=broker,
        router=router,
        database=database,
        repository=repository,
        store=store,
        model=model,
        kill_switch=kill_switch,
        controls=controls,
        execution=execution,
        engine=engine,
        runner=runner,
        dataset=dataset,
    )


def performance_of(context: EngineContext) -> PerformanceReport:
    return compute_performance(
        context.engine.ledger.closed,
        context.engine.snapshots,
        trading_date=context.calendar.trading_date,
        start_equity=context.config.broker.mock.initial_cash,
    )


async def run_simulation(
    config: AppConfig, options: SimulationOptions, *, context: EngineContext | None = None
) -> SimulationReport:
    """Run one simulation end to end and record it (engine_runs, model_versions, backtest_runs)."""
    ctx = context or await build_simulation(config, options)
    repo, run = ctx.repository, ctx.run
    window_start = next(ctx.calendar.sessions_between(options.start, options.end)).open
    window_end = list(ctx.calendar.sessions_between(options.start, options.end))[-1].close
    symbols = list(config.trading.symbols)
    dataset_version = ctx.dataset.version if ctx.dataset is not None else None
    status = "FAILED"
    try:
        await repo.register_model_version(ctx.model.metadata, created_at=ctx.clock.now())
        await repo.save_run(run, ctx.config, ctx.model.metadata)
        await repo.save_backtest_run(
            run, ctx.config, ctx.model.metadata, start=window_start, end=window_end, symbols=symbols,
            metrics=None, finished_at=None, status="RUNNING", dataset_version=dataset_version,
        )  # fmt: skip
        await ctx.engine.start()
        result = await ctx.runner.run()
        status = "INTERRUPTED" if result.interrupted else "COMPLETED"
        performance = performance_of(ctx)
        summary = ctx.engine.summary()
        metrics = {"performance": performance.model_dump(mode="json"), "counters": summary["counters"]}
        if result.completed:
            await ctx.engine.stop()
        await repo.save_backtest_run(
            run, ctx.config, ctx.model.metadata, start=window_start, end=window_end, symbols=symbols,
            metrics=metrics, finished_at=ctx.clock.now(), status=status, dataset_version=dataset_version,
        )  # fmt: skip
        await repo.finish_run(stopped_at=ctx.clock.now(), status=status, summary=_jsonable_summary(summary))
        return SimulationReport(
            run_id=run.run_id,
            namespace=run.namespace,
            status=status,
            result=result,
            summary=summary,
            performance=performance,
            broker_submissions=ctx.execution.broker_submissions,
            extra={
                **model_usage(ctx.model),
                **(
                    {"dataset": {"name": ctx.dataset.name, "version": ctx.dataset.version}}
                    if ctx.dataset
                    else {}
                ),
            },
            trades=ctx.engine.ledger.closed,
        )
    except BaseException:
        with contextlib.suppress(Exception):  # the original error matters more than this bookkeeping
            await repo.finish_run(stopped_at=ctx.clock.now(), status=status, summary={})
        raise
    finally:
        await ctx.database.dispose()


def model_usage(model: JEVModel) -> dict[str, Any]:
    """API usage and estimated cost of remote models (empty for local ones)."""
    if not isinstance(model, TypeSafeJEVModel):
        return {}
    usage = model.usage
    return {
        "jev_usage": {
            "api_model": model.params.api_model,
            "api_calls": usage.api_calls,
            "cache_hits": usage.cache_hits,
            "input_tokens": usage.input_tokens,
            "estimated_cost_usd": model.cost_usd,
        }
    }


def _jsonable_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(summary, default=str))
