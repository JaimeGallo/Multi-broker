"""TradingEngine: one decision per bar, broker events and timers.

The same engine runs in every mode; only the runner (who delivers events), the clock and the adapters change.
Handlers are awaited sequentially, so the same input stream always produces the same decisions.
"""

from __future__ import annotations

import logging
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Protocol

from packages.analytics.ledger import TradeLedger
from packages.analytics.predictions import PredictionOutcomeTracker
from packages.brokers.base import ExecutionRequirements
from packages.brokers.router import BrokerRouter
from packages.common.calendar import MarketCalendar
from packages.common.clock import Clock
from packages.common.config import AppConfig
from packages.common.entities import (
    SIGNAL_TRANSITIONS,
    DataQualityReport,
    FeatureVector,
    Fill,
    MarketBar,
    MarketEvent,
    MarketQuote,
    MarketTrade,
    Order,
    OrderEvent,
    OrderRequest,
    PortfolioSnapshot,
    RiskDecision,
    Signal,
)
from packages.common.enums import (
    DataQualityStatus,
    Direction,
    HealthState,
    NoTradeReason,
    OrderClass,
    OrderIntent,
    OrderStatus,
    OrderType,
    SignalStatus,
)
from packages.common.errors import BrokerError, ModelError
from packages.common.events import (
    BarRecord,
    BrokerEvent,
    EventBus,
    PredictionRecord,
    RiskEvent,
    SystemEvent,
    Topics,
)
from packages.common.ids import make_client_order_id
from packages.common.run import RunContext
from packages.common.safety import assert_paper_account
from packages.data_quality.engine import DataQualityEngine
from packages.execution.engine import BrokerExecutionEngine
from packages.execution.order_store import OrderStore
from packages.execution.position_manager import ManagedPosition, PositionManager
from packages.execution.reconciliation import Reconciler, ReconciliationReport
from packages.market_data.aggregator import BarAggregator
from packages.market_data.base import MarketDataAdapter
from packages.market_data.engine import MarketDataEngine
from packages.pipeline.decision import DecisionPipeline, PipelineResult
from packages.pipeline.health import HealthCheckResult, HealthMonitor
from packages.risk.engine import LOSS_LIMIT_CHECKS, RiskContext, RiskEngine
from packages.risk.kill_switch import (
    KillSwitch,
    KillSwitchReason,
    KillSwitchState,
    TradingControls,
    TradingControlState,
)
from packages.risk.portfolio import PortfolioTracker

log = logging.getLogger(__name__)
EPSILON = 1e-9
FINAL_SIGNAL_STATES = frozenset({SignalStatus.REJECTED, SignalStatus.EXPIRED, SignalStatus.EXECUTED})


class StateStore(Protocol):
    async def get_state(self, key: str) -> dict[str, Any] | None: ...

    async def set_state(self, key: str, value: Mapping[str, Any], *, at: datetime) -> None: ...


SignalLoader = Callable[[str], Awaitable[Signal | None]]


@dataclass
class EngineCounters:
    bars: int = 0
    rejected_bars: int = 0
    quality: Counter[str] = field(default_factory=Counter)
    predictions: int = 0
    signals_generated: int = 0
    signal_outcomes: Counter[str] = field(default_factory=Counter)
    no_trade: Counter[str] = field(default_factory=Counter)
    risk_rejections: Counter[str] = field(default_factory=Counter)
    approved: int = 0
    entries_submitted: int = 0
    orders_rejected: int = 0
    fills: int = 0
    trades: int = 0
    model_errors: int = 0
    anomalies: Counter[str] = field(default_factory=Counter)

    def as_dict(self) -> dict[str, Any]:
        return {k: dict(v) if isinstance(v, Counter) else v for k, v in self.__dict__.items()}


class TradingEngine:
    def __init__(
        self,
        *,
        config: AppConfig,
        run: RunContext,
        clock: Clock,
        calendar: MarketCalendar,
        bus: EventBus,
        market_data: MarketDataAdapter,
        market_engine: MarketDataEngine,
        quality: DataQualityEngine,
        pipeline: DecisionPipeline,
        risk: RiskEngine,
        router: BrokerRouter,
        execution: BrokerExecutionEngine,
        store: OrderStore,
        positions: PositionManager,
        reconciler: Reconciler,
        portfolio: PortfolioTracker,
        ledger: TradeLedger,
        outcomes: PredictionOutcomeTracker,
        kill_switch: KillSwitch,
        controls: TradingControls,
        health: HealthMonitor,
        aggregator: BarAggregator | None = None,
        state_store: StateStore | None = None,
        signal_loader: SignalLoader | None = None,
    ) -> None:
        self._config = config
        self._run = run
        self._clock = clock
        self._calendar = calendar
        self._bus = bus
        self._market_data = market_data
        self._market_engine = market_engine
        self._quality = quality
        self._pipeline = pipeline
        self._risk = risk
        self._router = router
        self._execution = execution
        self._store = store
        self._positions = positions
        self._reconciler = reconciler
        self._portfolio = portfolio
        self._ledger = ledger
        self._outcomes = outcomes
        self._kill_switch = kill_switch
        self._controls = controls
        self._health = health
        self._aggregator = aggregator
        self._state_store = state_store
        self._signal_loader = signal_loader

        self.counters = EngineCounters()
        self.snapshots: list[PortfolioSnapshot] = []
        self.reconciliation: ReconciliationReport | None = None
        self._live_signals: dict[str, Signal] = {}
        self._entry_references: dict[str, float] = {}
        self._entry_slippage: deque[float] = deque(maxlen=config.kill_switch.slippage_window)
        self._consecutive_model_errors = 0
        self._last_snapshot_at: datetime | None = None
        self._kill_effects_applied_for: datetime | None = None

        execution.set_fill_listener(self._on_fill)
        execution.set_anomaly_listener(self._on_anomaly)
        positions.set_exit_failure_listener(self._on_exit_failure)
        kill_switch.set_listener(self._on_kill_switch_change)
        controls.set_listener(self._on_controls_change)
        bus.set_error_handler(self._on_bus_error)

    # ------------------------------------------------------------------ properties

    @property
    def run(self) -> RunContext:
        return self._run

    @property
    def ledger(self) -> TradeLedger:
        return self._ledger

    @property
    def kill_switch(self) -> KillSwitch:
        return self._kill_switch

    @property
    def portfolio(self) -> PortfolioTracker:
        return self._portfolio

    @property
    def positions(self) -> PositionManager:
        return self._positions

    @property
    def health(self) -> HealthMonitor:
        return self._health

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> ReconciliationReport:
        await self._market_data.connect()
        for adapter in self._router.adapters.values():
            await adapter.connect()
            assert_paper_account(adapter.capabilities, self._run.mode)
            account = await adapter.get_account()
            await self._bus.publish(
                Topics.BROKER_EVENT,
                BrokerEvent(
                    timestamp=self._clock.now(),
                    broker=adapter.name,
                    event_type="connected",
                    details={
                        "account": account.model_dump(mode="json"),
                        "capabilities": adapter.capabilities.model_dump(mode="json"),
                    },
                ),
            )
        await self._health.run(self._clock.now())
        report = await self._reconcile()
        await self._refresh_account()
        await self._system_event(
            "INFO",
            "engine",
            "engine.started",
            f"engine started in {self._run.mode.value} mode",
            {
                "run_id": self._run.run_id,
                "namespace": self._run.namespace,
                "kill_switch": self._kill_switch.engaged,
            },
        )
        return report

    async def finalize(self) -> None:
        """Final snapshot and audit flush (end of a simulation or a controlled shutdown)."""
        await self._snapshot(self._clock.now(), force=True)
        await self._flush_quietly()

    async def stop(self) -> None:
        await self._system_event(
            "INFO", "engine", "engine.stopped", "engine stopped", self.counters.as_dict()
        )
        await self._flush_quietly()
        for adapter in self._router.adapters.values():
            await adapter.disconnect()
        await self._market_data.disconnect()

    async def _reconcile(self) -> ReconciliationReport:
        broker = self._router.primary()
        report = await self._reconciler.reconcile_orders(broker)
        orders = await self._store.list_all()
        restored = self._positions.rebuild(
            orders,
            horizon=timedelta(minutes=self._config.trading.horizon_minutes),
            signal_ttl=timedelta(seconds=self._config.signals.signal_ttl_seconds),
        )
        broker_positions = await broker.get_positions()
        self._reconciler.compare_positions(report, self._positions.expected_positions(), broker_positions)
        self._portfolio.load_positions(broker_positions)
        await self._restore_ledger(restored, orders)
        self.reconciliation = report
        await self._system_event(
            "INFO" if report.clean else "ERROR",
            "reconciliation",
            "reconciliation.completed",
            "clean" if report.clean else "discrepancies found",
            report.summary(),
        )
        if report.unknown_broker_orders or report.missing_at_broker:
            await self._engage(
                KillSwitchReason.UNEXPECTED_ORDER,
                f"unknown={report.unknown_broker_orders} missing={report.missing_at_broker}",
            )
        if report.unexpected_positions or report.position_mismatches:
            await self._engage(
                KillSwitchReason.UNEXPECTED_POSITION,
                f"unexpected={report.unexpected_positions} mismatches={report.position_mismatches}",
            )
        return report

    async def _restore_ledger(self, restored: list[ManagedPosition], orders: list[Order]) -> None:
        if self._signal_loader is None:
            return
        by_id = {o.client_order_id: o for o in orders}
        for position in restored:
            entry = by_id.get(position.entry_client_order_id)
            if entry is None or entry.filled_quantity <= EPSILON or entry.average_fill_price is None:
                continue
            signal = await self._signal_loader(position.signal_id)
            if signal is None:
                continue
            self._ledger.restore(
                signal,
                broker=entry.broker,
                entry_quantity=entry.filled_quantity,
                entry_price=entry.average_fill_price,
                entry_time=position.entry_time or entry.updated_at,
                take_profit=position.take_profit_price,
                stop_loss=position.stop_loss_price,
            )

    # ------------------------------------------------------------------ market events

    async def handle_market_event(self, event: MarketEvent) -> None:
        now = self._clock.now()
        if isinstance(event, MarketQuote):
            self._market_engine.ingest_quote(event, now)
        elif isinstance(event, MarketTrade):
            self._market_engine.ingest_trade(event, now)
        else:
            await self._handle_bar(event, now)

    async def _handle_bar(self, bar: MarketBar, now: datetime) -> None:
        ingest = self._market_engine.check_bar(bar)
        if not ingest.accepted:
            self._market_engine.record_rejected(bar, ingest, now)
            self.counters.rejected_bars += 1
            kind = (
                "conflicting_duplicate"
                if ingest.conflicting
                else "duplicate"
                if ingest.duplicate
                else "out_of_order"
            )
            self.counters.quality[kind] += 1
            if kind != "duplicate":
                await self._system_event(
                    "WARNING", "market_data", f"bar.{kind}", f"{bar.symbol} {bar.start.isoformat()}", {}
                )
            return
        history = self._market_engine.bars(bar.symbol)
        quote = self._market_engine.latest_quote(bar.symbol)
        report = self._quality.evaluate_bar(
            bar, ingest, history, quote, now, self._market_engine.bars_since_gap(bar.symbol)
        )
        self.counters.quality[report.status.value] += 1
        await self._bus.publish(Topics.MARKET_BAR, BarRecord(bar, report))
        if report.status is DataQualityStatus.INVALID:
            self.counters.no_trade[NoTradeReason.DATA_INVALID.value] += 1
            await self._system_event(
                "WARNING",
                "data_quality",
                "bar.invalid",
                f"{bar.symbol} {bar.start.isoformat()}",
                {"issues": report.codes},
            )
            return  # never committed: an invalid bar cannot contaminate the feature window
        self._market_engine.commit_bar(bar, ingest, now)
        self.counters.bars += 1
        self._portfolio.mark(bar.symbol, bar.close)
        self._ledger.on_bar(bar)
        for outcome in self._outcomes.on_bar(bar):
            await self._bus.publish(Topics.PREDICTION_OUTCOME, outcome)
        if self._aggregator is None:
            await self._decide(bar, report, quote)
            return
        for decision_bar in self._aggregator.add(bar):
            await self._bus.publish(Topics.MARKET_BAR, BarRecord(decision_bar, report))
            await self._decide(decision_bar, report, quote)

    async def _decide(self, bar: MarketBar, report: DataQualityReport, quote: MarketQuote | None) -> None:
        cfg = self._config
        status = report.status
        if status is DataQualityStatus.STALE or (
            status is DataQualityStatus.DEGRADED and cfg.data_quality.block_on_degraded
        ):
            self._pipeline.observe(bar)
            reason = (
                NoTradeReason.DATA_STALE if status is DataQualityStatus.STALE else NoTradeReason.DATA_DEGRADED
            )
            self.counters.no_trade[reason.value] += 1
            return
        started = perf_counter()
        try:
            result = self._pipeline.evaluate(bar, quote)
        except ModelError as exc:
            self.counters.model_errors += 1
            self._consecutive_model_errors += 1
            self.counters.no_trade[NoTradeReason.MODEL_ERROR.value] += 1
            await self._system_event("ERROR", "model", "model.error", str(exc), {})
            if self._consecutive_model_errors >= cfg.kill_switch.max_consecutive_model_errors:
                await self._engage(KillSwitchReason.MODEL_UNAVAILABLE, str(exc))
            return
        self._consecutive_model_errors = 0
        latency_ms = (perf_counter() - started) * 1000.0
        await self._publish_pipeline(result, latency_ms)
        if latency_ms > cfg.kill_switch.max_decision_latency_ms:
            await self._engage(KillSwitchReason.ABNORMAL_LATENCY, f"decision took {latency_ms:.0f} ms")

        signal = result.signal
        if signal is None:
            if result.no_trade_reason is not None:
                self.counters.no_trade[result.no_trade_reason.value] += 1
            return
        self.counters.signals_generated += 1
        if signal.status is SignalStatus.REJECTED:
            self.counters.no_trade[(result.no_trade_reason or NoTradeReason.INSUFFICIENT_EDGE).value] += 1
            self.counters.signal_outcomes[SignalStatus.REJECTED.value] += 1
            await self._publish_signal(signal)
            return
        await self._publish_signal(signal)
        assert result.features is not None
        await self._risk_and_execute(signal, result.features)

    async def _publish_pipeline(self, result: PipelineResult, latency_ms: float) -> None:
        if result.features is not None:
            await self._bus.publish(Topics.FEATURES, result.features)
        if result.prediction is not None and result.regime is not None and result.features is not None:
            self.counters.predictions += 1
            await self._bus.publish(
                Topics.PREDICTION,
                PredictionRecord(result.prediction, result.regime, latency_ms, result.no_trade_reason),
            )
            self._outcomes.add(result.prediction, result.features.close)

    # ------------------------------------------------------------------ risk & execution

    async def _risk_and_execute(self, signal: Signal, features: FeatureVector) -> None:
        cfg = self._config
        now = self._clock.now()
        try:
            broker = await self._router.select_broker(
                signal.symbol,
                cfg.trading.asset_class,
                self._run.strategy,
                ExecutionRequirements(bracket=True, short=signal.direction is Direction.SHORT),
            )
            account = await broker.get_account()
            instrument = await broker.get_instrument(signal.symbol)
        except BrokerError as exc:
            self.counters.no_trade[NoTradeReason.BROKER_UNAVAILABLE.value] += 1
            await self._set_signal_status(
                signal, SignalStatus.REJECTED, NoTradeReason.BROKER_UNAVAILABLE.value
            )
            await self._system_event("WARNING", "broker", "broker.unavailable", str(exc), {})
            return
        self._portfolio.update_account(account)
        health_ok, health_detail = self._health.trading_allowed()
        context = RiskContext(
            now=now,
            account=account,
            positions=self._portfolio.open_positions(),
            pending_entry_symbols=frozenset(self._positions.pending_entry_symbols()),
            gross_exposure=self._portfolio.gross_exposure(),
            instrument=instrument,
            broker_capabilities=broker.capabilities,
            atr=features.get(f"atr_{self._pipeline.features.spec.atr_period}"),
            reference_price=features.close,
            peak_equity=self._portfolio.peak_equity,
            session=self._calendar.session_for(now),
            kill_switch_engaged=self._kill_switch.engaged,
            trading_paused=self._controls.paused,
            health_ok=health_ok,
            health_detail=health_detail,
        )
        try:
            decision = self._risk.evaluate(signal, context)
        except Exception as exc:
            await self._engage(KillSwitchReason.RISK_ENGINE_UNAVAILABLE, str(exc))
            await self._set_signal_status(signal, SignalStatus.REJECTED, "risk_engine_error")
            return
        await self._bus.publish(Topics.RISK_DECISION, decision)
        if not decision.approved:
            for reason in decision.reasons:
                self.counters.risk_rejections[reason] += 1
            self.counters.no_trade[NoTradeReason.RISK_REJECTED.value] += 1
            await self._set_signal_status(signal, SignalStatus.REJECTED, f"risk:{decision.reasons[0]}")
            breached = LOSS_LIMIT_CHECKS.intersection(decision.reasons)
            if "daily_loss_limit" in breached:
                await self._engage(KillSwitchReason.DAILY_LOSS, "daily loss limit reached")
            if "max_drawdown" in breached:
                await self._engage(KillSwitchReason.MAX_DRAWDOWN, "max drawdown reached")
            return

        self.counters.approved += 1
        await self._set_signal_status(signal, SignalStatus.APPROVED)
        try:
            await self._bus.flush()  # audit barrier: the decision chain is durable before any order exists
        except Exception as exc:
            await self._on_database_failure(f"audit barrier failed: {exc}")
            await self._set_signal_status(signal, SignalStatus.EXPIRED, NoTradeReason.AUDIT_UNAVAILABLE.value)
            return
        request = self._entry_request(signal, decision)
        self._positions.register_pending(signal, decision, request.client_order_id)
        self._ledger.register(signal, decision)
        self._live_signals[signal.signal_id] = signal
        self._entry_references[signal.signal_id] = signal.reference_price
        order = await self._execution.submit(request)
        self.counters.entries_submitted += 1
        await self._after_order_change(order)

    def _entry_request(self, signal: Signal, decision: RiskDecision) -> OrderRequest:
        assert decision.side is not None
        return OrderRequest(
            client_order_id=make_client_order_id(signal.signal_id, OrderIntent.ENTRY),
            symbol=signal.symbol,
            side=decision.side,
            quantity=decision.quantity,
            order_type=OrderType.MARKET,
            time_in_force=self._config.execution.time_in_force,
            order_class=OrderClass.BRACKET,
            take_profit_price=decision.take_profit,
            stop_loss_price=decision.stop_loss,
            intent=OrderIntent.ENTRY,
            signal_id=signal.signal_id,
            asset_class=self._config.trading.asset_class,
        )

    # ------------------------------------------------------------------ broker events

    async def handle_order_event(self, event: OrderEvent) -> None:
        order = await self._execution.handle_order_event(event)
        if order is not None:
            await self._after_order_change(order)

    async def _after_order_change(self, order: Order) -> None:
        is_entry = order.intent is OrderIntent.ENTRY and order.parent_client_order_id is None
        if is_entry:
            self._positions.attach_entry_order(order)
        await self._positions.on_order(order)
        if not (is_entry and order.is_terminal and order.signal_id):
            return
        signal = self._live_signals.get(order.signal_id)
        if signal is None or order.filled_quantity > EPSILON:
            return
        if order.status is OrderStatus.REJECTED:
            self.counters.orders_rejected += 1
            self.counters.no_trade[NoTradeReason.ORDER_REJECTED.value] += 1
            await self._set_signal_status(
                signal, SignalStatus.REJECTED, f"order_rejected:{order.reject_reason}"
            )
        else:
            await self._set_signal_status(signal, SignalStatus.EXPIRED, "entry_not_filled")

    async def _on_fill(self, fill: Fill) -> None:
        self.counters.fills += 1
        self._portfolio.apply_fill(fill)
        self._positions.on_fill(fill)
        exit_reference = (
            self._positions.exit_reference(fill.signal_id, fill.intent) if fill.signal_id else None
        )
        trade = self._ledger.on_fill(fill, exit_reference)
        await self._bus.publish(Topics.FILL, fill)
        if fill.intent is OrderIntent.ENTRY and fill.signal_id:
            signal = self._live_signals.get(fill.signal_id)
            if signal is not None and signal.status is SignalStatus.APPROVED:
                await self._set_signal_status(signal, SignalStatus.EXECUTED)
            await self._track_slippage(fill)
        if trade is not None:
            self.counters.trades += 1
            await self._bus.publish(Topics.TRADE, trade)

    async def _track_slippage(self, fill: Fill) -> None:
        reference = self._entry_references.get(fill.signal_id or "")
        if not reference:
            return
        self._entry_slippage.append(fill.side.sign * (fill.price - reference) / reference * 1e4)
        cfg = self._config.kill_switch
        if len(self._entry_slippage) >= cfg.min_fills_for_slippage:
            average = sum(self._entry_slippage) / len(self._entry_slippage)
            if average > cfg.max_avg_slippage_bps:
                await self._engage(
                    KillSwitchReason.ABNORMAL_SLIPPAGE, f"average entry slippage {average:.1f} bps"
                )

    # ------------------------------------------------------------------ timers

    async def handle_timer(self, now: datetime) -> None:
        await self._health.run(now)
        await self._check_health_conditions(now)
        await self._check_feed_staleness(now)
        flatten = False
        if self._kill_switch.engaged:
            await self._apply_kill_switch_effects()
            flatten = self._config.kill_switch.flatten_on_kill
        await self._positions.on_timer(now, self._portfolio.prices(), flatten_all=flatten)
        await self._execution.check_timeouts(now)
        await self._snapshot(now)

    async def _check_health_conditions(self, now: datetime) -> None:
        since = self._health.failing_since("broker_connected")
        limit = self._config.kill_switch.broker_disconnect_seconds
        if since is not None and (now - since).total_seconds() >= limit:
            await self._engage(
                KillSwitchReason.BROKER_DISCONNECTED, f"broker disconnected since {since.isoformat()}"
            )
        database = self._health.last.get("database_available")
        if database is not None and database.state is HealthState.FAIL:
            await self._engage(KillSwitchReason.DATABASE_UNAVAILABLE, database.detail)

    async def _check_feed_staleness(self, now: datetime) -> None:
        session = self._calendar.session_for(now)
        stale_after = self._config.kill_switch.stale_data_seconds
        if session is None or session.minutes_since_open(now) * 60.0 < stale_after:
            return
        last = self._market_engine.last_message_at
        if DataQualityEngine.feed_is_stale(now, last, stale_after):
            await self._engage(KillSwitchReason.STALE_DATA, f"no market data since {last}")

    async def _apply_kill_switch_effects(self) -> None:
        engaged_at = self._kill_switch.state.engaged_at
        if engaged_at is None or self._kill_effects_applied_for == engaged_at:
            return
        self._kill_effects_applied_for = engaged_at
        if self._config.kill_switch.cancel_entries_on_kill:
            cancelled = await self._positions.cancel_pending_entries()
            if cancelled:
                await self._system_event(
                    "WARNING", "risk", "kill_switch.entries_cancelled", f"{cancelled} entries", {}
                )

    async def _snapshot(self, now: datetime, *, force: bool = False) -> None:
        every = timedelta(minutes=self._config.persistence.snapshot_every_minutes)
        if not force and self._last_snapshot_at is not None and now - self._last_snapshot_at < every:
            return
        self._last_snapshot_at = now
        await self._refresh_account()
        snapshot = self._portfolio.snapshot(now)
        if snapshot is not None:
            self.snapshots.append(snapshot)
            await self._bus.publish(Topics.PORTFOLIO, snapshot)

    async def _refresh_account(self) -> None:
        try:
            self._portfolio.update_account(await self._router.primary().get_account())
        except BrokerError as exc:
            log.warning("account refresh failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------ signals, kill switch, events

    async def _set_signal_status(
        self, signal: Signal, status: SignalStatus, reason: str | None = None
    ) -> None:
        if status not in SIGNAL_TRANSITIONS[signal.status]:
            return
        signal.transition(status, self._clock.now(), reason)
        if reason is not None and status is SignalStatus.REJECTED:
            signal.rejection_reasons.append(reason)
        if status in FINAL_SIGNAL_STATES:
            self.counters.signal_outcomes[status.value] += 1
            self._live_signals.pop(signal.signal_id, None)
        await self._publish_signal(signal)

    async def _publish_signal(self, signal: Signal) -> None:
        await self._bus.publish(Topics.SIGNAL, signal.model_copy(deep=True))

    async def _engage(self, reason: KillSwitchReason, detail: str) -> None:
        if await self._kill_switch.engage(reason, detail):
            log.error("KILL SWITCH ENGAGED", extra={"reason": reason.value, "detail": detail})

    async def _on_kill_switch_change(self, state: KillSwitchState, event_type: str) -> None:
        now = self._clock.now()
        try:
            if self._state_store is not None:
                await self._state_store.set_state("kill_switch", state.model_dump(mode="json"), at=now)
        except Exception:
            log.exception("could not persist kill switch state")
        await self._bus.publish(
            Topics.RISK_EVENT,
            RiskEvent(
                timestamp=now,
                event_type=event_type,
                reason=state.reason.value if state.reason else "",
                detail=state.detail,
                details=state.model_dump(mode="json"),
            ),
        )

    async def _on_controls_change(self, state: TradingControlState) -> None:
        now = self._clock.now()
        try:
            if self._state_store is not None:
                await self._state_store.set_state("trading_controls", state.model_dump(mode="json"), at=now)
        except Exception:
            log.exception("could not persist trading controls")
        await self._bus.publish(
            Topics.RISK_EVENT,
            RiskEvent(
                timestamp=now,
                event_type="TRADING_PAUSED" if state.paused else "TRADING_RESUMED",
                reason="MANUAL",
                detail=state.note,
                details=state.model_dump(mode="json"),
            ),
        )

    async def _on_anomaly(self, kind: str, detail: str, details: dict[str, Any]) -> None:
        self.counters.anomalies[kind] += 1
        await self._system_event("WARNING", "execution", f"anomaly.{kind}", detail, details)
        if kind == "unknown_order_event":
            await self._engage(KillSwitchReason.UNEXPECTED_ORDER, detail)

    async def _on_exit_failure(self, position: ManagedPosition, reason: str) -> None:
        await self._system_event(
            "ERROR",
            "execution",
            "exit.failed",
            reason,
            {"signal_id": position.signal_id, "symbol": position.symbol},
        )
        await self._engage(KillSwitchReason.UNEXPECTED_POSITION, f"cannot close {position.symbol}: {reason}")

    async def _on_bus_error(self, topic: str, exc: BaseException) -> None:
        await self._on_database_failure(f"audit write failed on {topic}: {exc}")

    async def _on_database_failure(self, detail: str) -> None:
        self._health.report(HealthCheckResult("database_available", HealthState.FAIL, detail))
        await self._engage(KillSwitchReason.DATABASE_UNAVAILABLE, detail)

    async def _system_event(
        self, level: str, component: str, event_type: str, message: str, details: dict[str, Any]
    ) -> None:
        await self._bus.publish(
            Topics.SYSTEM_EVENT,
            SystemEvent(
                timestamp=self._clock.now(),
                level=level,
                component=component,
                event_type=event_type,
                message=message,
                details=details,
            ),
        )

    async def _flush_quietly(self) -> None:
        try:
            await self._bus.flush()
        except Exception:
            log.exception("final audit flush failed")

    def summary(self) -> dict[str, Any]:
        return {
            "counters": self.counters.as_dict(),
            "kill_switch": self._kill_switch.state.model_dump(mode="json"),
            "open_positions": self._portfolio.open_positions(),
            "open_trades": self._ledger.open_count,
            "closed_trades": len(self._ledger.closed),
            "reconciliation": self.reconciliation.summary() if self.reconciliation else None,
        }
