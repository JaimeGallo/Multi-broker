"""AuditRecorder: persists every audit-relevant event published on the bus.

High-volume records (bars, features, predictions, signals, snapshots) are batched; order events, trades, risk and
broker events are written immediately. The engine calls `bus.flush()` before sending any order, so the full
decision chain is durable before the order exists. Orders themselves are written by `SqlOrderStore`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from packages.common.config import PersistenceSection
from packages.common.entities import (
    AccountSnapshot,
    BrokerCapabilities,
    FeatureVector,
    OrderEvent,
    PortfolioSnapshot,
    RiskDecision,
    Signal,
    Trade,
)
from packages.common.events import (
    BarRecord,
    BrokerEvent,
    EventBus,
    PredictionRecord,
    RiskEvent,
    SystemEvent,
    Topics,
)
from packages.persistence.repositories import AuditRepository

IMMEDIATE_TOPICS = frozenset({Topics.ORDER_EVENT, Topics.TRADE, Topics.RISK_EVENT, Topics.BROKER_EVENT})


class AuditRecorder:
    def __init__(
        self, repository: AuditRepository, config: PersistenceSection, *, immediate_writes: bool = True
    ) -> None:
        """`immediate_writes=False` (backtests) batches order events and trades too. The audit barrier before
        every order is unaffected: `bus.flush()` still makes the whole decision chain durable first."""
        self._repo = repository
        self._cfg = config
        self._immediate = immediate_writes
        self._next_auto_flush = config.batch_size  # grows after failures: no retry storm while the DB is down
        self._bars: list[dict[str, Any]] = []
        self._features: list[dict[str, Any]] = []
        self._predictions: list[dict[str, Any]] = []
        self._signals: dict[str, dict[str, Any]] = {}
        self._decisions: list[dict[str, Any]] = []
        self._order_events: list[dict[str, Any]] = []
        self._trades: list[dict[str, Any]] = []
        self._snapshots: list[dict[str, Any]] = []
        self._risk_events: list[dict[str, Any]] = []
        self._system_events: list[dict[str, Any]] = []
        self._broker_events: list[dict[str, Any]] = []
        self._outcomes: list[dict[str, Any]] = []
        self._accounts: list[tuple[AccountSnapshot, BrokerCapabilities, datetime]] = []
        self._pending = 0

    def attach(self, bus: EventBus) -> None:
        bus.subscribe("", self.handle)
        bus.add_flusher(self.flush)

    async def handle(self, topic: str, payload: Any) -> None:
        repo, cfg = self._repo, self._cfg
        if topic == Topics.MARKET_BAR and isinstance(payload, BarRecord):
            if not cfg.store_bars:
                return
            self._bars.append(repo.bar_row(payload.bar, payload.quality.status.value))
        elif topic == Topics.FEATURES and isinstance(payload, FeatureVector):
            if not cfg.store_features:
                return
            self._features.append(repo.feature_row(payload))
        elif topic == Topics.PREDICTION and isinstance(payload, PredictionRecord):
            if not cfg.store_predictions:
                return
            self._predictions.append(repo.prediction_row(payload))
        elif topic == Topics.PREDICTION_OUTCOME:
            if not cfg.store_predictions:
                return
            self._outcomes.append(
                {
                    "b_id": payload.prediction_id,
                    "b_realized": payload.realized_return,
                    "b_correct": payload.direction_correct,
                    "b_resolved": payload.resolved_at,
                }
            )
        elif topic == Topics.SIGNAL and isinstance(payload, Signal):
            self._signals[payload.signal_id] = repo.signal_row(payload)
        elif topic == Topics.RISK_DECISION and isinstance(payload, RiskDecision):
            self._decisions.append(repo.decision_row(payload))
        elif topic == Topics.ORDER_EVENT and isinstance(payload, OrderEvent):
            self._order_events.append(repo.order_event_row(payload))
        elif topic == Topics.TRADE and isinstance(payload, Trade):
            self._trades.append(repo.trade_row(payload))
        elif topic == Topics.PORTFOLIO and isinstance(payload, PortfolioSnapshot):
            self._snapshots.append(repo.snapshot_row(payload))
        elif topic == Topics.RISK_EVENT and isinstance(payload, RiskEvent):
            self._risk_events.append(repo.risk_event_row(payload))
        elif topic == Topics.SYSTEM_EVENT and isinstance(payload, SystemEvent):
            self._system_events.append(repo.system_event_row(payload))
        elif topic == Topics.BROKER_EVENT and isinstance(payload, BrokerEvent):
            self._broker_events.append(repo.broker_event_row(payload))
            if payload.event_type == "connected" and "account" in payload.details:
                self._accounts.append(
                    (
                        AccountSnapshot.model_validate(payload.details["account"]),
                        BrokerCapabilities.model_validate(payload.details["capabilities"]),
                        payload.timestamp,
                    )
                )
        else:
            return
        self._pending += 1
        if (self._immediate and topic in IMMEDIATE_TOPICS) or self._pending >= self._next_auto_flush:
            await self.flush()

    async def flush(self) -> None:
        """Write every buffered record in ONE transaction, in dependency order. Buffers are cleared only after
        the transaction commits, so a failed write loses nothing and partial batches are never stored."""
        if self._pending == 0 and not self._accounts:
            return
        repo = self._repo
        try:
            await self._write(repo)
        except Exception:
            # Keep everything buffered; retry automatically only once the backlog has doubled. The audit
            # barrier before an order (bus.flush) always retries, so no order can skip a failed write.
            self._next_auto_flush = max(self._cfg.batch_size, 2 * self._pending)
            raise
        self._next_auto_flush = self._cfg.batch_size
        self._bars, self._features, self._predictions, self._outcomes = [], [], [], []
        self._signals, self._decisions, self._order_events, self._trades = {}, [], [], []
        self._snapshots, self._risk_events, self._system_events, self._broker_events = [], [], [], []
        self._accounts = []
        self._pending = 0

    async def _write(self, repo: AuditRepository) -> None:
        async with repo.transaction() as session:
            await repo.insert_bars(self._bars, session=session)
            await repo.insert_features(self._features, session=session)
            await repo.insert_predictions(self._predictions, session=session)
            await repo.update_prediction_outcomes(self._outcomes, session=session)
            await repo.upsert_signals(list(self._signals.values()), session=session)
            await repo.insert_risk_decisions(self._decisions, session=session)
            await repo.insert_order_events(self._order_events, session=session)
            await repo.insert_trades(self._trades, session=session)
            await repo.insert_snapshots(self._snapshots, session=session)
            await repo.insert_risk_events(self._risk_events, session=session)
            await repo.insert_system_events(self._system_events, session=session)
            await repo.insert_broker_events(self._broker_events, session=session)
            for account, capabilities, seen_at in self._accounts:
                await repo.upsert_broker_account(account, capabilities, seen_at=seen_at, session=session)
