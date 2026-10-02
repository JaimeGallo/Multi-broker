"""Repositories: write-ahead order store and the audit repository (records, state, decision traces)."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Any, cast

from sqlalchemy import Table, bindparam, func, inspect, select, update
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncSession

from packages.common.config import AppConfig
from packages.common.entities import (
    AccountSnapshot,
    BrokerCapabilities,
    ExpectedValue,
    FeatureVector,
    ModelMetadata,
    Order,
    OrderEvent,
    PortfolioSnapshot,
    RiskDecision,
    Signal,
    Trade,
)
from packages.common.enums import TERMINAL_ORDER_STATUSES, TradingMode
from packages.common.events import BrokerEvent, PredictionRecord, RiskEvent, SystemEvent
from packages.common.run import RunContext
from packages.common.secrets import redact_url
from packages.persistence.database import Database
from packages.persistence.models import (
    BacktestRunRow,
    BacktestTradeRow,
    Base,
    BrokerAccountRow,
    BrokerEventRow,
    EngineRunRow,
    ExperimentRow,
    FeatureRow,
    MarketBarRow,
    ModelVersionRow,
    OrderEventRow,
    OrderRow,
    PortfolioSnapshotRow,
    PredictionRow,
    RiskDecisionRow,
    RiskEventRow,
    SignalRow,
    SystemEventRow,
    SystemStateRow,
    TradeRow,
    UTCDateTime,
)

TERMINAL_STATUSES = [status.value for status in TERMINAL_ORDER_STATUSES]
BACKTEST_MODES = (TradingMode.BACKTEST, TradingMode.REPLAY)


def finite(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) else None


def sanitized_config(config: AppConfig) -> dict[str, Any]:
    """Configuration as stored with a run: connection URLs never keep their passwords."""
    data = config.model_dump(mode="json")
    data["persistence"]["database_url"] = redact_url(data["persistence"]["database_url"])
    data["redis"]["url"] = redact_url(data["redis"]["url"])
    return data


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def row_to_dict(row: Base) -> dict[str, Any]:
    return {attr.key: _jsonable(getattr(row, attr.key)) for attr in inspect(row).mapper.column_attrs}


# --------------------------------------------------------------------------- orders

_ORDER_FIELDS = (
    "broker_order_id", "broker", "symbol", "side", "quantity", "order_type", "time_in_force", "order_class",
    "intent", "status", "limit_price", "stop_price", "take_profit_price", "stop_loss_price",
    "parent_client_order_id", "leg_client_order_ids", "signal_id", "asset_class", "filled_quantity",
    "average_fill_price", "reject_reason", "created_at", "submitted_at", "updated_at",
)  # fmt: skip


def _order_values(order: Order) -> dict[str, Any]:
    data = order.model_dump(include=set(_ORDER_FIELDS), mode="python")
    for key in ("side", "order_type", "time_in_force", "order_class", "intent", "status", "asset_class"):
        data[key] = data[key].value
    data["leg_client_order_ids"] = list(order.leg_client_order_ids)
    return data


def _order_from_row(row: OrderRow) -> Order:
    payload = {name: getattr(row, name) for name in _ORDER_FIELDS}
    payload["client_order_id"] = row.client_order_id
    payload["leg_client_order_ids"] = list(row.leg_client_order_ids or [])
    return Order.model_validate(payload)


class SqlOrderStore:
    """Durable order store with a write-through, in-memory view of this engine's orders.

    Every `save` is written to the database before the in-memory view changes, so the database stays the
    source of truth for restarts. Reads are served from memory: this engine is the only writer of its scope
    (one run in backtest/replay, its mode in paper/shadow), whose orders are loaded once on first use.
    """

    def __init__(self, db: Database, *, mode: TradingMode, run_id: str) -> None:
        self._db = db
        self._mode = mode
        self._run_id = run_id
        self._orders: dict[str, Order] | None = None
        self._open: set[str] = set()
        self._by_signal: dict[str, list[str]] = {}
        self._upsert: Any = None

    async def _view(self) -> dict[str, Order]:
        if self._orders is None:
            self._orders = {}
            for order in await self._query(self._scoped().order_by(OrderRow.created_at)):
                self._index(order)
        return self._orders

    def _index(self, order: Order) -> None:
        assert self._orders is not None
        cid = order.client_order_id
        if cid not in self._orders and order.signal_id:
            self._by_signal.setdefault(order.signal_id, []).append(cid)
        self._orders[cid] = order
        if order.is_terminal:
            self._open.discard(cid)
        else:
            self._open.add(cid)

    async def get(self, client_order_id: str) -> Order | None:
        order = (await self._view()).get(client_order_id)
        return order.model_copy(deep=True) if order is not None else None

    async def save(self, order: Order) -> None:
        await self._view()
        values = {"client_order_id": order.client_order_id, "mode": self._mode.value, "run_id": self._run_id}
        values.update(_order_values(order))
        async with self._db.sessions() as session, session.begin():
            await session.execute(self._upsert_statement(), [values])
        stored = order.model_copy(deep=True)
        stored.legs = []
        self._index(stored)

    def _upsert_statement(self) -> Any:
        """Single-statement insert-or-update, built once (one round trip per save)."""
        if self._upsert is None:
            dialect = postgresql if self._db.dialect == "postgresql" else sqlite
            statement = dialect.insert(OrderRow)
            fixed = ("client_order_id", "mode", "run_id")
            columns = [c.name for c in OrderRow.__table__.columns if c.name not in fixed]
            self._upsert = statement.on_conflict_do_update(
                index_elements=["client_order_id"], set_={name: statement.excluded[name] for name in columns}
            )
        return self._upsert

    def _scoped(self) -> Any:
        """Orders of this engine only: one run in backtest/replay, every run of the mode in paper/shadow
        (their ids are stable across restarts). Other runs sharing the database are never reconciled."""
        statement = select(OrderRow).where(OrderRow.mode == self._mode.value)
        if self._mode in BACKTEST_MODES:
            statement = statement.where(OrderRow.run_id == self._run_id)
        return statement

    async def _select(self, ids: Any, keep: Any = None) -> list[Order]:
        view = await self._view()
        orders = [view[cid] for cid in ids if keep is None or keep(view[cid])]
        orders.sort(key=lambda o: (o.created_at, o.client_order_id))
        return [o.model_copy(deep=True) for o in orders]

    async def list_open(self, broker: str | None = None) -> list[Order]:
        await self._view()
        return await self._select(list(self._open), lambda o: broker is None or o.broker == broker)

    async def list_by_signal(self, signal_id: str) -> list[Order]:
        await self._view()
        return await self._select(list(self._by_signal.get(signal_id, ())))

    async def list_all(self) -> list[Order]:
        return await self._select(list(await self._view()))

    async def _query(self, statement: Any) -> list[Order]:
        async with self._db.sessions() as session:
            rows = (await session.execute(statement)).scalars().all()
            return [_order_from_row(row) for row in rows]


# --------------------------------------------------------------------------- audit records


class AuditRepository:
    def __init__(self, db: Database, *, run_id: str, mode: TradingMode) -> None:
        self._db = db
        self.run_id = run_id
        self.mode = mode

    # ---- low-level helpers

    def _insert(self, model: type[Base]) -> Any:
        if self._db.dialect == "postgresql":
            return postgresql.insert(model)
        if self._db.dialect == "sqlite":
            return sqlite.insert(model)
        raise NotImplementedError(f"unsupported database dialect: {self._db.dialect}")

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """One transaction for several writes (the recorder flushes all its buffers atomically)."""
        async with self._db.sessions() as session, session.begin():
            yield session

    async def _execute(
        self, statement: Any, rows: list[dict[str, Any]], session: AsyncSession | None
    ) -> None:
        if session is not None:
            await session.execute(statement, rows)
            return
        async with self.transaction() as own:
            await own.execute(statement, rows)

    async def _insert_ignore(
        self, model: type[Base], rows: Sequence[Mapping[str, Any]], session: AsyncSession | None = None
    ) -> None:
        if not rows:
            return
        await self._execute(self._insert(model).on_conflict_do_nothing(), [dict(r) for r in rows], session)

    async def _upsert(
        self,
        model: type[Base],
        rows: Sequence[Mapping[str, Any]],
        keys: Sequence[str],
        session: AsyncSession | None = None,
    ) -> None:
        if not rows:
            return
        statement = self._insert(model)
        columns = [c.name for c in model.__table__.columns if c.name not in keys and not c.primary_key]
        statement = statement.on_conflict_do_update(
            index_elements=list(keys), set_={name: statement.excluded[name] for name in columns}
        )
        await self._execute(statement, [dict(r) for r in rows], session)

    # ---- runs, models, accounts, state

    async def save_run(self, run: RunContext, config: AppConfig, model: ModelMetadata) -> None:
        await self._upsert(
            EngineRunRow,
            [
                {
                    "run_id": run.run_id,
                    "mode": run.mode.value,
                    "broker": run.broker,
                    "market_data": run.market_data,
                    "strategy": run.strategy,
                    "namespace": run.namespace,
                    "config_hash": run.config_hash,
                    "config": sanitized_config(config),
                    "git_commit": run.git_commit,
                    "model_name": model.model_name,
                    "model_version": model.model_version,
                    "feature_version": model.feature_version,
                    "started_at": run.started_at,
                    "stopped_at": None,
                    "status": "RUNNING",
                    "summary": None,
                }
            ],
            ["run_id"],
        )

    async def finish_run(self, *, stopped_at: datetime, status: str, summary: dict[str, Any]) -> None:
        async with self._db.sessions() as session, session.begin():
            row = await session.get(EngineRunRow, self.run_id)
            if row is not None:
                row.stopped_at = stopped_at
                row.status = status
                row.summary = summary

    async def save_backtest_run(
        self,
        run: RunContext,
        config: AppConfig,
        model: ModelMetadata,
        *,
        start: datetime,
        end: datetime,
        symbols: Sequence[str],
        metrics: dict[str, Any] | None,
        finished_at: datetime | None,
        status: str,
        dataset_version: str | None = None,
    ) -> None:
        await self._upsert(
            BacktestRunRow,
            [
                {
                    "run_id": run.run_id,
                    "created_at": run.started_at,
                    "finished_at": finished_at,
                    "status": status,
                    "config": sanitized_config(config),
                    "model_name": model.model_name,
                    "model_version": model.model_version,
                    "feature_version": model.feature_version,
                    "dataset_version": dataset_version,
                    "start": start,
                    "end": end,
                    "symbols": list(symbols),
                    "metrics": metrics,
                    "git_commit": run.git_commit,
                }
            ],
            ["run_id"],
        )

    async def register_model_version(self, model: ModelMetadata, *, created_at: datetime) -> None:
        await self._insert_ignore(
            ModelVersionRow,
            [
                {
                    "model_name": model.model_name,
                    "model_version": model.model_version,
                    "feature_version": model.feature_version,
                    "dataset_version": model.dataset_version,
                    "training_date": model.training_date,
                    "git_commit": model.git_commit,
                    "params": dict(model.params),
                    "metrics": None,
                    "artifact_path": None,
                    "description": model.description,
                    "created_at": created_at,
                }
            ],
        )

    async def save_experiment(self, row: Mapping[str, Any]) -> None:
        await self._upsert(ExperimentRow, [row], ["experiment_id"])

    async def list_experiments(self, *, limit: int = 20) -> list[dict[str, Any]]:
        statement = select(ExperimentRow).order_by(ExperimentRow.created_at.desc()).limit(limit)
        async with self._db.sessions() as session:
            return [row_to_dict(row) for row in (await session.execute(statement)).scalars().all()]

    async def model_params(self, model_name: str, model_version: str) -> dict[str, Any] | None:
        async with self._db.sessions() as session:
            row = (
                await session.execute(
                    select(ModelVersionRow).where(
                        ModelVersionRow.model_name == model_name,
                        ModelVersionRow.model_version == model_version,
                    )
                )
            ).scalar_one_or_none()
            return dict(row.params) if row is not None else None

    async def upsert_broker_account(
        self,
        account: AccountSnapshot,
        capabilities: BrokerCapabilities,
        *,
        seen_at: datetime,
        session: AsyncSession | None = None,
    ) -> None:
        await self._upsert(
            BrokerAccountRow,
            [
                {
                    "broker": account.broker,
                    "account_ref": account.account_ref,
                    "is_paper": account.is_paper,
                    "currency": account.currency,
                    "status": account.status,
                    "capabilities": capabilities.model_dump(mode="json"),
                    "last_seen_at": seen_at,
                }
            ],
            ["broker", "account_ref"],
            session,
        )

    async def get_state(self, key: str) -> dict[str, Any] | None:
        async with self._db.sessions() as session:
            row = await session.get(SystemStateRow, key)
            return dict(row.value) if row is not None else None

    async def set_state(self, key: str, value: Mapping[str, Any], *, at: datetime) -> None:
        await self._upsert(SystemStateRow, [{"key": key, "value": dict(value), "updated_at": at}], ["key"])

    # ---- batched audit writes (called by AuditRecorder)

    async def insert_bars(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(MarketBarRow, rows, session)

    async def insert_features(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(FeatureRow, rows, session)

    async def insert_predictions(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(PredictionRow, rows, session)

    async def upsert_signals(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._upsert(SignalRow, rows, ["signal_id"], session)

    async def insert_risk_decisions(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(RiskDecisionRow, rows, session)

    async def insert_order_events(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(OrderEventRow, rows, session)

    async def insert_trades(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        model = BacktestTradeRow if self.mode in BACKTEST_MODES else TradeRow
        await self._insert_ignore(model, rows, session)

    async def insert_snapshots(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(PortfolioSnapshotRow, rows, session)

    async def insert_risk_events(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(RiskEventRow, rows, session)

    async def insert_system_events(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(SystemEventRow, rows, session)

    async def insert_broker_events(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        await self._insert_ignore(BrokerEventRow, rows, session)

    async def update_prediction_outcomes(
        self, rows: Sequence[Mapping[str, Any]], *, session: AsyncSession | None = None
    ) -> None:
        if not rows:
            return
        table = cast(Table, PredictionRow.__table__)  # Core table: executemany with custom bind names
        statement = (
            update(table)
            .where(table.c.prediction_id == bindparam("b_id"))
            .values(
                realized_return=bindparam("b_realized"),
                direction_correct=bindparam("b_correct"),
                resolved_at=bindparam("b_resolved", type_=UTCDateTime()),
            )
        )
        await self._execute(statement, [dict(r) for r in rows], session)

    # ---- row builders

    @staticmethod
    def bar_row(bar: Any, quality_status: str | None) -> dict[str, Any]:
        return {
            "symbol": bar.symbol,
            "timeframe": bar.timeframe.value,
            "start": bar.start,
            "end": bar.end,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "vwap": bar.vwap,
            "trade_count": bar.trade_count,
            "source": bar.source,
            "received_at": bar.received_at,
            "quality_status": quality_status,
        }

    def feature_row(self, features: FeatureVector) -> dict[str, Any]:
        return {
            "feature_id": features.feature_id,
            "symbol": features.symbol,
            "ts": features.timestamp,
            "timeframe": features.timeframe.value,
            "feature_version": features.feature_version,
            "spec_hash": features.spec_hash,
            "close": features.close,
            "values": {name: finite(value) for name, value in features.values.items()},
            "quote": features.quote.model_dump(mode="json") if features.quote is not None else None,
            "window_start": features.window_start,
            "window_end": features.window_end,
            "n_bars": features.n_bars,
            "source": features.source,
            "run_id": self.run_id,
        }

    def prediction_row(self, record: PredictionRecord) -> dict[str, Any]:
        p = record.prediction
        return {
            "prediction_id": p.prediction_id,
            "feature_id": p.feature_id,
            "symbol": p.symbol,
            "ts": p.timestamp,
            "horizon_minutes": p.horizon_minutes,
            "direction": p.direction.value,
            "probability_up": p.probability_up,
            "probability_down": p.probability_down,
            "expected_return": p.expected_return,
            "expected_volatility": p.expected_volatility,
            "confidence": p.confidence,
            "model_name": p.model_name,
            "model_version": p.model_version,
            "feature_version": p.feature_version,
            "regime": record.regime.regime.value,
            "no_trade_reason": record.no_trade_reason.value if record.no_trade_reason else None,
            "latency_ms": record.latency_ms,
            "realized_return": None,
            "direction_correct": None,
            "resolved_at": None,
            "run_id": self.run_id,
        }

    def signal_row(self, signal: Signal) -> dict[str, Any]:
        ev = signal.expected_value
        return {
            "signal_id": signal.signal_id,
            "prediction_id": signal.prediction_id,
            "symbol": signal.symbol,
            "ts": signal.timestamp,
            "direction": signal.direction.value,
            "probability": signal.probability,
            "confidence": signal.confidence,
            "expected_return": signal.expected_return,
            "expected_volatility": signal.expected_volatility,
            "horizon_minutes": signal.horizon_minutes,
            "market_regime": signal.market_regime.value,
            "model_name": signal.model_name,
            "model_version": signal.model_version,
            "feature_version": signal.feature_version,
            "reference_price": signal.reference_price,
            "spread_bps": finite(signal.spread_bps),
            "gross_edge_bps": ev.gross_edge_bps if ev else None,
            "cost_bps": ev.costs.total_bps if ev else None,
            "net_edge_bps": ev.net_edge_bps if ev else None,
            "costs": ev.model_dump(mode="json") if ev else None,
            "status": signal.status.value,
            "status_reason": signal.status_reason,
            "rejection_reasons": list(signal.rejection_reasons),
            "expires_at": signal.expires_at,
            "source": signal.source,
            "strategy": signal.strategy,
            "updated_at": signal.updated_at,
            "run_id": self.run_id,
        }

    def decision_row(self, decision: RiskDecision) -> dict[str, Any]:
        return {
            "decision_id": decision.decision_id,
            "signal_id": decision.signal_id,
            "ts": decision.timestamp,
            "verdict": decision.verdict.value,
            "reasons": list(decision.reasons),
            "checks": [check.model_dump(mode="json") for check in decision.checks],
            "side": decision.side.value if decision.side else None,
            "quantity": decision.quantity,
            "entry_reference_price": decision.entry_reference_price,
            "stop_loss": decision.stop_loss,
            "take_profit": decision.take_profit,
            "stop_distance": decision.stop_distance,
            "max_loss": decision.max_loss,
            "risk_reward": decision.risk_reward,
            "notional": decision.notional,
            "sizing_method": decision.sizing_method,
            "account_equity": decision.account_equity,
            "run_id": self.run_id,
        }

    def order_event_row(self, event: OrderEvent) -> dict[str, Any]:
        order = event.order
        return {
            "event_id": event.event_id,
            "client_order_id": order.client_order_id,
            "broker_order_id": order.broker_order_id,
            "broker": event.broker,
            "event_type": event.event_type.value,
            "status": order.status.value,
            "ts": event.timestamp,
            "received_at": event.received_at,
            "fill_quantity": event.fill_quantity,
            "fill_price": event.fill_price,
            "cumulative_quantity": order.filled_quantity,
            "average_fill_price": order.average_fill_price,
            "fee": event.fee,
            "reason": event.reason,
            "raw": dict(event.raw),
            "run_id": self.run_id,
        }

    def trade_row(self, trade: Trade) -> dict[str, Any]:
        data = trade.model_dump(mode="python")
        for key in ("direction", "exit_reason", "market_regime"):
            data[key] = data[key].value
        data["mode"] = self.mode.value
        data["run_id"] = self.run_id
        return data

    def snapshot_row(self, snapshot: PortfolioSnapshot) -> dict[str, Any]:
        data = snapshot.model_dump(mode="python")
        data["ts"] = data.pop("timestamp")
        data["mode"] = self.mode.value
        data["run_id"] = self.run_id
        return data

    def risk_event_row(self, event: RiskEvent) -> dict[str, Any]:
        return {
            "ts": event.timestamp,
            "event_type": event.event_type,
            "reason": event.reason,
            "detail": event.detail,
            "details": dict(event.details),
            "run_id": self.run_id,
        }

    def system_event_row(self, event: SystemEvent) -> dict[str, Any]:
        return {
            "ts": event.timestamp,
            "level": event.level,
            "component": event.component,
            "event_type": event.event_type,
            "message": event.message,
            "details": dict(event.details),
            "run_id": self.run_id,
        }

    def broker_event_row(self, event: BrokerEvent) -> dict[str, Any]:
        return {
            "ts": event.timestamp,
            "broker": event.broker,
            "event_type": event.event_type,
            "details": dict(event.details),
            "run_id": self.run_id,
        }

    # ---- queries

    async def count(self, model: type[Base]) -> int:
        async with self._db.sessions() as session:
            return int((await session.execute(select(func.count()).select_from(model))).scalar_one())

    async def load_signal(self, signal_id: str) -> Signal | None:
        async with self._db.sessions() as session:
            row = await session.get(SignalRow, signal_id)
            if row is None:
                return None
            return Signal(
                signal_id=row.signal_id,
                prediction_id=row.prediction_id,
                symbol=row.symbol,
                timestamp=row.ts,
                direction=row.direction,
                probability=row.probability,
                confidence=row.confidence,
                expected_return=row.expected_return,
                expected_volatility=row.expected_volatility,
                horizon_minutes=row.horizon_minutes,
                market_regime=row.market_regime,
                model_name=row.model_name,
                model_version=row.model_version,
                feature_version=row.feature_version,
                reference_price=row.reference_price,
                spread_bps=row.spread_bps,
                expected_value=ExpectedValue.model_validate(row.costs) if row.costs else None,
                status=row.status,
                status_reason=row.status_reason,
                rejection_reasons=list(row.rejection_reasons or []),
                expires_at=row.expires_at,
                source=row.source,
                strategy=row.strategy,
                updated_at=row.updated_at,
            )

    async def list_signals(
        self, *, limit: int | None = 20, status: str | None = None, run_id: str | None = None
    ) -> list[dict[str, Any]]:
        statement = select(SignalRow).order_by(SignalRow.ts.desc(), SignalRow.signal_id)
        if status is not None:
            statement = statement.where(SignalRow.status == status)
        if run_id is not None:
            statement = statement.where(SignalRow.run_id == run_id)
        if limit is not None:
            statement = statement.limit(limit)
        async with self._db.sessions() as session:
            return [row_to_dict(row) for row in (await session.execute(statement)).scalars().all()]

    async def decision_trace(self, signal_id: str) -> dict[str, Any] | None:
        """Everything needed to reconstruct one decision (spec §55)."""
        async with self._db.sessions() as session:
            signal = await session.get(SignalRow, signal_id)
            if signal is None:
                return None
            prediction = await session.get(PredictionRow, signal.prediction_id)
            features = (
                await session.get(FeatureRow, prediction.feature_id) if prediction is not None else None
            )
            bars: list[dict[str, Any]] = []
            if features is not None:
                bar_rows = (
                    (
                        await session.execute(
                            select(MarketBarRow)
                            .where(
                                MarketBarRow.symbol == features.symbol,
                                MarketBarRow.timeframe == features.timeframe,
                                MarketBarRow.source == features.source,
                                MarketBarRow.start >= features.window_start,
                                MarketBarRow.start < features.window_end,
                            )
                            .order_by(MarketBarRow.start)
                        )
                    )
                    .scalars()
                    .all()
                )
                bars = [row_to_dict(row) for row in bar_rows]
            decision = (
                await session.execute(select(RiskDecisionRow).where(RiskDecisionRow.signal_id == signal_id))
            ).scalar_one_or_none()
            orders = (
                (
                    await session.execute(
                        select(OrderRow).where(OrderRow.signal_id == signal_id).order_by(OrderRow.created_at)
                    )
                )
                .scalars()
                .all()
            )
            order_ids = [o.client_order_id for o in orders]
            events = (
                (
                    await session.execute(
                        select(OrderEventRow)
                        .where(OrderEventRow.client_order_id.in_(order_ids))
                        .order_by(OrderEventRow.ts, OrderEventRow.event_id)
                    )
                )
                .scalars()
                .all()
                if order_ids
                else []
            )
            trade_model: type[TradeRow] | type[BacktestTradeRow]
            trade = None
            for trade_model in (TradeRow, BacktestTradeRow):
                trade = (
                    await session.execute(select(trade_model).where(trade_model.signal_id == signal_id))
                ).scalar_one_or_none()
                if trade is not None:
                    break
            run = await session.get(EngineRunRow, signal.run_id)
            return {
                "signal": row_to_dict(signal),
                "prediction": row_to_dict(prediction) if prediction is not None else None,
                "features": row_to_dict(features) if features is not None else None,
                "bars": bars,
                "risk_decision": row_to_dict(decision) if decision is not None else None,
                "orders": [row_to_dict(o) for o in orders],
                "order_events": [row_to_dict(e) for e in events],
                "trade": row_to_dict(trade) if trade is not None else None,
                "run": {
                    "run_id": run.run_id,
                    "mode": run.mode,
                    "namespace": run.namespace,
                    "git_commit": run.git_commit,
                    "config_hash": run.config_hash,
                    "config": dict(run.config),
                }
                if run is not None
                else None,
            }
