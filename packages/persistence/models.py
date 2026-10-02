"""SQL schema. Tables from spec §40 plus `engine_runs` and `system_state`.

Audit chain keys: market_bars → features.feature_id → model_predictions.prediction_id → signals.signal_id →
risk_decisions / orders → order_events → trades.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, Dialect, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware UTC datetimes on every backend (SQLite stores naive UTC)."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetimes cannot be stored")
        value = value.astimezone(UTC)
        return value if dialect.name == "postgresql" else value.replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


JsonType = JSON().with_variant(JSONB(), "postgresql")
ID = String(96)


class Base(DeclarativeBase):
    pass


class EngineRunRow(Base):
    __tablename__ = "engine_runs"
    run_id: Mapped[str] = mapped_column(ID, primary_key=True)
    mode: Mapped[str] = mapped_column(String(16))
    broker: Mapped[str] = mapped_column(String(32))
    market_data: Mapped[str] = mapped_column(String(32))
    strategy: Mapped[str] = mapped_column(String(64))
    namespace: Mapped[str] = mapped_column(String(160))
    config_hash: Mapped[str] = mapped_column(String(32))
    config: Mapped[dict[str, Any]] = mapped_column(JsonType)
    git_commit: Mapped[str | None] = mapped_column(String(64))
    model_name: Mapped[str] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(32))
    started_at: Mapped[datetime] = mapped_column(UTCDateTime)
    stopped_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    status: Mapped[str] = mapped_column(String(16))
    summary: Mapped[dict[str, Any] | None] = mapped_column(JsonType)


class MarketBarRow(Base):
    __tablename__ = "market_bars"
    __table_args__ = (UniqueConstraint("symbol", "timeframe", "start", "source", name="uq_market_bars"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timeframe: Mapped[str] = mapped_column(String(8))
    start: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    end: Mapped[datetime] = mapped_column(UTCDateTime)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float | None] = mapped_column(Float)
    trade_count: Mapped[int | None] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String(64))
    received_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    quality_status: Mapped[str | None] = mapped_column(String(16))


class MarketTickRow(Base):
    __tablename__ = "market_ticks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    price: Mapped[float | None] = mapped_column(Float)
    volume: Mapped[float | None] = mapped_column(Float)
    bid: Mapped[float | None] = mapped_column(Float)
    ask: Mapped[float | None] = mapped_column(Float)
    bid_size: Mapped[float | None] = mapped_column(Float)
    ask_size: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(64))
    received_at: Mapped[datetime | None] = mapped_column(UTCDateTime)


class FeatureRow(Base):
    __tablename__ = "features"
    feature_id: Mapped[str] = mapped_column(ID, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    timeframe: Mapped[str] = mapped_column(String(8))
    feature_version: Mapped[str] = mapped_column(String(32))
    spec_hash: Mapped[str] = mapped_column(String(32))
    close: Mapped[float] = mapped_column(Float)
    values: Mapped[dict[str, Any]] = mapped_column(JsonType)
    quote: Mapped[dict[str, Any] | None] = mapped_column(JsonType)
    window_start: Mapped[datetime] = mapped_column(UTCDateTime)
    window_end: Mapped[datetime] = mapped_column(UTCDateTime)
    n_bars: Mapped[int] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str] = mapped_column(ID, index=True)


class PredictionRow(Base):
    __tablename__ = "model_predictions"
    prediction_id: Mapped[str] = mapped_column(ID, primary_key=True)
    feature_id: Mapped[str] = mapped_column(ID, index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    horizon_minutes: Mapped[int] = mapped_column(Integer)
    direction: Mapped[str] = mapped_column(String(16))
    probability_up: Mapped[float] = mapped_column(Float)
    probability_down: Mapped[float] = mapped_column(Float)
    expected_return: Mapped[float] = mapped_column(Float)
    expected_volatility: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float)
    model_name: Mapped[str] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(32))
    regime: Mapped[str | None] = mapped_column(String(24))
    no_trade_reason: Mapped[str | None] = mapped_column(String(32))
    latency_ms: Mapped[float | None] = mapped_column(Float)
    realized_return: Mapped[float | None] = mapped_column(Float)
    direction_correct: Mapped[bool | None] = mapped_column(Boolean)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class SignalRow(Base):
    __tablename__ = "signals"
    signal_id: Mapped[str] = mapped_column(ID, primary_key=True)
    prediction_id: Mapped[str] = mapped_column(ID, index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    direction: Mapped[str] = mapped_column(String(16))
    probability: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float)
    expected_return: Mapped[float] = mapped_column(Float)
    expected_volatility: Mapped[float] = mapped_column(Float)
    horizon_minutes: Mapped[int] = mapped_column(Integer)
    market_regime: Mapped[str] = mapped_column(String(24))
    model_name: Mapped[str] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(32))
    reference_price: Mapped[float] = mapped_column(Float)
    spread_bps: Mapped[float | None] = mapped_column(Float)
    gross_edge_bps: Mapped[float | None] = mapped_column(Float)
    cost_bps: Mapped[float | None] = mapped_column(Float)
    net_edge_bps: Mapped[float | None] = mapped_column(Float)
    costs: Mapped[dict[str, Any] | None] = mapped_column(JsonType)
    status: Mapped[str] = mapped_column(String(16), index=True)
    status_reason: Mapped[str | None] = mapped_column(String(128))
    rejection_reasons: Mapped[list[str]] = mapped_column(JsonType)
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime)
    source: Mapped[str] = mapped_column(String(32))
    strategy: Mapped[str] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class RiskDecisionRow(Base):
    __tablename__ = "risk_decisions"
    decision_id: Mapped[str] = mapped_column(ID, primary_key=True)
    signal_id: Mapped[str] = mapped_column(ID, index=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime)
    verdict: Mapped[str] = mapped_column(String(16))
    reasons: Mapped[list[str]] = mapped_column(JsonType)
    checks: Mapped[list[dict[str, Any]]] = mapped_column(JsonType)
    side: Mapped[str | None] = mapped_column(String(8))
    quantity: Mapped[float] = mapped_column(Float)
    entry_reference_price: Mapped[float | None] = mapped_column(Float)
    stop_loss: Mapped[float | None] = mapped_column(Float)
    take_profit: Mapped[float | None] = mapped_column(Float)
    stop_distance: Mapped[float | None] = mapped_column(Float)
    max_loss: Mapped[float | None] = mapped_column(Float)
    risk_reward: Mapped[float | None] = mapped_column(Float)
    notional: Mapped[float | None] = mapped_column(Float)
    sizing_method: Mapped[str | None] = mapped_column(String(32))
    account_equity: Mapped[float | None] = mapped_column(Float)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class OrderRow(Base):
    __tablename__ = "orders"
    client_order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(96))
    broker: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8))
    quantity: Mapped[float] = mapped_column(Float)
    order_type: Mapped[str] = mapped_column(String(16))
    time_in_force: Mapped[str] = mapped_column(String(8))
    order_class: Mapped[str] = mapped_column(String(16))
    intent: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(20), index=True)
    limit_price: Mapped[float | None] = mapped_column(Float)
    stop_price: Mapped[float | None] = mapped_column(Float)
    take_profit_price: Mapped[float | None] = mapped_column(Float)
    stop_loss_price: Mapped[float | None] = mapped_column(Float)
    parent_client_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    leg_client_order_ids: Mapped[list[str]] = mapped_column(JsonType)
    signal_id: Mapped[str | None] = mapped_column(ID, index=True)
    asset_class: Mapped[str] = mapped_column(String(16))
    filled_quantity: Mapped[float] = mapped_column(Float)
    average_fill_price: Mapped[float | None] = mapped_column(Float)
    reject_reason: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    submitted_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)
    mode: Mapped[str] = mapped_column(String(16))
    run_id: Mapped[str] = mapped_column(ID, index=True)


class OrderEventRow(Base):
    __tablename__ = "order_events"
    event_id: Mapped[str] = mapped_column(ID, primary_key=True)
    client_order_id: Mapped[str] = mapped_column(String(64), index=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(96))
    broker: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(24))
    status: Mapped[str] = mapped_column(String(20))
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    received_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    fill_quantity: Mapped[float | None] = mapped_column(Float)
    fill_price: Mapped[float | None] = mapped_column(Float)
    cumulative_quantity: Mapped[float] = mapped_column(Float)
    average_fill_price: Mapped[float | None] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    reason: Mapped[str | None] = mapped_column(String(128))
    raw: Mapped[dict[str, Any]] = mapped_column(JsonType)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class PositionRow(Base):
    __tablename__ = "positions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    broker: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32))
    quantity: Mapped[float] = mapped_column(Float)
    average_entry_price: Mapped[float] = mapped_column(Float)
    market_price: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(16))
    run_id: Mapped[str] = mapped_column(ID, index=True)


class _TradeColumns:
    trade_id: Mapped[str] = mapped_column(ID, primary_key=True)
    signal_id: Mapped[str] = mapped_column(ID, index=True)
    broker: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    direction: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[float] = mapped_column(Float)
    entry_time: Mapped[datetime] = mapped_column(UTCDateTime)
    entry_price: Mapped[float] = mapped_column(Float)
    entry_reference_price: Mapped[float] = mapped_column(Float)
    exit_time: Mapped[datetime] = mapped_column(UTCDateTime)
    exit_price: Mapped[float] = mapped_column(Float)
    exit_reference_price: Mapped[float] = mapped_column(Float)
    exit_reason: Mapped[str] = mapped_column(String(16))
    gross_pnl: Mapped[float] = mapped_column(Float)
    fees: Mapped[float] = mapped_column(Float)
    net_pnl: Mapped[float] = mapped_column(Float)
    model_pnl: Mapped[float] = mapped_column(Float)
    execution_shortfall: Mapped[float] = mapped_column(Float)
    entry_slippage_bps: Mapped[float] = mapped_column(Float)
    exit_slippage_bps: Mapped[float] = mapped_column(Float)
    mae_bps: Mapped[float] = mapped_column(Float)
    mfe_bps: Mapped[float] = mapped_column(Float)
    holding_minutes: Mapped[float] = mapped_column(Float)
    model_name: Mapped[str] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(32))
    market_regime: Mapped[str] = mapped_column(String(24))
    confidence: Mapped[float] = mapped_column(Float)
    probability: Mapped[float] = mapped_column(Float)
    mode: Mapped[str] = mapped_column(String(16))
    run_id: Mapped[str] = mapped_column(ID, index=True)


class TradeRow(_TradeColumns, Base):
    __tablename__ = "trades"


class BacktestTradeRow(_TradeColumns, Base):
    __tablename__ = "backtest_trades"


class PortfolioSnapshotRow(Base):
    __tablename__ = "portfolio_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    broker: Mapped[str] = mapped_column(String(32))
    cash: Mapped[float] = mapped_column(Float)
    equity: Mapped[float] = mapped_column(Float)
    buying_power: Mapped[float] = mapped_column(Float)
    gross_exposure: Mapped[float] = mapped_column(Float)
    net_exposure: Mapped[float] = mapped_column(Float)
    open_positions: Mapped[int] = mapped_column(Integer)
    daily_pnl: Mapped[float] = mapped_column(Float)
    peak_equity: Mapped[float] = mapped_column(Float)
    drawdown: Mapped[float] = mapped_column(Float)
    mode: Mapped[str] = mapped_column(String(16))
    run_id: Mapped[str] = mapped_column(ID, index=True)


class RiskEventRow(Base):
    __tablename__ = "risk_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    event_type: Mapped[str] = mapped_column(String(48))
    reason: Mapped[str] = mapped_column(String(48))
    detail: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, Any]] = mapped_column(JsonType)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class SystemEventRow(Base):
    __tablename__ = "system_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    level: Mapped[str] = mapped_column(String(16))
    component: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(64))
    message: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, Any]] = mapped_column(JsonType)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class ModelVersionRow(Base):
    __tablename__ = "model_versions"
    __table_args__ = (UniqueConstraint("model_name", "model_version", name="uq_model_versions"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_name: Mapped[str] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(32))
    dataset_version: Mapped[str | None] = mapped_column(String(64))
    training_date: Mapped[datetime | None] = mapped_column(UTCDateTime)
    git_commit: Mapped[str | None] = mapped_column(String(64))
    params: Mapped[dict[str, Any]] = mapped_column(JsonType)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JsonType)
    artifact_path: Mapped[str | None] = mapped_column(String(256))
    description: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)


class BacktestRunRow(Base):
    __tablename__ = "backtest_runs"
    run_id: Mapped[str] = mapped_column(ID, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    status: Mapped[str] = mapped_column(String(16))
    config: Mapped[dict[str, Any]] = mapped_column(JsonType)
    model_name: Mapped[str] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(32))
    feature_version: Mapped[str] = mapped_column(String(32))
    dataset_version: Mapped[str | None] = mapped_column(String(64))
    start: Mapped[datetime] = mapped_column(UTCDateTime)
    end: Mapped[datetime] = mapped_column(UTCDateTime)
    symbols: Mapped[list[str]] = mapped_column(JsonType)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JsonType)
    git_commit: Mapped[str | None] = mapped_column(String(64))


class ExperimentRow(Base):
    """Experiment tracking (phase 3): which models, data, folds, configuration and code produced which results."""

    __tablename__ = "experiments"
    experiment_id: Mapped[str] = mapped_column(ID, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    dataset: Mapped[str] = mapped_column(String(96))
    dataset_version: Mapped[str] = mapped_column(String(64))
    models: Mapped[list[str]] = mapped_column(JsonType)
    folds: Mapped[list[dict[str, Any]]] = mapped_column(JsonType)
    config: Mapped[dict[str, Any]] = mapped_column(JsonType)
    git_commit: Mapped[str | None] = mapped_column(String(64))
    output_dir: Mapped[str] = mapped_column(String(512))
    results: Mapped[dict[str, Any]] = mapped_column(JsonType)


class BrokerAccountRow(Base):
    __tablename__ = "broker_accounts"
    __table_args__ = (UniqueConstraint("broker", "account_ref", name="uq_broker_accounts"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker: Mapped[str] = mapped_column(String(32))
    account_ref: Mapped[str] = mapped_column(String(64))
    is_paper: Mapped[bool] = mapped_column(Boolean)
    currency: Mapped[str] = mapped_column(String(8))
    status: Mapped[str] = mapped_column(String(32))
    capabilities: Mapped[dict[str, Any]] = mapped_column(JsonType)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime)


class BrokerEventRow(Base):
    __tablename__ = "broker_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(UTCDateTime, index=True)
    broker: Mapped[str] = mapped_column(String(32))
    event_type: Mapped[str] = mapped_column(String(48))
    details: Mapped[dict[str, Any]] = mapped_column(JsonType)
    run_id: Mapped[str] = mapped_column(ID, index=True)


class SystemStateRow(Base):
    __tablename__ = "system_state"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JsonType)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)
