"""Normalized domain entities shared by every layer.

Adapters translate native broker/provider payloads into these types. No layer above the adapters ever
sees a provider-specific format. All timestamps are timezone-aware UTC.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from datetime import datetime
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from packages.common.clock import ensure_utc
from packages.common.enums import (
    AssetClass,
    ConnectionStatus,
    DataQualityStatus,
    Direction,
    MarketRegime,
    OrderClass,
    OrderEventType,
    OrderIntent,
    OrderStatus,
    OrderType,
    RiskVerdict,
    Side,
    SignalStatus,
    TimeInForce,
    Timeframe,
)
from packages.common.errors import InvalidSignalTransition

UtcDatetime = Annotated[datetime, AfterValidator(ensure_utc)]

CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:\-]{1,64}$")


class Frozen(BaseModel):
    """Immutable value object."""

    model_config = ConfigDict(frozen=True, extra="forbid", protected_namespaces=())


class Mutable(BaseModel):
    """Entity whose state evolves (signals, orders)."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())


# --------------------------------------------------------------------------- instruments & market data


class InstrumentInfo(Frozen):
    symbol: str
    asset_class: AssetClass = AssetClass.US_EQUITY
    tradable: bool = True
    shortable: bool = True
    easy_to_borrow: bool = True
    fractionable: bool = False
    tick_size: float = 0.01
    min_quantity: float = 1.0
    quantity_increment: float = 1.0


class MarketBar(Frozen):
    """OHLCV bar. `start` is the beginning of the interval; the bar is known at `end`."""

    symbol: str
    timeframe: Timeframe
    start: UtcDatetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    trade_count: int | None = None
    source: str
    received_at: UtcDatetime | None = None

    @property
    def end(self) -> datetime:
        return self.start + self.timeframe.delta

    @property
    def typical_price(self) -> float:
        if self.vwap is not None and self.vwap > 0:
            return self.vwap
        return (self.high + self.low + self.close) / 3.0

    def same_values(self, other: MarketBar) -> bool:
        return (
            self.open == other.open
            and self.high == other.high
            and self.low == other.low
            and self.close == other.close
            and self.volume == other.volume
        )


class MarketQuote(Frozen):
    symbol: str
    timestamp: UtcDatetime
    bid: float
    ask: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    source: str
    received_at: UtcDatetime | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def spread_bps(self) -> float:
        mid = self.mid
        return (self.spread / mid) * 1e4 if mid > 0 else math.inf

    @property
    def imbalance(self) -> float:
        total = self.bid_size + self.ask_size
        return (self.bid_size - self.ask_size) / total if total > 0 else 0.0


class MarketTrade(Frozen):
    symbol: str
    timestamp: UtcDatetime
    price: float
    size: float
    source: str
    exchange: str | None = None
    conditions: tuple[str, ...] = ()
    received_at: UtcDatetime | None = None


class MarketTick(Frozen):
    """Merged last-trade + top-of-book snapshot (spec §10 example)."""

    symbol: str
    timestamp: UtcDatetime
    price: float
    volume: float
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    source: str
    received_at: UtcDatetime | None = None


MarketEvent = MarketBar | MarketQuote | MarketTrade


def event_time(event: MarketEvent) -> datetime:
    """Time at which a market event becomes known."""
    return event.end if isinstance(event, MarketBar) else event.timestamp


class DataIssue(Frozen):
    code: str
    status: DataQualityStatus
    detail: str = ""


class DataQualityReport(Frozen):
    symbol: str
    timestamp: UtcDatetime
    status: DataQualityStatus
    issues: tuple[DataIssue, ...] = ()

    @property
    def codes(self) -> list[str]:
        return [issue.code for issue in self.issues]


# --------------------------------------------------------------------------- features & model


class FeatureVector(Frozen):
    """Features as of `timestamp` (close of the last bar), computed only from bars in the window."""

    feature_id: str
    symbol: str
    timestamp: UtcDatetime
    timeframe: Timeframe
    feature_version: str
    spec_hash: str
    close: float
    values: dict[str, float]
    window_start: UtcDatetime
    window_end: UtcDatetime
    n_bars: int
    source: str = ""
    quote: MarketQuote | None = None

    def get(self, name: str) -> float:
        return self.values.get(name, math.nan)

    def missing(self, names: Iterable[str]) -> list[str]:
        return [name for name in names if not math.isfinite(self.values.get(name, math.nan))]


class ModelMetadata(Frozen):
    model_name: str
    model_version: str
    feature_version: str
    horizon_minutes: int
    dataset_version: str | None = None
    training_date: UtcDatetime | None = None
    git_commit: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    description: str = ""

    @property
    def key(self) -> str:
        return f"{self.model_name}@{self.model_version}"


class JEVPrediction(Frozen):
    prediction_id: str
    symbol: str
    timestamp: UtcDatetime
    horizon_minutes: int
    direction: Direction
    probability_up: float = Field(ge=0.0, le=1.0)
    probability_down: float = Field(ge=0.0, le=1.0)
    expected_return: float
    expected_volatility: float = Field(ge=0.0)
    confidence: float = Field(ge=0.0, le=1.0)
    model_name: str
    model_version: str
    feature_version: str
    feature_id: str

    @model_validator(mode="after")
    def _check(self) -> JEVPrediction:
        if self.probability_up + self.probability_down > 1.0 + 1e-9:
            raise ValueError("probability_up + probability_down must not exceed 1")
        if not (math.isfinite(self.expected_return) and math.isfinite(self.expected_volatility)):
            raise ValueError("expected_return and expected_volatility must be finite")
        return self

    @property
    def directional_probability(self) -> float:
        if self.direction is Direction.LONG:
            return self.probability_up
        if self.direction is Direction.SHORT:
            return self.probability_down
        return 0.0


class RegimeAssessment(Frozen):
    symbol: str
    timestamp: UtcDatetime
    regime: MarketRegime
    adx: float | None = None
    trend_strength: float | None = None
    volatility_ratio: float | None = None
    reason: str = ""


# --------------------------------------------------------------------------- signals


class CostEstimate(Frozen):
    """Estimated round-trip trading costs in basis points of notional."""

    spread_bps: float
    slippage_bps: float
    commission_bps: float
    regulatory_bps: float
    latency_bps: float
    total_bps: float

    @classmethod
    def of(
        cls,
        *,
        spread_bps: float,
        slippage_bps: float,
        commission_bps: float,
        regulatory_bps: float,
        latency_bps: float,
    ) -> CostEstimate:
        total = spread_bps + slippage_bps + commission_bps + regulatory_bps + latency_bps
        return cls(
            spread_bps=spread_bps,
            slippage_bps=slippage_bps,
            commission_bps=commission_bps,
            regulatory_bps=regulatory_bps,
            latency_bps=latency_bps,
            total_bps=total,
        )


class ExpectedValue(Frozen):
    gross_edge_bps: float
    costs: CostEstimate
    net_edge_bps: float
    edge_to_volatility: float


SIGNAL_TRANSITIONS: dict[SignalStatus, frozenset[SignalStatus]] = {
    SignalStatus.GENERATED: frozenset({SignalStatus.ELIGIBLE, SignalStatus.REJECTED}),
    SignalStatus.ELIGIBLE: frozenset({SignalStatus.APPROVED, SignalStatus.REJECTED, SignalStatus.EXPIRED}),
    SignalStatus.APPROVED: frozenset({SignalStatus.EXECUTED, SignalStatus.EXPIRED, SignalStatus.REJECTED}),
    SignalStatus.REJECTED: frozenset(),
    SignalStatus.EXPIRED: frozenset(),
    SignalStatus.EXECUTED: frozenset(),
}


class Signal(Mutable):
    signal_id: str
    prediction_id: str
    symbol: str
    timestamp: UtcDatetime
    direction: Direction
    probability: float
    confidence: float
    expected_return: float
    expected_volatility: float
    horizon_minutes: int
    market_regime: MarketRegime
    model_name: str
    model_version: str
    feature_version: str
    reference_price: float
    spread_bps: float | None = None
    expected_value: ExpectedValue | None = None
    status: SignalStatus = SignalStatus.GENERATED
    status_reason: str | None = None
    rejection_reasons: list[str] = Field(default_factory=list)
    expires_at: UtcDatetime
    source: str = "jev"
    strategy: str
    updated_at: UtcDatetime

    def transition(self, status: SignalStatus, at: datetime, reason: str | None = None) -> None:
        if status not in SIGNAL_TRANSITIONS[self.status]:
            raise InvalidSignalTransition(f"{self.signal_id}: {self.status} -> {status} not allowed")
        self.status = status
        self.updated_at = ensure_utc(at)
        if reason is not None:
            self.status_reason = reason


# --------------------------------------------------------------------------- risk


class RiskCheck(Frozen):
    name: str
    passed: bool
    detail: str = ""
    value: float | None = None
    limit: float | None = None


class RiskDecision(Frozen):
    decision_id: str
    signal_id: str
    timestamp: UtcDatetime
    verdict: RiskVerdict
    reasons: tuple[str, ...] = ()
    checks: tuple[RiskCheck, ...] = ()
    side: Side | None = None
    quantity: float = 0.0
    entry_reference_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    stop_distance: float | None = None
    max_loss: float | None = None
    risk_reward: float | None = None
    notional: float | None = None
    sizing_method: str | None = None
    account_equity: float | None = None

    @property
    def approved(self) -> bool:
        return self.verdict is RiskVerdict.APPROVED


# --------------------------------------------------------------------------- orders


class OrderRequest(Frozen):
    client_order_id: str
    symbol: str
    side: Side
    quantity: float = Field(gt=0)
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: float | None = None
    stop_price: float | None = None
    order_class: OrderClass = OrderClass.SIMPLE
    take_profit_price: float | None = None
    stop_loss_price: float | None = None
    intent: OrderIntent
    signal_id: str | None = None
    asset_class: AssetClass = AssetClass.US_EQUITY
    extended_hours: bool = False

    @model_validator(mode="after")
    def _check(self) -> OrderRequest:
        if not CLIENT_ORDER_ID_PATTERN.match(self.client_order_id):
            raise ValueError(f"invalid client_order_id: {self.client_order_id!r}")
        if self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and self.limit_price is None:
            raise ValueError("limit orders need limit_price")
        if self.order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and self.stop_price is None:
            raise ValueError("stop orders need stop_price")
        if self.order_class is OrderClass.BRACKET:
            tp, sl = self.take_profit_price, self.stop_loss_price
            if tp is None or sl is None:
                raise ValueError("bracket orders need take_profit_price and stop_loss_price")
            if self.side is Side.BUY and not sl < tp:
                raise ValueError("buy bracket requires stop_loss_price < take_profit_price")
            if self.side is Side.SELL and not tp < sl:
                raise ValueError("sell bracket requires take_profit_price < stop_loss_price")
        return self


class OrderReplace(Frozen):
    quantity: float | None = Field(default=None, gt=0)
    limit_price: float | None = None
    stop_price: float | None = None
    time_in_force: TimeInForce | None = None


class Order(Mutable):
    """Normalized order state (local record or the broker's view)."""

    client_order_id: str
    broker: str
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType
    time_in_force: TimeInForce
    intent: OrderIntent
    status: OrderStatus = OrderStatus.CREATED
    broker_order_id: str | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    order_class: OrderClass = OrderClass.SIMPLE
    take_profit_price: float | None = None
    stop_loss_price: float | None = None
    parent_client_order_id: str | None = None
    leg_client_order_ids: list[str] = Field(default_factory=list)
    legs: list[Order] = Field(default_factory=list)
    signal_id: str | None = None
    asset_class: AssetClass = AssetClass.US_EQUITY
    filled_quantity: float = 0.0
    average_fill_price: float | None = None
    reject_reason: str | None = None
    created_at: UtcDatetime
    submitted_at: UtcDatetime | None = None
    updated_at: UtcDatetime

    @property
    def remaining_quantity(self) -> float:
        return max(0.0, self.quantity - self.filled_quantity)

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @classmethod
    def from_request(cls, request: OrderRequest, *, broker: str, at: datetime) -> Order:
        return cls(
            client_order_id=request.client_order_id,
            broker=broker,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            order_type=request.order_type,
            time_in_force=request.time_in_force,
            intent=request.intent,
            limit_price=request.limit_price,
            stop_price=request.stop_price,
            order_class=request.order_class,
            take_profit_price=request.take_profit_price,
            stop_loss_price=request.stop_loss_price,
            signal_id=request.signal_id,
            asset_class=request.asset_class,
            created_at=at,
            updated_at=at,
        )

    def snapshot(self) -> Order:
        """Deep copy, safe to hand to another component."""
        return self.model_copy(deep=True)


Order.model_rebuild()


class OrderEvent(Frozen):
    """Broker event, carrying the broker's full view of the order after the event."""

    event_id: str
    broker: str
    event_type: OrderEventType
    timestamp: UtcDatetime
    order: Order
    fill_quantity: float | None = None
    fill_price: float | None = None
    fee: float = 0.0
    reason: str | None = None
    received_at: UtcDatetime | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @property
    def client_order_id(self) -> str:
        return self.order.client_order_id


class Fill(Frozen):
    fill_id: str
    client_order_id: str
    broker: str
    symbol: str
    side: Side
    quantity: float
    price: float
    fee: float = 0.0
    timestamp: UtcDatetime
    intent: OrderIntent
    signal_id: str | None = None


# --------------------------------------------------------------------------- account & portfolio


class Position(Frozen):
    broker: str
    symbol: str
    quantity: float
    average_entry_price: float
    market_price: float | None = None
    asset_class: AssetClass = AssetClass.US_EQUITY

    @property
    def market_value(self) -> float:
        price = self.market_price if self.market_price is not None else self.average_entry_price
        return self.quantity * price

    @property
    def unrealized_pnl(self) -> float:
        if self.market_price is None:
            return 0.0
        return self.quantity * (self.market_price - self.average_entry_price)


class AccountSnapshot(Frozen):
    broker: str
    account_ref: str
    is_paper: bool
    currency: str = "USD"
    cash: float
    equity: float
    buying_power: float
    last_equity: float
    long_market_value: float = 0.0
    short_market_value: float = 0.0
    status: str = "ACTIVE"
    timestamp: UtcDatetime

    @property
    def daily_pnl(self) -> float:
        return self.equity - self.last_equity


class PortfolioSnapshot(Frozen):
    timestamp: UtcDatetime
    broker: str
    cash: float
    equity: float
    buying_power: float
    gross_exposure: float
    net_exposure: float
    open_positions: int
    daily_pnl: float
    peak_equity: float
    drawdown: float


class Trade(Frozen):
    """Round trip. Reference prices separate model quality from execution quality."""

    trade_id: str
    signal_id: str
    broker: str
    symbol: str
    direction: Direction
    quantity: float
    entry_time: UtcDatetime
    entry_price: float
    entry_reference_price: float
    exit_time: UtcDatetime
    exit_price: float
    exit_reference_price: float
    exit_reason: OrderIntent
    gross_pnl: float
    fees: float
    net_pnl: float
    model_pnl: float
    execution_shortfall: float
    entry_slippage_bps: float
    exit_slippage_bps: float
    mae_bps: float
    mfe_bps: float
    holding_minutes: float
    model_name: str
    model_version: str
    market_regime: MarketRegime
    confidence: float
    probability: float


# --------------------------------------------------------------------------- broker & feed status


class BrokerCapabilities(Frozen):
    broker: str
    is_paper: bool
    asset_classes: tuple[AssetClass, ...]
    supports_short: bool
    supports_fractional: bool
    supports_bracket: bool
    supports_replace: bool
    supports_extended_hours: bool
    max_client_order_id_length: int = 64


class BrokerHealth(Frozen):
    broker: str
    status: ConnectionStatus
    connected: bool
    order_stream_connected: bool
    account_available: bool
    latency_ms: float | None = None
    last_event_at: UtcDatetime | None = None
    server_time: UtcDatetime | None = None
    detail: str = ""


class MarketDataHealth(Frozen):
    provider: str
    status: ConnectionStatus
    connected: bool
    last_message_at: UtcDatetime | None = None
    subscribed_symbols: tuple[str, ...] = ()
    detail: str = ""
