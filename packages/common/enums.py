"""Shared enumerations. Values are persisted and exposed through the API: keep them stable."""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum


class AssetClass(StrEnum):
    US_EQUITY = "US_EQUITY"
    ETF = "ETF"
    CRYPTO = "CRYPTO"
    FOREX = "FOREX"
    FUTURES = "FUTURES"


class TradingMode(StrEnum):
    BACKTEST = "backtest"
    REPLAY = "replay"
    SHADOW = "shadow"
    PAPER = "paper"
    LIVE = "live"


class Timeframe(StrEnum):
    MIN_1 = "1Min"
    MIN_5 = "5Min"
    MIN_15 = "15Min"
    HOUR_1 = "1Hour"

    @property
    def minutes(self) -> int:
        return _TIMEFRAME_MINUTES[self]

    @property
    def seconds(self) -> int:
        return self.minutes * 60

    @property
    def delta(self) -> timedelta:
        return timedelta(minutes=self.minutes)


_TIMEFRAME_MINUTES = {Timeframe.MIN_1: 1, Timeframe.MIN_5: 5, Timeframe.MIN_15: 15, Timeframe.HOUR_1: 60}


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_TRADE = "NO_TRADE"

    @property
    def sign(self) -> int:
        if self is Direction.LONG:
            return 1
        if self is Direction.SHORT:
            return -1
        return 0

    @property
    def entry_side(self) -> Side:
        if self is Direction.LONG:
            return Side.BUY
        if self is Direction.SHORT:
            return Side.SELL
        raise ValueError("NO_TRADE has no entry side")


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class TimeInForce(StrEnum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


class OrderClass(StrEnum):
    SIMPLE = "simple"
    BRACKET = "bracket"


class OrderIntent(StrEnum):
    ENTRY = "entry"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    TIME_EXIT = "time_exit"
    EOD_EXIT = "eod_exit"
    KILL_EXIT = "kill_exit"
    RISK_EXIT = "risk_exit"
    MANUAL = "manual"

    @property
    def code(self) -> str:
        """Short code used inside deterministic client order ids."""
        return _INTENT_CODES[self]

    @property
    def is_exit(self) -> bool:
        return self not in (OrderIntent.ENTRY, OrderIntent.MANUAL)


_INTENT_CODES = {
    OrderIntent.ENTRY: "en",
    OrderIntent.TAKE_PROFIT: "tp",
    OrderIntent.STOP_LOSS: "sl",
    OrderIntent.TIME_EXIT: "tx",
    OrderIntent.EOD_EXIT: "ex",
    OrderIntent.KILL_EXIT: "kx",
    OrderIntent.RISK_EXIT: "rx",
    OrderIntent.MANUAL: "mn",
}


class OrderStatus(StrEnum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    ERROR = "ERROR"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_ORDER_STATUSES


TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
)


class OrderEventType(StrEnum):
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIAL_FILL = "partial_fill"
    FILL = "fill"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    REPLACED = "replaced"
    ERROR = "error"


class SignalStatus(StrEnum):
    GENERATED = "GENERATED"
    ELIGIBLE = "ELIGIBLE"
    REJECTED = "REJECTED"
    APPROVED = "APPROVED"
    EXPIRED = "EXPIRED"
    EXECUTED = "EXECUTED"


class RiskVerdict(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class DataQualityStatus(StrEnum):
    VALID = "VALID"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    INVALID = "INVALID"

    @property
    def severity(self) -> int:
        return _QUALITY_SEVERITY[self]

    @staticmethod
    def worst(statuses: list[DataQualityStatus]) -> DataQualityStatus:
        return max(statuses, key=lambda s: s.severity, default=DataQualityStatus.VALID)


_QUALITY_SEVERITY = {
    DataQualityStatus.VALID: 0,
    DataQualityStatus.DEGRADED: 1,
    DataQualityStatus.STALE: 2,
    DataQualityStatus.INVALID: 3,
}


class MarketRegime(StrEnum):
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGE = "RANGE"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    LOW_VOLATILITY = "LOW_VOLATILITY"
    UNKNOWN = "UNKNOWN"


class ConnectionStatus(StrEnum):
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    DEGRADED = "DEGRADED"
    NOT_CONFIGURED = "NOT_CONFIGURED"
    AVAILABLE = "AVAILABLE"
    ERROR = "ERROR"


class HealthState(StrEnum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    FAIL = "FAIL"
    NOT_APPLICABLE = "N/A"


class NoTradeReason(StrEnum):
    """Why a bar did not lead to an order. NO_TRADE is a valid, desirable outcome."""

    WARMUP = "warmup"
    DATA_STALE = "data_stale"
    DATA_INVALID = "data_invalid"
    DATA_DEGRADED = "data_degraded"
    FEATURES_INCOMPLETE = "features_incomplete"
    MODEL_NO_TRADE = "model_no_trade"
    MODEL_ERROR = "model_error"
    LOW_PROBABILITY = "low_probability"
    LOW_CONFIDENCE = "low_confidence"
    INSUFFICIENT_EDGE = "insufficient_edge"
    HIGH_SPREAD = "high_spread"
    REGIME_BLOCKED = "regime_blocked"
    RISK_REJECTED = "risk_rejected"
    BROKER_UNAVAILABLE = "broker_unavailable"
    AUDIT_UNAVAILABLE = "audit_unavailable"
    ORDER_REJECTED = "order_rejected"
