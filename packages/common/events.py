"""In-process asynchronous event bus.

Handlers run sequentially in subscription order, so a given input always produces the same sequence of
side effects (reproducible backtests). A failing subscriber never stops the engine: the error is logged and
reported through `on_error` (which marks the database unhealthy, for instance). `flush()` DOES propagate errors:
it is the audit barrier the engine crosses before sending any order. Phase 5 bridges this bus to Redis.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from packages.common.entities import DataQualityReport, JEVPrediction, MarketBar, RegimeAssessment
from packages.common.enums import NoTradeReason

log = logging.getLogger(__name__)

Handler = Callable[[str, Any], Awaitable[None]]
Flusher = Callable[[], Awaitable[None]]
ErrorHandler = Callable[[str, BaseException], Awaitable[None]]


class Topics:
    MARKET_BAR = "market.bar"
    FEATURES = "features"
    PREDICTION = "prediction"
    PREDICTION_OUTCOME = "prediction.outcome"
    SIGNAL = "signal"
    RISK_DECISION = "risk.decision"
    ORDER = "order.update"
    ORDER_EVENT = "order.event"
    FILL = "fill"
    TRADE = "trade"
    PORTFOLIO = "portfolio.snapshot"
    RISK_EVENT = "risk.event"
    SYSTEM_EVENT = "system.event"
    BROKER_EVENT = "broker.event"


@dataclass(frozen=True)
class BarRecord:
    bar: MarketBar
    quality: DataQualityReport


@dataclass(frozen=True)
class PredictionRecord:
    prediction: JEVPrediction
    regime: RegimeAssessment
    latency_ms: float
    no_trade_reason: NoTradeReason | None = None


@dataclass(frozen=True)
class SystemEvent:
    timestamp: datetime
    level: str
    component: str
    event_type: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RiskEvent:
    timestamp: datetime
    event_type: str
    reason: str
    detail: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerEvent:
    timestamp: datetime
    broker: str
    event_type: str
    details: dict[str, Any] = field(default_factory=dict)


class EventBus:
    def __init__(self, on_error: ErrorHandler | None = None) -> None:
        self._subscribers: list[tuple[str, Handler]] = []
        self._flushers: list[Flusher] = []
        self._on_error = on_error

    def set_error_handler(self, on_error: ErrorHandler | None) -> None:
        self._on_error = on_error

    def subscribe(self, prefix: str, handler: Handler) -> None:
        """Receive every topic starting with `prefix` ("" subscribes to everything)."""
        self._subscribers.append((prefix, handler))

    def add_flusher(self, flusher: Flusher) -> None:
        self._flushers.append(flusher)

    async def publish(self, topic: str, payload: Any) -> None:
        for prefix, handler in list(self._subscribers):
            if not topic.startswith(prefix):
                continue
            try:
                await handler(topic, payload)
            except Exception as exc:
                log.exception("event handler failed", extra={"topic": topic})
                if self._on_error is not None:
                    await self._on_error(topic, exc)

    async def flush(self) -> None:
        for flusher in self._flushers:
            await flusher()
