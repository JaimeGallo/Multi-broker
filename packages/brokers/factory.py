"""Build the configured broker adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from packages.brokers.alpaca.adapter import AlpacaBrokerAdapter
from packages.brokers.base import BrokerAdapter
from packages.brokers.mock import MockBrokerAdapter
from packages.common.calendar import MarketCalendar
from packages.common.clock import Clock
from packages.common.config import AppConfig
from packages.common.costs import CostModel
from packages.common.errors import ConfigError
from packages.common.websocket import Connector, websockets_connector
from packages.market_data.alpaca_history import credentials


@dataclass
class AlpacaWiring:
    """How the Alpaca adapters reach the network (tests swap in fakes). Keys come from the environment."""

    environ: Mapping[str, str] | None = None
    trading_transport: httpx.AsyncBaseTransport | None = None
    data_transport: httpx.AsyncBaseTransport | None = None
    connector: Connector = websockets_connector
    extra: dict[str, Any] = field(default_factory=dict)


def build_brokers(
    config: AppConfig,
    *,
    clock: Clock,
    calendar: MarketCalendar,
    cost_model: CostModel,
    alpaca: AlpacaWiring | None = None,
) -> dict[str, BrokerAdapter]:
    adapters: dict[str, BrokerAdapter] = {}
    if config.broker.mock.enabled:
        adapters["mock"] = MockBrokerAdapter(config.broker.mock, cost_model, clock, calendar)
    if config.broker.alpaca.enabled:
        if alpaca is None:
            raise ConfigError("the Alpaca broker runs in real time only (`run`), not in simulations")
        key, secret = credentials(alpaca.environ)
        adapters["alpaca"] = AlpacaBrokerAdapter(
            config.broker.alpaca, cost_model, clock, key=key, secret=secret,
            transport=alpaca.trading_transport, connector=alpaca.connector, **alpaca.extra,
        )  # fmt: skip
    if config.broker.ibkr.enabled:
        raise ConfigError(
            "IBKRBrokerAdapter arrives in phase 8 (docs/BROKER_ARCHITECTURE.md §7); set broker.ibkr.enabled: false"
        )
    referenced = {config.broker.active, *config.broker.routing.values()}
    missing = sorted(name for name in referenced if name not in adapters)
    if missing:
        raise ConfigError(f"brokers referenced by broker.active/routing are not enabled: {missing}")
    return adapters
