"""Build the configured broker adapters."""

from __future__ import annotations

from packages.brokers.base import BrokerAdapter
from packages.brokers.mock import MockBrokerAdapter
from packages.common.calendar import MarketCalendar
from packages.common.clock import Clock
from packages.common.config import AppConfig
from packages.common.costs import CostModel
from packages.common.errors import ConfigError


def build_brokers(
    config: AppConfig, *, clock: Clock, calendar: MarketCalendar, cost_model: CostModel
) -> dict[str, BrokerAdapter]:
    adapters: dict[str, BrokerAdapter] = {}
    if config.broker.mock.enabled:
        adapters["mock"] = MockBrokerAdapter(config.broker.mock, cost_model, clock, calendar)
    if config.broker.alpaca.enabled:
        raise ConfigError(
            "AlpacaBrokerAdapter arrives in phase 4 (docs/BROKER_ARCHITECTURE.md §6); set broker.alpaca.enabled: false"
        )
    if config.broker.ibkr.enabled:
        raise ConfigError(
            "IBKRBrokerAdapter arrives in phase 8 (docs/BROKER_ARCHITECTURE.md §7); set broker.ibkr.enabled: false"
        )
    referenced = {config.broker.active, *config.broker.routing.values()}
    missing = sorted(name for name in referenced if name not in adapters)
    if missing:
        raise ConfigError(f"brokers referenced by broker.active/routing are not enabled: {missing}")
    return adapters
