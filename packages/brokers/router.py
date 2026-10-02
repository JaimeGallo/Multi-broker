"""BrokerRouter: picks the execution venue. It never changes the order, the signal or JEV's decision."""

from __future__ import annotations

from collections.abc import Mapping

from packages.brokers.base import BrokerAdapter, ExecutionRequirements
from packages.common.config import BrokerSection
from packages.common.entities import BrokerHealth
from packages.common.enums import AssetClass, ConnectionStatus
from packages.common.errors import BrokerUnavailable

KNOWN_BROKERS = ("mock", "alpaca", "ibkr")


class BrokerRouter:
    def __init__(self, config: BrokerSection, adapters: Mapping[str, BrokerAdapter]) -> None:
        self._cfg = config
        self._adapters = dict(adapters)

    @property
    def adapters(self) -> dict[str, BrokerAdapter]:
        return dict(self._adapters)

    def get(self, name: str) -> BrokerAdapter:
        try:
            return self._adapters[name]
        except KeyError:
            raise BrokerUnavailable(f"broker '{name}' is not configured") from None

    def name_for(self, asset_class: AssetClass) -> str:
        return self._cfg.routing.get(asset_class, self._cfg.active)

    def primary(self) -> BrokerAdapter:
        return self.get(self._cfg.active)

    async def select_broker(
        self,
        symbol: str,
        asset_class: AssetClass,
        strategy: str,
        execution_requirements: ExecutionRequirements | None = None,
    ) -> BrokerAdapter:
        """Return an enabled adapter able to execute the request (symbol/strategy reserved for future rules)."""
        adapter = self.get(self.name_for(asset_class))
        caps = adapter.capabilities
        requirements = execution_requirements or ExecutionRequirements()
        missing: list[str] = []
        if asset_class not in caps.asset_classes:
            missing.append(f"asset class {asset_class.value}")
        if requirements.bracket and not caps.supports_bracket:
            missing.append("bracket orders")
        if requirements.short and not caps.supports_short:
            missing.append("short selling")
        if requirements.fractional and not caps.supports_fractional:
            missing.append("fractional quantities")
        if requirements.extended_hours and not caps.supports_extended_hours:
            missing.append("extended hours")
        if missing:
            raise BrokerUnavailable(f"broker '{adapter.name}' cannot execute {symbol}: {', '.join(missing)}")
        return adapter

    async def statuses(self) -> dict[str, BrokerHealth | ConnectionStatus]:
        result: dict[str, BrokerHealth | ConnectionStatus] = {}
        for name in KNOWN_BROKERS:
            adapter = self._adapters.get(name)
            result[name] = await adapter.health() if adapter is not None else ConnectionStatus.NOT_CONFIGURED
        return result
