"""Position sizing strategies. Only `fixed_risk` is implemented; Kelly stays disabled by default."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from packages.common.config import SizingSection
from packages.common.errors import ConfigError
from packages.common.numbers import floor_to_increment


@dataclass(frozen=True)
class SizingInput:
    equity: float
    price: float
    stop_distance: float
    max_risk_per_trade: float
    quantity_increment: float


class PositionSizer(Protocol):
    name: str

    def size(self, inputs: SizingInput) -> float: ...


class FixedRiskSizer:
    """quantity = (equity × max_risk_per_trade) / stop_distance, floored to the instrument increment."""

    name = "fixed_risk"

    def size(self, inputs: SizingInput) -> float:
        if inputs.stop_distance <= 0 or inputs.equity <= 0 or inputs.price <= 0:
            return 0.0
        risk_amount = inputs.equity * inputs.max_risk_per_trade
        return floor_to_increment(risk_amount / inputs.stop_distance, inputs.quantity_increment)


def build_sizer(config: SizingSection) -> PositionSizer:
    if config.method == "fixed_risk":
        return FixedRiskSizer()
    if config.method == "fractional_kelly" and not config.kelly_enabled:
        raise ConfigError("fractional Kelly sizing is disabled by default; it requires kelly_enabled: true")
    raise ConfigError(f"sizing method '{config.method}' is planned but not implemented yet")
