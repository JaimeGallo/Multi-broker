"""Exact price/quantity rounding (Decimal based, avoids binary float artifacts at order boundaries)."""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Literal

_ROUNDING = {"nearest": ROUND_HALF_UP, "down": ROUND_FLOOR, "up": ROUND_CEILING}


def round_to_tick(price: float, tick: float, mode: Literal["nearest", "down", "up"] = "nearest") -> float:
    if tick <= 0:
        raise ValueError("tick must be positive")
    d_tick = Decimal(repr(tick))
    units = (Decimal(repr(price)) / d_tick).quantize(Decimal(1), rounding=_ROUNDING[mode])
    return float(units * d_tick)


def floor_to_increment(quantity: float, increment: float) -> float:
    if increment <= 0:
        raise ValueError("increment must be positive")
    d_increment = Decimal(repr(increment))
    units = (Decimal(repr(quantity)) / d_increment).to_integral_value(rounding=ROUND_FLOOR)
    return float(units * d_increment)
