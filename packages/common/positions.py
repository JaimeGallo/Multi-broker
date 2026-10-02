"""Signed position arithmetic shared by the portfolio tracker and the simulated broker."""

from __future__ import annotations

from dataclasses import dataclass

EPSILON = 1e-9


@dataclass
class PositionState:
    symbol: str
    quantity: float = 0.0
    average_price: float = 0.0
    realized_pnl: float = 0.0

    def apply(self, signed_quantity: float, price: float) -> float:
        """Apply a signed fill (+buy / -sell); return the realized PnL it produced (before fees)."""
        if abs(self.quantity) < EPSILON or (self.quantity > 0) == (signed_quantity > 0):
            new_quantity = self.quantity + signed_quantity
            self.average_price = (
                abs(self.quantity) * self.average_price + abs(signed_quantity) * price
            ) / abs(new_quantity)
            self.quantity = new_quantity
            return 0.0
        closing = min(abs(signed_quantity), abs(self.quantity))
        direction = 1.0 if self.quantity > 0 else -1.0
        realized = closing * (price - self.average_price) * direction
        self.quantity += signed_quantity
        if abs(self.quantity) < EPSILON:
            self.quantity = 0.0
            self.average_price = 0.0
        elif (self.quantity > 0) != (direction > 0):
            self.average_price = price
        self.realized_pnl += realized
        return realized
