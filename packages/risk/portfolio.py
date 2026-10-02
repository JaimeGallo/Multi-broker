"""Local portfolio view built from fills, reconciled against the broker (which stays the source of truth)."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from packages.common.entities import AccountSnapshot, Fill, PortfolioSnapshot, Position
from packages.common.positions import EPSILON, PositionState


class PortfolioTracker:
    def __init__(self, broker: str) -> None:
        self._broker = broker
        self._positions: dict[str, PositionState] = {}
        self._prices: dict[str, float] = {}
        self._account: AccountSnapshot | None = None
        self._peak_equity: float | None = None
        self.realized_pnl = 0.0
        self.fees = 0.0

    @property
    def account(self) -> AccountSnapshot | None:
        return self._account

    @property
    def peak_equity(self) -> float:
        if self._peak_equity is not None:
            return self._peak_equity
        return self._account.equity if self._account is not None else 0.0

    def apply_fill(self, fill: Fill) -> float:
        state = self._positions.setdefault(fill.symbol, PositionState(fill.symbol))
        realized = state.apply(fill.side.sign * fill.quantity, fill.price)
        self.realized_pnl += realized
        self.fees += fill.fee
        self._prices.setdefault(fill.symbol, fill.price)
        return realized

    def mark(self, symbol: str, price: float) -> None:
        self._prices[symbol] = price

    def load_positions(self, positions: Iterable[Position]) -> None:
        """Replace local positions with the broker's (authoritative) view."""
        self._positions = {
            p.symbol: PositionState(p.symbol, quantity=p.quantity, average_price=p.average_entry_price)
            for p in positions
            if abs(p.quantity) > EPSILON
        }
        for p in positions:
            if p.market_price is not None:
                self._prices[p.symbol] = p.market_price

    def update_account(self, account: AccountSnapshot) -> None:
        self._account = account
        if self._peak_equity is None or account.equity > self._peak_equity:
            self._peak_equity = account.equity

    def quantity(self, symbol: str) -> float:
        state = self._positions.get(symbol)
        return state.quantity if state is not None else 0.0

    def open_positions(self) -> dict[str, float]:
        return {s: p.quantity for s, p in self._positions.items() if abs(p.quantity) > EPSILON}

    def price(self, symbol: str) -> float | None:
        return self._prices.get(symbol)

    def prices(self) -> dict[str, float]:
        return dict(self._prices)

    def gross_exposure(self) -> float:
        return sum(abs(p.quantity) * self._prices.get(s, p.average_price) for s, p in self._positions.items())

    def net_exposure(self) -> float:
        return sum(p.quantity * self._prices.get(s, p.average_price) for s, p in self._positions.items())

    def snapshot(self, now: datetime) -> PortfolioSnapshot | None:
        account = self._account
        if account is None:
            return None
        peak = max(self.peak_equity, account.equity)
        return PortfolioSnapshot(
            timestamp=now,
            broker=self._broker,
            cash=account.cash,
            equity=account.equity,
            buying_power=account.buying_power,
            gross_exposure=self.gross_exposure(),
            net_exposure=self.net_exposure(),
            open_positions=len(self.open_positions()),
            daily_pnl=account.daily_pnl,
            peak_equity=peak,
            drawdown=1.0 - account.equity / peak if peak > 0 else 0.0,
        )
