"""Round-trip trade ledger.

Each trade keeps reference prices next to actual fills, which separates MODEL quality (PnL at reference prices)
from EXECUTION quality (slippage, spread, fees): `execution_shortfall = model_pnl - net_pnl`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from packages.common.entities import Fill, MarketBar, RiskDecision, Signal, Trade
from packages.common.enums import Direction, OrderIntent
from packages.common.ids import make_trade_id

EPSILON = 1e-9


@dataclass
class _OpenTrade:
    signal: Signal
    take_profit: float | None
    stop_loss: float | None
    broker: str = ""
    entry_quantity: float = 0.0
    entry_notional: float = 0.0
    entry_time: datetime | None = None
    exit_quantity: float = 0.0
    exit_notional: float = 0.0
    exit_reference_notional: float = 0.0
    exit_time: datetime | None = None
    exit_reason: OrderIntent | None = None
    fees: float = 0.0
    high: float | None = None
    low: float | None = None


class TradeLedger:
    def __init__(self) -> None:
        self._open: dict[str, _OpenTrade] = {}
        self._closed: list[Trade] = []

    @property
    def closed(self) -> list[Trade]:
        return list(self._closed)

    @property
    def open_count(self) -> int:
        return len(self._open)

    def register(self, signal: Signal, decision: RiskDecision) -> None:
        self._open[signal.signal_id] = _OpenTrade(
            signal=signal.model_copy(deep=True), take_profit=decision.take_profit, stop_loss=decision.stop_loss
        )

    def restore(
        self,
        signal: Signal,
        *,
        broker: str,
        entry_quantity: float,
        entry_price: float,
        entry_time: datetime,
        take_profit: float | None,
        stop_loss: float | None,
    ) -> None:
        """Re-open a trade after a restart from persisted signal and entry order data."""
        self._open[signal.signal_id] = _OpenTrade(
            signal=signal.model_copy(deep=True),
            take_profit=take_profit,
            stop_loss=stop_loss,
            broker=broker,
            entry_quantity=entry_quantity,
            entry_notional=entry_quantity * entry_price,
            entry_time=entry_time,
        )

    def on_bar(self, bar: MarketBar) -> None:
        for trade in self._open.values():
            if trade.signal.symbol != bar.symbol or trade.entry_time is None or bar.end <= trade.entry_time:
                continue
            trade.high = bar.high if trade.high is None else max(trade.high, bar.high)
            trade.low = bar.low if trade.low is None else min(trade.low, bar.low)

    def on_fill(self, fill: Fill, exit_reference_price: float | None) -> Trade | None:
        trade = self._open.get(fill.signal_id) if fill.signal_id else None
        if trade is None:
            return None
        trade.broker = fill.broker
        trade.fees += fill.fee
        if fill.intent is OrderIntent.ENTRY:
            trade.entry_quantity += fill.quantity
            trade.entry_notional += fill.quantity * fill.price
            if trade.entry_time is None:
                trade.entry_time = fill.timestamp
            return None
        reference = exit_reference_price if exit_reference_price is not None else fill.price
        trade.exit_quantity += fill.quantity
        trade.exit_notional += fill.quantity * fill.price
        trade.exit_reference_notional += fill.quantity * reference
        trade.exit_time = fill.timestamp
        if trade.exit_reason is None:
            trade.exit_reason = fill.intent
        if trade.entry_quantity > EPSILON and trade.exit_quantity >= trade.entry_quantity - EPSILON:
            closed = self._close(trade)
            del self._open[fill.signal_id or ""]
            self._closed.append(closed)
            return closed
        return None

    @staticmethod
    def _close(trade: _OpenTrade) -> Trade:
        signal = trade.signal
        sign = signal.direction.sign
        quantity = trade.entry_quantity
        entry = trade.entry_notional / trade.entry_quantity
        exit_ = trade.exit_notional / trade.exit_quantity
        entry_reference = signal.reference_price
        exit_reference = trade.exit_reference_notional / trade.exit_quantity
        gross = sign * (exit_ - entry) * quantity
        net = gross - trade.fees
        model = sign * (exit_reference - entry_reference) * quantity
        high = trade.high if trade.high is not None else max(entry, exit_)
        low = trade.low if trade.low is not None else min(entry, exit_)
        if signal.direction is Direction.LONG:
            mfe = (high - entry) / entry * 1e4
            mae = (entry - low) / entry * 1e4
        else:
            mfe = (entry - low) / entry * 1e4
            mae = (high - entry) / entry * 1e4
        entry_time = trade.entry_time or signal.timestamp
        exit_time = trade.exit_time or entry_time
        return Trade(
            trade_id=make_trade_id(signal.signal_id),
            signal_id=signal.signal_id,
            broker=trade.broker,
            symbol=signal.symbol,
            direction=signal.direction,
            quantity=quantity,
            entry_time=entry_time,
            entry_price=entry,
            entry_reference_price=entry_reference,
            exit_time=exit_time,
            exit_price=exit_,
            exit_reference_price=exit_reference,
            exit_reason=trade.exit_reason or OrderIntent.MANUAL,
            gross_pnl=gross,
            fees=trade.fees,
            net_pnl=net,
            model_pnl=model,
            execution_shortfall=model - net,
            entry_slippage_bps=sign * (entry - entry_reference) / entry_reference * 1e4,
            exit_slippage_bps=-sign * (exit_ - exit_reference) / exit_reference * 1e4,
            mae_bps=max(0.0, mae),
            mfe_bps=max(0.0, mfe),
            holding_minutes=(exit_time - entry_time).total_seconds() / 60.0,
            model_name=signal.model_name,
            model_version=signal.model_version,
            market_regime=signal.market_regime,
            confidence=signal.confidence,
            probability=signal.probability,
        )
