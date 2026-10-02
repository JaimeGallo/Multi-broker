"""Performance metrics. Undefined quantities are reported as None, never invented."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from datetime import date, datetime

import numpy as np
from pydantic import BaseModel, ConfigDict

from packages.common.entities import PortfolioSnapshot, Trade

MIN_DAYS_FOR_ANNUALIZATION = 30


class PerformanceReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    trades: int
    wins: int
    losses: int
    win_rate: float | None
    average_win: float | None
    average_loss: float | None
    expectancy: float | None
    profit_factor: float | None
    gross_pnl: float
    fees: float
    net_pnl: float
    model_pnl: float
    execution_shortfall: float
    avg_entry_slippage_bps: float | None
    avg_exit_slippage_bps: float | None
    avg_mae_bps: float | None
    avg_mfe_bps: float | None
    avg_holding_minutes: float | None
    start_equity: float | None
    end_equity: float | None
    total_return: float | None
    cagr: float | None
    sharpe: float | None
    sortino: float | None
    max_drawdown: float | None
    calmar: float | None
    turnover: float | None
    exposure: float | None
    days: float | None
    daily_observations: int


def _mean(values: Sequence[float]) -> float | None:
    return float(np.mean(values)) if values else None


def max_drawdown(equity: Sequence[float]) -> float | None:
    if len(equity) < 2:
        return None
    curve = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(curve)
    return float(np.max(1.0 - curve / peaks))


def sharpe_ratio(returns: Sequence[float], periods_per_year: float) -> float | None:
    if len(returns) < 2:
        return None
    values = np.asarray(returns, dtype=float)
    sd = float(np.std(values, ddof=1))
    if sd == 0:
        return None
    return float(np.mean(values) / sd * math.sqrt(periods_per_year))


def sortino_ratio(returns: Sequence[float], periods_per_year: float) -> float | None:
    if len(returns) < 2:
        return None
    values = np.asarray(returns, dtype=float)
    downside = math.sqrt(float(np.mean(np.minimum(values, 0.0) ** 2)))
    if downside == 0:
        return None
    return float(np.mean(values) / downside * math.sqrt(periods_per_year))


def cagr(start_equity: float, end_equity: float, days: float) -> float | None:
    if days < MIN_DAYS_FOR_ANNUALIZATION or start_equity <= 0 or end_equity <= 0:
        return None
    return float((end_equity / start_equity) ** (365.25 / days) - 1.0)


def daily_returns(
    snapshots: Sequence[PortfolioSnapshot], trading_date: Callable[[datetime], date], start_equity: float
) -> list[float]:
    closes: dict[date, float] = {}
    for snapshot in snapshots:
        closes[trading_date(snapshot.timestamp)] = snapshot.equity
    returns: list[float] = []
    previous = start_equity
    for day in sorted(closes):
        equity = closes[day]
        if previous > 0:
            returns.append(equity / previous - 1.0)
        previous = equity
    return returns


def compute_performance(
    trades: Sequence[Trade],
    snapshots: Sequence[PortfolioSnapshot],
    *,
    trading_date: Callable[[datetime], date],
    start_equity: float | None = None,
    periods_per_year: float = 252.0,
) -> PerformanceReport:
    pnl = [t.net_pnl for t in trades]
    wins = [p for p in pnl if p > 0]
    losses = [p for p in pnl if p < 0]
    gross_profit, gross_loss = sum(wins), -sum(losses)

    ordered = sorted(snapshots, key=lambda s: s.timestamp)
    equity_curve = [s.equity for s in ordered]
    first_equity = start_equity if start_equity is not None else (equity_curve[0] if equity_curve else None)
    last_equity = equity_curve[-1] if equity_curve else None
    days = (
        (ordered[-1].timestamp - ordered[0].timestamp).total_seconds() / 86_400 if len(ordered) > 1 else None
    )
    rets = daily_returns(ordered, trading_date, first_equity) if first_equity else []
    total_return = last_equity / first_equity - 1.0 if first_equity and last_equity else None
    growth = cagr(first_equity, last_equity, days) if first_equity and last_equity and days else None
    drawdown = max_drawdown([first_equity, *equity_curve]) if first_equity else max_drawdown(equity_curve)
    traded_notional = sum(t.quantity * (t.entry_price + t.exit_price) for t in trades)
    average_equity = float(np.mean(equity_curve)) if equity_curve else None
    exposure = (
        float(np.mean([s.gross_exposure / s.equity for s in ordered if s.equity > 0])) if ordered else None
    )

    return PerformanceReport(
        trades=len(trades),
        wins=len(wins),
        losses=len(losses),
        win_rate=len(wins) / len(trades) if trades else None,
        average_win=_mean(wins),
        average_loss=_mean(losses),
        expectancy=_mean(pnl),
        profit_factor=gross_profit / gross_loss if gross_loss > 0 else None,
        gross_pnl=sum(t.gross_pnl for t in trades),
        fees=sum(t.fees for t in trades),
        net_pnl=sum(pnl),
        model_pnl=sum(t.model_pnl for t in trades),
        execution_shortfall=sum(t.execution_shortfall for t in trades),
        avg_entry_slippage_bps=_mean([t.entry_slippage_bps for t in trades]),
        avg_exit_slippage_bps=_mean([t.exit_slippage_bps for t in trades]),
        avg_mae_bps=_mean([t.mae_bps for t in trades]),
        avg_mfe_bps=_mean([t.mfe_bps for t in trades]),
        avg_holding_minutes=_mean([t.holding_minutes for t in trades]),
        start_equity=first_equity,
        end_equity=last_equity,
        total_return=total_return,
        cagr=growth,
        sharpe=sharpe_ratio(rets, periods_per_year),
        sortino=sortino_ratio(rets, periods_per_year),
        max_drawdown=drawdown,
        calmar=growth / drawdown if growth is not None and drawdown else None,
        turnover=traded_notional / average_equity if average_equity else None,
        exposure=exposure,
        days=days,
        daily_observations=len(rets),
    )


def segment_trades(trades: Sequence[Trade], key: Callable[[Trade], str]) -> dict[str, dict[str, float]]:
    """Per-segment count, net PnL, win rate and expectancy (by symbol, hour, regime, confidence, version...)."""
    groups: dict[str, list[Trade]] = {}
    for trade in trades:
        groups.setdefault(key(trade), []).append(trade)
    result: dict[str, dict[str, float]] = {}
    for name, group in sorted(groups.items()):
        pnl = [t.net_pnl for t in group]
        result[name] = {
            "trades": float(len(group)),
            "net_pnl": float(sum(pnl)),
            "win_rate": sum(1 for p in pnl if p > 0) / len(group),
            "expectancy": float(np.mean(pnl)),
        }
    return result


def confidence_bucket(trade: Trade) -> str:
    low = min(4, int(trade.confidence * 5)) / 5
    return f"{low:.1f}-{low + 0.2:.1f}"
