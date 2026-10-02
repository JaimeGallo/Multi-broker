"""Model evaluation across folds: daily PnL, bootstrap confidence intervals, long/short and paired comparisons.

Statistics are computed on DAILY net PnL (trades within a day are not independent). Confidence intervals use a
seeded bootstrap, so the same results always print the same intervals. Nothing here is a forecast: a positive
mean with an interval that includes zero is not evidence of an edge.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime
from typing import Any

import numpy as np

from packages.common.entities import Trade
from packages.common.enums import Direction

BOOTSTRAP_SAMPLES = 2_000
SEED = 20_240_101


def daily_pnl(
    trades: Iterable[Trade], trading_date: Callable[[datetime], date], days: Iterable[date]
) -> dict[date, float]:
    """Net PnL per session (exit date). Sessions without trades count as 0: not trading is a result too."""
    result = {day: 0.0 for day in days}
    for trade in trades:
        day = trading_date(trade.exit_time)
        result[day] = result.get(day, 0.0) + trade.net_pnl
    return dict(sorted(result.items()))


def bootstrap_mean_ci(
    values: Sequence[float], *, samples: int = BOOTSTRAP_SAMPLES, seed: int = SEED
) -> tuple[float, float] | None:
    if len(values) < 2:
        return None
    data = np.asarray(values, dtype=float)
    rng = np.random.default_rng(seed)
    means = data[rng.integers(0, len(data), size=(samples, len(data)))].mean(axis=1)
    low, high = np.percentile(means, [2.5, 97.5])
    return float(low), float(high)


def _side_stats(trades: Sequence[Trade]) -> dict[str, Any]:
    pnl = [t.net_pnl for t in trades]
    wins = [p for p in pnl if p > 0]
    losses = [-p for p in pnl if p < 0]
    return {
        "trades": len(trades),
        "net_pnl": float(sum(pnl)),
        "win_rate": len(wins) / len(trades) if trades else None,
        "profit_factor": sum(wins) / sum(losses) if losses else None,
        "expectancy": float(np.mean(pnl)) if pnl else None,
    }


def model_summary(
    trades: Sequence[Trade],
    *,
    trading_date: Callable[[datetime], date],
    sessions: Sequence[date],
    start_equity: float,
    fold_of: Callable[[date], str],
) -> dict[str, Any]:
    daily = daily_pnl(trades, trading_date, sessions)
    values = list(daily.values())
    mean = float(np.mean(values)) if values else None
    sd = float(np.std(values, ddof=1)) if len(values) > 1 else None
    sharpe = mean / sd * math.sqrt(252) if mean is not None and sd else None
    folds: dict[str, float] = {}
    for day, pnl in daily.items():
        folds[fold_of(day)] = folds.get(fold_of(day), 0.0) + pnl
    symbols: dict[str, float] = {}
    for trade in trades:
        symbols[trade.symbol] = symbols.get(trade.symbol, 0.0) + trade.net_pnl
    return {
        **_side_stats(trades),
        "gross_pnl": float(sum(t.gross_pnl for t in trades)),
        "fees": float(sum(t.fees for t in trades)),
        "model_pnl": float(sum(t.model_pnl for t in trades)),
        "execution_shortfall": float(sum(t.execution_shortfall for t in trades)),
        "sessions": len(values),
        "mean_daily_pnl": mean,
        "mean_daily_pnl_ci95": bootstrap_mean_ci(values),
        "daily_sharpe_annualized": sharpe,
        "return_on_equity": float(sum(values)) / start_equity if start_equity > 0 else None,
        "positive_folds": sum(1 for v in folds.values() if v > 0),
        "folds": dict(sorted(folds.items())),
        "long": _side_stats([t for t in trades if t.direction is Direction.LONG]),
        "short": _side_stats([t for t in trades if t.direction is Direction.SHORT]),
        "by_symbol": dict(sorted(symbols.items())),
    }


def paired_difference(
    candidate: dict[date, float],
    reference: dict[date, float],
    *,
    samples: int = BOOTSTRAP_SAMPLES,
    seed: int = SEED,
) -> dict[str, Any]:
    """Mean daily PnL difference (candidate - reference) over the same sessions, with a paired bootstrap CI."""
    days = sorted(set(candidate) & set(reference))
    differences = [candidate[d] - reference[d] for d in days]
    return {
        "sessions": len(days),
        "mean_daily_difference": float(np.mean(differences)) if differences else None,
        "ci95": bootstrap_mean_ci(differences, samples=samples, seed=seed),
    }
