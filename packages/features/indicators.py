"""Technical indicators as pure numpy functions over a finite window.

Recursive indicators (EMA, Wilder smoothing) are seeded inside the window, so every value is a deterministic
function of the window alone: identical in backtest, replay and live, and recomputable from stored bars.
Functions return NaN when the window is too short; callers treat NaN as "unavailable".
"""

from __future__ import annotations

import math

import numpy as np
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]
NAN = math.nan


def sma_last(values: FloatArray, period: int) -> float:
    if len(values) < period:
        return NAN
    return float(np.mean(values[-period:]))


def ema_series(values: FloatArray, period: int) -> FloatArray:
    """EMA seeded with the SMA of the first `period` values; NaN before that."""
    out = np.full(len(values), np.nan)
    if len(values) < period:
        return out
    alpha = 2.0 / (period + 1.0)
    previous = float(np.mean(values[:period]))
    out[period - 1] = previous
    # Plain Python floats: same IEEE arithmetic as numpy scalars, several times faster in a loop.
    data = values.tolist()
    for i in range(period, len(data)):
        previous = alpha * data[i] + (1.0 - alpha) * previous
        out[i] = previous
    return out


def wilder_series(values: FloatArray, period: int) -> FloatArray:
    """Wilder smoothing (RMA) seeded with the mean of the first `period` values."""
    out = np.full(len(values), np.nan)
    if len(values) < period:
        return out
    previous = float(np.mean(values[:period]))
    out[period - 1] = previous
    data = values.tolist()
    for i in range(period, len(data)):
        previous = (previous * (period - 1) + data[i]) / period
        out[i] = previous
    return out


def rsi_last(close: FloatArray, period: int = 14) -> float:
    if len(close) < period + 1:
        return NAN
    changes = np.diff(close)
    avg_gain = wilder_series(np.clip(changes, 0.0, None), period)[-1]
    avg_loss = wilder_series(np.clip(-changes, 0.0, None), period)[-1]
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return float(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))


def true_range(high: FloatArray, low: FloatArray, close: FloatArray) -> FloatArray:
    previous_close = np.concatenate(([close[0]], close[:-1]))
    return np.maximum.reduce([high - low, np.abs(high - previous_close), np.abs(low - previous_close)])


def atr_last(high: FloatArray, low: FloatArray, close: FloatArray, period: int = 14) -> float:
    if len(close) < period + 1:
        return NAN
    return float(wilder_series(true_range(high, low, close)[1:], period)[-1])


def adx_last(
    high: FloatArray, low: FloatArray, close: FloatArray, period: int = 14
) -> tuple[float, float, float]:
    """Return (ADX, +DI, -DI). Needs at least 2*period+1 bars."""
    if len(close) < 2 * period + 1:
        return NAN, NAN, NAN
    up = high[1:] - high[:-1]
    down = low[:-1] - low[1:]
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = true_range(high, low, close)[1:]
    atr = wilder_series(tr, period)
    smoothed_plus = wilder_series(plus_dm, period)
    smoothed_minus = wilder_series(minus_dm, period)
    with np.errstate(divide="ignore", invalid="ignore"):
        plus_di = 100.0 * smoothed_plus / atr
        minus_di = 100.0 * smoothed_minus / atr
        dx = 100.0 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
    dx_valid = dx[period - 1 :]
    dx_valid = np.where(np.isfinite(dx_valid), dx_valid, 0.0)
    adx = wilder_series(dx_valid, period)[-1]
    pdi = float(plus_di[-1]) if math.isfinite(plus_di[-1]) else 0.0
    mdi = float(minus_di[-1]) if math.isfinite(minus_di[-1]) else 0.0
    return float(adx), pdi, mdi


def macd_last(
    close: FloatArray, fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[float, float, float]:
    """Return (MACD line, signal line, histogram)."""
    if len(close) < slow + signal:
        return NAN, NAN, NAN
    line = ema_series(close, fast) - ema_series(close, slow)
    valid = line[slow - 1 :]
    signal_line = ema_series(valid, signal)
    macd = float(valid[-1])
    sig = float(signal_line[-1])
    return macd, sig, macd - sig


def roc_last(close: FloatArray, period: int) -> float:
    if len(close) < period + 1 or close[-1 - period] == 0:
        return NAN
    return float(close[-1] / close[-1 - period] - 1.0)


def log_returns(close: FloatArray) -> FloatArray:
    return np.diff(np.log(close))


def rolling_std_last(returns: FloatArray, period: int) -> float:
    if len(returns) < period or period < 2:
        return NAN
    return float(np.std(returns[-period:], ddof=1))


def realized_vol_last(returns: FloatArray, period: int) -> float:
    """Root mean square of the last `period` returns (per-bar volatility)."""
    if len(returns) < period:
        return NAN
    window = returns[-period:]
    return float(np.sqrt(np.mean(window * window)))


def trend_tstat_last(close: FloatArray, period: int) -> float:
    """t-statistic of the OLS slope of log price over the last `period` bars (clipped to ±50)."""
    if len(close) < period or period < 3:
        return NAN
    y = np.log(close[-period:])
    x = np.arange(period, dtype=float)
    x -= x.mean()
    sxx = float(x @ x)
    slope = float(x @ (y - y.mean())) / sxx
    residuals = y - y.mean() - slope * x
    s2 = float(residuals @ residuals) / (period - 2)
    if s2 <= 0:
        return 0.0 if slope == 0 else math.copysign(50.0, slope)
    return float(np.clip(slope / math.sqrt(s2 / sxx), -50.0, 50.0))


def zscore_last(values: FloatArray, period: int) -> float:
    """z-score of the last value against the `period` values before it."""
    if len(values) < period + 1 or period < 2:
        return NAN
    reference = values[-period - 1 : -1]
    sd = float(np.std(reference, ddof=1))
    if sd == 0:
        return NAN
    return float((values[-1] - np.mean(reference)) / sd)
