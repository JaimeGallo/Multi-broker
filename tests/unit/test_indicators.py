from __future__ import annotations

import math

import numpy as np
import pytest

from packages.features import indicators as ind


def arr(*values: float) -> np.ndarray:
    return np.asarray(values, dtype=float)


def test_sma_last_and_short_window() -> None:
    assert ind.sma_last(arr(1, 2, 3, 4, 5), 3) == pytest.approx(4.0)
    assert math.isnan(ind.sma_last(arr(1, 2), 3))


def test_ema_is_seeded_with_the_sma() -> None:
    out = ind.ema_series(arr(1, 2, 3, 4), 2)
    assert math.isnan(out[0])
    assert out[1] == pytest.approx(1.5)
    assert out[2] == pytest.approx(2 / 3 * 3 + 1 / 3 * 1.5)
    assert out[3] == pytest.approx(2 / 3 * 4 + 1 / 3 * out[2])


def test_rsi_extremes() -> None:
    rising = np.arange(1.0, 40.0)
    assert ind.rsi_last(rising, 14) == 100.0
    assert ind.rsi_last(rising[::-1], 14) == pytest.approx(0.0)
    assert ind.rsi_last(np.full(30, 10.0), 14) == 50.0
    assert math.isnan(ind.rsi_last(rising[:10], 14))


def test_atr_of_constant_ranges() -> None:
    close = np.full(40, 100.0)
    assert ind.atr_last(close + 0.5, close - 0.5, close, 14) == pytest.approx(1.0)


def test_adx_detects_a_clean_trend() -> None:
    close = np.linspace(100.0, 130.0, 60)
    adx, plus_di, minus_di = ind.adx_last(close + 0.2, close - 0.2, close, 14)
    assert adx > 25 and plus_di > minus_di
    assert all(math.isnan(x) for x in ind.adx_last(close[:20], close[:20], close[:20], 14))


def test_macd_sign_follows_the_trend() -> None:
    up = np.linspace(100.0, 120.0, 80)
    macd, signal, hist = ind.macd_last(up)
    assert macd > 0 and hist == pytest.approx(macd - signal)
    assert ind.macd_last(up[::-1])[0] < 0


def test_returns_volatility_and_zscore() -> None:
    close = arr(100, 101, 102, 101)
    assert ind.roc_last(close, 2) == pytest.approx(101 / 101 - 1.0)
    returns = ind.log_returns(close)
    assert len(returns) == 3
    assert ind.realized_vol_last(returns, 3) == pytest.approx(float(np.sqrt(np.mean(returns**2))))
    assert math.isnan(ind.rolling_std_last(returns, 1))
    values = arr(1, 2, 1, 2, 1, 10)
    assert ind.zscore_last(values, 5) > 3
    assert math.isnan(ind.zscore_last(np.full(10, 3.0), 5))


def test_trend_tstat_sign_and_clipping() -> None:
    rng = np.random.default_rng(3)
    noisy_up = 100.0 * np.exp(np.cumsum(0.002 + rng.normal(0, 0.001, 50)))
    assert ind.trend_tstat_last(noisy_up, 30) > 2
    assert ind.trend_tstat_last(noisy_up[::-1], 30) < -2
    perfect = np.exp(np.linspace(0.0, 0.1, 30))
    assert ind.trend_tstat_last(perfect, 30) == pytest.approx(50.0)
