"""Feature engine: identical computation in every mode.

A FeatureVector is a pure function of (last `window` bars, last quote received before the decision, spec).
Nothing after the as-of time can influence it; `tests/unit/test_features.py` enforces this.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime

import numpy as np

from packages.common.entities import FeatureVector, MarketBar, MarketQuote
from packages.common.ids import make_feature_id
from packages.features import indicators as ind
from packages.features.spec import FeatureSpec

NAN = math.nan
DayStart = Callable[[datetime], datetime]


def _ratio_minus_one(numerator: float, denominator: float) -> float:
    if not (math.isfinite(numerator) and math.isfinite(denominator)) or denominator <= 0:
        return NAN
    return numerator / denominator - 1.0


def compute_features(
    bars: Sequence[MarketBar], spec: FeatureSpec, quote: MarketQuote | None, day_start: DayStart
) -> dict[str, float]:
    n = len(bars)
    o = np.fromiter((b.open for b in bars), dtype=float, count=n)
    h = np.fromiter((b.high for b in bars), dtype=float, count=n)
    lo = np.fromiter((b.low for b in bars), dtype=float, count=n)
    c = np.fromiter((b.close for b in bars), dtype=float, count=n)
    v = np.fromiter((b.volume for b in bars), dtype=float, count=n)
    tp = np.fromiter((b.typical_price for b in bars), dtype=float, count=n)
    r = ind.log_returns(c)
    last = float(c[-1])
    values: dict[str, float] = {}

    # Price
    values["ret_1"] = ind.roc_last(c, 1)
    values["log_ret_1"] = float(r[-1]) if len(r) else NAN
    for k in spec.return_horizons:
        values[f"ret_{k}"] = ind.roc_last(c, k)
    values["range_pct"] = float((h[-1] - lo[-1]) / last) if last > 0 else NAN
    values["gap"] = float(o[-1] / c[-2] - 1.0) if n >= 2 and c[-2] > 0 else NAN

    # Momentum
    values[f"rsi_{spec.rsi_period}"] = ind.rsi_last(c, spec.rsi_period)
    macd, signal, hist = ind.macd_last(c, spec.macd_fast, spec.macd_slow, spec.macd_signal)
    values["macd_norm"] = macd / last
    values["macd_signal_norm"] = signal / last
    values["macd_hist_norm"] = hist / last
    values[f"roc_{spec.roc_period}"] = ind.roc_last(c, spec.roc_period)
    atr = ind.atr_last(h, lo, c, spec.atr_period)
    mp = spec.momentum_period
    values[f"momentum_{mp}_atr"] = float((c[-1] - c[-1 - mp]) / atr) if n > mp and atr > 0 else NAN

    # Trend
    for p in spec.sma_periods:
        values[f"sma_{p}_dist"] = _ratio_minus_one(last, ind.sma_last(c, p))
    for p in spec.ema_periods:
        values[f"ema_{p}_dist"] = _ratio_minus_one(last, float(ind.ema_series(c, p)[-1]))
    adx, plus_di, minus_di = ind.adx_last(h, lo, c, spec.adx_period)
    values[f"adx_{spec.adx_period}"] = adx
    values[f"plus_di_{spec.adx_period}"] = plus_di
    values[f"minus_di_{spec.adx_period}"] = minus_di
    values[f"trend_strength_{spec.trend_window}"] = ind.trend_tstat_last(c, spec.trend_window)

    # Volatility
    values[f"atr_{spec.atr_period}"] = atr
    values[f"atr_{spec.atr_period}_pct"] = atr / last if last > 0 else NAN
    std_short = ind.rolling_std_last(r, spec.std_short)
    std_long = ind.rolling_std_last(r, spec.std_long)
    values[f"rolling_std_{spec.std_short}"] = std_short
    values[f"realized_vol_{spec.realized_vol_window}"] = ind.realized_vol_last(r, spec.realized_vol_window)
    values[f"vol_ratio_{spec.std_short}_{spec.std_long}"] = std_short / std_long if std_long > 0 else NAN

    # Volume
    vw = spec.volume_window
    mean_volume = float(np.mean(v[-vw - 1 : -1])) if n > vw else NAN
    values[f"relative_volume_{vw}"] = float(v[-1] / mean_volume) if mean_volume > 0 else NAN
    values[f"volume_zscore_{vw}"] = ind.zscore_last(v, vw)
    cutoff = day_start(bars[-1].start)
    first = n
    while first > 0 and bars[first - 1].start >= cutoff:
        first -= 1
    session_volume = float(v[first:].sum())
    session_vwap = float((tp[first:] * v[first:]).sum() / session_volume) if session_volume > 0 else NAN
    values["vwap_dist"] = _ratio_minus_one(last, session_vwap)

    # Microstructure (only with a sane quote known before the decision)
    if spec.include_microstructure:
        if quote is not None and quote.bid > 0 and quote.ask > quote.bid:
            values["spread_bps"] = quote.spread_bps
            values["bid_ask_imbalance"] = quote.imbalance
        else:
            values["spread_bps"] = NAN
            values["bid_ask_imbalance"] = NAN
    return values


class FeatureEngine:
    def __init__(self, spec: FeatureSpec, *, namespace: str, day_start: DayStart) -> None:
        self._spec = spec
        self._namespace = namespace
        self._day_start = day_start
        self._hash = spec.spec_hash()
        self._buffers: dict[str, deque[MarketBar]] = {}

    @property
    def spec(self) -> FeatureSpec:
        return self._spec

    @property
    def spec_hash(self) -> str:
        return self._hash

    def observe(self, bar: MarketBar) -> None:
        """Append a bar to the window without computing features (STALE/DEGRADED bars keep the window continuous)."""
        buffer = self._buffers.get(bar.symbol)
        if buffer is None:
            buffer = deque(maxlen=self._spec.window)
            self._buffers[bar.symbol] = buffer
        buffer.append(bar)

    def warm_up(self, bars: Iterable[MarketBar]) -> None:
        for bar in bars:
            self.observe(bar)

    def buffered(self, symbol: str) -> int:
        buffer = self._buffers.get(symbol)
        return len(buffer) if buffer is not None else 0

    def update(self, bar: MarketBar, quote: MarketQuote | None = None) -> FeatureVector | None:
        self.observe(bar)
        buffer = self._buffers[bar.symbol]
        if len(buffer) < self._spec.min_bars:
            return None
        return self.compute(list(buffer), quote)

    def compute(self, bars: Sequence[MarketBar], quote: MarketQuote | None = None) -> FeatureVector:
        window = list(bars[-self._spec.window :])
        last = window[-1]
        used_quote = quote if self._spec.include_microstructure else None
        values = compute_features(window, self._spec, used_quote, self._day_start)
        return FeatureVector(
            feature_id=make_feature_id(self._namespace, last.symbol, last.end, self._spec.version, self._hash),
            symbol=last.symbol,
            timestamp=last.end,
            timeframe=last.timeframe,
            feature_version=self._spec.version,
            spec_hash=self._hash,
            close=last.close,
            values=values,
            window_start=window[0].start,
            window_end=last.end,
            n_bars=len(window),
            quote=used_quote,
        )
