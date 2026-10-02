"""Deterministic synthetic market (regime-switching GBM). For development and tests ONLY.

Nothing produced here resembles a real instrument; profitability on this data means nothing.
Each symbol has its own seeded RNG, so a symbol's path does not depend on which other symbols are generated.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime

import numpy as np

from packages.common.calendar import MarketCalendar
from packages.common.config import SyntheticMarketSection
from packages.common.entities import MarketBar, MarketQuote
from packages.common.enums import Timeframe
from packages.common.ids import digest

REGIMES = ("range", "trend_up", "trend_down", "high_vol")
_REGIME_WEIGHTS = (0.4, 0.2, 0.2, 0.2)
_SESSION_MINUTES_PER_YEAR = 252 * 390


@dataclass
class _SymbolState:
    price: float
    regime: int
    rng: np.random.Generator


def _symbol_rng(seed: int, symbol: str) -> np.random.Generator:
    material = hashlib.sha256(f"{seed}|{symbol}".encode()).digest()[:8]
    return np.random.default_rng(int.from_bytes(material, "big"))


class SyntheticMarket:
    def __init__(
        self, config: SyntheticMarketSection, calendar: MarketCalendar, timeframe: Timeframe = Timeframe.MIN_1
    ) -> None:
        self._cfg = config
        self._calendar = calendar
        self._tf = timeframe
        self._sigma_bar = config.annual_volatility / math.sqrt(_SESSION_MINUTES_PER_YEAR / timeframe.minutes)
        self.source = f"mock:{digest(config.model_dump_json(), timeframe.value, length=6)}"

    @property
    def timeframe(self) -> Timeframe:
        return self._tf

    def generate(
        self, symbols: Sequence[str], start: date, end: date
    ) -> Iterator[tuple[datetime, list[MarketBar], list[MarketQuote]]]:
        """Yield `(timestamp, bars ending at timestamp, quotes at timestamp)` in time order."""
        states = {
            s: _SymbolState(price=self._cfg.start_price, regime=0, rng=_symbol_rng(self._cfg.seed, s))
            for s in symbols
        }
        first_session = True
        for session in self._calendar.sessions_between(start, end):
            if not first_session:
                for symbol in symbols:
                    state = states[symbol]
                    state.price *= math.exp(state.rng.normal(0.0, self._cfg.overnight_gap_bps / 1e4))
            first_session = False
            n_bars = int((session.close - session.open) / self._tf.delta)
            for i in range(n_bars):
                bar_start = session.open + i * self._tf.delta
                session_fraction = (i + 0.5) / n_bars
                bars: list[MarketBar] = []
                quotes: list[MarketQuote] = []
                for symbol in symbols:
                    bar, quote = self._next_bar(symbol, states[symbol], bar_start, session_fraction)
                    bars.append(bar)
                    quotes.append(quote)
                yield bar_start + self._tf.delta, bars, quotes

    def _next_bar(
        self, symbol: str, state: _SymbolState, bar_start: datetime, session_fraction: float
    ) -> tuple[MarketBar, MarketQuote]:
        cfg = self._cfg
        rng = state.rng
        if rng.random() > cfg.regime_persistence:
            state.regime = int(rng.choice(len(REGIMES), p=_REGIME_WEIGHTS))
        regime = REGIMES[state.regime]

        u_shape = 1.0 + 0.6 * ((session_fraction - 0.5) / 0.5) ** 2
        vol = self._sigma_bar * u_shape * (cfg.high_vol_multiplier if regime == "high_vol" else 1.0)
        drift_sign = 1.0 if regime == "trend_up" else -1.0 if regime == "trend_down" else 0.0
        drift = drift_sign * cfg.trend_drift_bps / 1e4

        n = cfg.ticks_per_bar
        step = vol / math.sqrt(n)
        increments = rng.normal(drift / n - 0.5 * step * step, step, size=n)
        path = state.price * np.exp(np.cumsum(increments))

        open_ = round(state.price, 2)
        close = round(float(path[-1]), 2)
        high = max(round(float(path.max()), 2), open_, close)
        low = min(round(float(path.min()), 2), open_, close)
        volume_scale = 1.6 if regime == "high_vol" else 1.0
        volume = float(max(100, round(cfg.base_volume * u_shape * volume_scale * rng.lognormal(0.0, 0.35))))
        vwap = round(float(np.mean(np.concatenate(([state.price], path)))), 4)

        spread_bps = cfg.base_spread_bps * (1.0 + 0.5 * (vol / self._sigma_bar - 1.0))
        half = close * spread_bps / 2e4
        bid = round(close - half, 2)
        ask = round(close + half, 2)
        if ask <= bid:
            ask = round(bid + 0.01, 2)
        bid_size, ask_size = (int(x) * 100 for x in rng.integers(1, 20, size=2))

        state.price = float(path[-1])
        bar_end = bar_start + self._tf.delta
        bar = MarketBar(
            symbol=symbol,
            timeframe=self._tf,
            start=bar_start,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            vwap=vwap,
            trade_count=int(volume // 100),
            source=self.source,
        )
        quote = MarketQuote(
            symbol=symbol,
            timestamp=bar_end,
            bid=bid,
            ask=ask,
            bid_size=float(bid_size),
            ask_size=float(ask_size),
            source=self.source,
        )
        return bar, quote
