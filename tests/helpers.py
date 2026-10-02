"""Shared test helpers. Every test is offline and deterministic: synthetic data, simulated clocks, local SQLite."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from packages.common.config import AppConfig, deep_merge, load_config
from packages.common.entities import FeatureVector, MarketBar, MarketQuote
from packages.common.enums import Timeframe

SESSION_DAY = date(2024, 3, 4)  # a Monday
SESSION_OPEN = datetime(2024, 3, 4, 14, 30, tzinfo=UTC)  # 09:30 New York (EST)


def make_bar(
    *,
    symbol: str = "TEST",
    start: datetime = SESSION_OPEN,
    close: float = 100.0,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
    volume: float = 1_000.0,
    timeframe: Timeframe = Timeframe.MIN_1,
    source: str = "test",
) -> MarketBar:
    open_ = close if open_ is None else open_
    return MarketBar(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        open=open_,
        high=max(open_, close) + 0.05 if high is None else high,
        low=min(open_, close) - 0.05 if low is None else low,
        close=close,
        volume=volume,
        source=source,
    )


def random_walk_bars(
    n: int, *, seed: int = 1, symbol: str = "TEST", start: datetime = SESSION_OPEN
) -> list[MarketBar]:
    """`n` consecutive 1-minute bars of a seeded random walk (all inside one regular session when n <= 390)."""
    rng = random.Random(seed)
    price = 100.0
    bars: list[MarketBar] = []
    for i in range(n):
        open_ = price
        price = round(price * math.exp(rng.gauss(0.0, 0.001)), 2)
        high = max(open_, price) + round(rng.uniform(0.0, 0.05), 2)
        low = min(open_, price) - round(rng.uniform(0.0, 0.05), 2)
        bars.append(
            make_bar(
                symbol=symbol,
                start=start + timedelta(minutes=i),
                close=price,
                open_=open_,
                high=high,
                low=low,
                volume=float(rng.randint(500, 5_000)),
            )
        )
    return bars


def make_quote(
    symbol: str = "TEST", *, at: datetime = SESSION_OPEN, bid: float = 99.99, ask: float = 100.01
) -> MarketQuote:
    return MarketQuote(
        symbol=symbol, timestamp=at, bid=bid, ask=ask, bid_size=500, ask_size=300, source="test"
    )


def make_features(
    values: Mapping[str, float], *, symbol: str = "TEST", close: float = 100.0
) -> FeatureVector:
    return FeatureVector(
        feature_id="F-TEST",
        symbol=symbol,
        timestamp=SESSION_OPEN + timedelta(hours=1),
        timeframe=Timeframe.MIN_1,
        feature_version="test",
        spec_hash="test",
        close=close,
        values=dict(values),
        window_start=SESSION_OPEN,
        window_end=SESSION_OPEN + timedelta(hours=1),
        n_bars=60,
    )


def make_config(overrides: Mapping[str, Any] | None = None) -> AppConfig:
    """Default configuration, isolated from the developer's environment variables."""
    return load_config(overrides=dict(overrides or {}), environ={})


def sqlite_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


def sim_config(symbols: list[str], extra: Mapping[str, Any] | None = None) -> AppConfig:
    base: dict[str, Any] = {
        "trading": {"mode": "backtest", "symbols": symbols},
        "logging": {"level": "WARNING"},
    }
    return make_config(deep_merge(base, extra or {}))
