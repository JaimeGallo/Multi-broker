"""Aggregate fine bars (e.g. 1Min) into coarser, clock-aligned bars (e.g. 5Min).

The same aggregator runs in every mode, so 5-minute decisions are identical in backtest and paper.
"""

from __future__ import annotations

from datetime import UTC, datetime

from packages.common.entities import MarketBar
from packages.common.enums import Timeframe


class BarAggregator:
    def __init__(self, source: Timeframe, target: Timeframe) -> None:
        if target.minutes % source.minutes != 0 or target.minutes < source.minutes:
            raise ValueError(f"cannot aggregate {source.value} into {target.value}")
        self._source = source
        self._target = target
        self._buckets: dict[str, list[MarketBar]] = {}

    @property
    def target(self) -> Timeframe:
        return self._target

    def bucket_start(self, ts: datetime) -> datetime:
        epoch_minutes = int(ts.timestamp() // 60)
        floored = epoch_minutes - epoch_minutes % self._target.minutes
        return datetime.fromtimestamp(floored * 60, UTC)

    def add(self, bar: MarketBar) -> list[MarketBar]:
        """Add a source bar; return the coarser bars completed by it (a partial bucket is flushed when the
        next bucket starts, e.g. when the last minute of a bucket had no bar)."""
        completed: list[MarketBar] = []
        start = self.bucket_start(bar.start)
        bucket = self._buckets.pop(bar.symbol, [])
        if bucket and self.bucket_start(bucket[0].start) != start:
            completed.append(self._merge(bucket))
            bucket = []
        bucket.append(bar)
        if bar.end >= start + self._target.delta:
            completed.append(self._merge(bucket))
        else:
            self._buckets[bar.symbol] = bucket
        return completed

    def _merge(self, bars: list[MarketBar]) -> MarketBar:
        volume = sum(b.volume for b in bars)
        vwap = sum(b.typical_price * b.volume for b in bars) / volume if volume > 0 else None
        counts = [b.trade_count for b in bars]
        return MarketBar(
            symbol=bars[0].symbol,
            timeframe=self._target,
            start=self.bucket_start(bars[0].start),
            open=bars[0].open,
            high=max(b.high for b in bars),
            low=min(b.low for b in bars),
            close=bars[-1].close,
            volume=volume,
            vwap=vwap,
            trade_count=None if any(c is None for c in counts) else sum(c for c in counts if c is not None),
            source=bars[0].source,
            received_at=bars[-1].received_at,
        )
