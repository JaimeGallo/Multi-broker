"""Provider-independent ingestion: deduplication, ordering, gap detection, latency and buffering."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

from packages.common.calendar import MarketCalendar
from packages.common.entities import MarketBar, MarketQuote, MarketTrade


@dataclass(frozen=True)
class IngestResult:
    accepted: bool
    duplicate: bool = False
    conflicting: bool = False
    out_of_order: bool = False
    gap_bars: int = 0


@dataclass
class FeedStats:
    bars: int = 0
    duplicates: int = 0
    conflicting: int = 0
    out_of_order: int = 0
    gaps: int = 0
    missing_bars: int = 0
    quotes: int = 0
    trades: int = 0
    last_latency_ms: float | None = None
    max_latency_ms: float = 0.0


@dataclass
class _Feed:
    bars: deque[MarketBar]
    last_quote: MarketQuote | None = None
    last_trade: MarketTrade | None = None
    bars_since_gap: int | None = None
    stats: FeedStats = field(default_factory=FeedStats)


class MarketDataEngine:
    """Two-step bar ingestion: `check_bar` (dedupe/order/gaps) then `commit_bar` once quality allows it.

    INVALID bars are never committed, so they cannot contaminate the feature window.
    """

    def __init__(self, calendar: MarketCalendar, buffer_size: int = 512) -> None:
        self._calendar = calendar
        self._buffer_size = buffer_size
        self._feeds: dict[str, _Feed] = {}
        self._last_message_at: datetime | None = None

    def _feed(self, symbol: str) -> _Feed:
        feed = self._feeds.get(symbol)
        if feed is None:
            feed = _Feed(bars=deque(maxlen=self._buffer_size))
            self._feeds[symbol] = feed
        return feed

    @property
    def last_message_at(self) -> datetime | None:
        return self._last_message_at

    def _touch(self, when: datetime | None) -> None:
        if when is not None and (self._last_message_at is None or when > self._last_message_at):
            self._last_message_at = when

    def check_bar(self, bar: MarketBar) -> IngestResult:
        feed = self._feeds.get(bar.symbol)
        last = feed.bars[-1] if feed is not None and feed.bars else None
        if last is None:
            return IngestResult(accepted=True)
        if bar.start == last.start:
            return IngestResult(accepted=False, duplicate=True, conflicting=not bar.same_values(last))
        if bar.start < last.start:
            return IngestResult(accepted=False, out_of_order=True)
        return IngestResult(accepted=True, gap_bars=self._missing_between(last, bar))

    def record_rejected(self, bar: MarketBar, result: IngestResult, received_at: datetime | None) -> None:
        stats = self._feed(bar.symbol).stats
        if result.duplicate:
            stats.duplicates += 1
        if result.conflicting:
            stats.conflicting += 1
        if result.out_of_order:
            stats.out_of_order += 1
        self._touch(received_at)

    def commit_bar(self, bar: MarketBar, result: IngestResult, received_at: datetime | None) -> None:
        feed = self._feed(bar.symbol)
        feed.bars.append(bar)
        stats = feed.stats
        stats.bars += 1
        if result.gap_bars > 0:
            feed.bars_since_gap = 0
            stats.gaps += 1
            stats.missing_bars += result.gap_bars
        elif feed.bars_since_gap is not None:
            feed.bars_since_gap += 1
        if received_at is not None:
            latency = (received_at - bar.end).total_seconds() * 1000.0
            stats.last_latency_ms = latency
            stats.max_latency_ms = max(stats.max_latency_ms, latency)
        self._touch(received_at)

    def ingest_quote(self, quote: MarketQuote, received_at: datetime | None) -> bool:
        feed = self._feed(quote.symbol)
        self._touch(received_at)
        if feed.last_quote is not None and quote.timestamp < feed.last_quote.timestamp:
            feed.stats.out_of_order += 1
            return False
        feed.last_quote = quote
        feed.stats.quotes += 1
        return True

    def ingest_trade(self, trade: MarketTrade, received_at: datetime | None) -> bool:
        feed = self._feed(trade.symbol)
        self._touch(received_at)
        if feed.last_trade is not None and trade.timestamp < feed.last_trade.timestamp:
            feed.stats.out_of_order += 1
            return False
        feed.last_trade = trade
        feed.stats.trades += 1
        return True

    def bars(self, symbol: str) -> list[MarketBar]:
        feed = self._feeds.get(symbol)
        return list(feed.bars) if feed is not None else []

    def last_bar(self, symbol: str) -> MarketBar | None:
        feed = self._feeds.get(symbol)
        return feed.bars[-1] if feed is not None and feed.bars else None

    def latest_quote(self, symbol: str) -> MarketQuote | None:
        feed = self._feeds.get(symbol)
        return feed.last_quote if feed is not None else None

    def bars_since_gap(self, symbol: str) -> int | None:
        feed = self._feeds.get(symbol)
        return feed.bars_since_gap if feed is not None else None

    def stats(self) -> dict[str, FeedStats]:
        return {symbol: feed.stats for symbol, feed in self._feeds.items()}

    def _missing_between(self, last: MarketBar, bar: MarketBar) -> int:
        step = bar.timeframe.delta
        expected = last.end
        if bar.start <= expected:
            return 0
        previous_session = self._calendar.session_for(last.start)
        current_session = self._calendar.session_for(bar.start)
        if previous_session is not None and current_session is not None and previous_session.day == current_session.day:
            return int((bar.start - expected) / step)
        missing = 0
        if previous_session is not None:
            missing += max(0, int((previous_session.close - expected) / step))
        if current_session is not None:
            missing += max(0, int((bar.start - current_session.open) / step))
        return missing
