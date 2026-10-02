"""Classify every bar before it can influence a decision.

STALE and INVALID always lead to NO_TRADE. DEGRADED blocks new entries when `block_on_degraded` is true.
The engine never repairs data: it only describes what is wrong.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime

import numpy as np

from packages.common.config import DataQualitySection
from packages.common.entities import DataIssue, DataQualityReport, MarketBar, MarketQuote
from packages.common.enums import DataQualityStatus
from packages.market_data.engine import IngestResult

INVALID = DataQualityStatus.INVALID
STALE = DataQualityStatus.STALE
DEGRADED = DataQualityStatus.DEGRADED


class DataQualityEngine:
    def __init__(self, config: DataQualitySection) -> None:
        self._cfg = config

    @property
    def config(self) -> DataQualitySection:
        return self._cfg

    def evaluate_bar(
        self,
        bar: MarketBar,
        ingest: IngestResult,
        history: Sequence[MarketBar],
        quote: MarketQuote | None,
        now: datetime,
        bars_since_gap: int | None = None,
    ) -> DataQualityReport:
        """`history` holds previously committed bars of the same symbol (oldest first, excluding `bar`)."""
        cfg = self._cfg
        issues: list[DataIssue] = []

        prices = (bar.open, bar.high, bar.low, bar.close)
        if not all(math.isfinite(p) and p > 0 for p in prices):
            issues.append(DataIssue(code="non_positive_price", status=INVALID, detail=str(prices)))
        elif (
            bar.high + 1e-9 < max(bar.open, bar.close)
            or bar.low - 1e-9 > min(bar.open, bar.close)
            or bar.high + 1e-9 < bar.low
        ):
            issues.append(DataIssue(code="ohlc_inconsistent", status=INVALID, detail=str(prices)))
        if not math.isfinite(bar.volume) or bar.volume < 0:
            issues.append(DataIssue(code="invalid_volume", status=INVALID, detail=str(bar.volume)))
        future_skew = (bar.end - now).total_seconds()
        if future_skew > cfg.max_future_skew_seconds:
            issues.append(DataIssue(code="timestamp_in_future", status=INVALID, detail=f"{future_skew:.1f}s"))
        if ingest.conflicting:
            issues.append(DataIssue(code="conflicting_duplicate", status=INVALID))
        if ingest.out_of_order:
            issues.append(DataIssue(code="out_of_order", status=INVALID))

        delay = (now - bar.end).total_seconds()
        if delay > cfg.max_bar_delay_seconds:
            issues.append(DataIssue(code="bar_delayed", status=STALE, detail=f"{delay:.0f}s"))

        if ingest.gap_bars > 0:
            issues.append(DataIssue(code="gap", status=DEGRADED, detail=f"{ingest.gap_bars} missing bars"))
        elif bars_since_gap is not None and bars_since_gap < cfg.gap_memory_bars:
            issues.append(DataIssue(code="recent_gap", status=DEGRADED, detail=f"{bars_since_gap} bars ago"))
        if bar.volume == 0:
            issues.append(DataIssue(code="zero_volume", status=DEGRADED))

        jump = self._jump_sigma(bar, history)
        if jump is not None and jump > cfg.max_abs_return_sigma:
            issues.append(DataIssue(code="abnormal_jump", status=DEGRADED, detail=f"{jump:.1f} sigma"))

        if quote is not None:
            if quote.bid <= 0 or quote.ask <= 0 or quote.ask <= quote.bid:
                issues.append(DataIssue(code="crossed_or_locked_quote", status=DEGRADED))
            elif quote.spread_bps > cfg.max_spread_bps:
                issues.append(
                    DataIssue(code="wide_spread", status=DEGRADED, detail=f"{quote.spread_bps:.1f}bps")
                )
            if (now - quote.timestamp).total_seconds() > cfg.max_bar_delay_seconds:
                issues.append(DataIssue(code="stale_quote", status=DEGRADED))

        status = DataQualityStatus.worst([issue.status for issue in issues])
        return DataQualityReport(symbol=bar.symbol, timestamp=bar.end, status=status, issues=tuple(issues))

    def _jump_sigma(self, bar: MarketBar, history: Sequence[MarketBar]) -> float | None:
        lookback = self._cfg.jump_lookback_bars
        closes = [b.close for b in history[-(lookback + 1) :]]
        if len(closes) < lookback + 1 or bar.close <= 0 or any(c <= 0 for c in closes):
            return None
        returns = np.diff(np.log(np.asarray(closes, dtype=float)))
        sigma = float(np.std(returns, ddof=1))
        if sigma <= 0:
            return None
        return abs(math.log(bar.close / closes[-1])) / sigma

    @staticmethod
    def feed_is_stale(now: datetime, last_message_at: datetime | None, stale_after_seconds: float) -> bool:
        """True when a feed that should be active has been silent for too long."""
        return last_message_at is None or (now - last_message_at).total_seconds() > stale_after_seconds
