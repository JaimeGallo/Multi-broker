from __future__ import annotations

from datetime import timedelta

from packages.common.calendar import RegularHoursCalendar
from packages.common.config import DataQualitySection
from packages.common.enums import DataQualityStatus
from packages.data_quality.engine import DataQualityEngine
from packages.market_data.engine import IngestResult, MarketDataEngine
from tests.helpers import SESSION_OPEN, make_bar, make_quote, random_walk_bars

DQ = DataQualityEngine(DataQualitySection())
OK = IngestResult(accepted=True)


def evaluate(bar, *, ingest=OK, history=(), quote=None, delay_seconds: float = 1.0, bars_since_gap=None):
    return DQ.evaluate_bar(
        bar, ingest, list(history), quote, bar.end + timedelta(seconds=delay_seconds), bars_since_gap
    )


def test_valid_bar() -> None:
    report = evaluate(make_bar())
    assert report.status is DataQualityStatus.VALID and report.issues == ()


def test_invalid_prices_and_timestamps() -> None:
    assert evaluate(make_bar(close=100, high=99, low=98)).status is DataQualityStatus.INVALID
    assert evaluate(make_bar(close=-1, open_=1)).status is DataQualityStatus.INVALID
    future = evaluate(make_bar(), delay_seconds=-60)
    assert future.status is DataQualityStatus.INVALID and "timestamp_in_future" in future.codes
    conflicting = evaluate(make_bar(), ingest=IngestResult(accepted=False, duplicate=True, conflicting=True))
    assert conflicting.status is DataQualityStatus.INVALID


def test_stale_and_degraded() -> None:
    assert evaluate(make_bar(), delay_seconds=600).status is DataQualityStatus.STALE
    gap = evaluate(make_bar(), ingest=IngestResult(accepted=True, gap_bars=3))
    assert gap.status is DataQualityStatus.DEGRADED and "gap" in gap.codes
    assert "recent_gap" in evaluate(make_bar(), bars_since_gap=2).codes
    assert evaluate(make_bar(volume=0)).status is DataQualityStatus.DEGRADED
    wide = evaluate(make_bar(), quote=make_quote(at=SESSION_OPEN + timedelta(minutes=1), bid=99.0, ask=101.0))
    assert "wide_spread" in wide.codes


def test_abnormal_jump_is_degraded() -> None:
    history = random_walk_bars(30)
    jump = make_bar(start=history[-1].end, close=round(history[-1].close * 1.2, 2), open_=history[-1].close)
    report = evaluate(jump, history=history)
    assert report.status is DataQualityStatus.DEGRADED and "abnormal_jump" in report.codes


def test_market_engine_dedupes_orders_and_counts_gaps() -> None:
    engine = MarketDataEngine(RegularHoursCalendar())
    first = make_bar(start=SESSION_OPEN)
    engine.commit_bar(first, engine.check_bar(first), first.end)
    assert engine.check_bar(first).duplicate and not engine.check_bar(first).conflicting
    assert engine.check_bar(first.model_copy(update={"close": 101.0, "high": 101.1})).conflicting
    late = make_bar(start=SESSION_OPEN - timedelta(minutes=1))
    assert engine.check_bar(late).out_of_order
    gapped = make_bar(start=SESSION_OPEN + timedelta(minutes=4))
    result = engine.check_bar(gapped)
    assert result.accepted and result.gap_bars == 3
    engine.commit_bar(gapped, result, gapped.end)
    assert engine.bars_since_gap("TEST") == 0
    assert engine.stats()["TEST"].missing_bars == 3


def test_overnight_is_not_a_gap() -> None:
    engine = MarketDataEngine(RegularHoursCalendar())
    last = make_bar(start=SESSION_OPEN + timedelta(minutes=389))
    engine.commit_bar(last, engine.check_bar(last), last.end)
    next_open = make_bar(start=SESSION_OPEN + timedelta(days=1))
    assert engine.check_bar(next_open).gap_bars == 0


def test_feed_staleness() -> None:
    now = SESSION_OPEN + timedelta(minutes=10)
    assert DataQualityEngine.feed_is_stale(now, None, 60)
    assert DataQualityEngine.feed_is_stale(now, now - timedelta(seconds=120), 60)
    assert not DataQualityEngine.feed_is_stale(now, now - timedelta(seconds=30), 60)
