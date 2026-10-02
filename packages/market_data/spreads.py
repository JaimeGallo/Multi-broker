"""Measured spreads: typical quoted bid/ask spread per symbol, sampled from historical SIP quotes.

Datasets hold bars only, so without this every symbol would pay the same assumed spread. The calibration samples a
few moments of evenly spaced sessions (never the first or last minutes of the session) and keeps, per symbol, the
distribution of the median spread observed in a short window at each moment. The engine uses the per-symbol
MEDIAN as the typical spread (`costs.spread_by_symbol`) for both the Signal Engine and simulated fills.

The result is written next to the dataset (`spreads.json`) with its own content hash, and the values end up in the
configuration recorded with every run, so a backtest and its `verify` always use the same costs.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from packages.common.errors import DataError
from packages.market_data.alpaca_history import AlpacaHistoricalClient, RawQuote, fetch_quotes
from packages.market_data.dataset import Dataset

SPREADS_FILE = "spreads.json"
DEFAULT_TIMES = (time(9, 45), time(10, 30), time(12, 0), time(13, 30), time(15, 0), time(15, 45))
WINDOW = timedelta(seconds=5)
MAX_VALID_SPREAD_BPS = 500.0  # anything wider is a bad print, not a market


def spread_bps(quote: RawQuote) -> float | None:
    if quote.bid <= 0 or quote.ask <= quote.bid:
        return None  # crossed, locked or one-sided
    value = (quote.ask - quote.bid) / ((quote.ask + quote.bid) / 2.0) * 1e4
    return value if value <= MAX_VALID_SPREAD_BPS else None


def sample_days(sessions: Sequence[date], count: int) -> list[date]:
    """`count` sessions spread evenly over the range (deterministic)."""
    if count <= 0 or not sessions:
        return []
    if count >= len(sessions):
        return list(sessions)
    step = (len(sessions) - 1) / (count - 1) if count > 1 else 0
    return sorted({sessions[round(i * step)] for i in range(count)})


def _percentile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low, high = int(position), min(int(position) + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def calibrate_spreads(
    client: AlpacaHistoricalClient,
    dataset: Dataset,
    *,
    days: int = 15,
    times: Sequence[time] = DEFAULT_TIMES,
    symbols: Sequence[str] | None = None,
    timezone: str = "America/New_York",
    progress: Callable[[str, int], None] | None = None,
) -> dict[str, Any]:
    calendar = dataset.calendar(timezone)
    zone = ZoneInfo(timezone)
    sessions = [s.day for s in calendar.sessions_between(dataset.start, dataset.end)]
    chosen = sample_days(sessions, days)
    result: dict[str, Any] = {}
    for symbol in symbols or dataset.symbols:
        samples: list[float] = []
        for day in chosen:
            session = calendar.session_on(day)
            assert session is not None
            for moment in times:
                start = datetime.combine(day, moment, tzinfo=zone).astimezone(UTC)
                if not (
                    session.open + timedelta(minutes=10) <= start <= session.close - timedelta(minutes=10)
                ):
                    continue  # skip the opening/closing auctions and early-close afternoons
                spreads = [
                    s
                    for q in fetch_quotes(client, symbol, start, start + WINDOW)
                    if (s := spread_bps(q)) is not None
                ]
                if spreads:
                    samples.append(statistics.median(spreads))
        if not samples:
            raise DataError(f"no valid quotes found for {symbol}")
        result[symbol] = {
            "median_bps": round(statistics.median(samples), 4),
            "mean_bps": round(statistics.fmean(samples), 4),
            "p25_bps": round(_percentile(samples, 0.25), 4),
            "p75_bps": round(_percentile(samples, 0.75), 4),
            "p90_bps": round(_percentile(samples, 0.90), 4),
            "samples": len(samples),
        }
        if progress is not None:
            progress(symbol, len(samples))
    payload: dict[str, Any] = {
        "dataset": dataset.name,
        "dataset_version": dataset.version,
        "feed": dataset.manifest["feed"],
        "sampled_days": [d.isoformat() for d in chosen],
        "sampled_times": [t.strftime("%H:%M") for t in times],
        "window_seconds": WINDOW.total_seconds(),
        "symbols": result,
    }
    payload["calibration_version"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[
        :16
    ]
    payload["created_at"] = datetime.now(UTC).isoformat()
    return payload


def write_spreads(dataset: Dataset, payload: dict[str, Any]) -> None:
    (dataset.path / SPREADS_FILE).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_spreads(dataset: Dataset) -> dict[str, Any] | None:
    path = dataset.path / SPREADS_FILE
    if not path.exists():
        return None
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("dataset_version") != dataset.version:
        raise DataError(f"{path} was measured for another version of dataset {dataset.name}; recalibrate")
    return payload


def typical_spreads(payload: dict[str, Any]) -> dict[str, float]:
    return {symbol: float(stats["median_bps"]) for symbol, stats in payload["symbols"].items()}
