"""Injected clocks. Components never call datetime.now() directly, so every mode is reproducible."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol


def ensure_utc(value: datetime) -> datetime:
    """Return `value` in UTC. Naive datetimes are rejected: their meaning is ambiguous."""
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"naive datetime not allowed: {value!r}")
    return value.astimezone(UTC)


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """Wall clock (paper, shadow, live)."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class SimulatedClock:
    """Manually advanced clock (backtest, replay). Time never moves backwards."""

    def __init__(self, start: datetime) -> None:
        self._now = ensure_utc(start)

    def now(self) -> datetime:
        return self._now

    def advance_to(self, when: datetime) -> None:
        when = ensure_utc(when)
        if when < self._now:
            raise ValueError(f"simulated clock cannot go backwards: {when} < {self._now}")
        self._now = when

    def advance(self, delta: timedelta) -> None:
        self.advance_to(self._now + delta)
