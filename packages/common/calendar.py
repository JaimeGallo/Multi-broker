"""Market sessions.

`RegularHoursCalendar` knows weekday sessions at fixed local hours. It does NOT know exchange holidays or early
closes unless they are passed in explicitly; the official calendar comes from the broker API in phase 4.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from packages.common.clock import ensure_utc


@dataclass(frozen=True)
class Session:
    day: date
    open: datetime
    close: datetime

    def contains(self, ts: datetime) -> bool:
        return self.open <= ts < self.close

    def minutes_since_open(self, ts: datetime) -> float:
        return (ts - self.open).total_seconds() / 60.0

    def minutes_to_close(self, ts: datetime) -> float:
        return (self.close - ts).total_seconds() / 60.0


class MarketCalendar(Protocol):
    def session_on(self, day: date) -> Session | None: ...

    def session_for(self, ts: datetime) -> Session | None: ...

    def is_open(self, ts: datetime) -> bool: ...

    def trading_date(self, ts: datetime) -> date: ...

    def day_start(self, ts: datetime) -> datetime: ...

    def sessions_between(self, start: date, end: date) -> Iterator[Session]: ...


def _parse_hhmm(value: str) -> time:
    hours, minutes = value.split(":")
    return time(int(hours), int(minutes))


class RegularHoursCalendar:
    def __init__(
        self,
        timezone: str = "America/New_York",
        open_time: time | str = time(9, 30),
        close_time: time | str = time(16, 0),
        holidays: Iterable[date] = (),
        early_closes: Mapping[date, time] | None = None,
    ) -> None:
        self._tz = ZoneInfo(timezone)
        self._open = _parse_hhmm(open_time) if isinstance(open_time, str) else open_time
        self._close = _parse_hhmm(close_time) if isinstance(close_time, str) else close_time
        self._holidays = frozenset(holidays)
        self._early = dict(early_closes or {})

    @property
    def timezone(self) -> ZoneInfo:
        return self._tz

    def session_on(self, day: date) -> Session | None:
        if day.weekday() >= 5 or day in self._holidays:
            return None
        close_time = self._early.get(day, self._close)
        opened = datetime.combine(day, self._open, tzinfo=self._tz).astimezone(UTC)
        closed = datetime.combine(day, close_time, tzinfo=self._tz).astimezone(UTC)
        return Session(day=day, open=opened, close=closed)

    def trading_date(self, ts: datetime) -> date:
        return ensure_utc(ts).astimezone(self._tz).date()

    def day_start(self, ts: datetime) -> datetime:
        """Local midnight (exchange timezone) of the day containing `ts`, in UTC."""
        return datetime.combine(self.trading_date(ts), time(0), tzinfo=self._tz).astimezone(UTC)

    def session_for(self, ts: datetime) -> Session | None:
        session = self.session_on(self.trading_date(ts))
        return session if session is not None and session.contains(ensure_utc(ts)) else None

    def is_open(self, ts: datetime) -> bool:
        return self.session_for(ts) is not None

    def sessions_between(self, start: date, end: date) -> Iterator[Session]:
        day = start
        while day <= end:
            session = self.session_on(day)
            if session is not None:
                yield session
            day += timedelta(days=1)
