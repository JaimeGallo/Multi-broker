"""In-memory stand-in for Alpaca's REST market data API (calendar + paginated bars), built on httpx.MockTransport.

Prices come from the synthetic market, so tests can run real backtests on a "downloaded" dataset offline.
Each regular-session day also gets pre-market and after-hours bars, which the dataset builder must drop.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import httpx

from packages.common.calendar import RegularHoursCalendar
from packages.common.config import SyntheticMarketSection
from packages.market_data.synthetic import SyntheticMarket

HOLIDAY = date(2024, 3, 29)  # Good Friday 2024: NYSE closed
EARLY_CLOSE = date(2024, 7, 3)  # 13:00 close
INDEPENDENCE_DAY = date(2024, 7, 4)


class FakeAlpaca:
    def __init__(
        self, *, page_size: int = 500, holidays: tuple[date, ...] = (HOLIDAY, INDEPENDENCE_DAY)
    ) -> None:
        self.page_size = page_size
        self.holidays = holidays
        self.requests: list[httpx.Request] = []
        self.fail_next: list[int] = []
        self._cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def _calendar(self, start: date, end: date) -> list[dict[str, str]]:
        days = []
        day = start
        while day <= end:
            if day.weekday() < 5 and day not in self.holidays:
                close = "13:00" if day == EARLY_CLOSE else "16:00"
                days.append({"date": day.isoformat(), "open": "09:30", "close": close})
            day += timedelta(days=1)
        return days

    def _bars(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        key = (symbol, start.isoformat(), end.isoformat())
        if key in self._cache:
            return self._cache[key]
        calendar = RegularHoursCalendar(holidays=self.holidays)
        market = SyntheticMarket(SyntheticMarketSection(), calendar)
        rows: list[dict[str, Any]] = []
        for _, bars, _ in market.generate([symbol], start, end):
            for bar in bars:
                rows.append(
                    {"t": bar.start.isoformat().replace("+00:00", "Z"), "o": bar.open, "h": bar.high,
                     "l": bar.low, "c": bar.close, "v": bar.volume, "n": bar.trade_count, "vw": bar.vwap}
                )  # fmt: skip
        extended: list[dict[str, Any]] = []
        for row in rows:
            started = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
            session = calendar.session_for(started)
            if session is not None and started == session.open:
                pre = (started - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
                extended.append({**row, "t": pre})
            if session is not None and started == session.close - timedelta(minutes=1):
                post = (started + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
                extended.append({**row, "t": post})
        # An early-close day still trades after 13:00 in our fake feed: the builder must drop those bars.
        rows = sorted(rows + extended, key=lambda r: r["t"])
        self._cache[key] = rows
        return rows

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.fail_next:
            return httpx.Response(self.fail_next.pop(0), text="temporary failure")
        if request.headers.get("APCA-API-KEY-ID") != "test-key":
            return httpx.Response(401, text="unauthorized")
        params = request.url.params
        if request.url.path == "/v2/calendar":
            return httpx.Response(
                200,
                json=self._calendar(date.fromisoformat(params["start"]), date.fromisoformat(params["end"])),
            )
        if request.url.path == "/v2/stocks/bars":
            assert params["feed"] in ("sip", "iex") and params["timeframe"] == "1Min"
            symbol = params["symbols"]
            start = date.fromisoformat(params["start"][:10])
            end = date.fromisoformat(params["end"][:10])
            rows = self._bars(symbol, start, end)
            offset = int(params.get("page_token", "0"))
            page = rows[offset : offset + self.page_size]
            next_token = str(offset + self.page_size) if offset + self.page_size < len(rows) else None
            return httpx.Response(200, json={"bars": {symbol: page}, "next_page_token": next_token})
        return httpx.Response(404, text="not found")
