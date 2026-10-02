"""Alpaca historical market data (REST): minute bars and the official trading calendar.

Only official, documented endpoints are used:
- `GET {data_url}/v2/stocks/bars`   bars for many symbols, paginated with `next_page_token`;
- `GET {trading_url}/v2/calendar`   trading days with open/close times (holidays and early closes).

Keys come from the environment (`APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`), never from configuration. The calendar
is read from the PAPER trading API. On the free (Basic) plan SIP data of the last 15 minutes is not available, so
downloads must end before today.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from packages.common.config import AlpacaDataSection
from packages.common.errors import ConfigError, DataError

log = logging.getLogger(__name__)

KEY_ENV = "APCA_API_KEY_ID"
SECRET_ENV = "APCA_API_SECRET_KEY"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_RETRIES = 5


@dataclass(frozen=True)
class RawBar:
    symbol: str
    t: str  # RFC 3339 start of the bar (UTC)
    o: float
    h: float
    low: float
    c: float
    v: float
    n: int | None
    vw: float | None


@dataclass(frozen=True)
class CalendarDay:
    day: date
    open: str  # "HH:MM", exchange local time
    close: str


def credentials(environ: Mapping[str, str] | None = None) -> tuple[str, str]:
    env = os.environ if environ is None else environ
    key, secret = env.get(KEY_ENV, "").strip(), env.get(SECRET_ENV, "").strip()
    if not key or not secret:
        raise ConfigError(
            f"Alpaca data needs {KEY_ENV} and {SECRET_ENV} in the environment (.env, never in git)"
        )
    return key, secret


class AlpacaHistoricalClient:
    def __init__(
        self,
        config: AlpacaDataSection,
        *,
        key: str,
        secret: str,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        timeout: float = 30.0,
    ) -> None:
        self._cfg = config
        self._sleep = sleep
        self._last_request = 0.0
        self._client = httpx.Client(
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret, "Accept": "application/json"},
            transport=transport,
            timeout=timeout,
        )
        self.requests = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> AlpacaHistoricalClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _get(self, url: str, params: Mapping[str, Any]) -> Any:
        for attempt in range(MAX_RETRIES + 1):
            wait = self._cfg.min_request_interval_seconds - (time.monotonic() - self._last_request)
            if wait > 0:
                self._sleep(wait)
            self._last_request = time.monotonic()
            self.requests += 1
            try:
                response = self._client.get(url, params=dict(params))
            except httpx.TransportError as exc:
                if attempt == MAX_RETRIES:
                    raise DataError(f"Alpaca request failed: {exc}") from exc
                self._sleep(min(30.0, 2.0**attempt))
                continue
            if response.status_code in RETRY_STATUSES and attempt < MAX_RETRIES:
                retry_after = response.headers.get("retry-after")
                self._sleep(
                    float(retry_after) if retry_after and retry_after.isdigit() else min(30.0, 2.0**attempt)
                )
                continue
            if response.status_code in (401, 403):
                raise DataError(
                    f"Alpaca refused the request ({response.status_code}): {response.text[:200]}. Check the keys, "
                    "and on the free plan request SIP data older than 15 minutes only."
                )
            if response.status_code >= 400:
                raise DataError(f"Alpaca error {response.status_code}: {response.text[:200]}")
            return response.json()
        raise DataError("Alpaca request failed after retries")  # pragma: no cover

    def calendar(self, start: date, end: date) -> list[CalendarDay]:
        payload = self._get(
            f"{self._cfg.trading_url}/v2/calendar", {"start": start.isoformat(), "end": end.isoformat()}
        )
        return [CalendarDay(date.fromisoformat(d["date"]), d["open"], d["close"]) for d in payload]

    def bars(
        self, symbols: Sequence[str], start: date, end: date, timeframe: str = "1Min"
    ) -> Iterator[RawBar]:
        """All bars (including extended hours) for `start..end` inclusive, page by page."""
        params: dict[str, Any] = {
            "symbols": ",".join(symbols),
            "timeframe": timeframe,
            "start": f"{start.isoformat()}T00:00:00Z",
            "end": f"{end.isoformat()}T23:59:59Z",
            "feed": self._cfg.feed,
            "adjustment": self._cfg.adjustment,
            "limit": self._cfg.page_limit,
            "sort": "asc",
        }
        while True:
            payload = self._get(f"{self._cfg.data_url}/v2/stocks/bars", params)
            for symbol, rows in (payload.get("bars") or {}).items():
                for row in rows or []:
                    yield RawBar(
                        symbol=symbol,
                        t=row["t"],
                        o=float(row["o"]),
                        h=float(row["h"]),
                        low=float(row["l"]),
                        c=float(row["c"]),
                        v=float(row["v"]),
                        n=int(row["n"]) if row.get("n") is not None else None,
                        vw=float(row["vw"]) if row.get("vw") is not None else None,
                    )
            token = payload.get("next_page_token")
            if not token:
                return
            params["page_token"] = token
