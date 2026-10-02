"""AlpacaMarketDataAdapter: real-time minute bars and quotes from Alpaca's market data WebSocket.

- Feed from configuration (`market_data.alpaca.feed`): `iex` is free on the Basic plan (one exchange, part of the
  volume); `sip` (consolidated tape) needs a paid subscription for real time.
- Protocol: connect to `<stream_url>/<feed>`, authenticate, subscribe to `bars` and `quotes`. Messages arrive as
  JSON arrays (`T` = "b" bar, "q" quote, "u" corrected bar, "success", "subscription", "error").
- Corrected bars ("u") are counted and ignored: the decision on the original bar was already taken.
- Reconnection: exponential backoff with jitter; after reconnecting, the minutes missed are backfilled by REST
  before live bars resume, so the feature window has no silent hole.
- A bar for [t, t+1m) is published by Alpaca shortly after t+1m; IEX publishes no bar for a minute without IEX
  trades, which the data quality engine treats as a gap.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from packages.brokers.alpaca.mapping import parse_time
from packages.common.clock import Clock
from packages.common.config import AlpacaDataSection
from packages.common.entities import MarketBar, MarketDataHealth, MarketEvent, MarketQuote
from packages.common.enums import ConnectionStatus, Timeframe
from packages.common.errors import DataError
from packages.common.websocket import Connector, WebSocketLike, backoff_seconds, decode, websockets_connector
from packages.market_data.base import MarketDataAdapter

log = logging.getLogger(__name__)

FATAL_CODES = {402: "authentication failed", 404: "authentication timeout", 406: "connection limit exceeded "
               "(another program is using this Alpaca data stream)", 409: "insufficient subscription for this feed"}  # fmt: skip


class AlpacaMarketDataAdapter(MarketDataAdapter):
    name = "alpaca"

    def __init__(
        self,
        config: AlpacaDataSection,
        clock: Clock,
        *,
        key: str,
        secret: str,
        transport: httpx.AsyncBaseTransport | None = None,
        connector: Connector = websockets_connector,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._cfg = config
        self._clock = clock
        self._key = key
        self._secret = secret
        self._transport = transport
        self._connector = connector
        self._sleep = sleep
        self._client: httpx.AsyncClient | None = None
        self._bars: list[str] = []
        self._quotes: list[str] = []
        self._timeframe = Timeframe.MIN_1
        self._queue: asyncio.Queue[MarketEvent | BaseException | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._connected = False
        self._ready = asyncio.Event()
        self._last_message_at: datetime | None = None
        self._last_bar_start: dict[str, datetime] = {}
        self._detail = ""
        self.corrected_bars = 0
        self.backfilled_bars = 0
        self.reconnections = 0

    @property
    def source(self) -> str:
        return f"alpaca:{self._cfg.feed}"

    # ------------------------------------------------------------------ MarketDataAdapter

    async def connect(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._cfg.data_url,
                headers={"APCA-API-KEY-ID": self._key, "APCA-API-SECRET-KEY": self._secret},
                transport=self._transport,
                timeout=30.0,
            )
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        self._queue.put_nowait(None)
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def subscribe_bars(self, symbols: Sequence[str], timeframe: Timeframe) -> None:
        if Timeframe(timeframe) is not Timeframe.MIN_1:
            raise DataError(
                "the Alpaca stream publishes 1Min bars; aggregate with trading.decision_timeframe"
            )
        self._bars = sorted(set(self._bars) | set(symbols))
        self._timeframe = timeframe

    async def subscribe_quotes(self, symbols: Sequence[str]) -> None:
        self._quotes = sorted(set(self._quotes) | set(symbols))

    async def subscribe_trades(self, symbols: Sequence[str]) -> None:
        raise DataError("trade-by-trade data is not used by the engine (bars and quotes only)")

    async def get_historical_bars(
        self, symbol: str, start: datetime, end: datetime, timeframe: Timeframe = Timeframe.MIN_1
    ) -> list[MarketBar]:
        if self._client is None:
            await self.connect()
        assert self._client is not None
        params: dict[str, Any] = {
            "symbols": symbol, "timeframe": "1Min", "start": _rfc3339(start), "end": _rfc3339(end),
            "feed": self._cfg.feed, "adjustment": "raw", "limit": 10_000, "sort": "asc",
        }  # fmt: skip
        bars: list[MarketBar] = []
        while True:
            payload = await self._get("/v2/stocks/bars", params)
            for row in (payload.get("bars") or {}).get(symbol) or []:
                bar = self._bar(symbol, row)
                if start <= bar.start < end:
                    bars.append(bar)
            token = payload.get("next_page_token")
            if not token:
                return bars
            params["page_token"] = token

    async def stream(self) -> AsyncIterator[MarketEvent]:
        if not self._bars and not self._quotes:
            raise DataError("subscribe to bars or quotes before streaming")
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="alpaca-market-data")
        while True:
            item = await self._queue.get()
            if item is None:
                if not self._connected:
                    return
                continue
            if isinstance(item, BaseException):
                raise item
            yield item

    async def health(self) -> MarketDataHealth:
        ready = self._connected and self._ready.is_set()
        return MarketDataHealth(
            provider=self.source,
            status=ConnectionStatus.CONNECTED if ready else ConnectionStatus.DISCONNECTED,
            connected=ready,
            last_message_at=self._last_message_at,
            subscribed_symbols=tuple(sorted(set(self._bars) | set(self._quotes))),
            detail=self._detail,
        )

    # ------------------------------------------------------------------ stream

    async def _run(self) -> None:
        attempt = 0
        first = True
        while self._connected:
            socket: WebSocketLike | None = None
            try:
                socket = await self._connector(f"{self._cfg.stream_url.rstrip('/')}/{self._cfg.feed}")
                await self._handshake(socket)
                if not first:
                    self.reconnections += 1
                    await self._backfill()
                first = False
                attempt = 0
                self._detail = ""
                self._ready.set()
                while True:
                    for message in _messages(await socket.recv()):
                        self._handle(message)
            except asyncio.CancelledError:
                raise
            except DataError as exc:
                self._ready.clear()
                self._detail = str(exc)
                self._queue.put_nowait(exc)  # fatal (keys, plan, another connection): stop the run
                return
            except Exception as exc:
                self._ready.clear()
                self._detail = f"market data stream lost: {exc}"
                log.warning(
                    "market data stream lost; reconnecting", extra={"error": str(exc), "attempt": attempt}
                )
                await self._sleep(backoff_seconds(attempt))
                attempt += 1
            finally:
                if socket is not None:
                    with contextlib.suppress(Exception):
                        await socket.close()

    async def _handshake(self, socket: WebSocketLike) -> None:
        await self._expect(socket, "connected")
        await socket.send(json.dumps({"action": "auth", "key": self._key, "secret": self._secret}))
        await self._expect(socket, "authenticated")
        await socket.send(json.dumps({"action": "subscribe", "bars": self._bars, "quotes": self._quotes}))
        while True:
            for message in _messages(await socket.recv()):
                self._check_error(message)
                if message.get("T") == "subscription":
                    return

    async def _expect(self, socket: WebSocketLike, text: str) -> None:
        while True:
            for message in _messages(await socket.recv()):
                self._check_error(message)
                if message.get("T") == "success" and message.get("msg") == text:
                    return

    @staticmethod
    def _check_error(message: dict[str, Any]) -> None:
        if message.get("T") != "error":
            return
        code = int(message.get("code", 0))
        text = FATAL_CODES.get(code, message.get("msg", "unknown error"))
        if code in FATAL_CODES:
            raise DataError(f"Alpaca market data: {text} (code {code})")
        raise ConnectionError(f"Alpaca market data error {code}: {text}")

    def _handle(self, message: dict[str, Any]) -> None:
        kind = message.get("T")
        now = self._clock.now()
        if kind == "b":
            self._last_message_at = now
            bar = self._bar(message["S"], message, received_at=now)
            last = self._last_bar_start.get(bar.symbol)
            if last is None or bar.start > last:
                self._last_bar_start[bar.symbol] = bar.start
            self._queue.put_nowait(bar)
        elif kind == "q":
            self._last_message_at = now
            quote = self._quote(message, received_at=now)
            if quote is not None:
                self._queue.put_nowait(quote)
        elif kind == "u":
            self.corrected_bars += 1
        elif kind == "error":
            self._check_error(message)

    async def _backfill(self) -> None:
        """Minutes missed while disconnected, by REST, before live bars resume (ordered by bar end)."""
        now = self._clock.now()
        missed: list[MarketBar] = []
        for symbol in self._bars:
            last = self._last_bar_start.get(symbol)
            if last is None:
                continue
            start = last + timedelta(minutes=1)
            if start >= now:
                continue
            bars = await self.get_historical_bars(symbol, start, now)
            missed.extend(b for b in bars if b.end <= now)
        for bar in sorted(missed, key=lambda b: (b.end, b.symbol)):
            self._last_bar_start[bar.symbol] = max(self._last_bar_start.get(bar.symbol, bar.start), bar.start)
            self.backfilled_bars += 1
            self._queue.put_nowait(bar)

    # ------------------------------------------------------------------ parsing & HTTP

    def _bar(self, symbol: str, row: dict[str, Any], *, received_at: datetime | None = None) -> MarketBar:
        start = parse_time(row["t"])
        assert start is not None
        return MarketBar(
            symbol=symbol,
            timeframe=Timeframe.MIN_1,
            start=start,
            open=float(row["o"]),
            high=float(row["h"]),
            low=float(row["l"]),
            close=float(row["c"]),
            volume=float(row["v"]),
            vwap=float(row["vw"]) if row.get("vw") else None,
            trade_count=int(row["n"]) if row.get("n") else None,
            source=self.source,
            received_at=received_at,
        )

    def _quote(self, row: dict[str, Any], *, received_at: datetime) -> MarketQuote | None:
        bid, ask = float(row.get("bp") or 0.0), float(row.get("ap") or 0.0)
        if bid <= 0 or ask <= 0 or ask < bid:
            return None  # one side empty on this exchange, or crossed: not a usable quote
        stamp = parse_time(row.get("t"))
        return MarketQuote(
            symbol=row["S"],
            timestamp=stamp or received_at,
            bid=bid,
            ask=ask,
            bid_size=float(row.get("bs") or 0.0),
            ask_size=float(row.get("as") or 0.0),
            source=self.source,
            received_at=received_at,
        )

    async def _get(self, path: str, params: dict[str, Any]) -> Any:
        assert self._client is not None
        for attempt in range(4):
            try:
                response = await self._client.get(path, params=params)
            except httpx.TransportError as exc:
                if attempt == 3:
                    raise ConnectionError(f"Alpaca data unreachable: {exc}") from exc
                await self._sleep(backoff_seconds(attempt, base=0.5, cap=5.0))
                continue
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 3:
                await self._sleep(backoff_seconds(attempt, base=0.5, cap=5.0))
                continue
            if response.status_code in (401, 403):
                raise DataError(
                    f"Alpaca refused the data request ({response.status_code}): {response.text[:200]}"
                )
            if response.status_code >= 400:
                raise ConnectionError(f"Alpaca data error {response.status_code}: {response.text[:200]}")
            return response.json()
        raise ConnectionError("Alpaca data request failed after retries")  # pragma: no cover


def _messages(raw: str | bytes) -> list[dict[str, Any]]:
    payload = decode(raw)
    if isinstance(payload, dict):
        return [payload]
    return [m for m in payload if isinstance(m, dict)]


def _rfc3339(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
