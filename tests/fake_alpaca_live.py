"""In-memory Alpaca for phase 4 tests: paper trading REST API, `trade_updates` stream, market data stream and a
tiny exchange that fills orders on the next bar (bracket legs with OCO behaviour, Alpaca's "insufficient qty"
rule while legs hold the shares, duplicate client_order_id rejection).

The test drives time: `advance(bar_minute)` moves the shared clock, lets the exchange trade through the bars, then
publishes them on the data stream, as Alpaca does a moment after each minute closes.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from packages.common.calendar import RegularHoursCalendar
from packages.common.clock import SimulatedClock
from packages.common.entities import MarketBar
from tests.fake_alpaca import FakeAlpaca

TRADING_HOST = "paper-api.alpaca.markets"
DATA_HOST = "data.alpaca.markets"


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class FakeSocket:
    def __init__(self, server: FakeAlpacaLive, kind: str) -> None:
        self.server = server
        self.kind = kind
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.ready = False
        self.closed = False
        if kind == "data":
            self.inbox.put_nowait(json.dumps([{"T": "success", "msg": "connected"}]))

    async def send(self, message: str) -> None:
        payload = json.loads(message)
        action = payload.get("action")
        if action == "authenticate" and self.kind == "trading":
            ok = (payload.get("data") or {}).get("key_id") == "test-key"
            status = "authorized" if ok else "unauthorized"
            self.inbox.put_nowait(
                json.dumps({"stream": "authorization", "data": {"status": status, "action": "authenticate"}})
            )
        elif action == "auth":
            ok = payload.get("key") == "test-key"
            if self.kind == "trading":
                status = "authorized" if ok else "unauthorized"
                self.inbox.put_nowait(
                    json.dumps(
                        {"stream": "authorization", "data": {"status": status, "action": "authenticate"}}
                    )
                )
            else:
                reply = (
                    {"T": "success", "msg": "authenticated"}
                    if ok
                    else {"T": "error", "code": 402, "msg": "auth failed"}
                )
                self.inbox.put_nowait(json.dumps([reply]))
        elif action == "listen":
            self.ready = True
            self.inbox.put_nowait(json.dumps({"stream": "listening", "data": {"streams": ["trade_updates"]}}))
        elif action == "subscribe":
            self.ready = True
            self.server.subscriptions.append(payload)
            self.inbox.put_nowait(
                json.dumps(
                    [
                        {
                            "T": "subscription",
                            "bars": payload.get("bars", []),
                            "quotes": payload.get("quotes", []),
                        }
                    ]
                )
            )

    async def recv(self) -> str:
        message = await self.inbox.get()
        if message is None:
            raise ConnectionError("socket dropped by the fake server")
        return message

    async def close(self) -> None:
        self.closed = True

    def drop(self) -> None:
        self.inbox.put_nowait(None)


@dataclass
class FakePosition:
    qty: float = 0.0
    avg: float = 0.0


class FakeAlpacaLive:
    def __init__(self, clock: SimulatedClock, *, cash: float = 100_000.0) -> None:
        self.clock = clock
        self.cash = cash
        self.initial = cash
        self.market = FakeAlpaca(page_size=5000)
        self.orders: dict[str, dict[str, Any]] = {}  # native id -> raw order
        self.by_client: dict[str, str] = {}
        self.positions: dict[str, FakePosition] = {}
        self.last_price: dict[str, float] = {}
        self.sockets: list[FakeSocket] = []
        self.subscriptions: list[dict[str, Any]] = []
        self.order_requests: list[dict[str, Any]] = []
        self.rejections: list[str] = []
        self.live_bars: list[MarketBar] = []

    # ------------------------------------------------------------------ wiring

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    async def connector(self, url: str) -> FakeSocket:
        kind = "trading" if TRADING_HOST in url else "data"
        socket = FakeSocket(self, kind)
        self.sockets.append(socket)
        return socket

    def drop_connections(self, kind: str) -> None:
        for socket in self.sockets:
            if socket.kind == kind and not socket.closed:
                socket.drop()

    # ------------------------------------------------------------------ market

    def regular_bars(self, symbol: str, first: date, day: date) -> list[MarketBar]:
        """Today's regular-session bars of the same series the REST warm-up serves (`first`..`day`)."""
        calendar = RegularHoursCalendar(holidays=self.market.holidays)
        bars = []
        for row in self.market._bars(symbol, first, day):
            start = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
            if start.date() != day or not calendar.is_open(start):
                continue
            bars.append(
                MarketBar(symbol=symbol, timeframe="1Min", start=start, open=row["o"], high=row["h"], low=row["l"],
                          close=row["c"], volume=row["v"], vwap=row["vw"], trade_count=row["n"], source="fake")
            )  # fmt: skip
        return bars

    async def advance(self, bars: Iterable[MarketBar], *, settle: float = 0.02) -> None:
        """One minute of market: clock to the bar end (+1 s), exchange fills, then the bars on the stream."""
        bars = list(bars)
        if not bars:
            return
        end = max(b.end for b in bars)
        self.clock.advance_to(end + timedelta(seconds=1))
        for bar in bars:
            self._trade_through(bar)
        await asyncio.sleep(settle)
        message = json.dumps(
            [
                {
                    "T": "b",
                    "S": b.symbol,
                    "o": b.open,
                    "h": b.high,
                    "l": b.low,
                    "c": b.close,
                    "v": b.volume,
                    "t": _iso(b.start),
                    "n": b.trade_count,
                    "vw": b.vwap,
                }
                for b in bars
            ]
        )
        for socket in self.sockets:
            if socket.kind == "data" and socket.ready and not socket.closed:
                socket.inbox.put_nowait(message)
        self.live_bars.extend(bars)
        await asyncio.sleep(settle)

    # ------------------------------------------------------------------ exchange

    def _trade_through(self, bar: MarketBar) -> None:
        for raw in list(self.orders.values()):
            if raw["symbol"] != bar.symbol or raw["status"] not in ("new", "accepted", "partially_filled"):
                continue
            if datetime.fromisoformat(raw["submitted_at"].replace("Z", "+00:00")) > bar.start:
                continue  # sent during this bar: only later bars can fill it (no look-ahead)
            kind, side = raw["type"], raw["side"]
            price: float | None = None
            if kind == "market":
                price = bar.open
            elif kind == "stop":
                stop = float(raw["stop_price"])
                if (side == "sell" and bar.low <= stop) or (side == "buy" and bar.high >= stop):
                    price = min(bar.open, stop) if side == "sell" else max(bar.open, stop)
            elif kind == "limit":
                limit = float(raw["limit_price"])
                if (side == "sell" and bar.high >= limit) or (side == "buy" and bar.low <= limit):
                    price = max(bar.open, limit) if side == "sell" else min(bar.open, limit)
            if price is not None:
                self._fill(raw, round(price, 2))
        self.last_price[bar.symbol] = bar.close

    def _fill(self, raw: dict[str, Any], price: float) -> None:
        qty = float(raw["qty"])
        raw.update(status="filled", filled_qty=str(int(qty)), filled_avg_price=f"{price:.2f}",
                   filled_at=_iso(self.clock.now()), updated_at=_iso(self.clock.now()))  # fmt: skip
        signed = qty if raw["side"] == "buy" else -qty
        position = self.positions.setdefault(raw["symbol"], FakePosition())
        new_qty = position.qty + signed
        if position.qty == 0 or (position.qty > 0) == (signed > 0):
            position.avg = (position.avg * abs(position.qty) + price * qty) / abs(new_qty)
        elif new_qty != 0 and (new_qty > 0) != (position.qty > 0):
            position.avg = price
        position.qty = new_qty
        self.cash -= signed * price
        self._emit("fill", raw, price=price, qty=qty)
        parent_id = raw.get("_parent")
        if parent_id is None:
            for leg_id in raw.get("_legs", []):
                leg = self.orders[leg_id]
                leg.update(
                    status="new", submitted_at=_iso(self.clock.now()), updated_at=_iso(self.clock.now())
                )
                self._emit("new", leg)
        else:
            for sibling_id in self.orders[parent_id].get("_legs", []):
                sibling = self.orders[sibling_id]
                if sibling_id != raw["id"] and sibling["status"] in ("new", "held", "accepted"):
                    self._cancel(sibling)

    def _cancel(self, raw: dict[str, Any]) -> None:
        raw.update(status="canceled", updated_at=_iso(self.clock.now()), canceled_at=_iso(self.clock.now()))
        self._emit("canceled", raw)

    def _emit(
        self, event: str, raw: dict[str, Any], *, price: float | None = None, qty: float | None = None
    ) -> None:
        data: dict[str, Any] = {
            "event": event,
            "timestamp": _iso(self.clock.now()),
            "order": self._public(raw),
        }
        if price is not None:
            data.update(price=f"{price:.2f}", qty=str(int(qty or 0)), execution_id=str(uuid.uuid4()))
        message = json.dumps({"stream": "trade_updates", "data": data})
        for socket in self.sockets:
            if socket.kind == "trading" and socket.ready and not socket.closed:
                socket.inbox.put_nowait(message)

    def _public(self, raw: dict[str, Any], *, nested: bool = True) -> dict[str, Any]:
        view = {k: v for k, v in raw.items() if not k.startswith("_")}
        legs = raw.get("_legs")
        view["legs"] = [self._public(self.orders[i]) for i in legs] if legs and nested else None
        return view

    def _new_order(self, body: dict[str, Any], *, parent: str | None = None) -> dict[str, Any]:
        now = _iso(self.clock.now())
        native = str(uuid.uuid4())
        raw: dict[str, Any] = {
            "id": native, "client_order_id": body.get("client_order_id") or str(uuid.uuid4()), "created_at": now,
            "updated_at": now, "submitted_at": now, "filled_at": None, "symbol": body["symbol"], "asset_class": "us_equity",
            "qty": str(body["qty"]), "filled_qty": "0", "filled_avg_price": None,
            "order_class": body.get("order_class", "simple") if parent is None else "bracket",
            "type": body["type"], "side": body["side"], "time_in_force": body.get("time_in_force", "day"),
            "limit_price": body.get("limit_price"), "stop_price": body.get("stop_price"),
            "status": "held" if parent is not None else "new", "extended_hours": False, "_parent": parent,
        }  # fmt: skip
        self.orders[native] = raw
        self.by_client[raw["client_order_id"]] = native
        return raw

    def _submit(self, body: dict[str, Any]) -> httpx.Response:
        self.order_requests.append(body)
        cid = body["client_order_id"]
        if cid in self.by_client:
            return httpx.Response(422, json={"code": 40010001, "message": "client_order_id must be unique"})
        symbol, side, qty = body["symbol"], body["side"], float(body["qty"])
        held = sum(
            float(o["qty"]) for o in self.orders.values()
            if o["symbol"] == symbol and o["side"] == side and o["status"] in ("new", "held", "accepted")
            and o["_parent"] is not None
        )  # fmt: skip
        position = self.positions.get(symbol, FakePosition()).qty
        closing = (side == "sell" and position > 0) or (side == "buy" and position < 0)
        if closing and held > 0 and qty > abs(position) - held:
            self.rejections.append(cid)
            return httpx.Response(
                403,
                json={
                    "code": 40310000,
                    "message": f"insufficient qty available for order (requested: {int(qty)}, available: 0)",
                },
            )
        if symbol not in self.last_price:
            return httpx.Response(422, json={"code": 42210000, "message": "no price for symbol"})
        raw = self._new_order(body)
        if body.get("order_class") == "bracket":
            exit_side = "sell" if side == "buy" else "buy"
            tp = self._new_order({"symbol": symbol, "qty": body["qty"], "side": exit_side, "type": "limit",
                                  "limit_price": body["take_profit"]["limit_price"]}, parent=raw["id"])  # fmt: skip
            sl = self._new_order({"symbol": symbol, "qty": body["qty"], "side": exit_side, "type": "stop",
                                  "stop_price": body["stop_loss"]["stop_price"]}, parent=raw["id"])  # fmt: skip
            raw["_legs"] = [tp["id"], sl["id"]]
        self._emit("new", raw)
        return httpx.Response(200, json=self._public(raw))

    # ------------------------------------------------------------------ REST

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get("APCA-API-KEY-ID") != "test-key":
            return httpx.Response(401, json={"message": "unauthorized"})
        path, params = request.url.path, request.url.params
        if request.url.host == DATA_HOST:
            if path == "/v2/stocks/bars":
                return self.market.handle(request)
            return httpx.Response(404, json={"message": "not found"})
        if path == "/v2/account":
            equity = self.cash + sum(p.qty * self.last_price.get(s, p.avg) for s, p in self.positions.items())
            return httpx.Response(200, json={
                "account_number": "PA1234TEST", "status": "ACTIVE", "currency": "USD", "cash": str(self.cash),
                "equity": str(equity), "last_equity": str(self.initial), "buying_power": str(2 * equity),
                "long_market_value": "0", "short_market_value": "0", "trading_blocked": False,
            })  # fmt: skip
        if path == "/v2/clock":
            return httpx.Response(200, json={"timestamp": _iso(self.clock.now()), "is_open": True})
        if path == "/v2/calendar":
            return self.market.handle(request)
        if path == "/v2/positions":
            return httpx.Response(200, json=[
                {"symbol": s, "qty": str(int(p.qty)), "side": "long" if p.qty > 0 else "short",
                 "avg_entry_price": f"{p.avg:.4f}", "current_price": str(self.last_price.get(s, p.avg))}
                for s, p in self.positions.items() if p.qty != 0
            ])  # fmt: skip
        if path.startswith("/v2/assets/"):
            symbol = path.rsplit("/", 1)[1]
            return httpx.Response(200, json={"symbol": symbol, "status": "active", "tradable": True, "shortable": True,
                                             "easy_to_borrow": True, "fractionable": True})  # fmt: skip
        if path == "/v2/orders" and request.method == "POST":
            return self._submit(json.loads(request.content))
        if path == "/v2/orders" and request.method == "GET":
            status = params.get("status", "open")
            parents = [o for o in self.orders.values() if o["_parent"] is None]
            open_states = ("new", "held", "accepted", "partially_filled")

            def is_open(o: dict[str, Any]) -> bool:
                return o["status"] in open_states or any(
                    self.orders[i]["status"] in open_states for i in o.get("_legs", [])
                )

            if status == "open":
                parents = [o for o in parents if is_open(o)]
            elif status == "closed":
                parents = [o for o in parents if not is_open(o)]
            return httpx.Response(200, json=[self._public(o) for o in parents])
        if path == "/v2/orders:by_client_order_id":
            native = self.by_client.get(params["client_order_id"])
            if native is None:
                return httpx.Response(404, json={"message": "order not found"})
            return httpx.Response(200, json=self._public(self.orders[native]))
        if path.startswith("/v2/orders/"):
            native = path.rsplit("/", 1)[1]
            raw = self.orders.get(native)
            if raw is None:
                return httpx.Response(404, json={"message": "order not found"})
            if request.method == "DELETE":
                if raw["status"] not in ("new", "held", "accepted", "partially_filled"):
                    return httpx.Response(422, json={"message": "order is not cancelable"})
                self._cancel(raw)
                for leg_id in raw.get("_legs", []):
                    if self.orders[leg_id]["status"] in ("new", "held", "accepted"):
                        self._cancel(self.orders[leg_id])
                parent = raw.get("_parent")
                if parent is not None:  # OCO: cancelling one leg cancels its sibling
                    for sibling_id in self.orders[parent].get("_legs", []):
                        if self.orders[sibling_id]["status"] in ("new", "held", "accepted"):
                            self._cancel(self.orders[sibling_id])
                return httpx.Response(204)
            return httpx.Response(200, json=self._public(raw))
        return httpx.Response(404, json={"message": f"not found: {path}"})
