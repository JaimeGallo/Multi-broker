"""AlpacaBrokerAdapter: Alpaca PAPER trading through its official REST API and `trade_updates` stream.

Safety: the adapter only talks to `paper-api.alpaca.markets`. Any other trading endpoint (or `paper: false`) is
refused when the adapter is built, so `capabilities.is_paper` is true by construction.

Errors follow the BrokerAdapter contract:
- connection refused / not sent         -> BrokerUnavailable (safe to retry with the same client_order_id);
- timeout or 5xx after the request left -> AmbiguousSubmission (the execution engine asks by client_order_id);
- "client_order_id must be unique"      -> DuplicateClientOrderId;
- any other 4xx on submission           -> OrderRejected with Alpaca's message.

Order events come from the `trade_updates` WebSocket. After a reconnection the adapter re-reads every order it
knows that is still open and emits a synthetic event for each change it missed, so no fill is ever lost.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import httpx

from packages.brokers.alpaca.mapping import (
    EVENT_TYPE,
    leg_client_order_id,
    leg_intent,
    order_from_alpaca,
    parse_time,
    position_from_alpaca,
    price_for_alpaca,
    split_leg_id,
)
from packages.brokers.base import BrokerAdapter, OrderQueryStatus
from packages.common.clock import Clock
from packages.common.config import PAPER_TRADING_STREAM_URL, PAPER_TRADING_URL, AlpacaSection
from packages.common.costs import CostModel
from packages.common.entities import (
    AccountSnapshot,
    BrokerCapabilities,
    BrokerHealth,
    InstrumentInfo,
    Order,
    OrderEvent,
    OrderReplace,
    OrderRequest,
    Position,
)
from packages.common.enums import AssetClass, ConnectionStatus, OrderClass, OrderEventType, OrderStatus
from packages.common.errors import (
    AmbiguousSubmission,
    BrokerUnavailable,
    DuplicateClientOrderId,
    OrderNotFound,
    OrderRejected,
    SafetyError,
)
from packages.common.websocket import Connector, WebSocketLike, backoff_seconds, decode, websockets_connector
from packages.market_data.alpaca_history import CalendarDay

log = logging.getLogger(__name__)

PAPER_HOST = urlparse(PAPER_TRADING_URL).hostname
GET_RETRIES = 3
CLOCK_REFRESH_SECONDS = 30.0
STREAM_READY_TIMEOUT = 20.0
SYNC_EVENT_TYPE = {
    OrderStatus.FILLED: OrderEventType.FILL,
    OrderStatus.PARTIALLY_FILLED: OrderEventType.PARTIAL_FILL,
    OrderStatus.CANCELLED: OrderEventType.CANCELLED,
    OrderStatus.EXPIRED: OrderEventType.EXPIRED,
    OrderStatus.REJECTED: OrderEventType.REJECTED,
    OrderStatus.CANCEL_REQUESTED: OrderEventType.CANCEL_REQUESTED,
}


def check_paper_endpoints(config: AlpacaSection) -> None:
    """Paper only, enforced on the endpoint itself (the paper host only serves paper accounts)."""
    if not config.paper:
        raise SafetyError("broker.alpaca.paper must be true: live trading is not available in this build")
    for url in (config.trading_url, config.trading_stream_url):
        if urlparse(url).hostname != PAPER_HOST:
            raise SafetyError(
                f"Alpaca endpoint {url!r} is not the paper endpoint ({PAPER_TRADING_URL}, {PAPER_TRADING_STREAM_URL})"
            )


class AlpacaBrokerAdapter(BrokerAdapter):
    name = "alpaca"

    def __init__(
        self,
        config: AlpacaSection,
        cost_model: CostModel,
        clock: Clock,
        *,
        key: str,
        secret: str,
        transport: httpx.AsyncBaseTransport | None = None,
        connector: Connector = websockets_connector,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        check_paper_endpoints(config)
        self._cfg = config
        self._costs = cost_model
        self._clock = clock
        self._key = key
        self._secret = secret
        self._transport = transport
        self._connector = connector
        self._sleep = sleep
        self._client: httpx.AsyncClient | None = None
        self._connected = False
        self._account_ok = False
        self._stream_ready = asyncio.Event()
        self._stream_task: asyncio.Task[None] | None = None
        self._events: asyncio.Queue[OrderEvent | None] = asyncio.Queue()
        self._native: dict[str, str] = {}  # canonical client_order_id -> Alpaca order id
        self._legs: dict[str, tuple[str, str]] = {}  # Alpaca leg id -> (canonical leg id, parent id)
        self._known: dict[str, Order] = {}  # last view of every order we track (sync after reconnect)
        self._signals: dict[str, str | None] = {}  # client_order_id -> signal id (from our requests)
        self._instruments: dict[tuple[str, date], InstrumentInfo] = {}
        self._server_time: tuple[timedelta, float] | None = None  # (server - local offset, when)
        self._last_event_at: datetime | None = None
        self._latency_ms: float | None = None
        self._detail = ""
        self._sync_counter = 0

    # ------------------------------------------------------------------ connectivity

    @property
    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            broker=self.name,
            is_paper=True,  # guaranteed by check_paper_endpoints
            asset_classes=(AssetClass.US_EQUITY, AssetClass.ETF),
            supports_short=True,
            supports_fractional=False,  # brackets do not accept fractional quantities
            supports_bracket=True,
            supports_replace=False,
            supports_extended_hours=False,  # brackets are regular-session only
            max_client_order_id_length=64,
        )

    async def connect(self) -> None:
        if self._connected:
            return
        self._client = httpx.AsyncClient(
            base_url=self._cfg.trading_url,
            headers={
                "APCA-API-KEY-ID": self._key,
                "APCA-API-SECRET-KEY": self._secret,
                "Accept": "application/json",
            },
            transport=self._transport,
            timeout=self._cfg.request_timeout_seconds,
        )
        account = await self._get("/v2/account")
        number = str(account.get("account_number", ""))
        if not number.upper().startswith("PA"):
            log.warning("Alpaca paper account number does not start with PA", extra={"account": number[-4:]})
        if account.get("trading_blocked") or account.get("account_blocked"):
            raise BrokerUnavailable("the Alpaca paper account is blocked for trading")
        self._account_ok = True
        self._connected = True
        await self._refresh_clock()
        self._stream_task = asyncio.create_task(self._run_stream(), name="alpaca-trade-updates")
        try:
            await asyncio.wait_for(self._stream_ready.wait(), STREAM_READY_TIMEOUT)
        except TimeoutError as exc:
            await self.disconnect()
            raise BrokerUnavailable(f"trade_updates stream not ready: {self._detail or 'timeout'}") from exc

    async def disconnect(self) -> None:
        self._connected = False
        if self._stream_task is not None:
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._stream_task
            self._stream_task = None
        self._stream_ready.clear()
        self._events.put_nowait(None)  # wakes stream_order_events consumers
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def health(self) -> BrokerHealth:
        if self._connected and (
            self._server_time is None or time.monotonic() - self._server_time[1] > CLOCK_REFRESH_SECONDS
        ):
            with contextlib.suppress(BrokerUnavailable):
                await self._refresh_clock()
        server_time = None
        if self._server_time is not None:
            offset, _ = self._server_time
            server_time = self._clock.now() + offset
        stream = self._stream_ready.is_set()
        status = ConnectionStatus.CONNECTED if self._connected and stream else ConnectionStatus.DEGRADED
        if not self._connected:
            status = ConnectionStatus.DISCONNECTED
        return BrokerHealth(
            broker=self.name,
            status=status,
            connected=self._connected,
            order_stream_connected=self._connected and stream,
            account_available=self._connected and self._account_ok,
            latency_ms=self._latency_ms,
            last_event_at=self._last_event_at,
            server_time=server_time,
            detail=self._detail,
        )

    async def calendar(self, start: date, end: date) -> list[CalendarDay]:
        payload = await self._get("/v2/calendar", {"start": start.isoformat(), "end": end.isoformat()})
        return [CalendarDay(date.fromisoformat(d["date"]), d["open"], d["close"]) for d in payload]

    async def _refresh_clock(self) -> None:
        sent = self._clock.now()
        payload = await self._get("/v2/clock")
        received = self._clock.now()
        stamp = parse_time(payload.get("timestamp"))
        if stamp is not None:
            # server minus local clock, local time taken halfway through the request (half the round trip)
            local = sent + (received - sent) / 2
            self._server_time = (stamp - local, time.monotonic())

    # ------------------------------------------------------------------ account

    async def get_account(self) -> AccountSnapshot:
        try:
            raw = await self._get("/v2/account")
        except BrokerUnavailable:
            self._account_ok = False
            raise
        self._account_ok = True
        number = str(raw.get("account_number", ""))
        return AccountSnapshot(
            broker=self.name,
            account_ref="***" + number[-4:],
            is_paper=True,
            currency=raw.get("currency", "USD"),
            cash=float(raw["cash"]),
            equity=float(raw["equity"]),
            buying_power=float(raw["buying_power"]),
            last_equity=float(raw.get("last_equity") or raw["equity"]),
            long_market_value=float(raw.get("long_market_value") or 0.0),
            short_market_value=float(raw.get("short_market_value") or 0.0),
            status=str(raw.get("status", "ACTIVE")),
            timestamp=self._clock.now(),
        )

    async def get_positions(self) -> list[Position]:
        payload = await self._get("/v2/positions")
        return [position_from_alpaca(raw, broker=self.name) for raw in payload]

    async def get_instrument(self, symbol: str) -> InstrumentInfo:
        key = (symbol, self._clock.now().date())
        cached = self._instruments.get(key)
        if cached is not None:
            return cached
        raw = await self._get(f"/v2/assets/{symbol}")
        info = InstrumentInfo(
            symbol=symbol,
            tradable=bool(raw.get("tradable")) and raw.get("status", "active") == "active",
            shortable=bool(raw.get("shortable")),
            easy_to_borrow=bool(raw.get("easy_to_borrow")),
            fractionable=bool(raw.get("fractionable")),
        )
        self._instruments[key] = info
        return info

    # ------------------------------------------------------------------ orders

    async def get_orders(self, status: OrderQueryStatus = OrderQueryStatus.OPEN) -> list[Order]:
        payload = await self._get(
            "/v2/orders", {"status": status.value, "nested": "true", "limit": 500, "direction": "asc"}
        )
        return [self._normalize(raw) for raw in payload]

    async def get_order(self, client_order_id: str) -> Order | None:
        leg = split_leg_id(client_order_id)
        parent_id = leg[0] if leg is not None else client_order_id
        raw = await self._get_optional("/v2/orders:by_client_order_id", {"client_order_id": parent_id})
        if raw is None:
            return None
        if raw.get("order_class") == "bracket" and not raw.get("legs"):
            raw = await self._get(f"/v2/orders/{raw['id']}", {"nested": "true"})
        parent = self._normalize(raw)
        if leg is None:
            return parent
        return next(
            (candidate for candidate in parent.legs if candidate.client_order_id == client_order_id), None
        )

    async def submit_order(self, order: OrderRequest) -> Order:
        request = order
        if request.extended_hours:
            raise OrderRejected("extended hours not supported", request.client_order_id)
        if request.quantity != int(request.quantity):
            raise OrderRejected("fractional quantities not supported", request.client_order_id)
        body: dict[str, Any] = {
            "symbol": request.symbol,
            "qty": str(int(request.quantity)),
            "side": request.side.value,
            "type": request.order_type.value,
            "time_in_force": request.time_in_force.value,
            "client_order_id": request.client_order_id,
        }
        if request.limit_price is not None:
            body["limit_price"] = price_for_alpaca(request.limit_price)
        if request.stop_price is not None:
            body["stop_price"] = price_for_alpaca(request.stop_price)
        if request.order_class is OrderClass.BRACKET:
            assert request.take_profit_price is not None and request.stop_loss_price is not None
            body["order_class"] = "bracket"
            body["take_profit"] = {"limit_price": price_for_alpaca(request.take_profit_price)}
            body["stop_loss"] = {"stop_price": price_for_alpaca(request.stop_loss_price)}
        self._signals[request.client_order_id] = request.signal_id
        raw = await self._submit(body, request.client_order_id)
        return self._normalize(raw)

    async def cancel_order(self, client_order_id: str) -> None:
        native = self._native.get(client_order_id)
        if native is None:
            current = await self.get_order(client_order_id)
            if current is None:
                raise OrderNotFound(client_order_id)
            native = self._native.get(client_order_id)
            if current.is_terminal or native is None:
                return
        response = await self._request("DELETE", f"/v2/orders/{native}")
        if response.status_code == 404:
            raise OrderNotFound(client_order_id)
        if response.status_code == 422:
            return  # no longer cancelable (filled or already closed): the stream reports the final state
        self._raise_for_status(response)

    async def replace_order(self, client_order_id: str, changes: OrderReplace) -> Order:
        raise OrderRejected(
            "order replacement is not used with Alpaca (it changes the order id)", client_order_id
        )

    async def stream_order_events(self) -> AsyncIterator[OrderEvent]:
        while self._connected or not self._events.empty():
            event = await self._events.get()
            if event is None:
                if not self._connected:
                    return
                continue
            yield event

    # ------------------------------------------------------------------ normalization

    def _normalize(self, raw: Mapping[str, Any]) -> Order:
        """Alpaca order (with nested legs) -> Order whose legs carry canonical ids; learns the id maps."""
        now = self._clock.now()
        data = dict(raw)
        native_id = data.get("id")
        mapped = self._legs.get(native_id) if native_id else None
        if mapped is not None:
            canonical, parent_id = mapped
            order = order_from_alpaca(
                data, broker=self.name, client_order_id=canonical, intent=leg_intent(data),
                parent_client_order_id=parent_id, signal_id=self._signals.get(parent_id), received_at=now,
            )  # fmt: skip
            self._known[canonical] = order
            return order
        cid = data["client_order_id"]
        if native_id:
            self._native[cid] = native_id
        order = order_from_alpaca(data, broker=self.name, signal_id=self._signals.get(cid), received_at=now)
        legs: list[Order] = []
        for leg_raw in data.get("legs") or []:
            intent = leg_intent(leg_raw)
            canonical = leg_client_order_id(cid, intent)
            self._legs[leg_raw["id"]] = (canonical, cid)
            self._native[canonical] = leg_raw["id"]
            leg = order_from_alpaca(
                leg_raw, broker=self.name, client_order_id=canonical, intent=intent, parent_client_order_id=cid,
                signal_id=self._signals.get(cid), received_at=now,
            )  # fmt: skip
            legs.append(leg)
            self._known[canonical] = leg
        order.legs = legs
        order.leg_client_order_ids = [leg.client_order_id for leg in legs]
        if not order.legs and order.order_class is OrderClass.BRACKET and cid in self._known:
            order.leg_client_order_ids = list(self._known[cid].leg_client_order_ids)
        known = order.model_copy(deep=True)
        known.legs = []
        self._known[cid] = known
        return order

    def _event(self, message: Mapping[str, Any]) -> OrderEvent | None:
        raw_order = message.get("order")
        if not isinstance(raw_order, Mapping):
            return None
        kind = str(message.get("event", ""))
        order = self._normalize(raw_order)
        timestamp = parse_time(message.get("timestamp")) or self._clock.now()
        fee = 0.0
        if kind in ("fill", "partial_fill") and message.get("qty") and message.get("price"):
            fee = self._costs.fees(order.side, float(message["qty"]), float(message["price"]))
        event_id = message.get("execution_id") or f"{raw_order.get('id')}:{kind}:{message.get('timestamp')}"
        self._last_event_at = timestamp
        return OrderEvent(
            event_id=f"alpaca-{event_id}",
            broker=self.name,
            event_type=EVENT_TYPE.get(kind, OrderEventType.ACKNOWLEDGED),
            timestamp=timestamp,
            order=order,
            fee=fee,
            reason=kind if kind not in ("fill", "partial_fill", "new") else None,
            received_at=self._clock.now(),
            raw={
                k: message[k]
                for k in ("event", "price", "qty", "position_qty", "execution_id")
                if k in message
            },
        )

    # ------------------------------------------------------------------ trade_updates stream

    async def _run_stream(self) -> None:
        attempt = 0
        first = True
        while self._connected:
            socket: WebSocketLike | None = None
            try:
                socket = await self._connector(self._cfg.trading_stream_url)
                # Same handshake as Alpaca's official SDK (alpaca-py TradingStream).
                await socket.send(
                    json.dumps(
                        {"action": "authenticate", "data": {"key_id": self._key, "secret_key": self._secret}}
                    )
                )
                reply = decode(await socket.recv())
                if (reply.get("data") or {}).get("status") != "authorized":
                    self._detail = f"trade_updates authorization failed: {reply.get('data')}"
                    raise SafetyError(self._detail)
                await socket.send(json.dumps({"action": "listen", "data": {"streams": ["trade_updates"]}}))
                if not first:
                    await self._sync_after_reconnect()
                first = False
                attempt = 0
                self._detail = ""
                self._stream_ready.set()
                while True:
                    message = decode(await socket.recv())
                    if message.get("stream") != "trade_updates":
                        continue  # "listening" confirmation and other control messages
                    event = self._event(message.get("data") or {})
                    if event is not None:
                        self._events.put_nowait(event)
            except asyncio.CancelledError:
                raise
            except SafetyError:
                self._stream_ready.clear()
                await self._sleep(30.0)  # wrong keys: do not hammer the server
            except Exception as exc:
                self._stream_ready.clear()
                self._detail = f"trade_updates disconnected: {exc}"
                log.warning(
                    "trade_updates stream lost; reconnecting", extra={"error": str(exc), "attempt": attempt}
                )
                await self._sleep(backoff_seconds(attempt))
                attempt += 1
            finally:
                if socket is not None:
                    with contextlib.suppress(Exception):
                        await socket.close()

    async def _sync_after_reconnect(self) -> None:
        """Re-read every order still open in our view and emit what changed while the stream was down."""
        before = dict(self._known)
        for cid, previous in before.items():
            if split_leg_id(cid) is not None:
                continue
            legs = [before.get(leg_id) for leg_id in previous.leg_client_order_ids]
            if previous.is_terminal and all(leg is None or leg.is_terminal for leg in legs):
                continue  # nothing left to learn about this order
            try:
                current = await self.get_order(cid)
            except Exception as exc:
                log.warning("order sync failed", extra={"client_order_id": cid, "error": str(exc)})
                continue
            if current is None:
                continue
            for view in (current, *current.legs):
                old = before.get(view.client_order_id)
                if old is not None and (view.status, view.filled_quantity) == (
                    old.status,
                    old.filled_quantity,
                ):
                    continue
                snapshot = view.model_copy(deep=True)
                snapshot.legs = []
                self._events.put_nowait(
                    OrderEvent(
                        event_id=f"alpaca-sync-{view.client_order_id}-{view.status.value}-{view.filled_quantity:g}",
                        broker=self.name,
                        event_type=SYNC_EVENT_TYPE.get(view.status, OrderEventType.ACKNOWLEDGED),
                        timestamp=view.updated_at,
                        order=snapshot,
                        reason="sync_after_reconnect",
                        received_at=self._clock.now(),
                    )
                )

    # ------------------------------------------------------------------ HTTP

    async def _request(
        self, method: str, path: str, *, params: Mapping[str, Any] | None = None, body: Any = None
    ) -> httpx.Response:
        if self._client is None:
            raise BrokerUnavailable("Alpaca adapter is not connected")
        started = time.monotonic()
        response = await self._client.request(method, path, params=dict(params or {}), json=body)
        self._latency_ms = (time.monotonic() - started) * 1000.0
        return response

    async def _get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        response = await self._get_response(path, params)
        self._raise_for_status(response)
        return response.json()

    async def _get_optional(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        response = await self._get_response(path, params)
        if response.status_code == 404:
            return None
        self._raise_for_status(response)
        return response.json()

    async def _get_response(self, path: str, params: Mapping[str, Any] | None) -> httpx.Response:
        for attempt in range(GET_RETRIES + 1):
            try:
                response = await self._request("GET", path, params=params)
            except httpx.TransportError as exc:
                if attempt == GET_RETRIES:
                    raise BrokerUnavailable(f"Alpaca unreachable: {exc}") from exc
                await self._sleep(backoff_seconds(attempt, base=0.5, cap=5.0))
                continue
            if response.status_code in (429, 500, 502, 503, 504) and attempt < GET_RETRIES:
                await self._sleep(backoff_seconds(attempt, base=0.5, cap=5.0))
                continue
            return response
        raise BrokerUnavailable("Alpaca request failed after retries")  # pragma: no cover

    async def _submit(self, body: dict[str, Any], client_order_id: str) -> dict[str, Any]:
        try:
            response = await self._request("POST", "/v2/orders", body=body)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            raise BrokerUnavailable(f"order not sent: {exc}") from exc
        except httpx.TransportError as exc:
            raise AmbiguousSubmission(f"no answer after sending the order: {exc}") from exc
        if response.status_code in (200, 201):
            result: dict[str, Any] = response.json()
            return result
        message = _message(response)
        if response.status_code >= 500:
            raise AmbiguousSubmission(f"Alpaca {response.status_code}: {message}")
        if response.status_code == 429:
            raise BrokerUnavailable(f"rate limited: {message}")
        if response.status_code == 401:
            raise BrokerUnavailable(f"Alpaca refused the keys: {message}")
        if "client_order_id must be unique" in message.lower():
            raise DuplicateClientOrderId(client_order_id)
        raise OrderRejected(message, client_order_id)

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code < 400:
            return
        message = _message(response)
        if response.status_code in (401, 403):
            raise BrokerUnavailable(f"Alpaca refused the request ({response.status_code}): {message}")
        raise BrokerUnavailable(f"Alpaca error {response.status_code}: {message}")


def _message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text[:300]
    if isinstance(payload, Mapping):
        return str(payload.get("message") or payload)[:300]
    return str(payload)[:300]
