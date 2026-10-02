"""BrokerExecutionEngine: idempotent submission and broker event handling.

Idempotency protocol (docs/EXECUTION.md §4):
1. if the client_order_id is already stored, return the stored order (never resend);
2. persist CREATED, then SUBMITTED (write-ahead) before calling the broker;
3. on an ambiguous outcome, ask the broker by client_order_id before retrying with the SAME id;
4. every fill comes from the broker's cumulative quantity, whatever the path (event, submit response,
   reconciliation), and is reported exactly once through the fill listener.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from packages.brokers.base import BrokerAdapter, ExecutionRequirements
from packages.brokers.router import BrokerRouter
from packages.common.clock import Clock
from packages.common.config import ExecutionSection
from packages.common.entities import Fill, Order, OrderEvent, OrderReplace, OrderRequest
from packages.common.enums import OrderClass, OrderEventType, OrderIntent, OrderStatus, Side
from packages.common.errors import (
    AmbiguousSubmission,
    BrokerError,
    BrokerUnavailable,
    DuplicateClientOrderId,
    OrderNotFound,
    OrderRejected,
)
from packages.common.events import EventBus, Topics
from packages.execution.base import ExecutionEngine
from packages.execution.order_store import OrderStore
from packages.execution.state_machine import AppliedUpdate, apply_event, apply_snapshot

log = logging.getLogger(__name__)

FillListener = Callable[[Fill], Awaitable[None]]
AnomalyListener = Callable[[str, str, dict[str, Any]], Awaitable[None]]


def _state_key(order: Order) -> tuple[object, ...]:
    return (
        order.status,
        order.filled_quantity,
        order.broker_order_id,
        order.quantity,
        order.limit_price,
        order.stop_price,
        tuple(order.leg_client_order_ids),
        order.reject_reason,
    )


class BrokerExecutionEngine(ExecutionEngine):
    def __init__(
        self,
        *,
        router: BrokerRouter,
        store: OrderStore,
        clock: Clock,
        bus: EventBus,
        config: ExecutionSection,
        strategy: str,
        seen_event_capacity: int = 50_000,
    ) -> None:
        self._router = router
        self._store = store
        self._clock = clock
        self._bus = bus
        self._cfg = config
        self._strategy = strategy
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._seen_capacity = seen_event_capacity
        self._fill_listener: FillListener | None = None
        self._anomaly_listener: AnomalyListener | None = None
        self.broker_submissions = 0

    def set_fill_listener(self, listener: FillListener | None) -> None:
        self._fill_listener = listener

    def set_anomaly_listener(self, listener: AnomalyListener | None) -> None:
        self._anomaly_listener = listener

    # ------------------------------------------------------------------ ExecutionEngine

    async def submit(self, order: OrderRequest) -> Order:
        request = order
        existing = await self._store.get(request.client_order_id)
        if existing is not None:
            log.info(
                "idempotent submit: order already exists",
                extra={"client_order_id": request.client_order_id, "status": existing.status.value},
            )
            return existing
        requirements = ExecutionRequirements(
            bracket=request.order_class is OrderClass.BRACKET,
            short=request.intent is OrderIntent.ENTRY and request.side is Side.SELL,
        )
        broker = await self._router.select_broker(request.symbol, request.asset_class, self._strategy, requirements)
        now = self._clock.now()
        local = Order.from_request(request, broker=broker.name, at=now)
        await self._save(local)  # write-ahead: durable before any broker call
        local.status = OrderStatus.SUBMITTED
        local.submitted_at = now
        await self._save(local)

        remote: Order | None = None
        ambiguous = False
        for attempt in range(1, self._cfg.max_submit_attempts + 1):
            try:
                remote = await broker.submit_order(request)
                self.broker_submissions += 1
                break
            except OrderRejected as exc:
                return await self._mark(local, OrderStatus.REJECTED, exc.reason)
            except DuplicateClientOrderId:
                remote = await self._lookup(broker, request.client_order_id)
                if remote is None:
                    await self._anomaly("duplicate_without_order", request.client_order_id, {})
                    return await self._mark(local, OrderStatus.ERROR, "duplicate_client_order_id_without_order")
                break
            except AmbiguousSubmission as exc:
                ambiguous = True
                remote = await self._lookup(broker, request.client_order_id)
                if remote is not None:
                    break
                log.warning(
                    "ambiguous submission; retrying with the same client_order_id",
                    extra={"client_order_id": request.client_order_id, "attempt": attempt, "error": str(exc)},
                )
            except BrokerUnavailable as exc:
                if ambiguous:
                    return await self._mark(local, OrderStatus.ERROR, f"outcome_unknown: {exc}")
                return await self._mark(local, OrderStatus.REJECTED, f"broker_unavailable: {exc}")
        if remote is None:
            await self._anomaly(
                "submission_outcome_unknown", request.client_order_id, {"attempts": self._cfg.max_submit_attempts}
            )
            return await self._mark(local, OrderStatus.ERROR, "submission_outcome_unknown")

        merged = await self._merge(local, remote, reason="submit")
        for leg in remote.legs:
            await self.adopt_leg(leg, merged)
        if remote.legs:
            # The returned view carries the legs so callers can track them right away (they are stored separately).
            merged = merged.model_copy(update={"legs": [leg.model_copy(deep=True) for leg in remote.legs]})
        return merged

    async def cancel(self, client_order_id: str) -> None:
        order = await self._store.get(client_order_id)
        if order is None or order.is_terminal:
            return
        if order.status is OrderStatus.CREATED:
            await self._mark(order, OrderStatus.CANCELLED, "cancelled_before_submission")
            return
        broker = self._router.get(order.broker)
        if order.status is not OrderStatus.CANCEL_REQUESTED:
            order.status = OrderStatus.CANCEL_REQUESTED
            order.updated_at = max(order.updated_at, self._clock.now())
            await self._save(order)
        try:
            await broker.cancel_order(client_order_id)
        except OrderNotFound:
            await self.reconcile_order(client_order_id)
        except BrokerError as exc:
            log.warning("cancel failed; it will be retried", extra={"client_order_id": client_order_id, "error": str(exc)})

    async def replace(self, client_order_id: str, changes: OrderReplace) -> Order:
        order = await self._store.get(client_order_id)
        if order is None:
            raise OrderNotFound(client_order_id)
        remote = await self._router.get(order.broker).replace_order(client_order_id, changes)
        return await self._merge(order, remote, reason="replace")

    # ------------------------------------------------------------------ broker events & reconciliation

    async def handle_order_event(self, event: OrderEvent) -> Order | None:
        if self._already_seen(event.event_id):
            return None
        await self._bus.publish(Topics.ORDER_EVENT, event)
        order = await self._store.get(event.client_order_id)
        if order is None:
            parent_id = event.order.parent_client_order_id
            parent = await self._store.get(parent_id) if parent_id else None
            if parent is None:
                await self._anomaly(
                    "unknown_order_event",
                    f"event for unknown order {event.client_order_id}",
                    {"event_id": event.event_id, "client_order_id": event.client_order_id},
                )
                return None
            await self.adopt_leg(event.order, parent)
            order = await self._store.get(event.client_order_id)
            if order is None:
                return None
        return await self._commit(order, apply_event(order, event), from_event=True)

    async def reconcile_order(self, client_order_id: str) -> Order | None:
        """Refresh one local order from the broker's view. Returns the local order (possibly unchanged)."""
        order = await self._store.get(client_order_id)
        if order is None:
            return None
        remote = await self._lookup(self._router.get(order.broker), client_order_id)
        if remote is None:
            return order
        return await self._merge(order, remote, reason="reconciliation")

    async def adopt_remote(self, order: Order, remote: Order) -> Order:
        return await self._merge(order, remote, reason="reconciliation")

    async def adopt_leg(self, leg: Order, parent: Order) -> None:
        """Record a broker-created leg (bracket TP/SL) of one of our orders, then apply its current state."""
        if await self._store.get(leg.client_order_id) is not None:
            return
        target = leg.model_copy(deep=True)
        target.legs = []
        target.parent_client_order_id = target.parent_client_order_id or parent.client_order_id
        target.signal_id = target.signal_id or parent.signal_id
        baseline = target.model_copy(deep=True)
        baseline.status = OrderStatus.SUBMITTED
        baseline.filled_quantity = 0.0
        baseline.average_fill_price = None
        await self._save(baseline)
        if target.status is not OrderStatus.SUBMITTED or target.filled_quantity > 0:
            await self._commit(
                baseline,
                apply_snapshot(baseline, target, timestamp=target.updated_at, reason="adopted_leg"),
                from_event=False,
                reason="adopted_leg",
            )

    async def mark_local(self, order: Order, status: OrderStatus, reason: str) -> Order:
        return await self._mark(order, status, reason)

    async def check_timeouts(self, now: datetime) -> None:
        for order in await self._store.list_open():
            if order.status is not OrderStatus.SUBMITTED or order.submitted_at is None:
                continue
            if (now - order.submitted_at).total_seconds() < self._cfg.ack_timeout_seconds:
                continue
            refreshed = await self.reconcile_order(order.client_order_id)
            if refreshed is not None and refreshed.status is OrderStatus.SUBMITTED:
                await self._mark(refreshed, OrderStatus.ERROR, "ack_timeout")
                await self._anomaly("ack_timeout", refreshed.client_order_id, {})

    # ------------------------------------------------------------------ internals

    async def _merge(self, order: Order, remote: Order, *, reason: str) -> Order:
        update = apply_snapshot(order, remote, timestamp=remote.updated_at, reason=reason)
        return await self._commit(order, update, from_event=False, reason=reason)

    async def _commit(
        self, order: Order, update: AppliedUpdate, *, from_event: bool, reason: str | None = None
    ) -> Order:
        if update.anomaly is not None:
            await self._anomaly(
                "order_update_anomaly", update.anomaly, {"client_order_id": order.client_order_id}
            )
        if not update.applied:
            return order
        if update.fill is None and _state_key(update.order) == _state_key(order):
            return order
        await self._save(update.order)
        fill = update.fill
        if fill is not None:
            if not from_event:
                # Fill observed outside the event stream: keep the audit trail complete.
                await self._bus.publish(
                    Topics.ORDER_EVENT,
                    OrderEvent(
                        event_id=f"sync-{fill.fill_id}",
                        broker=update.order.broker,
                        event_type=OrderEventType.FILL
                        if update.order.status is OrderStatus.FILLED
                        else OrderEventType.PARTIAL_FILL,
                        timestamp=fill.timestamp,
                        order=update.order.snapshot(),
                        fill_quantity=fill.quantity,
                        fill_price=fill.price,
                        fee=fill.fee,
                        reason=reason,
                    ),
                )
            if self._fill_listener is not None:
                await self._fill_listener(fill)
        return update.order

    async def _mark(self, order: Order, status: OrderStatus, reason: str) -> Order:
        order.status = status
        order.reject_reason = reason
        order.updated_at = max(order.updated_at, self._clock.now())
        await self._save(order)
        return order

    async def _save(self, order: Order) -> None:
        await self._store.save(order)
        await self._bus.publish(Topics.ORDER, order.snapshot())

    async def _lookup(self, broker: BrokerAdapter, client_order_id: str) -> Order | None:
        try:
            return await broker.get_order(client_order_id)
        except BrokerError as exc:
            log.warning("order lookup failed", extra={"client_order_id": client_order_id, "error": str(exc)})
            return None

    def _already_seen(self, event_id: str) -> bool:
        if event_id in self._seen:
            return True
        self._seen[event_id] = None
        if len(self._seen) > self._seen_capacity:
            self._seen.popitem(last=False)
        return False

    async def _anomaly(self, kind: str, detail: str, details: dict[str, Any]) -> None:
        log.warning("execution anomaly", extra={"kind": kind, "detail": detail})
        if self._anomaly_listener is not None:
            await self._anomaly_listener(kind, detail, details)
