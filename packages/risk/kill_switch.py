"""Kill switch and manual trading controls.

The kill switch blocks new entries immediately, survives restarts (persisted by the `on_change` callback) and
can only be reset by an identified human. Pausing is a softer, manual, freely reversible control.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from packages.common.clock import Clock
from packages.common.errors import SafetyError


class KillSwitchReason(StrEnum):
    DAILY_LOSS = "DAILY_LOSS"
    MAX_DRAWDOWN = "MAX_DRAWDOWN"
    STALE_DATA = "STALE_DATA"
    BROKER_DISCONNECTED = "BROKER_DISCONNECTED"
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    ABNORMAL_SLIPPAGE = "ABNORMAL_SLIPPAGE"
    ABNORMAL_LATENCY = "ABNORMAL_LATENCY"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"
    RISK_ENGINE_UNAVAILABLE = "RISK_ENGINE_UNAVAILABLE"
    UNEXPECTED_POSITION = "UNEXPECTED_POSITION"
    UNEXPECTED_ORDER = "UNEXPECTED_ORDER"
    MANUAL = "MANUAL"


class KillSwitchState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    engaged: bool = False
    reason: KillSwitchReason | None = None
    detail: str = ""
    engaged_at: datetime | None = None
    engaged_by: str | None = None


class TradingControlState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    paused: bool = False
    changed_at: datetime | None = None
    changed_by: str | None = None
    note: str = ""


KillSwitchListener = Callable[[KillSwitchState, str], Awaitable[None]]
ControlListener = Callable[[TradingControlState], Awaitable[None]]

SYSTEM_ACTOR = "system"


class KillSwitch:
    def __init__(
        self,
        clock: Clock,
        *,
        state: KillSwitchState | None = None,
        on_change: KillSwitchListener | None = None,
    ) -> None:
        self._clock = clock
        self._state = state or KillSwitchState()
        self._on_change = on_change

    @property
    def state(self) -> KillSwitchState:
        return self._state

    @property
    def engaged(self) -> bool:
        return self._state.engaged

    def set_listener(self, on_change: KillSwitchListener | None) -> None:
        self._on_change = on_change

    async def engage(self, reason: KillSwitchReason, detail: str = "", by: str = SYSTEM_ACTOR) -> bool:
        """Engage the switch. Returns False if it was already engaged (the first reason is kept)."""
        if self._state.engaged:
            return False
        self._state = KillSwitchState(
            engaged=True, reason=reason, detail=detail, engaged_at=self._clock.now(), engaged_by=by
        )
        if self._on_change is not None:
            await self._on_change(self._state, "KILL_SWITCH_ENGAGED")
        return True

    async def reset(self, *, by: str, note: str) -> None:
        """Manual reset only: requires an identified human and a reason."""
        if not by or by.strip().lower() == SYSTEM_ACTOR:
            raise SafetyError("the kill switch can only be reset manually by an identified person")
        if not note.strip():
            raise SafetyError("a reset reason is required")
        previous = self._state
        self._state = KillSwitchState()
        if self._on_change is not None and previous.engaged:
            await self._on_change(
                KillSwitchState(engaged=False, detail=f"reset by {by}: {note}", engaged_by=by),
                "KILL_SWITCH_RESET",
            )


class TradingControls:
    def __init__(
        self,
        clock: Clock,
        *,
        state: TradingControlState | None = None,
        on_change: ControlListener | None = None,
    ) -> None:
        self._clock = clock
        self._state = state or TradingControlState()
        self._on_change = on_change

    @property
    def paused(self) -> bool:
        return self._state.paused

    @property
    def state(self) -> TradingControlState:
        return self._state

    def set_listener(self, on_change: ControlListener | None) -> None:
        self._on_change = on_change

    async def pause(self, *, by: str, note: str = "") -> None:
        await self._set(True, by, note)

    async def resume(self, *, by: str, note: str = "") -> None:
        await self._set(False, by, note)

    async def _set(self, paused: bool, by: str, note: str) -> None:
        self._state = TradingControlState(
            paused=paused, changed_at=self._clock.now(), changed_by=by, note=note
        )
        if self._on_change is not None:
            await self._on_change(self._state)
