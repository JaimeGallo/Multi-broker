"""Risk Engine: the final, independent authority over every entry.

It does not import anything from `packages.jev`: it receives an already-formed Signal and a RiskContext built
from the broker (account, instrument), the local portfolio and the system state. Deterministic, no I/O.
Every decision records ALL checks (passed and failed) with value and limit, for audit.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from packages.common.calendar import Session
from packages.common.config import RiskSection
from packages.common.entities import (
    AccountSnapshot,
    BrokerCapabilities,
    InstrumentInfo,
    RiskCheck,
    RiskDecision,
    Signal,
)
from packages.common.enums import Direction, RiskVerdict, Side, SignalStatus
from packages.common.ids import make_decision_id
from packages.common.numbers import floor_to_increment, round_to_tick
from packages.risk.sizing import PositionSizer, SizingInput

EPSILON = 1e-9
LOSS_LIMIT_CHECKS = frozenset({"daily_loss_limit", "max_drawdown"})


@dataclass(frozen=True)
class RiskContext:
    now: datetime
    account: AccountSnapshot
    positions: Mapping[str, float]
    pending_entry_symbols: frozenset[str]
    gross_exposure: float
    instrument: InstrumentInfo
    broker_capabilities: BrokerCapabilities
    atr: float | None
    reference_price: float
    peak_equity: float
    session: Session | None
    kill_switch_engaged: bool
    trading_paused: bool
    health_ok: bool
    health_detail: str = ""


class RiskEngine(ABC):
    @abstractmethod
    def evaluate(self, signal: Signal, context: RiskContext) -> RiskDecision: ...


class StandardRiskEngine(RiskEngine):
    def __init__(self, config: RiskSection, sizer: PositionSizer) -> None:
        self._cfg = config
        self._sizer = sizer

    @property
    def config(self) -> RiskSection:
        return self._cfg

    def evaluate(self, signal: Signal, context: RiskContext) -> RiskDecision:
        cfg = self._cfg
        ctx = context
        checks: list[RiskCheck] = []

        def check(
            name: str, passed: bool, detail: str = "", value: float | None = None, limit: float | None = None
        ) -> bool:
            checks.append(RiskCheck(name=name, passed=bool(passed), detail=detail, value=value, limit=limit))
            return bool(passed)

        equity = ctx.account.equity
        price = ctx.reference_price

        # System state and signal validity
        check("kill_switch", not ctx.kill_switch_engaged)
        check("trading_not_paused", not ctx.trading_paused)
        check("system_health", ctx.health_ok, ctx.health_detail)
        check("signal_eligible", signal.status is SignalStatus.ELIGIBLE, signal.status.value)
        check(
            "signal_not_expired", ctx.now <= signal.expires_at, f"expires_at={signal.expires_at.isoformat()}"
        )
        directional = check(
            "direction_tradable", signal.direction is not Direction.NO_TRADE, signal.direction.value
        )
        check("instrument_tradable", ctx.instrument.tradable)
        if signal.direction is Direction.SHORT:
            check("short_allowed", cfg.allow_short and ctx.broker_capabilities.supports_short)
            check("instrument_shortable", ctx.instrument.shortable and ctx.instrument.easy_to_borrow)

        # Trading window
        if ctx.session is None:
            check("market_open", False, "market closed")
        else:
            since_open = ctx.session.minutes_since_open(ctx.now)
            to_close = ctx.session.minutes_to_close(ctx.now)
            check(
                "entry_window",
                since_open >= cfg.no_entry_first_minutes and to_close >= cfg.no_entry_last_minutes,
                f"since_open={since_open:.0f}m to_close={to_close:.0f}m",
            )

        # Loss limits
        last_equity = ctx.account.last_equity
        daily_pnl = equity - last_equity
        daily_limit = -cfg.max_daily_loss * last_equity
        check(
            "daily_loss_limit", daily_pnl > daily_limit, f"daily_pnl={daily_pnl:.2f}", daily_pnl, daily_limit
        )
        peak = max(ctx.peak_equity, equity)
        drawdown = 1.0 - equity / peak if peak > 0 else 0.0
        check(
            "max_drawdown",
            drawdown < cfg.max_drawdown,
            f"drawdown={drawdown:.4f}",
            drawdown,
            cfg.max_drawdown,
        )

        # Concentration
        active = {s for s, q in ctx.positions.items() if abs(q) > EPSILON} | set(ctx.pending_entry_symbols)
        check(
            "max_open_positions",
            len(active) < cfg.max_open_positions,
            "",
            len(active),
            cfg.max_open_positions,
        )
        check("no_position_in_symbol", signal.symbol not in active)

        # Stops, targets and size
        side = signal.direction.entry_side if directional else None
        atr = ctx.atr
        atr_ok = check(
            "atr_available",
            atr is not None and math.isfinite(atr) and atr > 0,
            value=atr if atr is not None else None,
        )
        stop_loss = take_profit = stop_distance = risk_reward = None
        quantity = 0.0
        if atr_ok and atr is not None and side is not None and price > 0:
            tick = ctx.instrument.tick_size
            distance = max(cfg.atr_stop_multiple * atr, price * cfg.min_stop_bps / 1e4)
            check(
                "stop_distance_within_limits",
                distance <= price * cfg.max_stop_bps / 1e4,
                value=distance / price * 1e4,
                limit=cfg.max_stop_bps,
            )
            if side is Side.BUY:
                stop_loss = round_to_tick(price - distance, tick, "down")
                take_profit = round_to_tick(price + cfg.take_profit_rr * distance, tick, "up")
                correct_side = 0 < stop_loss < price < take_profit
            else:
                stop_loss = round_to_tick(price + distance, tick, "up")
                take_profit = round_to_tick(price - cfg.take_profit_rr * distance, tick, "down")
                correct_side = 0 < take_profit < price < stop_loss
            check("stop_on_correct_side", correct_side, f"sl={stop_loss} tp={take_profit} ref={price}")
            stop_distance = abs(price - stop_loss)
            risk_reward = abs(take_profit - price) / stop_distance if stop_distance > 0 else 0.0
            check(
                "risk_reward",
                risk_reward >= cfg.min_risk_reward - EPSILON,
                "",
                risk_reward,
                cfg.min_risk_reward,
            )

            increment = ctx.instrument.quantity_increment
            caps = {
                "risk": self._sizer.size(
                    SizingInput(
                        equity=equity,
                        price=price,
                        stop_distance=stop_distance,
                        max_risk_per_trade=cfg.max_risk_per_trade,
                        quantity_increment=increment,
                    )
                ),
                "symbol_exposure": floor_to_increment(equity * cfg.max_symbol_exposure / price, increment),
                "total_exposure": floor_to_increment(
                    max(0.0, equity * cfg.max_total_exposure - ctx.gross_exposure) / price, increment
                ),
                "buying_power": floor_to_increment(
                    max(0.0, ctx.account.buying_power * cfg.buying_power_usage) / price, increment
                ),
            }
            binding = min(caps, key=lambda name: caps[name])
            quantity = max(0.0, caps[binding])
            detail = " ".join(f"{name}={value:g}" for name, value in caps.items()) + f" binding={binding}"
            check(
                "position_size",
                quantity >= ctx.instrument.min_quantity,
                detail,
                quantity,
                ctx.instrument.min_quantity,
            )
            exposure_after = ctx.gross_exposure + quantity * price
            max_exposure = equity * cfg.max_total_exposure
            check("total_exposure", exposure_after <= max_exposure + 1e-6, "", exposure_after, max_exposure)
            loss = quantity * stop_distance
            risk_budget = equity * cfg.max_risk_per_trade
            check("max_loss_per_trade", loss <= risk_budget * (1 + 1e-9) + 1e-9, "", loss, risk_budget)

        approved = all(c.passed for c in checks)
        return RiskDecision(
            decision_id=make_decision_id(signal.signal_id),
            signal_id=signal.signal_id,
            timestamp=ctx.now,
            verdict=RiskVerdict.APPROVED if approved else RiskVerdict.REJECTED,
            reasons=tuple(c.name for c in checks if not c.passed),
            checks=tuple(checks),
            side=side,
            quantity=quantity if approved else 0.0,
            entry_reference_price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            stop_distance=stop_distance,
            max_loss=quantity * stop_distance if approved and stop_distance is not None else None,
            risk_reward=risk_reward,
            notional=quantity * price if approved else None,
            sizing_method=self._sizer.name,
            account_equity=equity,
        )
