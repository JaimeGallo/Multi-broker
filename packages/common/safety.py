"""Safety gates that no configuration can bypass."""

from __future__ import annotations

import os
from collections.abc import Mapping

from packages.common.entities import BrokerCapabilities
from packages.common.enums import TradingMode
from packages.common.errors import LiveTradingNotAllowed, SafetyError

LIVE_ENV_FLAG = "LIVE_TRADING_ENABLED"


def live_flag_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return env.get(LIVE_ENV_FLAG, "").strip().lower() == "true"


def enforce_mode_gate(mode: TradingMode, broker_mode: str, environ: Mapping[str, str] | None = None) -> None:
    """Refuse live trading. Two gates: the env flag, and this build simply does not implement live."""
    if mode is TradingMode.LIVE or broker_mode == "live":
        if not live_flag_enabled(environ):
            raise LiveTradingNotAllowed(
                "Live trading requires LIVE_TRADING_ENABLED=true plus validated risk configuration, broker "
                "connectivity, market data and database health, and an armed kill switch."
            )
        raise LiveTradingNotAllowed(
            "Live trading is not implemented in this build. Complete the phase 10 live-readiness checklist "
            "(docs/IMPLEMENTATION_PLAN.md) and remove this gate deliberately."
        )


def assert_paper_account(capabilities: BrokerCapabilities, mode: TradingMode) -> None:
    """In any non-live mode, a broker reporting a real-money account is a fatal misconfiguration."""
    if mode is not TradingMode.LIVE and not capabilities.is_paper:
        raise SafetyError(
            f"broker '{capabilities.broker}' reports a LIVE account while running in {mode.value} mode; refusing"
        )


def mode_banner(mode: TradingMode, broker: str, market_data: str) -> str:
    rule = "=" * 78
    lines = [
        rule,
        f"  JEV TRADING ENGINE  |  MODE: {mode.value.upper()}  |  BROKER: {broker.upper()}  |  DATA: {market_data.upper()}",
        "  NO REAL MONEY  |  LIVE TRADING DISABLED",
    ]
    if market_data == "mock":
        lines.append("  SYNTHETIC DATA: results say nothing about real markets")
    lines.append(rule)
    return "\n".join(lines)
