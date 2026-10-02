"""Health checks (spec §42). Any critical failure blocks new entries; persistent failures engage the kill switch."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from packages.common.enums import HealthState


@dataclass(frozen=True)
class HealthCheckResult:
    name: str
    state: HealthState
    detail: str = ""
    critical: bool = True


CheckFunction = Callable[[], Awaitable[HealthCheckResult]]

STANDARD_CHECKS = (
    "market_data_connected",
    "broker_connected",
    "account_available",
    "order_stream_connected",
    "database_available",
    "redis_available",
    "clock_synchronized",
)


class HealthMonitor:
    def __init__(self) -> None:
        self._checks: dict[str, CheckFunction] = {}
        self._reported: dict[str, HealthCheckResult] = {}
        self._last: dict[str, HealthCheckResult] = {}
        self._failing_since: dict[str, datetime] = {}

    def register(self, name: str, check: CheckFunction) -> None:
        self._checks[name] = check

    def report(self, result: HealthCheckResult) -> None:
        """Component-reported state (e.g. a failed database write); a reported FAIL dominates its check."""
        self._reported[result.name] = result
        self._last[result.name] = result

    def clear_report(self, name: str) -> None:
        self._reported.pop(name, None)

    async def run(self, now: datetime) -> dict[str, HealthCheckResult]:
        results: dict[str, HealthCheckResult] = {}
        for name, check in self._checks.items():
            try:
                result = await check()
            except Exception as exc:
                result = HealthCheckResult(name, HealthState.FAIL, f"check raised: {exc}")
            reported = self._reported.get(name)
            if reported is not None and reported.state is HealthState.FAIL:
                result = reported
            results[name] = result
        for name, reported in self._reported.items():
            results.setdefault(name, reported)
        for name, result in results.items():
            if result.state is HealthState.FAIL:
                self._failing_since.setdefault(name, now)
            else:
                self._failing_since.pop(name, None)
        self._last = results
        return results

    @property
    def last(self) -> dict[str, HealthCheckResult]:
        return dict(self._last)

    def failing_since(self, name: str) -> datetime | None:
        return self._failing_since.get(name)

    def trading_allowed(self) -> tuple[bool, str]:
        failing = [r for r in self._last.values() if r.critical and r.state is HealthState.FAIL]
        return not failing, "; ".join(f"{r.name}: {r.detail}" for r in failing)
