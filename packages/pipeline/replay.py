"""Decision replay (spec §55): rebuild one decision from the audit trail and check it is reproduced exactly.

From `signal_id` the verifier loads the stored bars of the feature window, recomputes the FeatureVector with the
feature spec and namespace of the original run, rebuilds the model from the parameters registered in
`model_versions`, and compares features, prediction, regime and the Signal Engine verdict with what was recorded.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from packages.common.calendar import RegularHoursCalendar
from packages.common.config import (
    CostsSection,
    FeaturesSection,
    ModelSection,
    RegimeSection,
    SignalsSection,
    TradingSection,
)
from packages.common.costs import CostModel
from packages.common.entities import MarketBar, MarketQuote
from packages.common.enums import DataQualityStatus, NoTradeReason, SignalStatus, Timeframe
from packages.features.engine import FeatureEngine
from packages.features.spec import FeatureSpec
from packages.jev.registry import build_model
from packages.persistence.repositories import AuditRepository
from packages.signals.engine import SignalEngine
from packages.signals.regime import RegimeEngine

REL_TOLERANCE = 1e-12
ABS_TOLERANCE = 1e-12
SIGNAL_STAGE_REASONS = frozenset(
    reason.value
    for reason in (
        NoTradeReason.LOW_PROBABILITY,
        NoTradeReason.LOW_CONFIDENCE,
        NoTradeReason.INSUFFICIENT_EDGE,
        NoTradeReason.HIGH_SPREAD,
        NoTradeReason.REGIME_BLOCKED,
    )
)
PREDICTION_FIELDS = (
    "probability_up",
    "probability_down",
    "expected_return",
    "expected_volatility",
    "confidence",
)


@dataclass
class VerificationResult:
    signal_id: str
    found: bool = True
    checks: dict[str, bool] = field(default_factory=dict)
    mismatches: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.found and bool(self.checks) and all(self.checks.values())

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks[name] = self.checks.get(name, True) and passed
        if not passed:
            self.mismatches.append(f"{name}: {detail}" if detail else name)

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "found": self.found,
            "ok": self.ok,
            "checks": dict(self.checks),
            "mismatches": list(self.mismatches),
            "details": dict(self.details),
        }


def same_number(stored: float | None, replayed: float | None) -> bool:
    """Stored non-finite values are NULL; anything else must match to floating-point precision."""
    stored_finite = stored is not None and math.isfinite(stored)
    replayed_finite = replayed is not None and math.isfinite(replayed)
    if not stored_finite or not replayed_finite:
        return stored_finite == replayed_finite
    assert stored is not None and replayed is not None
    return math.isclose(stored, replayed, rel_tol=REL_TOLERANCE, abs_tol=ABS_TOLERANCE)


def _datetime(value: Any) -> datetime:
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def bar_from_row(row: dict[str, Any]) -> MarketBar:
    return MarketBar(
        symbol=row["symbol"],
        timeframe=Timeframe(row["timeframe"]),
        start=_datetime(row["start"]),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        vwap=row["vwap"],
        trade_count=row["trade_count"],
        source=row["source"],
        received_at=_datetime(row["received_at"]) if row.get("received_at") else None,
    )


class DecisionVerifier:
    def __init__(self, repository: AuditRepository) -> None:
        self._repo = repository

    async def verify_decision(self, signal_id: str) -> VerificationResult:
        result = VerificationResult(signal_id=signal_id)
        trace = await self._repo.decision_trace(signal_id)
        if trace is None:
            result.found = False
            result.mismatches.append("signal not found")
            return result
        signal, prediction, stored_features, run = (
            trace["signal"],
            trace["prediction"],
            trace["features"],
            trace["run"],
        )
        for name, record in (("prediction", prediction), ("features", stored_features), ("run", run)):
            if record is None:
                result.check(f"{name}_recorded", False, "missing from the audit trail")
        if prediction is None or stored_features is None or run is None:
            return result
        config: dict[str, Any] = run["config"]
        trading = TradingSection.model_validate(config["trading"])
        calendar = RegularHoursCalendar(
            trading.exchange_timezone, trading.session_open, trading.session_close
        )
        namespace = run["namespace"]

        # 1. features
        spec = FeatureSpec.from_config(FeaturesSection.model_validate(config["features"]))
        result.check(
            "spec_hash", spec.spec_hash() == stored_features["spec_hash"], stored_features["spec_hash"]
        )
        bars = [
            bar_from_row(row)
            for row in trace["bars"]
            if row.get("quality_status") != DataQualityStatus.INVALID.value
        ]
        result.details["bars"] = len(bars)
        result.check(
            "window_size",
            len(bars) == stored_features["n_bars"],
            f"{len(bars)} != {stored_features['n_bars']}",
        )
        if not bars:
            return result
        quote = MarketQuote.model_validate(stored_features["quote"]) if stored_features["quote"] else None
        engine = FeatureEngine(spec, namespace=namespace, day_start=calendar.day_start)
        features = engine.compute(bars, quote)
        result.check("feature_id", features.feature_id == stored_features["feature_id"], features.feature_id)
        stored_values: dict[str, float | None] = stored_features["values"]
        differing = [
            name
            for name in sorted(set(stored_values) | set(features.values))
            if not same_number(stored_values.get(name), features.values.get(name))
        ]
        result.check("feature_values", not differing, ",".join(differing))

        # 2. prediction (model rebuilt from its registered parameters)
        params = await self._repo.model_params(prediction["model_name"], prediction["model_version"])
        result.check(
            "model_registered",
            params is not None,
            f"{prediction['model_name']}@{prediction['model_version']}",
        )
        if params is None:
            return result
        model = build_model(
            ModelSection(name=prediction["model_name"], version=prediction["model_version"], params=params),
            namespace=namespace,
            feature_version=prediction["feature_version"],
            horizon_minutes=prediction["horizon_minutes"],
            bar_minutes=features.timeframe.minutes,
        )
        replayed = model.predict(features)
        result.check(
            "prediction_id", replayed.prediction_id == prediction["prediction_id"], replayed.prediction_id
        )
        result.check(
            "direction", replayed.direction.value == prediction["direction"], replayed.direction.value
        )
        for name in PREDICTION_FIELDS:
            value = getattr(replayed, name)
            result.check("prediction_values", same_number(prediction[name], value), f"{name}={value}")

        # 3. regime and Signal Engine verdict
        regime = RegimeEngine(RegimeSection.model_validate(config["regime"])).classify(features)
        result.check("regime", regime.regime.value == prediction["regime"], regime.regime.value)
        signals = SignalEngine(
            SignalsSection.model_validate(config["signals"]),
            CostModel(CostsSection.model_validate(config["costs"])),
            namespace=namespace,
            strategy=signal["strategy"],
            bar_seconds=features.timeframe.seconds,
        )
        replayed_signal = signals.evaluate(
            replayed,
            regime,
            reference_price=features.close,
            quote=quote,
            volatility_per_bar=features.get(f"realized_vol_{spec.realized_vol_window}"),
            now=features.timestamp,
        )
        if replayed_signal is None:
            result.check("signal", False, "replayed prediction is NO_TRADE")
            return result
        result.check("signal_id", replayed_signal.signal_id == signal["signal_id"], replayed_signal.signal_id)
        ev = replayed_signal.expected_value
        result.check("net_edge", same_number(signal["net_edge_bps"], ev.net_edge_bps if ev else None))
        stored_stage = [r for r in signal["rejection_reasons"] or [] if r in SIGNAL_STAGE_REASONS]
        if replayed_signal.status is SignalStatus.REJECTED:
            expected = replayed_signal.rejection_reasons
            result.check(
                "signal_verdict",
                signal["status"] == SignalStatus.REJECTED.value and stored_stage[: len(expected)] == expected,
                f"replayed REJECTED {expected}, stored {signal['status']} {signal['rejection_reasons']}",
            )
        else:
            result.check(
                "signal_verdict", not stored_stage, f"replayed ELIGIBLE, stored {signal['rejection_reasons']}"
            )
        result.details.update(
            {
                "symbol": signal["symbol"],
                "timestamp": signal["ts"],
                "direction": replayed.direction.value,
                "status": signal["status"],
                "model": model.metadata.key,
                "orders": len(trace["orders"]),
            }
        )
        return result
