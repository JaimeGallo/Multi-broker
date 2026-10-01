"""JEV v0 — transparent, deterministic STAND-IN model.

This is NOT a validated predictive model and no statistical edge is claimed. It exists so the full pipeline
(signals, risk, execution, audit) can be exercised end to end until the real JEV model is researched and
validated with walk-forward testing (phase 3). Its parameters are arbitrary.

Logic: a momentum/trend score in volatility units, mapped to a probability with a logistic function and
shrunk towards 0.5. Expected volatility scales per-bar realized volatility to the horizon; expected return is
a deliberately small fraction of it.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict

from packages.common.entities import FeatureVector, JEVPrediction, ModelMetadata
from packages.common.errors import ModelError
from packages.jev.base import JEVModel, decide_direction


class HeuristicParams(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    w_momentum: float = 0.6
    w_trend: float = 0.3
    w_rsi: float = 0.1
    steepness: float = 1.2
    shrink: float = 0.8
    edge_scale: float = 0.5
    entry_threshold: float = 0.55
    confidence_scale: float = 0.3
    momentum_lookback: int = 15


class HeuristicJEVModel(JEVModel):
    NAME = "jev-heuristic"
    REQUIRED = ("ret_15", "realized_vol_30", "trend_strength_30", "rsi_14")

    def __init__(
        self,
        *,
        namespace: str,
        version: str,
        feature_version: str,
        horizon_minutes: int,
        bar_minutes: int,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        self.params = HeuristicParams(**dict(params or {}))
        if self.params.momentum_lookback != 15:
            raise ModelError("feature set v0.1 provides ret_15 only; momentum_lookback must be 15")
        self._horizon_bars = horizon_minutes / bar_minutes
        metadata = ModelMetadata(
            model_name=self.NAME,
            model_version=version,
            feature_version=feature_version,
            horizon_minutes=horizon_minutes,
            params=self.params.model_dump(),
            description="Deterministic momentum/trend stand-in. No validated edge.",
        )
        super().__init__(metadata, namespace=namespace)

    @property
    def required_features(self) -> tuple[str, ...]:
        return self.REQUIRED

    def predict(self, features: FeatureVector) -> JEVPrediction:
        missing = features.missing(self.REQUIRED)
        if missing:
            raise ModelError(f"missing features: {missing}")
        p = self.params
        vol = max(features.values["realized_vol_30"], 1e-6)
        z_momentum = _clip(features.values["ret_15"] / (vol * math.sqrt(p.momentum_lookback)), 4.0)
        z_trend = _clip(features.values["trend_strength_30"] / 3.0, 4.0)
        z_rsi = (features.values["rsi_14"] - 50.0) / 50.0
        score = p.w_momentum * z_momentum + p.w_trend * z_trend + p.w_rsi * z_rsi

        raw = 1.0 / (1.0 + math.exp(-p.steepness * score))
        probability_up = 0.5 + p.shrink * (raw - 0.5)
        probability_down = 1.0 - probability_up
        expected_volatility = vol * math.sqrt(self._horizon_bars)
        expected_return = (probability_up - probability_down) * expected_volatility * p.edge_scale
        confidence = min(1.0, abs(probability_up - probability_down) / p.confidence_scale)
        return self._prediction(
            features,
            direction=decide_direction(probability_up, probability_down, p.entry_threshold),
            probability_up=probability_up,
            probability_down=probability_down,
            expected_return=expected_return,
            expected_volatility=expected_volatility,
            confidence=confidence,
        )


def _clip(value: float, limit: float) -> float:
    return max(-limit, min(limit, value))
