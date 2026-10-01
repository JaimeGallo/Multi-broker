"""Baseline models used as controls (phase 3 adds moving average, logistic regression, RF and GB)."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from typing import Any

from packages.common.entities import FeatureVector, JEVPrediction, ModelMetadata
from packages.common.enums import Direction
from packages.common.ids import digest
from packages.jev.base import JEVModel, decide_direction


class RandomJEVModel(JEVModel):
    """Coin-flip control. Deterministic per (seed, symbol, timestamp): replayable like any other model."""

    NAME = "baseline-random"
    REQUIRED = ("realized_vol_30",)

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
        options = dict(params or {})
        self._seed = int(options.get("seed", 0))
        self._threshold = float(options.get("entry_threshold", 0.55))
        self._edge_scale = float(options.get("edge_scale", 0.5))
        self._horizon_bars = horizon_minutes / bar_minutes
        metadata = ModelMetadata(
            model_name=self.NAME,
            model_version=version,
            feature_version=feature_version,
            horizon_minutes=horizon_minutes,
            params={"seed": self._seed, "entry_threshold": self._threshold, "edge_scale": self._edge_scale},
            description="Random control model.",
        )
        super().__init__(metadata, namespace=namespace)

    @property
    def required_features(self) -> tuple[str, ...]:
        return self.REQUIRED

    def predict(self, features: FeatureVector) -> JEVPrediction:
        rng = random.Random(digest(self._seed, features.symbol, features.timestamp.isoformat(), length=12))
        probability_up = rng.uniform(0.3, 0.7)
        probability_down = 1.0 - probability_up
        vol = max(features.values["realized_vol_30"], 1e-6) * math.sqrt(self._horizon_bars)
        return self._prediction(
            features,
            direction=decide_direction(probability_up, probability_down, self._threshold),
            probability_up=probability_up,
            probability_down=probability_down,
            expected_return=(probability_up - probability_down) * vol * self._edge_scale,
            expected_volatility=vol,
            confidence=min(1.0, abs(probability_up - probability_down) / 0.3),
        )


class FlatJEVModel(JEVModel):
    """Always NO_TRADE. A pipeline with this model must never send an order."""

    NAME = "baseline-flat"

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
        metadata = ModelMetadata(
            model_name=self.NAME,
            model_version=version,
            feature_version=feature_version,
            horizon_minutes=horizon_minutes,
            description="Never trades.",
        )
        super().__init__(metadata, namespace=namespace)

    def predict(self, features: FeatureVector) -> JEVPrediction:
        return self._prediction(
            features,
            direction=Direction.NO_TRADE,
            probability_up=0.5,
            probability_down=0.5,
            expected_return=0.0,
            expected_volatility=0.0,
            confidence=0.0,
        )
