"""JEV model contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from packages.common.entities import FeatureVector, JEVPrediction, ModelMetadata
from packages.common.enums import Direction
from packages.common.ids import make_prediction_id


def decide_direction(probability_up: float, probability_down: float, threshold: float) -> Direction:
    if probability_up >= threshold and probability_up > probability_down:
        return Direction.LONG
    if probability_down >= threshold and probability_down > probability_up:
        return Direction.SHORT
    return Direction.NO_TRADE


class JEVModel(ABC):
    """JEV's predictive core.

    Contract:
    - `predict` is a pure function of the FeatureVector (no I/O, no clock, no hidden state), so every
      prediction can be replayed and verified;
    - it proposes LONG / SHORT / NO_TRADE with probabilities, expected return and expected volatility over
      `metadata.horizon_minutes`;
    - it NEVER sizes positions, sets stops or loss limits, or talks to a broker. The Risk Engine has
      authority over every signal.
    """

    def __init__(self, metadata: ModelMetadata, *, namespace: str) -> None:
        self._metadata = metadata
        self._namespace = namespace

    @property
    def metadata(self) -> ModelMetadata:
        return self._metadata

    @property
    def required_features(self) -> tuple[str, ...]:
        return ()

    @abstractmethod
    def predict(self, features: FeatureVector) -> JEVPrediction: ...

    def _prediction(
        self,
        features: FeatureVector,
        *,
        direction: Direction,
        probability_up: float,
        probability_down: float,
        expected_return: float,
        expected_volatility: float,
        confidence: float,
    ) -> JEVPrediction:
        md = self._metadata
        return JEVPrediction(
            prediction_id=make_prediction_id(
                self._namespace, md.model_name, md.model_version, features.symbol, features.timestamp
            ),
            symbol=features.symbol,
            timestamp=features.timestamp,
            horizon_minutes=md.horizon_minutes,
            direction=direction,
            probability_up=probability_up,
            probability_down=probability_down,
            expected_return=expected_return,
            expected_volatility=expected_volatility,
            confidence=confidence,
            model_name=md.model_name,
            model_version=md.model_version,
            feature_version=md.feature_version,
            feature_id=features.feature_id,
        )
