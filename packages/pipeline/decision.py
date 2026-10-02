"""Decision pipeline: features → JEV → regime → signal. Identical in backtest, replay, shadow and paper."""

from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter

from packages.common.clock import Clock
from packages.common.entities import (
    FeatureVector,
    JEVPrediction,
    MarketBar,
    MarketQuote,
    RegimeAssessment,
    Signal,
)
from packages.common.enums import Direction, NoTradeReason, SignalStatus
from packages.common.errors import ModelError
from packages.features.engine import FeatureEngine
from packages.jev.base import JEVModel
from packages.signals.engine import SignalEngine
from packages.signals.regime import RegimeEngine


def _elapsed_ms(started: float) -> float:
    return (perf_counter() - started) * 1000.0


@dataclass
class PipelineResult:
    bar: MarketBar
    features: FeatureVector | None = None
    prediction: JEVPrediction | None = None
    regime: RegimeAssessment | None = None
    signal: Signal | None = None
    no_trade_reason: NoTradeReason | None = None
    detail: str = ""
    timings_ms: dict[str, float] = field(default_factory=dict)


class DecisionPipeline:
    def __init__(
        self,
        *,
        features: FeatureEngine,
        model: JEVModel,
        regime: RegimeEngine,
        signals: SignalEngine,
        clock: Clock,
    ) -> None:
        self._features = features
        self._model = model
        self._regime = regime
        self._signals = signals
        self._clock = clock
        self._vol_feature = f"realized_vol_{features.spec.realized_vol_window}"

    @property
    def model(self) -> JEVModel:
        return self._model

    @property
    def features(self) -> FeatureEngine:
        return self._features

    def observe(self, bar: MarketBar) -> None:
        """Keep the feature window continuous without deciding (stale/degraded bars)."""
        self._features.observe(bar)

    def evaluate(self, bar: MarketBar, quote: MarketQuote | None) -> PipelineResult:
        result = PipelineResult(bar=bar)
        started = perf_counter()
        features = self._features.update(bar, quote)
        result.timings_ms["features"] = _elapsed_ms(started)
        if features is None:
            result.no_trade_reason = NoTradeReason.WARMUP
            return result
        result.features = features
        missing = features.missing(self._model.required_features)
        if missing:
            result.no_trade_reason = NoTradeReason.FEATURES_INCOMPLETE
            result.detail = ",".join(missing)
            return result

        started = perf_counter()
        try:
            prediction = self._model.predict(features)
        except Exception as exc:
            raise ModelError(f"{self._model.metadata.key} failed on {features.feature_id}: {exc}") from exc
        result.timings_ms["model"] = _elapsed_ms(started)
        result.prediction = prediction
        result.regime = self._regime.classify(features)
        if prediction.direction is Direction.NO_TRADE:
            result.no_trade_reason = NoTradeReason.MODEL_NO_TRADE
            return result

        started = perf_counter()
        signal = self._signals.evaluate(
            prediction,
            result.regime,
            reference_price=features.close,
            quote=quote,
            volatility_per_bar=features.get(self._vol_feature),
            now=self._clock.now(),
        )
        result.timings_ms["signal"] = _elapsed_ms(started)
        result.signal = signal
        if signal is not None and signal.status is SignalStatus.REJECTED and signal.rejection_reasons:
            result.no_trade_reason = NoTradeReason(signal.rejection_reasons[0])
        return result
