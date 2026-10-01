"""Signal Engine: turns a directional prediction into a signal only if its expected value survives costs.

`probability > 50%` is never enough. The engine estimates round-trip costs (spread, slippage, commissions,
regulatory fees, latency) and requires a configurable minimum net edge, probability and confidence, an
acceptable spread and an allowed market regime. It does not look at the account: that is the Risk Engine's job.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from packages.common.config import SignalsSection
from packages.common.costs import CostModel
from packages.common.entities import (
    ExpectedValue,
    JEVPrediction,
    MarketQuote,
    RegimeAssessment,
    Signal,
)
from packages.common.enums import Direction, NoTradeReason, SignalStatus
from packages.common.ids import make_signal_id


class SignalEngine:
    def __init__(
        self,
        config: SignalsSection,
        cost_model: CostModel,
        *,
        namespace: str,
        strategy: str,
        bar_seconds: float,
    ) -> None:
        self._cfg = config
        self._costs = cost_model
        self._namespace = namespace
        self._strategy = strategy
        self._bar_seconds = bar_seconds

    def expected_value(
        self,
        prediction: JEVPrediction,
        *,
        reference_price: float,
        quote: MarketQuote | None,
        volatility_per_bar: float,
    ) -> ExpectedValue:
        costs = self._costs.estimate_round_trip(
            price=reference_price,
            quote_spread_bps=quote.spread_bps if quote is not None else None,
            volatility_per_bar=volatility_per_bar,
            bar_seconds=self._bar_seconds,
        )
        gross = prediction.direction.sign * prediction.expected_return * 1e4
        net = gross - costs.total_bps
        vol_bps = prediction.expected_volatility * 1e4
        return ExpectedValue(
            gross_edge_bps=gross,
            costs=costs,
            net_edge_bps=net,
            edge_to_volatility=net / vol_bps if vol_bps > 0 else 0.0,
        )

    def evaluate(
        self,
        prediction: JEVPrediction,
        regime: RegimeAssessment,
        *,
        reference_price: float,
        quote: MarketQuote | None,
        volatility_per_bar: float,
        now: datetime,
    ) -> Signal | None:
        """Return None for NO_TRADE predictions, otherwise a signal in state ELIGIBLE or REJECTED."""
        if prediction.direction is Direction.NO_TRADE:
            return None
        cfg = self._cfg
        ev = self.expected_value(
            prediction, reference_price=reference_price, quote=quote, volatility_per_bar=volatility_per_bar
        )
        spread_bps = quote.spread_bps if quote is not None else None
        signal = Signal(
            signal_id=make_signal_id(self._namespace, self._strategy, prediction.symbol, prediction.timestamp),
            prediction_id=prediction.prediction_id,
            symbol=prediction.symbol,
            timestamp=prediction.timestamp,
            direction=prediction.direction,
            probability=prediction.directional_probability,
            confidence=prediction.confidence,
            expected_return=prediction.expected_return,
            expected_volatility=prediction.expected_volatility,
            horizon_minutes=prediction.horizon_minutes,
            market_regime=regime.regime,
            model_name=prediction.model_name,
            model_version=prediction.model_version,
            feature_version=prediction.feature_version,
            reference_price=reference_price,
            spread_bps=spread_bps,
            expected_value=ev,
            expires_at=prediction.timestamp + timedelta(seconds=cfg.signal_ttl_seconds),
            strategy=self._strategy,
            updated_at=now,
        )
        reasons: list[str] = []
        if signal.probability < cfg.min_probability:
            reasons.append(NoTradeReason.LOW_PROBABILITY.value)
        if signal.confidence < cfg.min_confidence:
            reasons.append(NoTradeReason.LOW_CONFIDENCE.value)
        if ev.net_edge_bps < cfg.min_net_edge_bps:
            reasons.append(NoTradeReason.INSUFFICIENT_EDGE.value)
        if spread_bps is not None and spread_bps > cfg.max_spread_bps:
            reasons.append(NoTradeReason.HIGH_SPREAD.value)
        if regime.regime in cfg.blocked_regimes:
            reasons.append(NoTradeReason.REGIME_BLOCKED.value)
        if reasons:
            signal.rejection_reasons = reasons
            signal.transition(SignalStatus.REJECTED, now, reason=reasons[0])
        else:
            signal.transition(SignalStatus.ELIGIBLE, now)
        return signal
