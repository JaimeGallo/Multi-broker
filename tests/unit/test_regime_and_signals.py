from __future__ import annotations

from datetime import timedelta

import pytest

from packages.common.config import CostsSection, RegimeSection, SignalsSection
from packages.common.costs import CostModel
from packages.common.entities import JEVPrediction, RegimeAssessment
from packages.common.enums import Direction, MarketRegime, NoTradeReason, SignalStatus
from packages.signals.engine import SignalEngine
from packages.signals.regime import RegimeEngine
from tests.helpers import SESSION_OPEN, make_features, make_quote

REGIME = RegimeEngine(RegimeSection())
NOW = SESSION_OPEN + timedelta(hours=1)


def classify(adx: float, trend: float, vol_ratio: float) -> MarketRegime:
    features = make_features({"adx_14": adx, "trend_strength_30": trend, "vol_ratio_20_100": vol_ratio})
    return REGIME.classify(features).regime


@pytest.mark.parametrize(
    ("adx", "trend", "vol_ratio", "expected"),
    [
        (30, 3.0, 1.0, MarketRegime.TRENDING_UP),
        (30, -3.0, 1.0, MarketRegime.TRENDING_DOWN),
        (10, 0.0, 2.0, MarketRegime.HIGH_VOLATILITY),
        (10, 0.0, 0.5, MarketRegime.LOW_VOLATILITY),
        (10, 0.0, 1.0, MarketRegime.RANGE),
        (22, 0.0, 1.0, MarketRegime.UNKNOWN),
        (float("nan"), 0.0, 1.0, MarketRegime.UNKNOWN),
    ],
)
def test_regime_classification(adx: float, trend: float, vol_ratio: float, expected: MarketRegime) -> None:
    assert classify(adx, trend, vol_ratio) is expected


def prediction(direction: Direction, *, expected_return: float, p_up: float = 0.7) -> JEVPrediction:
    return JEVPrediction(
        prediction_id="P-1",
        symbol="TEST",
        timestamp=NOW,
        horizon_minutes=15,
        direction=direction,
        probability_up=p_up,
        probability_down=1 - p_up,
        expected_return=expected_return,
        expected_volatility=0.004,
        confidence=0.8,
        model_name="m",
        model_version="1",
        feature_version="f",
        feature_id="F-1",
    )


def regime(kind: MarketRegime = MarketRegime.TRENDING_UP) -> RegimeAssessment:
    return RegimeAssessment(symbol="TEST", timestamp=NOW, regime=kind)


def signal_engine(**config: float) -> SignalEngine:
    return SignalEngine(
        SignalsSection(**config), CostModel(CostsSection()), namespace="ns", strategy="s", bar_seconds=60
    )


def evaluate(engine: SignalEngine, pred: JEVPrediction, reg: RegimeAssessment, quote=None):
    return engine.evaluate(pred, reg, reference_price=100.0, quote=quote, volatility_per_bar=0.001, now=NOW)


def test_expected_value_subtracts_round_trip_costs() -> None:
    engine = signal_engine()
    ev = engine.expected_value(
        prediction(Direction.LONG, expected_return=0.002),
        reference_price=100.0,
        quote=None,
        volatility_per_bar=0.001,
    )
    assert ev.gross_edge_bps == pytest.approx(20.0)
    assert ev.costs.total_bps > 0
    assert ev.net_edge_bps == pytest.approx(ev.gross_edge_bps - ev.costs.total_bps)


def test_probability_above_half_is_not_enough() -> None:
    weak = evaluate(signal_engine(), prediction(Direction.LONG, expected_return=0.0002), regime())
    assert weak is not None and weak.status is SignalStatus.REJECTED
    assert NoTradeReason.INSUFFICIENT_EDGE.value in weak.rejection_reasons


def test_eligible_signal_and_deterministic_id() -> None:
    engine = signal_engine()
    first = evaluate(engine, prediction(Direction.LONG, expected_return=0.003), regime())
    second = evaluate(engine, prediction(Direction.LONG, expected_return=0.003), regime())
    assert first is not None and second is not None
    assert first.status is SignalStatus.ELIGIBLE and first.signal_id == second.signal_id
    assert first.expires_at == NOW + timedelta(seconds=SignalsSection().signal_ttl_seconds)


def test_rejections_regime_spread_probability() -> None:
    engine = signal_engine()
    blocked = evaluate(
        engine, prediction(Direction.LONG, expected_return=0.003), regime(MarketRegime.UNKNOWN)
    )
    assert blocked is not None and NoTradeReason.REGIME_BLOCKED.value in blocked.rejection_reasons
    wide = evaluate(
        engine,
        prediction(Direction.LONG, expected_return=0.01),
        regime(),
        make_quote(at=NOW, bid=99.5, ask=100.5),
    )
    assert wide is not None and NoTradeReason.HIGH_SPREAD.value in wide.rejection_reasons
    unlikely = evaluate(engine, prediction(Direction.LONG, expected_return=0.003, p_up=0.52), regime())
    assert unlikely is not None and NoTradeReason.LOW_PROBABILITY.value in unlikely.rejection_reasons


def test_no_trade_prediction_produces_no_signal() -> None:
    assert (
        evaluate(signal_engine(), prediction(Direction.NO_TRADE, expected_return=0.0, p_up=0.5), regime())
        is None
    )
