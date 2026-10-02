from __future__ import annotations

from datetime import timedelta

import pytest

from packages.analytics.ledger import TradeLedger
from packages.analytics.metrics import cagr, compute_performance, max_drawdown, sharpe_ratio, sortino_ratio
from packages.analytics.predictions import PredictionOutcomeTracker, summarize_outcomes
from packages.common.calendar import RegularHoursCalendar
from packages.common.config import CostsSection
from packages.common.costs import CostModel
from packages.common.entities import Fill, JEVPrediction, PortfolioSnapshot, RiskDecision
from packages.common.enums import Direction, OrderIntent, RiskVerdict, Side
from tests.helpers import SESSION_OPEN, make_bar
from tests.unit.test_risk import signal


def test_fees_apply_regulatory_charges_to_sells_only() -> None:
    model = CostModel(
        CostsSection(
            commission_per_share=0.005,
            min_commission=1.0,
            sec_fee_rate=0.0001,
            taf_per_share=0.001,
            taf_max_per_trade=0.05,
        )
    )
    assert model.fees(Side.BUY, 100, 50.0) == pytest.approx(1.0)  # min commission
    assert model.fees(Side.SELL, 1000, 50.0) == pytest.approx(5.0 + 50_000 * 0.0001 + 0.05)
    assert model.fees(Side.BUY, 0, 50.0) == 0.0


def test_execution_price_is_adverse_and_costs_add_up() -> None:
    model = CostModel(CostsSection(slippage_bps=2.0, default_spread_bps=4.0))
    assert model.execution_price(Side.BUY, 100.0, None) == pytest.approx(100.04)
    assert model.execution_price(Side.SELL, 100.0, 2.0) == pytest.approx(99.97)
    estimate = model.estimate_round_trip(
        price=100.0, quote_spread_bps=None, volatility_per_bar=0.001, bar_seconds=60
    )
    parts = (
        estimate.spread_bps,
        estimate.slippage_bps,
        estimate.commission_bps,
        estimate.regulatory_bps,
        estimate.latency_bps,
    )
    assert estimate.total_bps == pytest.approx(sum(parts))
    assert estimate.slippage_bps == pytest.approx(4.0) and estimate.latency_bps > 0


def test_metric_primitives_report_none_when_undefined() -> None:
    assert max_drawdown([100, 120, 90, 130]) == pytest.approx(0.25)
    assert max_drawdown([100]) is None
    assert sharpe_ratio([0.01], 252) is None
    assert sharpe_ratio([0.01, 0.01], 252) is None  # zero variance
    assert sharpe_ratio([0.01, 0.02, -0.01], 252) is not None
    assert sortino_ratio([0.01, 0.02], 252) is None  # no downside
    assert cagr(100, 110, 10) is None  # too short to annualize
    assert cagr(100, 110, 365.25) == pytest.approx(0.10)


def snapshot(minutes: int, equity: float) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=SESSION_OPEN + timedelta(minutes=minutes),
        broker="mock",
        cash=equity,
        equity=equity,
        buying_power=equity,
        gross_exposure=0.0,
        net_exposure=0.0,
        open_positions=0,
        daily_pnl=0.0,
        peak_equity=equity,
        drawdown=0.0,
    )


def fill(intent: OrderIntent, side: Side, price: float, minutes: int) -> Fill:
    return Fill(
        fill_id=f"{intent.value}-{minutes}",
        client_order_id=f"jev-S-TEST-1-{intent.code}",
        broker="mock",
        symbol="TEST",
        side=side,
        quantity=100,
        price=price,
        fee=1.0,
        timestamp=SESSION_OPEN + timedelta(hours=1, minutes=minutes),
        intent=intent,
        signal_id="S-TEST-1",
    )


def test_ledger_separates_model_and_execution_quality() -> None:
    ledger = TradeLedger()
    decision = RiskDecision(
        decision_id="R-TEST-1",
        signal_id="S-TEST-1",
        timestamp=SESSION_OPEN,
        verdict=RiskVerdict.APPROVED,
        side=Side.BUY,
        quantity=100,
        stop_loss=99.6,
        take_profit=100.6,
    )
    ledger.register(signal(), decision)  # reference price 100.0
    assert ledger.on_fill(fill(OrderIntent.ENTRY, Side.BUY, 100.05, 0), None) is None
    ledger.on_bar(
        make_bar(start=SESSION_OPEN + timedelta(hours=1, minutes=1), close=100.4, high=100.7, low=99.9)
    )
    trade = ledger.on_fill(fill(OrderIntent.TAKE_PROFIT, Side.SELL, 100.6, 5), 100.6)
    assert trade is not None and ledger.open_count == 0
    assert trade.gross_pnl == pytest.approx(100 * (100.6 - 100.05))
    assert trade.net_pnl == pytest.approx(trade.gross_pnl - 2.0)
    assert trade.model_pnl == pytest.approx(100 * (100.6 - 100.0))
    assert trade.execution_shortfall == pytest.approx(trade.model_pnl - trade.net_pnl)
    assert trade.entry_slippage_bps == pytest.approx(5.0)

    calendar = RegularHoursCalendar()
    report = compute_performance(
        [trade],
        [snapshot(0, 100_000), snapshot(60, 100_053)],
        trading_date=calendar.trading_date,
        start_equity=100_000,
    )
    assert report.trades == 1 and report.wins == 1 and report.win_rate == 1.0
    assert report.net_pnl == pytest.approx(trade.net_pnl)
    assert report.profit_factor is None  # no losing trade: undefined, not infinite
    assert report.cagr is None and report.sharpe is None


def test_prediction_outcomes_resolve_after_the_horizon_only() -> None:
    tracker = PredictionOutcomeTracker()
    prediction = JEVPrediction(
        prediction_id="P-1",
        symbol="TEST",
        timestamp=SESSION_OPEN + timedelta(minutes=1),
        horizon_minutes=5,
        direction=Direction.LONG,
        probability_up=0.6,
        probability_down=0.4,
        expected_return=0.001,
        expected_volatility=0.002,
        confidence=0.5,
        model_name="m",
        model_version="1",
        feature_version="f",
        feature_id="F-1",
    )
    tracker.add(prediction, 100.0)
    assert tracker.on_bar(make_bar(start=SESSION_OPEN + timedelta(minutes=4), close=101)) == []
    outcomes = tracker.on_bar(make_bar(start=SESSION_OPEN + timedelta(minutes=5), close=101))
    assert len(outcomes) == 1 and outcomes[0].direction_correct is True
    assert outcomes[0].realized_return == pytest.approx(0.01)
    summary = summarize_outcomes(outcomes)
    assert summary["hit_rate"] == 1.0 and summary["brier_score"] == pytest.approx(0.16)
