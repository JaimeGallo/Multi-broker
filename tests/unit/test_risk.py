from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from packages.common.calendar import RegularHoursCalendar
from packages.common.config import RiskSection, SizingSection
from packages.common.entities import AccountSnapshot, BrokerCapabilities, InstrumentInfo, Signal
from packages.common.enums import AssetClass, Direction, MarketRegime, Side, SignalStatus
from packages.common.errors import ConfigError
from packages.risk.engine import RiskContext, StandardRiskEngine
from packages.risk.sizing import FixedRiskSizer, SizingInput, build_sizer
from tests.helpers import SESSION_OPEN

CALENDAR = RegularHoursCalendar()
NOW = SESSION_OPEN + timedelta(hours=1)
CONFIG = RiskSection()


def signal(direction: Direction = Direction.LONG, *, symbol: str = "TEST") -> Signal:
    return Signal(
        signal_id="S-TEST-1",
        prediction_id="P-1",
        symbol=symbol,
        timestamp=NOW,
        direction=direction,
        probability=0.7,
        confidence=0.6,
        expected_return=0.002,
        expected_volatility=0.004,
        horizon_minutes=15,
        market_regime=MarketRegime.TRENDING_UP,
        model_name="m",
        model_version="1",
        feature_version="f",
        reference_price=100.0,
        status=SignalStatus.ELIGIBLE,
        expires_at=NOW + timedelta(minutes=2),
        strategy="s",
        updated_at=NOW,
    )


def context(**changes: object) -> RiskContext:
    account = AccountSnapshot(
        broker="mock",
        account_ref="***0001",
        is_paper=True,
        cash=100_000,
        equity=100_000,
        buying_power=100_000,
        last_equity=100_000,
        timestamp=NOW,
    )
    base = RiskContext(
        now=NOW,
        account=account,
        positions={},
        pending_entry_symbols=frozenset(),
        gross_exposure=0.0,
        instrument=InstrumentInfo(symbol="TEST"),
        broker_capabilities=BrokerCapabilities(
            broker="mock",
            is_paper=True,
            asset_classes=(AssetClass.US_EQUITY,),
            supports_short=True,
            supports_fractional=False,
            supports_bracket=True,
            supports_replace=True,
            supports_extended_hours=False,
        ),
        atr=0.2,
        reference_price=100.0,
        peak_equity=100_000,
        session=CALENDAR.session_for(NOW),
        kill_switch_engaged=False,
        trading_paused=False,
        health_ok=True,
    )
    return replace(base, **changes)  # type: ignore[arg-type]


ENGINE = StandardRiskEngine(CONFIG, FixedRiskSizer())


def test_approves_and_sizes_by_fixed_risk() -> None:
    decision = ENGINE.evaluate(signal(), context())
    assert decision.approved, decision.reasons
    assert decision.side is Side.BUY
    assert decision.stop_loss == pytest.approx(99.6) and decision.take_profit == pytest.approx(100.6)
    # risk cap = 100_000 * 0.005 / 0.4 = 1250 shares; symbol exposure cap = 10_000 / 100 = 100 shares binds
    assert decision.quantity == 100
    assert decision.max_loss is not None and decision.max_loss <= 100_000 * CONFIG.max_risk_per_trade
    assert all(check.passed for check in decision.checks)


def test_short_brackets_are_mirrored() -> None:
    decision = ENGINE.evaluate(signal(Direction.SHORT), context())
    assert decision.approved and decision.side is Side.SELL
    assert decision.take_profit is not None and decision.stop_loss is not None
    assert decision.take_profit < 100.0 < decision.stop_loss


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"kill_switch_engaged": True}, "kill_switch"),
        ({"trading_paused": True}, "trading_not_paused"),
        ({"health_ok": False}, "system_health"),
        ({"now": SESSION_OPEN + timedelta(minutes=2)}, "entry_window"),
        ({"session": None}, "market_open"),
        ({"atr": None}, "atr_available"),
        ({"positions": {"TEST": 10.0}}, "no_position_in_symbol"),
        ({"pending_entry_symbols": frozenset({"A", "B", "C", "D", "E"})}, "max_open_positions"),
        ({"peak_equity": 120_000}, "max_drawdown"),
        ({"instrument": InstrumentInfo(symbol="TEST", tradable=False)}, "instrument_tradable"),
    ],
)
def test_rejections(changes: dict[str, object], reason: str) -> None:
    decision = ENGINE.evaluate(signal(), context(**changes))
    assert not decision.approved
    assert reason in decision.reasons
    assert decision.quantity == 0.0


def test_daily_loss_limit() -> None:
    ctx = context()
    losing = replace(ctx, account=ctx.account.model_copy(update={"equity": 97_000.0}), peak_equity=97_000)
    assert "daily_loss_limit" in ENGINE.evaluate(signal(), losing).reasons


def test_shorts_can_be_disabled() -> None:
    engine = StandardRiskEngine(RiskSection(allow_short=False), FixedRiskSizer())
    assert "short_allowed" in engine.evaluate(signal(Direction.SHORT), context()).reasons
    not_borrowable = context(instrument=InstrumentInfo(symbol="TEST", shortable=False))
    assert "instrument_shortable" in ENGINE.evaluate(signal(Direction.SHORT), not_borrowable).reasons


def test_expired_signal_is_rejected() -> None:
    late = context(now=NOW + timedelta(minutes=5))
    assert "signal_not_expired" in ENGINE.evaluate(signal(), late).reasons


def test_sizer_and_factory() -> None:
    sizer = FixedRiskSizer()
    assert sizer.size(SizingInput(100_000, 50.0, 0.5, 0.005, 1.0)) == 1000
    assert sizer.size(SizingInput(100_000, 50.0, 0.0, 0.005, 1.0)) == 0
    assert build_sizer(SizingSection()).name == "fixed_risk"
    with pytest.raises(ConfigError):
        build_sizer(SizingSection(method="fractional_kelly"))
