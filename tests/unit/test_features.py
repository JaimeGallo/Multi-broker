"""Feature engine: no look-ahead, determinism and window invariance (docs/ARCHITECTURE.md)."""

from __future__ import annotations

import math

import pytest

from packages.common.calendar import RegularHoursCalendar
from packages.common.entities import FeatureVector
from packages.features.engine import FeatureEngine
from packages.features.spec import FeatureSpec
from tests.helpers import make_quote, random_walk_bars

CALENDAR = RegularHoursCalendar()
SPEC = FeatureSpec()


def engine(namespace: str = "ns") -> FeatureEngine:
    return FeatureEngine(SPEC, namespace=namespace, day_start=CALENDAR.day_start)


def same_values(a: FeatureVector, b: FeatureVector) -> bool:
    if set(a.values) != set(b.values):
        return False
    return all(
        (math.isnan(a.values[k]) and math.isnan(b.values[k])) or a.values[k] == b.values[k] for k in a.values
    )


def test_warmup_until_min_bars() -> None:
    fe = engine()
    bars = random_walk_bars(SPEC.min_bars)
    outputs = [fe.update(bar) for bar in bars]
    assert all(out is None for out in outputs[:-1])
    assert outputs[-1] is not None
    assert outputs[-1].n_bars == SPEC.min_bars


def test_no_look_ahead_future_bars_never_change_past_features() -> None:
    bars = random_walk_bars(250)
    streamed = engine()
    by_time = {}
    for bar in bars:
        vector = streamed.update(bar)
        if vector is not None:
            by_time[vector.timestamp] = vector
    for cut in (SPEC.min_bars, 160, 249):
        truncated = engine().compute(bars[:cut])
        assert same_values(truncated, by_time[truncated.timestamp])
        assert truncated.window_end == bars[cut - 1].end


def test_determinism_same_inputs_same_vector_and_id() -> None:
    bars = random_walk_bars(180)
    quote = make_quote(at=bars[-1].end)
    first, second = engine().compute(bars, quote), engine().compute(bars, quote)
    assert first.feature_id == second.feature_id
    assert same_values(first, second)


def test_namespace_scopes_feature_ids() -> None:
    bars = random_walk_bars(150)
    assert engine("a").compute(bars).feature_id != engine("b").compute(bars).feature_id


def test_window_invariance_older_history_is_irrelevant() -> None:
    bars = random_walk_bars(300)
    full = engine().compute(bars)
    window_only = engine().compute(bars[-SPEC.window :])
    assert full.n_bars == SPEC.window
    assert same_values(full, window_only)


def test_feature_names_match_and_microstructure_needs_a_quote() -> None:
    bars = random_walk_bars(150)
    without_quote = engine().compute(bars)
    assert sorted(without_quote.values) == sorted(SPEC.feature_names())
    assert math.isnan(without_quote.values["spread_bps"])
    with_quote = engine().compute(bars, make_quote(at=bars[-1].end))
    assert with_quote.values["spread_bps"] == pytest.approx(2.0, rel=1e-3)
    assert with_quote.values["bid_ask_imbalance"] == pytest.approx(0.25)


def test_spec_rejects_too_few_bars() -> None:
    with pytest.raises(ValueError):
        FeatureSpec(min_bars=50, window=150)
    assert FeatureSpec().spec_hash() == FeatureSpec().spec_hash()
    assert FeatureSpec(window=160).spec_hash() != FeatureSpec().spec_hash()
