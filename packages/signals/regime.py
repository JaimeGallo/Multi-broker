"""Market regime classification from features (pure function, replayable).

No regime is assumed to be profitable: whether any regime helps must be shown by backtests. UNKNOWN is the
honest answer when the evidence is ambiguous, and it blocks trading by default.
"""

from __future__ import annotations

import math

from packages.common.config import RegimeSection
from packages.common.entities import FeatureVector, RegimeAssessment
from packages.common.enums import MarketRegime

ADX = "adx_14"
TREND = "trend_strength_30"
VOL_RATIO = "vol_ratio_20_100"


class RegimeEngine:
    REQUIRED = (ADX, TREND, VOL_RATIO)

    def __init__(self, config: RegimeSection) -> None:
        self._cfg = config

    def classify(self, features: FeatureVector) -> RegimeAssessment:
        adx, trend, vol_ratio = (features.get(name) for name in self.REQUIRED)
        cfg = self._cfg

        def result(regime: MarketRegime, reason: str) -> RegimeAssessment:
            return RegimeAssessment(
                symbol=features.symbol,
                timestamp=features.timestamp,
                regime=regime,
                adx=adx if math.isfinite(adx) else None,
                trend_strength=trend if math.isfinite(trend) else None,
                volatility_ratio=vol_ratio if math.isfinite(vol_ratio) else None,
                reason=reason,
            )

        if not all(math.isfinite(x) for x in (adx, trend, vol_ratio)):
            return result(MarketRegime.UNKNOWN, "insufficient_features")
        if vol_ratio >= cfg.high_vol_ratio:
            return result(MarketRegime.HIGH_VOLATILITY, f"vol_ratio={vol_ratio:.2f}")
        if adx >= cfg.adx_trend and trend >= cfg.trend_tstat:
            return result(MarketRegime.TRENDING_UP, f"adx={adx:.1f} t={trend:.1f}")
        if adx >= cfg.adx_trend and trend <= -cfg.trend_tstat:
            return result(MarketRegime.TRENDING_DOWN, f"adx={adx:.1f} t={trend:.1f}")
        if vol_ratio <= cfg.low_vol_ratio:
            return result(MarketRegime.LOW_VOLATILITY, f"vol_ratio={vol_ratio:.2f}")
        if adx < cfg.adx_range:
            return result(MarketRegime.RANGE, f"adx={adx:.1f}")
        return result(MarketRegime.UNKNOWN, "ambiguous")
