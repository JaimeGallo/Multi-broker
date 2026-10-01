"""Feature set definition. Changing any definition requires a new `version`; `spec_hash` records the params."""

from __future__ import annotations

import hashlib

from pydantic import BaseModel, ConfigDict, model_validator

from packages.common.config import FeaturesSection

FEATURE_VERSION = "0.1.0"


class FeatureSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: str = FEATURE_VERSION
    window: int = 150
    min_bars: int = 101
    include_microstructure: bool = True
    rsi_period: int = 14
    atr_period: int = 14
    adx_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    roc_period: int = 10
    momentum_period: int = 10
    sma_periods: tuple[int, ...] = (20, 50)
    ema_periods: tuple[int, ...] = (12, 26)
    return_horizons: tuple[int, ...] = (5, 15, 30)
    std_short: int = 20
    std_long: int = 100
    realized_vol_window: int = 30
    trend_window: int = 30
    volume_window: int = 20

    @classmethod
    def from_config(cls, config: FeaturesSection) -> FeatureSpec:
        return cls(
            version=config.version,
            window=config.window,
            min_bars=config.min_bars,
            include_microstructure=config.include_microstructure,
        )

    def required_bars(self) -> int:
        return max(
            self.std_long + 1,
            2 * self.adx_period + 1,
            self.macd_slow + self.macd_signal,
            max(self.sma_periods),
            max(self.ema_periods),
            max(self.return_horizons) + 1,
            self.realized_vol_window + 1,
            self.trend_window,
            self.volume_window + 1,
            self.momentum_period + 1,
        )

    @model_validator(mode="after")
    def _check(self) -> FeatureSpec:
        if self.min_bars < self.required_bars():
            raise ValueError(f"min_bars must be >= {self.required_bars()} for this feature set")
        if self.window < self.min_bars:
            raise ValueError("window must be >= min_bars")
        return self

    def spec_hash(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode("utf-8")).hexdigest()[:12]

    def feature_names(self) -> list[str]:
        names = ["ret_1", "log_ret_1", *(f"ret_{k}" for k in self.return_horizons), "range_pct", "gap"]
        names += [
            f"rsi_{self.rsi_period}",
            "macd_norm",
            "macd_signal_norm",
            "macd_hist_norm",
            f"roc_{self.roc_period}",
            f"momentum_{self.momentum_period}_atr",
        ]
        names += [f"sma_{p}_dist" for p in self.sma_periods] + [f"ema_{p}_dist" for p in self.ema_periods]
        names += [
            f"adx_{self.adx_period}",
            f"plus_di_{self.adx_period}",
            f"minus_di_{self.adx_period}",
            f"trend_strength_{self.trend_window}",
        ]
        names += [
            f"atr_{self.atr_period}",
            f"atr_{self.atr_period}_pct",
            f"rolling_std_{self.std_short}",
            f"realized_vol_{self.realized_vol_window}",
            f"vol_ratio_{self.std_short}_{self.std_long}",
        ]
        names += [f"relative_volume_{self.volume_window}", f"volume_zscore_{self.volume_window}", "vwap_dist"]
        if self.include_microstructure:
            names += ["spread_bps", "bid_ask_imbalance"]
        return names
