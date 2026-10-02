"""Trading cost model.

One model serves two purposes so they can never drift apart:
- ex-ante: the Signal Engine estimates round-trip costs to compute the expected value of a signal;
- ex-post: the simulated broker charges the same fees and slippage on simulated fills.
"""

from __future__ import annotations

import math

from packages.common.config import CostsSection
from packages.common.entities import CostEstimate
from packages.common.enums import Side


class CostModel:
    def __init__(self, config: CostsSection) -> None:
        self.config = config

    def fees(self, side: Side, quantity: float, price: float) -> float:
        """Commission plus sell-side regulatory fees for one fill, in currency."""
        if quantity <= 0:
            return 0.0
        cfg = self.config
        notional = quantity * price
        commission = cfg.commission_per_share * quantity + notional * cfg.commission_bps / 1e4
        commission = max(commission, cfg.min_commission)
        regulatory = 0.0
        if side is Side.SELL:
            regulatory += notional * cfg.sec_fee_rate
            taf = cfg.taf_per_share * quantity
            if cfg.taf_max_per_trade > 0:
                taf = min(taf, cfg.taf_max_per_trade)
            regulatory += taf
        return commission + regulatory

    def spread_bps(self, quote_spread_bps: float | None) -> float:
        if quote_spread_bps is None or not math.isfinite(quote_spread_bps):
            return self.config.default_spread_bps
        return max(0.0, quote_spread_bps)

    def execution_price(self, side: Side, reference: float, quote_spread_bps: float | None) -> float:
        """Adverse execution price for a marketable order: half spread plus slippage (not tick-rounded)."""
        adverse_bps = self.spread_bps(quote_spread_bps) / 2.0 + self.config.slippage_bps
        return reference * (1.0 + side.sign * adverse_bps / 1e4)

    def estimate_round_trip(
        self,
        *,
        price: float,
        quote_spread_bps: float | None,
        volatility_per_bar: float,
        bar_seconds: float,
    ) -> CostEstimate:
        cfg = self.config
        commission_bps = (
            2.0 * (cfg.commission_per_share / price * 1e4 + cfg.commission_bps) if price > 0 else 0.0
        )
        regulatory_bps = cfg.sec_fee_rate * 1e4 + (cfg.taf_per_share / price * 1e4 if price > 0 else 0.0)
        vol = volatility_per_bar if math.isfinite(volatility_per_bar) and volatility_per_bar > 0 else 0.0
        latency_fraction = math.sqrt((cfg.latency_ms / 1000.0) / bar_seconds) if bar_seconds > 0 else 0.0
        latency_bps = 2.0 * cfg.latency_cost_factor * vol * 1e4 * latency_fraction
        return CostEstimate.of(
            spread_bps=self.spread_bps(quote_spread_bps),
            slippage_bps=2.0 * cfg.slippage_bps,
            commission_bps=commission_bps,
            regulatory_bps=regulatory_bps,
            latency_bps=latency_bps,
        )
