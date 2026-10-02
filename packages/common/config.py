"""Validated configuration.

Precedence: config/default.yaml < profile file < env vars JEV__SECTION__KEY < explicit overrides (CLI).
The schema is strict (`extra="forbid"`): a misspelled key is an error, never a silently ignored value.
Secrets are NOT part of this model; they are read from the environment only (see packages.common.secrets).
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from packages.common.enums import AssetClass, MarketRegime, Timeframe, TimeInForce, TradingMode
from packages.common.errors import ConfigError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "default.yaml"
ENV_PREFIX = "JEV__"


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())


FractionValue = Annotated[float, Field(gt=0.0, le=1.0)]


class AppSection(Section):
    name: str = "jev-trading"
    environment: str = "local"


class TradingSection(Section):
    mode: TradingMode = TradingMode.PAPER
    strategy: str = "jev-intraday-v1"
    symbols: list[str] = Field(default_factory=lambda: ["MOCKA", "MOCKB", "MOCKC"], min_length=1)
    asset_class: AssetClass = AssetClass.US_EQUITY
    timeframe: Timeframe = Timeframe.MIN_1
    decision_timeframe: Timeframe = Timeframe.MIN_1
    horizon_minutes: int = Field(default=15, gt=0)
    exchange_timezone: str = "America/New_York"
    session_open: str = "09:30"
    session_close: str = "16:00"

    @model_validator(mode="after")
    def _check(self) -> TradingSection:
        if self.decision_timeframe.minutes % self.timeframe.minutes != 0:
            raise ValueError("decision_timeframe must be a multiple of timeframe")
        if self.horizon_minutes % self.decision_timeframe.minutes != 0:
            raise ValueError("horizon_minutes must be a multiple of decision_timeframe")
        return self


class SyntheticMarketSection(Section):
    seed: int = 7
    start_price: float = Field(default=100.0, gt=0)
    annual_volatility: float = Field(default=0.35, gt=0)
    base_spread_bps: float = Field(default=2.0, ge=0)
    base_volume: float = Field(default=20_000, gt=0)
    ticks_per_bar: int = Field(default=12, ge=2)
    regime_persistence: float = Field(default=0.985, gt=0, lt=1)
    trend_drift_bps: float = 1.5
    high_vol_multiplier: float = Field(default=2.5, ge=1)
    overnight_gap_bps: float = Field(default=30.0, ge=0)
    gap_rate: float = Field(default=0.0, ge=0, le=1)
    duplicate_rate: float = Field(default=0.0, ge=0, le=1)
    outlier_rate: float = Field(default=0.0, ge=0, le=1)


class AlpacaDataSection(Section):
    """Alpaca market data (historical download in phase 3; real-time stream in phase 4)."""

    feed: Literal["sip", "iex"] = "sip"  # sip = consolidated tape; iex = a single exchange
    adjustment: Literal["raw", "split", "dividend", "all"] = "split"
    data_url: str = "https://data.alpaca.markets"
    trading_url: str = "https://paper-api.alpaca.markets"  # paper only: used for the trading calendar
    page_limit: int = Field(default=10_000, ge=1, le=10_000)
    min_request_interval_seconds: float = Field(default=0.35, ge=0)  # Basic plan: 200 requests/minute


class HistoricalDataSection(Section):
    root: str = "data/datasets"
    dataset: str | None = None  # name of a downloaded dataset (python -m apps.trading_engine data list)


class MarketDataSection(Section):
    provider: Literal["mock", "historical", "alpaca", "ibkr"] = "mock"
    mock: SyntheticMarketSection = Field(default_factory=SyntheticMarketSection)
    alpaca: AlpacaDataSection = Field(default_factory=AlpacaDataSection)
    historical: HistoricalDataSection = Field(default_factory=HistoricalDataSection)


class MockBrokerSection(Section):
    enabled: bool = True
    account_ref: str = "MOCK-PAPER-001"
    initial_cash: float = Field(default=100_000.0, gt=0)
    buying_power_multiplier: float = Field(default=1.0, gt=0)
    partial_fills: bool = False
    participation_rate: float = Field(default=0.10, gt=0, le=1)
    intrabar_policy: Literal["conservative", "optimistic"] = "conservative"
    shortable_symbols: list[str] | None = None


class AlpacaSection(Section):
    enabled: bool = False
    paper: bool = True
    feed: Literal["iex", "sip"] = "iex"


class IBKRSection(Section):
    enabled: bool = False
    paper: bool = True
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17


class BrokerSection(Section):
    mode: Literal["paper", "live"] = "paper"
    active: Literal["mock", "alpaca", "ibkr"] = "mock"
    routing: dict[AssetClass, Literal["mock", "alpaca", "ibkr"]] = Field(default_factory=dict)
    mock: MockBrokerSection = Field(default_factory=MockBrokerSection)
    alpaca: AlpacaSection = Field(default_factory=AlpacaSection)
    ibkr: IBKRSection = Field(default_factory=IBKRSection)


class DataQualitySection(Section):
    max_bar_delay_seconds: float = Field(default=90.0, gt=0)
    max_future_skew_seconds: float = Field(default=5.0, ge=0)
    max_abs_return_sigma: float = Field(default=12.0, gt=0)
    jump_lookback_bars: int = Field(default=20, ge=5)
    max_spread_bps: float = Field(default=50.0, gt=0)
    gap_memory_bars: int = Field(default=15, ge=0)
    block_on_degraded: bool = True


class FeaturesSection(Section):
    version: str = "0.1.0"
    window: int = Field(default=150, gt=0)
    min_bars: int = Field(default=101, gt=0)
    include_microstructure: bool = True


class ModelSection(Section):
    name: str = "jev-heuristic"
    version: str = "0.1.0"
    params: dict[str, Any] = Field(default_factory=dict)


class RegimeSection(Section):
    adx_trend: float = 25.0
    adx_range: float = 20.0
    trend_tstat: float = 2.0
    high_vol_ratio: float = 1.5
    low_vol_ratio: float = 0.6


class SignalsSection(Section):
    min_probability: float = Field(default=0.55, ge=0.5, le=1.0)
    min_confidence: float = Field(default=0.10, ge=0.0, le=1.0)
    min_net_edge_bps: float = 1.0
    max_spread_bps: float = Field(default=15.0, gt=0)
    blocked_regimes: list[MarketRegime] = Field(default_factory=lambda: [MarketRegime.UNKNOWN])
    signal_ttl_seconds: float = Field(default=120.0, gt=0)


class CostsSection(Section):
    default_spread_bps: float = Field(default=2.0, ge=0)
    slippage_bps: float = Field(default=2.0, ge=0)
    commission_per_share: float = Field(default=0.0, ge=0)
    commission_bps: float = Field(default=0.0, ge=0)
    min_commission: float = Field(default=0.0, ge=0)
    sec_fee_rate: float = Field(default=0.0, ge=0)
    taf_per_share: float = Field(default=0.0, ge=0)
    taf_max_per_trade: float = Field(default=0.0, ge=0)
    latency_ms: float = Field(default=250.0, ge=0)
    latency_cost_factor: float = Field(default=0.5, ge=0)
    # Typical quoted spread per symbol (bps), used when no live quote is available. Filled automatically from a
    # dataset's measured spreads (`data spreads`) unless use_measured_spreads is false; explicit values win.
    spread_by_symbol: dict[str, Annotated[float, Field(ge=0)]] = Field(default_factory=dict)
    use_measured_spreads: bool = True


class SizingSection(Section):
    method: Literal[
        "fixed_risk", "fixed_fraction", "volatility_adjusted", "risk_parity", "fractional_kelly"
    ] = "fixed_risk"
    kelly_enabled: bool = False
    kelly_fraction: float = Field(default=0.25, gt=0, le=1)


class RiskSection(Section):
    max_risk_per_trade: FractionValue = 0.005
    max_daily_loss: FractionValue = 0.02
    max_total_exposure: FractionValue = 0.25
    max_open_positions: int = Field(default=5, ge=1)
    max_symbol_exposure: FractionValue = 0.10
    max_drawdown: FractionValue = 0.10
    allow_short: bool = True
    atr_stop_multiple: float = Field(default=2.0, gt=0)
    take_profit_rr: float = Field(default=1.5, gt=0)
    min_risk_reward: float = Field(default=1.0, gt=0)
    min_stop_bps: float = Field(default=10.0, ge=0)
    max_stop_bps: float = Field(default=300.0, gt=0)
    no_entry_first_minutes: int = Field(default=5, ge=0)
    no_entry_last_minutes: int = Field(default=20, ge=0)
    buying_power_usage: FractionValue = 0.95
    sizing: SizingSection = Field(default_factory=SizingSection)

    @model_validator(mode="after")
    def _check(self) -> RiskSection:
        if self.max_symbol_exposure > self.max_total_exposure:
            raise ValueError("max_symbol_exposure cannot exceed max_total_exposure")
        if self.min_stop_bps >= self.max_stop_bps:
            raise ValueError("min_stop_bps must be below max_stop_bps")
        return self


class ExecutionSection(Section):
    entry_order_type: Literal["market"] = "market"
    time_in_force: TimeInForce = TimeInForce.DAY
    # v1 always places broker-side protective legs (stop loss + take profit): no position ever lacks a stop.
    use_bracket: Literal[True] = True
    exit_at_horizon: bool = True
    flatten_minutes_before_close: int = Field(default=5, ge=0)
    ack_timeout_seconds: float = Field(default=15.0, gt=0)
    max_submit_attempts: int = Field(default=2, ge=1)
    max_exit_attempts: int = Field(default=3, ge=1)


class KillSwitchSection(Section):
    stale_data_seconds: float = Field(default=180.0, gt=0)
    broker_disconnect_seconds: float = Field(default=60.0, gt=0)
    max_avg_slippage_bps: float = Field(default=25.0, gt=0)
    slippage_window: int = Field(default=20, ge=1)
    min_fills_for_slippage: int = Field(default=5, ge=1)
    max_decision_latency_ms: float = Field(default=5000.0, gt=0)
    max_consecutive_model_errors: int = Field(default=3, ge=1)
    cancel_entries_on_kill: bool = True
    flatten_on_kill: bool = False


class PersistenceSection(Section):
    database_url: str | None = "sqlite+aiosqlite:///data/jev.db"
    store_bars: bool = True
    store_features: bool = True
    store_predictions: bool = True
    batch_size: int = Field(default=500, ge=1)
    snapshot_every_minutes: int = Field(default=5, ge=1)


class RedisSection(Section):
    url: str | None = None


class LoggingSection(Section):
    level: str = "INFO"
    json_format: bool = True


class AppConfig(Section):
    app: AppSection = Field(default_factory=AppSection)
    trading: TradingSection = Field(default_factory=TradingSection)
    market_data: MarketDataSection = Field(default_factory=MarketDataSection)
    broker: BrokerSection = Field(default_factory=BrokerSection)
    data_quality: DataQualitySection = Field(default_factory=DataQualitySection)
    features: FeaturesSection = Field(default_factory=FeaturesSection)
    model: ModelSection = Field(default_factory=ModelSection)
    regime: RegimeSection = Field(default_factory=RegimeSection)
    signals: SignalsSection = Field(default_factory=SignalsSection)
    costs: CostsSection = Field(default_factory=CostsSection)
    risk: RiskSection = Field(default_factory=RiskSection)
    execution: ExecutionSection = Field(default_factory=ExecutionSection)
    kill_switch: KillSwitchSection = Field(default_factory=KillSwitchSection)
    persistence: PersistenceSection = Field(default_factory=PersistenceSection)
    redis: RedisSection = Field(default_factory=RedisSection)
    logging: LoggingSection = Field(default_factory=LoggingSection)

    @model_validator(mode="after")
    def _check(self) -> AppConfig:
        if self.features.min_bars > self.features.window:
            raise ValueError("features.min_bars cannot exceed features.window")
        return self


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def env_overrides(environ: Mapping[str, str]) -> dict[str, Any]:
    """Translate JEV__SECTION__KEY=value variables into a nested mapping (values parsed as YAML scalars)."""
    result: dict[str, Any] = {}
    for name, raw in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        path = [part.lower() for part in name[len(ENV_PREFIX) :].split("__") if part]
        if not path:
            continue
        cursor = result
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[path[-1]] = yaml.safe_load(raw) if raw != "" else None
    return result


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"config file must contain a mapping: {path}")
    return data


def load_config(
    path: str | Path | None = None,
    *,
    overrides: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
) -> AppConfig:
    env = os.environ if environ is None else environ
    data: dict[str, Any] = _read_yaml(DEFAULT_CONFIG_PATH) if DEFAULT_CONFIG_PATH.exists() else {}
    if path is not None:
        data = deep_merge(data, _read_yaml(Path(path)))
    data = deep_merge(data, env_overrides(env))
    if env.get("DATABASE_URL"):
        data = deep_merge(data, {"persistence": {"database_url": env["DATABASE_URL"]}})
    if overrides:
        data = deep_merge(data, overrides)
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid configuration:\n{exc}") from exc


def config_hash(config: AppConfig) -> str:
    """Stable fingerprint of a configuration (recorded with every run)."""
    payload = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
