from __future__ import annotations

import io
import json
import logging
from pathlib import Path

import pytest

from packages.common.config import AppConfig, config_hash, load_config
from packages.common.entities import BrokerCapabilities
from packages.common.enums import AssetClass, TradingMode
from packages.common.errors import ConfigError, LiveTradingNotAllowed, SafetyError
from packages.common.logging import RedactingFilter, configure_logging
from packages.common.safety import assert_paper_account, enforce_mode_gate, live_flag_enabled, mode_banner
from packages.common.secrets import load_dotenv, redact_url, sensitive_values
from packages.persistence.repositories import sanitized_config


def test_default_config_loads_and_is_safe(config: AppConfig) -> None:
    assert config.broker.mode == "paper"
    assert config.trading.mode is TradingMode.PAPER
    assert config.execution.use_bracket is True
    assert config_hash(config) == config_hash(load_config(environ={}))


def test_unknown_keys_and_bad_relationships_are_rejected() -> None:
    with pytest.raises(ConfigError):
        load_config(overrides={"risk": {"max_daily_los": 0.01}}, environ={})
    with pytest.raises(ConfigError):
        load_config(overrides={"risk": {"max_symbol_exposure": 0.5, "max_total_exposure": 0.25}}, environ={})
    with pytest.raises(ConfigError):
        load_config(overrides={"trading": {"decision_timeframe": "5Min", "horizon_minutes": 12}}, environ={})


def test_precedence_profile_env_overrides(tmp_path: Path) -> None:
    profile = tmp_path / "profile.yaml"
    profile.write_text("risk:\n  max_daily_loss: 0.03\n  max_drawdown: 0.2\n", encoding="utf-8")
    env = {"JEV__RISK__MAX_DAILY_LOSS": "0.01", "DATABASE_URL": "sqlite+aiosqlite:///x.db"}
    config = load_config(profile, overrides={"risk": {"max_open_positions": 2}}, environ=env)
    assert config.risk.max_daily_loss == 0.01
    assert config.risk.max_drawdown == 0.2
    assert config.risk.max_open_positions == 2
    assert config.persistence.database_url == "sqlite+aiosqlite:///x.db"
    with pytest.raises(ConfigError):
        load_config(tmp_path / "missing.yaml", environ={})


def test_live_trading_is_blocked_even_with_the_flag() -> None:
    enforce_mode_gate(TradingMode.PAPER, "paper", environ={})
    enforce_mode_gate(TradingMode.BACKTEST, "paper", environ={})
    with pytest.raises(LiveTradingNotAllowed, match="LIVE_TRADING_ENABLED"):
        enforce_mode_gate(TradingMode.LIVE, "paper", environ={})
    with pytest.raises(LiveTradingNotAllowed, match="not implemented"):
        enforce_mode_gate(TradingMode.PAPER, "live", environ={"LIVE_TRADING_ENABLED": "true"})
    assert live_flag_enabled({"LIVE_TRADING_ENABLED": "TRUE"})
    assert not live_flag_enabled({})


def test_a_live_account_is_refused_outside_live_mode() -> None:
    capabilities = BrokerCapabilities(
        broker="x",
        is_paper=False,
        asset_classes=(AssetClass.US_EQUITY,),
        supports_short=True,
        supports_fractional=False,
        supports_bracket=True,
        supports_replace=True,
        supports_extended_hours=False,
    )
    with pytest.raises(SafetyError):
        assert_paper_account(capabilities, TradingMode.PAPER)
    assert mode_banner(TradingMode.BACKTEST, "mock", "mock").isascii()


def test_secrets_are_redacted_from_logs() -> None:
    stream = io.StringIO()
    configure_logging("INFO", json_format=True, secrets=["supersecretvalue"], stream=stream)
    logger = logging.getLogger("tests.redaction")
    logger.info("connecting with supersecretvalue", extra={"api_key": "abc", "symbol": "TEST"})
    payload = json.loads(stream.getvalue().strip().splitlines()[-1])
    assert "supersecretvalue" not in stream.getvalue()
    assert payload["msg"] == "connecting with ***"
    assert payload["api_key"] == "***" and payload["symbol"] == "TEST"
    assert RedactingFilter(["abc"]).redact("abc") == "abc"  # values shorter than 4 chars are ignored


def test_secret_helpers(tmp_path: Path) -> None:
    env = {
        "APCA_API_SECRET_KEY": "s3cr3t-value",
        "DATABASE_URL": "postgresql://jev:pa55word@db/jev",
        "OTHER": "x",
    }
    values = sensitive_values(env)
    assert "s3cr3t-value" in values and "pa55word" in values
    assert redact_url("postgresql://jev:pa55word@db/jev") == "postgresql://jev:***@db/jev"
    assert redact_url("sqlite:///x.db") == "sqlite:///x.db"
    dotenv = tmp_path / ".env"
    dotenv.write_text("# comment\nA=1\nB='two'\nEXISTING=new\n", encoding="utf-8")
    target = {"EXISTING": "old"}
    assert load_dotenv(dotenv, target) == ["A", "B"]
    assert target == {"EXISTING": "old", "A": "1", "B": "two"}


def test_stored_config_never_keeps_passwords() -> None:
    config = load_config(environ={"DATABASE_URL": "postgresql+asyncpg://jev:pa55word@db/jev"})
    stored = sanitized_config(config)
    assert "pa55word" not in json.dumps(stored)
