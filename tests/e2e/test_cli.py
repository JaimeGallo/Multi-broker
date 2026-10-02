"""CLI end to end: simulate -> trace -> verify -> kill-switch, plus the refusals."""

from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from apps.trading_engine.cli import EXIT_FAILED, EXIT_OK, EXIT_REFUSED, main
from packages.common.config import PROJECT_ROOT
from tests.helpers import sqlite_url


def cli(*args: str, db: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = main(["--db", sqlite_url(db), "--log-level", "ERROR", *args], stdout=out)
    text = out.getvalue()
    assert text.isascii(), "console output must be plain ASCII"
    return code, text


@pytest.fixture(scope="module")
def simulated(tmp_path_factory: pytest.TempPathFactory) -> Path:
    db = tmp_path_factory.mktemp("cli") / "cli.db"
    code, text = cli("simulate", "--start", "2024-03-04", "--symbols", "MOCKA", "--run-id", "run_cli", db=db)
    assert code == EXIT_OK, text
    assert "MODE: BACKTEST" in text and "LIVE TRADING DISABLED" in text and "run_cli" in text
    return db


def test_simulate_json_report(tmp_path: Path) -> None:
    code, text = cli(
        "simulate",
        "--start",
        "2024-03-04",
        "--symbols",
        "MOCKB",
        "--model",
        "baseline-flat",
        "--json",
        db=tmp_path / "j.db",
    )
    assert code == EXIT_OK
    payload = json.loads(text)
    assert payload["status"] == "COMPLETED" and payload["broker_submissions"] == 0
    assert payload["model"].startswith("baseline-flat@")


def test_trace_list_and_detail(simulated: Path) -> None:
    code, listing = cli("trace", "--list", "5", "--run-id", "run_cli", db=simulated)
    assert code == EXIT_OK
    first = listing.splitlines()[0].split()[0]
    assert first.startswith("S-MOCKA-")
    code, detail = cli("trace", first, db=simulated)
    trace = json.loads(detail)
    assert code == EXIT_OK and trace["signal"]["signal_id"] == first
    assert trace["features"] is not None and trace["prediction"] is not None and trace["bars"]
    code, missing = cli("trace", "S-UNKNOWN", db=simulated)
    assert code == EXIT_FAILED and "not found" in missing


def test_verify_reproduces_every_decision(simulated: Path) -> None:
    code, text = cli("verify", "--run-id", "run_cli", db=simulated)
    assert code == EXIT_OK, text
    assert "MISMATCH" not in text and "decisions reproduced exactly" in text
    code, text = cli("verify", "S-UNKNOWN", db=simulated)
    assert code == EXIT_FAILED and "NOT FOUND" in text


def test_kill_switch_commands(tmp_path: Path) -> None:
    db = tmp_path / "ks.db"
    assert cli("kill-switch", "status", db=db)[1].count("engaged      False") == 1
    code, text = cli("kill-switch", "engage", "--by", "ana", "--note", "drill", db=db)
    assert code == EXIT_OK and "engaged      True" in text and "MANUAL" in text
    assert cli("kill-switch", "engage", db=db)[0] == EXIT_REFUSED  # needs --by
    assert cli("kill-switch", "reset", "--by", "system", "--note", "x", db=db)[0] == EXIT_REFUSED
    assert cli("kill-switch", "reset", "--by", "ana", db=db)[0] == EXIT_REFUSED  # needs a reason
    code, text = cli("kill-switch", "reset", "--by", "ana", "--note", "drill over", db=db)
    assert code == EXIT_OK and "engaged      False" in text


def test_run_is_not_available_yet_and_live_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    code, text = cli("run", db=tmp_path / "run.db")
    assert code == EXIT_REFUSED and "phase 4" in text
    monkeypatch.setenv("JEV__TRADING__MODE", "live")
    code, text = cli("run", db=tmp_path / "run.db")
    assert code == EXIT_REFUSED and "REFUSED" in text
    monkeypatch.setenv("JEV__TRADING__MODE", "paper")
    monkeypatch.setenv("JEV__BROKER__MODE", "live")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    code, text = cli("simulate", "--start", "2024-03-04", db=tmp_path / "run.db")
    assert code == EXIT_REFUSED and "not implemented" in text


def test_module_entry_point() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "apps.trading_engine", "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0
    for command in ("simulate", "trace", "verify", "kill-switch", "run"):
        assert command in result.stdout
