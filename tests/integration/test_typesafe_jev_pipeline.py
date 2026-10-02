"""TypeSafe Jev through the whole pipeline, with a fake API: record once, replay offline, verify every decision."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from apps.trading_engine.bootstrap import SimulationOptions, run_simulation
from apps.trading_engine.cli import EXIT_OK, main
from packages.common.enums import TradingMode
from packages.jev import typesafe
from packages.persistence.database import Database
from packages.persistence.repositories import AuditRepository
from packages.pipeline.replay import DecisionVerifier
from tests.helpers import SESSION_DAY, sim_config, sqlite_url
from tests.unit.test_typesafe_jev import FakeJev

PROFILE = Path(__file__).resolve().parents[2] / "config" / "profiles" / "typesafe-jev.yaml"


def jev_config(cache: Path, *, offline: bool = False):  # type: ignore[no-untyped-def]
    params = {"api_model": "jev-1.13.0", "cache_path": str(cache), "offline": offline}
    return sim_config(["MOCKA"], {"model": {"name": "typesafe-jev", "params": params}})


async def test_record_then_replay_offline_and_verify(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeJev()
    monkeypatch.setattr(typesafe, "CLIENT_FACTORY", lambda runtime: fake)
    cache = tmp_path / "answers.jsonl"
    db = tmp_path / "jev.db"

    live = await run_simulation(
        jev_config(cache),
        SimulationOptions(
            start=SESSION_DAY, end=SESSION_DAY, run_id="run_jev", database_url=sqlite_url(db), git_commit="t"
        ),
    )
    usage = live.extra["jev_usage"]
    assert live.status == "COMPLETED" and usage["api_calls"] == len(fake.calls) > 0
    assert usage["estimated_cost_usd"] == pytest.approx(usage["input_tokens"] * 0.042 / 1e6)
    assert live.summary["counters"]["predictions"] == usage["api_calls"]

    offline = await run_simulation(
        jev_config(cache, offline=True),
        SimulationOptions(
            start=SESSION_DAY,
            end=SESSION_DAY,
            run_id="run_jev",
            database_url=sqlite_url(tmp_path / "again.db"),
            git_commit="t",
        ),
    )
    assert offline.extra["jev_usage"]["api_calls"] == 0 and len(fake.calls) == usage["api_calls"]
    assert offline.summary["counters"] == live.summary["counters"]
    assert offline.performance.net_pnl == live.performance.net_pnl

    database = Database(sqlite_url(db))
    try:
        repo = AuditRepository(database, run_id="verifier", mode=TradingMode.BACKTEST)
        verifier = DecisionVerifier(repo)
        signals = await repo.list_signals(limit=None, run_id="run_jev")
        assert signals
        results = [await verifier.verify_decision(row["signal_id"]) for row in signals]
        assert all(r.ok for r in results), [r.mismatches for r in results if not r.ok][:3]
    finally:
        await database.dispose()
    assert len(fake.calls) == usage["api_calls"]  # verify replayed from the cache, never from the API


def test_jev_check_command(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = FakeJev()
    monkeypatch.setattr(typesafe, "CLIENT_FACTORY", lambda runtime: fake)
    out = io.StringIO()
    code = main(
        [
            "--config",
            str(PROFILE),
            "--db",
            sqlite_url(tmp_path / "x.db"),
            "--log-level",
            "ERROR",
            "jev-check",
        ],
        stdout=out,
    )
    text = out.getvalue()
    assert code == EXIT_OK, text
    assert "decision" in text and "input tokens" in text and len(fake.calls) == 1
    refused = io.StringIO()
    assert main(["--db", sqlite_url(tmp_path / "x.db"), "jev-check"], stdout=refused) != EXIT_OK
    assert "typesafe-jev.yaml" in refused.getvalue()
