"""Phase 3 end to end without network: download (fake Alpaca) -> dataset -> backtest -> verify -> experiment -> CLI."""

from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from apps.trading_engine import cli
from apps.trading_engine.bootstrap import SimulationOptions, run_simulation
from apps.trading_engine.experiment import run_experiment
from packages.common.config import AlpacaDataSection
from packages.common.enums import TradingMode
from packages.common.errors import ConfigError
from packages.market_data.alpaca_history import AlpacaHistoricalClient
from packages.market_data.dataset import DatasetStore
from packages.market_data.spreads import calibrate_spreads, write_spreads
from packages.persistence.database import Database
from packages.persistence.models import BacktestTradeRow, FeatureRow, MarketBarRow, PredictionRow
from packages.persistence.repositories import AuditRepository
from packages.pipeline.replay import DecisionVerifier
from tests.fake_alpaca import FakeAlpaca
from tests.helpers import make_config, sqlite_url

START, END = date(2024, 3, 25), date(2024, 4, 5)  # 9 sessions (Good Friday closed), two monthly folds


@pytest.fixture(scope="module")
def dataset_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("datasets")
    fake = FakeAlpaca(page_size=2000)
    cfg = AlpacaDataSection(min_request_interval_seconds=0)
    with AlpacaHistoricalClient(cfg, key="test-key", secret="s", transport=fake.transport()) as client:
        DatasetStore(root).download(
            client, name="sip-test", symbols=["SPY", "AAPL"], start=START, end=END, feed="sip",
            adjustment="split", today=date(2026, 1, 1),
        )  # fmt: skip
    return root


def historical_config(
    root: Path, *, symbols: list[str], model: str = "jev-heuristic", db: Path | None = None
):  # type: ignore[no-untyped-def]
    overrides: dict[str, Any] = {
        "trading": {"mode": "backtest", "symbols": symbols},
        "market_data": {"provider": "historical", "historical": {"root": str(root), "dataset": "sip-test"}},
        "model": {"name": model},
        "logging": {"level": "WARNING"},
    }
    if db is not None:
        overrides["persistence"] = {"database_url": sqlite_url(db)}
    return make_config(overrides)


async def test_backtest_on_a_dataset_is_audited_and_reproducible(dataset_root: Path, tmp_path: Path) -> None:
    db = tmp_path / "hist.db"
    config = historical_config(dataset_root, symbols=["SPY"])
    report = await run_simulation(
        config,
        SimulationOptions(start=date(2024, 4, 1), end=date(2024, 4, 3), run_id="run_hist",
                          database_url=sqlite_url(db), git_commit="t"),
    )  # fmt: skip
    assert report.status == "COMPLETED" and report.result.events == 3 * 390
    assert report.extra["dataset"]["name"] == "sip-test"
    assert report.summary["open_positions"] == {} and not report.summary["kill_switch"]["engaged"]
    assert report.performance.fees > 0  # SEC Section 31 + FINRA TAF on sells are now charged
    database = Database(sqlite_url(db))
    try:
        repo = AuditRepository(database, run_id="v", mode=TradingMode.BACKTEST)
        signals = await repo.list_signals(limit=None, run_id="run_hist")
        assert signals
        verifier = DecisionVerifier(repo)
        assert all([(await verifier.verify_decision(s["signal_id"])).ok for s in signals[:60]])
    finally:
        await database.dispose()


async def test_experiment_compares_models_fold_by_fold(dataset_root: Path, tmp_path: Path) -> None:
    config = historical_config(dataset_root, symbols=["AAPL"], db=tmp_path / "main.db")
    result = await run_experiment(
        config, models=["baseline-ma", "baseline-flat"], workers=1, output_root=tmp_path / "experiments"
    )
    payload = result.payload
    assert [f["name"] for f in payload["folds"]] == ["2024-03", "2024-04"]
    assert {j["status"] for j in payload["jobs"]} == {"COMPLETED"} and len(payload["jobs"]) == 4
    flat, ma = payload["summaries"]["baseline-flat"], payload["summaries"]["baseline-ma"]
    assert flat["trades"] == 0 and flat["net_pnl"] == 0
    assert ma["trades"] > 0 and ma["long"]["trades"] + ma["short"]["trades"] == ma["trades"]
    assert ma["sessions"] == 9 and set(ma["folds"]) == {"2024-03", "2024-04"}
    assert payload["comparisons"]["baseline-flat"]["sessions"] == 9
    on_disk = json.loads((result.output_dir / "results.json").read_text(encoding="utf-8"))
    assert on_disk["dataset"]["version"] == payload["dataset"]["version"]
    assert payload["audit"] == {"level": "lean", "job_databases_kept": False}
    assert [p.name for p in result.output_dir.iterdir()] == ["results.json"]  # job databases deleted
    database = Database(sqlite_url(tmp_path / "main.db"))
    try:
        recorded = await AuditRepository(database, run_id="x", mode=TradingMode.BACKTEST).list_experiments()
        assert recorded[0]["experiment_id"] == result.experiment_id
    finally:
        await database.dispose()


async def test_full_audit_experiment_keeps_verifiable_job_databases(
    dataset_root: Path, tmp_path: Path
) -> None:
    config = historical_config(dataset_root, symbols=["AAPL"], db=tmp_path / "main.db")
    result = await run_experiment(
        config, models=["baseline-ma"], audit="full", start=date(2024, 4, 1), end=date(2024, 4, 5),
        output_root=tmp_path / "experiments",
    )  # fmt: skip
    assert result.payload["audit"] == {"level": "full", "job_databases_kept": True}
    job = result.payload["jobs"][0]
    database = Database(job["database_url"])
    try:
        repo = AuditRepository(database, run_id=job["run_id"], mode=TradingMode.BACKTEST)
        assert await repo.count(MarketBarRow) == 5 * 390
        verifier = DecisionVerifier(repo)
        signals = await repo.list_signals(limit=20, run_id=job["run_id"])
        assert signals and all([(await verifier.verify_decision(s["signal_id"])).ok for s in signals])
    finally:
        await database.dispose()


async def test_lean_job_databases_hold_no_bars(dataset_root: Path, tmp_path: Path) -> None:
    config = historical_config(dataset_root, symbols=["AAPL"], db=tmp_path / "main.db")
    result = await run_experiment(
        config, models=["baseline-ma"], keep_dbs=True, start=date(2024, 4, 1), end=date(2024, 4, 5),
        output_root=tmp_path / "experiments",
    )  # fmt: skip
    job = result.payload["jobs"][0]
    database = Database(job["database_url"])
    try:
        repo = AuditRepository(database, run_id=job["run_id"], mode=TradingMode.BACKTEST)
        assert await repo.count(MarketBarRow) == 0 and await repo.count(FeatureRow) == 0
        assert await repo.count(PredictionRow) == 0 and await repo.count(BacktestTradeRow) > 0
    finally:
        await database.dispose()


async def test_experiment_refuses_to_start_without_disk(
    dataset_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import apps.trading_engine.experiment as experiment

    monkeypatch.setattr(experiment, "free_bytes", lambda path: 512 * 1024**2)
    with pytest.raises(ConfigError, match="not enough disk"):
        await run_experiment(
            historical_config(dataset_root, symbols=["AAPL"], db=tmp_path / "main.db"), models=["baseline-flat"],
            output_root=tmp_path / "experiments",
        )  # fmt: skip
    assert not (tmp_path / "experiments").exists()


def test_cli_data_commands(dataset_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeAlpaca(page_size=2000)

    def fake_client(cfg: AlpacaDataSection, *, key: str, secret: str) -> AlpacaHistoricalClient:
        return AlpacaHistoricalClient(
            cfg, key="test-key", secret=secret, transport=fake.transport(), sleep=lambda s: None
        )

    monkeypatch.setattr(cli, "AlpacaHistoricalClient", fake_client)
    monkeypatch.setenv("APCA_API_KEY_ID", "test-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("JEV__MARKET_DATA__HISTORICAL__ROOT", str(tmp_path / "ds"))
    base = ["--db", sqlite_url(tmp_path / "cli.db"), "--log-level", "ERROR"]

    def run(*args: str) -> tuple[int, str]:
        out = io.StringIO()
        code = cli.main([*base, *args], stdout=out)
        assert out.getvalue().isascii()
        return code, out.getvalue()

    code, text = run(
        "data",
        "download",
        "--name",
        "cli-test",
        "--symbols",
        "SPY",
        "--start",
        "2024-04-01",
        "--end",
        "2024-04-02",
    )
    assert code == 0 and "780 bars" in text
    code, text = run("data", "list")
    assert code == 0 and "cli-test" in text
    code, text = run("data", "info", "cli-test", "--verify")
    assert code == 0 and "hashes match" in text
    code, text = run(
        "data",
        "download",
        "--name",
        "cli-test",
        "--symbols",
        "SPY",
        "--start",
        "2024-04-01",
        "--end",
        "2024-04-02",
    )
    assert code == cli.EXIT_CONFIG and "already exists" in text
    code, text = run("simulate", "--dataset", "cli-test", "--symbols", "SPY", "--model", "baseline-flat")
    assert code == 0 and "dataset           cli-test" in text and "2024-04-02" in text


async def test_measured_spreads_flow_into_costs_runs_and_verify(dataset_root: Path, tmp_path: Path) -> None:
    store = DatasetStore(dataset_root)
    dataset = store.load("sip-test")
    cfg = AlpacaDataSection(min_request_interval_seconds=0)
    with AlpacaHistoricalClient(
        cfg, key="test-key", secret="s", transport=FakeAlpaca().transport()
    ) as client:
        write_spreads(dataset, calibrate_spreads(client, dataset, days=2))
    try:
        db = tmp_path / "spreads.db"
        config = historical_config(dataset_root, symbols=["SPY"])
        report = await run_simulation(
            config,
            SimulationOptions(start=date(2024, 4, 1), end=date(2024, 4, 2), run_id="run_spreads",
                              database_url=sqlite_url(db), git_commit="t"),
        )  # fmt: skip
        assert report.performance.trades > 0
        # a measured SPY spread of ~0.4 bps instead of the 2 bps default: entries pay ~0.8 bps less
        assert report.performance.avg_entry_slippage_bps is not None
        assert report.performance.avg_entry_slippage_bps < 2.0 / 2 + config.costs.slippage_bps
        database = Database(sqlite_url(db))
        try:
            repo = AuditRepository(database, run_id="v", mode=TradingMode.BACKTEST)
            trace = await repo.decision_trace(
                (await repo.list_signals(limit=1, run_id="run_spreads"))[0]["signal_id"]
            )
            assert trace is not None and trace["run"]["config"]["costs"]["spread_by_symbol"][
                "SPY"
            ] == pytest.approx(0.4, rel=0.02)
            verifier = DecisionVerifier(repo)
            signals = await repo.list_signals(limit=40, run_id="run_spreads")
            assert all([(await verifier.verify_decision(s["signal_id"])).ok for s in signals])
        finally:
            await database.dispose()
        lines: list[str] = []
        result = await run_experiment(
            historical_config(dataset_root, symbols=["SPY"], db=tmp_path / "m.db"),
            models=["baseline-flat"], workers=2, output_root=tmp_path / "exp", report=lines.append, report_interval=0,
        )  # fmt: skip
        costs = result.payload["costs"]
        assert costs["spread_calibration"] and costs["spread_by_symbol"]["SPY"] == pytest.approx(
            0.4, rel=0.02
        )
        assert {j["status"] for j in result.payload["jobs"]} == {"COMPLETED"}  # ran in 2 spawned processes
        assert lines and "sessions 9/9" in lines[-1] and "jobs 2/2" in lines[-1]
    finally:
        (dataset.path / "spreads.json").unlink()
