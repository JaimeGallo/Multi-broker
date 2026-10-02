"""End-to-end simulation on SQLite: audit chain, flat book at the close, determinism, replay, controls."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from apps.trading_engine.bootstrap import SimulationOptions, SimulationReport, run_simulation
from packages.common.enums import TradingMode
from packages.persistence.database import Database
from packages.persistence.repositories import AuditRepository
from packages.pipeline.replay import DecisionVerifier
from tests.helpers import SESSION_DAY, sim_config, sqlite_url

SYMBOLS = ["MOCKA", "MOCKB"]
RUN_ID = "run_integration"


def simulate(
    db: Path,
    *,
    symbols: list[str] = SYMBOLS,
    run_id: str = RUN_ID,
    extra: dict[str, Any] | None = None,
    days: int = 1,
) -> SimulationReport:
    config = sim_config(symbols, extra)
    options = SimulationOptions(
        start=SESSION_DAY,
        end=SESSION_DAY + timedelta(days=days - 1),
        run_id=run_id,
        database_url=sqlite_url(db),
        git_commit="test",
    )
    return asyncio.run(run_simulation(config, options))


def rows(db: Path, query: str, *params: Any) -> list[sqlite3.Row]:
    with sqlite3.connect(db) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(query, params).fetchall()


@pytest.fixture(scope="module")
def sim(tmp_path_factory: pytest.TempPathFactory) -> tuple[SimulationReport, Path]:
    db = tmp_path_factory.mktemp("sim") / "jev.db"
    return simulate(db), db


def test_simulation_completes_and_trades(sim: tuple[SimulationReport, Path]) -> None:
    report, _ = sim
    counters = report.summary["counters"]
    assert report.status == "COMPLETED" and report.result.completed
    assert report.result.events == 4 * 390  # 2 symbols x (bar + quote) per minute
    assert counters["bars"] == 2 * 390 and counters["rejected_bars"] == 0
    assert counters["entries_submitted"] > 0 and report.performance.trades > 0
    assert counters["no_trade"]["warmup"] > 0  # NO_TRADE is a normal outcome
    assert not report.summary["kill_switch"]["engaged"]


def test_full_audit_chain_for_every_order(sim: tuple[SimulationReport, Path]) -> None:
    _, db = sim
    orders = rows(db, "select * from orders where run_id = ?", RUN_ID)
    assert orders
    for order in orders:
        assert order["signal_id"], order["client_order_id"]
        signal = rows(db, "select * from signals where signal_id = ?", order["signal_id"])[0]
        prediction = rows(
            db, "select * from model_predictions where prediction_id = ?", signal["prediction_id"]
        )[0]
        feature = rows(db, "select * from features where feature_id = ?", prediction["feature_id"])[0]
        bars = rows(
            db,
            "select count(*) n from market_bars where symbol = ? and start >= ? and start < ?",
            feature["symbol"], feature["window_start"], feature["window_end"],
        )  # fmt: skip
        assert bars[0]["n"] == feature["n_bars"]
        decision = rows(db, "select * from risk_decisions where signal_id = ?", signal["signal_id"])[0]
        assert decision["verdict"] == "APPROVED"
        events = rows(db, "select * from order_events where client_order_id = ?", order["client_order_id"])
        if order["status"] in ("FILLED", "CANCELLED"):
            assert events, f"no broker events for {order['client_order_id']}"
    entries = rows(
        db, "select * from orders where intent = 'entry' and status = 'FILLED' and run_id = ?", RUN_ID
    )
    trades = rows(db, "select * from backtest_trades where run_id = ?", RUN_ID)
    assert {t["signal_id"] for t in trades} == {e["signal_id"] for e in entries}


def test_flat_book_and_no_working_orders_at_the_close(sim: tuple[SimulationReport, Path]) -> None:
    report, db = sim
    assert report.summary["open_positions"] == {} and report.summary["open_trades"] == 0
    working = rows(db, "select * from orders where status not in ('FILLED','CANCELLED','REJECTED','EXPIRED')")
    assert working == []
    exits = {r["intent"] for r in rows(db, "select distinct intent from orders where intent != 'entry'")}
    assert exits <= {"take_profit", "stop_loss", "time_exit", "eod_exit", "risk_exit"}


def test_run_metadata_is_recorded(sim: tuple[SimulationReport, Path]) -> None:
    report, db = sim
    run = rows(db, "select * from engine_runs where run_id = ?", RUN_ID)[0]
    assert run["status"] == "COMPLETED" and run["mode"] == "backtest" and run["git_commit"] == "test"
    backtest = rows(db, "select * from backtest_runs where run_id = ?", RUN_ID)[0]
    assert backtest["status"] == "COMPLETED" and backtest["metrics"] is not None
    model = rows(db, "select * from model_versions where model_name = 'jev-heuristic'")[0]
    assert model["params"] is not None
    snapshots = rows(db, "select count(*) n from portfolio_snapshots where run_id = ?", RUN_ID)[0]["n"]
    assert snapshots > 0
    assert report.performance.execution_shortfall == pytest.approx(
        report.performance.model_pnl - report.performance.net_pnl
    )


async def test_every_decision_is_reproduced_exactly(sim: tuple[SimulationReport, Path]) -> None:
    _, db_path = sim
    database = Database(sqlite_url(db_path))
    try:
        repo = AuditRepository(database, run_id="verifier", mode=TradingMode.BACKTEST)
        signals = await repo.list_signals(limit=None, run_id=RUN_ID)
        assert signals
        verifier = DecisionVerifier(repo)
        failures = []
        for row in signals:
            result = await verifier.verify_decision(row["signal_id"])
            if not result.ok:
                failures.append((row["signal_id"], result.mismatches))
        assert failures == []
        missing = await verifier.verify_decision("S-NOPE")
        assert not missing.found and not missing.ok
    finally:
        await database.dispose()


def test_determinism_same_feed_same_decisions(sim: tuple[SimulationReport, Path], tmp_path: Path) -> None:
    first_report, first_db = sim
    second_report = simulate(tmp_path / "again.db")
    second_db = tmp_path / "again.db"
    order_query = "select client_order_id, status, filled_quantity, average_fill_price, quantity from orders order by client_order_id"
    signal_query = "select signal_id, status, status_reason from signals order by signal_id"
    trade_query = "select trade_id, net_pnl, exit_reason from backtest_trades order by trade_id"
    for query in (order_query, signal_query, trade_query):
        assert [tuple(r) for r in rows(first_db, query)] == [tuple(r) for r in rows(second_db, query)]
    assert first_report.summary["counters"] == second_report.summary["counters"]
    assert first_report.performance.net_pnl == second_report.performance.net_pnl


def test_flat_model_never_sends_an_order(tmp_path: Path) -> None:
    db = tmp_path / "flat.db"
    report = simulate(db, symbols=["MOCKA"], run_id="run_flat", extra={"model": {"name": "baseline-flat"}})
    assert report.broker_submissions == 0
    assert report.summary["counters"]["predictions"] > 0
    assert rows(db, "select count(*) n from orders")[0]["n"] == 0
    assert rows(db, "select count(*) n from signals")[0]["n"] == 0


def test_five_minute_decisions_are_aggregated(tmp_path: Path) -> None:
    db = tmp_path / "agg.db"
    extra = {"trading": {"decision_timeframe": "5Min"}, "features": {"window": 110, "min_bars": 101}}
    report = simulate(db, symbols=["MOCKA"], run_id="run_5m", extra=extra, days=2)
    assert report.status == "COMPLETED"
    assert rows(db, "select count(*) n from market_bars where timeframe = '5Min'")[0]["n"] == 2 * 78
    features = rows(db, "select timeframe, count(*) n from features group by timeframe")
    assert [(r["timeframe"], r["n"]) for r in features] == [("5Min", 2 * 78 - 101 + 1)]
    assert report.summary["counters"]["predictions"] == 2 * 78 - 101 + 1


def test_anomalous_feed_is_flagged_not_traded_blindly(tmp_path: Path) -> None:
    db = tmp_path / "noisy.db"
    extra = {"market_data": {"mock": {"duplicate_rate": 0.02, "gap_rate": 0.02, "outlier_rate": 0.01}}}
    report = simulate(db, symbols=["MOCKA"], run_id="run_noisy", extra=extra)
    counters = report.summary["counters"]
    assert counters["rejected_bars"] > 0 and counters["quality"].get("duplicate", 0) > 0
    assert counters["no_trade"].get("data_degraded", 0) > 0
    degraded = rows(db, "select count(*) n from market_bars where quality_status = 'DEGRADED'")[0]["n"]
    assert degraded > 0
