"""Command line interface: `python -m apps.trading_engine <command>`.

Commands: simulate, trace, verify, kill-switch, run, jev-check, data, experiment, experiments. Console output is plain ASCII; logs go to stderr.
Exit codes: 0 ok, 1 verification failed / not found, 2 refused or not available, 3 configuration or data error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any, TextIO

from apps.trading_engine.bootstrap import (
    CONTROLS_KEY,
    KILL_SWITCH_KEY,
    SimulationOptions,
    SimulationReport,
    build_calendar,
    load_dataset,
    run_simulation,
)
from apps.trading_engine.experiment import JobResult, run_experiment
from packages.common.clock import SystemClock
from packages.common.config import AppConfig, load_config
from packages.common.enums import TradingMode
from packages.common.errors import ConfigError, DataError, LiveTradingNotAllowed, SafetyError
from packages.common.logging import configure_logging
from packages.common.safety import enforce_mode_gate, mode_banner
from packages.common.secrets import load_dotenv, redact_url, sensitive_values
from packages.features.engine import FeatureEngine
from packages.features.spec import FeatureSpec
from packages.jev.registry import available_models
from packages.jev.typesafe import SdkJevClient, TypeSafeJEVModel
from packages.market_data.alpaca_history import AlpacaHistoricalClient, credentials
from packages.market_data.dataset import DatasetStore
from packages.market_data.synthetic import SyntheticMarket
from packages.persistence.database import Database
from packages.persistence.repositories import AuditRepository
from packages.pipeline.replay import DecisionVerifier, VerificationResult
from packages.risk.kill_switch import KillSwitch, KillSwitchReason, KillSwitchState

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_REFUSED = 2
EXIT_CONFIG = 3
CLI_ACTOR_RUN = "cli"


def _ascii(text: str) -> str:
    return text.encode("ascii", "replace").decode("ascii")


class Console:
    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def line(self, text: str = "") -> None:
        self._stream.write(_ascii(text) + "\n")

    def json(self, payload: Any) -> None:
        self.line(json.dumps(payload, indent=2, sort_keys=True, default=str, ensure_ascii=True))


# --------------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m apps.trading_engine", description="JEV trading engine")
    parser.add_argument("--config", help="profile YAML merged over config/default.yaml")
    parser.add_argument("--db", help="database URL (overrides persistence.database_url / DATABASE_URL)")
    parser.add_argument("--log-level", default=None, help="log level (default: logging.level from config)")
    sub = parser.add_subparsers(dest="command", required=True)

    sim = sub.add_parser(
        "simulate", help="run the full pipeline (backtest) on the synthetic market or a dataset"
    )
    sim.add_argument("--dataset", help="replay a downloaded dataset instead of the synthetic market")
    sim.add_argument(
        "--start", type=date.fromisoformat, help="first day (YYYY-MM-DD; default: dataset start)"
    )
    sim.add_argument("--end", type=date.fromisoformat, help="last day (default: --start, or dataset end)")
    sim.add_argument("--symbols", help="comma separated symbols (default: trading.symbols)")
    sim.add_argument("--model", help=f"model name ({', '.join(available_models())})")
    sim.add_argument("--run-id", help="explicit run id")
    sim.add_argument("--max-events", type=int, help="stop after N market events (simulated crash)")
    sim.add_argument("--pace", type=float, default=0.0, help="seconds to sleep between bars (visual runs)")
    sim.add_argument("--json", action="store_true", help="print the report as JSON")

    trace = sub.add_parser("trace", help="show the audit chain of one decision")
    trace.add_argument("signal_id", nargs="?")
    trace.add_argument("--list", type=int, metavar="N", help="list the N most recent signals instead")
    trace.add_argument("--run-id", help="with --list: only signals of this run")

    verify = sub.add_parser("verify", help="replay decisions from the audit trail and compare")
    verify.add_argument("signal_ids", nargs="*")
    verify.add_argument("--run-id", help="verify every signal of this run")
    verify.add_argument("--limit", type=int, help="with --run-id: at most N signals")
    verify.add_argument("--json", action="store_true")

    kill = sub.add_parser("kill-switch", help="inspect, engage or reset the kill switch")
    kill.add_argument("action", choices=["status", "engage", "reset"])
    kill.add_argument("--by", help="who acts (required for engage/reset)")
    kill.add_argument("--note", default="", help="reason (required for reset)")
    kill.add_argument("--run-id", help="act on a backtest run's switch instead of the paper/shadow switch")

    sub.add_parser("run", help="paper trading against a real broker (phase 4)")

    data = sub.add_parser("data", help="download and inspect historical datasets (Alpaca)")
    data_sub = data.add_subparsers(dest="data_command", required=True)
    download = data_sub.add_parser("download", help="download 1Min bars into a versioned local dataset")
    download.add_argument("--name", required=True, help="dataset name, e.g. sip-2024")
    download.add_argument("--symbols", help="comma separated symbols (default: trading.symbols)")
    download.add_argument("--start", type=date.fromisoformat, required=True)
    download.add_argument("--end", type=date.fromisoformat, required=True)
    data_sub.add_parser("list", help="list local datasets")
    info = data_sub.add_parser("info", help="show a dataset manifest summary")
    info.add_argument("name")
    info.add_argument("--verify", action="store_true", help="recompute file hashes")

    exp = sub.add_parser("experiment", help="compare models on a dataset, fold by fold (phase 3)")
    exp.add_argument("--dataset", required=True)
    exp.add_argument(
        "--models", required=True, help=f"comma separated, from: {', '.join(available_models())}"
    )
    exp.add_argument("--reference", help="model the others are compared with (default: the first)")
    exp.add_argument(
        "--workers", type=int, default=1, help="parallel processes (one backtest per model and fold)"
    )
    exp.add_argument("--start", type=date.fromisoformat)
    exp.add_argument("--end", type=date.fromisoformat)
    exp.add_argument("--symbols", help="comma separated subset of the dataset symbols")
    exp.add_argument("--json", action="store_true")
    sub.add_parser("experiments", help="list recorded experiments")

    sub.add_parser(
        "jev-check", help="test the TypeSafe Jev setup with one live call (use with the typesafe-jev profile)"
    )
    return parser


# --------------------------------------------------------------------------- commands


def _config(args: argparse.Namespace, overrides: dict[str, Any] | None = None) -> AppConfig:
    data: dict[str, Any] = dict(overrides or {})
    if args.db:
        data.setdefault("persistence", {})["database_url"] = args.db
    return load_config(args.config, overrides=data)


def _database_url(config: AppConfig) -> str:
    url = config.persistence.database_url
    if not url:
        raise ConfigError("no database configured (persistence.database_url / DATABASE_URL / --db)")
    return url


def _symbols(raw: str | None) -> list[str] | None:
    return [x.strip().upper() for x in raw.split(",") if x.strip()] if raw else None


def _dataset_overrides(name: str | None) -> dict[str, Any]:
    return {"market_data": {"provider": "historical", "historical": {"dataset": name}}} if name else {}


async def cmd_simulate(args: argparse.Namespace, out: Console) -> int:
    overrides: dict[str, Any] = {
        "trading": {"mode": TradingMode.BACKTEST.value},
        **_dataset_overrides(args.dataset),
    }
    if symbols := _symbols(args.symbols):
        overrides["trading"]["symbols"] = symbols
    if args.model:
        overrides["model"] = {"name": args.model}
    config = _config(args, overrides)
    dataset = load_dataset(config)
    if dataset is None and args.start is None:
        out.line("simulate: --start is required on the synthetic market")
        return EXIT_REFUSED
    start = args.start or (dataset.start if dataset else None)
    end = args.end or (args.start if args.start else dataset.end if dataset else None)
    assert start is not None and end is not None
    options = SimulationOptions(
        start=start,
        end=end,
        run_id=args.run_id,
        database_url=_database_url(config),
        max_events=args.max_events,
        pace_seconds=args.pace,
    )
    if not args.json:
        out.line(mode_banner(TradingMode.BACKTEST, config.broker.active, config.market_data.provider))
    report = await run_simulation(config, options)
    if args.json:
        out.json(report_payload(report, config))
    else:
        print_report(out, report, config)
    return EXIT_OK


def report_payload(report: SimulationReport, config: AppConfig) -> dict[str, Any]:
    return {
        "run_id": report.run_id,
        "status": report.status,
        "model": f"{config.model.name}@{config.model.version}",
        "database": redact_url(config.persistence.database_url),
        "events": report.result.events,
        "first_timestamp": report.result.first_timestamp,
        "last_timestamp": report.result.last_timestamp,
        "broker_submissions": report.broker_submissions,
        "summary": report.summary,
        "performance": report.performance.model_dump(mode="json"),
        **report.extra,
    }


def _fmt(value: float | None, pattern: str = "{:.4f}") -> str:
    return "n/a" if value is None else pattern.format(value)


def print_report(out: Console, report: SimulationReport, config: AppConfig) -> None:
    counters = report.summary["counters"]
    perf = report.performance
    kill = report.summary["kill_switch"]
    out.line(f"run_id            {report.run_id}")
    out.line(f"status            {report.status}")
    out.line(f"model             {config.model.name}@{config.model.version} (no validated edge)")
    out.line(f"database          {redact_url(config.persistence.database_url)}")
    out.line(
        f"market events     {report.result.events}  ({report.result.first_timestamp} -> {report.result.last_timestamp})"
    )
    out.line(f"bars accepted     {counters['bars']}  rejected {counters['rejected_bars']}")
    out.line(f"predictions       {counters['predictions']}")
    out.line(f"signals           {counters['signals_generated']}  outcomes {counters['signal_outcomes']}")
    out.line(f"no-trade reasons  {counters['no_trade']}")
    out.line(f"risk rejections   {counters['risk_rejections']}")
    out.line(
        f"orders sent       {report.broker_submissions}  fills {counters['fills']}  trades {perf.trades}"
    )
    out.line(f"net pnl           {perf.net_pnl:.2f}  (gross {perf.gross_pnl:.2f}, fees {perf.fees:.2f})")
    out.line(
        f"model vs exec     model pnl {perf.model_pnl:.2f}, execution shortfall {perf.execution_shortfall:.2f}"
    )
    out.line(f"win rate          {_fmt(perf.win_rate)}  profit factor {_fmt(perf.profit_factor)}")
    out.line(
        f"max drawdown      {_fmt(perf.max_drawdown)}  sharpe {_fmt(perf.sharpe, '{:.2f}')} (few days: not meaningful)"
    )
    out.line(f"open positions    {report.summary['open_positions'] or 'none'}")
    if "dataset" in report.extra:
        out.line(
            f"dataset           {report.extra['dataset']['name']} (version {report.extra['dataset']['version']})"
        )
    usage = report.extra.get("jev_usage")
    if usage:
        out.line(
            f"typesafe jev      {usage['api_model']}: {usage['api_calls']} API calls, {usage['cache_hits']} cached, "
            f"{usage['input_tokens']} input tokens, ~USD {usage['estimated_cost_usd']:.4f}"
        )
    if kill.get("engaged"):
        out.line(f"KILL SWITCH       ENGAGED: {kill['reason']} - {kill['detail']}")
    else:
        out.line("kill switch       not engaged")
    out.line("")
    out.line("Synthetic data: these numbers say nothing about real markets.")
    out.line(f"Inspect a decision: python -m apps.trading_engine trace --list 10 --run-id {report.run_id}")


async def _repository(config: AppConfig) -> tuple[Database, AuditRepository]:
    database = Database(_database_url(config))
    await database.create_all()
    return database, AuditRepository(database, run_id=CLI_ACTOR_RUN, mode=TradingMode.BACKTEST)


async def cmd_trace(args: argparse.Namespace, out: Console) -> int:
    config = _config(args)
    database, repo = await _repository(config)
    try:
        if args.list is not None:
            rows = await repo.list_signals(limit=args.list, run_id=args.run_id)
            for row in rows:
                out.line(
                    f"{row['signal_id']}  {row['ts']}  {row['symbol']:<6} {row['direction']:<5} "
                    f"{row['status']:<9} {row['status_reason'] or ''}"
                )
            if not rows:
                out.line("no signals found")
            return EXIT_OK
        if not args.signal_id:
            out.line("trace: give a SIGNAL_ID or --list N")
            return EXIT_REFUSED
        trace = await repo.decision_trace(args.signal_id)
        if trace is None:
            out.line(f"signal {args.signal_id} not found")
            return EXIT_FAILED
        if trace["run"] is not None:
            trace["run"].pop("config", None)
        out.json(trace)
        return EXIT_OK
    finally:
        await database.dispose()


def print_verification(out: Console, result: VerificationResult) -> None:
    if not result.found:
        out.line(f"NOT FOUND  {result.signal_id}")
        return
    status = "OK      " if result.ok else "MISMATCH"
    out.line(f"{status}   {result.signal_id}  checks={len(result.checks)}")
    for mismatch in result.mismatches:
        out.line(f"           - {mismatch}")


async def cmd_verify(args: argparse.Namespace, out: Console) -> int:
    config = _config(args)
    database, repo = await _repository(config)
    try:
        signal_ids = list(args.signal_ids)
        if args.run_id:
            rows = await repo.list_signals(limit=args.limit, run_id=args.run_id)
            signal_ids += [row["signal_id"] for row in reversed(rows)]
        if not signal_ids:
            out.line("verify: give SIGNAL_IDs or --run-id")
            return EXIT_REFUSED
        verifier = DecisionVerifier(repo)
        results = [await verifier.verify_decision(signal_id) for signal_id in signal_ids]
        if args.json:
            out.json([r.as_dict() for r in results])
        else:
            for result in results:
                print_verification(out, result)
            ok = sum(1 for r in results if r.ok)
            out.line(f"verified {ok}/{len(results)} decisions reproduced exactly")
        return EXIT_OK if all(r.ok for r in results) else EXIT_FAILED
    finally:
        await database.dispose()


async def cmd_kill_switch(args: argparse.Namespace, out: Console) -> int:
    config = _config(args)
    database, repo = await _repository(config)
    prefix = f"{args.run_id}:" if args.run_id else ""
    key = prefix + KILL_SWITCH_KEY
    try:
        stored = await repo.get_state(key)
        clock = SystemClock()

        async def persist(state: KillSwitchState, event_type: str) -> None:
            await repo.set_state(key, state.model_dump(mode="json"), at=clock.now())
            out.line(f"{event_type}")

        switch = KillSwitch(
            clock, state=KillSwitchState.model_validate(stored) if stored else None, on_change=persist
        )
        if args.action == "engage":
            if not args.by:
                out.line("kill-switch engage requires --by")
                return EXIT_REFUSED
            if not await switch.engage(KillSwitchReason.MANUAL, args.note, by=args.by):
                out.line("already engaged (the first reason is kept)")
        elif args.action == "reset":
            try:
                await switch.reset(by=args.by or "", note=args.note)
            except SafetyError as exc:
                out.line(f"refused: {exc}")
                return EXIT_REFUSED
        state = switch.state
        controls = await repo.get_state(prefix + CONTROLS_KEY)
        out.line(f"scope        {args.run_id or 'paper/shadow'}")
        out.line(f"engaged      {state.engaged}")
        if state.engaged:
            out.line(f"reason       {state.reason.value if state.reason else ''}")
            out.line(f"detail       {state.detail}")
            out.line(f"engaged_at   {state.engaged_at}")
            out.line(f"engaged_by   {state.engaged_by}")
        out.line(f"paused       {bool(controls and controls.get('paused'))}")
        return EXIT_OK
    finally:
        await database.dispose()


async def cmd_run(args: argparse.Namespace, out: Console) -> int:
    config = _config(args)
    enforce_mode_gate(config.trading.mode, config.broker.mode)
    out.line(
        "Paper trading against a real broker arrives in phase 4 (AlpacaBrokerAdapter, Alpaca Paper only)."
    )
    out.line("Nothing was started. Use `simulate` to run the full pipeline on the synthetic market.")
    return EXIT_REFUSED


async def cmd_jev_check(args: argparse.Namespace, out: Console) -> int:
    """One real API call on synthetic features: checks key, SDK, pinned model name, latency and cost."""
    config = _config(args)
    if config.model.name != TypeSafeJEVModel.NAME:
        out.line("jev-check: select the model first: --config config/profiles/typesafe-jev.yaml")
        return EXIT_REFUSED
    with tempfile.TemporaryDirectory() as scratch:
        params = {**config.model.params, "offline": False, "cache_path": str(Path(scratch) / "check.jsonl")}
        spec = FeatureSpec.from_config(config.features)
        calendar = build_calendar(config)
        model = TypeSafeJEVModel(
            namespace="jev-check",
            version=config.model.version,
            feature_version=spec.version,
            horizon_minutes=config.trading.horizon_minutes,
            bar_minutes=config.trading.decision_timeframe.minutes,
            params=params,
        )
        client = model.client
        if isinstance(client, SdkJevClient):
            names = await asyncio.to_thread(client.list_models)
            out.line(f"model aliases     {', '.join(names) or 'none'} (pinned versions are not listed)")
        day = next(calendar.sessions_between(date(2024, 3, 4), date(2024, 3, 8)))
        bars = [
            bar
            for _, batch, _ in SyntheticMarket(config.market_data.mock, calendar).generate(
                ["MOCKA"], day.day, day.day
            )
            for bar in batch
        ][: spec.window]
        features = FeatureEngine(spec, namespace="jev-check", day_start=calendar.day_start).compute(bars)
        started = perf_counter()
        try:
            prediction = await asyncio.to_thread(model.predict, features)
        except Exception as exc:
            out.line(f"FAILED            {type(exc).__name__}: {exc}")
            return EXIT_FAILED
        elapsed_ms = (perf_counter() - started) * 1000
    # The adapter rejects any answer from a model other than the pinned one, so reaching here confirms it.
    out.line(f"api model         {model.params.api_model} (pinned version accepted and confirmed by the API)")
    out.line(
        f"decision          {prediction.direction.value}  p_up={prediction.probability_up:.3f} "
        f"p_down={prediction.probability_down:.3f} confidence={prediction.confidence:.3f}"
    )
    out.line(
        f"latency           {elapsed_ms:.0f} ms (kill switch limit {config.kill_switch.max_decision_latency_ms:.0f} ms)"
    )
    out.line(f"input tokens      {model.usage.input_tokens}  (~USD {model.cost_usd:.6f} for this call)")
    out.line("Synthetic input: this checks the integration, not whether Jev has any edge.")
    return EXIT_OK


async def cmd_data(args: argparse.Namespace, out: Console) -> int:
    config = _config(args)
    store = DatasetStore(config.market_data.historical.root)
    if args.data_command == "list":
        names = store.names()
        for name in names:
            ds = store.load(name)
            out.line(f"{name:<24} {ds.start}..{ds.end}  {len(ds.symbols)} symbols  {ds.source}")
        if not names:
            out.line(f"no datasets in {store.root}")
        return EXIT_OK
    if args.data_command == "info":
        ds = store.load(args.name)
        m = ds.manifest
        out.line(f"name        {ds.name}")
        out.line(f"version     {ds.version}  ({ds.source})")
        out.line(f"range       {ds.start}..{ds.end}  sessions {len(m['calendar'])}")
        out.line(f"symbols     {', '.join(ds.symbols)}")
        out.line(
            f"bars        {sum(f['bars'] for f in m['files'].values())}  (dropped outside session: {m['dropped_outside_regular_session']})"
        )
        early = [d["date"] for d in m["calendar"] if d["close"] != "16:00"]
        out.line(f"early close {', '.join(early) or 'none'}")
        if args.verify:
            ds.verify()
            out.line("files       OK (hashes match the manifest)")
        return EXIT_OK
    cfg = config.market_data.alpaca
    symbols = _symbols(args.symbols) or list(config.trading.symbols)
    key, secret = credentials()
    out.line(
        f"downloading {len(symbols)} symbols {args.start}..{args.end} feed={cfg.feed} adjustment={cfg.adjustment}"
    )
    with AlpacaHistoricalClient(cfg, key=key, secret=secret) as client:
        dataset = await asyncio.to_thread(
            store.download,
            client,
            name=args.name,
            symbols=symbols,
            start=args.start,
            end=args.end,
            feed=cfg.feed,
            adjustment=cfg.adjustment,
            timezone=config.trading.exchange_timezone,
            progress=lambda symbol, bars: out.line(f"  {symbol:<6} {bars} bars"),
        )
        requests = client.requests
    out.line(f"dataset {dataset.name} version {dataset.version} ({requests} requests)")
    return EXIT_OK


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


def _money(value: float | None) -> str:
    return "n/a" if value is None else f"{value:,.2f}"


def _ci(ci: list[float] | tuple[float, float] | None) -> str:
    return "n/a" if not ci else f"[{ci[0]:,.2f}, {ci[1]:,.2f}]"


def print_experiment(out: Console, payload: dict[str, Any]) -> None:
    out.line(f"experiment        {payload['experiment_id']}")
    out.line(f"dataset           {payload['dataset']['name']} (version {payload['dataset']['version']})")
    rng = payload["range"]
    out.line(
        f"range             {rng['start']}..{rng['end']}  {rng['sessions']} sessions, {len(payload['folds'])} monthly folds"
    )
    out.line(f"symbols           {', '.join(payload['symbols'])}")
    out.line("")
    header = f"{'model':<16} {'trades':>7} {'net pnl':>12} {'model pnl':>11} {'exec cost':>10} {'win':>6} {'PF':>5} {'mean/day':>9}  {'95% CI mean/day':<22} {'+folds':>6}"
    out.line(header)
    out.line("-" * len(header))
    for model in payload["models"]:
        m = payload["summaries"][model]
        pf = "n/a" if m["profit_factor"] is None else f"{m['profit_factor']:.2f}"
        out.line(
            f"{model:<16} {m['trades']:>7} {_money(m['net_pnl']):>12} {_money(m['model_pnl']):>11} "
            f"{_money(m['execution_shortfall']):>10} {_pct(m['win_rate']):>6} {pf:>5} {_money(m['mean_daily_pnl']):>9}  "
            f"{_ci(m['mean_daily_pnl_ci95']):<22} {m['positive_folds']:>3}/{len(m['folds'])}"
        )
    out.line("")
    out.line(f"{'model':<16} {'long trades':>11} {'long pnl':>11} {'short trades':>12} {'short pnl':>11}")
    for model in payload["models"]:
        m = payload["summaries"][model]
        out.line(
            f"{model:<16} {m['long']['trades']:>11} {_money(m['long']['net_pnl']):>11} "
            f"{m['short']['trades']:>12} {_money(m['short']['net_pnl']):>11}"
        )
    out.line("")
    out.line(f"paired difference vs {payload['reference']} (mean daily net pnl, same sessions):")
    for model, c in payload["comparisons"].items():
        verdict = "inconclusive"
        if c["ci95"] and c["ci95"][0] > 0:
            verdict = "better"
        elif c["ci95"] and c["ci95"][1] < 0:
            verdict = "worse"
        out.line(
            f"  {model:<16} {_money(c['mean_daily_difference']):>9}  95% CI {_ci(c['ci95'])}  -> {verdict}"
        )
    for model in payload["models"]:
        api = payload["summaries"][model].get("api")
        if api:
            out.line(
                f"  {model}: {api['api_calls']} API calls, {api['cache_hits']} cached, ~USD {api['estimated_cost_usd']:.4f}"
            )
        failed = payload["summaries"][model]["failed_folds"]
        if failed:
            out.line(f"  WARNING {model}: folds not completed: {', '.join(failed)}")
    out.line("")
    for caveat in payload["caveats"]:
        out.line(f"note: {caveat}")


async def cmd_experiment(args: argparse.Namespace, out: Console) -> int:
    overrides: dict[str, Any] = {
        "trading": {"mode": TradingMode.BACKTEST.value},
        **_dataset_overrides(args.dataset),
    }
    if symbols := _symbols(args.symbols):
        overrides["trading"]["symbols"] = symbols
    config = _config(args, overrides)
    models = [m.strip() for m in args.models.split(",") if m.strip()]

    def progress(result: JobResult, done: int, total: int) -> None:
        pnl = sum(t["net_pnl"] for t in result.trades)
        out.line(
            f"  [{done}/{total}] {result.model:<16} {result.fold}  {result.status:<9} trades {len(result.trades):>5}  net {pnl:,.2f}"
        )

    if not args.json:
        out.line(mode_banner(TradingMode.BACKTEST, config.broker.active, "historical"))
    result = await run_experiment(
        config,
        models=models,
        reference=args.reference,
        workers=args.workers,
        start=args.start,
        end=args.end,
        progress=None if args.json else progress,
    )
    if args.json:
        out.json(result.payload)
    else:
        out.line("")
        print_experiment(out, result.payload)
        out.line(f"details: {(result.output_dir / 'results.json').as_posix()}")
    return EXIT_OK


async def cmd_experiments(args: argparse.Namespace, out: Console) -> int:
    config = _config(args)
    database, repo = await _repository(config)
    try:
        rows = await repo.list_experiments()
        for row in rows:
            out.line(
                f"{row['experiment_id']}  {row['created_at']}  {row['dataset']}  {', '.join(row['models'])}"
            )
        if not rows:
            out.line("no experiments recorded")
        return EXIT_OK
    finally:
        await database.dispose()


COMMANDS = {
    "simulate": cmd_simulate,
    "trace": cmd_trace,
    "verify": cmd_verify,
    "kill-switch": cmd_kill_switch,
    "run": cmd_run,
    "jev-check": cmd_jev_check,
    "data": cmd_data,
    "experiment": cmd_experiment,
    "experiments": cmd_experiments,
}


def main(argv: Sequence[str] | None = None, *, stdout: TextIO | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    out = Console(stdout or sys.stdout)
    try:
        config = _config(args)
        configure_logging(
            args.log_level or config.logging.level,
            json_format=config.logging.json_format,
            secrets=sensitive_values(),
        )
        return asyncio.run(COMMANDS[args.command](args, out))
    except LiveTradingNotAllowed as exc:
        out.line(f"REFUSED: {exc}")
        return EXIT_REFUSED
    except (ConfigError, SafetyError) as exc:
        out.line(f"configuration error: {exc}")
        return EXIT_CONFIG
    except DataError as exc:
        out.line(f"data error: {exc}")
        return EXIT_CONFIG
