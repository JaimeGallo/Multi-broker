"""Command line interface: `python -m apps.trading_engine <command>`.

Commands: simulate, trace, verify, kill-switch, run. Console output is plain ASCII; logs go to stderr.
Exit codes: 0 ok, 1 verification failed / not found, 2 refused or not available, 3 configuration error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import date
from typing import Any, TextIO

from apps.trading_engine.bootstrap import (
    CONTROLS_KEY,
    KILL_SWITCH_KEY,
    SimulationOptions,
    SimulationReport,
    run_simulation,
)
from packages.common.clock import SystemClock
from packages.common.config import AppConfig, load_config
from packages.common.enums import TradingMode
from packages.common.errors import ConfigError, LiveTradingNotAllowed, SafetyError
from packages.common.logging import configure_logging
from packages.common.safety import enforce_mode_gate, mode_banner
from packages.common.secrets import load_dotenv, redact_url, sensitive_values
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

    sim = sub.add_parser("simulate", help="run the full pipeline on the synthetic market (backtest mode)")
    sim.add_argument("--start", type=date.fromisoformat, required=True, help="first day (YYYY-MM-DD)")
    sim.add_argument("--end", type=date.fromisoformat, help="last day (default: --start)")
    sim.add_argument("--symbols", help="comma separated symbols (default: trading.symbols)")
    sim.add_argument("--model", help="model name (jev-heuristic, baseline-random, baseline-flat)")
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


async def cmd_simulate(args: argparse.Namespace, out: Console) -> int:
    overrides: dict[str, Any] = {"trading": {"mode": TradingMode.BACKTEST.value}}
    if args.symbols:
        overrides["trading"]["symbols"] = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if args.model:
        overrides["model"] = {"name": args.model}
    config = _config(args, overrides)
    options = SimulationOptions(
        start=args.start,
        end=args.end or args.start,
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
    }


def _fmt(value: float | None, pattern: str = "{:.4f}") -> str:
    return "n/a" if value is None else pattern.format(value)


def print_report(out: Console, report: SimulationReport, config: AppConfig) -> None:
    counters = report.summary["counters"]
    perf = report.performance
    kill = report.summary["kill_switch"]
    out.line(f"run_id            {report.run_id}")
    out.line(f"status            {report.status}")
    out.line(f"model             {config.model.name}@{config.model.version} (stand-in, no validated edge)")
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


COMMANDS = {
    "simulate": cmd_simulate,
    "trace": cmd_trace,
    "verify": cmd_verify,
    "kill-switch": cmd_kill_switch,
    "run": cmd_run,
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
