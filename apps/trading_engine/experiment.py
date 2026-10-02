"""Experiments (phase 3): several models, same dataset, same folds, same costs and risk rules.

Each (model, fold) pair is an independent backtest with its own SQLite file under the experiment directory, so
jobs can run in parallel processes and every decision stays verifiable (`verify --db <job db>`). Results are
aggregated per model (daily-PnL statistics with bootstrap intervals, long/short split, per fold and per symbol),
compared pairwise against a reference model, written to `results.json` and recorded in the `experiments` table.

Models without training are evaluated fold by fold (every fold is out of sample). Trained models (walk-forward with
purging/embargo, `packages.backtesting.splits.walk_forward`) plug in here once they exist.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from multiprocessing import get_context
from pathlib import Path
from queue import Empty
from typing import Any

from apps.trading_engine.bootstrap import (
    SimulationOptions,
    load_dataset,
    run_simulation,
    with_measured_spreads,
)
from apps.trading_engine.progress import ProgressBoard
from packages.analytics.evaluation import daily_pnl, model_summary, paired_difference
from packages.backtesting.splits import Fold, monthly_folds
from packages.common.config import PROJECT_ROOT, AppConfig, config_hash, load_config
from packages.common.entities import Trade
from packages.common.enums import TradingMode
from packages.common.errors import ConfigError
from packages.common.ids import new_id
from packages.common.logging import configure_logging
from packages.common.run import detect_git_commit
from packages.jev.registry import available_models
from packages.market_data.spreads import load_spreads
from packages.persistence.database import Database
from packages.persistence.repositories import AuditRepository, sanitized_config

# Models whose parameters live in their own profile (loaded when the experiment's base config uses another model).
MODEL_PROFILES = {"typesafe-jev": PROJECT_ROOT / "config" / "profiles" / "typesafe-jev.yaml"}


@dataclass
class Job:
    model: str
    fold: Fold
    config: dict[str, Any]
    run_id: str
    database_url: str
    git_commit: str | None
    in_worker_process: bool = False
    progress_queue: Any = None  # multiprocessing queue: one item per simulated session


@dataclass
class JobResult:
    model: str
    fold: str
    run_id: str
    status: str
    database_url: str
    trades: list[dict[str, Any]] = field(default_factory=list)
    counters: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


def model_config(base: AppConfig, model: str, *, dataset_version: str, fold: Fold) -> dict[str, Any]:
    data = base.model_dump(mode="json")
    if model == base.model.name:
        section = data["model"]
    elif model in MODEL_PROFILES:
        section = load_config(MODEL_PROFILES[model], environ={}).model_dump(mode="json")["model"]
    else:
        section = {"name": model, "version": base.model.version, "params": {}}
    if section["name"] == "typesafe-jev":
        # One answer cache per dataset version and fold: parallel jobs never share a file, reruns are free.
        section["params"] = {
            **section["params"],
            "cache_path": f"data/jev_cache/{dataset_version}/{fold.name}.jsonl",
        }
    data["model"] = section
    return data


def run_job(job: Job) -> JobResult:
    """Runs in a worker process (or inline with one worker)."""
    if job.in_worker_process:
        configure_logging("WARNING", json_format=True)
    config = AppConfig.model_validate(job.config)
    options = SimulationOptions(
        start=job.fold.start,
        end=job.fold.end,
        run_id=job.run_id,
        database_url=job.database_url,
        git_commit=job.git_commit,
        on_session=(lambda day: job.progress_queue.put(1)) if job.progress_queue is not None else None,
    )
    try:
        report = asyncio.run(run_simulation(config, options))
    except Exception as exc:  # one failed job must not lose the others; it is reported as FAILED
        logging.getLogger(__name__).exception("experiment job failed")
        return JobResult(job.model, job.fold.name, job.run_id, "FAILED", job.database_url, error=str(exc))
    return JobResult(
        model=job.model,
        fold=job.fold.name,
        run_id=job.run_id,
        status=report.status,
        database_url=job.database_url,
        trades=[t.model_dump(mode="json") for t in report.trades],
        counters=report.summary["counters"],
        usage=report.extra.get("jev_usage", {}),
    )


class _InlineQueue:
    """Same interface as a multiprocessing queue, for jobs run in this process."""

    def __init__(self, board: ProgressBoard) -> None:
        self._board = board

    def put(self, item: int) -> None:
        self._board.session_done(item)


def _drain(queue: Any, board: ProgressBoard | None) -> None:
    if queue is None or board is None:
        return
    count = 0
    while True:
        try:
            count += queue.get_nowait()
        except Empty:
            break
    if count:
        board.session_done(count)


@dataclass
class ExperimentResult:
    experiment_id: str
    output_dir: Path
    payload: dict[str, Any]


async def run_experiment(
    base: AppConfig,
    *,
    models: list[str],
    reference: str | None = None,
    workers: int = 1,
    start: date | None = None,
    end: date | None = None,
    experiment_id: str | None = None,
    output_root: str | Path = "data/experiments",
    progress: Callable[[JobResult, int, int], None] | None = None,
    report: Callable[[str], None] | None = None,
    report_interval: float = 30.0,
) -> ExperimentResult:
    unknown = sorted(set(models) - set(available_models()))
    if unknown:
        raise ConfigError(f"unknown models {unknown}; available: {available_models()}")
    if len(set(models)) != len(models) or not models:
        raise ConfigError("give each model once")
    reference = reference or models[0]
    if reference not in models:
        raise ConfigError(f"reference model {reference!r} is not in the experiment")
    dataset = load_dataset(base)
    if dataset is None:
        raise ConfigError("experiments run on a downloaded dataset (--dataset NAME)")
    measured = load_spreads(dataset)
    calendar = dataset.calendar(base.trading.exchange_timezone)
    first, last = start or dataset.start, end or dataset.end
    sessions = [s.day for s in calendar.sessions_between(first, last)]
    folds = monthly_folds(sessions)
    if not folds:
        raise ConfigError("no sessions in the requested range")

    experiment_id = experiment_id or new_id("exp")
    output = Path(output_root) / experiment_id
    output.mkdir(parents=True, exist_ok=True)
    git_commit = detect_git_commit()
    jobs = [
        Job(
            model=model,
            fold=fold,
            config=model_config(base, model, dataset_version=dataset.version, fold=fold),
            run_id=f"{experiment_id}-{model}-{fold.name}",
            database_url=f"sqlite+aiosqlite:///{(output / f'{model}__{fold.name}.db').as_posix()}",
            git_commit=git_commit,
        )
        for model in models
        for fold in folds
    ]
    results: list[JobResult] = []
    board = (
        ProgressBoard(
            total_sessions=sum(job.fold.sessions for job in jobs),
            total_jobs=len(jobs),
            write=report,
            interval=report_interval,
        )
        if report is not None
        else None
    )

    def finished(result: JobResult) -> None:
        results.append(result)
        if board is not None:
            board.job_done()
        if progress is not None:
            progress(result, len(results), len(jobs))

    if workers <= 1:
        for job in jobs:
            if board is not None:
                job.progress_queue = _InlineQueue(board)
            finished(await asyncio.to_thread(run_job, job))
    else:
        for job in jobs:
            job.in_worker_process = True

        def run_pool() -> None:
            context = get_context("spawn")
            with (
                context.Manager() as manager,
                ProcessPoolExecutor(max_workers=workers, mp_context=context) as pool,
            ):
                queue = manager.Queue() if board is not None else None
                for job in jobs:
                    job.progress_queue = queue
                pending = {pool.submit(run_job, job) for job in jobs}
                while pending:
                    done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                    _drain(queue, board)
                    for future in done:
                        finished(future.result())
                _drain(queue, board)

        await asyncio.to_thread(run_pool)

    fold_by_day = {day: fold.name for fold in folds for day in sessions if fold.start <= day <= fold.end}
    start_equity = base.broker.mock.initial_cash
    summaries: dict[str, Any] = {}
    dailies: dict[str, dict[date, float]] = {}
    for model in models:
        model_results = [r for r in results if r.model == model]
        trades = [Trade.model_validate(t) for r in model_results for t in r.trades]
        summaries[model] = model_summary(
            trades,
            trading_date=calendar.trading_date,
            sessions=sessions,
            start_equity=start_equity,
            fold_of=fold_by_day.__getitem__,
        )
        summaries[model]["failed_folds"] = [r.fold for r in model_results if r.status != "COMPLETED"]
        usage = [r.usage for r in model_results if r.usage]
        if usage:
            summaries[model]["api"] = {
                "api_calls": sum(u["api_calls"] for u in usage),
                "cache_hits": sum(u["cache_hits"] for u in usage),
                "input_tokens": sum(u["input_tokens"] for u in usage),
                "estimated_cost_usd": sum(u["estimated_cost_usd"] for u in usage),
            }
        dailies[model] = daily_pnl(trades, calendar.trading_date, sessions)
    comparisons = {
        model: paired_difference(dailies[model], dailies[reference]) for model in models if model != reference
    }
    payload: dict[str, Any] = {
        "experiment_id": experiment_id,
        "created_at": datetime.now(UTC).isoformat(),
        "dataset": {"name": dataset.name, "version": dataset.version, "symbols": dataset.symbols},
        "range": {"start": first.isoformat(), "end": last.isoformat(), "sessions": len(sessions)},
        "folds": [
            {"name": f.name, "start": f.start.isoformat(), "end": f.end.isoformat(), "sessions": f.sessions}
            for f in folds
        ],
        "models": models,
        "reference": reference,
        "symbols": list(base.trading.symbols),
        "config_hash": config_hash(base),
        "costs": {
            "default_spread_bps": base.costs.default_spread_bps,
            "slippage_bps": base.costs.slippage_bps,
            "spread_calibration": (measured or {}).get("calibration_version")
            if base.costs.use_measured_spreads
            else None,
            "spread_by_symbol": with_measured_spreads(base, dataset).costs.spread_by_symbol,
        },
        "git_commit": git_commit,
        "jobs": [
            {
                "model": r.model,
                "fold": r.fold,
                "run_id": r.run_id,
                "status": r.status,
                "database_url": r.database_url,
                "error": r.error,
            }
            for r in sorted(results, key=lambda r: (models.index(r.model), r.fold))
        ],
        "summaries": summaries,
        "comparisons": comparisons,
        "caveats": [
            "Fixed symbol lists of today's large caps carry survivorship bias.",
            "Spreads are typical values (measured medians or the default), not the quote at each trade.",
            "Confidence intervals come from a bootstrap of daily PnL; an interval including 0 is not an edge.",
        ],
    }
    (output / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    await _record(base, payload, output)
    return ExperimentResult(experiment_id, output, payload)


async def _record(base: AppConfig, payload: dict[str, Any], output: Path) -> None:
    url = base.persistence.database_url
    if not url:
        return
    database = Database(url)
    try:
        await database.create_all()
        repo = AuditRepository(database, run_id=payload["experiment_id"], mode=TradingMode.BACKTEST)
        await repo.save_experiment(
            {
                "experiment_id": payload["experiment_id"],
                "created_at": datetime.fromisoformat(payload["created_at"]),
                "dataset": payload["dataset"]["name"],
                "dataset_version": payload["dataset"]["version"],
                "models": payload["models"],
                "folds": payload["folds"],
                "config": sanitized_config(base),
                "git_commit": payload["git_commit"],
                "output_dir": output.as_posix(),
                "results": json.loads(
                    json.dumps(
                        {"summaries": payload["summaries"], "comparisons": payload["comparisons"]},
                        default=str,
                    )
                ),
            }
        )
    finally:
        await database.dispose()
