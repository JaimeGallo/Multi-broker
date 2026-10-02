"""Run context: identifies every record produced by one engine execution."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from packages.common.config import PROJECT_ROOT, AppConfig, config_hash
from packages.common.enums import TradingMode
from packages.common.ids import new_id


@dataclass(frozen=True)
class RunContext:
    run_id: str
    mode: TradingMode
    namespace: str
    strategy: str
    broker: str
    market_data: str
    started_at: datetime
    config_hash: str
    git_commit: str | None

    @classmethod
    def create(
        cls,
        config: AppConfig,
        *,
        mode: TradingMode,
        started_at: datetime,
        run_id: str | None = None,
        git_commit: str | None = None,
    ) -> RunContext:
        run_id = run_id or new_id("run")
        strategy = config.trading.strategy
        broker = config.broker.active
        # Backtests are scoped to their run; paper/shadow ids must survive restarts (idempotency).
        if mode in (TradingMode.BACKTEST, TradingMode.REPLAY):
            namespace = run_id
        else:
            namespace = f"{strategy}:{mode.value}:{broker}"
        return cls(
            run_id=run_id,
            mode=mode,
            namespace=namespace,
            strategy=strategy,
            broker=broker,
            market_data=config.market_data.provider,
            started_at=started_at,
            config_hash=config_hash(config),
            git_commit=git_commit,
        )


def detect_git_commit(root: Path = PROJECT_ROOT) -> str | None:
    """Short commit hash, suffixed with '-dirty' when the working tree has uncommitted changes."""
    try:
        head = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if head.returncode != 0 or not head.stdout.strip():
            return None
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, timeout=5, check=False
        )
        dirty = status.returncode == 0 and bool(status.stdout.strip())
        return head.stdout.strip() + ("-dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        return None
