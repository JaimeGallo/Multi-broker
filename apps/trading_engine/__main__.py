"""Entry point: `python -m apps.trading_engine --help`."""

from apps.trading_engine.cli import main

if __name__ == "__main__":  # required: experiment workers are spawned processes that re-import this module
    raise SystemExit(main())
