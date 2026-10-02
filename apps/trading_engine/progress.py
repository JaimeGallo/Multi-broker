"""Console progress for long backtests: sessions processed, jobs finished, elapsed time and a rough ETA.

Thread-safe (experiments update it from a monitoring thread). Prints at most once per `interval` seconds, so a
year-long experiment stays readable.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


def _duration(seconds: float) -> str:
    seconds = int(seconds)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m{secs:02d}s"


class ProgressBoard:
    def __init__(
        self,
        *,
        total_sessions: int,
        total_jobs: int = 1,
        write: Callable[[str], None],
        interval: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._total_sessions = max(1, total_sessions)
        self._total_jobs = total_jobs
        self._write = write
        self._interval = interval
        self._clock = clock
        self._lock = threading.Lock()
        self._started = clock()
        self._last_print = self._started
        self.sessions = 0
        self.jobs = 0

    def session_done(self, count: int = 1) -> None:
        with self._lock:
            self.sessions += count
            self._maybe_print()

    def job_done(self) -> None:
        with self._lock:
            self.jobs += 1
            self._maybe_print()

    def line(self) -> str:
        elapsed = self._clock() - self._started
        done = min(self.sessions, self._total_sessions)
        fraction = done / self._total_sessions
        eta = elapsed / done * (self._total_sessions - done) if done else None
        parts = [
            f"progress {fraction * 100:5.1f}%",
            f"sessions {done:,}/{self._total_sessions:,}",
        ]
        if self._total_jobs > 1:
            parts.append(f"jobs {self.jobs}/{self._total_jobs}")
        parts.append(f"elapsed {_duration(elapsed)}")
        parts.append(f"ETA ~{_duration(eta)}" if eta is not None else "ETA n/a")
        return "  " + " | ".join(parts)

    def _maybe_print(self) -> None:
        now = self._clock()
        if now - self._last_print >= self._interval:
            self._last_print = now
            self._write(self.line())
