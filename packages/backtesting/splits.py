"""Time-based splits. Never random: every split respects the arrow of time.

- `monthly_folds`: consecutive evaluation windows (calendar months) over a list of sessions. For models without
  training (heuristics, baselines, a remote model such as TypeSafe Jev) every fold is already out of sample.
- `walk_forward`: rolling TRAIN -> (embargo) -> TEST windows for trained models. The embargo removes the sessions
  right after the training window, so labels computed over the prediction horizon can never overlap the test data.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Fold:
    name: str
    start: date
    end: date
    sessions: int


@dataclass(frozen=True)
class WalkForwardSplit:
    name: str
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    embargo_sessions: int


def monthly_folds(sessions: Sequence[date], *, min_sessions: int = 5) -> list[Fold]:
    """One fold per calendar month; a trailing month shorter than `min_sessions` joins the previous fold."""
    days = sorted(set(sessions))
    groups: list[list[date]] = []
    for day in days:
        if groups and (groups[-1][0].year, groups[-1][0].month) == (day.year, day.month):
            groups[-1].append(day)
        else:
            groups.append([day])
    if len(groups) > 1 and len(groups[-1]) < min_sessions:
        groups[-2].extend(groups.pop())
    return [Fold(f"{g[0]:%Y-%m}", g[0], g[-1], len(g)) for g in groups]


def walk_forward(
    sessions: Sequence[date],
    *,
    train_sessions: int,
    test_sessions: int,
    embargo_sessions: int = 1,
    step_sessions: int | None = None,
) -> list[WalkForwardSplit]:
    """Rolling splits: TRAIN (train_sessions) | EMBARGO (embargo_sessions) | TEST (test_sessions)."""
    if train_sessions <= 0 or test_sessions <= 0 or embargo_sessions < 0:
        raise ValueError("train/test sizes must be positive and the embargo non-negative")
    days = sorted(set(sessions))
    step = step_sessions or test_sessions
    splits: list[WalkForwardSplit] = []
    start = 0
    while start + train_sessions + embargo_sessions + test_sessions <= len(days):
        train = days[start : start + train_sessions]
        test_from = start + train_sessions + embargo_sessions
        test = days[test_from : test_from + test_sessions]
        splits.append(
            WalkForwardSplit(
                f"wf{len(splits) + 1:02d}", train[0], train[-1], test[0], test[-1], embargo_sessions
            )
        )
        start += step
    return splits
