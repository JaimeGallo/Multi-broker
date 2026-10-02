"""Model quality, independent of execution.

Every prediction (traded or not) is compared with the price `horizon_minutes` later. Outcomes are resolved only
once that time has passed and are never fed back into decisions (no leakage). The first bar ending at or after
the target time is used; predictions near the close therefore include the overnight move.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict

from packages.common.entities import JEVPrediction, MarketBar
from packages.common.enums import Direction


class PredictionOutcome(BaseModel):
    model_config = ConfigDict(frozen=True)

    prediction_id: str
    symbol: str
    timestamp: datetime
    direction: Direction
    probability_up: float
    realized_return: float
    direction_correct: bool | None
    resolved_at: datetime


class PredictionOutcomeTracker:
    def __init__(self) -> None:
        self._pending: dict[str, deque[tuple[JEVPrediction, float]]] = {}

    @property
    def pending(self) -> int:
        return sum(len(queue) for queue in self._pending.values())

    def add(self, prediction: JEVPrediction, reference_price: float) -> None:
        self._pending.setdefault(prediction.symbol, deque()).append((prediction, reference_price))

    def on_bar(self, bar: MarketBar) -> list[PredictionOutcome]:
        queue = self._pending.get(bar.symbol)
        outcomes: list[PredictionOutcome] = []
        while queue:
            prediction, reference = queue[0]
            if prediction.timestamp + timedelta(minutes=prediction.horizon_minutes) > bar.end:
                break
            queue.popleft()
            realized = bar.close / reference - 1.0
            correct: bool | None = None
            if prediction.direction is Direction.LONG:
                correct = realized > 0
            elif prediction.direction is Direction.SHORT:
                correct = realized < 0
            outcomes.append(
                PredictionOutcome(
                    prediction_id=prediction.prediction_id,
                    symbol=prediction.symbol,
                    timestamp=prediction.timestamp,
                    direction=prediction.direction,
                    probability_up=prediction.probability_up,
                    realized_return=realized,
                    direction_correct=correct,
                    resolved_at=bar.end,
                )
            )
        return outcomes


def summarize_outcomes(outcomes: Sequence[PredictionOutcome]) -> dict[str, float | int | None]:
    directional = [o for o in outcomes if o.direction_correct is not None]
    hits = sum(1 for o in directional if o.direction_correct)
    brier = (
        sum((o.probability_up - (1.0 if o.realized_return > 0 else 0.0)) ** 2 for o in outcomes) / len(outcomes)
        if outcomes
        else None
    )
    return {
        "resolved": len(outcomes),
        "directional": len(directional),
        "hit_rate": hits / len(directional) if directional else None,
        "brier_score": brier,
    }
