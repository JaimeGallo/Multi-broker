"""Model factory. Phase 3 adds trained models loaded from versioned artifacts in models/."""

from __future__ import annotations

from typing import Protocol

from packages.common.config import ModelSection
from packages.common.errors import ConfigError
from packages.jev.base import JEVModel
from packages.jev.baselines import FlatJEVModel, RandomJEVModel
from packages.jev.heuristic import HeuristicJEVModel


class _ModelFactory(Protocol):
    def __call__(
        self,
        *,
        namespace: str,
        version: str,
        feature_version: str,
        horizon_minutes: int,
        bar_minutes: int,
        params: dict[str, object] | None = None,
    ) -> JEVModel: ...


MODEL_FACTORIES: dict[str, _ModelFactory] = {
    HeuristicJEVModel.NAME: HeuristicJEVModel,
    RandomJEVModel.NAME: RandomJEVModel,
    FlatJEVModel.NAME: FlatJEVModel,
}


def available_models() -> list[str]:
    return sorted(MODEL_FACTORIES)


def build_model(
    config: ModelSection, *, namespace: str, feature_version: str, horizon_minutes: int, bar_minutes: int
) -> JEVModel:
    factory = MODEL_FACTORIES.get(config.name)
    if factory is None:
        raise ConfigError(f"unknown model '{config.name}'. Available: {available_models()}")
    return factory(
        namespace=namespace,
        version=config.version,
        feature_version=feature_version,
        horizon_minutes=horizon_minutes,
        bar_minutes=bar_minutes,
        params=dict(config.params),
    )
