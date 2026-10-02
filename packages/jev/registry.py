"""Model factory. Phase 3 adds trained models loaded from versioned artifacts in models/."""

from __future__ import annotations

from typing import Any, Protocol

from packages.common.config import ModelSection
from packages.common.errors import ConfigError
from packages.jev.base import JEVModel
from packages.jev.baselines import FlatJEVModel, MovingAverageJEVModel, RandomJEVModel
from packages.jev.heuristic import HeuristicJEVModel
from packages.jev.typesafe import RUNTIME_KEYS as TYPESAFE_RUNTIME_KEYS
from packages.jev.typesafe import TypeSafeJEVModel


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
    MovingAverageJEVModel.NAME: MovingAverageJEVModel,
    TypeSafeJEVModel.NAME: TypeSafeJEVModel,  # remote, disabled unless selected explicitly
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


def replay_params(name: str, stored: dict[str, Any], configured: dict[str, Any]) -> dict[str, Any]:
    """Parameters to rebuild a model for decision replay: the stored decision parameters plus, for remote
    models, the run's runtime options and offline mode (replay never calls an external API)."""
    if name == TypeSafeJEVModel.NAME:
        runtime = {k: v for k, v in configured.items() if k in TYPESAFE_RUNTIME_KEYS}
        return {**stored, **runtime, "offline": True}
    return dict(stored)
