"""TypeSafe Jev adapter: mapping, pinning, recorded-answer cache, offline replay and the real SDK wire format.

No test touches the network: a fake client stands in for the API, and the SDK test uses an in-memory transport.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pytest

from packages.common.calendar import RegularHoursCalendar
from packages.common.config import ModelSection
from packages.common.entities import FeatureVector
from packages.common.enums import Direction
from packages.common.errors import ConfigError, ModelError
from packages.features.engine import FeatureEngine
from packages.features.spec import FeatureSpec
from packages.jev import typesafe
from packages.jev.registry import build_model, replay_params
from packages.jev.typesafe import JevAnswer, SdkJevClient, TypeSafeJEVModel
from tests.helpers import random_walk_bars


class FakeJev:
    """Deterministic stand-in for the API: leans with 15-bar momentum."""

    def __init__(self, *, model: str | None = None, labels: tuple[str, ...] = ("up", "down", "flat")) -> None:
        self.calls: list[dict[str, Any]] = []
        self._model = model
        self._labels = labels

    def decide(self, *, state: dict[str, Any], question: dict[str, Any], model: str) -> JevAnswer:
        self.calls.append({"state": state, "question": question, "model": model})
        momentum = state["features"]["ret_15"] or 0.0
        up = min(0.9, max(0.05, 0.45 + 40 * momentum))
        down = min(0.9, max(0.05, 0.45 - 40 * momentum))
        flat = max(0.0, 1.0 - up - down)
        probabilities = dict(zip(self._labels, (up, down, flat), strict=False))
        return JevAnswer(
            choice=max(probabilities, key=probabilities.__getitem__),
            confidence=abs(up - down),
            probabilities=probabilities,
            model=self._model or model,
            input_tokens=len(json.dumps(state)) // 4,
        )


def features(seed: int = 3) -> FeatureVector:
    spec = FeatureSpec()
    return FeatureEngine(spec, namespace="ns", day_start=RegularHoursCalendar().day_start).compute(
        random_walk_bars(150, seed=seed)
    )


def model(tmp_path: Path, client: Any = None, **params: Any) -> TypeSafeJEVModel:
    return TypeSafeJEVModel(
        namespace="ns",
        version="0.1.0",
        feature_version="0.1.0",
        horizon_minutes=15,
        bar_minutes=1,
        params={"cache_path": str(tmp_path / "cache.jsonl"), **params},
        client=client,
    )


def test_prediction_maps_jev_probabilities_and_keeps_volatility_local(tmp_path: Path) -> None:
    fake = FakeJev()
    vector = features()
    prediction = model(tmp_path, fake).predict(vector)
    sent = fake.calls[0]
    assert sent["model"] == "jev-1.13.0"
    assert sent["question"]["type"] == "choice" and set(sent["question"]["criteria"]) == {
        "up",
        "down",
        "flat",
    }
    answer = fake.decide(state=sent["state"], question=sent["question"], model=sent["model"])
    assert prediction.probability_up == pytest.approx(answer.probabilities["up"])
    assert prediction.probability_down == pytest.approx(answer.probabilities["down"])
    assert prediction.confidence == pytest.approx(answer.confidence)
    assert prediction.expected_volatility == pytest.approx(vector.values["realized_vol_30"] * math.sqrt(15))
    assert prediction.model_name == "typesafe-jev"


def test_state_is_compact_and_free_of_account_data(tmp_path: Path) -> None:
    fake = FakeJev()
    model(tmp_path, fake).predict(features())
    state = fake.calls[0]["state"]
    assert set(state) == {"instrument", "as_of", "bar_timeframe", "horizon_minutes", "features"}
    assert set(state["features"]) == set(typesafe.STATE_FEATURES)
    assert state["features"]["spread_bps"] is None  # NaN (no quote) is sent as null, never as NaN
    json.dumps(state, allow_nan=False)


def test_flat_answer_means_no_trade(tmp_path: Path) -> None:
    class Flat(FakeJev):
        def decide(self, *, state: dict[str, Any], question: dict[str, Any], model: str) -> JevAnswer:
            return JevAnswer("flat", 0.8, {"up": 0.1, "down": 0.1, "flat": 0.8}, model, 100)

    assert model(tmp_path, Flat()).predict(features()).direction is Direction.NO_TRADE


def test_answers_are_recorded_and_replayed_offline(tmp_path: Path) -> None:
    fake = FakeJev()
    live = model(tmp_path, fake)
    vector = features()
    first = live.predict(vector)
    again = live.predict(vector)
    assert len(fake.calls) == 1 and live.usage.api_calls == 1 and live.usage.cache_hits == 1
    assert live.usage.input_tokens > 0 and live.cost_usd > 0
    offline = model(tmp_path, offline=True)  # new process, no client, same cache file
    assert offline.client is None
    replayed = offline.predict(vector)
    assert replayed == first == again
    with pytest.raises(ModelError, match="offline"):
        offline.predict(features(seed=4))


def test_pinned_model_and_labels_are_enforced(tmp_path: Path) -> None:
    with pytest.raises(ModelError, match="pinned"):
        model(tmp_path, FakeJev(model="jev-1.14.0")).predict(features())
    with pytest.raises(ModelError, match="labels"):
        model(tmp_path / "b", FakeJev(labels=("yes", "no", "maybe"))).predict(features())
    assert not (tmp_path / "cache.jsonl").exists()  # rejected answers are never recorded


def test_identity_excludes_runtime_options(tmp_path: Path) -> None:
    jev = model(tmp_path, FakeJev(), timeout_seconds=1.0, max_retries=1)
    assert set(jev.metadata.params) == {
        "api_model",
        "prompt_version",
        "entry_threshold",
        "edge_scale",
        "significant_digits",
    }
    with pytest.raises(ValueError):
        model(tmp_path, FakeJev(), api_modle="typo")
    stored = jev.metadata.params
    rebuilt = replay_params("typesafe-jev", stored, {"cache_path": "x.jsonl", "entry_threshold": 0.9})
    assert (
        rebuilt["offline"] is True
        and rebuilt["cache_path"] == "x.jsonl"
        and rebuilt["entry_threshold"] == 0.55
    )
    assert replay_params("jev-heuristic", {"a": 1}, {"b": 2}) == {"a": 1}


def test_registry_builds_it_only_when_selected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeJev()
    monkeypatch.setattr(typesafe, "CLIENT_FACTORY", lambda runtime: fake)
    section = ModelSection(name="typesafe-jev", params={"cache_path": str(tmp_path / "c.jsonl")})
    built = build_model(section, namespace="ns", feature_version="0.1.0", horizon_minutes=15, bar_minutes=1)
    assert isinstance(built, TypeSafeJEVModel) and built.client is fake
    default = build_model(
        ModelSection(), namespace="ns", feature_version="0.1.0", horizon_minutes=15, bar_minutes=1
    )
    assert not isinstance(default, TypeSafeJEVModel)  # disabled by default


def test_missing_api_key_is_a_configuration_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(ConfigError):
        model(tmp_path)  # live mode, no client injected: the real factory refuses without a key


def test_sdk_client_speaks_the_official_wire_format(monkeypatch: pytest.MonkeyPatch) -> None:
    httpx2 = pytest.importorskip("httpx2")
    pytest.importorskip("typesafe_sdk")
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test-key-not-real")
    seen: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx2.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "usage": {"input_tokens": 210, "output_tokens": 3},
                "answers": {
                    "direction": {
                        "type": "choice",
                        "choice": "up",
                        "confidence": 0.7,
                        "probabilities": {"up": 0.6, "down": 0.25, "flat": 0.15},
                    }
                },
            },
        )

    client = SdkJevClient(timeout_seconds=1.0, max_retries=0, transport=httpx2.MockTransport(handler))
    question = {"type": "choice", "instructions": "q", "criteria": {"up": "u", "down": "d", "flat": "f"}}
    answer = client.decide(state={"features": {"ret_15": 0.001}}, question=question, model="jev-1.13.0")
    assert seen["url"] == "https://api.typesafe.ai/v1/systemone"
    assert seen["auth"] == "Bearer ts-test-key-not-real"
    assert seen["body"] == {
        "state": {"features": {"ret_15": 0.001}},
        "model": "jev-1.13.0",
        "questions": {"direction": question},
    }
    assert answer == JevAnswer("up", 0.7, {"up": 0.6, "down": 0.25, "flat": 0.15}, "jev-1.13.0", 210)
