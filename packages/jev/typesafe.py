"""TypeSafe Jev adapter: JEV's direction decision delegated to TypeSafe AI's System One model.

DISABLED by default: it is only used when `model.name: typesafe-jev` (see config/profiles/typesafe-jev.yaml),
it needs the optional `typesafe` extra and a `TYPESAFE_API_KEY` in the environment. No edge is claimed: Jev is
a general decision model, not a market model, and it must earn its place in walk-forward tests (phase 3).

How a prediction is made:
- the state sent to Jev is a compact JSON of selected features (no account, position or broker data);
- one `Choice` question asks for `up` / `down` / `flat` over the horizon; Jev returns calibrated probabilities
  and a confidence, which become `probability_up`, `probability_down` and `confidence`;
- expected volatility and expected return are NOT asked to Jev (TypeSafe documents weak arithmetic): they come
  from realized volatility, exactly as in the heuristic stand-in.

Reproducibility: every answer is recorded in a JSONL cache keyed by a hash of (model, state, question). A cached
answer is reused instead of calling the API again, so backtests re-run identically and for free, and
`verify` replays decisions OFFLINE from the cache (it never calls the API). The API model is pinned
(`api_model`); if the API reports a different model the call fails rather than mixing versions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from packages.common.entities import FeatureVector, JEVPrediction, ModelMetadata
from packages.common.errors import ConfigError, ModelError
from packages.jev.base import JEVModel, decide_direction

log = logging.getLogger(__name__)

API_KEY_ENV = "TYPESAFE_API_KEY"
LABELS = ("up", "down", "flat")
QUESTION_NAME = "direction"
PROMPT_VERSION = "1"  # bump whenever the state layout or the question text changes

STATE_FEATURES = (
    "ret_1",
    "ret_5",
    "ret_15",
    "ret_30",
    "rsi_14",
    "macd_hist_norm",
    "momentum_10_atr",
    "sma_20_dist",
    "sma_50_dist",
    "adx_14",
    "trend_strength_30",
    "realized_vol_30",
    "vol_ratio_20_100",
    "relative_volume_20",
    "vwap_dist",
    "spread_bps",
)


class TypeSafeJevParams(BaseModel):
    """Decision parameters: stored in `model_versions` and part of the model's identity."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    api_model: str = "jev-1.13.0"  # pinned; check the names your account accepts with `jev-check`
    prompt_version: str = PROMPT_VERSION
    entry_threshold: float = Field(default=0.55, ge=0.5, le=1.0)
    edge_scale: float = Field(default=0.5, ge=0.0)
    significant_digits: int = Field(default=6, ge=3, le=12)


class TypeSafeJevRuntime(BaseModel):
    """Runtime options: how answers are obtained, not what is decided (NOT part of the model's identity)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    offline: bool = False  # True: answers only from the cache, never the network (verify, CI)
    cache_path: str = "data/typesafe_jev_cache.jsonl"
    timeout_seconds: float = Field(default=3.0, gt=0)  # keep well under kill_switch.max_decision_latency_ms
    max_retries: int = Field(default=0, ge=0, le=3)
    usd_per_million_input_tokens: float = Field(
        default=0.042, ge=0
    )  # verify the current price at typesafe.ai


RUNTIME_KEYS = frozenset(TypeSafeJevRuntime.model_fields)


@dataclass(frozen=True)
class JevAnswer:
    choice: str
    confidence: float
    probabilities: dict[str, float]
    model: str
    input_tokens: int | None = None


class JevClient(Protocol):
    def decide(self, *, state: dict[str, Any], question: dict[str, Any], model: str) -> JevAnswer: ...


class SdkJevClient:
    """Thin wrapper over the official `typesafe-sdk` (synchronous client)."""

    def __init__(self, *, timeout_seconds: float, max_retries: int, transport: Any = None) -> None:
        """`transport` (an httpx2 transport) is for tests only; production uses the SDK's own HTTP client."""
        try:
            from typesafe_sdk import Choice, RetryPolicy, TypeSafeClient
        except ImportError as exc:  # pragma: no cover - depends on the optional extra
            raise ConfigError('typesafe-jev needs the optional SDK: pip install -e ".[typesafe]"') from exc
        if not os.environ.get(API_KEY_ENV, "").strip():
            raise ConfigError(f"typesafe-jev needs {API_KEY_ENV} in the environment (.env, never in git)")
        self._choice = Choice
        self._client = TypeSafeClient(
            timeout=timeout_seconds, retry=RetryPolicy(max_retries=max_retries), transport=transport
        )

    def decide(self, *, state: dict[str, Any], question: dict[str, Any], model: str) -> JevAnswer:
        choice = self._choice.model_validate(question)  # validated against the official schema before sending
        response = self._client.system_one(state=state, questions={QUESTION_NAME: choice}, model=model)
        answer = response.choices[QUESTION_NAME]
        return JevAnswer(
            choice=answer.choice,
            confidence=float(answer.confidence),
            probabilities={k: float(v) for k, v in answer.probabilities.items()},
            model=response.model,
            input_tokens=response.usage.input_tokens,
        )

    def list_models(self) -> list[str]:
        return [model.name for model in self._client.models.list().models]


def default_client_factory(runtime: TypeSafeJevRuntime) -> JevClient:
    return SdkJevClient(timeout_seconds=runtime.timeout_seconds, max_retries=runtime.max_retries)


# Tests replace this to run without network access.
CLIENT_FACTORY = default_client_factory


@dataclass
class JevUsage:
    api_calls: int = 0
    cache_hits: int = 0
    input_tokens: int = 0

    def cost_usd(self, usd_per_million: float) -> float:
        return self.input_tokens * usd_per_million / 1e6


class AnswerCache:
    """Append-only JSONL file of recorded answers keyed by request hash."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._answers: dict[str, JevAnswer] = {}
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    record = json.loads(line)
                    self._answers[record["key"]] = JevAnswer(**record["answer"])

    def __len__(self) -> int:
        return len(self._answers)

    def get(self, key: str) -> JevAnswer | None:
        return self._answers.get(key)

    def put(self, key: str, answer: JevAnswer) -> None:
        self._answers[key] = answer
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {"key": key, "answer": answer.__dict__}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def _round(value: float, digits: int) -> float | None:
    if not math.isfinite(value):
        return None
    return float(f"{value:.{digits}g}")


def request_key(model: str, state: Mapping[str, Any], question: Mapping[str, Any]) -> str:
    payload = json.dumps({"model": model, "state": state, "question": question}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class TypeSafeJEVModel(JEVModel):
    NAME = "typesafe-jev"
    REQUIRED = ("realized_vol_30", "ret_15", "rsi_14", "trend_strength_30")

    def __init__(
        self,
        *,
        namespace: str,
        version: str,
        feature_version: str,
        horizon_minutes: int,
        bar_minutes: int,
        params: Mapping[str, Any] | None = None,
        client: JevClient | None = None,
    ) -> None:
        options = dict(params or {})
        runtime_options = {k: options.pop(k) for k in list(options) if k in RUNTIME_KEYS}
        self.params = TypeSafeJevParams(**options)
        self.runtime = TypeSafeJevRuntime(**runtime_options)
        self._horizon_minutes = horizon_minutes
        self._horizon_bars = horizon_minutes / bar_minutes
        self._cache = AnswerCache(self.runtime.cache_path)
        self._client = client
        self.usage = JevUsage()
        metadata = ModelMetadata(
            model_name=self.NAME,
            model_version=version,
            feature_version=feature_version,
            horizon_minutes=horizon_minutes,
            params=self.params.model_dump(),
            description=f"TypeSafe Jev ({self.params.api_model}) direction decision. No validated edge.",
        )
        super().__init__(metadata, namespace=namespace)
        if client is None and not self.runtime.offline:
            self._client = CLIENT_FACTORY(self.runtime)

    @property
    def required_features(self) -> tuple[str, ...]:
        return self.REQUIRED

    @property
    def client(self) -> JevClient | None:
        return self._client

    @property
    def cost_usd(self) -> float:
        return self.usage.cost_usd(self.runtime.usd_per_million_input_tokens)

    def state_for(self, features: FeatureVector) -> dict[str, Any]:
        digits = self.params.significant_digits
        return {
            "instrument": features.symbol,
            "as_of": features.timestamp.isoformat(),
            "bar_timeframe": features.timeframe.value,
            "horizon_minutes": self._horizon_minutes,
            "features": {name: _round(features.get(name), digits) for name in STATE_FEATURES},
        }

    def question(self) -> dict[str, Any]:
        return {
            "type": "choice",
            "instructions": (
                "Given these technical indicators of a liquid US stock as of the close of the latest bar, "
                f"will the price be higher or lower {self._horizon_minutes} minutes later, by more than "
                "typical noise? Answer flat when there is no clear edge."
            ),
            "criteria": {
                "up": "The price is more likely to rise meaningfully over the horizon.",
                "down": "The price is more likely to fall meaningfully over the horizon.",
                "flat": "No clear directional edge; movement is likely noise.",
            },
        }

    def answer_for(self, features: FeatureVector) -> JevAnswer:
        state, question = self.state_for(features), self.question()
        key = request_key(self.params.api_model, state, question)
        cached = self._cache.get(key)
        if cached is not None:
            self.usage.cache_hits += 1
            return cached
        if self._client is None:
            raise ModelError(f"no recorded Jev answer for {features.feature_id} (offline mode)")
        answer = self._client.decide(state=state, question=question, model=self.params.api_model)
        self.usage.api_calls += 1
        self.usage.input_tokens += answer.input_tokens or 0
        if answer.model != self.params.api_model:
            raise ModelError(f"API answered with model {answer.model!r}, pinned {self.params.api_model!r}")
        if set(answer.probabilities) != set(LABELS):
            raise ModelError(f"unexpected Jev labels: {sorted(answer.probabilities)}")
        self._cache.put(key, answer)
        return answer

    def predict(self, features: FeatureVector) -> JEVPrediction:
        missing = features.missing(self.REQUIRED)
        if missing:
            raise ModelError(f"missing features: {missing}")
        answer = self.answer_for(features)
        total = sum(answer.probabilities.values())
        if total <= 0:
            raise ModelError("Jev returned no probability mass")
        probability_up = answer.probabilities["up"] / total
        probability_down = answer.probabilities["down"] / total
        vol = max(features.values["realized_vol_30"], 1e-6)
        expected_volatility = vol * math.sqrt(self._horizon_bars)
        expected_return = (probability_up - probability_down) * expected_volatility * self.params.edge_scale
        return self._prediction(
            features,
            direction=decide_direction(probability_up, probability_down, self.params.entry_threshold),
            probability_up=probability_up,
            probability_down=probability_down,
            expected_return=expected_return,
            expected_volatility=expected_volatility,
            confidence=min(1.0, max(0.0, answer.confidence)),
        )
