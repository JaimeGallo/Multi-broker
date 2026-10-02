"""Minimal WebSocket contract shared by the real-time adapters (Alpaca market data and trade updates).

Adapters depend on `Connector` only, so tests inject in-memory sockets and production uses the `websockets`
library (installed with `pip install -e ".[alpaca]"`).
"""

from __future__ import annotations

import json
import random
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from packages.common.errors import ConfigError


class WebSocketLike(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


Connector = Callable[[str], Awaitable[WebSocketLike]]


async def websockets_connector(url: str) -> WebSocketLike:
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ConfigError(
            'real-time streams need the websockets package: pip install -e ".[alpaca]"'
        ) from exc
    connection: WebSocketLike = await websockets.connect(
        url, open_timeout=15, ping_interval=20, ping_timeout=20, max_size=2**23
    )
    return connection


def decode(message: str | bytes) -> Any:
    return json.loads(message.decode("utf-8") if isinstance(message, bytes) else message)


def backoff_seconds(
    attempt: int, *, base: float = 1.0, cap: float = 30.0, rng: random.Random | None = None
) -> float:
    """Exponential backoff with full jitter: attempt 0 -> up to 1 s, 1 -> 2 s, ... capped at `cap`."""
    ceiling = min(cap, base * (2**attempt))
    return (rng or random).uniform(ceiling / 2, ceiling)
