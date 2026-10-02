"""Identifier helpers.

Decision identifiers (features, predictions, signals, orders) are DETERMINISTIC: the same inputs always produce
the same id. Reprocessing a bar after a restart regenerates the same `client_order_id`, which the execution
engine and the broker deduplicate. The `namespace` scopes ids: the run id in backtests, a stable
`strategy:mode:broker` string in paper/shadow so that ids survive restarts.
"""

from __future__ import annotations

import hashlib
import re
import secrets
import time
from datetime import datetime

from packages.common.enums import OrderIntent

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_NON_ALNUM = re.compile(r"[^A-Za-z0-9]")


def _base32(data: bytes, length: int) -> str:
    number = int.from_bytes(data, "big")
    chars: list[str] = []
    for _ in range(length):
        chars.append(_CROCKFORD[number & 31])
        number >>= 5
    return "".join(reversed(chars))


def digest(*parts: object, length: int = 10) -> str:
    """Stable short hash of the given parts (Crockford base32)."""
    raw = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return _base32(hashlib.sha256(raw).digest(), length)


def new_id(prefix: str) -> str:
    """Random, roughly time-sortable id for records that need no determinism (events, runs)."""
    millis = int(time.time() * 1000).to_bytes(6, "big")
    return f"{prefix}_{_base32(millis + secrets.token_bytes(10), 26)}"


def compact_symbol(symbol: str) -> str:
    return _NON_ALNUM.sub("", symbol).upper()


def _stamp(timestamp: datetime) -> str:
    return timestamp.strftime("%Y%m%d%H%M")


def make_feature_id(
    namespace: str, symbol: str, timestamp: datetime, feature_version: str, spec_hash: str
) -> str:
    code = digest(namespace, symbol, timestamp.isoformat(), feature_version, spec_hash, length=8)
    return f"F-{compact_symbol(symbol)}-{_stamp(timestamp)}-{code}"


def make_prediction_id(
    namespace: str, model_name: str, model_version: str, symbol: str, timestamp: datetime
) -> str:
    code = digest(namespace, model_name, model_version, symbol, timestamp.isoformat(), length=8)
    return f"P-{compact_symbol(symbol)}-{_stamp(timestamp)}-{code}"


def make_signal_id(namespace: str, strategy: str, symbol: str, timestamp: datetime) -> str:
    code = digest(namespace, strategy, symbol, timestamp.isoformat(), length=8)
    return f"S-{compact_symbol(symbol)}-{_stamp(timestamp)}-{code}"


def make_client_order_id(signal_id: str, intent: OrderIntent, attempt: int = 1) -> str:
    suffix = intent.code if attempt <= 1 else f"{intent.code}{attempt}"
    return f"jev-{signal_id}-{suffix}"


def make_decision_id(signal_id: str) -> str:
    return "R" + signal_id[1:]


def make_trade_id(signal_id: str) -> str:
    return "T" + signal_id[1:]
