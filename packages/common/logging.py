"""Structured JSON logging with secret redaction."""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TextIO

_RESERVED = set(logging.LogRecord("x", logging.INFO, "x", 0, "x", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}
_SENSITIVE_FIELD = re.compile(r"(key|secret|token|password|authorization)", re.IGNORECASE)
_HANDLER_NAME = "jev-handler"


def _extras(record: logging.LogRecord) -> dict[str, object]:
    return {k: v for k, v in record.__dict__.items() if k not in _RESERVED and not k.startswith("_")}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(_extras(record))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        stamp = datetime.fromtimestamp(record.created, UTC).strftime("%H:%M:%S")
        extras = " ".join(f"{k}={v}" for k, v in _extras(record).items())
        line = f"{stamp} {record.levelname:<7} {record.name}: {record.getMessage()}"
        if extras:
            line = f"{line} | {extras}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


class RedactingFilter(logging.Filter):
    """Masks known secret values anywhere in the message and any field whose name looks sensitive."""

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__()
        self._secrets = sorted({s for s in secrets if s and len(s) >= 4}, key=len, reverse=True)

    def redact(self, text: str) -> str:
        for secret in self._secrets:
            text = text.replace(secret, "***")
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redact(record.getMessage())
        record.args = None
        for key, value in list(record.__dict__.items()):
            if key in _RESERVED or key.startswith("_"):
                continue
            if _SENSITIVE_FIELD.search(key):
                record.__dict__[key] = "***"
            elif isinstance(value, str):
                record.__dict__[key] = self.redact(value)
        return True


def configure_logging(
    level: str = "INFO",
    *,
    json_format: bool = True,
    secrets: Iterable[str] = (),
    stream: TextIO | None = None,
) -> None:
    """Install (or replace) the platform handler on the root logger."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if handler.get_name() == _HANDLER_NAME:
            root.removeHandler(handler)
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(JsonFormatter() if json_format else TextFormatter())
    handler.addFilter(RedactingFilter(secrets))
    root.addHandler(handler)
    root.setLevel(level.upper())
