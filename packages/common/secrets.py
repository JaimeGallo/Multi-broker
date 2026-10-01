"""Secrets come only from the environment (or a local, git-ignored .env file).

They are never stored in YAML, in the database, in logs or in anything sent to a frontend.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

SENSITIVE_NAME = re.compile(r"(KEY|SECRET|TOKEN|PASSWORD|PASSWD|CREDENTIAL)", re.IGNORECASE)
URL_VARIABLES = ("DATABASE_URL", "REDIS_URL")


def get_secret(name: str, environ: Mapping[str, str] | None = None) -> str | None:
    env = os.environ if environ is None else environ
    value = env.get(name, "")
    return value or None


def sensitive_values(environ: Mapping[str, str] | None = None) -> list[str]:
    """Values that must never appear in logs: sensitive variables and passwords embedded in URLs."""
    env = os.environ if environ is None else environ
    values = [v for k, v in env.items() if v and len(v) >= 6 and SENSITIVE_NAME.search(k)]
    for name in URL_VARIABLES:
        url = env.get(name)
        if url:
            password = urlsplit(url).password
            if password and len(password) >= 4:
                values.append(password)
    return values


def redact_url(url: str | None) -> str | None:
    """Mask the password part of a connection URL."""
    if not url:
        return url
    parts = urlsplit(url)
    if not parts.password:
        return url
    netloc = parts.netloc.replace(f":{parts.password}@", ":***@")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def load_dotenv(
    path: str | Path = ".env", environ: MutableMapping[str, str] | None = None, *, override: bool = False
) -> list[str]:
    """Minimal .env loader (KEY=VALUE lines). Returns the names that were set; never logs values."""
    env = os.environ if environ is None else environ
    file = Path(path)
    if not file.exists():
        return []
    loaded: list[str] = []
    for line in file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and (override or key not in env):
            env[key] = value
            loaded.append(key)
    return loaded
