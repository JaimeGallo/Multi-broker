"""Async database access (SQLite for local development, PostgreSQL via DATABASE_URL)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from packages.persistence.models import Base

_SQLITE_PREFIXES = ("sqlite+aiosqlite:///", "sqlite:///")


def _ensure_sqlite_directory(url: str) -> None:
    for prefix in _SQLITE_PREFIXES:
        if url.startswith(prefix):
            path = url[len(prefix) :]
            if path and path != ":memory:":
                Path(path).parent.mkdir(parents=True, exist_ok=True)
            return


def _sqlite_pragmas(dbapi_connection: Any, _record: Any) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()


class Database:
    def __init__(self, url: str, *, echo: bool = False) -> None:
        self.url = url
        if url.startswith("sqlite"):
            _ensure_sqlite_directory(url)
        self.engine: AsyncEngine = create_async_engine(url, echo=echo)
        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine.sync_engine, "connect", _sqlite_pragmas)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    @property
    def dialect(self) -> str:
        return self.engine.dialect.name

    async def create_all(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def dispose(self) -> None:
        await self.engine.dispose()
