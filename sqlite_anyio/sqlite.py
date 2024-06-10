from __future__ import annotations

__all__ = ["connect", "Connection", "Cursor"]

import sqlite3
from collections.abc import Callable, Sequence
from functools import partial, wraps
from logging import Logger, getLogger
from types import TracebackType
from typing import Any

from anyio import CapacityLimiter, to_thread


class Connection:
    def __init__(
        self,
        _real_connection: sqlite3.Connection,
        _exception_handler: Callable[[type[BaseException], BaseException, TracebackType, Logger], bool] | None = None,
        _log: Logger | None = None,
    ) -> None:
        self._real_connection = _real_connection
        self._exception_handler = _exception_handler
        self._log = _log or getLogger(__name__)
        self._limiter = CapacityLimiter(1)

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        if exc_val is None:
            await self.commit()  # type: ignore[call-arg]
            return None

        assert exc_type is not None
        assert exc_val is not None
        assert exc_tb is not None
        await self.rollback()  # type: ignore[call-arg]
        exception_handled = False
        if self._exception_handler is not None:
            exception_handled = self._exception_handler(exc_type, exc_val, exc_tb, self._log)
        return exception_handled

    @wraps(sqlite3.Connection.close)
    async def close(self):
        return await to_thread.run_sync(self._real_connection.close, limiter=self._limiter)

    @wraps(sqlite3.Connection.commit)
    async def commit(self):
        return await to_thread.run_sync(self._real_connection.commit, limiter=self._limiter)

    @wraps(sqlite3.Connection.rollback)
    async def rollback(self):
        return await to_thread.run_sync(self._real_connection.rollback, limiter=self._limiter)

    async def cursor(self, factory: Callable[[sqlite3.Connection], sqlite3.Cursor] = sqlite3.Cursor) -> Cursor:
        real_cursor = await to_thread.run_sync(self._real_connection.cursor, factory, limiter=self._limiter)
        return Cursor(real_cursor, self._limiter)


class Cursor:
    def __init__(self, real_cursor: sqlite3.Cursor, limiter: CapacityLimiter) -> None:
        self._real_cursor = real_cursor
        self._limiter = limiter

    @property
    def description(self) -> Any:
        return self._real_cursor.description

    @property
    def rowcount(self) -> int:
        return self._real_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._real_cursor.arraysize

    @wraps(sqlite3.Cursor.close)
    async def close(self) -> None:
        await to_thread.run_sync(self._real_cursor.close, limiter=self._limiter)

    @wraps(sqlite3.Cursor.execute)
    async def execute(self, sql: str, parameters: Sequence[Any] = (), /) -> Cursor:
        real_cursor = await to_thread.run_sync(self._real_cursor.execute, sql, parameters, limiter=self._limiter)
        return Cursor(real_cursor, self._limiter)

    @wraps(sqlite3.Cursor.executemany)
    async def executemany(self, sql: str, parameters: Sequence[Any], /) -> Cursor:
        real_cursor = await to_thread.run_sync(self._real_cursor.executemany, sql, parameters, limiter=self._limiter)
        return Cursor(real_cursor, self._limiter)

    @wraps(sqlite3.Cursor.executescript)
    async def executescript(self, sql_script: str, /) -> Cursor:
        real_cursor = await to_thread.run_sync(self._real_cursor.executescript, sql_script, limiter=self._limiter)
        return Cursor(real_cursor, self._limiter)

    @wraps(sqlite3.Cursor.fetchone)
    async def fetchone(self) -> tuple[Any, ...] | None:
        return await to_thread.run_sync(self._real_cursor.fetchone, limiter=self._limiter)

    @wraps(sqlite3.Cursor.fetchmany)
    async def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        return await to_thread.run_sync(self._real_cursor.fetchmany, size, limiter=self._limiter)

    @wraps(sqlite3.Cursor.fetchall)
    async def fetchall(self) -> list[tuple[Any, ...]]:
        return await to_thread.run_sync(self._real_cursor.fetchall, limiter=self._limiter)


async def connect(
    database: str,
    uri: bool | None = None,
    exception_handler: Callable[[type[BaseException], BaseException, TracebackType, Logger], bool] | None = None,
    log: Logger | None = None,
) -> Connection:
    real_connection = await to_thread.run_sync(
        partial(sqlite3.connect, database, uri=uri, check_same_thread=False)
    )
    return Connection(real_connection, exception_handler, log)


def exception_logger(
    exc_type: type[BaseException] | None,
    exc_val: BaseException | None,
    exc_tb: TracebackType | None,
    log: Logger,
) -> bool:
    """An exception handler that logs the exception and discards it."""
    log.error("SQLite exception", exc_info=exc_val)
    return True  # the exception was handled
