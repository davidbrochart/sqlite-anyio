from __future__ import annotations

__all__ = ["connect", "Connection", "Cursor"]

import sqlite3
import sys
import threading
from collections.abc import Callable, Sequence
from functools import partial
from logging import Logger, getLogger
from types import TracebackType
from typing import Any, TypeVar

import anyio
from anyio import to_thread, from_thread

if sys.version_info >= (3, 11):
    from typing import Self, TypeVarTuple, Unpack
else:  # pragma: nocover
    from exceptiongroup import BaseExceptionGroup
    from typing_extensions import Self, TypeVarTuple, Unpack

T_Retval = TypeVar("T_Retval")
PosArgsT = TypeVarTuple("PosArgsT")


async def _interruptible_dispatch(
    self: Connection | Cursor,
    func: Callable[[Unpack[PosArgsT]], T_Retval],
    *args: Unpack[PosArgsT]
) -> T_Retval:
    if isinstance(self, Connection):
        real_connection = self._real_connection
    elif isinstance(self, Cursor):
        real_connection = self._real_cursor.connection
    else:  # pragma: nocover
        raise AssertionError("Unknown type:", self)

    ev = anyio.Event()
    lock = threading.Lock()
    need_interrupt = False

    async def cancel_detector() -> None:
        try:
            await ev.wait()
        except anyio.get_cancelled_exc_class():
            # Block progress in the thread while checking this flag.
            # Our guard_interrupt thread only ever holds the lock briefly,
            # so there's no risk of blocking the event loop.
            with lock:
                # Due to race conditions, the first calls to interrupt may be
                # ignored. This race is quick so this loop should not cycle much.
                while need_interrupt:
                    real_connection.interrupt()
                    await anyio.lowlevel.cancel_shielded_checkpoint()
            # we do NOT re-raise the cancellation so that the task group
            # does not swallow our retval. If a Cancelled is to propagate,
            # it should come out of to_thread.run_sync

    def guard_interrupt() -> T_Retval:
        nonlocal need_interrupt

        with lock:
            from_thread.check_cancelled()
            need_interrupt = True
        try:
            return func(*args)
        except sqlite3.OperationalError as e:
            if str(e) == "interrupted":
                from_thread.check_cancelled()
            raise
        finally:
            need_interrupt = False

    try:
        async with anyio.create_task_group() as g:
            g.start_soon(cancel_detector)
            retval = await to_thread.run_sync(guard_interrupt, limiter=self._limiter)
            ev.set()
    except BaseExceptionGroup as eg:
        if len(eg.exceptions) == 1:
            if isinstance(eg.exceptions[0], Exception):
                raise eg.exceptions[0]
        raise  # pragma: nocover (would be an internal error that should fail other tests)

    return retval


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
        self._limiter = anyio.CapacityLimiter(1)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        if exc_val is None:
            await self.commit()
            return None

        assert exc_type is not None
        assert exc_val is not None
        assert exc_tb is not None
        await self.rollback()
        exception_handled = False
        if self._exception_handler is not None:
            exception_handled = self._exception_handler(exc_type, exc_val, exc_tb, self._log)
        return exception_handled

    async def execute(self, sql: str, parameters: Sequence[Any] = (), /) -> Cursor:
        real_cursor = await _interruptible_dispatch(self, self._real_connection.execute, sql, parameters)
        return Cursor(real_cursor, self._limiter, self._exception_handler, self._log)

    async def close(self) -> None:
        with anyio.CancelScope(shield=True):
            await to_thread.run_sync(self._real_connection.close, limiter=self._limiter)

    async def commit(self) -> None:
        await _interruptible_dispatch(self, self._real_connection.commit)

    async def rollback(self) -> None:
        with anyio.CancelScope(shield=True):
            await to_thread.run_sync(self._real_connection.rollback, limiter=self._limiter)

    async def cursor(self, factory: Callable[[sqlite3.Connection], sqlite3.Cursor] = sqlite3.Cursor) -> Cursor:
        real_cursor = await to_thread.run_sync(self._real_connection.cursor, factory, limiter=self._limiter)
        return Cursor(real_cursor, self._limiter, self._exception_handler, self._log)


class Cursor:
    def __init__(
        self,
        real_cursor: sqlite3.Cursor,
        limiter: anyio.CapacityLimiter,
        _exception_handler: Callable[[type[BaseException], BaseException, TracebackType, Logger], bool] | None,
        _log: Logger,
    ) -> None:
        self._real_cursor = real_cursor
        self._limiter = limiter
        self._exception_handler = _exception_handler
        self._log = _log

    @property
    def description(self) -> Any:
        return self._real_cursor.description

    @property
    def rowcount(self) -> int:
        return self._real_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._real_cursor.arraysize

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        await self.close()
        if exc_val is None:
            return None

        assert exc_type is not None
        assert exc_val is not None
        assert exc_tb is not None
        exception_handled = False
        if self._exception_handler is not None:
            exception_handled = self._exception_handler(exc_type, exc_val, exc_tb, self._log)
        return exception_handled

    async def close(self) -> None:
        with anyio.CancelScope(shield=True):
            await to_thread.run_sync(self._real_cursor.close, limiter=self._limiter)

    async def execute(self, sql: str, parameters: Sequence[Any] = (), /) -> Cursor:
        await _interruptible_dispatch(self, self._real_cursor.execute, sql, parameters)
        return self

    async def executemany(self, sql: str, parameters: Sequence[Any], /) -> Cursor:
        await _interruptible_dispatch(self, self._real_cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str, /) -> Cursor:
        await _interruptible_dispatch(self, self._real_cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        return await _interruptible_dispatch(self, self._real_cursor.fetchone)

    async def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        return await _interruptible_dispatch(self, self._real_cursor.fetchmany, size)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return await _interruptible_dispatch(self, self._real_cursor.fetchall)


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
