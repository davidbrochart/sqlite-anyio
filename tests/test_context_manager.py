import logging
import sqlite3

import pytest
import sqlite_anyio


async def test_context_manager_commit(anyio_backend):
    mem_uri = f"file:{anyio_backend}_mem0?mode=memory&cache=shared"
    async with await sqlite_anyio.connect(mem_uri, uri=True) as acon0:
        acur0 = await acon0.cursor()
        await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
        await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))

    async with await sqlite_anyio.connect(mem_uri, uri=True) as acon1:
        acur1 = await acon1.cursor()
        await acur1.execute("SELECT name FROM lang")
        assert await acur1.fetchone() == ("Python",)
        await acur1.execute("DROP TABLE IF EXISTS lang;")

async def test_context_manager_execute(anyio_backend):
    mem_uri = f"file:{anyio_backend}_mem0?mode=memory&cache=shared"
    async with await sqlite_anyio.connect(mem_uri, uri=True) as acon0:
        await acon0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
        await acon0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))

    async with await sqlite_anyio.connect(mem_uri, uri=True) as acon1:
        acur1 = await acon1.cursor()
        await acur1.execute("SELECT name FROM lang")
        assert await acur1.fetchone() == ("Python",)
        await acur1.execute("DROP TABLE IF EXISTS lang;")


async def test_context_manager_rollback(anyio_backend):
    mem_uri = f"file:{anyio_backend}_mem1?mode=memory&cache=shared"
    with pytest.raises(RuntimeError):
        async with await sqlite_anyio.connect(mem_uri, uri=True) as acon0:
            acur0 = await acon0.cursor()
            await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
            await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))
            raise RuntimeError("foo")

    async with await sqlite_anyio.connect(mem_uri, uri=True) as acon1:
        acur1 = await acon1.cursor()
        await acur1.execute("SELECT name FROM lang")
        assert await acur1.fetchone() is None
        await acur1.execute("DROP TABLE IF EXISTS lang;")


async def test_cursor_context_manager(anyio_backend, caplog):
    caplog.set_level(logging.INFO)
    mem_uri = f"file:{anyio_backend}_mem2?mode=memory&cache=shared"
    log = logging.getLogger("logger")
    async with await sqlite_anyio.connect(mem_uri, uri=True, exception_handler=sqlite_anyio.exception_logger, log=log) as acon0:
        async with await acon0.cursor() as acur0:
            await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")

        with pytest.raises(sqlite3.ProgrammingError):
            await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))

        async with await acon0.cursor() as acur1:
            await acur1.execute("SELECT name FROM lang")
            assert await acur1.fetchone() is None
            await acur1.execute("INSERT INTO foo(name) VALUES(?)", ("Python",))

    assert "SQLite exception" in caplog.text


async def test_exception_logger(anyio_backend, caplog):
    caplog.set_level(logging.INFO)
    mem_uri = f"file:{anyio_backend}_mem3?mode=memory&cache=shared"
    log = logging.getLogger("logger")
    async with await sqlite_anyio.connect(mem_uri, uri=True, exception_handler=sqlite_anyio.exception_logger, log=log) as acon0:
        acur0 = await acon0.cursor()
        await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
        await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))
        raise RuntimeError("foo")

    assert "SQLite exception" in caplog.text
