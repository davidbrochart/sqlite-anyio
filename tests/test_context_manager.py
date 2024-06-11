import logging
import sqlite3

import pytest
import sqlite_anyio


pytestmark = pytest.mark.anyio


async def test_context_manager_commit():
    mem_uri = "file:mem0?mode=memory&cache=shared"
    acon0 = await sqlite_anyio.connect(mem_uri, uri=True)
    acur0 = await acon0.cursor()
    async with acon0:
        await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
        await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))

    acon1 = await sqlite_anyio.connect(mem_uri, uri=True)
    acur1 = await acon1.cursor()
    await acur1.execute("SELECT name FROM lang")
    assert await acur1.fetchone() == ("Python",)


async def test_context_manager_rollback():
    mem_uri = "file:mem1?mode=memory&cache=shared"
    acon0 = await sqlite_anyio.connect(mem_uri, uri=True)
    acur0 = await acon0.cursor()
    with pytest.raises(RuntimeError):
        async with acon0:
            await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
            await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))
            raise RuntimeError("foo")

    acon1 = await sqlite_anyio.connect(mem_uri, uri=True)
    acur1 = await acon1.cursor()
    await acur1.execute("SELECT name FROM lang")
    assert await acur1.fetchone() is None


async def test_exception_logger(caplog):
    caplog.set_level(logging.INFO)
    mem_uri = "file:mem2?mode=memory&cache=shared"
    log = logging.getLogger("logger")
    acon0 = await sqlite_anyio.connect(mem_uri, uri=True, exception_handler=sqlite_anyio.exception_logger, log=log)
    acur0 = await acon0.cursor()
    async with acon0:
        await acur0.execute("CREATE TABLE lang(id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)")
        await acur0.execute("INSERT INTO lang(name) VALUES(?)", ("Python",))
        raise RuntimeError("foo")

    assert "SQLite exception" in caplog.text
