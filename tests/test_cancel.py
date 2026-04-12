import anyio
import pytest
import sqlite3

from anyio import CancelScope, move_on_after

import sqlite_anyio


pytestmark = pytest.mark.anyio


@pytest.fixture(scope="function")
async def acon():
    return await sqlite_anyio.connect(":memory:")


async def test_connection_and_close(tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("sqlite_cancel")
    adb_path = dir_path / "asqlite_cancel.db"

    acon = None

    with CancelScope() as c:
        c.cancel()
        acon = await sqlite_anyio.connect(adb_path)

    assert not adb_path.exists()
    assert acon is None

    with move_on_after(0.0001) as c:
        acon = await sqlite_anyio.connect(adb_path)
    # assert c.cancel_called
    assert acon is not None

    with CancelScope() as c:
        c.cancel()
        await acon.close()
    assert c.cancel_called

    with pytest.raises(sqlite3.ProgrammingError) as excinfo:
        await acon.cursor()
    assert str(excinfo.value) == "Cannot operate on a closed database."

    # check that there is no lock on the file, especially for Windows where there can be:
    # PermissionError: [WinError 32] The process cannot access the file because it is being used by another process
    adb_path.rename(adb_path.with_suffix(".keep"))


async def test_execute(acon):
    command = "CREATE TABLE connums(num)"
    ares = None
    with CancelScope() as c:
        c.cancel()
        ares = await acon.execute(command)
    assert ares is None
    # with move_on_after(.0001) as c:
    #     ares = await acon.execute(command)
    # assert ares is None

    command = """SELECT count(*) FROM sqlite_master
    WHERE type='table' AND name='connums';"""
    ares = await acon.execute(command)
    assert await ares.fetchone() == (0,)

    # cursor too
    acur = ares

    ares = None
    command = "CREATE TABLE curnums(num)"
    with CancelScope() as c:
        c.cancel()
        ares = await acur.execute(command)
    assert await acur.fetchone() is None

    command = """SELECT count(*) FROM sqlite_master
    WHERE type='table' AND name='curnums';"""
    ares = await acur.execute(command)
    assert await ares.fetchone() == (0,)


async def test_commit(acon):
    acur = await acon.execute("BEGIN")
    await acur.execute("CREATE TABLE foo(bar)")
    await acur.execute("COMMIT")

    command = """    
        WITH RECURSIVE c(x) AS
        (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x < ?)
        INSERT INTO foo
        SELECT x FROM c;
    """
    value = (10000,)  # Tune this delay for CI

    await acur.execute("BEGIN DEFERRED")
    await acur.execute(command, value)
    with CancelScope() as c:
        c.cancel()
        await acon.commit()
    await acon.rollback()

    command = """SELECT count(*) FROM foo;"""
    await acur.execute(command)
    assert await acur.fetchone() < value


async def test_execute_long(acon):
    acur = await acon.execute("BEGIN")
    await acur.execute("CREATE TABLE foo(bar)")
    await acur.execute("COMMIT")

    command = """    
        WITH RECURSIVE c(x) AS
        (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x < ?)
        INSERT INTO foo
        SELECT x FROM c;
    """
    value = (1000000,)  # Tune this delay for CI

    await acur.execute("BEGIN DEFERRED")
    with move_on_after(0.001):
        await acur.execute(command, value)

    command = """SELECT count(*) FROM foo;"""
    await acur.execute(command)
    assert await acur.fetchone() < value


async def test_cursor_close(acon):
    acur = await acon.cursor()
    with CancelScope() as c:
        c.cancel()
        await acur.close()
    assert c.cancel_called

    command = "SELECT title FROM movie"
    with pytest.raises(sqlite3.ProgrammingError) as excinfo:
        res = await acur.execute(command)
    assert str(excinfo.value) == "Cannot operate on a closed cursor."
