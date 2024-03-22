import anyio
import pytest
import sqlite3
import sqlite_anyio


pytestmark = pytest.mark.anyio


async def test_connection(tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("sqlite")
    pytest.db_path = dir_path / "sqlite.db"
    pytest.adb_path = dir_path / "asqlite.db"

    pytest.con = sqlite3.connect(pytest.db_path)
    pytest.acon = await sqlite_anyio.connect(pytest.adb_path)

    pytest.db_content = pytest.db_path.read_bytes()
    pytest.adb_content = pytest.adb_path.read_bytes()
    assert pytest.db_content == pytest.adb_content == b""


async def test_cursor_execute():
    pytest.cur = pytest.con.cursor()
    pytest.acur = await pytest.acon.cursor()

    command = "CREATE TABLE movie(title, year, score)"
    pytest.cur.execute(command)
    await pytest.acur.execute(command)

    command = "SELECT name FROM sqlite_master"
    res = pytest.cur.execute(command)
    ares = await pytest.acur.execute(command)
    assert res.fetchone() == await ares.fetchone() == ("movie",)

    command = "SELECT name FROM sqlite_master WHERE name='spam'"
    res = pytest.cur.execute(command)
    ares = await pytest.acur.execute(command)
    assert res.fetchone() is await ares.fetchone() is None

    pytest.db_content = pytest.db_path.read_bytes()
    pytest.adb_content = pytest.adb_path.read_bytes()
    assert pytest.db_content == pytest.adb_content != b""


async def test_cursor_description():
    assert pytest.cur.description == pytest.acur.description == (("name", None, None, None, None, None, None),)


async def test_commit():
    command = """INSERT INTO movie VALUES
        ('Monty Python and the Holy Grail', 1975, 8.2),
        ('And Now for Something Completely Different', 1971, 7.5)
    """
    pytest.cur.execute(command)
    await pytest.acur.execute(command)

    pytest.con.commit()
    await pytest.acon.commit()

    assert pytest.db_content != pytest.db_path.read_bytes()
    pytest.db_content = pytest.db_path.read_bytes()
    pytest.adb_content = pytest.adb_path.read_bytes()
    assert pytest.db_content == pytest.adb_content


async def test_cursor_rowcount():
    assert pytest.cur.rowcount == pytest.acur.rowcount == 2


async def test_cursor_arraysize():
    assert pytest.cur.arraysize == pytest.acur.arraysize == 1


async def test_cursor_executemany():
    rows = [
        ("foo", 1970, 7.0),
        ("bar", 1971, 7.1),
    ]
    command = "INSERT INTO movie VALUES(?, ?, ?)"
    pytest.cur.executemany(command, rows)
    await pytest.acur.executemany(command, rows)

    pytest.con.commit()
    await pytest.acon.commit()

    assert pytest.db_content != pytest.db_path.read_bytes()
    pytest.db_content = pytest.db_path.read_bytes()
    pytest.adb_content = pytest.adb_path.read_bytes()
    assert pytest.db_content == pytest.adb_content



async def test_rollback():
    command = """INSERT INTO movie VALUES
        ('foo', 1970, 7.1)
    """
    pytest.cur.execute(command)
    await pytest.acur.execute(command)

    pytest.con.rollback()
    await pytest.acon.rollback()

    assert pytest.db_content == pytest.db_path.read_bytes() == pytest.adb_path.read_bytes()


async def test_fetchall():
    command = "SELECT title FROM movie"
    res = pytest.cur.execute(command)
    ares = await pytest.acur.execute(command)
    assert res.fetchall() == await ares.fetchall() == [
        ("Monty Python and the Holy Grail",),
        ("And Now for Something Completely Different",),
        ("foo",),
        ("bar",),
    ]


async def test_fetchmany():
    command = "SELECT title FROM movie"
    res = pytest.cur.execute(command)
    ares = await pytest.acur.execute(command)
    assert res.fetchmany(3) == await ares.fetchmany(3) == [
        ("Monty Python and the Holy Grail",),
        ("And Now for Something Completely Different",),
        ("foo",),
    ]


async def test_cursor_executescript():
    script = """
        BEGIN;
        CREATE TABLE person(firstname, lastname, age);
        CREATE TABLE book(title, author, published);
        CREATE TABLE publisher(name, address);
        COMMIT;
    """
    pytest.cur.executescript(script)
    await pytest.acur.executescript(script)
    assert pytest.db_content != pytest.db_path.read_bytes()
    pytest.db_content = pytest.db_path.read_bytes()
    pytest.adb_content = pytest.adb_path.read_bytes()
    assert pytest.db_content == pytest.adb_content


async def test_cursor_close():
    pytest.cur.close()
    await pytest.acur.close()

    command = "SELECT title FROM movie"
    with pytest.raises(sqlite3.ProgrammingError) as excinfo:
        res = pytest.cur.execute(command)
    assert str(excinfo.value) == "Cannot operate on a closed cursor."
    with pytest.raises(sqlite3.ProgrammingError) as excinfo:
        res = await pytest.acur.execute(command)
    assert str(excinfo.value) == "Cannot operate on a closed cursor."


async def test_close():
    pytest.con.close()
    await pytest.acon.close()

    with pytest.raises(sqlite3.ProgrammingError) as excinfo:
        pytest.con.cursor()
    assert str(excinfo.value) == "Cannot operate on a closed database."
    with pytest.raises(sqlite3.ProgrammingError) as excinfo:
        await pytest.acon.cursor()
    assert str(excinfo.value) == "Cannot operate on a closed database."

    # check that there is no lock on the file, especially for Windows where there can be:
    # PermissionError: [WinError 32] The process cannot access the file because it is being used by another process
    await anyio.Path(pytest.db_path).rename(pytest.db_path.with_suffix(".keep"))
    await anyio.Path(pytest.adb_path).rename(pytest.adb_path.with_suffix(".keep"))
