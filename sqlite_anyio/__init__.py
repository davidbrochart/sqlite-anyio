import importlib.metadata

from .sqlite import Connection as Connection
from .sqlite import Cursor as Cursor
from .sqlite import connect as connect
from .sqlite import exception_logger as exception_logger


try:
    __version__ = importlib.metadata.version("sqlite_anyio")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
