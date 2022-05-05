from typing import Optional
from pathlib import Path
import contextlib

from pymongolite.exceptions import MissingDatabaseName
from pymongolite.database import Database
from pymongolite.backend.command import COMMANDS, Command
from pymongolite.backend.session import Session


class MongoClient:
    def __init__(self, dirpath: str, database: Optional[str] = None):
        self.dirpath = dirpath
        self.__session = Session(self.dirpath)
        self.__default_database_name = database
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def path(self) -> str:
        return str(Path(self.dirpath).absolute())

    def get_database(self, name: Optional[str] = None):
        if name is None:
            if self.__default_database_name is None:
                raise MissingDatabaseName()
            name = self.__default_database_name

        return Database(self, name)

    def drop_database(self, name: Optional[str] = None):
        if name is None:
            if self.__default_database_name is None:
                raise MissingDatabaseName()
            name = self.__default_database_name

        self.__session.exc_command(
            command=Command(cmd=COMMANDS.drop_database, database_name=name),
        )

    def get_default_database(self, default: Optional[str] = None):
        if default is None:
            if self.__default_database_name is None:
                raise MissingDatabaseName()

            default = self.__default_database_name

        return Database(self, default)

    def close(self):
        self._closed = True
        self.__session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @contextlib.contextmanager
    def _open_session(self):
        yield self.__session
