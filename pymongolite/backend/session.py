from typing import Any
from pathlib import Path

from .exceptions import SessionClosedError
from .storage_engine.files_engine import FilesEngine
from .indexing_engine.v1_engine import V1Engine
from .execution_engine.chunked_engine import ChunkedEngine
from pymongolite.backend.command import Command


class Session:
    def __init__(self, dirpath: str, **kwargs):
        self.__dirpath = Path(dirpath)
        self._storage_engine = FilesEngine(self.__dirpath, **kwargs)
        self._indexing_engine = V1Engine()
        self._execution_engine = ChunkedEngine(
            storage_engine=self._storage_engine, indexing_engine=self._indexing_engine
        )
        self._closed = False

    def exc_command(self, command: Command) -> Any:
        if self.closed:
            raise SessionClosedError()

        return self._execution_engine.execute_command(command)

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self):
        self._closed = True

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
