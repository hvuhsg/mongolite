from typing import Any, Union
from threading import RLock
from pathlib import Path

from command import Command, COMMANDS
from backend.storage_engine import StorageEngine
from backend.utils import document_filter_match, update_with_fields, grouper, update_document_with_override
from backend.exceptions import DatabaseIsRequired, CollectionIsRequired

MAX_TIMEOUT = 20


class Session:
    def __init__(self, dirpath: str, **kwargs):
        self.__dirpath = Path(dirpath)
        self._storage_engine = StorageEngine(self.__dirpath, **kwargs)
        self.__lock = RLock()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def _enforce_database(self, database: Union[str, None]):
        if database is None:
            raise DatabaseIsRequired()

    def _enforce_collection(self, database: Union[str, None], collection: Union[str, None]):
        self._enforce_database(database)
        if collection is None:
            raise CollectionIsRequired()

    def exc_command(
            self,
            command: Command,
            database: str = None,
            collection: str = None,
    ) -> Any:
        if command.cmd == COMMANDS.drop_database:
            self._enforce_database(database)
            return self._storage_engine.drop_database(database)

        if command.cmd == COMMANDS.create_database:
            self._enforce_database(database)
            return self._storage_engine.create_database(database)

        if command.cmd == COMMANDS.drop_collection:
            self._enforce_collection(database, collection)
            return self._storage_engine.drop_collection(database, collection)

        if command.cmd == COMMANDS.create_collection:
            self._enforce_collection(database, collection)
            return self._storage_engine.create_collection(database, collection)

        if command.cmd == COMMANDS.get_collection_list:
            self._enforce_database(database)
            return self._storage_engine.collection_list(database)

        if command.cmd == COMMANDS.insert:
            self._enforce_collection(database, collection)
            return self._storage_engine.add_documents(database, collection, command.documents)

        if command.cmd == COMMANDS.delete:
            self._enforce_collection(database, collection)
            return self.delete(command, database, collection)

        if command.cmd == COMMANDS.find:
            self._enforce_collection(database, collection)
            return self.find(command, database, collection)

        if command.cmd == COMMANDS.update:
            self._enforce_collection(database, collection)
            return self.update(command, database, collection)

        if command.cmd == COMMANDS.replace:
            self._enforce_collection(database, collection)
            return self.replace(command, database, collection)

        return None

    def close(self):
        self._closed = True

    def _iter_read_documents(self, database: str, collection: str, buffer_size: int = 10):
        offset_id = self._storage_engine.create_read_offset()
        while True:
            documents = self._storage_engine.get_documents(
                database,
                collection,
                number_of_documents=buffer_size,
                offset_id=offset_id,
            )

            if not documents:
                break

            yield from documents

    def _iter_documents_filtered(self, database: str, collection: str, filter: dict, **kwargs):
        for document in self._iter_read_documents(database, collection, **kwargs):
            if document_filter_match(document.data, filter):
                yield document

    def find(self, command: Command, database: str, collection: str):
        for document in self._iter_documents_filtered(database, collection, command.filter):
            fields = {}
            if command.fields:
                fields = command.fields

            yield update_with_fields(document.data, fields)

    def update(self, command: Command, database: str, collection: str):
        group_size = 10

        for documents in grouper(
                group_size,
                self._iter_documents_filtered(
                    database,
                    collection,
                    command.filter,
                    buffer_size=group_size
                )
        ):
            if not command.many:
                documents = documents[:1]

            documents_updated = {}
            for document in documents:
                updated_document = update_document_with_override(document.data, command.override)
                if updated_document != document.data:
                    documents_updated[document] = updated_document

            if documents_updated:
                self._storage_engine.update_documents(documents_updated)

            if command.many:
                break

    def delete(self, command: Command, database: str, collection: str):
        group_size = 10

        for documents in grouper(
                group_size,
                self._iter_documents_filtered(
                    database,
                    collection,
                    command.filter,
                    buffer_size=group_size
                )
        ):
            if not command.many:
                documents = documents[:1]

            self._storage_engine.delete_documents(documents)

            if not command.many:
                break

    def replace(self, command: Command, database: str, collection: str):
        group_size = 10

        for documents in grouper(
                group_size,
                self._iter_documents_filtered(
                    database,
                    collection,
                    command.filter,
                    buffer_size=group_size
                )
        ):
            if not command.many:
                documents = documents[:1]

            documents_updated = {}
            for document in documents:
                if command.replacement != document.data:
                    documents_updated[document] = command.replacement

            if documents_updated:
                self._storage_engine.update_documents(documents_updated)

            if not command.many:
                break
