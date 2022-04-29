from typing import List, Union
from threading import RLock

from .base_engine import BaseEngine
from .exceptions import DatabaseIsRequired, CollectionIsRequired
from .utils import (
    document_filter_match,
    update_document_with_override,
    update_with_fields,
    grouper,
)
from ..storage_engine.base_engine import BaseEngine as BaseStorageEngine
from ..storage_engine.read_instructions import ReadInstructions
from ..storage_engine.insert_instruction import InsertInstructions
from ..storage_engine.update_instructions import UpdateInstructions
from pymongolite.command import Command, COMMANDS

DEFAULT_CHUNK_SIZE = 5 * 1024


class ChunkedEngine(BaseEngine):
    def __init__(
        self, storage_engine: BaseStorageEngine, chunk_size: int = DEFAULT_CHUNK_SIZE
    ):
        self.__lock = RLock()
        self._closed = False
        self._chunk_size = chunk_size

        super().__init__(storage_engine=storage_engine)

    def execute_command(self, command: Command):
        self._raise_on_none_database(command.database_name)

        if command.cmd == COMMANDS.drop_database:
            return self.drop_database(database_name=command.database_name)

        if command.cmd == COMMANDS.create_database:
            return self.create_database(database_name=command.database_name)

        if command.cmd == COMMANDS.drop_collection:
            self._raise_on_none_collection(command.collection_name)
            return self.drop_collection(
                database_name=command.database_name,
                collection_name=command.collection_name,
            )

        if command.cmd == COMMANDS.create_collection:
            self._raise_on_none_collection(command.collection_name)
            return self.create_collection(
                database_name=command.database_name,
                collection_name=command.collection_name,
            )

        if command.cmd == COMMANDS.get_collection_list:
            return self.get_collections_list(database_name=command.database_name)

        if command.cmd == COMMANDS.insert:
            self._raise_on_none_collection(command.collection_name)
            return self.insert(
                database_name=command.database_name,
                collection_name=command.collection_name,
                documents=command.documents,
            )

        if command.cmd == COMMANDS.delete:
            self._raise_on_none_collection(command.collection_name)
            return self.delete(
                database_name=command.database_name,
                collection_name=command.collection_name,
                filter_=command.filter,
                many=command.many,
            )

        if command.cmd == COMMANDS.find:
            self._raise_on_none_collection(command.collection_name)
            return self.find(
                database_name=command.database_name,
                collection_name=command.collection_name,
                filter_=command.filter,
                fields=command.fields,
                many=command.many,
            )

        if command.cmd == COMMANDS.update:
            self._raise_on_none_collection(command.collection_name)
            return self.update(
                database_name=command.database_name,
                collection_name=command.collection_name,
                filter_=command.filter,
                override=command.override,
                many=command.many,
            )

        if command.cmd == COMMANDS.replace:
            self._raise_on_none_collection(command.collection_name)
            return self.replace(
                database_name=command.database_name,
                collection_name=command.collection_name,
                filter_=command.filter,
                replacement=command.replacement,
                many=command.many,
            )

        return None

    def create_database(self, database_name: str) -> bool:
        return self._storage_engine.create_database(database_name=database_name)

    def drop_database(self, database_name: str) -> bool:
        return self._storage_engine.drop_database(database_name=database_name)

    def create_collection(self, database_name: str, collection_name: str) -> bool:
        return self._storage_engine.create_collection(
            database_name=database_name,
            collection_name=collection_name,
        )

    def drop_collection(self, database_name: str, collection_name: str) -> bool:
        return self._storage_engine.drop_collection(
            database_name=database_name,
            collection_name=collection_name,
        )

    def get_collections_list(self, database_name: str) -> List[str]:
        return self._storage_engine.get_collections_list(database_name=database_name)

    def find(
        self,
        database_name: str,
        collection_name: str,
        filter_: dict,
        fields: dict = None,
        many: bool = True,
        **kwargs
    ):
        if fields is None:
            fields = {}

        for document in self._iter_documents_filtered(
            database_name, collection_name, filter_
        ):
            yield update_with_fields(document.data, fields)

            # find_one stop iterating after returning one
            if not many:
                break

    def update(
        self,
        database_name: str,
        collection_name: str,
        filter_: dict,
        override: dict,
        many: bool = True,
    ):
        for documents in grouper(
            self._chunk_size,
            self._iter_documents_filtered(database_name, collection_name, filter_),
        ):
            documents_updated = {}

            for document in documents:
                updated_document = update_document_with_override(
                    document.data, override
                )

                # Document was updated
                if updated_document != document.data:
                    documents_updated[document.index] = updated_document

                    if not many:
                        break

            if documents_updated:
                self._storage_engine.update_documents(
                    database_name=database_name,
                    collection_name=collection_name,
                    update_instructions=UpdateInstructions(
                        overwrites=documents_updated
                    ),
                )

            if not many:
                break

    def replace(
        self,
        database_name: str,
        collection_name: str,
        filter_: dict,
        replacement: dict,
        many: bool = True,
    ):
        for documents in grouper(
            self._chunk_size,
            self._iter_documents_filtered(database_name, collection_name, filter_),
        ):
            documents_updated = {}

            for document in documents:
                updated_document = replacement

                # Document was updated
                if updated_document != document.data:
                    documents_updated[document.index] = updated_document

                    if not many:
                        break

            if documents_updated:
                self._storage_engine.update_documents(
                    database_name=database_name,
                    collection_name=collection_name,
                    update_instructions=UpdateInstructions(
                        overwrites=documents_updated
                    ),
                )

            if not many:
                break

    def delete(
        self, database_name: str, collection_name: str, filter_: dict, many: bool = True
    ):

        for documents in grouper(
            self._chunk_size,
            self._iter_documents_filtered(database_name, collection_name, filter_),
        ):
            if not many:
                documents = documents[:1]

            documents_indexes = {document.index for document in documents}

            self._storage_engine.delete_documents(
                database_name=database_name,
                collection_name=collection_name,
                delete_instructions=ReadInstructions(indexes=documents_indexes),
            )

            if not many:
                break

    def insert(self, database_name: str, collection_name: str, documents: List[dict]):
        return self._storage_engine.insert_documents(
            database_name=database_name,
            collection_name=collection_name,
            insert_instructions=InsertInstructions(documents=documents),
        )

    def close(self):
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    @staticmethod
    def _raise_on_none_database(database: Union[str, None]):
        if database is None:
            raise DatabaseIsRequired()

    @staticmethod
    def _raise_on_none_collection(collection: Union[str, None]):
        if collection is None:
            raise CollectionIsRequired()

    def _iter_read_documents(self, database: str, collection: str):
        read_instructions = ReadInstructions(offset=0, chunk_size=self._chunk_size)

        while not read_instructions.ended:
            documents = self._storage_engine.get_documents(
                database_name=database,
                collection_name=collection,
                read_instructions=read_instructions,
            )

            if not documents:
                break

            yield from documents

    def _iter_documents_filtered(self, database: str, collection: str, filter_: dict):
        for document in self._iter_read_documents(database, collection):
            if document_filter_match(document.data, filter_):
                yield document
