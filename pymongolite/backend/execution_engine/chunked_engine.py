from typing import List, Union, Tuple
from threading import RLock
from collections import defaultdict

from pymongolite.backend.command import Command, COMMANDS
from pymongolite.backend.utils import (
    document_filter_match,
    update_document_with_override,
    update_with_fields,
    grouper,
)
from pymongolite.backend.objectid import ObjectId
from pymongolite.backend.storage_engine.base_engine import BaseEngine as BaseStorageEngine
from pymongolite.backend.read_instructions import ReadInstructions
from pymongolite.backend.storage_engine.insert_instruction import InsertInstructions
from pymongolite.backend.storage_engine.update_instructions import UpdateInstructions
from pymongolite.backend.indexing_engine.base_engine import BaseEngine as BaseIndexingEngine
from pymongolite.backend.execution_engine.exceptions import DatabaseIsRequired, CollectionIsRequired
from pymongolite.backend.execution_engine.cursor import Cursor
from pymongolite.backend.execution_engine.base_engine import BaseEngine

DEFAULT_CHUNK_SIZE = 5 * 1024


class ChunkedEngine(BaseEngine):
    def __init__(
        self,
        storage_engine: BaseStorageEngine,
        indexing_engine: BaseIndexingEngine = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self.__collection_locks = defaultdict(RLock)
        self._closed = False
        self._chunk_size = chunk_size

        super().__init__(storage_engine=storage_engine, indexing_engine=indexing_engine)

    def execute_command(self, command: Command):
        if command.collection_name is None:
            return self._execute_command(command)

        with self.__collection_locks[command.collection_name]:
            return self._execute_command(command)

    def _execute_command(self, command: Command):
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
            return Cursor(self.find(
                database_name=command.database_name,
                collection_name=command.collection_name,
                filter_=command.filter,
                fields=command.fields,
                many=command.many,
            ))

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

        if command.cmd == COMMANDS.create_index:
            self._raise_on_none_collection(command.collection_name)
            return self.create_index(
                database_name=command.database_name,
                collection_name=command.collection_name,
                index=command.index,
            )

        if command.cmd == COMMANDS.delete_index:
            self._raise_on_none_collection(command.collection_name)
            return self.delete_index(
                database_name=command.database_name,
                collection_name=command.collection_name,
                index_id=command.index_id,
            )

        if command.cmd == COMMANDS.get_index_list:
            self._raise_on_none_collection(command.collection_name)
            return self.get_indexes_list(
                database_name=command.database_name,
                collection_name=command.collection_name,
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
                updated_document["_id"] = str(updated_document["_id"])

                # Document was updated
                if updated_document != document.data:
                    documents_updated[document.lookup_key] = updated_document

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
                updated_document["_id"] = str(ObjectId())

                # Document was updated
                if updated_document != document.data:
                    documents_updated[document.lookup_key] = updated_document

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

            documents_indexes = {document.lookup_key for document in documents}

            self._storage_engine.delete_documents(
                database_name=database_name,
                collection_name=collection_name,
                delete_instructions=ReadInstructions(indexes=documents_indexes),
            )

            if not self._is_indexing_engine_used:
                self._indexing_engine.delete_documents(
                    database_name, collection_name, documents
                )

            if not many:
                break

    def insert(self, database_name: str, collection_name: str, documents: List[dict]):
        inserted_object_ids = []

        for document in documents:
            oid = ObjectId()
            document["_id"] = str(oid)
            inserted_object_ids.append(oid)

        documents_lookup_keys = self._storage_engine.insert_documents(
            database_name=database_name,
            collection_name=collection_name,
            insert_instructions=InsertInstructions(documents=documents),
        )

        if self._is_indexing_engine_used:
            self._indexing_engine.insert_documents(
                database_name,
                collection_name,
                documents=[
                    (document, lookup_key)
                    for document, lookup_key in zip(documents, documents_lookup_keys)
                ],
            )

        return inserted_object_ids

    def create_index(self, database_name: str, collection_name: str, index: dict):
        if not self._is_indexing_engine_used:
            return

        index_uuid = self._indexing_engine.create_index(
            database_name, collection_name, index
        )
        if index_uuid is None:
            return False

        index_field = next(iter(index.keys()))
        filter_ = {index_field: {"$exists": True}}

        for documents in grouper(
            self._chunk_size,
            self._iter_documents_filtered(
                database_name, collection_name, filter_, use_indexes=False
            ),
        ):
            documents = [(document.data, document.lookup_key) for document in documents]
            self._indexing_engine.insert_documents(
                database_name, collection_name, documents=documents
            )

        return index_uuid

    def delete_index(
        self, database_name: str, collection_name: str, index_id: str
    ) -> bool:
        if not self._is_indexing_engine_used:
            return False

        return self._indexing_engine.delete_index(
            database_name, collection_name, index_id
        )

    def get_indexes_list(self, database_name: str, collection_name: str) -> list:
        if not self._is_indexing_engine_used:
            return []

        return self._indexing_engine.get_indexes_list(database_name, collection_name)

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

    def _iter_read_documents(
        self, database: str, collection: str, read_instructions: ReadInstructions
    ):
        while not read_instructions.ended:
            documents = self._storage_engine.get_documents(
                database_name=database,
                collection_name=collection,
                read_instructions=read_instructions,
            )

            if not documents:
                break

            for document in documents:
                document.data["_id"] = ObjectId(document.data["_id"])
                yield document

    def _pre_extraction_filtering(
            self,
            database_name: str,
            collection_name: str,
            read_instructions: ReadInstructions,
            filter_: dict,
            use_indexes: bool,
    ) -> Tuple[ReadInstructions, bool]:
        """
        Use the indexing engine to shrink the search zone
        :return: ReadInstructions for smaller or equal zone,
                 is port extraction filtering needed (maybe its bullseye zone)
        """
        if not use_indexes:
            return read_instructions, True

        return self._indexing_engine.query(
            database_name=database_name,
            collection_name=collection_name,
            read_instructions=read_instructions,
            filter_=filter_,
        ), True

    def _iter_documents_filtered(
        self, database: str, collection: str, filter_: dict, use_indexes: bool = True
    ):
        read_instructions = ReadInstructions(offset=0, chunk_size=self._chunk_size)

        read_instructions, is_post_filtering_needed = self._pre_extraction_filtering(
            database_name=database,
            collection_name=collection,
            read_instructions=read_instructions,
            filter_=filter_,
            use_indexes=use_indexes,
        )

        for document in self._iter_read_documents(
            database, collection, read_instructions
        ):
            if is_post_filtering_needed:
                if document_filter_match(document.data, filter_):
                    yield document
            else:
                yield document
