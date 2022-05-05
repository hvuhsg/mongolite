from typing import List, Tuple, Union, Dict, Any
from uuid import uuid4, UUID

from pymongolite.backend.objectid import ObjectId
from pymongolite.backend.read_instructions import ReadInstructions
from pymongolite.backend.indexing_engine.base_engine import BaseEngine
from pymongolite.backend.indexing_engine.index_metadata import IndexMetadata
from pymongolite.backend.indexing_engine.base_index import BaseIndex
from pymongolite.backend.indexing_engine.index_types.sorted_list_basic_index import SortedListBasicIndex


class V1Engine(BaseEngine):
    def __init__(self):
        self._root_index: Dict[ObjectId, Any] = {}  # {ObjectID: file_index}
        self._indexes: Dict[str, Dict[str, Dict[str, BaseIndex]]] = {}  # {db_name: {collection_name: {field: index}}
        self._indexes_meta: Dict[str, IndexMetadata] = {}  # {index_id: index_metadata}

    def create_index(
        self, database_name: str, collection_name: str, index: dict
    ) -> Union[UUID, None]:
        if len(index) > 1:
            raise ValueError("Index must be with one pair of key and value")

        if database_name not in self._indexes:
            self._indexes[database_name] = {}

        if collection_name not in self._indexes[database_name]:
            self._indexes[database_name][collection_name] = {}

        field, index_type = next(iter(index.items()))
        index_uuid = None

        if field not in self._indexes[database_name][collection_name]:
            index_uuid = uuid4()
            if index_type == 1:
                self._indexes[database_name][collection_name][field] = SortedListBasicIndex()
            else:
                raise TypeError(f"Index of type '{index_type}' not implemented")
            self._indexes_meta[str(index_uuid)] = IndexMetadata(
                field=field, type_=index_type
            )

        return index_uuid

    def delete_index(
            self,
            database_name: str,
            collection_name: str,
            index_uuid: str,
    ) -> bool:
        if (
            database_name not in self._indexes
            or collection_name not in self._indexes[database_name]
        ):
            return False

        index_metadata: IndexMetadata = self._indexes_meta.pop(index_uuid, None)

        if index_metadata is None:
            return False

        self._indexes[database_name][collection_name].pop(index_metadata.field)

        return True

    def get_indexes_list(self, database_name: str, collection_name: str) -> list:
        if (
            database_name not in self._indexes
            or collection_name not in self._indexes[database_name]
        ):
            return []

        return [
            {
                "id": index_uuid,
                "field": index_metadata.field,
                "type": index_metadata.type_,
                "size": len(
                    self._indexes[database_name][collection_name][index_metadata.field]
                ),
            }
            for index_uuid, index_metadata in self._indexes_meta.items()
        ]

    def _insert_to_root_index(self, document_id: ObjectId, lookup_key: int):
        self._root_index[document_id] = lookup_key

    def _remove_from_root_index(self, document_id: ObjectId) -> bool:
        return self._root_index.pop(document_id, None) is not None

    def insert_documents(
        self,
        database_name: str,
        collection_name: str,
        documents: List[Tuple[dict, int]],
    ):
        for document, lookup_key in documents:
            document_id = document["_id"]
            self._insert_to_root_index(document_id, lookup_key)

        if (
            database_name not in self._indexes
            or collection_name not in self._indexes[database_name]
        ):
            return

        for document, lookup_key in documents:
            document_id = document["_id"]

            for field in document.keys():
                if (
                    index := self._indexes[database_name][collection_name].get(
                        field, None
                    )
                ) is not None:
                    index.add(document[field], document_id)

    def delete_documents(
        self, database_name: str, collection_name: str, documents: List[dict]
    ):
        for document in documents:
            document_id = document["_id"]
            self._remove_from_root_index(document_id)

        if (
            database_name not in self._indexes
            or collection_name not in self._indexes[database_name]
        ):
            return

        fields_with_indexes = set(self._indexes[database_name][collection_name].keys())

        for document in documents:
            document_id = document["_id"]

            for field in fields_with_indexes.intersection(set(document.keys())):
                index = self._indexes[database_name][collection_name][field]
                index.remove(document[field], document_id)

    def _query(
            self,
            database_name: str,
            collection_name: str,
            filter_: dict,
    ) -> ReadInstructions:
        if len(filter_) > 1:
            raise ValueError("Can't handle filter with multiple expressions use query instead")

        field, expression = list(filter_.items())[0]

        # {"name": "mosh"} -> {"name": {"$eq": "mosh"}}
        if not isinstance(expression, dict):
            expression = {field: {"$eq": expression}}

        have_collection_indexes = (
                database_name in self._indexes
                and collection_name in self._indexes[database_name]
        )

        if (
            not have_collection_indexes
            or field not in self._indexes[database_name][collection_name]
        ):
            if field == "_id" and isinstance(expression, ObjectId):
                return ReadInstructions(indexes={self._root_index[expression],})
            return ReadInstructions(offset=0)

        index = self._indexes[database_name][collection_name][field]
        operation, value = list(expression.items())[0]
        ids = index.query(operation, value)

        if ids is None:
            return ReadInstructions(offset=0)

        if not ids:
            read_instructions = ReadInstructions(offset=0)
            read_instructions.end()
            return read_instructions

        return ReadInstructions(indexes={self._root_index[id_] for id_ in ids})
