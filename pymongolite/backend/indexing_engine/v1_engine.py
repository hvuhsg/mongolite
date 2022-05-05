from typing import List, Tuple, Union
from bisect import bisect_right, bisect_left
from uuid import uuid4, UUID

from blist import sortedlist

from .base_engine import BaseEngine
from .index_metadata import IndexMetadata
from ..storage_engine.read_instructions import ReadInstructions
from ..objectid import ObjectId


class V1Engine(BaseEngine):
    def __init__(self):
        self._root_index = {}  # {ObjectID: file_index}
        self._indexes = {}  # {db_name: {collection_name: {index_id: index}}
        self._indexes_meta = {}  # {index_id: index_metadata}

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
            self._indexes[database_name][collection_name][field] = sortedlist(
                key=lambda t: t[0]
            )
            self._indexes_meta[index_uuid] = IndexMetadata(
                field=field, type_=index_type
            )

        return index_uuid

    def delete_index(
        self, database_name: str, collection_name: str, index_uuid: str
    ) -> bool:
        if (
            database_name not in self._indexes
            or collection_name not in self._indexes[database_name]
        ):
            return False

        index_metadata: IndexMetadata = self._indexes_meta.pop(index_uuid)

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

    def _insert_to_root_index(self, document_id: str, lookup_key: int):
        self._root_index[document_id] = lookup_key

    def _remove_from_root_index(self, document_id: str) -> bool:
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
                    index.add((document[field], document_id))

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
                index.remove((document[field], document_id))

    def get_documents(
        self, database_name: str, collection_name: str, filter_: dict
    ) -> ReadInstructions:
        have_collection_indexes = (
            database_name in self._indexes
            and collection_name in self._indexes[database_name]
        )
        ids_set = None

        for field, expression in filter_.items():
            if field == "_id" and isinstance(expression, ObjectId):
                ids_set = set()
                ids_set.add(expression)
                continue

            if (
                not have_collection_indexes
                or field not in self._indexes[database_name][collection_name]
            ):
                continue

            index = self._indexes[database_name][collection_name][field]
            index_values = [item[0] for item in index]

            if not isinstance(expression, dict):
                expression = {field: {"$eq": expression}}

            for operator, value in expression.items():
                if operator not in [
                    "$gt",
                    "$gte",
                    "$eq",
                    "$ne",
                    "$lt",
                    "$lte",
                    "$exists",
                ]:
                    continue

                ids = None

                if operator == "$gt":
                    i = bisect_right(index_values, value)
                    ids = {value_id[1] for value_id in index[i:]}

                if operator == "$gte":
                    i = bisect_left(index_values, value)
                    ids = {value_id[1] for value_id in index[i:]}

                if operator == "$eq":
                    s = bisect_left(index_values, value)
                    e = bisect_right(index_values, value)
                    if items := index[s:e]:
                        ids = {value_id[1] for value_id in items}
                    else:
                        ids = set()

                if operator == "$ne":
                    s = bisect_left(index_values, value)
                    e = bisect_right(index_values, value)
                    si = index[:s]
                    ei = index[e:]
                    items = si + ei
                    if items:
                        ids = {value_id[1] for value_id in items}
                    else:
                        ids = set()

                if operator == "$lt":
                    i = bisect_left(index_values, value)
                    ids = {value_id[1] for value_id in index[:i]}

                if operator == "$lte":
                    i = bisect_right(index_values, value)
                    ids = {value_id[1] for value_id in index[:i]}

                if operator == "$exists" and value:
                    ids = {value_id[1] for value_id in index}

                if ids is not None and ids_set is None:
                    ids_set = ids
                elif ids is not None:
                    ids_set.intersection_update(ids)

        if ids_set is None:
            return ReadInstructions(offset=0)

        if not ids_set:
            read_instructions = ReadInstructions(offset=0)
            read_instructions.end()
            return read_instructions

        return ReadInstructions(indexes={self._root_index[id_] for id_ in ids_set})
