from typing import List, Tuple
from bisect import bisect_right, bisect_left

from blist import sortedlist

from .base_engine import BaseEngine
from ..storage_engine.read_instructions import ReadInstructions


class V1Engine(BaseEngine):
    def __init__(self):
        self.root_index = {}  # {ObjectID: file_index}
        self.indexes = {}  # {db_name: {collection_name: {field_name: index}}

    def create_index(self, database_name: str, collection_name: str, index: dict):
        if len(index) > 1:
            raise ValueError("Index must be with one pair of key and value")

        if database_name not in self.indexes:
            self.indexes[database_name] = {}

        if collection_name not in self.indexes[database_name]:
            self.indexes[database_name][collection_name] = {}

        field, index_type = next(iter(index.items()))
        if field not in self.indexes[database_name][collection_name]:
            self.indexes[database_name][collection_name][field] = sortedlist(key=lambda t: t[0])

    def delete_index(self, database_name: str, collection_name: str, field: str) -> bool:
        if database_name in self.indexes and collection_name in self.indexes[database_name]:
            index = self.indexes[database_name][collection_name].pop(field, None)
            return index is not None
        return False

    def _insert_to_root_index(self, document_id: str, lookup_key: int):
        self.root_index[document_id] = lookup_key

    def _remove_from_root_index(self, document_id: str) -> bool:
        return self.root_index.pop(document_id, None) is not None

    def insert_documents(self, database_name: str, collection_name: str, documents: List[Tuple[dict, int]]):
        if database_name not in self.indexes or collection_name not in self.indexes[database_name]:
            return

        for document, lookup_key in documents:
            document_id = document['_id']

            for field in document.keys():
                if (index := self.indexes[database_name][collection_name].get(field, None)) is not None:
                    index.add((document[field], document_id))
                    self._insert_to_root_index(document_id, lookup_key)

    def delete_documents(self, database_name: str, collection_name: str, documents: List[dict]):
        if database_name not in self.indexes or collection_name not in self.indexes[database_name]:
            return

        fields_with_indexes = set(self.indexes[database_name][collection_name].keys())

        for document in documents:
            document_id = document['_id']

            for field in fields_with_indexes.intersection(set(document.keys())):
                index = self.indexes[database_name][collection_name][field]
                index.remove((document[field], document_id))

            self._remove_from_root_index(document_id)

    def get_documents(self, database_name: str, collection_name: str, filter_: dict) -> ReadInstructions:
        if database_name not in self.indexes or collection_name not in self.indexes[database_name]:
            return ReadInstructions(offset=0)

        ids_set = None

        for field, expression in filter_.items():
            if field not in self.indexes[database_name][collection_name]:
                continue

            index = self.indexes[database_name][collection_name][field]
            index_values = [item[0] for item in index]

            for operator, value in expression.items():
                if operator not in ['$gt', '$gte', '$eq', '$ne', '$lt', '$lte']:
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

                if ids is not None and ids_set is None:
                    ids_set = ids
                elif ids is not None:
                    ids_set.intersection_update(ids)

        if ids_set is None:
            return ReadInstructions(offset=0)

        return ReadInstructions(indexes={self.root_index[id_] for id_ in ids_set})

    def print(self):
        for db, collections in self.indexes.items():
            print('DB:', db)
            for collection, fields in collections.items():
                print('\tCollection:', collection)
                for field, index in fields.items():
                    print('\t\tField:', field)
                    print('\t\tIndex:', index)
