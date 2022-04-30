from typing import List

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

    def insert_to_root_index(self, document_id: str, file_index: int):
        self.root_index[document_id] = file_index

    def insert_documents(self, database_name: str, collection_name: str, documents: List[dict]):
        if database_name not in self.indexes or collection_name not in self.indexes[database_name]:
            return

        for document in documents:
            for field in document.keys():
                if index := self.indexes[database_name][collection_name].get(field, None):
                    index.append((document[field], document['_id']))

    def delete_documents(self, database_name: str, collection_name: str, documents: List[dict]):
        if database_name not in self.indexes or collection_name not in self.indexes[database_name]:
            return

        fields_with_indexes = set(self.indexes[database_name][collection_name].keys())

        for document in documents:
            document_id = document['_id']

            for field in fields_with_indexes.intersection(set(document.keys())):
                index = self.indexes[database_name][collection_name][field]
                index.remove((document[field], document_id))

            del self.root_index[document_id]

    def get_documents(self, database_name: str, collection_name: str, filter_: dict) -> ReadInstructions:
        raise NotImplementedError

    def print(self):
        for db, collections in self.indexes.items():
            print('DB:', db)
            for collection, fields in collections.items():
                print('\tCollection:', collection)
                for field, index in fields.items():
                    print('\t\tField:', field)
                    print('\t\tIndex:', index)
