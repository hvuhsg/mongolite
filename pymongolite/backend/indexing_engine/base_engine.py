from typing import List
from abc import ABC, abstractmethod

from ..storage_engine.read_instructions import ReadInstructions


class BaseEngine(ABC):

    @abstractmethod
    def create_index(self, database_name: str, collection_name: str, index: dict):
        raise NotImplementedError

    @abstractmethod
    def delete_index(self, database_name: str, collection_name: str, index_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def insert_to_root_index(self, document_id: str, file_index: int):
        raise NotImplementedError

    @abstractmethod
    def insert_documents(self, database_name: str, collection_name: str, documents: List[dict]):
        raise NotImplementedError

    @abstractmethod
    def delete_documents(self, database_name: str, collection_name: str, documents: List[dict]):
        raise NotImplementedError

    @abstractmethod
    def get_documents(self, database_name: str, collection_name: str, filter_: dict) -> ReadInstructions:
        raise NotImplementedError
