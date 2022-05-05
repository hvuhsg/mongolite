from typing import List, Tuple, Any
from abc import ABC, abstractmethod

from ..storage_engine.read_instructions import ReadInstructions


class BaseEngine(ABC):
    @abstractmethod
    def create_index(
        self, database_name: str, collection_name: str, index: dict
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def delete_index(
        self, database_name: str, collection_name: str, index_id: str
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_indexes_list(self, database_name: str, collection_name: str) -> list:
        raise NotImplementedError

    @abstractmethod
    def insert_documents(
        self,
        database_name: str,
        collection_name: str,
        documents: List[Tuple[dict, Any]],
    ):
        raise NotImplementedError

    @abstractmethod
    def delete_documents(
        self, database_name: str, collection_name: str, documents: List[dict]
    ):
        raise NotImplementedError

    @abstractmethod
    def get_documents(
        self, database_name: str, collection_name: str, filter_: dict
    ) -> ReadInstructions:
        raise NotImplementedError
