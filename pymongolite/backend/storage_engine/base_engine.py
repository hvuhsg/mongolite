from typing import List, Any
from abc import ABC, abstractmethod

from .read_instructions import ReadInstructions
from .update_instructions import UpdateInstructions
from .insert_instruction import InsertInstructions


class BaseEngine(ABC):
    @abstractmethod
    def create_database(self, database_name: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def drop_database(self, database_name: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def create_collection(self, database_name: str, collection_name: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def drop_collection(self, database_name: str, collection_name: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_collections_list(self, database_name: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def get_documents(
        self,
        database_name: str,
        collection_name: str,
        read_instructions: ReadInstructions,
    ):
        raise NotImplementedError

    @abstractmethod
    def update_documents(
        self,
        database_name: str,
        collection_name: str,
        update_instructions: UpdateInstructions,
    ):
        raise NotImplementedError

    @abstractmethod
    def delete_documents(
        self,
        database_name: str,
        collection_name: str,
        delete_instructions: ReadInstructions,
    ):
        raise NotImplementedError

    @abstractmethod
    def insert_documents(
        self,
        database_name: str,
        collection_name: str,
        insert_instructions: InsertInstructions,
    ) -> List[Any]:
        raise NotImplementedError
