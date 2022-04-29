from typing import List
from abc import ABC, abstractmethod

from ..storage_engine.base_engine import BaseEngine as BaseStorgeEngine


class BaseEngine(ABC):
    def __init__(self, storage_engine: BaseStorgeEngine):
        self._storage_engine = storage_engine

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
    def find(
        self,
        database: str,
        collection: str,
        filter_: dict,
        fields: dict = None,
        many: bool = True,
        **kwargs
    ):
        pass

    @abstractmethod
    def update(
        self,
        database_name: str,
        collection_name: str,
        filter_: dict,
        override: dict,
        many: bool = True,
    ):
        raise NotImplementedError

    @abstractmethod
    def replace(
        self,
        database_name: str,
        collection_name: str,
        filter_: dict,
        replacement: dict,
        many: bool = True,
    ):
        raise NotImplementedError

    @abstractmethod
    def delete(
        self, database_name: str, collection_name: str, filter_: dict, many: bool = True
    ):
        raise NotImplementedError

    @abstractmethod
    def insert(self, database_name: str, collection_name: str, filter_: dict):
        raise NotImplementedError
