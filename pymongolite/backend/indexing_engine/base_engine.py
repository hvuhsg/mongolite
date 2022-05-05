from typing import List, Tuple, Any
from abc import ABC, abstractmethod
from functools import reduce

from pymongolite.backend.read_instructions import ReadInstructions
from pymongolite.backend.utils import is_condition


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
    def _query(
        self, database_name: str, collection_name: str, filter_: dict
    ) -> ReadInstructions:
        raise NotImplementedError

    def query(
            self,
            database_name: str,
            collection_name: str,
            read_instructions: ReadInstructions,
            filter_: dict
    ) -> ReadInstructions:
        if not filter_:
            return read_instructions

        for field, pattern in filter_.items():
            pattern_is_condition = is_condition(pattern)
            field_is_gate_condition = field.startswith("$")

            if not pattern_is_condition and not field_is_gate_condition:
                result = self._query(database_name, collection_name, {field: {"$eq": pattern}})
                return read_instructions & result

            if field_is_gate_condition:
                if field == "$and":
                    read_instructions &= reduce(
                        lambda a, b: a & b,
                        map(
                            lambda subfilter: self.query(
                                database_name,
                                collection_name,
                                read_instructions,
                                subfilter
                            ),
                            pattern,
                        )
                    )
                if field == "$or":
                    read_instructions &= reduce(
                        lambda a, b: a | b,
                        map(
                            lambda subfilter: self.query(
                                database_name,
                                collection_name,
                                read_instructions,
                                subfilter
                            ),
                            pattern,
                        )
                    )
                if field == "$nor":
                    read_instructions &= ~reduce(
                        lambda a, b: a | b,
                        map(
                            lambda subfilter: self.query(
                                database_name,
                                collection_name,
                                read_instructions,
                                subfilter
                            ),
                            pattern,
                        )
                    )
                return read_instructions

            if "$not" in pattern:
                res = self.query(
                    database_name,
                    collection_name,
                    read_instructions,
                    {field: pattern["$not"]},
                )
                read_instructions &= ~res

            if subpattern := pattern.get("$eq"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$eq": subpattern}})

            if subpattern := pattern.get("$ne"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$ne": subpattern}})

            if subpattern := pattern.get("$gt"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$gt": subpattern}})

            if subpattern := pattern.get("$gte"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$gte": subpattern}})

            if subpattern := pattern.get("$lt"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$lt": subpattern}})

            if subpattern := pattern.get("$lte"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$lte": subpattern}})

            if subpattern := pattern.get("$exists"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$exists": subpattern}})

            if subpattern := pattern.get("$in"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$in": subpattern}})

            if subpattern := pattern.get("$nin"):
                read_instructions &= self._query(database_name, collection_name, {field: {"$nin": subpattern}})

        return read_instructions
