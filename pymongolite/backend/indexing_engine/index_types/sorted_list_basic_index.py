from typing import Union
from bisect import bisect_left, bisect_right
from itertools import chain

from sortedcontainers import SortedKeyList as sortedlist

from pymongolite.backend.indexing_engine.base_index import BaseIndex


class SortedListBasicIndex(BaseIndex):
    def __init__(self):
        self.__sortedlist = sortedlist(key=lambda t: t[0])
        self.__index_values = sortedlist(self._calculate_index_values())

    def _calculate_index_values(self) -> list:
        return [value_id[0] for value_id in self.__sortedlist]

    def add(self, value, id_):
        self.__sortedlist.add((value, id_))
        self.__index_values.add(value)

    def remove(self, value, id_):
        self.__sortedlist.remove((value, id_))
        self.__index_values.remove(value)

    def query(self, operation: str, value) -> Union[set, None]:
        if operation == "$gt":
            i = bisect_right(self.__index_values, value)
            return {value_id[1] for value_id in self.__sortedlist[i:]}

        if operation == "$gte":
            i = bisect_left(self.__index_values, value)
            return {value_id[1] for value_id in self.__sortedlist[i:]}

        if operation == "$eq":
            s = bisect_left(self.__index_values, value)
            e = bisect_right(self.__index_values, value)
            return {value_id[1] for value_id in self.__sortedlist[s:e]}

        if operation == "$ne":
            return None  # TODO: implement exclude
            # s = bisect_left(self.__index_values, value)
            # e = bisect_right(self.__index_values, value)
            # si = self.__sortedlist[:s]
            # ei = self.__sortedlist[e:]
            # return {value_id[1] for value_id in chain(si, ei)}

        if operation == "$lt":
            i = bisect_left(self.__index_values, value)
            return {value_id[1] for value_id in self.__sortedlist[:i]}

        if operation == "$lte":
            i = bisect_right(self.__index_values, value)
            return {value_id[1] for value_id in self.__sortedlist[:i]}

        if operation == "$exists":
            if not value:
                return None  # TODO: implement exclude
            return {value_id[1] for value_id in self.__sortedlist}

        if operation == "$in":
            ids = set()
            for item in value:
                s = bisect_left(self.__index_values, item)
                e = bisect_right(self.__index_values, item)
                ids.update({value_id[1] for value_id in self.__sortedlist[s:e]})
            return ids

        if operation == "$nin":
            return None  # TODO: implement exclude
            # ids = set()
            # for item in value:
            #     s = bisect_left(self.__index_values, item)
            #     e = bisect_right(self.__index_values, item)
            #     si = self.__sortedlist[:s]
            #     ei = self.__sortedlist[e:]
            #     ids.update({value_id[1] for value_id in chain(si, ei)})
            # return ids

        return None

    def __len__(self):
        return len(self.__sortedlist)
