from typing import Set
from itertools import count

DocumentIndex = int


class ReadInstructions:
    def __init__(
        self,
        offset: DocumentIndex = None,
        indexes: Set[DocumentIndex] = None,
        exclude_indexes: Set[DocumentIndex] = None,
        chunk_size: int = None,
    ):
        if offset is None and indexes is None:
            raise ValueError("You must pass offset or indexes")

        if exclude_indexes is None:
            exclude_indexes = set()

        self.indexes = indexes
        self.exclude_indexes = exclude_indexes
        self.offset: DocumentIndex = offset
        self.chunk_size = chunk_size

        self._ended = False

    @classmethod
    def from_set_of_indexes(cls, indexes: Set[DocumentIndex]):
        return cls(indexes=indexes)

    @property
    def ended(self) -> bool:
        return self._ended

    @property
    def is_index_list(self):
        return bool(self.indexes)

    def end(self):
        self._ended = True

    def __iter__(self):
        if self.indexes:
            iterator = self.indexes
        else:
            iterator = count(self.offset, 1)

        yield from iterator
        self.end()

    def __and__(self, other):
        if not isinstance(other, ReadInstructions):
            print(type(self) == type(other))
            return NotImplemented

        new_instruction = self.__class__(self.offset, self.indexes, self.exclude_indexes, self.chunk_size)

        if new_instruction is not None and other.offset is not None:
            if new_instruction.offset < other.offset:
                new_instruction.offset = other.offset

        if new_instruction.indexes is not None and other.indexes is not None:
            new_instruction.indexes.intersection_update(other.indexes)

        if new_instruction.indexes is None and other.indexes is not None:
            new_instruction.offset = None
            new_instruction.indexes = other.indexes

        new_instruction.exclude_indexes.update(other.exclude_indexes)

        return new_instruction

    def __or__(self, other):
        if not isinstance(other, ReadInstructions):
            return NotImplemented

        new_instruction = self.__class__(self.offset, self.indexes, self.exclude_indexes, self.chunk_size)

        if new_instruction is not None and other.offset is not None:
            if new_instruction.offset > other.offset:
                new_instruction.offset = other.offset

        if new_instruction.indexes is not None and other.indexes is not None:
            new_instruction.indexes.update(other.indexes)

        if new_instruction.offset is None and other.offset is not None:
            new_instruction.offset = other.offset
            new_instruction.indexes = None

        new_instruction.exclude_indexes.intersection_update(other.exclude_indexes)

        return new_instruction

    def __invert__(self):
        if self.indexes:
            self.indexes, self.exclude_indexes = self.exclude_indexes, self.indexes

        if self.indexes is not None and len(self.indexes) == 0:
            self.indexes = None
            self.offset = 0

        return self
