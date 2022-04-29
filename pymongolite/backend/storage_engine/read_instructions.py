from typing import Set
from itertools import count

DocumentIndex = int


class ReadInstructions:
    def __init__(
        self,
        offset: DocumentIndex = None,
        length: int = None,
        indexes: Set[DocumentIndex] = None,
        chunk_size: int = None,
    ):
        if offset is None and indexes is None:
            raise ValueError("You must pass offset or indexes")

        self.indexes = indexes
        self.offset: DocumentIndex = offset
        self.length = length
        self.chunk_size = chunk_size

        self._ended = False

    @classmethod
    def from_set_of_indexes(cls, indexes: Set[DocumentIndex]):
        return cls(indexes=indexes)

    @classmethod
    def from_offset_and_length(cls, offset: DocumentIndex, length: int):
        return cls(offset=offset, length=length)

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
        elif self.length:
            iterator = range(self.offset, self.offset + self.length)
        else:
            iterator = count(self.offset, 1)

        yield from iterator
        self.end()
