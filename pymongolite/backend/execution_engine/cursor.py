from typing import Iterable


class Cursor:
    def __init__(self, iterator: Iterable):
        self._iterator = iterator
        self._finished = False
        self._closed = False

    def __iter__(self):
        if self._closed:
            raise StopIteration()

        for i in self._iterator:
            if self._closed:
                break

            yield i

        self.close()

    def __next__(self):
        return next(self.__iter__())

    def close(self):
        self._closed = True
        del self._iterator
