from typing import Union
from abc import ABC, abstractmethod


class BaseIndex(ABC):
    @abstractmethod
    def add(self, value, id):
        raise NotImplementedError

    @abstractmethod
    def remove(self, value, id):
        raise NotImplementedError

    @abstractmethod
    def query(self, operation: str, value) -> Union[set, None]:
        raise NotImplementedError

    @abstractmethod
    def __len__(self):
        raise NotImplementedError
