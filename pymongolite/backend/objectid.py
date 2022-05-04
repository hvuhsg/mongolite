from uuid import uuid4


class ObjectId:
    def __init__(self, oid: str = None):
        if oid is None:
            oid = str(uuid4())

        self._oid = oid

    def __gt__(self, other):
        if isinstance(other, str):
            return self._oid > other
        elif isinstance(other, ObjectId):
            return self._oid > other._oid
        raise NotImplemented

    def __eq__(self, other):
        if isinstance(other, str):
            return self._oid == other
        elif isinstance(other, ObjectId):
            return self._oid == other._oid
        raise NotImplemented

    def __lt__(self, other):
        if isinstance(other, str):
            return self._oid < other
        elif isinstance(other, ObjectId):
            return self._oid < other._oid
        raise NotImplemented

    def __str__(self):
        return self._oid

    def __repr__(self):
        return f"ObjectId({self._oid})"

    def __hash__(self):
        return hash(self._oid)
