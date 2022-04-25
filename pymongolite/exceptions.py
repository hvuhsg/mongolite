class MongoliteException(Exception):
    pass


class MissingDatabaseName(MongoliteException):
    pass


class CollectionInvalid(MongoliteException):
    def __init__(self, msg: str):
        self.message = msg


class InvalidName(MongoliteException):
    def __init__(self, msg: str):
        self.message = msg


class ReadWritePermissionsAreRequired(MongoliteException):
    pass
