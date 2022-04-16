class MongoliteBackendException(Exception):
    pass


class DatabaseNotFound(MongoliteBackendException):
    def __init__(self, database_name: str):
        self.db_name = database_name

    def __str__(self):
        return f"Database '{self.db_name}' not found"


class DatabaseAlreadyExists(MongoliteBackendException):
    def __init__(self, database_name: str):
        self.db_name = database_name

    def __str__(self):
        return f"Database '{self.db_name}' already exists"


class CollectionNotFound(MongoliteBackendException):
    def __init__(self, database_name: str, collection_name: str):
        self.col_name = collection_name
        self.db_name = database_name

    def __str__(self):
        return f"Collection '{self.col_name}' not found in database '{self.db_name}'"


class CollectionAlreadyExists(MongoliteBackendException):
    def __init__(self, database_name: str, collection_name: str):
        self.db_name = database_name
        self.col_name = collection_name

    def __str__(self):
        return f"Collection '{self.col_name}' already exists in database '{self.db_name}'"


class CollectionIsRequired(MongoliteBackendException):
    pass


class DatabaseIsRequired(MongoliteBackendException):
    pass
