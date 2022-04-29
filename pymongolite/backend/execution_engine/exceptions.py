from ..exceptions import MongoliteBackendException

__all__ = ["CollectionIsRequired", "DatabaseIsRequired"]


class CollectionIsRequired(MongoliteBackendException):
    pass


class DatabaseIsRequired(MongoliteBackendException):
    pass
