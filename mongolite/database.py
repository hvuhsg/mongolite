from typing import Any, Optional, Dict, Mapping, NoReturn, Iterable, List, Union
import contextlib

from .collection import Collection
from .command import Command, COMMANDS


class Database:
    def __init__(self, client, name: str):
        self.__client = client
        self.__name = name

        self._create()

    @property
    def client(self):
        return self.__client

    @property
    def name(self) -> str:
        return self.__name

    def __eq__(self, other):
        if isinstance(other, Database):
            return self.__client == other.client and self.__name == other.name
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash((self.__client, self.__name))

    def __repr__(self):
        return "Database(%r, %r)" % (self.__client, self.__name)

    def __getattr__(self, name: str):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith("_"):
            raise AttributeError(
                "Database has no attribute %r. To access the %s"
                " collection, use database[%r]." % (name, name, name)
            )
        return self.__getitem__(name)

    def __getitem__(self, name: str):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def _create(self):
        with self.__client._open_session() as session:
            session.exc_command(
                command=Command(cmd=COMMANDS.create_database),
                database=self.__name,
            )

    def get_collection(
        self,
        name: str,
    ) -> Collection:
        """Get a :class:`~pymongo.collection.Collection` with the given name
        and options.

        Useful for creating a :class:`~pymongo.collection.Collection` with
        different codec options, read preference, and/or write concern from
        this :class:`Database`.

        :Parameters:
          - `name`: The name of the collection - a string.
        """

        return Collection(self, name, False)

    def create_collection(
        self,
        name: str,
        **kwargs: Any,
    ) -> Collection:
        return Collection(
            self,
            name,
            create=True,
            **kwargs,
        )

    def list_collections(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> Iterable[Collection]:
        """Get a cursor over the collections of this database.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `filter` (optional):  A query document to filter the list of
            collections returned from the listCollections command.
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `**kwargs` (optional): Optional parameters of the
            `listCollections command
            <https://mongodb.com/docs/manual/reference/command/listCollections/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.


        :Returns:
          An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionadded:: 3.6
        """
        if filter is not None:
            kwargs["filter"] = filter
        if comment is not None:
            kwargs["comment"] = comment

        with self.__client._open_session() as session:
            collection_name_iter = session.exc_command(
                command=Command(cmd=COMMANDS.get_collection_list, **kwargs),
                database=self.__name
            )

            collection_iter = (
                Collection(self, collection_name, create=False)
                for collection_name in collection_name_iter
            )
            return collection_iter

    def list_collection_names(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Get a list of all the collection names in this database.

        For example, to list all non-system collections::

            filter = {"name": {"$regex": r"^(?!system\\.)"}}
            db.list_collection_names(filter=filter)

        :Parameters:
          - `filter` (optional):  A query document to filter the list of
            collections returned from the listCollections command.
          - `comment` (optional): A user-provided comment to attach to this
            command.
          - `**kwargs` (optional): Optional parameters of the
            `listCollections command
            <https://mongodb.com/docs/manual/reference/command/listCollections/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.

        .. versionchanged:: 3.8
           Added the ``filter`` and ``**kwargs`` parameters.

        .. versionadded:: 3.6
        """
        if comment is not None:
            kwargs["comment"] = comment

        return [collection.name for collection in self.list_collections(**kwargs)]

    def drop_collection(
        self,
        name_or_collection: Union[str, Collection],
        comment: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Drop a collection.

        :Parameters:
          - `name_or_collection`: the name of a collection to drop or the
            collection object itself
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `comment` (optional): A user-provided comment to attach to this
            command.


        .. note:: The :attr:`~pymongo.database.Database.write_concern` of
           this database is automatically applied to this operation.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this database's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, str):
            raise TypeError("name_or_collection must be an instance of str")

        with self.__client._open_session() as session:
            return session.exc_command(
                command=Command(cmd=COMMANDS.drop_collection, comment=comment),
                collection=name,
                database=self.__name,
            )

    # See PYTHON-3084.
    __iter__ = None

    def __next__(self) -> NoReturn:
        raise TypeError("'Database' object is not iterable")

    next = __next__

    def __bool__(self) -> NoReturn:
        raise NotImplementedError(
            "Database objects do not implement truth "
            "value testing or bool(). Please compare "
            "with None instead: database is not None"
        )

    @contextlib.contextmanager
    def _open_session(self):
        with self.__client._open_session() as session:
            yield session
