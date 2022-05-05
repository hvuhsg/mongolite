from typing import Optional, Any, NoReturn, Dict, List

from pymongolite.exceptions import InvalidName
from pymongolite.backend.command import Command, COMMANDS


class Collection:
    def __init__(
        self, database, name: str, create: Optional[bool] = False, **kwargs: Any
    ):
        if not isinstance(name, str):
            raise TypeError("name must be an instance of str")

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (
            name.startswith("oplog.$main") or name.startswith("$cmd")
        ):
            raise InvalidName("collection names must not contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName(
                "collection names must not start or end with '.': %r" % name
            )
        if "\x00" in name:
            raise InvalidName("collection names must not contain the null character")

        self.__database = database
        self.__name = name
        self.__full_name = "%s.%s" % (self.__database.name, self.__name)
        if create or kwargs:
            self.__create(kwargs)

    @property
    def database(self):
        return self.__database

    @property
    def name(self):
        return self.__name

    def __create(self, options):
        """Sends a create command with the given options."""
        with self.__database._open_session() as session:
            session.exc_command(
                command=Command(
                    cmd=COMMANDS.create_collection,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    **options,
                ),
            )

    def __getattr__(self, name: str) -> "Collection":
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith("_"):
            full_name = "%s.%s" % (self.__name, name)
            raise AttributeError(
                "Collection has no attribute %r. To access the %s"
                " collection, use database['%s']." % (name, full_name, full_name)
            )
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> "Collection":
        return Collection(
            self.__database,
            "%s.%s" % (self.__name, name),
            create=False,
        )

    def __repr__(self):
        return "Collection(%r, %r)" % (self.__database, self.__name)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return self.__database == other.database and self.__name == other.name
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash((self.__database, self.__name))

    def __bool__(self) -> NoReturn:
        raise NotImplementedError(
            "Collection objects do not implement truth "
            "value testing or bool(). Please compare "
            "with None instead: collection is not None"
        )

    def insert_one(self, doc: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.insert,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    documents=[doc],
                ),
            )[0]

    def insert_many(self, docs: List[Dict]):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.insert,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    documents=docs,
                ),
            )

    def delete_one(self, filter: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.delete,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    many=False,
                ),
            )

    def delete_many(self, filter: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.delete,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    many=True,
                ),
            )

    def update_one(self, filter: Dict, override: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.update,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    override=override,
                    many=False,
                ),
            )

    def update_many(self, filter: Dict, override: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.update,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    override=override,
                    many=True,
                ),
            )

    def replace_one(self, filter: Dict, replacement: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.replace,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    replacement=replacement,
                    many=False,
                ),
            )

    def replace_many(self, filter: Dict, replacement: Dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.replace,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    replacement=replacement,
                    many=True,
                ),
            )

    def drop(self, comment: Optional[Any] = None):
        return self.__database.drop_collection(self.__name, comment=comment)

    def find(
        self,
        filter: Dict,
        fields: Optional[Dict] = None,
        many: Optional[bool] = True,
        **kwargs
    ):
        if fields is None:
            fields = {}

        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.find,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    filter=filter,
                    fields=fields,
                    many=many,
                    **kwargs,
                ),
            )

    def find_one(self, *args, **kwargs):
        results = self.find(many=False, *args, **kwargs)

        if not results:
            return None

        try:
            return next(results)
        except StopIteration:
            return None

    def create_index(self, index: dict):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.create_index,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    index=index,
                ),
            )

    def delete_index(self, index_id: str):
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.delete_index,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                    index_id=index_id,
                ),
            )

    def get_indexes(self) -> list:
        with self.__database._open_session() as session:
            return session.exc_command(
                command=Command(
                    cmd=COMMANDS.get_index_list,
                    database_name=self.__database.name,
                    collection_name=self.__name,
                ),
            )
