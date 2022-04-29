from enum import IntEnum


class COMMANDS(IntEnum):
    create_database = 0
    drop_database = 1
    create_collection = 2
    drop_collection = 3
    get_collection_list = 4
    insert = 5
    update = 6
    delete = 7
    find = 8
    replace = 9


class Command:
    def __init__(
        self,
        database_name: str,
        cmd: COMMANDS,
        collection_name: str = None,
        **arguments,
    ):
        self.cmd = cmd
        self.arguments = arguments
        self.database_name = database_name
        self.collection_name = collection_name

    def __getattr__(self, item):
        try:
            return self.arguments[item]
        except KeyError:
            return getattr(self, item, None)
