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
    def __init__(self, cmd: COMMANDS, **arguments):
        self.cmd = cmd
        self.arguments = arguments

    def __getattr__(self, item):
        try:
            return self.arguments[item]
        except KeyError:
            return getattr(self, item)
