class Document:
    def __init__(
        self, data: dict, file_offset: int, length: int, database: str, collection: str
    ):
        self.data = data
        self.file_offset = file_offset
        self.length = length
        self.collection = collection
        self.database = database
