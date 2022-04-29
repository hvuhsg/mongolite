from typing import List


class InsertInstructions:
    def __init__(self, documents: List[dict]):
        self.documents = documents

    def __iter__(self):
        yield from self.documents
