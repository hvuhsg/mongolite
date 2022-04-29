from typing import Dict

DocumentIndex = int


class UpdateInstructions:
    def __init__(self, overwrites: Dict[DocumentIndex, dict]):
        self.overwrites = overwrites

    def __iter__(self):
        yield from self.overwrites.items()
