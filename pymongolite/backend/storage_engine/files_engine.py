from typing import Union, List
from pathlib import Path
from threading import Lock
from collections import defaultdict
from contextlib import contextmanager
from itertools import count
from uuid import uuid4
import json
import os
import io
import shutil

from pymongolite.backend.exceptions import (
    DatabaseNotFound,
    CollectionNotFound,
)
from pymongolite.backend.document import Document
from pymongolite.backend.storage_engine.base_engine import BaseEngine
from pymongolite.backend.storage_engine.insert_instruction import InsertInstructions
from pymongolite.backend.storage_engine.read_instructions import ReadInstructions
from pymongolite.backend.storage_engine.update_instructions import UpdateInstructions


class FilesEngine(BaseEngine):
    def __init__(self, dirpath: Union[str, Path], **kwargs):
        self._dirpath = str(dirpath)
        self.options = kwargs
        self._collection_locks = defaultdict(Lock)
        self._offsets = {}

        self._ensure_root_dir()

    @property
    def _root_path(self):
        return Path(self._dirpath).absolute()

    def _ensure_root_dir(self):
        if not os.path.exists(self._root_path):
            os.mkdir(self._root_path)

    def create_read_offset(self) -> str:
        offset = str(uuid4())
        self._offsets[offset] = 0
        return offset

    def delete_read_offset(self, offset_id: str) -> bool:
        return self._offsets.pop(offset_id, None) is not None

    def _get_database_path(
        self, database_name: str, error_not_found: bool = False
    ) -> Path:
        if error_not_found and not self.is_database_exists(database_name):
            raise DatabaseNotFound(database_name)

        return self._root_path / database_name

    def _get_collection_path(
        self, database_name: str, collection_name: str, error_not_found: bool = False
    ) -> Path:
        if error_not_found and not self.is_collection_exists(
            database_name, collection_name
        ):
            raise CollectionNotFound(database_name, collection_name)

        return self._get_database_path(database_name) / collection_name

    def _serialize_document(self, document: dict) -> str:
        return json.dumps(document)

    def _deserialize_document(self, serialized_document: str) -> dict:
        return json.loads(serialized_document)

    def _mark_document_as_deleted(self, file, index: int):
        file.seek(index)
        file.write("0")

    def _is_line_marked_as_deleted(self, line: str) -> bool:
        return line.startswith("0")

    def _insert_document(self, file, document: dict) -> int:
        file.seek(0, io.SEEK_END)
        seek_value = file.tell()
        file.write(self._serialize_document(document) + "\n")
        return seek_value

    @contextmanager
    def _collection_lock(self, database_name: str, collection_name: str):
        collection_full_name = f"{database_name}.{collection_name}"
        self._collection_locks[collection_full_name].acquire(blocking=True)
        yield
        self._collection_locks[collection_full_name].release()

    def is_database_exists(self, database_name: str) -> bool:
        database_dir_path = self._get_database_path(database_name)
        return os.path.exists(database_dir_path)

    def is_collection_exists(self, database_name: str, collection_name: str) -> bool:
        collection_file_path = self._get_collection_path(database_name, collection_name)
        return os.path.exists(collection_file_path)

    def create_database(self, database_name: str, **options) -> bool:
        if self.is_database_exists(database_name):
            return False

        database_dir_path = self._get_database_path(database_name)
        os.mkdir(database_dir_path)

        return True

    def drop_database(self, database_name: str) -> bool:
        if not self.is_database_exists(database_name):
            return False

        database_dir_path = self._get_database_path(database_name, error_not_found=True)
        shutil.rmtree(database_dir_path)

        return True

    def create_collection(self, database_name: str, collection_name: str) -> bool:
        if self.is_collection_exists(database_name, collection_name):
            return False

        collection_path = self._get_collection_path(database_name, collection_name)
        collection_path.touch()

        return True

    def drop_collection(self, database_name: str, collection_name: str) -> bool:
        if not self.is_collection_exists(database_name, collection_name):
            return False

        collection_path = self._get_collection_path(
            database_name, collection_name, error_not_found=True
        )
        os.remove(collection_path)

        return True

    def get_collections_list(self, database_name: str) -> List[str]:
        database_path = self._get_database_path(database_name, error_not_found=True)
        database_dir = os.scandir(database_path)

        return [entry.name for entry in database_dir if entry.is_file()]

    def get_documents(
        self,
        database_name: str,
        collection_name: str,
        read_instructions: ReadInstructions,
    ) -> List[Document]:
        collection_path = self._get_collection_path(database_name, collection_name)
        documents = []

        with self._collection_lock(database_name, collection_name):
            with open(collection_path, "r") as collection_file:
                if read_instructions.chunk_size is None:
                    restrict_loop = count(0, 1)
                else:
                    restrict_loop = range(read_instructions.chunk_size)

                for document_index, _ in zip(read_instructions, restrict_loop):
                    if read_instructions.is_index_list:
                        collection_file.seek(document_index)
                    else:
                        document_index = collection_file.tell()

                    # Every line is a serialized document
                    line = collection_file.readline()

                    # End of file
                    if line == "":
                        read_instructions.end()
                        break

                    # Deleted document
                    if self._is_line_marked_as_deleted(line):
                        continue

                    document = Document(
                        data=self._deserialize_document(line),
                        lookup_key=document_index,
                    )
                    documents.append(document)

        return documents

    def update_documents(
        self,
        database_name: str,
        collection_name: str,
        update_instructions: UpdateInstructions,
    ):
        collection_path = self._get_collection_path(
            database_name=database_name, collection_name=collection_name
        )
        with open(collection_path, "r+") as file:
            for index, updated_document in update_instructions:
                self._mark_document_as_deleted(file, index)
                self._insert_document(file, updated_document)

    def delete_documents(
        self,
        database_name: str,
        collection_name: str,
        delete_instructions: ReadInstructions,
    ):
        collection_path = self._get_collection_path(
            database_name=database_name, collection_name=collection_name
        )
        with open(collection_path, "r+") as file:
            for index in delete_instructions:
                self._mark_document_as_deleted(file, index)

    def insert_documents(
        self,
        database_name: str,
        collection_name: str,
        insert_instructions: InsertInstructions,
    ) -> List[int]:
        collection_path = self._get_collection_path(
            database_name=database_name, collection_name=collection_name
        )
        lookup_keys = []

        with open(collection_path, "r+") as file:
            for document in insert_instructions:
                document_lookup_key = self._insert_document(file, document)
                lookup_keys.append(document_lookup_key)

        return lookup_keys
