from typing import Union, List, Dict
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

from .exceptions import (
    DatabaseNotFound,
    CollectionNotFound,
)
from .document import Document


class StorageEngine:
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

    def _get_database_path(self, database_name: str, error_not_found: bool = False) -> Path:
        if error_not_found and not self.is_database_exists(database_name):
            raise DatabaseNotFound(database_name)

        return self._root_path / database_name

    def _get_collection_path(
            self,
            database_name: str,
            collection_name: str,
            error_not_found: bool = False
    ) -> Path:
        if error_not_found and not self.is_collection_exists(database_name, collection_name):
            raise CollectionNotFound(database_name, collection_name)

        return self._get_database_path(database_name) / collection_name

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

    def create_database(self, database_name: str, **options):
        if self.is_database_exists(database_name):
            return

        database_dir_path = self._get_database_path(database_name)
        os.mkdir(database_dir_path)

    def drop_database(self, database_name: str):
        database_dir_path = self._get_database_path(database_name, error_not_found=True)
        shutil.rmtree(database_dir_path)

    def create_collection(self, database_name: str, collection_name: str):
        if self.is_collection_exists(database_name, collection_name):
            return

        collection_path = self._get_collection_path(database_name, collection_name)
        collection_path.touch()

    def drop_collection(self, database_name: str, collection_name: str):
        collection_path = self._get_collection_path(database_name, collection_name, error_not_found=True)
        os.remove(collection_path)

    def collection_list(self, database_name: str):
        database_path = self._get_database_path(database_name, error_not_found=True)
        database_dir = os.scandir(database_path)

        return [
            entry.name
            for entry in database_dir
            if entry.is_file()
        ]

    def add_documents(self, database_name: str, collection_name: str, documents: List[dict]):
        with self._collection_lock(database_name, collection_name):
            with open(self._get_collection_path(database_name, collection_name), 'a') as file:
                documents_jsons = [json.dumps(document) for document in documents]
                file.writelines('\n'.join(documents_jsons) + '\n')

    def get_documents(
            self,
            database_name: str,
            collection_name: str,
            number_of_documents: int = None,
            offset_id: str = None,
    ) -> List[Document]:
        documents = []

        with self._collection_lock(database_name, collection_name):
            with open(self._get_collection_path(database_name, collection_name), 'r') as collection_file:
                if offset_id is not None:
                    offset = self._offsets[offset_id]
                    collection_file.seek(offset)

                if number_of_documents is None:
                    loop = count(0, 1)
                else:
                    loop = range(number_of_documents)

                for _ in loop:
                    document_offset = collection_file.tell()

                    line = collection_file.readline()
                    if line == '':
                        break

                    if line.startswith('0'):
                        continue
                    line.strip('0')

                    document = Document(
                        data=json.loads(line),
                        length=len(line),
                        file_offset=document_offset,
                        database=database_name,
                        collection=collection_name,
                    )
                    documents.append(document)

                if offset_id is not None:
                    self._offsets[offset_id] = collection_file.tell()

        return documents

    def _overwrite_documents(self, documents: Dict[Document, str]):
        document = next(iter(documents.keys()))
        collection_path = self._get_collection_path(database_name=document.database, collection_name=document.collection)

        with open(collection_path, 'r+') as file:
            for document, overwrite in documents.items():
                file.seek(document.file_offset)
                file.write(('0' * (document.length-1)) + "\n")
                file.seek(0, io.SEEK_END)
                file.write(overwrite+'\n')

    def update_documents(self, documents: Dict[Document, dict]):
        overwrite_documents = {document: json.dumps(updated) for document, updated in documents.items()}
        self._overwrite_documents(overwrite_documents)

    def delete_documents(self, documents: List[Document]):
        overwrite_documents = {document: '0'*(document.length-1) for document in documents}
        self._overwrite_documents(overwrite_documents)
