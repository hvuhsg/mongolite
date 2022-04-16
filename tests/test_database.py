import os
import shutil

import pytest

from mongolite import MongoClient


@pytest.fixture(scope="function")
def database():
    with MongoClient("db_test", database="db") as client:
        yield client.get_default_database()

    shutil.rmtree("db_test")


def test_create_and_drop_collection(database):
    col = database.create_collection("new_col")

    assert os.path.exists("db_test/db/new_col")

    database.drop_collection(col)

    assert not os.path.exists("db_test/db/new_col")


def test_get_collection_list(database):
    database.create_collection("a")
    database.create_collection("b")

    assert database.list_collection_names() == ["b", "a"]
