import shutil

import pytest

from pymongolite import MongoClient


@pytest.fixture(scope="function")
def collection():
    with MongoClient("col_test", database="db") as client:
        db = client.get_default_database()
        col = db.create_collection("col")
        yield col

    shutil.rmtree("col_test")


def test_index_creation(collection):
    collection.create_index({'age': 1})
    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    assert list(collection.find({"age": {"$gt": 20}}, {"_id": 0}))\
           == [{"name": "jon", "age": 22}, {"name": "nina", "age": 25}]
