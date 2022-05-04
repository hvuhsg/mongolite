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

    results = list(collection.find({"age": {"$gt": 20}}, {"_id": 0}))

    assert {"name": "jon", "age": 22} in results
    assert {"name": "nina", "age": 25} in results


def test_index_on_existing_data(collection):
    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    collection.create_index({'age': 1})

    results = list(collection.find({"age": {"$gt": 20}}, {"_id": 0}))

    assert {"name": "jon", "age": 22} in results
    assert {"name": "nina", "age": 25} in results


def test_multiple_indexes(collection):
    collection.create_index({'age': 1})
    collection.create_index({'name': 1})

    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    results = list(collection.find({"age": {"$gt": 20}, "name": {"$gt": "mosh"}}, {"_id": 0}))

    assert {"name": "nina", "age": 25} in results


def test_empty_result_index(collection):
    collection.create_index({'age': 1})

    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    results = list(collection.find({"age": {"$lt": 0}}, {"_id": 0}))

    assert results == []
