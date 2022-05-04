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
    index_id = collection.create_index({'age': 1})

    assert index_id is not None

    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    results = list(collection.find({"age": {"$gt": 20}}, {"_id": 0}))

    assert len(results) == 2
    assert {"name": "jon", "age": 22} in results
    assert {"name": "nina", "age": 25} in results


def test_index_on_existing_data(collection):
    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    collection.create_index({'age': 1})

    results = list(collection.find({"age": {"$gt": 20}}, {"_id": 0}))

    assert len(results) == 2
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

    assert len(results) == 1
    assert {"name": "nina", "age": 25} in results


def test_empty_result_index(collection):
    collection.create_index({'age': 1})

    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    results = list(collection.find({"age": {"$lt": 0}}, {"_id": 0}))

    assert results == []


def test_simple_query(collection):
    collection.create_index({'age': 1})

    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    results = list(collection.find({"age": 11}, {"_id": 0}))

    assert results == [{"name": "mosh", "age": 11}]


def test_get_index_list(collection):
    assert collection.get_indexes() == []

    index_id = collection.create_index({"age": 1})

    assert len(collection.get_indexes()) == 1

    collection.delete_index(index_id)

    assert collection.get_indexes() == []


def test_delete_document_from_index(collection):
    collection.create_index({'age': 1})

    collection.insert_one({"name": "jon", "age": 22})
    collection.insert_one({"name": "dave", "age": 15})
    collection.insert_one({"name": "mosh", "age": 11})
    collection.insert_one({"name": "nina", "age": 25})

    collection.delete_many({"age": {"$gt": 15}})

    assert list(collection.find({"age": {"$eq": 15}}, {"_id": 0})) == [{'age': 15, 'name': 'dave'}]
