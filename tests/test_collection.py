import os
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


def test_collection_drop(collection):
    collection.drop()

    assert not os.path.exists("col_test/db/col")


def test_insert(collection):
    collection.insert_one({"a": True})

    with open("col_test/db/col", "r") as file:
        data = file.read()

    assert data.startswith('{"a": true')


def test_insert_many(collection):
    collection.insert_many([{"a": True}, {"b": False}])

    with open("col_test/db/col", "r") as file:
        data = file.read()

    assert '"a": true' in data
    assert '"b": false' in data


def test_find_one(collection):
    collection.insert_one({"a": 1, "b": 2})

    doc = collection.find_one({"a": 1}, {'_id': 0})

    assert doc == {"a": 1, "b": 2}

    doc = collection.find_one({"a": 4})

    assert doc is None


def test_find_many(collection):
    collection.insert_one({"a": 1, "b": 2})
    collection.insert_one({"a": 1, "b": 3})
    collection.insert_one({"a": 1, "b": 4})
    collection.insert_one({"a": 5, "b": 2})

    docs = collection.find({"a": 1}, {"_id": 0})

    assert list(docs) == [{"a": 1, "b": 2}, {"a": 1, "b": 3}, {"a": 1, "b": 4}]


def test_find_with_fields(collection):
    collection.insert_one({"a": 10, "b": 2})

    doc = collection.find_one({}, {"a": 1})
    assert doc == {"a": 10}

    doc = collection.find_one({}, {"a": 0, '_id': 0})
    assert doc == {"b": 2}


def test_update_one(collection):
    collection.insert_one({"a": 1})
    collection.insert_one({"b": 5})

    collection.update_one({}, {"$set": {"b": 2}})

    doc = collection.find_one({"b": 2}, {'_id': 0})
    assert doc == {"a": 1, "b": 2}

    collection.update_one({"a": 1}, {"$inc": {"a": 9}})

    doc = collection.find_one({"a": 10}, {'_id': 0})
    assert doc == {"a": 10, "b": 2}


def test_update_many(collection):
    collection.insert_many([{"a": 100}, {"a": 100}])

    collection.update_many({}, {"$set": {"b": 2}})

    docs = collection.find({}, {"_id": 0})
    assert list(docs) == [{"a": 100, "b": 2}, {"a": 100, "b": 2}]

    collection.update_many({}, {"$inc": {"a": -99}})

    docs = collection.find({}, {"_id": 0})
    assert list(docs) == [{"a": 1, "b": 2}, {"a": 1, "b": 2}]


def test_delete_one(collection):
    collection.insert_many([{"b": 1}, {"a": 1}, {"c": 1}, {"a": 1}])
    collection.delete_one({"a": 1})

    assert list(collection.find({"a": 1}, {"_id": 0})) == [{"a": 1}]


def test_delete_many(collection):
    collection.insert_many([{"a": 1}, {"a": 1}])
    collection.delete_many({})

    assert list(collection.find({})) == []


def test_replace_one(collection):
    collection.insert_one({"a": 1})
    collection.replace_one({}, {"b": 1})

    assert collection.find_one({}, {"_id": 0}) == {"b": 1}


def test_replace_many(collection):
    collection.insert_many([{"a": 1}, {"a": 1}])
    collection.replace_many({}, {"b": 1})

    assert list(collection.find({}, {"_id": 0})) == [{"b": 1}, {"b": 1}]
