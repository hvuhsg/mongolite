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


def test_insert_one(benchmark, collection):
    def bench():
        collection.insert_one({'a': 1})

    benchmark(bench)


def test_insert_many(benchmark, collection):
    def bench():
        collection.insert_many([{'a': 1}]*1000)

    benchmark(bench)


def test_delete_one(benchmark, collection):
    collection.insert_one({'a': 1})

    def bench():
        collection.delete_one({'a': 1})

    benchmark(bench)


def test_delete_many(benchmark, collection):
    collection.insert_many([{'a': 1}]*1000)

    def bench():
        collection.delete_many({})

    benchmark(bench)


def test_update_one(benchmark, collection):
    collection.insert_many([{'a': 1}] * 1000)

    def bench():
        collection.update_one({}, {"$set": {'b': 2}})

    benchmark(bench)


def test_update_many(benchmark, collection):
    collection.insert_many([{'a': 1}] * 1000)

    def bench():
        collection.update_many({}, {"$set": {'b': 2}})

    benchmark(bench)


def test_find_one(benchmark, collection):
    collection.insert_many([{'a': 1}] * 1000)

    def bench():
        collection.update_one({}, {"$set": {'b': 2}})

    benchmark(bench)


def test_find_many(benchmark, collection):
    collection.insert_many([{'a': 1}] * 1000)

    def bench():
        collection.find({})

    benchmark(bench)
