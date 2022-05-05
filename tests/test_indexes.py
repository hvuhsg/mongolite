import pytest

from pymongolite.backend.indexing_engine.v1_engine import V1Engine
from pymongolite.backend.objectid import ObjectId
from pymongolite.backend.read_instructions import ReadInstructions


@pytest.fixture(scope="function")
def indexing_v1_engine():
    yield V1Engine()


def test_index_creation(indexing_v1_engine):
    index_uuid = indexing_v1_engine.create_index("db", "col", {"age": 1})

    assert indexing_v1_engine.get_indexes_list("db", "col") == [
        {
            'field': 'age',
            'id': str(index_uuid),
            'size': 0,
            'type': 1
        }
    ]


def test_delete_index(indexing_v1_engine):
    deleted = indexing_v1_engine.delete_index("db", "col", "invalid_index_id")
    assert deleted is False

    index_uuid = indexing_v1_engine.create_index("db", "col", {"age": 1})
    assert indexing_v1_engine.delete_index("db", "col", str(index_uuid)) is True

    assert indexing_v1_engine.get_indexes_list("db", "col") == []


def test_add_to_index_and_delete_from_index(indexing_v1_engine):
    indexing_v1_engine.create_index("db", "col", {"age": 1})

    oid = ObjectId()
    indexing_v1_engine.insert_documents("db", "col", [({"age": 5, "_id": oid}, 0)])

    assert len(indexing_v1_engine._indexes["db"]["col"]["age"]) == 1
    assert len(indexing_v1_engine._root_index) == 1

    indexing_v1_engine.delete_documents("db", "col", [{"age": 5, "_id": oid}])

    assert len(indexing_v1_engine._indexes["db"]["col"]["age"]) == 0
    assert len(indexing_v1_engine._root_index) == 0


def test_simple_queries(indexing_v1_engine):
    index_uuid = indexing_v1_engine.create_index("db", "col", {"age": 1})
    indexing_v1_engine.insert_documents(
        "db",
        "col",
        [
            ({"age": 5, "_id": ObjectId()}, 0),
            ({"age": 10, "_id": ObjectId()}, 1),
            ({"age": 15, "_id": ObjectId()}, 2),
        ]
    )

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": 5}
    ).indexes == {0}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$gt": 5}}
    ).indexes == {1, 2}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$gte": 5}}
    ).indexes == {0, 1, 2}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$eq": 5}}
    ).indexes == {0}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$lt": 15}}
    ).indexes == {0, 1}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$lte": 15}}
    ).indexes == {0, 1, 2}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$exists": True}}
    ).indexes == {0, 1, 2}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$in": [5, 10]}}
    ).indexes == {0, 1}


def test_complex_queries(indexing_v1_engine):
    index_uuid = indexing_v1_engine.create_index("db", "col", {"age": 1})
    indexing_v1_engine.insert_documents(
        "db",
        "col",
        [
            ({"age": 5, "_id": ObjectId()}, 0),
            ({"age": 10, "_id": ObjectId()}, 1),
            ({"age": 15, "_id": ObjectId()}, 2),
        ]
    )

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"$and": [{"age": {"$gt": 0}}, {"age": {"$lte": 10}}]}
    ).indexes == {0, 1}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"$or": [{"age": {"$eq": 10}}, {"age": {"$gt": 10}}]}
    ).indexes == {1, 2}

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"$nor": [{"age": {"$eq": 10}}, {"age": {"$gt": 10}}]}
    ).exclude_indexes == {1, 2}


def test_not_query(indexing_v1_engine):
    index_uuid = indexing_v1_engine.create_index("db", "col", {"age": 1})
    indexing_v1_engine.insert_documents(
        "db",
        "col",
        [
            ({"age": 5, "_id": ObjectId()}, 0),
            ({"age": 10, "_id": ObjectId()}, 1),
            ({"age": 15, "_id": ObjectId()}, 2),
        ]
    )

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$not": {"$gt": 10}}}
    ).exclude_indexes == {2}


def test_multi_index_query(indexing_v1_engine):
    indexing_v1_engine.create_index("db", "col", {"age": 1})
    indexing_v1_engine.create_index("db", "col", {"size": 1})
    indexing_v1_engine.insert_documents(
        "db",
        "col",
        [
            ({"age": 5, "size": 15, "_id": ObjectId()}, 0),
            ({"age": 10, "size": 3, "_id": ObjectId()}, 1),
            ({"age": 15, "size": 100, "_id": ObjectId()}, 2),
        ]
    )

    assert indexing_v1_engine.query(
        "db",
        "col",
        ReadInstructions(offset=0, chunk_size=5),
        filter_={"age": {"$lt": 15}, "size": {"$gt": 5}}
    ).indexes == {0}
