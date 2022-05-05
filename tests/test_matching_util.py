from pymongolite.backend.utils import document_filter_match


def test_simple_field_match():
    assert document_filter_match({"a": 1, "b": 2}, {"a": 1, "b": 3}) is False
    assert document_filter_match({"a": 1, "b": 2}, {"a": 1, "b": 2}) is True


def test_field_operator_gt_match():
    assert document_filter_match({"a": 1}, {"a": {"$gt": 0}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$gt": 1}}) is False
    assert document_filter_match({"a": 1}, {"a": {"$gt": 2}}) is False


def test_field_operator_lt_match():
    assert document_filter_match({"a": 1}, {"a": {"$lt": 0}}) is False
    assert document_filter_match({"a": 1}, {"a": {"$lt": 1}}) is False
    assert document_filter_match({"a": 1}, {"a": {"$lt": 2}}) is True


def test_field_operator_gte_match():
    assert document_filter_match({"a": 1}, {"a": {"$gte": 0}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$gte": 1}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$gte": 2}}) is False


def test_field_operator_lte_match():
    assert document_filter_match({"a": 1}, {"a": {"$lte": 0}}) is False
    assert document_filter_match({"a": 1}, {"a": {"$lte": 1}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$lte": 2}}) is True


def test_field_operator_eq_match():
    assert document_filter_match({"a": 1}, {"a": {"$eq": 0}}) is False
    assert document_filter_match({"a": 1}, {"a": {"$eq": 1}}) is True


def test_field_operator_ne_match():
    assert document_filter_match({"a": 1}, {"a": {"$ne": 0}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$ne": 1}}) is False


def test_field_exists():
    assert document_filter_match({"a": 1}, {"a": {"$exists": True}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$exists": False}}) is False
    assert document_filter_match({"b": 1}, {"a": {"$exists": True}}) is False
    assert document_filter_match({"b": 1}, {"a": {"$exists": False}}) is True


def test_field_in():
    assert document_filter_match({"a": 1}, {"a": {"$in": [0, 1]}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$in": [2, 3]}}) is False


def test_field_nin():
    assert document_filter_match({"a": 1}, {"a": {"$nin": [0, 1]}}) is False
    assert document_filter_match({"a": 1}, {"a": {"$nin": [2, 3]}}) is True


def test_not():
    assert document_filter_match({"a": 1}, {"a": {"$not": {"$gt": 2}}}) is True
    assert document_filter_match({"a": 1}, {"a": {"$not": {"$gt": 0}}}) is False


def test_and():
    assert (
        document_filter_match({"a": 1}, {"$and": [{"a": {"$gt": 0}}, {"a": {"$lt": 2}}]})
        is True
    )
    assert (
        document_filter_match({"a": 1}, {"$and": [{"a": {"$gt": 0}}, {"a": {"$eq": 0}}]})
        is False
    )


def test_or():
    assert (
        document_filter_match({"a": 1}, {"$or": [{"a": {"$gt": 2}}, {"a": {"$eq": 5}}]})
        is False
    )
    assert (
        document_filter_match({"a": 1}, {"$or": [{"a": {"$gt": 0}}, {"a": {"$eq": 0}}]})
        is True
    )
    assert (
        document_filter_match({"a": 1}, {"$or": [{"a": {"$gt": 0}}, {"a": {"$eq": 1}}]})
        is True
    )


def test_nor():
    assert (
        document_filter_match({"a": 1}, {"$nor": [{"a": {"$gt": 2}}, {"a": {"$eq": 0}}]})
        is True
    )
    assert (
        document_filter_match({"a": 1}, {"$nor": [{"a": {"$gt": 0}}, {"a": {"$eq": 5}}]})
        is False
    )
    assert (
        document_filter_match({"a": 1}, {"$nor": [{"a": {"$gt": 0}}, {"a": {"$eq": 1}}]})
        is False
    )
