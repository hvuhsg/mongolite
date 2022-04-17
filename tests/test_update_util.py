from mongolite.backend.utils import update_document_with_override


def test_set():
    doc = {"a": 1}
    assert update_document_with_override(doc, {"$set": {"b": 2}}) == {"a": 1, "b": 2}


def test_unset():
    doc = {"a": 1}
    assert update_document_with_override(doc, {"$unset": {"a": ""}}) == {}


def test_inc():
    doc = {"a": 1}
    assert update_document_with_override(doc, {"$inc": {"a": 9}}) == {"a": 10}

    doc = {"a": 10}
    assert update_document_with_override(doc, {"$inc": {"a": -9}}) == {"a": 1}


def test_add_to_set():
    doc = {"a": [1]}

    assert update_document_with_override(doc, {"$addToSet": {"a": 2}}) == {"a": [1, 2]}
    assert update_document_with_override(doc, {"$addToSet": {"a": 2}}) == {"a": [1, 2]}


def test_add_to_set_list():
    doc = {"a": [1]}

    assert update_document_with_override(
        doc,
        {"$addToSet": {"a": [2, 3]}}
    ) == {"a": [1, [2, 3]]}


def test_add_to_set_each():
    doc = {"a": [1]}

    assert update_document_with_override(
        doc,
        {"$addToSet": {"a": {"$each": [2, 3, 3]}}}
    ) == {"a": [1, 2, 3]}


def test_pull():
    doc = {"a": [1]}

    assert update_document_with_override(doc, {"$pull": {"a": 1}}) == {"a": []}


def test_pull_stay_the_same():
    doc = {"a": [1]}

    assert update_document_with_override(doc, {"$pull": {"a": 2}}) == {"a": [1]}


def test_pull_condition():
    doc = {"a": [{'b': 1}, {'b': 2}]}

    assert update_document_with_override(doc, {"$pull": {"a": {'b': 1}}}) == {"a": [{"b": 2}]}


def test_push():
    doc = {"a": [0]}

    assert update_document_with_override(doc, {"$push": {"a": 0}}) == {"a": [0, 0]}


def test_push_each():
    doc = {"a": [0]}

    assert update_document_with_override(
        doc,
        {"$push": {"a": {"$each": [1, 2, 3]}}}
    ) == {"a": [0, 1, 2, 3]}


def test_push_sort():
    doc = {"a": [0]}

    assert update_document_with_override(
        doc,
        {"$push": {"a": {"$each": [3, 1, 2], "$sort": 1}}}
    ) == {"a": [0, 1, 2, 3]}

    doc = {"a": [0]}

    assert update_document_with_override(
        doc,
        {"$push": {"a": {"$each": [3, 1, 2], "$sort": -1}}}
    ) == {"a": [3, 2, 1, 0]}


def test_push_sort_slice():
    doc = {"a": [0]}

    assert update_document_with_override(
        doc,
        {"$push": {"a": {"$each": [1, 2, 3], "$slice": 2}}}
    ) == {"a": [0, 1]}
