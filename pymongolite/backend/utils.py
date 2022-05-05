from itertools import islice


class Null:
    def __bool__(self):
        return False


def is_condition(item) -> bool:
    return isinstance(item, dict) and next(iter(item.keys())).startswith("$")


def document_filter_match(document: dict, filter: dict) -> bool:
    if not filter:
        return True

    for field, pattern in filter.items():
        pattern_is_condition = is_condition(pattern)
        field_is_gate_condition = field.startswith("$")
        value = document.get(field, Null())

        if not pattern_is_condition and not field_is_gate_condition:
            if value != pattern:
                return False
            else:
                continue

        if field_is_gate_condition:
            if field == "$and" and not all(
                    map(
                        lambda filter_: document_filter_match(document, filter_),
                        pattern,
                    )
            ):
                return False

            if field == "$or" and not any(
                    map(
                        lambda filter_: document_filter_match(document, filter_),
                        pattern,
                    )
            ):
                return False

            if field == "$nor" and any(
                    map(
                        lambda filter_: document_filter_match(document, filter_),
                        pattern,
                    )
            ):
                return False

        if "$eq" in pattern and value != pattern["$eq"]:
            return False

        if "$ne" in pattern and value == pattern["$ne"]:
            return False

        if "$gt" in pattern and value <= pattern["$gt"]:
            return False

        if "$gte" in pattern and value < pattern["$gte"]:
            return False

        if "$lt" in pattern and value >= pattern["$lt"]:
            return False

        if "$lte" in pattern and value > pattern["$lte"]:
            return False

        if "$exists" in pattern:
            if pattern["$exists"] and not field in document:
                return False
            if not pattern["$exists"] and field in document:
                return False

        if "$in" in pattern and value not in pattern["$in"]:
            return False

        if "$nin" in pattern and value in pattern["$nin"]:
            return False

        if "$not" in pattern and document_filter_match(
            document, {field: pattern["$not"]}
        ):
            return False

    return True


def update_with_fields(document: dict, fields: dict):
    if not fields:
        return document

    if next(iter(fields.values())) == 0:
        new_doc = document
    else:
        new_doc = {}

    for field, include in fields.items():
        if include and field in document:
            new_doc[field] = document[field]
        else:
            new_doc.pop(field, None)

    return new_doc


def update_document_with_override(document: dict, override: dict):
    document = document.copy()
    for action, fields in override.items():
        if action == "$set":
            for field, value in fields.items():
                document[field] = value

        if action == "$unset":
            for field, _ in fields.items():
                document.pop(field, None)

        if action == "$inc":
            for field, value in fields.items():
                if field in document:
                    document[field] += value

        if action == "$addToSet":
            for field, value in fields.items():
                if field in document and isinstance(document[field], list):
                    if not is_condition(value):
                        if value not in document[field]:
                            document[field].append(value)
                    elif "$each" in value:
                        items: list = value["$each"]

                        document[field].extend(items)
                        document[field] = list(set(document[field]))

        if action == "$push":
            for field, value in fields.items():
                if not is_condition(value):
                    document[field].append(value)
                else:
                    if "$each" in value:
                        items: list = value["$each"]

                        document[field].extend(items)

                        if "$sort" in value:
                            document[field].sort(reverse=value["$sort"] == -1)

                        if "$slice" in value:
                            document[field] = document[field][: value["$slice"]]

        if action == "$pull":
            for field, filter in fields.items():
                if not is_condition(filter):
                    try:
                        document[field].remove(filter)
                    except ValueError:
                        pass

                    continue

                sub_documents_to_remove = []
                for sub_document in document[field]:
                    if document_filter_match(sub_document, {field: filter}):
                        sub_documents_to_remove.append(sub_document)

                for sub_document_to_remove in sub_documents_to_remove:
                    document[field].remove(sub_document_to_remove)

    return document


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        yield chunk
