from itertools import islice


class Null:
    def __bool__(self):
        return False


def document_filter_match(document: dict, filter: dict) -> bool:
    if not filter:
        return True

    for field, pattern in filter.items():
        value = document.get(field, Null())

        if value == Null():
            continue

        if value == pattern:
            return True

    return False


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
    for action, values in override.items():
        if action == "$set":
            for field, value in values.items():
                document[field] = value

        if action == "$inc":
            for field, value in values.items():
                if field in document:
                    document[field] += value

        if action == "$addToSet":
            for field, value in values.items():
                if field in document and isinstance(document[field], list):
                    document[field].append(value)
    return document


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        yield chunk
