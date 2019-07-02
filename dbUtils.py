from pymongo import DESCENDING


def queryAll(collection, query=None, sort_by='published_date', order=DESCENDING):
    if query is None:
        query = {}

    cursor = None
    cursor = collection.find(query).sort(sort_by, order)
    return cursor


def queryOne(collection, query=None):
    if query is None:
        query = {}

    entry = collection.find_one(query)
    return entry


def insert(collection, entries=None):
    if entries is None:
        entries = []
    if len(entries) == 0:
        print("No data to insert. Provide atleast 1 document")
        return -1

    result = collection.insert_many(entries)
    return result
