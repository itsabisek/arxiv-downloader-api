from pymongo import MongoClient


def getDB(host='localhost', port=27017):
    client = MongoClient(host, port)
    db = client['arxiv']
    db.create_collection('arxivpapers')
    return db['arxivpapers']


def insert(collection,entries):
    result = collection.insert(entries)
    return result.inserted_ids

def queryAll(collection):
    pass



