import pymongo as mongo
import traceback
from logger_utils import bootstrap_logger

db_logger = bootstrap_logger(__name__)


def get_db_handle(collection_name='papers'):
    username = 'abisek'
    password = 'abisek24'
    connection_string = 'mongodb+srv://%s:%s@cluster0-fiaze.mongodb.net/arxivdl?authSource=admin&retryWrites=true&w' \
        '=majority' % (username, password)

    connection = mongo.MongoClient(connection_string)
    return connection


def bulk_insert_or_update(commit_buffer=[], db_handle=None):
    if db_handle is None:
        db_handle = get_db_handle()
    if not commit_buffer:
        db_logger.error("Commit buffer is empty. Will exit!")
        return False
    exit_value = False
    insert_count = 0
    update_count = 0
    try:
        arxiv_db = db_handle['arxiv_dl']['papers']
        insert_buffer = []
        for entry in commit_buffer:
            update = entry.pop('update', False)
            if update:
                id_query = {"paper_id": entry['paper_id']}
                updated_values = {"$set": entry}
                update_results = arxiv_db.update_one(id_query, updated_values)
                update_count += update_results.modified_count
            else:
                insert_buffer.append(entry)
        insert_results = arxiv_db.insert_many(insert_buffer)
        insert_count += len(insert_results.inserted_ids)
        exit_value = True
    except Exception as e:
        db_logger.exception(e)
    finally:
        db_logger.warn(
            f"DB Stats - Buffer Length={len(commit_buffer)} :: Inserts={insert_count} :: Updates={update_count} :: Exit Value={exit_value}")
        return exit_value
