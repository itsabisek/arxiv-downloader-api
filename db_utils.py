import pymongo as mongo
import json
import dateutil.parser as dateparser
from logger_utils import bootstrap_logger
import os

db_logger = bootstrap_logger(__name__)

CONNECTION_STRING = 'mongodb+srv://%s:%s@cluster0-fiaze.mongodb.net/arxivdl?authSource=admin&retryWrites=true&w=majority'


def get_db_handle(collection_name='papers'):
    try:
        username = os.environ.get('MONGO_USERNAME', '')
        password = os.environ.get('MONGO_PASSWORD', '')

        if not username or not password:
            raise Exception("Mongodb Username/ Password not found in environment varibales")

        connection_string = CONNECTION_STRING % (username, password)

        connection = mongo.MongoClient(connection_string)
        return connection
    except Exception as e:
        db_logger.exception('Exception while getting db handle - %s', e)


def bulk_insert_or_update_wrapper(commit_buffer, db_handle=None):

    def _decode_date_time(json_data):
        published_date = json_data.get('published_date', '')
        updated_date = json_data.get('updated_date', '')
        if published_date:
            json_data['published_date'] = dateparser.parse(published_date)
        if updated_date:
            json_data['updated_data'] = dateparser.parse(updated_date)
        return json_data

    real_commit_buffer = []
    try:
        db_handle = db_handle or get_db_handle()
        if not db_handle:
            raise Exception('Error while getting the database handle.')

        for commit_data in commit_buffer:
            real_commit_buffer.append(json.loads(commit_data, object_hook=_decode_date_time))
        if real_commit_buffer:
            success = bulk_insert_or_update(real_commit_buffer, db_handle)
            return success
        else:
            db_logger.error("Commit buffer is empty. Will exit!")
            return False
    except Exception as e:
        db_logger.exception("Exception occured while inserting data to db - %s", e)
        return False


def bulk_insert_or_update(commit_buffer=[], db_handle=None):

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
        return exit_value

    finally:
        db_logger.warning(f"DB Stats - Buffer Length={len(commit_buffer)} :: Inserts={insert_count} :: Updates={update_count} :: Exit Value={exit_value}")
        return exit_value
