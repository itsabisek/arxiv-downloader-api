from redis import Redis
from rq import Worker, Queue
from utils.db_utils import bulk_insert_or_update_wrapper, MIN_COMMIT_SIZE_TO_INSERT
from utils.logger_utils import bootstrap_logger
import os

redis_logger = bootstrap_logger(__name__)

PAUSE_SIGNAL = 'pause'
STOP_SIGNAL = 'stop'
GLOBAL_KEY = 'globals'
COMMIT_BUFFER = 'buffer'
VERSIONS_KEY = 'paper_versions'
MONGO_USERNAME = os.environ.get('MONGO_USERNAME', '')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD', '')


class RQHelper:
    def __init__(self, redis_conn):
        try:
            if redis_conn:
                self.redis_conn = redis_conn

                self.worker = Worker.all(connection=self.redis_conn)[0]
                self.parser_queue = None

                queues = self.worker.queues
                for queue in queues:
                    if queue.name == 'parser':
                        self.parser_queue = queue

                if not self.parser_queue:
                    self.parser_queue = Queue(
                        name='default', connection=self.redis_conn)
        except Exception as e:
            redis_logger.exception(e)

    def enqueue_db_job(self, force_insert=False):
        if MONGO_USERNAME and MONGO_PASSWORD:
            temp_commit_buffer = self.redis_conn.lrange(COMMIT_BUFFER, 0, -1)
            if temp_commit_buffer and (force_insert or len(temp_commit_buffer) >= MIN_COMMIT_SIZE_TO_INSERT):
                redis_logger.info(f"Found {len(temp_commit_buffer)} responses to commit. Will enque them all")
                self.enqueue_new_job(bulk_insert_or_update_wrapper,
                                     commit_buffer=temp_commit_buffer)
                self.redis_conn.delete(COMMIT_BUFFER)

    def enqueue_new_job(self, service_function, *args, **kwargs):
        result = None
        try:
            redis_logger.info(f"Enqueueing Service function = {service_function}")
            result = self.parser_queue.enqueue(
                service_function, *args, **kwargs)
        except Exception as e:
            redis_logger.exception(e)
        finally:
            return result

    def bulk_enqueue_jobs(self, function_list, args_list, kwargs_list):
        results = []
        for c, function in enumerate(function_list):
            results.append(self.enqueue_new_job(
                function, args_list[c], kwargs_list[c]))
        return results


class RedisHelper:
    def __init__(self, connection_dict=None):
        try:
            if not connection_dict:
                self.redis_conn = Redis()
            else:
                self.redis_conn = Redis(**connection_dict)
        except Exception as e:
            redis_logger.exception(e)

    def get_redis_connection(self):
        return self.redis_conn

    def _toggle_state(self, var, value):
        self.redis_conn.hset(GLOBAL_KEY, var, value)

    def set_signals(self, *args, **kwargs):
        if not args and not kwargs:
            return
        value = kwargs.get('value', None)
        values = kwargs.get('values', None)
        if value is None and values is None:
            return

        for index, var in enumerate(args):
            if values:
                val = values[index]
            else:
                val = value
            self.redis_conn.hset(GLOBAL_KEY, var, val)

    def send_pause_signal(self):
        pause_state = self.signal_state(PAUSE_SIGNAL)
        if not pause_state:
            self._toggle_state(PAUSE_SIGNAL, value=1)

    def send_stop_signal(self):
        stop_state = self.signal_state(STOP_SIGNAL)
        if not stop_state:
            self._toggle_state(STOP_SIGNAL, value=1)

    def signal_state(self, signal):
        return int(self.redis_conn.hget(GLOBAL_KEY, signal))

    def get_paper_versions_from_redis(self):
        paper_versions = {}
        try:
            paper_versions = self.redis_conn.hgetall(VERSIONS_KEY)
        except Exception as e:
            redis_logger.exception('Exception occured while trying to get paper versions - %s', e)
        finally:
            return paper_versions

    def update_paper_versions(self, version_dict):
        try:
            self.redis_conn.delete(VERSIONS_KEY)
            self.redis_conn.hmset(VERSIONS_KEY, version_dict)
            return True
        except Exception as e:
            redis_logger.exception('Exception occured while trying to update paper versions - %s', e)
            return False

    def update_commit_buffer(self, commit_buffer):
        try:
            if commit_buffer:
                self.redis_conn.lpush(COMMIT_BUFFER, *commit_buffer)
            return self.redis_conn.llen(COMMIT_BUFFER)
        except Exception as e:
            redis_logger.exception('Exception occured while updating - %s ', e)
            return False

    def clear_all_keys(self):
        for key in [COMMIT_BUFFER, VERSIONS_KEY]:
            self.redis_conn.delete(key)
