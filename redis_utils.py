from redis import Redis
import traceback
from rq import Worker, Queue
from logger_utils import bootstrap_logger

redis_logger = bootstrap_logger(__name__)


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

    def enqueue_new_job(self, service_function, *args, **kwargs):
        result = None
        try:
            redis_logger.info(
                f"Enqueueing Service function = {service_function}")
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

    def get_paper_versions_from_redis(self):
        paper_versions = {}
        try:
            return self.redis_conn.hgetall("paper_versions")
        except Exception as e:
            redis_logger.exception(e)
        finally:
            return paper_versions

    def update_paper_versions(self, version_dict):
        try:
            self.redis_conn.delete("paper_versions")
            self.redis_conn.hmset("paper_versions", version_dict)
        except Exception as e:
            redis_logger.exception(e)
