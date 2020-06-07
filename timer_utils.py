import time
from logger_utils import bootstrap_logger
from redis_utils import RedisHelper, PAUSE_SIGNAL, STOP_SIGNAL


timer_logger = bootstrap_logger(__name__)


class Timer:

    def __init__(self, retries=5, redis_helper=None):
        self._retries = retries
        self._current = 0
        self._timeout = False
        if redis_helper:
            self.redis_helper = redis_helper
        else:
            self.redis_helper = RedisHelper()

    def start(self):
        if self._timeout:
            return

        self._current += 1
        if self._current == self._retries:
            self._timeout = True
            self.redis_helper.send_stop_signal()

        timer_logger.info(f"Retry {self._current}: Pausing API calls for {self._current} min(s)....")
        time_to_wait = self._current * 60
        time.sleep(time_to_wait)

    def timeout(self):
        return self._timeout

    def set_timeout(self):
        self._timeout = True

    def reset(self):
        self._current = 0
        self.redis_helper.set_signals(PAUSE_SIGNAL, value=0)
