import time
from logger_utils import bootstrap_logger


timer_logger = bootstrap_logger(__name__)


class Timer:

    def __init__(self, retries=5):
        self._retries = retries
        self._current = 0
        self._timeout = False

    def start(self):
        if self._timeout:
            return

        self._current += 1
        if self._current == self._retries:
            self._timeout = True

        timer_logger.info(
            f"Retry {self._current}: Pausing API calls for {self._current} min(s)....")
        time_to_wait = self._current * 60
        time.sleep(time_to_wait)

    def timeout(self):
        return self._timeout

    def set_timeout(self):
        self._timeout = True
