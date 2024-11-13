# Timeout context manager for unit testing

import signal
from math import ceil

class TestTimeoutException(Exception):
    """Exception raised when a test takes too long"""
    pass

class TimeoutManager:
    # if a float is passed as seconds, it will be rounded up
    def __init__(self, seconds: int, description = "Test timed out"):
        self.seconds = ceil(seconds)
        self.description = description
        self.old_handler = None

    def __enter__(self):
        self.start()
        return self

    def start(self):
        assert self.old_handler is None, "Alarm already started"
        self.old_handler = signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(self.seconds)

    def timeout_handler(self, signum, frame):
        raise TestTimeoutException(self.description)

    def stop(self):
        if self.old_handler is not None:
            # Disable the alarm
            signal.alarm(0)
            # Restore old signal handler
            signal.signal(signal.SIGALRM, self.old_handler)
            self.old_handler = None

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        if exc_type is TestTimeoutException:
            return False  # Let the TestTimeoutException exception propagate
            
        # Propagate any other exceptions, or continue if no exception
        return False
