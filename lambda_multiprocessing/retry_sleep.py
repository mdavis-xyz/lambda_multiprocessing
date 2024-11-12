from time import sleep
from random import uniform

# a context manager to help manage retries with backoff
# this doesn't catch errors or do retries
# it just does an increasing sleep
# (exponential backoff)
class IncreasingDelayManager:
    def __init__(self, initial_delay=0.01, backoff=2, max_delay=1, jitter_factor=0.1):
        self.initial_delay = initial_delay
        self.backoff = backoff
        self.max_delay = max_delay
        self.jitter_factor = jitter_factor

    def __enter__(self):
        self.delay = self.initial_delay
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def sleep(self):
        sleep(self.delay)
        # increment by backoff factor
        self.delay *= self.backoff

        # clip to max
        self.delay = min(self.delay, self.max_delay)

        # apply jitter as +/- jitter_factor %
        jitter_scale = uniform(1-self.jitter_factor, 1+self.jitter_factor)
        self.delay *= jitter_scale
