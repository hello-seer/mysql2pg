import time


class Timer:
    def __init__(self):
        self._start = time.perf_counter()

    def elapsed(self):
        end = time.perf_counter()
        return end - self._start
