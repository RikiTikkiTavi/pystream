from functools import reduce
from time import sleep

from pystream.parallel_stream import ParallelStream
from pystream.stream import Stream
from pystream.infrastructure.collectors import to_collection


def _cpu_map(x):
    sleep(0.1)
    return x ** 2


def _cpu_filter(x):
    sleep(0.1)
    return x % 2 == 0


def _reducer(x, y):
    sleep(0.1)
    return x + y


iterable = tuple(range(100, 150))


def parallel(iterable):
    p = ParallelStream(iterable).reduce(0, _reducer)


def sequential(iterable):
    s = Stream(iterable).reduce(0, _reducer)

parallel(iterable)
sequential(iterable)
