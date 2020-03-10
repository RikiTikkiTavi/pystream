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
    return ParallelStream(iterable) \
        .map(_cpu_map) \
        .filter(_cpu_filter) \
        .sequential() \
        .all_match(_cpu_filter)


def sequential(iterable):
    return Stream(iterable) \
        .parallel()\
        .map(_cpu_map) \
        .filter(_cpu_filter) \
        .sequential() \
        .all_match(_cpu_filter)


print(parallel(iterable))
print(sequential(iterable))
