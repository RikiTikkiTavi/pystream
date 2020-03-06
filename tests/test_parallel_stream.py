import unittest
from multiprocessing.pool import Pool
from time import sleep

from pystream.collectors.single_thread import to_collection
from pystream.parallel_stream import ParallelStream
from pystream.stream import Stream


def _cpu_map(x):
    sleep(0.1)
    return x ** 2


def _cpu_filter(x):
    sleep(0.1)
    return x % 2 == 0


def _filter(x):
    return x % 2 == 0


def _map(x):
    return x ** 2


class ParallelStreamTest(unittest.TestCase):
    COLLECTION = [5, 3, 1, 10, 51, 42, 7]
    DIVIDES_BY_THREE = lambda x: x % 3 == 0
    BUMPY_COLLECTION = [[5, 3, 1], [10], [51, 42, 7]]

    def test_combined(self):
        t = ParallelStream(self.COLLECTION).filter(_cpu_filter).map(_cpu_map).collect(to_collection(tuple))
        correct = Stream(self.COLLECTION).filter(_cpu_filter).map(_cpu_map).collect(to_collection(tuple))
        self.assertTrue(t == correct)

    def test_map(self):
        t = ParallelStream(self.COLLECTION*3).map(_cpu_map).collect(to_collection(tuple))
        correct = Stream(self.COLLECTION*3).map(_map).collect(to_collection(tuple))
        self.assertTrue(t == correct)

    def test_filter(self):
        t = ParallelStream(self.COLLECTION).filter(_cpu_filter).collect(to_collection(tuple))
        correct = Stream(self.COLLECTION).filter(_filter).collect(to_collection(tuple))

        self.assertTrue(t == correct)


class AClassWithAMethod(object):

    def __init__(self, value: int):
        self.value = value

    def get_square(self) -> int:
        return self.value ** 2

    def increment(self, x: int) -> int:
        return self.value + x

    def sum_of_two_values(self, x, y) -> int:
        return x + y
