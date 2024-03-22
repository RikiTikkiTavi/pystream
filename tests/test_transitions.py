import unittest
from functools import reduce
from time import sleep

from pystream.collectors import to_collection
from pystream.parallel_stream import ParallelStream
from pystream.sequential_stream import SequentialStream


def DIVIDES_BY_THREE(x):
    return x % 3 == 0


def squared(x):
    return x ** 2


def sum_reducer(acc, el):
    return acc + el


class TransitionsTest(unittest.TestCase):
    COLLECTION = [5, 3, 1, 10, 51, 42, 7]

    def setUp(self):
        self.parallel_stream = ParallelStream(self.COLLECTION)
        self.stream = SequentialStream(self.COLLECTION)

    def test_transition_to_sequential_returns_sequential(self):
        self.assertIsInstance(self.parallel_stream.sequential(), SequentialStream)

    def test_parallelStream_filter_sequential_collect(self):
        s = self.parallel_stream.filter(DIVIDES_BY_THREE).sequential().collect(to_collection(list))
        c = list(filter(DIVIDES_BY_THREE, self.COLLECTION))
        self.assertTrue(s == c)

    def test_parallelStream_map_sequential_collect(self):
        s = self.parallel_stream.map(squared).sequential().collect(to_collection(list))
        c = list(map(squared, self.COLLECTION))
        self.assertTrue(s == c)

    def test_parallelStream_map_sequential_intermediateOp_collect(self):
        s = self.parallel_stream.map(squared).sequential().filter(DIVIDES_BY_THREE).collect(to_collection(list))
        c = list(filter(DIVIDES_BY_THREE, map(squared, self.COLLECTION)))
        self.assertTrue(s == c)

    def test_sequentialStream_intermediateOp_parallel_collect(self):
        s = self.stream.filter(DIVIDES_BY_THREE).parallel().collect(to_collection(list))
        c = list(filter(DIVIDES_BY_THREE, self.COLLECTION))
        self.assertTrue(s == c)

    def test_sequentialStream_intermediateOp_parallel_map_collect(self):
        s = self.stream.filter(DIVIDES_BY_THREE).parallel().map(squared).collect(to_collection(list))
        c = list(map(squared, filter(DIVIDES_BY_THREE, self.COLLECTION)))
        self.assertTrue(s == c)

    def test_sequentialStream_intermediateOp_parallel_filter_collect(self):
        s = self.stream.map(squared).parallel().filter(DIVIDES_BY_THREE).collect(to_collection(list))
        c = list(filter(DIVIDES_BY_THREE, map(squared, self.COLLECTION)))
        self.assertTrue(s == c)

    def test_sequentialStream_intermediateOp_parallel_reduce_collect(self):
        s = self.stream.map(squared).parallel().reduce(sum_reducer)
        c = reduce(sum_reducer, map(squared, self.COLLECTION))
        self.assertTrue(s == c)

    def test_parallelStream_map_reduce_sequential_terminalOp(self):
        s = self.parallel_stream \
            .map(squared) \
            .filter(DIVIDES_BY_THREE) \
            .sequential() \
            .all_match(DIVIDES_BY_THREE)

        self.assertTrue(s)