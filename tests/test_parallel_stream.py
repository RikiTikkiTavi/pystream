import unittest
from time import sleep

from pystream.infrastructure.collectors import to_collection
from pystream.parallel_stream import ParallelStream
from pystream.stream import Stream


def DIVIDES_BY_THREE(x):
    return x % 3 == 0


def squared(x):
    return x ** 2


def sum_reducer(acc, el):
    return acc + el


class ParallelStreamTest(unittest.TestCase):
    COLLECTION = [5, 3, 1, 10, 51, 42, 7]

    def setUp(self):
        self.stream = ParallelStream(self.COLLECTION)

    def test_whenMapping_thenReturnFunctionAppliedToAllElements(self):
        expected = [x for x in map(DIVIDES_BY_THREE, self.COLLECTION)]

        result = self.stream.map(DIVIDES_BY_THREE).collect(to_collection(list))

        self.assertEqual(expected, result)

    def test_whenFiltering_thenReturnElementsWhichEvaluateToTrue(self):
        expected = [x for x in filter(DIVIDES_BY_THREE, self.COLLECTION)]

        result = self.stream.filter(DIVIDES_BY_THREE).collect(to_collection(list))

        self.assertEqual(expected, result)

    def test_whenReducing_thenReturnFinalValueOfAccumulator(self):
        reduction = self.stream.reduce(0, sum_reducer)

        self.assertEqual(sum(self.COLLECTION), reduction)

    def test_givenFunctionWithTwoParameters_whenIteratingOverScalars_thenThrowTypeError(self):
        with self.assertRaises(TypeError):
            self.stream.map(sum_reducer).collect(to_collection(list))

    def test_givenClassReference_whenMapping_thenCallClassConstructor(self):
        squares = Stream.of(1, 2, 3, 4).map(AClassWithAMethod).map(AClassWithAMethod.get_square).collect(
            to_collection(list))

        self.assertEqual([1, 4, 9, 16], squares)

    def test_transition_to_sequential_returns_sequential(self):
        self.assertIsInstance(self.stream.sequential(), Stream)

    def test_transition_filter_sequential_collect(self):
        s = self.stream.filter(DIVIDES_BY_THREE).sequential().collect(to_collection(list))
        c = list(filter(DIVIDES_BY_THREE, self.COLLECTION))
        self.assertTrue(s == c)

    def test_transition_map_sequential_collect(self):
        s = self.stream.map(squared).sequential().collect(to_collection(list))
        c = list(map(squared, self.COLLECTION))
        self.assertTrue(s == c)

    def test_transition_map_sequential_filter_collect(self):
        s = self.stream.map(squared).sequential().filter(DIVIDES_BY_THREE).collect(to_collection(list))
        c = list(filter(DIVIDES_BY_THREE, map(squared, self.COLLECTION)))
        self.assertTrue(s == c)

    def test_sequentialStream_intermediateOp_parallel_collect(self):
        pass

    def test_sequentialStream_intermediateOp_parallel_map_collect(self):
        pass

    def test_sequentialStream_intermediateOp_parallel_filter_collect(self):
        pass

    def test_sequentialStream_intermediateOp_parallel_reduce_collect(self):
        pass


class AClassWithAMethod(object):

    def __init__(self, value: int):
        self.value = value

    def get_square(self) -> int:
        return self.value ** 2

    def increment(self, x: int) -> int:
        return self.value + x

    def sum_of_two_values(self, x, y) -> int:
        return x + y
