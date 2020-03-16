import unittest
from itertools import repeat
from time import sleep, time

from pystream.infrastructure.collectors import to_collection
from pystream.parallel_stream import ParallelStream
from pystream.sequential_stream import SequentialStream


def DIVIDES_BY_THREE(x):
    return x % 3 == 0


def DIVIDES_BY_TWO(x):
    return x % 2 == 0


def squared(x):
    return x ** 2


def sum_reducer(acc, el):
    return acc + el


class ParallelStreamTest(unittest.TestCase):
    COLLECTION = tuple(range(20))

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

    def test_map_filter(self):
        self.assertEqual(
            self.stream.map(squared).filter(DIVIDES_BY_THREE).collect(to_collection(tuple)),
            tuple(filter(DIVIDES_BY_THREE, map(squared, self.COLLECTION)))
        )

    def test_filter_map(self):
        self.assertEqual(
            self.stream.filter(DIVIDES_BY_THREE).map(squared).collect(to_collection(tuple)),
            tuple(map(squared, filter(DIVIDES_BY_THREE, self.COLLECTION)))
        )

    def test_filter_filter(self):
        self.assertEqual(
            self.stream.filter(DIVIDES_BY_THREE).filter(DIVIDES_BY_TWO).collect(to_collection(tuple)),
            tuple(filter(DIVIDES_BY_TWO, filter(DIVIDES_BY_THREE, self.COLLECTION)))
        )

    def test_whenReducing_thenReturnFinalValueOfAccumulator(self):
        reduction = self.stream.reduce(sum_reducer)

        self.assertEqual(sum(self.COLLECTION), reduction)

    def test_givenFunctionWithTwoParameters_whenIteratingOverScalars_thenThrowTypeError(self):
        with self.assertRaises(TypeError):
            self.stream.map(sum_reducer).collect(to_collection(list))

    def test_givenClassReference_whenMapping_thenCallClassConstructor(self):
        squares = SequentialStream.of(1, 2, 3, 4).map(AClassWithAMethod).map(AClassWithAMethod.get_square).collect(
            to_collection(list))

        self.assertEqual([1, 4, 9, 16], squares)

    def test_transition_to_sequential_returns_sequential(self):
        self.assertIsInstance(self.stream.sequential(), SequentialStream)

    def test_max(self):
        self.assertEqual(self.stream.max(), max(self.COLLECTION))

    def test_min(self):
        self.assertEqual(self.stream.min(), min(self.COLLECTION))

    def test_peek(self):
        s = self.stream.map(squared).peek(print).collect(to_collection(tuple))
        self.assertTupleEqual(s, tuple(map(squared, self.COLLECTION)))

    def test_objects_type_is_empty(self):
        tup = tuple(repeat(_Empty, 10))
        self.assertEqual(ParallelStream(tup).collect(to_collection(tuple)), tup)


class AClassWithAMethod(object):

    def __init__(self, value: int):
        self.value = value

    def get_square(self) -> int:
        return self.value ** 2

    def increment(self, x: int) -> int:
        return self.value + x

    def sum_of_two_values(self, x, y) -> int:
        return x + y


class _Empty:
    pass
