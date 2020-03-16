import unittest

from pystream.infrastructure.collectors import to_collection
from pystream.parallel_stream import ParallelStream
from pystream.sequential_stream import SequentialStream


class SequentialStreamTest(unittest.TestCase):
    COLLECTION = [5, 3, 1, 10, 51, 42, 7]
    DIVIDES_BY_THREE = lambda x: x % 3 == 0
    BUMPY_COLLECTION = [[5, 3, 1], [10], [51, 42, 7]]

    def setUp(self):
        self.stream = SequentialStream(self.COLLECTION)

    def test_whenMapping_thenReturnFunctionAppliedToAllElements(self):
        expected = [x for x in map(SequentialStreamTest.DIVIDES_BY_THREE, self.COLLECTION)]

        result = self.stream.map(SequentialStreamTest.DIVIDES_BY_THREE).collect(to_collection(list))

        self.assertEqual(expected, result)

    def test_whenFiltering_thenReturnElementsWhichEvaluateToTrue(self):
        expected = [x for x in filter(SequentialStreamTest.DIVIDES_BY_THREE, self.COLLECTION)]

        result = self.stream.filter(SequentialStreamTest.DIVIDES_BY_THREE).collect(to_collection(list))

        self.assertEqual(expected, result)

    def test_givenAnElementThatMatches_whenCheckingAnyMatch_thenReturnTrue(self):
        result = self.stream.any_match(SequentialStreamTest.DIVIDES_BY_THREE)

        self.assertTrue(result)

    def test_givenNotAllElementsMatch_whenCheckingAllMatch_thenReturnFalse(self):
        result = self.stream.all_match(SequentialStreamTest.DIVIDES_BY_THREE)

        self.assertFalse(result)

    def test_givenAllMatchingElements_whenCheckingAllMatch_thenReturnTrue(self):
        always_true = lambda x: True

        result = self.stream.all_match(always_true)

        self.assertTrue(result)

    def test_givenNoMatchingElements_whenCheckingAnyMatch_thenReturnFalse(self):
        always_false = lambda x: False

        result = self.stream.any_match(always_false)

        self.assertFalse(result)

    def test_whenCollectingToList_thenReturnAListContainingAllElements(self):
        result_list = self.stream.collect(to_collection(list))

        self.assertIsInstance(result_list, list)
        self.assertEqual(self.COLLECTION, result_list)

    def test_whenCollectingToSet_thenReturnASetContainingAllElements(self):
        result_set = self.stream.collect(to_collection(set))

        self.assertEqual(len(self.COLLECTION), len(result_set))
        self.assertIsInstance(result_set, set)
        for item in self.COLLECTION:
            self.assertTrue(item in result_set)

    def test_givenAListOfPairs_whenCollectingToDict_thenExpandPairsToKeyValue(self):
        dictionary = self.stream.map(lambda x: (x, SequentialStreamTest.DIVIDES_BY_THREE(x))).collect(to_collection(dict))

        self.assertEqual(len(self.COLLECTION), len(dictionary.keys()))
        self.assertIsInstance(dictionary, dict)
        for i in self.COLLECTION:
            self.assertEqual(dictionary[i], SequentialStreamTest.DIVIDES_BY_THREE(i))

    def test_whenForEach_thenCallFunctionOnAllItems(self):
        result = []
        add_to_list = lambda x: result.append(x)

        self.stream.for_each(add_to_list)

        self.assertEqual(self.COLLECTION, result)

    def test_givenFunctionWithTwoParameters_whenIteratingOverScalars_thenThrowTypeError(self):
        with self.assertRaises(TypeError):
            self.stream.map(lambda x, y: x + y).collect(to_collection(list))

    def test_givenStreamOfLists_whenFlattening_thenReturnStreamOfConcatenatedLists(self):
        result = SequentialStream(self.BUMPY_COLLECTION).flat_map(SequentialStream).collect(to_collection(list))

        self.assertEqual(self.COLLECTION, result)

    def test_givenNoMatchingElements_whenCheckingNoneMatch_thenReturnTrue(self):
        always_false = lambda x: False

        result = SequentialStream(self.COLLECTION).none_match(always_false)

        self.assertTrue(result)

    def test_givenMatchingElements_whenCheckingNoneMatch_thenReturnFalse(self):
        sometimes_true = lambda x: x % 2 == 0

        result = SequentialStream(self.COLLECTION).none_match(sometimes_true)

        self.assertFalse(result)

    def test_givenMultipleIterables_whenCreatingStream_thenIterablesAreConcatenated(self):
        result = SequentialStream(self.BUMPY_COLLECTION, self.COLLECTION).collect(to_collection(list))

        self.assertEqual(self.BUMPY_COLLECTION + self.COLLECTION, result)

    def test_whenZipping_thenIterateOverTheCollectionsTwoByTwo(self):
        expected = [(1, 4), (2, 5), (3, 6)]

        result = SequentialStream.zip([1, 2, 3], [4, 5, 6]).collect(to_collection(list))

        self.assertEqual(expected, result)

    def test_givenTupleIteration_whenUnzipping_thenReturnSeparateLists(self):
        expected = ((1, 3, 5), (2, 4, 6))

        first_list, second_list = SequentialStream([(1, 2), (3, 4), (5, 6)]).unzip()

        self.assertEqual(expected[0], first_list)
        self.assertEqual(expected[1], second_list)

    def test_whenCreatingFromNonIterableElements_thenCreateACollectionContainingAllParameters(self):
        result = SequentialStream.of(1, 2, 3, 4).collect(to_collection(list))

        self.assertEqual([1, 2, 3, 4], result)

    def test_whenCollectingToTuple_thenReturnATupleContainingTheCollection(self):
        result = SequentialStream([1, 2, 3, 4, 5]).collect(to_collection(tuple))

        self.assertIsInstance(result, tuple)
        self.assertEqual((1, 2, 3, 4, 5), result)

    def test_whenCalculatingSum_thenReturnSumOfCollection(self):
        stream_sum = self.stream.sum()

        self.assertEqual(sum(self.COLLECTION), stream_sum)

    def test_whenCheckingMinimum_thenReturnSmallestElementInTheCollection(self):
        smallest_element = self.stream.min()

        self.assertEqual(min(self.COLLECTION), smallest_element)

    def test_whenCheckingMaximum_thenReturnLargestElementInTheCollection(self):
        largest_element = self.stream.max()

        self.assertEqual(max(self.COLLECTION), largest_element)

    def test_whenCountingElementsOfACollection_thenReturnLengthOfCollection(self):
        count = self.stream.count()

        self.assertEqual(len(self.COLLECTION), count)

    def test_whenCountingElementsOfIterableWithoutLength_thenIndividuallyCountElements(self):
        self.stream = SequentialStream(zip(self.COLLECTION, self.COLLECTION))

        count = self.stream.count()

        self.assertEqual(len(self.COLLECTION), count)

    def test_whenReducing_thenReturnFinalValueOfAccumulator(self):
        sum_reducer = lambda accumulator, element: accumulator + element

        reduction = self.stream.reduce(0, sum_reducer)

        self.assertEqual(sum(self.COLLECTION), reduction)

    def test_whenTaking_thenReturnStreamWithOnlyFirstElements(self):
        first_elements = SequentialStream(range(0, 30)).limit(5).collect(to_collection(list))

        self.assertEqual([0, 1, 2, 3, 4], first_elements)

    def test_givenClassReference_whenMapping_thenCallClassConstructor(self):
        squares = SequentialStream.of(1, 2, 3, 4).map(AClassWithAMethod).map(AClassWithAMethod.get_square).collect(
            to_collection(list))

        self.assertEqual([1, 4, 9, 16], squares)

    def test_givenMethodWhichRequiresAParameter_whenMapping_thenInvokeMethodWithParameter(self):
        calculator_object = AClassWithAMethod(0)

        result = SequentialStream.of(1, 2, 3, 4).map(calculator_object.increment).collect(to_collection(list))

        self.assertEqual([1, 2, 3, 4], result)

    def test_givenBuiltinType_whenMapping_thenCallConstructorWithASingleParameter(self):
        result = SequentialStream.of("1", "2", "3").map(int).collect(to_collection(list))

        self.assertEqual([1, 2, 3], result)

    def test_givenMissingParameters_whenGettingRange_thenReturnInfiniteIterator(self):
        first_25 = SequentialStream.range().limit(25).collect(to_collection(list))

        self.assertEqual([x for x in range(0, 25)], first_25)

    def test_givenParameters_whenGettingRange_thenPassParametersToBuiltinRange(self):
        first5 = SequentialStream.range(5).collect(to_collection(list))

        self.assertEqual([x for x in range(0, 5)], first5)

    def test_whenGettingFirst_thenReturnNullableContainingFirstElementInIterable(self):
        first = SequentialStream.range().find_first().get()

        self.assertEqual(0, first)

    def test_givenEmptyStream_whenGettingFirst_thenReturnEmptyNullable(self):
        first = SequentialStream([]).find_first()

        self.assertFalse(first.is_present())

    def test_transition_to_parallel_returns_parallel(self):
        self.assertIsInstance(self.stream.parallel(), ParallelStream)


class AClassWithAMethod(object):

    def __init__(self, value: int):
        self.value = value

    def get_square(self) -> int:
        return self.value ** 2

    def increment(self, x: int) -> int:
        return self.value + x

    def sum_of_two_values(self, x, y) -> int:
        return x + y
