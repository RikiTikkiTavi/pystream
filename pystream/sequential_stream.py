from functools import reduce
from itertools import chain, islice, count
from typing import Generic, TypeVar, Callable, Iterable, Any, Tuple, Iterator, List, Union, Generator
from multiprocessing import cpu_count

import pystream.infrastructure.nullable as nullable
import pystream.parallel_stream as parallel_stream
import pystream.infrastructure.collectors as collectors
import pystream.core.utils as utils

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


class SequentialStream(Generic[_AT], Iterable[_AT]):
    """
    SequentialStream class to perform functional-style operations in an aesthetically-pleasing manner.
    Performs operations sequentially.

    :param `*iterables`: Source iterables for the SequentialStream object.  When multiple iterables are given, they will be concatenated.
    """

    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT]):
        self.__iterable = chain(*iterables)

    def __iter__(self):
        return self.iterator()

    def iterator(self) -> Iterator[_AT]:
        """
        Creates iterator from stream.
        This is terminal operation.

        :returns: Iterator over stream elements
        """
        return iter(self.__iterable)

    def partition_iterator(self, partition_size: int) -> Generator[List[_AT], None, None]:
        """
        Creates iterator over partitions of stream. This is terminal operation.

        :param partition_size: Length of partition
        :returns: Iterator over partitions of stream.
        """
        return utils.partition_generator(self.iterator(), partition_size)

    def map(self, mapper: Callable[[_AT], _RT]) -> "SequentialStream[_RT]":
        """
        Returns a stream consisting of the results of applying the given function to the elements of this stream.
        This is an intermediate operation.

        :param mapper: Mapper function
        :return: Stream with mapper operation lazily applied
        """
        return SequentialStream(map(mapper, self.__iterable))

    def filter(self, predicate: Callable[[_AT], bool]) -> "SequentialStream[_AT]":
        """
        Returns a stream consisting of the elements of this stream that match the given predicate.
        This is an intermediate operation.

        :param predicate: Predicate to apply to each element to determine if it should be included
        :return: The new stream
        """
        return SequentialStream(filter(predicate, self.__iterable))

    def reduce(self, identity: _RT, accumulator: Callable[[_RT, _AT], _RT]) -> _RT:
        """
        Performs a reduction on the elements of this stream, using the provided identity value and an associative
        accumulation function, and returns the reduced value.

        :param identity: The identity value for the accumulating function
        :param accumulator: Function for combining two values
        :return: The result of the reduction
        """
        return reduce(accumulator, self.__iterable, identity)

    def for_each(self, action: Callable[[_AT], Any]) -> None:
        """
        Performs an action for each element of this stream.
        This is terminal operation.

        :param action: An action to perform on the elements
        """
        for i in self.__iterable:
            action(i)

    def any_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """
        Returns whether any elements of this stream match the provided predicate.
        May not evaluate the predicate on all elements if not necessary for determining the result.
        If the stream is empty then false is returned and the predicate is not evaluated.

        :param predicate: A predicate to apply to elements of this stream.
        :return: true if any elements of the stream match the provided predicate, otherwise false.
        """
        return any(self.map(predicate))

    def all_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """
        Returns whether all elements of this stream match the provided predicate.
        May not evaluate the predicate on all elements if not necessary for determining the result.
        If the stream is empty then true is returned and the predicate is not evaluated.

        :param predicate: A predicate to apply to elements of this stream.
        :return: true if either all elements of the stream match the provided predicate or the stream is empty, otherwise false
        """
        return all(self.map(predicate))

    def none_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """
        Returns whether no elements of this stream match the provided predicate. May not evaluate the predicate on
        all elements if not necessary for determining the result. If the stream is empty then true is returned and
        the predicate is not evaluated.

        :param predicate: A predicate to apply to elements of this stream.
        :return: true if either no elements of the stream match the provided predicate or the stream is empty, otherwise false
        """
        return not self.any_match(predicate)

    def flat_map(self, mapper: Callable[[_AT], "SequentialStream[_RT]"]) -> "SequentialStream[_RT]":
        """
        Returns a stream consisting of the results of replacing each element of this stream with the contents of a
        mapped stream produced by applying the provided mapping function to each element.

        **API Note**:
        The flatMap() operation has the effect of applying a one-to-many transformation to the elements of the stream,
        and then flattening the resulting elements into a new stream.

        :param mapper: Function to apply to each element which produces a stream of new values.
        :return: The new stream
        """
        return SequentialStream(chain.from_iterable(map(mapper, self.__iterable)))

    def count(self) -> int:
        """
        Returns the number of elements in the SequentialStream. **Should never be used with an infinite stream!**
        """
        if hasattr(self.__iterable, '__len__'):
            # noinspection PyTypeChecker
            return len(self.__iterable)
        return self.reduce(0, lambda accumulator, element: accumulator + 1)

    def unzip(self) -> Tuple[tuple, ...]:
        """
        When iterating over tuples, unwraps the stream back to separate lists. This is a terminal operation.
        """
        return tuple(zip(*self.__iterable))

    def sum(self) -> Union[_AT, int]:
        """Returns the sum of all elements in the stream."""
        return sum(self.__iterable)

    def min(self) -> _AT:
        """Returns the min of all elements in the stream."""
        return min(self.__iterable)

    def max(self) -> _AT:
        """Returns the max of all elements in the stream."""
        return max(self.__iterable)

    def limit(self, number: int) -> "SequentialStream[_AT]":
        """
        Limit the stream to a specific number of items.
        """
        return SequentialStream(islice(self.__iterable, number))

    def find_first(self) -> nullable.Nullable[_AT]:
        """
        Returns an Nullable describing the first element of this stream, or an empty Nullable if the stream is empty.

        :return: An Nullable describing the first element of this stream, or an empty Nullable if the stream is empty
        """
        return nullable.Nullable(next(self.__iterable, None))

    def peek(self, action: Callable[[_AT], Any]) -> 'SequentialStream[_AT]':
        """
        Returns a stream consisting of the elements of this stream, additionally performing the provided action on each
        element as elements are consumed from the resulting stream.
        This is an intermediate operation.

        :param action: An action to perform on the elements as they are consumed from the stream
        :return: the new stream
        """

        def with_action(x):
            action(x)
            return x

        return self.map(with_action)

    def collect(self, collector: 'collectors.Collector[_AT, _RT]') -> _RT:
        return collector.collect(self)

    def parallel(self, n_processes: int = cpu_count(), chunk_size: int = 1) -> "parallel_stream.ParallelStream[_AT]":
        return parallel_stream.ParallelStream(self.__iterable, n_processes=n_processes, chunk_size=chunk_size)

    @staticmethod
    def range(*args) -> "SequentialStream[int]":
        """
        Creates an incrementing, integer stream.
        If arguments are supplied, they are passed as-is to the builtin `range` function.
        Otherwise, an infinite stream is created, starting at 0.
        """
        if len(args) == 0:
            return SequentialStream(count())
        else:
            return SequentialStream(range(*args))

    @staticmethod
    def of(*args: _RT) -> "SequentialStream[_RT]":
        """Creates a stream with non iterable arguments."""
        return SequentialStream(args)

    @staticmethod
    def zip(*iterables: Iterable[_AT]) -> "SequentialStream[Tuple[_AT, ...]]":
        """Creates a stream by *zipping* the iterables, instead of concatenating them."""
        return SequentialStream(zip(*iterables))
