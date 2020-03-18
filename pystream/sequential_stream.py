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


class SequentialStream(Generic[_AT]):
    """SequentialStream class to perform functional-style operations in an aesthetically-pleasing manner.

    Args:
        *iterables (Iterable) : Source iterables for the SequentialStream object. When multiple iterables are given,
        they will be concatenated.
    """

    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT]):
        self.__iterable = chain(*iterables)

    def __iter__(self):
        return self.iterator()

    def iterator(self) -> Iterator[_AT]:
        """Creates iterator from stream. This is terminal operation."""
        return iter(self.__iterable)

    def partition_iterator(self, partition_size: int) -> Generator[List[_AT], None, None]:
        """Creates iterator over partitions of stream. This is terminal operation."""
        return utils.partition_generator(self.iterator(), partition_size)

    def map(self, mapper: Callable[[_AT], _RT]) -> "SequentialStream[_RT]":
        """Maps elements using the supplied function."""
        return SequentialStream(map(mapper, self.__iterable))

    def filter(self, predicate: Callable[[_AT], bool]) -> "SequentialStream[_AT]":
        """Filters elements using the supplied function."""
        return SequentialStream(filter(predicate, self.__iterable))

    def reduce(self, start_value: _RT, reducer: Callable[[_RT, _AT], _RT]) -> _RT:
        """Reduce using the supplied function."""
        return reduce(reducer, self.__iterable, start_value)

    def for_each(self, action: Callable[[_AT], Any]) -> None:
        """Calls the function with each element. This is a terminal operation."""
        for i in self.__iterable:
            action(i)

    def any_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """Returns True if any element of the stream matches the criteria."""
        return any(self.map(predicate))

    def all_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """Returns True if all elements of the stream match the criteria."""
        return all(self.map(predicate))

    def none_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """Returns True if no element of the stream matches the criteria."""
        return not self.any_match(predicate)

    def flat_map(self, mapper: Callable[[_AT], "SequentialStream[_RT]"]) -> "SequentialStream[_RT]":
        """
        When iterating over lists, flattens the stream by concatenating all lists using mapper function.
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
        """Limit the stream to a specific number of items."""
        return SequentialStream(islice(self.__iterable, number))

    def find_first(self) -> nullable.Nullable[_AT]:
        """
        Returns a nullable containing the first element of the stream.
        If the stream is empty, returns an empty nullable.
        """
        return nullable.Nullable(next(self.__iterable, None))

    def find_any(self) -> 'nullable.Nullable[_AT]':
        return self.find_first()

    def peek(self, action: Callable[[_AT], Any]) -> 'SequentialStream[_AT]':
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
