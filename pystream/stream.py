from functools import reduce
from itertools import chain, islice, count
from typing import Generic, TypeVar, Callable, Iterable, Any, Tuple, Iterator, List

import pystream.infrastructure.nullable as nullable
import pystream.interfaces.stream_interface as stream_interface
import pystream.parallel_stream as parallel_stream
from multiprocessing import cpu_count
_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


class Stream(Generic[_AT], stream_interface.StreamInterface[_AT]):
    """Stream class to perform functional-style operations in an aesthetically-pleasing manner.

    Args:
        *iterables (Iterable) : Source iterables for the Stream object. When multiple iterables are given,
        they will be concatenated.
    """

    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT]):
        self.__iterable = chain(*iterables)

    def iterator(self) -> Iterator[_AT]:
        return iter(tuple(self.__iterable))

    def __iter__(self):
        return iter(self.__iterable)

    def partition_iterator(self, partition_size: int) -> Iterator[List[_AT]]:
        it: Iterator[_AT] = iter(tuple(self.__iterable))
        while True:
            partition: List[_AT] = list(islice(it, partition_size))
            if len(partition) > 0:
                yield partition
            else:
                break

    def sequential(self) -> 'stream_interface.StreamInterface[_AT]':
        raise NotImplemented

    def is_parallel(self) -> bool:
        raise NotImplemented

    def map(self, mapper: Callable[[_AT], _RT]) -> "Stream[_RT]":
        """Maps elements using the supplied function."""
        return Stream(map(mapper, self.__iterable))

    def filter(self, predicate: Callable[[_AT], bool]) -> "Stream[_AT]":
        """Filters elements using the supplied function."""
        return Stream(filter(predicate, self.__iterable))

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
        return not self.any(predicate)

    def flat_map(self, mapper: Callable[[_AT], "Stream[_RT]"]) -> "Stream[_RT]":
        """
        When iterating over lists, flattens the stream by concatenating all lists using mapper function.
        """
        # TODO: lazy chaining!
        return Stream(chain(*map(mapper, self.__iterable)))

    def count(self) -> int:
        """
        Returns the number of elements in the Stream. **Should never be used with an infinite stream!**
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

    def sum(self) -> _AT:
        """Returns the sum of all elements in the stream."""
        return sum(self.__iterable)

    def min(self) -> _AT:
        """Returns the min of all elements in the stream."""
        return min(self.__iterable)

    def max(self) -> _AT:
        """Returns the max of all elements in the stream."""
        return max(self.__iterable)

    def limit(self, number: int) -> "Stream[_AT]":
        """Limit the stream to a specific number of items."""
        return Stream(islice(self.__iterable, number))

    def find_first(self) -> nullable.Nullable[_AT]:
        """
        Returns a nullable containing the first element of the stream.
        If the stream is empty, returns an empty nullable.
        """
        return nullable.Nullable(next(self.__iterable, None))

    def find_any(self) -> 'nullable.Nullable[_AT]':
        return self.find_first()

    def peek(self, action: Callable[[_AT], Any]) -> 'Stream[_AT]':
        def with_action(x):
            action(x)
            return x

        return self.map(with_action)

    def collect(self, collector: 'Collector[_AT, _RT]') -> _RT:
        return collector.collect(self)

    def parallel(self, n_processes: int = cpu_count()) -> "parallel_stream.ParallelStream[_AT]":
        return parallel_stream.ParallelStream(self.__iterable, n_processes)

    @staticmethod
    def range(*args) -> "Stream[int]":
        """
        Creates an incrementing, integer stream.
        If arguments are supplied, they are passed as-is to the builtin `range` function.
        Otherwise, an infinite stream is created, starting at 0.
        """
        if len(args) == 0:
            return Stream(count())
        else:
            return Stream(range(*args))

    @staticmethod
    def of(*args: _RT) -> "Stream[_RT]":
        """Creates a stream with non iterable arguments."""
        return Stream(args)

    @staticmethod
    def zip(*iterables: Iterable[_AT]) -> "Stream[Tuple[_AT, ...]]":
        """Creates a stream by *zipping* the iterables, instead of concatenating them."""
        return Stream(zip(*iterables))
