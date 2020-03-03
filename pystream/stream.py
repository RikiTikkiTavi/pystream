import itertools
from functools import reduce
from typing import Generic, TypeVar, Callable, Iterable, Any, Tuple, Optional

from pystream.exceptions import SuppliedNoneException
from pystream.nullable import Nullable
import pystream.collectors.single_thread as collectors
import pystream.mixins.stream_creators_mixin as stream_creators_mixin
from pystream.abstracts.abstract_base_stream import AbstractBaseStream
import pystream.abstracts.abstract_collector as a_c

T = TypeVar('T')
S = TypeVar('S')


class Stream(Generic[T], AbstractBaseStream[T], stream_creators_mixin.StreamCreatorsMixin):
    """Stream class to perform functional-style operations in an aesthetically-pleasing manner.

    Args:
        *iterables (Iterable) : Source iterables for the Stream object. When multiple iterables are given,
        they will be concatenated.
    """

    def __init__(self, *iterables: Iterable[T]):
        super().__init__(*iterables)

    def map(self, fun: Callable[[T], S]) -> "Stream[S]":
        """Maps elements using the supplied function."""
        return Stream(map(fun, self._iterable))

    def filter(self, fun: Callable[[T], bool]) -> "Stream[T]":
        """Filters elements using the supplied function."""
        return Stream(filter(fun, self._iterable))

    def reduce(self, start_value: S, reducer: Callable[[S, T], S]) -> S:
        """Reduce using the supplied function."""
        return reduce(reducer, self._iterable, start_value)

    def for_each(self, fun: Callable[[T], Any]) -> None:
        """Calls the function with each element. This is a terminal operation."""
        for i in self._iterable:
            fun(i)

    def any(self, fun: Callable[[T], bool]) -> bool:
        """Returns True if any element of the stream matches the criteria."""
        return any(self.map(fun))

    def all(self, fun: Callable[[T], bool]) -> bool:
        """Returns True if all elements of the stream match the criteria."""
        return all(self.map(fun))

    def none(self, fun: Callable[[T], bool]) -> bool:
        """Returns True if no element of the stream matches the criteria."""
        return not self.any(fun)

    def find_first(self) -> Nullable[T]:
        """
        Returns an Nullable describing the first element of this stream, or an empty Nullable if the stream is empty.
        """
        for x in self._iterable:
            return Nullable(x).or_else_throw(SuppliedNoneException)
        return Nullable(None)

    def flat_map(self, mapper: Callable[[T], "Stream[S]"]) -> "Stream[S]":
        """
        When iterating over lists, flattens the stream by concatenating all lists using mapper function.
        """
        return Stream(itertools.chain(*map(mapper, self._iterable)))

    def count(self) -> int:
        """
        Returns the number of elements in the Stream. **Should never be used with an infinite stream!**
        """
        if hasattr(self._iterable, '__len__'):
            # noinspection PyTypeChecker
            return len(self._iterable)
        return self.reduce(0, lambda accumulator, element: accumulator + 1)

    def unzip(self) -> Tuple[tuple, ...]:
        """
        When iterating over tuples, unwraps the stream back to separate lists. This is a terminal operation.
        """
        return tuple(zip(*self._iterable))

    def sum(self) -> T:
        """Returns the sum of all elements in the stream."""
        return sum(self._iterable)

    def min(self) -> T:
        """Returns the min of all elements in the stream."""
        return min(self._iterable)

    def max(self) -> T:
        """Returns the max of all elements in the stream."""
        return max(self._iterable)

    def average(self) -> float:
        """Returns the average of all elements in the stream."""
        s: float = 0
        length: int = 0
        for i in self._iterable:
            s += i
            length += 1
        return s / length if length != 0 else 0

    def take(self, number: int) -> "Stream[T]":
        """Limit the stream to a specific number of items."""
        return Stream(itertools.islice(self._iterable, number))

    def first(self) -> Nullable[T]:
        """
        Returns a nullable containing the first element of the stream.
        If the stream is empty, returns an empty nullable.
        """
        return Nullable(next(self._iterable, None))

    def collect(self, collector: a_c.AbstractCollector[T, S]) -> S:
        return collector.collect(self)
