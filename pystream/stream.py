import itertools
from typing import Generic, TypeVar, Callable, Iterable, Any, List, Set, Tuple, ClassVar

from pystream.nullable import Nullable
import pystream.collectors as collectors
import pystream.mixins.stream_creators_mixin as stream_creators_mixin
from pystream.abstracts.abstract_base_stream import AbstractBaseStream

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
        """
        Maps elements using the supplied function. When iterating over tuples, the function can take multiple
        arguments.
        """
        return Stream(map(fun, self.iterable))

    def filter(self, fun: Callable[[T], bool]) -> "Stream[T]":
        """Filters elements using the supplied function."""
        return Stream(filter(fun, self.iterable))

    def for_each(self, fun: Callable[[T], Any]) -> None:
        """
        Calls the function with each element. This is a terminal operation.
        """
        for i in self:
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

    def find_first(self, fun: Callable[[T], bool]) -> Nullable[T]:
        """
        Returns a Nullable of the first element matching the criteria. If none exist, returns an empty Nullable.
        """
        for i in self:
            if fun(i):
                return Nullable(i)
        return Nullable.empty()

    def flat_map(self, mapper: Callable[[T], "Stream[S]"]) -> "Stream[S]":
        """
        When iterating over lists, flattens the stream by concatenating all lists using mapper function.
        """
        return Stream(itertools.chain(*map(mapper, self.iterable)))

    def count(self) -> int:
        """
        Returns the number of elements in the Stream. **Should never be used with an infinite stream!**
        """
        if hasattr(self.iterable, '__len__'):
            return len(self.iterable)
        return self.reduce(0, lambda accumulator, element: accumulator + 1)

    def reduce(self, start_value: S, reducer: Callable[[S, T], S]) -> S:
        """ Reduce using the supplied function.

        Args:
            start_value: starting value for the accumulator.
            reducer (Callable) : e.g. lambda accumulator, element: accumulator + element"""
        accumulator = start_value
        for element in self:
            accumulator = reducer(accumulator, element)
        return accumulator

    def unzip(self) -> Tuple[tuple, ...]:
        """
        When iterating over tuples, unwraps the stream back to separate lists. This is a terminal operation.
        """
        return tuple(zip(*self.iterable))

    def sum(self):
        """Returns the sum of all elements in the stream."""
        return sum(self)

    def min(self) -> T:
        """Returns the min of all elements in the stream."""
        return min(self)

    def max(self) -> T:
        """Returns the max of all elements in the stream."""
        return max(self)

    def average(self) -> float:
        """Returns the average of all elements in the stream."""
        s: float = 0
        length: int = 0
        for i in self:
            s += i
            length += 1
        return s / length if length != 0 else 0

    def take(self, number: int) -> "Stream[T]":
        """Limit the stream to a specific number of items."""
        return Stream(itertools.islice(self, number))

    def first(self) -> Nullable[T]:
        """
        Returns a nullable containing the first element of the stream.
        If the stream is empty, returns an empty nullable.
        """
        return Nullable(next(self.iterable, None))

    def collect(self, collector: collectors.Collector[T, S]) -> S:
        return collector.collect(self)
