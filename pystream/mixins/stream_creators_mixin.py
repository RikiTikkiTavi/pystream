import itertools
from typing import Iterable, Tuple, TypeVar
import pystream.stream as stream

T = TypeVar('T')
S = TypeVar('S')


class StreamCreatorsMixin:

    @staticmethod
    def range(*args) -> "stream.Stream[int]":
        """
        Creates an incrementing, integer stream.
        If arguments are supplied, they are passed as-is to the builtin `range` function.
        Otherwise, an infinite stream is created, starting at 0.
        """
        if len(args) == 0:
            return stream.Stream(itertools.count())
        else:
            return stream.Stream(range(*args))

    @staticmethod
    def of(*args: S) -> "stream.Stream[S]":
        """Creates a stream with non iterable arguments.

        Examples:
            >>> stream.Stream.of(1,2,3,4).toList()
            [1,2,3,4]
            """
        return stream.Stream(args)

    @staticmethod
    def zip(*iterables: Iterable[T]) -> "stream.Stream[Tuple[T, ...]]":
        """Creates a stream by *zipping* the iterables, instead of concatenating them.

        Returns:
            Stream of tuples.
        """
        return stream.Stream(zip(*iterables))
