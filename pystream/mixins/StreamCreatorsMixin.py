import itertools
from typing import Iterable, Tuple, TypeVar

from pystream.Stream import Stream

T = TypeVar('T')
S = TypeVar('S')


class StreamCreatorsMixin:

    @staticmethod
    def range(*args) -> "Stream[int]":
        """
        Creates an incrementing, integer stream.
        If arguments are supplied, they are passed as-is to the builtin `range` function.
        Otherwise, an infinite stream is created, starting at 0.
        """
        if len(args) == 0:
            return Stream(itertools.count())
        else:
            return Stream(range(*args))

    @staticmethod
    def of(*args: S) -> "Stream[S]":
        """Creates a stream with non iterable arguments.

        Examples:
            >>> Stream.of(1,2,3,4).toList()
            [1,2,3,4]
            """
        return Stream(args)

    @staticmethod
    def zip(*iterables: Iterable[T]) -> "Stream[Tuple[T, ...]]":
        """Creates a stream by *zipping* the iterables, instead of concatenating them.

        Returns:
            Stream of tuples.
        """
        return Stream(zip(*iterables))
