import itertools
from abc import ABC, abstractmethod
from numbers import Number
from typing import TypeVar, Generic, Iterator, Iterable

T = TypeVar('T', bound=Number)


class AbstractBaseStream(ABC, Generic[T], Iterable[T]):
    _iterable: Iterator[T]

    def __init__(self, *iterables: Iterable[T]):
        self._iterable = itertools.chain(*iterables)

    def __iter__(self) -> Iterator[T]:
        return self._iterable
