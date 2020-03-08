from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Callable, Any, Iterable

import pystream.interfaces.base_stream_interface as bsi
import pystream.infrastructure.nullable as nu

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


class StreamInterface(Generic[_AT], bsi.BaseStreamInterface[_AT], ABC):

    @abstractmethod
    def filter(self, predicate: Callable[[_AT], bool]) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the elements of this stream that match the given predicate.
        """
        ...

    @abstractmethod
    def map(self, mapper: Callable[[_AT], _RT]) -> 'StreamInterface[_RT]':
        """Maps elements using the supplied function."""
        ...

    @abstractmethod
    def all_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """Returns whether all elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def any_match(self, predicate: Callable[[_AT], bool]) -> bool:
        """Returns whether any elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def count(self) -> int:
        """Returns the count of elements in this stream."""
        ...

    @abstractmethod
    def find_any(self) -> 'nu.Nullable[_AT]':
        """
        Returns an Nullable describing some element of the stream, or an empty Nullable if the stream is empty.
        """
        ...

    @abstractmethod
    def find_first(self) -> 'nu.Nullable[_AT]':
        """
        Returns an Nullable describing first element of the stream, or an empty Nullable if the stream is empty.
        """
        ...

    @abstractmethod
    def flat_map(self, mapper: Callable[[_AT], 'StreamInterface[_RT]']) -> 'StreamInterface[_RT]':
        """
        Returns a stream consisting of the results of replacing each element of this stream
        with the contents of a mapped stream produced by applying the provided mapping function to each element.
        """
        ...

    @abstractmethod
    def for_each(self, action: Callable[[_AT], Any]) -> None:
        """
        Performs an action for each element of this stream.
        This is terminal operation.
        """
        ...

    @abstractmethod
    def peek(self, action: Callable[[_AT], Any]) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the elements of this stream, additionally performing the provided action
        on each element as elements are consumed from the resulting stream.
        """
        ...

    @abstractmethod
    def limit(self, max_size: int) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the elements of this stream, truncated to be no longer than maxSize in length.
        """
        ...

    @abstractmethod
    def min(self) -> _AT:
        """
        Returns the min of all elements in the stream.
        Elements must be comparable (They must implement __lt__, __gt__, __eq__ methods)
        """
        ...

    @abstractmethod
    def max(self) -> _AT:
        """
        Returns the max of all elements in the stream.
        Elements must be comparable (They must implement __lt__, __gt__, __eq__ methods)
        """
        ...

    @abstractmethod
    def reduce(self, start_value: _RT, accumulator: Callable[[_RT, _AT], _RT]) -> _RT:
        """
        Performs a reduction on the elements of this stream,
        using the provided initial value and an associative accumulation function, and returns the reduced value.
        """
        ...

    @abstractmethod
    def collect(self, collector):
        """
        TODO: docs, types
        """
        ...

    @staticmethod
    @abstractmethod
    def of(*elements: _AT) -> 'StreamInterface[_AT]':
        ...

    @staticmethod
    @abstractmethod
    def range(*elements: int) -> 'StreamInterface[int]':
        ...

    @staticmethod
    @abstractmethod
    def zip(*iterables: Iterable[_AT]) -> "StreamInterface[_AT]":
        ...
