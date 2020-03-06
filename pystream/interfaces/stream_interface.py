from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Callable, Any

import pystream.interfaces.base_stream_interface as bsi
import pystream.nullable as nu

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


class StreamInterface(Generic[_AT], ABC, bsi.BaseStreamInterface[_AT]):
    _Predicate = Callable[[_AT], bool]
    _Action = Callable[[_AT], Any]

    @abstractmethod
    def filter(self, predicate: _Predicate) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the elements of this stream that match the given predicate.
        """
        ...

    @abstractmethod
    def map(self, mapper: Callable[[_AT], _RT]) -> 'StreamInterface[_RT]':
        """Maps elements using the supplied function."""
        ...

    @abstractmethod
    def all_match(self, predicate: _Predicate) -> bool:
        """Returns whether all elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def any_match(self, predicate: _Predicate) -> bool:
        """Returns whether any elements of this stream match the provided predicate."""
        ...

    @abstractmethod
    def count(self) -> int:
        """Returns the count of elements in this stream."""
        ...

    @abstractmethod
    def distinct(self) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the distinct elements
        (according to Object == Object) of this stream.
        """
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
    def for_each(self, action: _Action) -> None:
        """
        Performs an action for each element of this stream.
        This is terminal operation.
        """
        ...

    @abstractmethod
    def peek(self, action: _Action) -> 'StreamInterface[_AT]':
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
    def skip(self, n: int) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the remaining elements of this stream after
        discarding the first n elements of the stream.
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
    def sorted(self, reversed: bool = False) -> 'StreamInterface[_AT]':
        """
        Returns a stream consisting of the elements of this stream, sorted according to natural order.
        """
        ...

    @abstractmethod
    def reduce(self, start_value: _RT, accumulator: Callable[[_RT, _AT], _RT]) -> _RT:
        """
        Performs a reduction on the elements of this stream,
        using the provided initial value and an associative accumulation function, and returns the reduced value.
        """
        ...

    def collect(self, collector):
        """
        TODO: docs, types
        """
        ...
