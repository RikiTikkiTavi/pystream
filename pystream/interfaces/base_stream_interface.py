from abc import abstractmethod, ABC
from typing import TypeVar, Generic, Iterator, Tuple, List, Generator
import pystream.interfaces.stream_interface as stream_interface

# The type of the stream elements
_AT = TypeVar('_AT')


class BaseStreamInterface(Generic[_AT], ABC):

    @abstractmethod
    def iterator(self) -> Iterator[_AT]:
        """
        Returns an iterator for the elements of this stream.
        This is a terminal operation.
        """
        raise NotImplemented

    @abstractmethod
    def partition_iterator(self, partition_size: int) -> Generator[List[_AT], None, None]:
        """
        Returns an iterator over the partitions of size partition_size.
        Each partition is a list of elements of type T.
        This is a terminal operation.
        """
        raise NotImplemented

    @abstractmethod
    def parallel(self) -> 'stream_interface.StreamInterface[_AT]':
        """
        Returns an equivalent stream that is parallel.
        """
        raise NotImplemented

    @abstractmethod
    def sequential(self) -> 'stream_interface.StreamInterface[_AT]':
        """
        Returns an equivalent stream that is sequential.
        """

    @abstractmethod
    def is_parallel(self) -> bool:
        """
        Returns whether this stream, if a terminal operation were to be executed, would execute in parallel.
        """
        raise NotImplemented
