from abc import ABC, abstractmethod
from typing import TypeVar, Any, Generic

from pystream.Stream import Stream

COLLECTED_TYPE = TypeVar('COLLECTED_TYPE')
STREAM_TYPE = TypeVar('STREAM_TYPE')


class CollectorInterface(ABC, Generic[STREAM_TYPE, COLLECTED_TYPE]):

    @abstractmethod
    def __call__(self, stream: Stream[STREAM_TYPE]) -> COLLECTED_TYPE: ...
