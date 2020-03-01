from abc import abstractmethod, ABC
from typing import Generic, Callable, TypeVar
import pystream.stream as s

T = TypeVar('T')
R = TypeVar('R')


class AbstractCollector(Generic[T, R], ABC):
    _collector_func: Callable[["s.Stream[T]"], R]

    def __init__(self, collector_func: Callable[["s.Stream[T]"], R]):
        self._collector_func = collector_func

    def collect(self, stream: "s.Stream[T]") -> R: ...
