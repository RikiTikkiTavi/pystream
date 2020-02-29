from typing import TypeVar, Callable, Iterable, Collection, Generic, Hashable, Dict
import pystream.stream as s

T = TypeVar('T')
R = TypeVar('R')
H = TypeVar('H', bound=Hashable)


class Collector(Generic[T, R]):
    __collector_func: Callable[["s.Stream[T]"], R]

    def __init__(self, collector_func: Callable[["s.Stream[T]"], R]):
        self.__collector_func = collector_func

    def collect(self, stream: "s.Stream[T]") -> R:
        return self.__collector_func(stream)


def to_collection(collection: Callable[[Iterable[T]], Collection[T]]) -> Collector[T, Collection[T]]:
    return Collector(collection)


def grouping_by(key_getter: Callable[[T], H]) -> Collector[T, Dict[H, T]]:
    def collector_func(stream: "s.Stream[T]") -> Dict[H, T]:
        d: Dict[H, T] = {}
        for element in iter(stream):
            d.setdefault(key_getter(element), []).append(element)
        return d

    return Collector(collector_func)
