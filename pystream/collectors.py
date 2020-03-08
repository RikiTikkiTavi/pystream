from typing import TypeVar, Callable, Iterable, Collection, Hashable, Dict, Generic, List
import pystream.stream as s

T = TypeVar('T')
R = TypeVar('R')
H = TypeVar('H', bound=Hashable)


class Collector(Generic[T, R]):
    _collector_func: Callable[['s.Stream[T]'], R]

    def __init__(self, collector_func: Callable[['s.Stream[T]'], R]):
        # noinspection Mypy
        self._collector_func = collector_func

    def collect(self, stream: 's.Stream[T]') -> R:
        return self._collector_func(stream)


def to_collection(collection: Callable[[Iterable[T]], R]) -> Collector[T, R]:
    return Collector(collection)


def grouping_by(key_getter: Callable[[T], H]) -> Collector[T, Dict[H, List[T]]]:
    def collector_func(stream: 's.Stream[T]') -> Dict[H, List[T]]:
        d: Dict[H, List[T]] = {}
        for element in iter(stream):
            d.setdefault(key_getter(element), []).append(element)
        return d

    return Collector(collector_func)
