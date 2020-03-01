from typing import TypeVar, Callable, Iterable, Collection, Hashable, Dict, Generic
import pystream.stream as s
import pystream.abstracts.abstract_collector as a_c

T = TypeVar('T')
R = TypeVar('R')
H = TypeVar('H', bound=Hashable)


class Collector(Generic[T, R], a_c.AbstractCollector[T, R]):

    def __init__(self, collector_func: Callable[["s.Stream[T]"], R]):
        super().__init__(collector_func)

    def collect(self, stream: "s.Stream[T]") -> R:
        return self._collector_func(stream)


def to_collection(collection: Callable[[Iterable[T]], Collection[T]]) -> Collector[T, Collection[T]]:
    return Collector(collection)


def grouping_by(key_getter: Callable[[T], H]) -> Collector[T, Dict[H, T]]:
    def collector_func(stream: "s.Stream[T]") -> Dict[H, T]:
        d: Dict[H, T] = {}
        for element in iter(stream):
            d.setdefault(key_getter(element), []).append(element)
        return d

    return Collector(collector_func)
