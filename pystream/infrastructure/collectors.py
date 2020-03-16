from typing import TypeVar, Callable, Iterable, Hashable, Dict, Generic, List
import pystream.sequential_stream as seq

_T = TypeVar('_T')
_R = TypeVar('_R')
_H = TypeVar('_H', bound=Hashable)


class Collector(Generic[_T, _R]):
    _collector_func: Callable[['seq.SequentialStream[_T]'], _R]

    def __init__(self, collector_func: Callable[['seq.SequentialStream[_T]'], _R]):
        # noinspection Mypy
        self._collector_func = collector_func

    def collect(self, stream: 'seq.SequentialStream[_T]') -> _R:
        return self._collector_func(stream)


def to_collection(collection: Callable[[Iterable[_T]], _R]) -> Collector[_T, _R]:
    return Collector(collection)


def grouping_by(key_getter: Callable[[_T], _H]) -> Collector[_T, Dict[_H, List[_T]]]:
    def collector_func(stream: 'seq.SequentialStream[_T]') -> Dict[_H, List[_T]]:
        d: Dict[_H, List[_T]] = {}
        for element in iter(stream):
            d.setdefault(key_getter(element), []).append(element)
        return d

    return Collector(collector_func)
