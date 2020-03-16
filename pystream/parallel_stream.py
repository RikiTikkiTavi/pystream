from functools import partial
from itertools import chain
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, Iterable, Tuple, Any
import pystream.core.utils as utils
import pystream.sequential_stream as stream
import pystream.core.pipe as pipe
import pystream.infrastructure.collectors as collectors

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


def _reducer(pair: Tuple[_AT, ...], /, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
    return reducer(*pair) if len(pair) == 2 else pair[0]


def _order_reducer(*args: _AT, selector: Callable[[Tuple[_AT, ...]], _AT]) -> _AT:
    return selector(args)


def _with_action(x: _AT, /, action: Callable[[_AT], Any]):
    action(x)
    return x


class ParallelStream(Generic[_AT]):
    __n_processes: int
    __pipe: pipe.Pipe[_AT]
    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT], n_processes: int = cpu_count()):
        self.__iterable = chain(*iterables)
        self.__n_processes = n_processes
        self.__pipe = pipe.Pipe()

    def __iterator_pipe(self, pool: Pool, chunk_size: int = 1):
        return pipe.filter_out_empty(pool.imap(self.__pipe.get_operation(), self.__iterable, chunksize=chunk_size))

    def map(self, mapper: Callable[[_AT], _RT]) -> 'ParallelStream[_RT]':
        self.__pipe = self.__pipe.map(mapper)
        return self

    def filter(self, predicate: Callable[[_AT], bool]) -> 'ParallelStream[_AT]':
        self.__pipe = self.__pipe.filter(predicate)
        return self

    def reduce(self, reducer: Callable[[_AT, _AT], _AT], chunk_size: int = 1) -> _AT:
        with Pool(processes=self.__n_processes) as pool:
            return utils.fold(
                self.__iterator_pipe(pool, chunk_size),
                partial(_reducer, reducer=reducer),
                pool,
                chunk_size
            )

    def max(self):
        return self.reduce(partial(_order_reducer, selector=max))

    def min(self):
        return self.reduce(partial(_order_reducer, selector=min))

    def for_each(self, action: Callable[[_AT], Any]) -> None:
        for _ in self.map(action).iterator(): pass

    def peek(self, action: Callable[[_AT], Any]) -> "ParallelStream[_AT]":
        return self.map(mapper=partial(_with_action, action=action))

    def iterator(self) -> Iterable[_AT]:
        with Pool(processes=self.__n_processes) as pool:
            for element in self.__iterator_pipe(pool): yield element

    def sequential(self) -> 'stream.SequentialStream[_AT]':
        return stream.SequentialStream(self.iterator())

    def collect(self, collector: 'collectors.Collector[_AT, _RT]', chunk_size: int = 1) -> _RT:
        with Pool(processes=self.__n_processes) as pool:
            return collector.collect(self.__iterator_pipe(pool, chunk_size))
