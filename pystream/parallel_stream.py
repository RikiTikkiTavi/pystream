from functools import partial
from itertools import chain
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, Iterable, Tuple, Any, Generator
import pystream.core.utils as utils
import pystream.sequential_stream as stream
import pystream.core.pipe as core_pipe
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
    __pipe: core_pipe.Pipe[_AT]
    __iterable: Iterable[_AT]

    def __init__(
            self,
            *iterables: Iterable[_AT],
            n_processes: int = cpu_count(),
            chunk_size: int = 1,
    ):
        self.__iterable = chain(*iterables)
        self.__n_processes = n_processes
        self.__pipe = core_pipe.Pipe()
        self.__chunk_size = chunk_size

    def __iterator_pipe(self, pool: Pool):
        return core_pipe.filter_out_empty(
            pool.imap(self.__pipe.get_operation(), self.__iterable, chunksize=self.__chunk_size)
        )

    def iterator(self) -> Generator[_AT, None, None]:
        with Pool(processes=self.__n_processes) as pool:
            for element in self.__iterator_pipe(pool): yield element

    def partition_iterator(self, partition_size: int) -> Generator[Tuple[_AT, ...], None, None]:
        return utils.partition_generator(self.iterator(), partition_size)

    def map(self, mapper: Callable[[_AT], _RT]) -> 'ParallelStream[_RT]':
        self.__pipe = self.__pipe.map(mapper)
        return self

    def filter(self, predicate: Callable[[_AT], bool]) -> 'ParallelStream[_AT]':
        self.__pipe = self.__pipe.filter(predicate)
        return self

    def peek(self, action: Callable[[_AT], Any]) -> "ParallelStream[_AT]":
        return self.map(mapper=partial(_with_action, action=action))

    def reduce(self, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
        with Pool(processes=self.__n_processes) as pool:
            return utils.fold(self.__iterator_pipe(pool), partial(_reducer, reducer=reducer), pool, self.__chunk_size)

    def max(self) -> _AT:
        return self.reduce(partial(_order_reducer, selector=max))

    def min(self) -> _AT:
        return self.reduce(partial(_order_reducer, selector=min))

    def for_each(self, action: Callable[[_AT], Any]) -> None:
        for _ in self.map(action).iterator(): pass

    def sequential(self) -> 'stream.SequentialStream[_AT]':
        return stream.SequentialStream(self.iterator())

    def collect(self, collector: 'collectors.Collector[_AT, _RT]') -> _RT:
        with Pool(processes=self.__n_processes) as pool:
            return collector.collect(stream.SequentialStream(self.__iterator_pipe(pool)))
