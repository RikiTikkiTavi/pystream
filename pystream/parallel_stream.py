from functools import partial, reduce
from itertools import chain, islice, tee
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, List, Iterable, Tuple, Any
import pystream.infrastructure.utils as utils
import pystream.stream as stream
import operator

import pystream.backends.parallel_stream as parallel_back

import pystream.infrastructure.pipe as pipe

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


def _filter_partition(element: _AT, predicate: Callable[[_AT], bool]) -> List[_AT]:
    return [element] if predicate(element) else []


def _reducer(pair: Tuple[_AT, ...], /, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
    return reducer(*pair) if len(pair) == 2 else pair[0]


def _order_reducer(*args: Tuple[_AT, ...], selector: Callable[[Tuple[_AT, ...]], _AT]) -> _AT:
    return selector(args)


def _with_action(x: _AT, action: Callable[[_AT], Any]):
    action(x)
    return x


class ParallelStream(Generic[_AT]):
    __n_processes: int
    __pipe: pipe.Pipe
    __pool: Pool
    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT], n_processes: int = cpu_count()):
        self.__iterable = chain(*iterables)
        self.__n_processes = n_processes
        self.__pipe = pipe.Pipe()

    # ------------------------------------------------------------------------------
    # Backends
    # ------------------------------------------------------------------------------

    def __schedule(self, op: Callable, **op_kwargs):
        self.__pipe.append(partial(op, **op_kwargs))

    def __map_lazy(
            self,
            iterable: Iterable[_AT],
            /,
            mapper: Callable[[_AT], _RT],
            chunk_size: int
    ) -> Iterable[_RT]:
        """
        Lazy map implementation
        """
        return self.__pool.imap(
            mapper,
            iterable,
            chunksize=chunk_size
        )

    def __map_active(
            self,
            iterable: Iterable[_AT],
            /,
            mapper: Callable[[_AT], _RT]
    ) -> Iterable[_RT]:
        """
        Active map implementation
        """
        return self.__pool.map(
            mapper,
            iterable
        )

    def __filter_lazy(
            self,
            iterable: Iterable[_AT], /,
            predicate: Callable[[_AT], bool],
            chunk_size: int = 1
    ) -> Iterable[_AT]:
        """
        Lazy filter implementation
        """
        return utils.lazy_flat_generator(self.__pool.imap(
            partial(_filter_partition, predicate=predicate),
            iterable,
            chunksize=chunk_size
        ))

    def __filter_active(
            self,
            iterable: Iterable[_AT], /,
            predicate: Callable[[_AT], bool]
    ) -> Iterable[_AT]:
        """
        Active filter implementation
        """
        return utils.lazy_flat_generator(self.__pool.map(
            partial(_filter_partition, predicate=predicate),
            iterable
        ))

    def __reduce_lazy(
            self,
            iterable: Iterable[_AT],
            /,
            reducer: Callable[[_AT, _AT], _AT],
            chunk_size: int = 1
    ) -> _AT:
        """
        Lazy reduce implementation
        """
        while True:
            iterable = self.__pool.imap_unordered(
                func=partial(_reducer, reducer=reducer),
                iterable=utils.reduction_pairs_generator(iterable)
            )
            first_pair = tuple(islice(iterable, 2))
            if len(first_pair) == 1: return first_pair[0]
            iterable = chain(first_pair, iterable)

    # ------------------------------------------------------------------------------
    # Frontends
    # ------------------------------------------------------------------------------

    def map(
            self,
            mapper: Callable[[_AT], _RT],
            lazy: bool = True,
            chunk_size: int = 1
    ) -> 'ParallelStream[_RT]':
        if lazy:
            self.__schedule(self.__map_lazy, mapper=mapper, chunk_size=chunk_size)
        else:
            self.__schedule(partial(self.__map_active, mapper=mapper))
        return self

    def filter(
            self,
            predicate: Callable[[_AT], bool],
            lazy: bool = True,
            chunk_size: int = 1
    ) -> 'ParallelStream[_AT]':
        if lazy:
            self.__schedule(partial(self.__filter_lazy, predicate=predicate, chunk_size=chunk_size))
        else:
            self.__schedule(partial(self.__filter_active, predicate=predicate))
        return self

    def reduce(self, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
        with Pool(processes=self.__n_processes) as self.__pool:
            return self.__reduce_lazy(self.__pipe.to_iterable(self.__iterable), reducer)

    def max(self):
        return self.reduce(partial(_order_reducer, selector=max))

    def min(self):
        return self.reduce(partial(_order_reducer, selector=min))

    def for_each(self, action: Callable[[_AT], Any], lazy: bool = True, chunk_size: int = 1) -> None:
        with Pool(processes=self.__n_processes) as self.__pool:
            for i in self.map(action, lazy, chunk_size).iterator(): pass

    def peek(self, action: Callable[[_AT], Any], lazy: bool = True, chunk_size: int = 1) -> "ParallelStream[_AT]":
        return self.map(mapper=partial(_with_action, action=action), lazy=lazy, chunk_size=chunk_size)

    def iterator(self) -> Iterable[_AT]:
        with Pool(processes=self.__n_processes) as self.__pool:
            for c in self.__pipe.to_iterable(self.__iterable):
                yield c

    def sequential(self) -> 'stream.Stream[_AT]':
        return stream.Stream(self.iterator())

    def collect(self, collector: 'Collector[_AT, _RT]') -> _RT:
        with Pool(processes=self.__n_processes) as self.__pool:
            return collector.collect(self.__pipe.to_iterable(self.__iterable))
