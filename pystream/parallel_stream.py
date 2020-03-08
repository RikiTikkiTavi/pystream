from functools import partial, reduce
from itertools import chain
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, List, Iterable, Tuple
import pystream.infrastructure.utils as utils

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


def _filter_partition(element: _AT, predicate: Callable[[_AT], bool]) -> Tuple[_AT, ...]:
    return [element] if predicate(element) else []


def _reducer(pair: Tuple[_AT, ...], /, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
    return reducer(*pair) if len(pair) == 2 else pair[0]


class ParallelStream(Generic[_AT]):
    __n_processes: int
    __scheduled: List[Callable]
    __pool: Pool
    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT], n_processes: int = cpu_count()):
        self.__iterable = chain(*iterables)
        self.__n_processes = n_processes
        self.__scheduled = []

    def __map_lazy(self, iterable: Iterable[_AT], /, mapper: Callable[[_AT], _RT], chunk_size: int) -> Iterable[_RT]:
        return self.__pool.imap(
            mapper,
            iterable,
            chunksize=chunk_size
        )

    def __map_active(self, iterable: Iterable[_AT], /, mapper: Callable[[_AT], _RT]) -> \
            Iterable[_RT]:
        return self.__pool.map(
            mapper,
            iterable
        )

    def map(self,
            mapper: Callable[[_AT], _RT],
            lazy: bool = True,
            chunk_size: int = 1
            ) -> 'ParallelStream[_RT]':
        if lazy:
            self.__scheduled.append(partial(self.__map_lazy, mapper=mapper, chunk_size=chunk_size))
        else:
            self.__scheduled.append(partial(self.__map_active, mapper=mapper))
        return self

    def __filter_lazy(self,
                      iterable: Iterable[_AT], /,
                      predicate: Callable[[_AT], bool],
                      chunk_size: int = 1
                      ) -> Iterable[_AT]:
        return iter(utils.lazy_flat_generator(self.__pool.imap(
            partial(_filter_partition, predicate=predicate),
            iterable,
            chunksize=chunk_size
        )))

    def __filter_active(self,
                        iterable: Iterable[_AT], /,
                        predicate: Callable[[_AT], bool]
                        ) -> Iterable[_AT]:
        return iter(utils.lazy_flat_generator(self.__pool.map(
            partial(_filter_partition, predicate=predicate),
            iterable
        )))

    def filter(self, predicate: Callable[[_AT], bool], lazy: bool = True, chunk_size: int = 1) -> 'ParallelStream[_AT]':
        if lazy:
            self.__scheduled.append(partial(self.__filter_lazy, predicate=predicate, chunk_size=chunk_size))
        else:
            self.__scheduled.append(partial(self.__filter_active, predicate=predicate))
        return self

    def __reduce(self, iterable: Iterable[_AT], /, reducer: Callable[[_RT, _AT], _RT]):
        while True:
            pairs: Generator[Tuple[_AT, ...], None, None] = utils.reduction_pairs_generator(iterable)
            iterable = self.__pool.map(
                func=partial(_reducer, reducer=reducer),
                iterable=pairs
            )
            if len(iterable) == 1:
                return iterable[0]

    def reduce(self, start_value: _AT, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
        with Pool(processes=self.__n_processes) as self.__pool:
            return self.__reduce(self.__collect(), reducer)

    def __collect(self):
        return reduce(lambda seq, fun: fun(seq), self.__scheduled, self.__iterable)

    def collect(self, collector: 'Collector[_AT, _RT]') -> _RT:
        with Pool(processes=self.__n_processes) as self.__pool:
            return collector.collect(self.__collect())
