from functools import partial, reduce
from itertools import chain, repeat, islice
from multiprocessing.pool import Pool, IMapIterator
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, NamedTuple, Any, List, Iterable, Iterator, Union, Generator, cast, Tuple, \
    Collection, Protocol, Optional

from pystream.abstracts import abstract_collector as a_c
from pystream.abstracts.abstract_base_stream import AbstractBaseStream
from pystream.collectors.single_thread import to_collection
from pystream.helpers.parallel_stream_helpers import partition_generator
from pystream.nullable import Nullable
from pystream.stream import Stream

T = TypeVar('T')
S = TypeVar('S')
G = TypeVar('G')


def _filter_partition(*elements: T, fun: Callable[[T], bool]) -> Tuple[T, ...]:
    return Stream(elements).filter(fun).collect(to_collection(tuple))


class ParallelStream(Generic[T], AbstractBaseStream[T]):
    __n_processes: int
    __scheduled: List[Callable]
    __pool: Pool

    def __init__(
            self,
            *iterables: Iterable[T],
            n_processes: int = cpu_count()
    ):
        super().__init__(*iterables)
        self.__n_processes = n_processes
        self.__scheduled = []

    def __create_partitions(self, collection: Collection[T]) -> Tuple[Tuple[T, ...], ...]:
        return *partition_generator(collection, self.__calculate_n_elements(collection)),

    def __calculate_n_elements(self, iterable: Iterable[T]) -> int:
        n: int = len(tuple(iterable)) // self.__n_processes
        return n if n > 0 else 1

    def __map(self,
              fun: Callable[[T], S],
              collection: Iterable[T],
              chunk_size: Optional[int] = 1) -> Iterator[S]:
        return self.__pool.imap(
            fun,
            collection,
            chunksize=Nullable(chunk_size).or_else(self.__calculate_n_elements(collection))
        )

    def map(self, fun: Callable[[T], S], chunk_size: Optional[int] = 1) -> 'ParallelStream[S]':
        self.__scheduled.append(partial(self.__map, fun=fun, chunk_size=chunk_size))
        return self

    def __filter(self,
                 fun: Callable[[T], bool],
                 collection: Iterable[T],
                 chunk_size: Optional[int] = 1
                 ) -> Iterator[T]:
        chunk_size: int = Nullable(chunk_size).or_else(self.__calculate_n_elements(collection))
        return self.__pool.imap(
            partial(_filter_partition, fun=fun),
            collection,
            chunksize=chunk_size
        )

    def filter(self, fun: Callable[[T], bool]) -> 'ParallelStream[T]':
        self.__scheduled.append(partial(self.__filter, fun))
        return self

    def __for_each(self):
        pass

    def for_each(self, fun: Callable[[T], Any]) -> None:
        pass

    def __collect(self) -> Stream[T]:
        with Pool(processes=self.__n_processes) as self.__pool:
            return Stream(reduce(lambda seq, fun: fun(seq), self.__scheduled, tuple(self._iterable)))

    def collect(self, collector: a_c.AbstractCollector[T, S]) -> S:
        return collector.collect(self.__collect())
