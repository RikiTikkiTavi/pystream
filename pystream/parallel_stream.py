from functools import partial, reduce
from itertools import chain, repeat, islice
from multiprocessing.pool import Pool
from os import cpu_count
from typing import Generic, TypeVar, Callable, NamedTuple, Any, List, Iterable, Iterator, Union, Generator, cast, Tuple, \
    Collection, Protocol

from pystream.abstracts import abstract_collector as a_c
from pystream.abstracts.abstract_base_stream import AbstractBaseStream
from pystream.collectors.single_thread import to_collection
from pystream.nullable import Nullable
from pystream.stream import Stream

T = TypeVar('T')
S = TypeVar('S')
G = TypeVar('G')


def _map_partition(partition: Iterable[T], fun: Callable[[T], S]) -> Tuple[S, ...]:
    return Stream(partition).map(fun).collect(to_collection(tuple))


def _filter_partition(partition: Iterable[T], fun: Callable[[T], bool]) -> Tuple[T, ...]:
    return Stream(partition).filter(fun).collect(to_collection(tuple))


class ParallelStream(Generic[T], AbstractBaseStream[T]):
    __n_workers: int
    __scheduled: List[Callable]
    __pool: Pool

    def __init__(self, *iterables: Iterable[T], n_workers: int = cpu_count()):
        super().__init__(*iterables)
        self.__n_workers = n_workers
        self.__scheduled = []

    def __partition_generator(self, collection: Collection[T], n_elements: int) -> Generator[Tuple[T, ...], None, None]:
        it: Iterator[T] = iter(collection)
        while True:
            partition: Tuple[T, ...] = tuple(islice(it, n_elements))
            if len(partition) > 0:
                yield partition
            else:
                break

    def __create_partitions(self, collection: Collection[T]) -> Tuple[Tuple[T, ...], ...]:
        return *self.__partition_generator(collection, self.__calculate_n_elements(collection)),

    def __calculate_n_elements(self, collection: Collection[T]) -> int:
        n: int = len(collection) // self.__n_workers
        return n if n > 0 else 1

    def __map(self, fun: Callable[[T], S], collection: Collection[T]) -> Tuple[S, ...]:
        partitions: Tuple[Tuple[T, ...], ...] = self.__create_partitions(collection)
        return tuple(chain(*self.__pool.map(partial(_map_partition, fun=fun), partitions)))

    def map(self, fun: Callable[[T], S]) -> 'ParallelStream[S]':
        self.__scheduled.append(partial(self.__map, fun))
        return self

    def __filter(self, fun: Callable[[T], bool], collection: Collection[T]) -> Tuple[T, ...]:
        partitions: Tuple[Tuple[T, ...], ...] = self.__create_partitions(collection)
        return tuple(chain(*self.__pool.map(partial(_filter_partition, fun=fun), partitions)))

    def filter(self, fun: Callable[[T], bool]) -> 'ParallelStream[T]':
        self.__scheduled.append(partial(self.__filter, fun))
        return self

    def __collect(self) -> Stream[T]:
        with Pool(processes=self.__n_workers) as self.__pool:
            return Stream(reduce(lambda seq, fun: fun(seq), self.__scheduled, tuple(self._iterable)))

    def collect(self, collector: a_c.AbstractCollector[T, S]) -> S:
        return collector.collect(self.__collect())