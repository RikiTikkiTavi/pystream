from functools import partial, reduce
from itertools import chain, repeat, islice
from multiprocessing.pool import Pool
from os import cpu_count
from typing import Generic, TypeVar, Callable, NamedTuple, Any, List, Iterable, Iterator, Union, Generator, cast

from pystream.abstracts import abstract_collector as a_c
from pystream.abstracts.abstract_base_stream import AbstractBaseStream
from pystream.collectors.single_thread import to_collection
from pystream.nullable import Nullable
from pystream.stream import Stream

T = TypeVar('T')
S = TypeVar('S')
G = TypeVar('G')


def _map_partition(partition: List[T], fun: Callable[[T], S]) -> List[S]:
    return Stream(partition).map(fun).collect(to_collection(list))


def _filter_partition(partition: List[T], fun: Callable[[T], bool]) -> List[T]:
    return cast(List[T], Stream(partition).filter(fun).collect(to_collection(list)))


class ParallelStream(Generic[T], AbstractBaseStream[T]):
    __empty: object = object()
    __n_workers: int

    __scheduled: List[Callable] = []

    def __init__(self, *iterables: Iterable[T], n_workers: int = cpu_count()):
        super().__init__(*iterables)
        self._iterable: List[T] = list(self._iterable)
        self.__n_workers = n_workers

    @classmethod
    def __partitions_gen(cls, collection: List[T], n_elements: int) -> Generator[List[T], None, None]:
        it: Iterator[T] = iter(collection)
        while True:
            partition: List[T] = list(islice(it, n_elements))
            if len(partition) > 0:
                yield partition
            else:
                break

    def __create_partitions(self, collection: List[T]) -> List[List[T]]:
        return [
            *self.__partitions_gen(
                collection=collection,
                n_elements=self.__calculate_n_elements(collection)
            )
        ]

    def __calculate_n_elements(self, collection: List[T]) -> int:
        n: int = len(collection) // self.__n_workers
        return n if n > 0 else 1

    def __map(self, fun: Callable[[T], S], collection: List[T]):
        partitions: List[List[T]] = self.__create_partitions(collection)
        with Pool(processes=self.__n_workers) as p:
            return list(chain(*p.map(partial(_map_partition, fun=fun), partitions)))

    def map(self, fun: Callable[[T], S]) -> 'ParallelStream[S]':
        # self.__pipe = partial(_composition, self.__pipe, fun)
        self.__scheduled.append(partial(self.__map, fun))
        return self

    def __filter(self, fun: Callable[[T], bool], collection: List[T]):
        partitions: List[List[T]] = self.__create_partitions(collection)
        with Pool(processes=self.__n_workers) as p:
            return list(chain(*p.map(partial(_filter_partition, fun=fun), partitions)))

    def filter(self, fun: Callable[[T], bool]) -> 'ParallelStream[T]':
        self.__scheduled.append(partial(self.__filter, fun))
        return self

    def to_list(self):
        return list(reduce(lambda seq, fun: fun(seq), self.__scheduled, self._iterable))
