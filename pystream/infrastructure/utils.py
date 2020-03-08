from itertools import islice
from typing import Generator, TypeVar, Tuple, Iterator, Iterable, List, Generic

T = TypeVar("T")


def partition_generator(iterable: Iterable[T], partition_length: int) -> Generator[Tuple[T, ...], None, None]:
    it: Iterator[T] = iter(iterable)
    while True:
        partition: Tuple[T, ...] = tuple(islice(it, partition_length))
        if len(partition) > 0:
            yield partition
        else:
            break


def lazy_flat_generator(iterable: Iterable[Iterable[T]]) -> Generator[T, None, None]:
    i: Iterable[T]
    j: T
    for i in iterable:
        for j in i:
            yield j


def reduction_pairs_generator(iterable: Iterable[T]) -> Generator[Tuple[T, ...], None, None]:
    it = iter(iterable)
    while True:
        pair: Tuple[T, ...] = tuple(islice(it, 2))
        if len(pair) == 0:
            break
        yield pair
